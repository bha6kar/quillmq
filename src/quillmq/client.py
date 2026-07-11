# SPDX-License-Identifier: Apache-2.0
"""Async client: Connection, Channel, Message."""

from __future__ import annotations

import asyncio
import contextlib
import itertools
import ssl
import uuid
from collections.abc import AsyncGenerator
from typing import Any
from urllib.parse import urlparse

from quillmq import protocol as p


class Message:
    def __init__(self, channel: Channel, frame: dict) -> None:
        self._ch = channel
        self.queue = frame["queue"]
        self.delivery_tag = frame["delivery_tag"]
        self.routing_key = frame["routing_key"]
        self.headers = frame.get("headers", {})
        self.body = frame.get("body")
        self.redelivered = frame.get("redelivered", False)

    async def ack(self) -> None:
        await self._ch._conn._write(
            {
                "type": p.ACK,
                "consumer_tag": self.queue,
                "delivery_tag": self.delivery_tag,
            }
        )

    async def nack(self, requeue: bool = True) -> None:
        await self._ch._conn._write(
            {
                "type": p.NACK,
                "consumer_tag": self.queue,
                "delivery_tag": self.delivery_tag,
                "requeue": requeue,
            }
        )


class Channel:
    def __init__(self, conn: Connection) -> None:
        self._conn = conn

    async def declare_exchange(self, name: str, type_: str = "direct") -> None:
        await self._conn._request(
            {"type": p.DECLARE_EXCHANGE, "exchange": name, "type_": type_}
        )

    async def declare_queue(self, name: str, durable: bool = False) -> None:
        await self._conn._request(
            {"type": p.DECLARE_QUEUE, "queue": name, "durable": durable}
        )

    async def bind(self, queue: str, exchange: str, routing_key: str) -> None:
        await self._conn._request(
            {
                "type": p.BIND,
                "exchange": exchange,
                "queue": queue,
                "routing_key": routing_key,
            }
        )

    async def publish(
        self, exchange: str, routing_key: str, body: Any, headers: dict | None = None
    ) -> None:
        await self._conn._request(
            {
                "type": p.PUBLISH,
                "exchange": exchange,
                "routing_key": routing_key,
                "body": body,
                "headers": headers or {},
            }
        )

    async def stats(self) -> dict:
        reply = await self._conn._request({"type": p.STATS})
        return reply.get("stats", {})

    async def consume(
        self, queue: str, prefetch: int = 0, auto_ack: bool = False
    ) -> AsyncGenerator[Message, None]:
        # consumer_tag == queue name (one consumer per queue per connection in v1).
        inbox: asyncio.Queue[dict] = asyncio.Queue()
        self._conn._inboxes[queue] = inbox
        await self._conn._request(
            {
                "type": p.CONSUME,
                "queue": queue,
                "consumer_tag": queue,
                "prefetch": prefetch,
                "auto_ack": auto_ack,
            }
        )
        try:
            while True:
                frame = await inbox.get()
                yield Message(self, frame)
        finally:
            self._conn._inboxes.pop(queue, None)

    async def rpc_call(
        self, target_queue: str, body: Any, timeout: float = 30.0
    ) -> Any:
        reply_queue = f"_rpc.reply.{uuid.uuid4().hex}"
        await self.declare_queue(reply_queue, durable=False)
        correlation_id = uuid.uuid4().hex
        agen = self.consume(reply_queue, auto_ack=True)

        async def _await_reply() -> Any:
            async for msg in agen:
                if msg.headers.get("correlation_id") == correlation_id:
                    return msg.body
            return None  # pragma: no cover - generator only ends on close

        await self.publish(
            "",
            target_queue,
            body,
            headers={"reply_to": reply_queue, "correlation_id": correlation_id},
        )
        try:
            return await asyncio.wait_for(_await_reply(), timeout)
        finally:
            await agen.aclose()


class Connection:
    def __init__(self, reader, writer, heartbeat_interval: float = 20.0) -> None:
        self._reader = reader
        self._writer = writer
        self._wlock = asyncio.Lock()
        self._rids = itertools.count(1)
        self._pending: dict[int, asyncio.Future] = {}
        self._inboxes: dict[str, asyncio.Queue] = {}
        self._reader_task = asyncio.create_task(self._read_loop())
        self._heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(heartbeat_interval)
        )

    async def _heartbeat_loop(self, interval: float) -> None:
        while True:
            await asyncio.sleep(interval)
            with contextlib.suppress(Exception):
                await self._write({"type": p.HEARTBEAT})

    async def _write(self, frame: dict) -> None:
        async with self._wlock:
            self._writer.write(p.encode_frame(frame))
            await self._writer.drain()

    async def _request(self, frame: dict) -> dict:
        rid = next(self._rids)
        frame = {**frame, "rid": rid}
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        self._pending[rid] = fut
        await self._write(frame)
        reply = await fut
        if reply.get("type") == p.ERROR:
            raise RuntimeError(reply.get("message", "broker error"))
        return reply

    async def _read_loop(self) -> None:
        while True:
            frame = await p.read_frame(self._reader)
            if frame is None:
                break
            if frame["type"] == p.DELIVER:
                inbox = self._inboxes.get(frame["queue"])
                if inbox is not None:
                    inbox.put_nowait(frame)
            elif frame["type"] in (p.OK, p.ERROR):
                rid = frame.get("rid")
                fut = self._pending.pop(rid, None) if rid is not None else None
                if fut is not None and not fut.done():
                    fut.set_result(frame)

    async def channel(self) -> Channel:
        return Channel(self)

    async def close(self) -> None:
        self._reader_task.cancel()
        self._heartbeat_task.cancel()
        self._writer.close()
        # best-effort: the peer may have already dropped the connection
        with contextlib.suppress(Exception):
            await self._writer.wait_closed()


async def connect(
    url: str,
    auth_token: str | None = None,
    ssl_context: ssl.SSLContext | None = None,
    heartbeat_interval: float = 20.0,
) -> Connection:
    parsed = urlparse(url)
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or 5555
    # A quills:// URL implies TLS; a caller may also pass an explicit context.
    if parsed.scheme == "quills" and ssl_context is None:
        ssl_context = ssl.create_default_context()
    reader, writer = await asyncio.open_connection(host, port, ssl=ssl_context)
    conn = Connection(reader, writer, heartbeat_interval=heartbeat_interval)
    hello = {"type": p.HELLO, "version": p.PROTOCOL_VERSION}
    if auth_token is not None:
        hello["token"] = auth_token
    try:
        await conn._request(hello)
    except BaseException:
        await conn.close()
        raise
    return conn
