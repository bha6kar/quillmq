# SPDX-License-Identifier: Apache-2.0
"""Asyncio TCP server exposing the broker over the QuillMQ frame protocol."""

from __future__ import annotations

import asyncio
import logging
import ssl

from quillmq import protocol as p
from quillmq.broker import Broker
from quillmq.queue import Consumer

logger = logging.getLogger("quillmq.server")


class BrokerServer:
    def __init__(
        self,
        broker: Broker,
        host: str = "0.0.0.0",
        port: int = 5555,
        auth_token: str | None = None,
        heartbeat: float = 60.0,
        ssl_context: ssl.SSLContext | None = None,
    ) -> None:
        self.broker = broker
        self.host = host
        self.port = port
        self.auth_token = auth_token
        # A connection is closed if it sends nothing for this many seconds; clients
        # send HEARTBEAT frames well within it to keep idle connections alive.
        self.heartbeat = heartbeat
        self.ssl_context = ssl_context
        self._server: asyncio.AbstractServer | None = None

    async def start(self) -> None:
        self._server = await asyncio.start_server(
            self._handle, self.host, self.port, ssl=self.ssl_context
        )
        if self.port == 0:
            self.port = self._server.sockets[0].getsockname()[1]
        logger.info("broker listening on %s:%s", self.host, self.port)

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
            logger.info("broker stopped")

    async def _read(self, reader: asyncio.StreamReader) -> dict | None:
        return await asyncio.wait_for(p.read_frame(reader), self.heartbeat)

    async def _handle(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        peer = writer.get_extra_info("peername")
        lock = asyncio.Lock()

        async def send(frame: dict) -> None:
            async with lock:
                writer.write(p.encode_frame(frame))
                await writer.drain()

        consumers: dict[str, tuple[str, Consumer]] = {}  # tag -> (queue, consumer)
        try:
            if not await self._handshake(reader, send):
                return
            while True:
                try:
                    frame = await self._read(reader)
                except TimeoutError:
                    logger.info("closing idle connection %s", peer)
                    break
                if frame is None:
                    break
                try:
                    await self._dispatch(frame, send, consumers)
                except Exception as exc:  # keep the connection alive on bad frames
                    logger.warning("frame error from %s: %s", peer, exc)
                    rid = frame.get("rid")
                    if rid is not None:
                        await send({"type": p.ERROR, "rid": rid, "message": str(exc)})
        except (ConnectionError, ssl.SSLError) as exc:  # pragma: no cover - transport
            logger.info("connection %s dropped: %s", peer, exc)
        finally:
            for queue, consumer in consumers.values():
                await self.broker.remove_consumer(queue, consumer)
            writer.close()

    async def _handshake(self, reader: asyncio.StreamReader, send) -> bool:
        """Require HELLO (and a valid token if configured) as the first frame."""
        try:
            frame = await self._read(reader)
        except TimeoutError:
            return False
        if frame is None:
            return False
        rid = frame.get("rid")
        if frame.get("type") != p.HELLO:
            await send({"type": p.ERROR, "rid": rid, "message": "expected hello"})
            return False
        if self.auth_token is not None and frame.get("token") != self.auth_token:
            await send({"type": p.ERROR, "rid": rid, "message": "unauthorized"})
            return False
        if rid is not None:
            await send({"type": p.OK, "rid": rid, "version": p.PROTOCOL_VERSION})
        return True

    async def _dispatch(self, frame, send, consumers) -> None:
        t = frame.get("type")
        rid = frame.get("rid")

        async def ok(extra: dict | None = None) -> None:
            if rid is not None:
                await send({"type": p.OK, "rid": rid, **(extra or {})})

        if t == p.DECLARE_EXCHANGE:
            await self.broker.declare_exchange(frame["exchange"], frame["type_"])
            await ok()
        elif t == p.DECLARE_QUEUE:
            await self.broker.declare_queue(frame["queue"], frame.get("durable", False))
            await ok()
        elif t == p.BIND:
            await self.broker.bind(
                frame["exchange"], frame["queue"], frame["routing_key"]
            )
            await ok()
        elif t == p.PUBLISH:
            await self.broker.publish(
                frame["exchange"],
                frame["routing_key"],
                frame.get("body"),
                frame.get("headers", {}),
            )
            await ok()
        elif t == p.CONSUME:
            consumer = Consumer(
                tag=frame["consumer_tag"],
                prefetch=frame.get("prefetch", 0),
                auto_ack=frame.get("auto_ack", False),
            )
            consumers[consumer.tag] = (frame["queue"], consumer)
            await ok()
            await self.broker.add_consumer(frame["queue"], consumer, send)
        elif t == p.ACK:
            queue, consumer = consumers[frame["consumer_tag"]]
            await self.broker.ack(queue, consumer, frame["delivery_tag"])
        elif t == p.NACK:
            queue, consumer = consumers[frame["consumer_tag"]]
            await self.broker.nack(
                queue, consumer, frame["delivery_tag"], frame.get("requeue", True)
            )
        elif t == p.STATS:
            await ok({"stats": self.broker.stats()})
        elif t == p.HEARTBEAT:
            pass
        else:
            if rid is not None:
                await send(
                    {"type": p.ERROR, "rid": rid, "message": f"unknown frame: {t}"}
                )
