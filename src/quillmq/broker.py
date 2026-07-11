# SPDX-License-Identifier: Apache-2.0
"""Broker core: topology, routing, delivery, and durability. No networking."""

from __future__ import annotations

import logging
import time
from collections.abc import Awaitable, Callable
from typing import Any

from quillmq import protocol as p
from quillmq.exchange import Exchange
from quillmq.metrics import Metrics
from quillmq.queue import Consumer, Message, Queue
from quillmq.store import Store

SendFn = Callable[[dict], Awaitable[None]]

logger = logging.getLogger("quillmq.broker")


class Broker:
    def __init__(
        self,
        store: Store | None = None,
        max_delivery_count: int = 0,
        dead_letter_queue: str | None = None,
    ) -> None:
        self.store = store
        # 0 means unlimited redeliveries; otherwise a message that has been
        # delivered this many times is dead-lettered instead of redelivered.
        self.max_delivery_count = max_delivery_count
        self.dead_letter_queue = dead_letter_queue
        self.metrics = Metrics()
        self.exchanges: dict[str, Exchange] = {"": Exchange("", "direct")}
        self.queues: dict[str, Queue] = {}
        # Keyed by consumer identity, NOT tag: several consumers on one queue
        # (competing workers) share a tag but must each keep their own socket.
        self._sends: dict[int, SendFn] = {}
        self._next_id = 1

    async def recover(self) -> None:
        if self.store is None:  # pragma: no cover - guard for storeless broker
            return
        topo = await self.store.load_topology()
        for name, type_ in topo["exchanges"]:
            self.exchanges[name] = Exchange(name, type_)
        for name, durable in topo["queues"]:
            await self.declare_queue(name, durable)
        for exchange, queue, rk in topo["bindings"]:
            await self.bind(exchange, queue, rk)
        for msg in await self.store.load_messages():
            if msg.queue in self.queues:
                # A recovered message may have been delivered-but-unacked before the
                # restart, so bump its count; the next delivery reports redelivered.
                msg.delivery_count += 1
                self.queues[msg.queue].enqueue(msg)
        self._next_id = await self.store.max_message_id() + 1

    async def declare_exchange(self, name: str, type_: str) -> None:
        if name not in self.exchanges:
            self.exchanges[name] = Exchange(name, type_)
        if self.store is not None:
            await self.store.upsert_exchange(name, type_)

    async def declare_queue(self, name: str, durable: bool) -> None:
        if name not in self.queues:
            self.queues[name] = Queue(name, durable)
            self.exchanges[""].bind(name, name)  # default-exchange routing by name
        if durable and self.store is not None:
            await self.store.upsert_queue(name, durable)

    async def bind(self, exchange: str, queue: str, routing_key: str) -> None:
        self.exchanges[exchange].bind(queue, routing_key)
        q = self.queues.get(queue)
        if self.store is not None and q is not None and q.durable:
            await self.store.add_binding(exchange, queue, routing_key)

    async def publish(
        self, exchange: str, routing_key: str, body: Any, headers: dict | None = None
    ) -> None:
        headers = self._apply_ttl(headers or {})
        targets = self.exchanges[exchange].route(routing_key)
        touched: list[Queue] = []
        for qname in targets:
            q = self.queues.get(qname)
            if q is None:  # pragma: no cover - binding without a live queue
                continue
            msg = Message(
                id=self._next_id,
                queue=qname,
                routing_key=routing_key,
                headers=headers,
                body=body,
            )
            self._next_id += 1
            if q.durable and self.store is not None:
                await self.store.add_message(msg)
            q.enqueue(msg)
            self.metrics.inc("published")
            touched.append(q)
        for q in touched:
            await self._deliver(q)

    async def add_consumer(self, queue: str, consumer: Consumer, send: SendFn) -> None:
        q = self.queues[queue]
        self._sends[id(consumer)] = send
        q.add_consumer(consumer)
        await self._deliver(q)

    async def remove_consumer(self, queue: str, consumer: Consumer) -> None:
        q = self.queues.get(queue)
        if q is None:  # pragma: no cover - defensive
            return
        self._sends.pop(id(consumer), None)
        for msg in q.remove_consumer(consumer):
            if self._over_limit(msg):
                await self._dead_letter(q, msg)
            else:
                q.enqueue(msg)
        await self._deliver(q)

    async def ack(self, queue: str, consumer: Consumer, delivery_tag: int) -> None:
        q = self.queues[queue]
        msg = q.ack(consumer, delivery_tag)
        if msg is not None:
            self.metrics.inc("acked")
            if q.durable and self.store is not None:
                await self.store.delete_message(delivery_tag)
        await self._deliver(q)

    async def nack(
        self, queue: str, consumer: Consumer, delivery_tag: int, requeue: bool
    ) -> None:
        q = self.queues[queue]
        msg = q.ack(consumer, delivery_tag)  # remove from the consumer's unacked set
        if msg is None:
            return
        self.metrics.inc("nacked")
        if requeue and not self._over_limit(msg):
            q.requeue(msg)
            if q.durable and self.store is not None:
                await self.store.update_delivery_count(msg.id, msg.delivery_count)
        else:
            # requeue=False (rejected) or too many attempts: dead-letter it.
            await self._dead_letter(q, msg)
        await self._deliver(q)

    async def _deliver(self, q: Queue) -> None:
        for consumer, msg in q.dispatch():
            if self._expired(msg):
                self.metrics.inc("expired")
                consumer.unacked.pop(msg.id, None)
                await self._dead_letter(q, msg)
                continue
            frame = {
                "type": p.DELIVER,
                "queue": q.name,
                "delivery_tag": msg.id,
                "routing_key": msg.routing_key,
                "headers": msg.headers,
                "body": msg.body,
                "redelivered": msg.redelivered,
            }
            if q.durable and self.store is not None and not consumer.auto_ack:
                await self.store.update_delivery_count(msg.id, msg.delivery_count)
            await self._sends[id(consumer)](frame)
            self.metrics.inc("delivered")
            if consumer.auto_ack and q.durable and self.store is not None:
                await self.store.delete_message(msg.id)

    def _over_limit(self, msg: Message) -> bool:
        return (
            self.max_delivery_count > 0
            and msg.delivery_count >= self.max_delivery_count
        )

    @staticmethod
    def _apply_ttl(headers: dict) -> dict:
        # A relative "ttl" (seconds) becomes an absolute "expires_at" at publish.
        if "ttl" in headers and "expires_at" not in headers:
            headers = {**headers, "expires_at": time.time() + headers["ttl"]}
        return headers

    @staticmethod
    def _expired(msg: Message) -> bool:
        expires_at = msg.headers.get("expires_at")
        return expires_at is not None and time.time() >= expires_at

    async def _dead_letter(self, q: Queue, msg: Message) -> None:
        self.metrics.inc("dead_lettered")
        if q.durable and self.store is not None:
            await self.store.delete_message(msg.id)
        if self.dead_letter_queue is None:
            logger.warning(
                "dropping message %s from %s (no dead-letter queue)", msg.id, q.name
            )
            return
        await self.declare_queue(self.dead_letter_queue, durable=True)
        logger.info("dead-lettering message %s from queue %s", msg.id, q.name)
        headers = {
            k: v for k, v in msg.headers.items() if k not in ("ttl", "expires_at")
        }
        headers["x-death-queue"] = q.name
        headers["x-death-count"] = msg.delivery_count
        await self.publish("", self.dead_letter_queue, msg.body, headers)

    def stats(self) -> dict:
        return {
            "queues": {
                name: {"depth": q.depth(), "consumers": len(q._consumers)}
                for name, q in self.queues.items()
            }
        }
