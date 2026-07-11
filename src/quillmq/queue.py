# SPDX-License-Identifier: Apache-2.0
"""In-memory queue state, consumers, and round-robin dispatch."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Any


@dataclass
class Message:
    id: int
    queue: str
    routing_key: str
    headers: dict
    body: Any
    delivery_count: int = 0

    @property
    def redelivered(self) -> bool:
        return self.delivery_count > 1


@dataclass
class Consumer:
    tag: str
    prefetch: int = 0
    auto_ack: bool = False
    unacked: dict[int, Message] = field(default_factory=dict)

    @property
    def available(self) -> bool:
        return self.prefetch == 0 or len(self.unacked) < self.prefetch


class Queue:
    def __init__(self, name: str, durable: bool) -> None:
        self.name = name
        self.durable = durable
        self._ready: deque[Message] = deque()
        self._consumers: list[Consumer] = []
        self._rr = 0

    def depth(self) -> int:
        return len(self._ready)

    def enqueue(self, msg: Message) -> None:
        self._ready.append(msg)

    def add_consumer(self, c: Consumer) -> None:
        self._consumers.append(c)

    def remove_consumer(self, c: Consumer) -> list[Message]:
        if c in self._consumers:
            self._consumers.remove(c)
        orphaned = list(c.unacked.values())
        c.unacked.clear()
        return orphaned

    def _next_available(self) -> Consumer | None:
        n = len(self._consumers)
        for _ in range(n):
            c = self._consumers[self._rr % n]
            self._rr += 1
            if c.available:
                return c
        return None

    def dispatch(self) -> list[tuple[Consumer, Message]]:
        delivered: list[tuple[Consumer, Message]] = []
        while self._ready and self._consumers:
            c = self._next_available()
            if c is None:
                break
            msg = self._ready.popleft()
            msg.delivery_count += 1
            if not c.auto_ack:
                c.unacked[msg.id] = msg
            delivered.append((c, msg))
        return delivered

    def ack(self, c: Consumer, msg_id: int) -> Message | None:
        return c.unacked.pop(msg_id, None)

    def requeue(self, msg: Message) -> None:
        # Put a redelivered message back at the front so it is retried next.
        self._ready.appendleft(msg)
