# SPDX-License-Identifier: Apache-2.0
"""Exchange types and routing for QuillMQ."""

from __future__ import annotations

from dataclasses import dataclass

VALID_TYPES = ("direct", "fanout", "topic")


def topic_match(pattern: str, routing_key: str) -> bool:
    return _match(pattern.split("."), routing_key.split("."))


def _match(p: list[str], k: list[str]) -> bool:
    if not p:
        return not k
    if p[0] == "#":
        # '#' matches zero or more words: try consuming 0..len(k) words.
        return any(_match(p[1:], k[i:]) for i in range(len(k) + 1))
    if not k:
        return False
    if p[0] == "*" or p[0] == k[0]:
        return _match(p[1:], k[1:])
    return False


@dataclass
class _Binding:
    queue: str
    routing_key: str


class Exchange:
    def __init__(self, name: str, type_: str) -> None:
        if type_ not in VALID_TYPES:
            raise ValueError(f"unknown exchange type: {type_}")
        self.name = name
        self.type = type_
        self._bindings: list[_Binding] = []

    def bind(self, queue: str, routing_key: str) -> None:
        self._bindings.append(_Binding(queue, routing_key))

    def route(self, routing_key: str) -> list[str]:
        out: list[str] = []
        for b in self._bindings:
            hit = (
                self.type == "fanout"
                or (self.type == "direct" and b.routing_key == routing_key)
                or (self.type == "topic" and topic_match(b.routing_key, routing_key))
            )
            if hit and b.queue not in out:
                out.append(b.queue)
        return out
