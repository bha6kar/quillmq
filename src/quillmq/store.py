# SPDX-License-Identifier: Apache-2.0
"""SQLite (WAL) persistence for durable queues, topology, and messages."""

from __future__ import annotations

import json

import aiosqlite

from quillmq.queue import Message

_SCHEMA = """
CREATE TABLE IF NOT EXISTS exchanges (name TEXT PRIMARY KEY, type TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS queues (name TEXT PRIMARY KEY, durable INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS bindings (
    exchange TEXT NOT NULL, queue TEXT NOT NULL, routing_key TEXT NOT NULL,
    UNIQUE(exchange, queue, routing_key)
);
CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY,
    queue TEXT NOT NULL,
    routing_key TEXT NOT NULL,
    headers TEXT NOT NULL,
    body TEXT NOT NULL,
    delivery_count INTEGER NOT NULL DEFAULT 0
);
"""


class Store:
    def __init__(self, path: str) -> None:
        self.path = path
        self._db: aiosqlite.Connection | None = None

    async def open(self) -> None:
        self._db = await aiosqlite.connect(self.path)
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._db.executescript(_SCHEMA)
        await self._db.commit()

    async def close(self) -> None:
        if self._db is not None:
            await self._db.close()
            self._db = None

    @property
    def db(self) -> aiosqlite.Connection:
        assert self._db is not None, "store not open"
        return self._db

    async def upsert_exchange(self, name: str, type_: str) -> None:
        await self.db.execute(
            "INSERT OR REPLACE INTO exchanges(name, type) VALUES (?, ?)", (name, type_)
        )
        await self.db.commit()

    async def upsert_queue(self, name: str, durable: bool) -> None:
        await self.db.execute(
            "INSERT OR REPLACE INTO queues(name, durable) VALUES (?, ?)",
            (name, int(durable)),
        )
        await self.db.commit()

    async def add_binding(self, exchange: str, queue: str, routing_key: str) -> None:
        await self.db.execute(
            "INSERT OR IGNORE INTO bindings(exchange, queue, routing_key)"
            " VALUES (?, ?, ?)",
            (exchange, queue, routing_key),
        )
        await self.db.commit()

    async def add_message(self, msg: Message) -> None:
        await self.db.execute(
            "INSERT OR REPLACE INTO"
            " messages(id, queue, routing_key, headers, body, delivery_count)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (
                msg.id,
                msg.queue,
                msg.routing_key,
                json.dumps(msg.headers),
                json.dumps(msg.body),
                msg.delivery_count,
            ),
        )
        await self.db.commit()

    async def delete_message(self, msg_id: int) -> None:
        await self.db.execute("DELETE FROM messages WHERE id = ?", (msg_id,))
        await self.db.commit()

    async def update_delivery_count(self, msg_id: int, count: int) -> None:
        await self.db.execute(
            "UPDATE messages SET delivery_count = ? WHERE id = ?", (count, msg_id)
        )
        await self.db.commit()

    async def load_topology(self) -> dict:
        async with self.db.execute("SELECT name, type FROM exchanges") as cur:
            exchanges = [(r[0], r[1]) async for r in cur]
        async with self.db.execute("SELECT name, durable FROM queues") as cur:
            queues = [(r[0], bool(r[1])) async for r in cur]
        async with self.db.execute(
            "SELECT exchange, queue, routing_key FROM bindings"
        ) as cur:
            bindings = [(r[0], r[1], r[2]) async for r in cur]
        return {"exchanges": exchanges, "queues": queues, "bindings": bindings}

    async def load_messages(self) -> list[Message]:
        async with self.db.execute(
            "SELECT id, queue, routing_key, headers, body, delivery_count"
            " FROM messages ORDER BY id"
        ) as cur:
            return [
                Message(
                    id=r[0],
                    queue=r[1],
                    routing_key=r[2],
                    headers=json.loads(r[3]),
                    body=json.loads(r[4]),
                    delivery_count=r[5],
                )
                async for r in cur
            ]

    async def max_message_id(self) -> int:
        async with self.db.execute("SELECT COALESCE(MAX(id), 0) FROM messages") as cur:
            row = await cur.fetchone()
        assert row is not None  # COALESCE guarantees exactly one row
        return int(row[0])
