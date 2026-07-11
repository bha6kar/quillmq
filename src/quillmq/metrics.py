# SPDX-License-Identifier: Apache-2.0
"""Broker counters and a minimal Prometheus /metrics HTTP endpoint.

The endpoint is a tiny stdlib asyncio server so observability adds no runtime
dependency. It renders the Prometheus text exposition format.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from quillmq.broker import Broker

logger = logging.getLogger("quillmq.metrics")

_COUNTERS = ("published", "delivered", "acked", "nacked", "dead_lettered", "expired")


class Metrics:
    def __init__(self) -> None:
        self.counters: dict[str, int] = dict.fromkeys(_COUNTERS, 0)

    def inc(self, name: str, n: int = 1) -> None:
        self.counters[name] += n

    def render(self, broker: Broker) -> str:
        lines: list[str] = []
        for name, value in self.counters.items():
            lines.append(f"# TYPE quillmq_{name}_total counter")
            lines.append(f"quillmq_{name}_total {value}")
        lines.append("# TYPE quillmq_queue_depth gauge")
        for qname, q in broker.queues.items():
            lines.append(f'quillmq_queue_depth{{queue="{qname}"}} {q.depth()}')
        return "\n".join(lines) + "\n"


class MetricsServer:
    def __init__(self, broker: Broker, host: str = "0.0.0.0", port: int = 9095) -> None:
        self.broker = broker
        self.host = host
        self.port = port
        self._server: asyncio.AbstractServer | None = None

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._handle, self.host, self.port)
        if self.port == 0:
            self.port = self._server.sockets[0].getsockname()[1]
        logger.info("metrics endpoint on %s:%s/metrics", self.host, self.port)

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    async def _handle(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            with contextlib.suppress(Exception):
                request = await asyncio.wait_for(reader.readline(), timeout=2)
                await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=2)
            if b"/metrics" in request:
                body = self.broker.metrics.render(self.broker).encode()
                head = (
                    b"HTTP/1.1 200 OK\r\n"
                    b"Content-Type: text/plain; version=0.0.4\r\n"
                    b"Content-Length: " + str(len(body)).encode() + b"\r\n\r\n"
                )
                writer.write(head + body)
            else:
                writer.write(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n")
            await writer.drain()
        finally:
            writer.close()
