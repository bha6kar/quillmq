# SPDX-License-Identifier: Apache-2.0
"""RPC server helper over the request/reply pattern."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from quillmq.client import Channel


class RPCServer:
    def __init__(self, channel: Channel, queue: str) -> None:
        self.channel = channel
        self.queue = queue

    async def serve(self, handler: Callable[[Any], Awaitable[Any]]) -> None:
        async for msg in self.channel.consume(self.queue):
            reply_to = msg.headers.get("reply_to")
            correlation_id = msg.headers.get("correlation_id")
            result = await handler(msg.body)
            if reply_to is not None:
                await self.channel.publish(
                    "",
                    reply_to,
                    result,
                    headers={"correlation_id": correlation_id},
                )
            await msg.ack()
