"""Pub/Sub subscriber. Pass a unique name so each subscriber gets its own
queue bound to the shared 'events' fanout exchange:

    uv run python examples/pubsub_subscriber.py audit
    uv run python examples/pubsub_subscriber.py notifier

Run subscribers before publishing so their queues exist and are bound.
"""

import asyncio
import sys

from quillmq import connect


async def main(name: str) -> None:
    conn = await connect("quill://127.0.0.1:5555")
    ch = await conn.channel()
    await ch.declare_exchange("events", "fanout")
    await ch.declare_queue(f"sub.{name}", durable=False)
    await ch.bind(f"sub.{name}", "events", "")
    print(f"subscriber '{name}' waiting for events...")
    async for msg in ch.consume(f"sub.{name}", auto_ack=True):
        print(f"[{name}] received {msg.body}")


if __name__ == "__main__":
    subscriber_name = sys.argv[1] if len(sys.argv) > 1 else "sub1"
    asyncio.run(main(subscriber_name))
