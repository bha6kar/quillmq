"""Pub/Sub publisher. Start the broker, then run one or more subscribers
(examples/pubsub_subscriber.py) BEFORE publishing so their queues are bound.
Every subscriber receives its own copy of each event.

    uv run quillmq serve --port 5555
"""

import asyncio

from quillmq import connect


async def main() -> None:
    conn = await connect("quill://127.0.0.1:5555")
    ch = await conn.channel()
    await ch.declare_exchange("events", "fanout")
    for i in range(3):
        await ch.publish("events", "", {"event": "tick", "n": i})
        print("published event", i)
    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
