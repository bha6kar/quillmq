"""Work-queue worker. Run several copies in separate terminals to see tasks
load-balanced across them. Start the broker and producer first (see
examples/producer.py). Each worker acks after handling a task, so an
unacked task is redelivered if the worker dies.
"""

import asyncio

from quillmq import connect


async def main() -> None:
    conn = await connect("quill://127.0.0.1:5555")
    ch = await conn.channel()
    await ch.declare_queue("tasks", durable=True)
    # prefetch=1: hold at most one unacked task at a time (fair dispatch).
    async for msg in ch.consume("tasks", prefetch=1):
        print("processing", msg.body)
        await msg.ack()


if __name__ == "__main__":
    asyncio.run(main())
