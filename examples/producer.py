"""Work-queue producer. Start a broker first:

    uv run quillmq serve --port 5555

then run this producer and one or more workers (examples/worker.py).
Each task is delivered to exactly one worker.
"""

import asyncio

from quillmq import connect


async def main() -> None:
    conn = await connect("quill://127.0.0.1:5555")
    ch = await conn.channel()
    await ch.declare_queue("tasks", durable=True)
    for i in range(5):
        await ch.publish("", "tasks", {"job": i})
        print("published", i)
    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
