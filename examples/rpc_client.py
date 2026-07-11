"""RPC client. Start the broker and examples/rpc_server.py first, then run
this. rpc_call publishes a request and blocks until the correlated reply
arrives (or the timeout elapses).

    uv run quillmq serve --port 5555
"""

import asyncio

from quillmq import connect


async def main() -> None:
    conn = await connect("quill://127.0.0.1:5555")
    ch = await conn.channel()
    reply = await ch.rpc_call("math.rpc", {"a": 2, "b": 3}, timeout=10)
    print("reply:", reply)
    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
