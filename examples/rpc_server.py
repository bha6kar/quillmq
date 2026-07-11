"""RPC server. Start the broker, run this, then run examples/rpc_client.py.
It consumes requests on 'math.rpc', computes a result, and replies to the
caller's reply queue using the request's correlation id.

    uv run quillmq serve --port 5555
"""

import asyncio

from quillmq import connect
from quillmq.rpc import RPCServer


async def main() -> None:
    conn = await connect("quill://127.0.0.1:5555")
    ch = await conn.channel()
    await ch.declare_queue("math.rpc", durable=False)

    async def handler(body: dict) -> dict:
        return {"sum": body["a"] + body["b"]}

    print("RPC server listening on 'math.rpc'...")
    await RPCServer(ch, "math.rpc").serve(handler)


if __name__ == "__main__":
    asyncio.run(main())
