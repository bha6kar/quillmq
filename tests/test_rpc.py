import asyncio

from quillmq import connect
from quillmq.broker import Broker
from quillmq.rpc import RPCServer
from quillmq.server import BrokerServer


async def test_rpc_request_reply():
    server = BrokerServer(Broker(), host="127.0.0.1", port=0)
    await server.start()
    try:
        srv_conn = await connect(f"quill://127.0.0.1:{server.port}")
        srv_ch = await srv_conn.channel()
        await srv_ch.declare_queue("math.rpc", durable=False)
        rpc = RPCServer(srv_ch, "math.rpc")

        async def handler(body):
            return {"sum": body["a"] + body["b"]}

        task = asyncio.create_task(rpc.serve(handler))

        cli_conn = await connect(f"quill://127.0.0.1:{server.port}")
        cli_ch = await cli_conn.channel()
        reply = await cli_ch.rpc_call("math.rpc", {"a": 2, "b": 3}, timeout=5)
        assert reply == {"sum": 5}

        task.cancel()
        await srv_conn.close()
        await cli_conn.close()
    finally:
        await server.stop()


async def test_rpc_call_times_out_without_server():
    server = BrokerServer(Broker(), host="127.0.0.1", port=0)
    await server.start()
    try:
        conn = await connect(f"quill://127.0.0.1:{server.port}")
        ch = await conn.channel()
        await ch.declare_queue("nobody.rpc", durable=False)
        try:
            await ch.rpc_call("nobody.rpc", {"a": 1}, timeout=0.2)
            raise AssertionError("expected timeout")
        except TimeoutError:
            pass
        await conn.close()
    finally:
        await server.stop()
