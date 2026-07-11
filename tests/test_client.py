import pytest

from quillmq import connect
from quillmq.broker import Broker
from quillmq.server import BrokerServer


async def test_publish_consume_and_ack_over_client():
    server = BrokerServer(Broker(), host="127.0.0.1", port=0)
    await server.start()
    try:
        conn = await connect(f"quill://127.0.0.1:{server.port}")
        ch = await conn.channel()
        await ch.declare_queue("tasks", durable=False)
        await ch.publish("", "tasks", {"n": 1})
        await ch.publish("", "tasks", {"n": 2})

        seen = []
        async for msg in ch.consume("tasks", prefetch=10):
            seen.append(msg.body)
            await msg.ack()
            if len(seen) == 2:
                break
        assert seen == [{"n": 1}, {"n": 2}]
        await conn.close()
    finally:
        await server.stop()


async def test_fanout_over_client():
    server = BrokerServer(Broker(), host="127.0.0.1", port=0)
    await server.start()
    try:
        conn = await connect(f"quill://127.0.0.1:{server.port}")
        ch = await conn.channel()
        await ch.declare_exchange("events", "fanout")
        await ch.declare_queue("sub1", durable=False)
        await ch.bind("sub1", "events", "")
        await ch.publish("events", "", {"evt": "done"})
        async for msg in ch.consume("sub1", auto_ack=True):
            assert msg.body == {"evt": "done"}
            break
        await conn.close()
    finally:
        await server.stop()


async def test_client_nack_requeues():
    server = BrokerServer(Broker(), host="127.0.0.1", port=0)
    await server.start()
    try:
        conn = await connect(f"quill://127.0.0.1:{server.port}")
        ch = await conn.channel()
        await ch.declare_queue("retry", durable=False)
        await ch.publish("", "retry", {"n": 1})
        attempts = 0
        async for msg in ch.consume("retry", prefetch=1):
            attempts += 1
            if attempts == 1:
                await msg.nack(requeue=True)
            else:
                await msg.ack()
                break
        assert attempts == 2
        await conn.close()
    finally:
        await server.stop()


async def test_client_stats():
    server = BrokerServer(Broker(), host="127.0.0.1", port=0)
    await server.start()
    try:
        conn = await connect(f"quill://127.0.0.1:{server.port}")
        ch = await conn.channel()
        await ch.declare_queue("tasks", durable=False)
        await ch.publish("", "tasks", {"n": 1})
        stats = await ch.stats()
        assert stats["queues"]["tasks"]["depth"] == 1
        await conn.close()
    finally:
        await server.stop()


async def test_publish_unknown_exchange_returns_error_and_keeps_connection():
    server = BrokerServer(Broker(), host="127.0.0.1", port=0)
    await server.start()
    try:
        conn = await connect(f"quill://127.0.0.1:{server.port}")
        ch = await conn.channel()
        with pytest.raises(RuntimeError):
            await ch.publish("does-not-exist", "k", {"n": 1})
        # connection still usable afterwards
        await ch.declare_queue("tasks", durable=False)
        stats = await ch.stats()
        assert "tasks" in stats["queues"]
        await conn.close()
    finally:
        await server.stop()


async def test_client_surfaces_broker_error():
    server = BrokerServer(Broker(), host="127.0.0.1", port=0, auth_token="secret")
    await server.start()
    try:
        with pytest.raises(RuntimeError):
            await connect(f"quill://127.0.0.1:{server.port}", auth_token="wrong")
    finally:
        await server.stop()
