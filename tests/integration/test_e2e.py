import asyncio

from quillmq import connect
from quillmq.broker import Broker
from quillmq.server import BrokerServer
from quillmq.store import Store


async def _serve(broker):
    server = BrokerServer(broker, host="127.0.0.1", port=0)
    await server.start()
    return server


async def test_work_queue_distributes_across_workers():
    server = await _serve(Broker())
    try:
        url = f"quill://127.0.0.1:{server.port}"
        prod = await connect(url)
        pch = await prod.channel()
        await pch.declare_queue("tasks", durable=False)
        for i in range(10):
            await pch.publish("", "tasks", {"n": i})

        counts = {"w1": 0, "w2": 0}
        done = asyncio.Event()

        async def worker(tag):
            conn = await connect(url)
            ch = await conn.channel()
            try:
                async for msg in ch.consume("tasks", prefetch=1):
                    counts[tag] += 1
                    await msg.ack()
                    if sum(counts.values()) >= 10:
                        done.set()
                        break
            finally:
                await conn.close()

        tasks = [asyncio.create_task(worker("w1")), asyncio.create_task(worker("w2"))]
        await asyncio.wait_for(done.wait(), timeout=5)
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        assert sum(counts.values()) == 10
        assert counts["w1"] > 0 and counts["w2"] > 0
        await prod.close()
    finally:
        await server.stop()


async def test_durability_survives_broker_restart(tmp_path):
    db = str(tmp_path / "quill.db")

    store = Store(db)
    await store.open()
    broker = Broker(store=store)
    await broker.recover()
    server = await _serve(broker)
    url = f"quill://127.0.0.1:{server.port}"
    conn = await connect(url)
    ch = await conn.channel()
    await ch.declare_queue("durable-tasks", durable=True)
    await ch.publish("", "durable-tasks", {"keep": "me"})
    await conn.close()
    await server.stop()
    await store.close()

    store2 = Store(db)
    await store2.open()
    broker2 = Broker(store=store2)
    await broker2.recover()
    server2 = await _serve(broker2)
    try:
        conn2 = await connect(f"quill://127.0.0.1:{server2.port}")
        ch2 = await conn2.channel()
        async for msg in ch2.consume("durable-tasks", prefetch=1):
            assert msg.body == {"keep": "me"}
            assert msg.redelivered is True
            await msg.ack()
            break
        await conn2.close()
    finally:
        await server2.stop()
        await store2.close()


async def test_nack_requeues_then_redelivers():
    server = await _serve(Broker())
    try:
        url = f"quill://127.0.0.1:{server.port}"
        conn = await connect(url)
        ch = await conn.channel()
        await ch.declare_queue("retry", durable=False)
        await ch.publish("", "retry", {"n": 1})
        attempts = 0
        async for msg in ch.consume("retry", prefetch=1):
            attempts += 1
            if attempts == 1:
                await msg.nack(requeue=True)
            else:
                assert msg.redelivered is True
                await msg.ack()
                break
        assert attempts == 2
        await conn.close()
    finally:
        await server.stop()


async def test_topic_routing_end_to_end():
    server = await _serve(Broker())
    try:
        url = f"quill://127.0.0.1:{server.port}"
        conn = await connect(url)
        ch = await conn.channel()
        await ch.declare_exchange("events", "topic")
        await ch.declare_queue("wp", durable=False)
        await ch.bind("wp", "events", "wp.#")
        await ch.publish("events", "wp.build.done", {"ok": True})
        async for msg in ch.consume("wp", auto_ack=True):
            assert msg.routing_key == "wp.build.done"
            assert msg.body == {"ok": True}
            break
        await conn.close()
    finally:
        await server.stop()
