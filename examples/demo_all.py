"""QuillMQ end-to-end demo: every pattern in one runnable file.

Run it with no broker already running:

    uv run python examples/demo_all.py

It starts an embedded broker in-process, then walks through work queues,
pub/sub fan-out, topic routing, RPC, observability metrics, and durability
across a restart, printing what happens at each step. Read alongside the output
to see how QuillMQ works.
"""

from __future__ import annotations

import asyncio
import os
import tempfile

from quillmq import connect
from quillmq.broker import Broker
from quillmq.metrics import MetricsServer
from quillmq.rpc import RPCServer
from quillmq.server import BrokerServer
from quillmq.store import Store


def section(title: str) -> None:
    print("\n" + "=" * 68)
    print(title)
    print("=" * 68)


async def work_queue_demo(url: str) -> None:
    section("1. Work queue: tasks load-balanced, each handled exactly once")
    prod = await connect(url)
    pch = await prod.channel()
    await pch.declare_queue("jobs", durable=False)

    counts = {"worker-A": 0, "worker-B": 0}
    done = asyncio.Event()

    async def worker(name: str) -> None:
        conn = await connect(url)
        ch = await conn.channel()
        try:
            # prefetch=1 means the broker hands this worker one task at a time,
            # so a slow worker never hogs the backlog.
            async for msg in ch.consume("jobs", prefetch=1):
                counts[name] += 1
                await asyncio.sleep(0.03)  # pretend the task takes some work
                print(f"  {name} handled task {msg.body['task']}")
                await msg.ack()
                if sum(counts.values()) >= 6:
                    done.set()
                    break
        finally:
            await conn.close()

    # Subscribe both workers first, then publish, so the broker round-robins
    # the tasks fairly between the two idle workers.
    workers = [
        asyncio.create_task(worker("worker-A")),
        asyncio.create_task(worker("worker-B")),
    ]
    await asyncio.sleep(0.1)
    for i in range(6):
        await pch.publish("", "jobs", {"task": i})
    print("  published 6 tasks to queue 'jobs'")

    await asyncio.wait_for(done.wait(), timeout=5)
    for w in workers:
        w.cancel()
    await asyncio.gather(*workers, return_exceptions=True)
    print(f"  result: {counts} (6 tasks, split between two workers)")
    await prod.close()


async def pubsub_demo(url: str) -> None:
    section("2. Pub/Sub fan-out: every subscriber gets its own copy")
    pub = await connect(url)
    pch = await pub.channel()
    await pch.declare_exchange("events", "fanout")

    subscribers = {}
    for name in ("audit", "notifier"):
        conn = await connect(url)
        ch = await conn.channel()
        await ch.declare_queue(f"sub.{name}", durable=False)
        await ch.bind(f"sub.{name}", "events", "")  # fanout ignores routing key
        subscribers[name] = (conn, ch)
    print("  two subscribers bound to the 'events' fanout exchange")

    await pch.publish("events", "", {"event": "user.signed_up", "id": 42})
    print("  published one event to 'events'")

    for name, (conn, ch) in subscribers.items():
        async for msg in ch.consume(f"sub.{name}", auto_ack=True):
            print(f"  subscriber '{name}' received {msg.body}")
            break
        await conn.close()
    await pub.close()


async def _drain(url: str, queue: str, n: int) -> list:
    conn = await connect(url)
    ch = await conn.channel()
    out: list = []
    try:
        async for msg in ch.consume(queue, auto_ack=True):
            out.append(msg.routing_key)
            if len(out) >= n:
                break
    finally:
        await conn.close()
    return out


async def topic_demo(url: str) -> None:
    section("3. Topic routing: patterns decide who receives")
    conn = await connect(url)
    ch = await conn.channel()
    await ch.declare_exchange("logs", "topic")
    await ch.declare_queue("q.errors", durable=False)
    await ch.declare_queue("q.auth", durable=False)
    await ch.bind("q.errors", "logs", "*.error")  # any 'something.error'
    await ch.bind("q.auth", "logs", "auth.#")  # anything under 'auth'
    print("  q.errors binds '*.error', q.auth binds 'auth.#'")

    for rk in ("auth.error", "auth.login", "payment.error"):
        await ch.publish("logs", rk, {"rk": rk})
    print("  published: auth.error, auth.login, payment.error")
    await conn.close()

    errors = await _drain(url, "q.errors", 2)
    auth = await _drain(url, "q.auth", 2)
    print(f"  q.errors received: {errors}   (the two '*.error' keys)")
    print(f"  q.auth   received: {auth}   (the two 'auth.*' keys)")


async def rpc_demo(url: str) -> None:
    section("4. RPC request/reply: a call that returns a value")
    srv = await connect(url)
    srv_ch = await srv.channel()
    await srv_ch.declare_queue("math.rpc", durable=False)

    async def handler(body: dict) -> dict:
        return {"result": body["a"] * body["b"]}

    server_task = asyncio.create_task(RPCServer(srv_ch, "math.rpc").serve(handler))

    cli = await connect(url)
    cli_ch = await cli.channel()
    reply = await cli_ch.rpc_call("math.rpc", {"a": 6, "b": 7}, timeout=5)
    print(f"  rpc_call math.rpc(a=6, b=7) -> {reply}")

    server_task.cancel()
    await srv.close()
    await cli.close()


async def observability_demo(metrics_port: int) -> None:
    section("5. Observability: Prometheus /metrics reflecting the activity above")
    reader, writer = await asyncio.open_connection("127.0.0.1", metrics_port)
    writer.write(b"GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n")
    await writer.drain()
    raw = await asyncio.wait_for(reader.read(65536), timeout=2)
    writer.close()
    body = raw.split(b"\r\n\r\n", 1)[1].decode()
    for line in body.splitlines():
        if line.startswith("quillmq_") and "_total " in line and " 0" not in line:
            print(f"  {line}")


async def durability_demo(workdir: str) -> None:
    section("6. Durability: a durable message survives a broker restart")
    db = os.path.join(workdir, "durable.db")

    store = Store(db)
    await store.open()
    broker = Broker(store=store)
    await broker.recover()
    server = BrokerServer(broker, host="127.0.0.1", port=0)
    await server.start()
    url = f"quill://127.0.0.1:{server.port}"
    conn = await connect(url)
    ch = await conn.channel()
    await ch.declare_queue("orders", durable=True)
    await ch.publish("", "orders", {"order": 1001})
    print("  published durable order 1001, now stopping the broker...")
    await conn.close()
    await server.stop()
    await store.close()

    store2 = Store(db)
    await store2.open()
    broker2 = Broker(store=store2)
    await broker2.recover()  # reloads durable state from SQLite
    server2 = BrokerServer(broker2, host="127.0.0.1", port=0)
    await server2.start()
    url2 = f"quill://127.0.0.1:{server2.port}"
    conn2 = await connect(url2)
    ch2 = await conn2.channel()
    async for msg in ch2.consume("orders", prefetch=1):
        print(
            f"  after restart, recovered order {msg.body['order']}"
            f" (redelivered={msg.redelivered})"
        )
        await msg.ack()
        break
    await conn2.close()
    await server2.stop()
    await store2.close()


async def main() -> None:
    # An embedded in-memory broker powers demos 1 to 4.
    broker = Broker()
    server = BrokerServer(broker, host="127.0.0.1", port=0)
    await server.start()
    metrics = MetricsServer(broker, host="127.0.0.1", port=0)
    await metrics.start()
    url = f"quill://127.0.0.1:{server.port}"
    print(f"embedded broker started on {url}")

    try:
        await work_queue_demo(url)
        await pubsub_demo(url)
        await topic_demo(url)
        await rpc_demo(url)
        await observability_demo(metrics.port)
    finally:
        await metrics.stop()
        await server.stop()

    # Durability needs its own on-disk broker, restarted mid-demo.
    with tempfile.TemporaryDirectory() as workdir:
        await durability_demo(workdir)

    print("\nAll demos completed successfully.")


if __name__ == "__main__":
    asyncio.run(main())
