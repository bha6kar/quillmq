import asyncio

from quillmq.broker import Broker
from quillmq.metrics import MetricsServer
from quillmq.queue import Consumer


def collector():
    got = []

    async def send(frame):
        got.append(frame)

    return got, send


async def test_counters_track_broker_activity():
    b = Broker(dead_letter_queue="dlq")
    await b.declare_queue("jobs", durable=False)
    got, send = collector()
    c = Consumer("jobs")
    await b.add_consumer("jobs", c, send)
    await b.publish("", "jobs", {"n": 1})
    await b.ack("jobs", c, got[0]["delivery_tag"])
    await b.publish("", "jobs", {"n": 2})
    await b.nack("jobs", c, got[1]["delivery_tag"], requeue=False)  # dead-lettered

    m = b.metrics.counters
    assert m["published"] >= 2
    assert m["delivered"] >= 2
    assert m["acked"] == 1
    assert m["nacked"] == 1
    assert m["dead_lettered"] == 1

    text = b.metrics.render(b)
    assert "quillmq_acked_total 1" in text
    assert 'quillmq_queue_depth{queue="jobs"}' in text


async def _get(port: int, path: str) -> tuple[str, str]:
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(f"GET {path} HTTP/1.1\r\nHost: x\r\n\r\n".encode())
    await writer.drain()
    raw = await asyncio.wait_for(reader.read(65536), timeout=2)
    writer.close()
    head, _, body = raw.partition(b"\r\n\r\n")
    return head.decode().splitlines()[0], body.decode()


async def test_metrics_http_endpoint_serves_and_404s():
    b = Broker()
    await b.declare_queue("jobs", durable=False)
    await b.publish("", "jobs", {"n": 1})
    server = MetricsServer(b, host="127.0.0.1", port=0)
    await server.start()
    try:
        status, body = await _get(server.port, "/metrics")
        assert "200" in status
        assert "quillmq_published_total 1" in body

        status_404, _ = await _get(server.port, "/nope")
        assert "404" in status_404
    finally:
        await server.stop()
