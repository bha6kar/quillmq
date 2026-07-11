import time

from quillmq.broker import Broker
from quillmq.queue import Consumer


def collector():
    got = []

    async def send(frame):
        got.append(frame)

    return got, send


async def test_nack_reject_dead_letters():
    b = Broker(dead_letter_queue="dlq")
    await b.declare_queue("jobs", durable=False)
    got, send = collector()
    c = Consumer("jobs")
    await b.add_consumer("jobs", c, send)
    await b.publish("", "jobs", {"n": 1})
    await b.nack("jobs", c, got[0]["delivery_tag"], requeue=False)
    assert b.queues["jobs"].depth() == 0

    dgot, dsend = collector()
    await b.add_consumer("dlq", Consumer("dlq"), dsend)
    assert dgot[0]["body"] == {"n": 1}
    assert dgot[0]["headers"]["x-death-queue"] == "jobs"


async def test_max_delivery_count_dead_letters():
    b = Broker(max_delivery_count=2, dead_letter_queue="dlq")
    await b.declare_queue("jobs", durable=False)
    got, send = collector()
    c = Consumer("jobs")
    await b.add_consumer("jobs", c, send)
    await b.publish("", "jobs", {"n": 1})
    await b.nack("jobs", c, got[0]["delivery_tag"], requeue=True)  # attempt 1, requeued
    await b.nack(
        "jobs", c, got[1]["delivery_tag"], requeue=True
    )  # attempt 2, over limit
    assert b.queues["jobs"].depth() == 0

    dgot, dsend = collector()
    await b.add_consumer("dlq", Consumer("dlq"), dsend)
    assert dgot[0]["body"] == {"n": 1}
    assert dgot[0]["headers"]["x-death-count"] == 2


async def test_dead_letter_dropped_without_queue():
    b = Broker(max_delivery_count=1)  # no dead-letter queue configured
    await b.declare_queue("jobs", durable=False)
    got, send = collector()
    c = Consumer("jobs")
    await b.add_consumer("jobs", c, send)
    await b.publish("", "jobs", {"n": 1})
    await b.nack("jobs", c, got[0]["delivery_tag"], requeue=True)  # over limit, no dlq
    assert b.queues["jobs"].depth() == 0
    assert "dlq" not in b.queues


async def test_expired_message_is_dead_lettered():
    b = Broker(dead_letter_queue="dlq")
    await b.declare_queue("jobs", durable=False)
    await b.publish("", "jobs", {"n": 1}, headers={"expires_at": time.time() - 1})
    got, send = collector()
    await b.add_consumer("jobs", Consumer("jobs"), send)
    assert got == []  # expired, never delivered to the consumer

    dgot, dsend = collector()
    await b.add_consumer("dlq", Consumer("dlq"), dsend)
    assert dgot[0]["body"] == {"n": 1}
    assert "expires_at" not in dgot[0]["headers"]


async def test_ttl_header_converts_and_expires():
    b = Broker()  # no dlq: expired message is simply dropped
    await b.declare_queue("jobs", durable=False)
    await b.publish("", "jobs", {"n": 1}, headers={"ttl": -1})  # already in the past
    got, send = collector()
    await b.add_consumer("jobs", Consumer("jobs"), send)
    assert got == []


async def test_unexpired_ttl_is_delivered():
    b = Broker()
    await b.declare_queue("jobs", durable=False)
    got, send = collector()
    await b.add_consumer("jobs", Consumer("jobs"), send)
    await b.publish("", "jobs", {"n": 1}, headers={"ttl": 60})
    assert got[0]["body"] == {"n": 1}
