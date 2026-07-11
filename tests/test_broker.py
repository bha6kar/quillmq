from quillmq.broker import Broker
from quillmq.queue import Consumer
from quillmq.store import Store


def collector():
    got = []

    async def send(frame):
        got.append(frame)

    return got, send


async def test_work_queue_delivers_to_one_consumer():
    b = Broker()
    await b.declare_queue("tasks", durable=False)
    got, send = collector()
    c = Consumer("c1", prefetch=0, auto_ack=False)
    await b.add_consumer("tasks", c, send)
    await b.publish("", "tasks", {"n": 1})
    assert len(got) == 1
    assert got[0]["type"] == "deliver"
    assert got[0]["body"] == {"n": 1}
    assert got[0]["delivery_tag"] >= 1


async def test_competing_consumers_each_use_their_own_callback():
    # Regression: two consumers on the SAME queue share a tag but must each
    # receive on their own send callback, not have one overwrite the other.
    b = Broker()
    await b.declare_queue("jobs", durable=False)
    got_a, send_a = collector()
    got_b, send_b = collector()
    ca = Consumer("jobs", prefetch=1, auto_ack=False)
    cb = Consumer("jobs", prefetch=1, auto_ack=False)
    await b.add_consumer("jobs", ca, send_a)
    await b.add_consumer("jobs", cb, send_b)
    await b.publish("", "jobs", {"n": 1})
    await b.publish("", "jobs", {"n": 2})
    # one message to each distinct callback, none lost
    assert len(got_a) == 1
    assert len(got_b) == 1
    assert {got_a[0]["body"]["n"], got_b[0]["body"]["n"]} == {1, 2}


async def test_fanout_reaches_all_bound_queues():
    b = Broker()
    await b.declare_exchange("events", "fanout")
    await b.declare_queue("a", durable=False)
    await b.declare_queue("b", durable=False)
    await b.bind("events", "a", "")
    await b.bind("events", "b", "")
    ga, sa = collector()
    gb, sb = collector()
    await b.add_consumer("a", Consumer("ca"), sa)
    await b.add_consumer("b", Consumer("cb"), sb)
    await b.publish("events", "", {"evt": "done"})
    assert ga[0]["body"] == {"evt": "done"}
    assert gb[0]["body"] == {"evt": "done"}


async def test_backlog_delivered_when_consumer_arrives():
    b = Broker()
    await b.declare_queue("tasks", durable=False)
    await b.publish("", "tasks", {"n": 1})
    got, send = collector()
    await b.add_consumer("tasks", Consumer("c1"), send)
    assert got[0]["body"] == {"n": 1}


async def test_declare_queue_is_idempotent():
    b = Broker()
    await b.declare_queue("tasks", durable=False)
    await b.declare_queue("tasks", durable=False)
    assert list(b.queues) == ["tasks"]


async def test_stats_reports_depth_and_consumers():
    b = Broker()
    await b.declare_queue("tasks", durable=False)
    await b.publish("", "tasks", {"n": 1})
    stats = b.stats()
    assert stats["queues"]["tasks"]["depth"] == 1
    assert stats["queues"]["tasks"]["consumers"] == 0


async def test_durable_publish_persists_and_ack_deletes(tmp_path):
    store = Store(str(tmp_path / "q.db"))
    await store.open()
    b = Broker(store=store)
    await b.declare_queue("tasks", durable=True)
    await b.publish("", "tasks", {"n": 1})
    assert len(await store.load_messages()) == 1
    got, send = collector()
    c = Consumer("c1", prefetch=0, auto_ack=False)
    await b.add_consumer("tasks", c, send)
    await b.ack("tasks", c, got[0]["delivery_tag"])
    assert await store.load_messages() == []
    await store.close()


async def test_durable_auto_ack_deletes_on_delivery(tmp_path):
    store = Store(str(tmp_path / "q.db"))
    await store.open()
    b = Broker(store=store)
    await b.declare_queue("tasks", durable=True)
    got, send = collector()
    await b.add_consumer("tasks", Consumer("c1", auto_ack=True), send)
    await b.publish("", "tasks", {"n": 1})
    assert got[0]["body"] == {"n": 1}
    assert await store.load_messages() == []
    await store.close()


async def test_durable_nack_requeue_updates_count(tmp_path):
    store = Store(str(tmp_path / "q.db"))
    await store.open()
    b = Broker(store=store)
    await b.declare_queue("tasks", durable=True)
    got, send = collector()
    c = Consumer("c1", prefetch=0, auto_ack=False)
    await b.add_consumer("tasks", c, send)
    await b.publish("", "tasks", {"n": 1})
    tag = got[0]["delivery_tag"]
    await b.nack("tasks", c, tag, requeue=True)
    stored = await store.load_messages()
    assert stored[0].delivery_count >= 1
    assert stored[0].id == tag
    await store.close()


async def test_durable_nack_drop_deletes(tmp_path):
    store = Store(str(tmp_path / "q.db"))
    await store.open()
    b = Broker(store=store)
    await b.declare_queue("tasks", durable=True)
    got, send = collector()
    c = Consumer("c1", prefetch=0, auto_ack=False)
    await b.add_consumer("tasks", c, send)
    await b.publish("", "tasks", {"n": 1})
    await b.nack("tasks", c, got[0]["delivery_tag"], requeue=False)
    assert await store.load_messages() == []
    await store.close()


async def test_recover_reloads_durable_topology_and_messages(tmp_path):
    store = Store(str(tmp_path / "q.db"))
    await store.open()
    b = Broker(store=store)
    await b.declare_exchange("events", "topic")
    await b.declare_queue("tasks", durable=True)
    await b.bind("events", "tasks", "wp.#")
    await b.publish("", "tasks", {"n": 42})
    await store.close()

    store2 = Store(str(tmp_path / "q.db"))
    await store2.open()
    b2 = Broker(store=store2)
    await b2.recover()
    assert "events" in b2.exchanges
    got, send = collector()
    await b2.add_consumer("tasks", Consumer("c1"), send)
    assert got[0]["body"] == {"n": 42}
    assert got[0]["redelivered"] is True  # delivery_count now 2
    await store2.close()


async def test_remove_consumer_requeues_unacked():
    b = Broker()
    await b.declare_queue("tasks", durable=False)
    got, send = collector()
    c = Consumer("c1", prefetch=0, auto_ack=False)
    await b.add_consumer("tasks", c, send)
    await b.publish("", "tasks", {"n": 1})
    await b.remove_consumer("tasks", c)
    assert b.queues["tasks"].depth() == 1
