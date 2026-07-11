from quillmq.queue import Message
from quillmq.store import Store


async def test_topology_roundtrip(tmp_path):
    s = Store(str(tmp_path / "q.db"))
    await s.open()
    await s.upsert_exchange("events", "fanout")
    await s.upsert_queue("tasks", True)
    await s.add_binding("events", "tasks", "wp.#")
    await s.add_binding("events", "tasks", "wp.#")  # idempotent
    await s.close()

    s2 = Store(str(tmp_path / "q.db"))
    await s2.open()
    topo = await s2.load_topology()
    await s2.close()
    assert ("events", "fanout") in topo["exchanges"]
    assert ("tasks", True) in topo["queues"]
    assert topo["bindings"].count(("events", "tasks", "wp.#")) == 1


async def test_message_persist_and_delete(tmp_path):
    s = Store(str(tmp_path / "q.db"))
    await s.open()
    await s.add_message(
        Message(
            id=1, queue="tasks", routing_key="tasks", headers={"h": 1}, body={"n": 1}
        )
    )
    await s.add_message(
        Message(id=2, queue="tasks", routing_key="tasks", headers={}, body={"n": 2})
    )
    await s.delete_message(1)
    msgs = await s.load_messages()
    assert [m.id for m in msgs] == [2]
    assert msgs[0].body == {"n": 2}
    assert await s.max_message_id() == 2
    await s.close()


async def test_delivery_count_survives_reload(tmp_path):
    s = Store(str(tmp_path / "q.db"))
    await s.open()
    await s.add_message(Message(id=1, queue="t", routing_key="t", headers={}, body=1))
    await s.update_delivery_count(1, 3)
    await s.close()

    s2 = Store(str(tmp_path / "q.db"))
    await s2.open()
    msgs = await s2.load_messages()
    await s2.close()
    assert msgs[0].delivery_count == 3


async def test_max_message_id_zero_when_empty(tmp_path):
    s = Store(str(tmp_path / "q.db"))
    await s.open()
    assert await s.max_message_id() == 0
    await s.close()
