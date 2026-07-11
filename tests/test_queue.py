from quillmq.queue import Consumer, Message, Queue


def mk(qname, i):
    return Message(id=i, queue=qname, routing_key=qname, headers={}, body={"n": i})


def test_single_consumer_gets_all_ready():
    q = Queue("tasks", durable=False)
    c = Consumer("c1", prefetch=0, auto_ack=False)
    q.add_consumer(c)
    q.enqueue(mk("tasks", 1))
    q.enqueue(mk("tasks", 2))
    delivered = q.dispatch()
    assert [m.id for _, m in delivered] == [1, 2]
    assert set(c.unacked) == {1, 2}


def test_round_robin_across_competing_consumers():
    q = Queue("tasks", durable=False)
    c1 = Consumer("c1", prefetch=0, auto_ack=False)
    c2 = Consumer("c2", prefetch=0, auto_ack=False)
    q.add_consumer(c1)
    q.add_consumer(c2)
    for i in range(1, 5):
        q.enqueue(mk("tasks", i))
    delivered = q.dispatch()
    owners = {m.id: c.tag for c, m in delivered}
    assert owners == {1: "c1", 2: "c2", 3: "c1", 4: "c2"}


def test_prefetch_limits_in_flight():
    q = Queue("tasks", durable=False)
    c = Consumer("c1", prefetch=1, auto_ack=False)
    q.add_consumer(c)
    q.enqueue(mk("tasks", 1))
    q.enqueue(mk("tasks", 2))
    assert [m.id for _, m in q.dispatch()] == [1]  # only 1 in flight
    q.ack(c, 1)
    assert [m.id for _, m in q.dispatch()] == [2]  # freed a slot


def test_dispatch_stops_when_all_consumers_saturated():
    q = Queue("tasks", durable=False)
    c = Consumer("c1", prefetch=1, auto_ack=False)
    q.add_consumer(c)
    q.enqueue(mk("tasks", 1))
    q.enqueue(mk("tasks", 2))
    q.dispatch()
    assert q.dispatch() == []  # saturated, nothing more goes out
    assert q.depth() == 1


def test_no_consumers_keeps_backlog():
    q = Queue("tasks", durable=False)
    q.enqueue(mk("tasks", 1))
    assert q.dispatch() == []
    assert q.depth() == 1


def test_auto_ack_does_not_track_unacked():
    q = Queue("tasks", durable=False)
    c = Consumer("c1", prefetch=0, auto_ack=True)
    q.add_consumer(c)
    q.enqueue(mk("tasks", 1))
    q.dispatch()
    assert c.unacked == {}


def test_ack_then_requeue_puts_message_back_and_marks_redelivered():
    q = Queue("tasks", durable=False)
    c = Consumer("c1", prefetch=0, auto_ack=False)
    q.add_consumer(c)
    q.enqueue(mk("tasks", 1))
    q.dispatch()
    m = q.ack(c, 1)
    assert m is not None and 1 not in c.unacked
    q.requeue(m)
    again = q.dispatch()
    assert again[0][1].id == 1
    assert again[0][1].redelivered is True


def test_ack_without_requeue_drops():
    q = Queue("tasks", durable=False)
    c = Consumer("c1", prefetch=0, auto_ack=False)
    q.add_consumer(c)
    q.enqueue(mk("tasks", 1))
    q.dispatch()
    assert q.ack(c, 1) is not None
    assert q.depth() == 0


def test_ack_unknown_tag_returns_none():
    q = Queue("tasks", durable=False)
    c = Consumer("c1")
    q.add_consumer(c)
    assert q.ack(c, 999) is None


def test_remove_consumer_returns_unacked_for_requeue():
    q = Queue("tasks", durable=False)
    c = Consumer("c1", prefetch=0, auto_ack=False)
    q.add_consumer(c)
    q.enqueue(mk("tasks", 1))
    q.dispatch()
    orphaned = q.remove_consumer(c)
    assert [m.id for m in orphaned] == [1]
