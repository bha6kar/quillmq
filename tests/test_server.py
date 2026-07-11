import asyncio

from quillmq import protocol as p
from quillmq.broker import Broker
from quillmq.server import BrokerServer


async def _open(server):
    return await asyncio.open_connection("127.0.0.1", server.port)


async def _send(writer, frame):
    writer.write(p.encode_frame(frame))
    await writer.drain()


async def test_hello_ok_and_publish_consume_roundtrip():
    server = BrokerServer(Broker(), host="127.0.0.1", port=0)
    await server.start()
    try:
        reader, writer = await _open(server)
        await _send(writer, {"type": p.HELLO, "version": 1, "rid": 1})
        assert (await p.read_frame(reader))["type"] == p.OK
        await _send(
            writer,
            {"type": p.DECLARE_QUEUE, "queue": "tasks", "durable": False, "rid": 2},
        )
        assert (await p.read_frame(reader))["rid"] == 2
        await _send(
            writer,
            {
                "type": p.CONSUME,
                "queue": "tasks",
                "consumer_tag": "c1",
                "prefetch": 0,
                "auto_ack": False,
                "rid": 3,
            },
        )
        assert (await p.read_frame(reader))["rid"] == 3
        await _send(
            writer,
            {
                "type": p.PUBLISH,
                "exchange": "",
                "routing_key": "tasks",
                "headers": {},
                "body": {"n": 7},
            },
        )
        deliver = await p.read_frame(reader)
        assert deliver["type"] == p.DELIVER and deliver["body"] == {"n": 7}
        # ack it, then nack path exercised via a second message
        await _send(
            writer,
            {
                "type": p.ACK,
                "consumer_tag": "c1",
                "delivery_tag": deliver["delivery_tag"],
            },
        )
        writer.close()
    finally:
        await server.stop()


async def test_declare_exchange_bind_and_stats_and_heartbeat():
    server = BrokerServer(Broker(), host="127.0.0.1", port=0)
    await server.start()
    try:
        reader, writer = await _open(server)
        await _send(writer, {"type": p.HELLO, "rid": 1})
        await p.read_frame(reader)
        await _send(
            writer,
            {
                "type": p.DECLARE_EXCHANGE,
                "exchange": "events",
                "type_": "fanout",
                "rid": 2,
            },
        )
        assert (await p.read_frame(reader))["rid"] == 2
        await _send(
            writer, {"type": p.DECLARE_QUEUE, "queue": "a", "durable": False, "rid": 3}
        )
        await p.read_frame(reader)
        await _send(
            writer,
            {
                "type": p.BIND,
                "exchange": "events",
                "queue": "a",
                "routing_key": "",
                "rid": 4,
            },
        )
        assert (await p.read_frame(reader))["rid"] == 4
        await _send(writer, {"type": p.HEARTBEAT})  # no reply expected
        await _send(writer, {"type": p.STATS, "rid": 5})
        reply = await p.read_frame(reader)
        assert reply["stats"]["queues"]["a"]["depth"] == 0
        writer.close()
    finally:
        await server.stop()


async def test_nack_requeue_over_the_wire():
    server = BrokerServer(Broker(), host="127.0.0.1", port=0)
    await server.start()
    try:
        reader, writer = await _open(server)
        await _send(writer, {"type": p.HELLO})
        await _send(writer, {"type": p.DECLARE_QUEUE, "queue": "q", "durable": False})
        await _send(
            writer,
            {
                "type": p.CONSUME,
                "queue": "q",
                "consumer_tag": "q",
                "prefetch": 1,
                "auto_ack": False,
            },
        )
        await _send(
            writer,
            {
                "type": p.PUBLISH,
                "exchange": "",
                "routing_key": "q",
                "headers": {},
                "body": {"n": 1},
            },
        )
        first = await p.read_frame(reader)
        await _send(
            writer,
            {
                "type": p.NACK,
                "consumer_tag": "q",
                "delivery_tag": first["delivery_tag"],
                "requeue": True,
            },
        )
        again = await p.read_frame(reader)
        assert again["redelivered"] is True
        writer.close()
    finally:
        await server.stop()


async def test_unknown_frame_returns_error():
    server = BrokerServer(Broker(), host="127.0.0.1", port=0)
    await server.start()
    try:
        reader, writer = await _open(server)
        await _send(writer, {"type": p.HELLO})
        await _send(writer, {"type": "bogus", "rid": 9})
        reply = await p.read_frame(reader)
        assert reply["type"] == p.ERROR and reply["rid"] == 9
        writer.close()
    finally:
        await server.stop()


async def test_auth_token_rejected_on_mismatch():
    server = BrokerServer(Broker(), host="127.0.0.1", port=0, auth_token="secret")
    await server.start()
    try:
        reader, writer = await _open(server)
        await _send(writer, {"type": p.HELLO, "version": 1, "token": "wrong", "rid": 1})
        reply = await p.read_frame(reader)
        assert reply["type"] == p.ERROR
        writer.close()
    finally:
        await server.stop()


async def test_auth_token_accepted_on_match():
    server = BrokerServer(Broker(), host="127.0.0.1", port=0, auth_token="secret")
    await server.start()
    try:
        reader, writer = await _open(server)
        await _send(
            writer, {"type": p.HELLO, "version": 1, "token": "secret", "rid": 1}
        )
        reply = await p.read_frame(reader)
        assert reply["type"] == p.OK
        writer.close()
    finally:
        await server.stop()


async def test_disconnect_requeues_unacked_to_new_consumer():
    broker = Broker()
    server = BrokerServer(broker, host="127.0.0.1", port=0)
    await server.start()
    try:
        r1, w1 = await _open(server)
        await _send(w1, {"type": p.HELLO, "version": 1})
        await _send(w1, {"type": p.DECLARE_QUEUE, "queue": "tasks", "durable": False})
        await _send(
            w1,
            {
                "type": p.CONSUME,
                "queue": "tasks",
                "consumer_tag": "c1",
                "prefetch": 0,
                "auto_ack": False,
            },
        )
        await _send(
            w1,
            {
                "type": p.PUBLISH,
                "exchange": "",
                "routing_key": "tasks",
                "headers": {},
                "body": {"n": 1},
            },
        )
        got = await p.read_frame(r1)
        assert got["body"] == {"n": 1}  # delivered, not acked
        w1.close()
        await w1.wait_closed()
        await asyncio.sleep(0.05)  # let server observe the disconnect

        r2, w2 = await _open(server)
        await _send(w2, {"type": p.HELLO, "version": 1})
        await _send(
            w2,
            {
                "type": p.CONSUME,
                "queue": "tasks",
                "consumer_tag": "c2",
                "prefetch": 0,
                "auto_ack": False,
            },
        )
        redelivered = await p.read_frame(r2)
        assert redelivered["body"] == {"n": 1}
        assert redelivered["redelivered"] is True
        w2.close()
    finally:
        await server.stop()
