import asyncio
import json
import logging
import sys

from quillmq import connect
from quillmq import protocol as p
from quillmq.broker import Broker
from quillmq.logging_config import JsonFormatter, configure_logging, logger
from quillmq.server import BrokerServer


async def _open(server):
    return await asyncio.open_connection("127.0.0.1", server.port)


async def test_idle_connection_is_closed():
    server = BrokerServer(Broker(), host="127.0.0.1", port=0, heartbeat=0.3)
    await server.start()
    try:
        reader, writer = await _open(server)
        writer.write(p.encode_frame({"type": p.HELLO, "rid": 1}))
        await writer.drain()
        assert (await p.read_frame(reader))["type"] == p.OK
        # stay idle: the server should close us once the heartbeat window lapses
        closed = await asyncio.wait_for(p.read_frame(reader), timeout=2)
        assert closed is None
        writer.close()
    finally:
        await server.stop()


async def test_connection_without_hello_times_out():
    server = BrokerServer(Broker(), host="127.0.0.1", port=0, heartbeat=0.3)
    await server.start()
    try:
        reader, writer = await _open(server)
        # never send HELLO
        closed = await asyncio.wait_for(p.read_frame(reader), timeout=2)
        assert closed is None
        writer.close()
    finally:
        await server.stop()


async def test_first_frame_must_be_hello():
    server = BrokerServer(Broker(), host="127.0.0.1", port=0)
    await server.start()
    try:
        reader, writer = await _open(server)
        writer.write(p.encode_frame({"type": p.STATS, "rid": 7}))
        await writer.drain()
        reply = await p.read_frame(reader)
        assert reply["type"] == p.ERROR
        assert reply["rid"] == 7
        assert "hello" in reply["message"]
        writer.close()
    finally:
        await server.stop()


async def test_client_heartbeat_keeps_connection_alive():
    server = BrokerServer(Broker(), host="127.0.0.1", port=0, heartbeat=0.4)
    await server.start()
    try:
        conn = await connect(f"quill://127.0.0.1:{server.port}", heartbeat_interval=0.1)
        ch = await conn.channel()
        await ch.declare_queue("q", durable=False)
        # idle longer than the server heartbeat; client pings keep it alive
        await asyncio.sleep(0.7)
        stats = await ch.stats()
        assert "q" in stats["queues"]
        await conn.close()
    finally:
        await server.stop()


def test_configure_logging_text_and_json():
    configure_logging("DEBUG", json_format=False)
    assert logger.level == logging.DEBUG
    assert not isinstance(logger.handlers[0].formatter, JsonFormatter)

    configure_logging("INFO", json_format=True)
    fmt = logger.handlers[0].formatter
    assert isinstance(fmt, JsonFormatter)

    rec = logging.LogRecord("quillmq", logging.INFO, __file__, 1, "hi", None, None)
    assert json.loads(fmt.format(rec))["message"] == "hi"

    try:
        raise ValueError("boom")
    except ValueError:
        rec2 = logging.LogRecord(
            "quillmq", logging.ERROR, __file__, 1, "err", None, sys.exc_info()
        )
    assert "boom" in json.loads(fmt.format(rec2))["exc"]
