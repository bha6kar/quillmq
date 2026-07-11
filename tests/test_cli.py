import asyncio
import contextlib
import threading

import pytest

from quillmq import connect
from quillmq.broker import Broker
from quillmq.cli import _stats_cmd, _tail_cmd, main, run_serve
from quillmq.server import BrokerServer


class _ServerThread:
    """Runs a broker in its own loop/thread so sync main() can talk to it."""

    def __init__(self) -> None:
        self.port = 0
        self._loop = None
        self._server = None
        self._ready = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._server = BrokerServer(Broker(), host="127.0.0.1", port=0)
        self._loop.run_until_complete(self._server.start())
        self.port = self._server.port
        self._ready.set()
        self._loop.run_forever()

    def start(self) -> None:
        self._thread.start()
        self._ready.wait(5)

    def stop(self) -> None:
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(5)


@pytest.fixture
def server_thread():
    st = _ServerThread()
    st.start()
    yield st
    st.stop()


def test_main_help_exits_zero():
    with pytest.raises(SystemExit) as exc:
        main(["--help"])
    assert exc.value.code == 0


def test_main_publish_stats_queues(server_thread, capsys):
    url = f"quill://127.0.0.1:{server_thread.port}"

    async def declare():
        conn = await connect(url)
        ch = await conn.channel()
        await ch.declare_queue("tasks", durable=False)
        await conn.close()

    asyncio.run(declare())

    assert main(["publish", "", "tasks", '{"n": 1}', "--url", url]) == 0
    assert main(["stats", "--url", url]) == 0
    assert '"tasks"' in capsys.readouterr().out
    assert main(["queues", "--url", url]) == 0


async def test_stats_cmd_helper():
    server = BrokerServer(Broker(), host="127.0.0.1", port=0)
    await server.start()
    try:
        url = f"quill://127.0.0.1:{server.port}"
        conn = await connect(url)
        ch = await conn.channel()
        await ch.declare_queue("q", durable=False)
        await conn.close()
        stats = await _stats_cmd(url)
        assert "q" in stats["queues"]
    finally:
        await server.stop()


async def test_run_serve_with_durability_starts_and_stops(tmp_path):
    task = asyncio.create_task(run_serve("127.0.0.1", 0, str(tmp_path / "q.db"), None))
    await asyncio.sleep(0.1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_run_serve_in_memory_starts_and_stops():
    task = asyncio.create_task(run_serve("127.0.0.1", 0, None, None))
    await asyncio.sleep(0.1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_tail_cmd_prints_bodies(capsys):
    server = BrokerServer(Broker(), host="127.0.0.1", port=0)
    await server.start()
    try:
        url = f"quill://127.0.0.1:{server.port}"
        conn = await connect(url)
        ch = await conn.channel()
        await ch.declare_queue("tasks", durable=False)
        await ch.publish("", "tasks", {"n": 1})
        await conn.close()

        task = asyncio.create_task(_tail_cmd(url, "tasks"))
        await asyncio.sleep(0.2)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    finally:
        await server.stop()
    assert '{"n": 1}' in capsys.readouterr().out
