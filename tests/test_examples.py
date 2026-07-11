"""Guard the shipped examples: the self-contained demo must run end-to-end.

This exercises every pattern (work queue, pub/sub, topic, RPC, observability,
durability) through the public API, so a regression in any of them fails here.
"""

import asyncio
import importlib.util
from pathlib import Path

_DEMO = Path(__file__).resolve().parent.parent / "examples" / "demo_all.py"


def _load_demo():
    spec = importlib.util.spec_from_file_location("quillmq_demo_all", _DEMO)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


async def test_demo_all_runs_end_to_end(capsys):
    demo = _load_demo()
    await asyncio.wait_for(demo.main(), timeout=20)
    out = capsys.readouterr().out
    assert "All demos completed successfully." in out
    assert "recovered order 1001" in out
    assert "{'result': 42}" in out
    assert "quillmq_published_total" in out  # observability section scraped /metrics
