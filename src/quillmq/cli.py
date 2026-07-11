# SPDX-License-Identifier: Apache-2.0
"""quillmq command-line interface."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import logging
import os
import signal
import ssl

from quillmq import connect
from quillmq.broker import Broker
from quillmq.logging_config import configure_logging
from quillmq.metrics import MetricsServer
from quillmq.server import BrokerServer
from quillmq.store import Store

logger = logging.getLogger("quillmq.cli")


async def run_serve(
    host: str,
    port: int,
    data: str | None,
    auth_token: str | None,
    *,
    heartbeat: float = 60.0,
    tls_cert: str | None = None,
    tls_key: str | None = None,
    max_delivery_count: int = 0,
    dead_letter_queue: str | None = None,
    metrics_port: int = 0,
) -> None:
    ssl_context = None
    if tls_cert and tls_key:
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ssl_context.load_cert_chain(tls_cert, tls_key)

    store = None
    if data:
        store = Store(data)
        await store.open()
    broker = Broker(
        store=store,
        max_delivery_count=max_delivery_count,
        dead_letter_queue=dead_letter_queue,
    )
    if store is not None:
        await broker.recover()
    server = BrokerServer(
        broker,
        host=host,
        port=port,
        auth_token=auth_token,
        heartbeat=heartbeat,
        ssl_context=ssl_context,
    )
    await server.start()
    logger.info(
        "quillmq serving on %s:%s (%s%s)",
        host,
        server.port,
        f"data={data}" if data else "in-memory",
        ", tls" if ssl_context else "",
    )

    metrics_server = None
    if metrics_port:
        metrics_server = MetricsServer(broker, host=host, port=metrics_port)
        await metrics_server.start()

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)
    try:
        await stop.wait()
    finally:
        logger.info("shutting down")
        if metrics_server is not None:
            await metrics_server.stop()
        await server.stop()
        if store is not None:
            await store.close()


async def _publish_cmd(
    url: str, exchange: str, routing_key: str, body_json: str
) -> None:
    conn = await connect(url)
    ch = await conn.channel()
    await ch.publish(exchange, routing_key, json.loads(body_json))
    await conn.close()


async def _stats_cmd(url: str) -> dict:
    conn = await connect(url)
    ch = await conn.channel()
    stats = await ch.stats()
    await conn.close()
    return stats


async def _tail_cmd(url: str, queue: str) -> None:
    conn = await connect(url)
    try:
        ch = await conn.channel()
        async for msg in ch.consume(queue, auto_ack=True):
            print(json.dumps(msg.body))
    finally:
        await conn.close()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="quillmq")
    sub = parser.add_subparsers(dest="command", required=True)

    default_url = os.getenv("QUILLMQ_URL", "quill://127.0.0.1:5555")

    s = sub.add_parser("serve", help="run the broker")
    s.add_argument("--host", default=os.getenv("QUILLMQ_HOST", "0.0.0.0"))
    s.add_argument("--port", type=int, default=int(os.getenv("QUILLMQ_PORT", "5555")))
    s.add_argument(
        "--data", default=os.getenv("QUILLMQ_DATA"), help="sqlite path for durability"
    )
    s.add_argument("--auth-token", default=os.getenv("QUILLMQ_AUTH_TOKEN"))
    s.add_argument(
        "--heartbeat",
        type=float,
        default=float(os.getenv("QUILLMQ_HEARTBEAT", "60")),
        help="seconds before an idle connection is closed",
    )
    s.add_argument("--tls-cert", default=os.getenv("QUILLMQ_TLS_CERT"))
    s.add_argument("--tls-key", default=os.getenv("QUILLMQ_TLS_KEY"))
    s.add_argument(
        "--max-delivery-count",
        type=int,
        default=int(os.getenv("QUILLMQ_MAX_DELIVERY_COUNT", "0")),
        help="dead-letter a message after this many delivery attempts (0 = unlimited)",
    )
    s.add_argument(
        "--dead-letter-queue", default=os.getenv("QUILLMQ_DEAD_LETTER_QUEUE")
    )
    s.add_argument(
        "--metrics-port",
        type=int,
        default=int(os.getenv("QUILLMQ_METRICS_PORT", "0")),
        help="expose Prometheus /metrics on this port (0 = disabled)",
    )
    s.add_argument("--log-level", default=os.getenv("QUILLMQ_LOG_LEVEL", "INFO"))
    s.add_argument(
        "--json-logs",
        action="store_true",
        default=bool(os.getenv("QUILLMQ_JSON_LOGS")),
    )

    pub = sub.add_parser("publish", help="publish one message")
    pub.add_argument("exchange")
    pub.add_argument("routing_key")
    pub.add_argument("body", help="JSON body")
    pub.add_argument("--url", default=default_url)

    for name in ("stats", "queues"):
        st = sub.add_parser(name, help="print broker queue stats")
        st.add_argument("--url", default=default_url)

    tail = sub.add_parser("tail", help="stream a queue to stdout")
    tail.add_argument("queue")
    tail.add_argument("--url", default=default_url)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.command == "serve":  # pragma: no cover - blocking entrypoint
        configure_logging(args.log_level, args.json_logs)
        with contextlib.suppress(KeyboardInterrupt):
            asyncio.run(
                run_serve(
                    args.host,
                    args.port,
                    args.data,
                    args.auth_token,
                    heartbeat=args.heartbeat,
                    tls_cert=args.tls_cert,
                    tls_key=args.tls_key,
                    max_delivery_count=args.max_delivery_count,
                    dead_letter_queue=args.dead_letter_queue,
                    metrics_port=args.metrics_port,
                )
            )
    elif args.command == "publish":
        asyncio.run(_publish_cmd(args.url, args.exchange, args.routing_key, args.body))
    elif args.command in ("stats", "queues"):
        print(json.dumps(asyncio.run(_stats_cmd(args.url)), indent=2))
    elif args.command == "tail":  # pragma: no cover - blocking entrypoint
        with contextlib.suppress(KeyboardInterrupt):
            asyncio.run(_tail_cmd(args.url, args.queue))
    return 0
