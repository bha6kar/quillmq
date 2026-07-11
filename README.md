# QuillMQ

[![CI](https://github.com/bha6kar/quillmq/actions/workflows/ci.yml/badge.svg)](https://github.com/bha6kar/quillmq/actions/workflows/ci.yml)
[![PyPI version](https://img.shields.io/pypi/v/quillmq)](https://pypi.org/project/quillmq/)
[![Python versions](https://img.shields.io/badge/python-3.11%20%7C%203.12%20%7C%203.13-blue.svg)](https://pypi.org/project/quillmq/)
[![Licence](https://img.shields.io/badge/licence-Apache--2.0-blue.svg)](LICENSE)

A lightweight, single-node message broker written from scratch in Python asyncio.
It provides work queues, pub/sub fan-out, RPC request/reply, and at-least-once
durable delivery over a small custom TCP protocol. No RabbitMQ or AMQP dependency.

QuillMQ is to RabbitMQ what SQLite is to PostgreSQL: not a replacement for a
clustered production broker, but the better fit when you want embeddable,
single-node, zero-ops messaging you can read and own. See
[COMPARISON.md](COMPARISON.md) for an honest side-by-side.

## Install

    uv sync --extra dev

## Run the broker

    uv run quillmq serve --port 5555 --data ./quill.db

Omit `--data` for an in-memory broker.

## Python client

```python
from quillmq import connect

conn = await connect("quill://127.0.0.1:5555")
ch = await conn.channel()
await ch.declare_queue("tasks", durable=True)
await ch.publish("", "tasks", {"job": 1})
async for msg in ch.consume("tasks", prefetch=10):
    handle(msg.body)
    await msg.ack()
```

## Patterns

- Work queue: many consumers on one queue compete; each message is delivered once.
- Pub/sub: bind several queues to a `fanout` or `topic` exchange.
- RPC: `await ch.rpc_call("service.rpc", {...})`, served by `quillmq.rpc.RPCServer`.

## CLI

    quillmq serve --port 5555 --data ./quill.db
    quillmq publish "" tasks '{"job": 1}'
    quillmq stats
    quillmq tail tasks

## Running in production (single node)

QuillMQ is single-node by design (no clustering), but a single instance is built
to run as a real service:

    quillmq serve \
      --port 5555 \
      --data ./quill.db \
      --auth-token "$QUILLMQ_AUTH_TOKEN" \
      --tls-cert cert.pem --tls-key key.pem \
      --heartbeat 60 \
      --max-delivery-count 10 --dead-letter-queue dead \
      --metrics-port 9095 \
      --json-logs

- Structured logging (text or JSON), and graceful shutdown on SIGTERM/SIGINT.
- Idle connections are reaped after `--heartbeat` seconds; clients send
  keepalives automatically.
- Poison messages are dead-lettered after `--max-delivery-count` attempts; set
  a per-message TTL with a `ttl` header (seconds).
- TLS: pass `--tls-cert`/`--tls-key` and connect with a `quills://` URL.
- Prometheus metrics are served at `http://host:9095/metrics`.
- Every flag has a `QUILLMQ_*` environment variable equivalent.

## Docker

    docker compose up --build

This starts a durable broker on port 5555 with a named volume for persistence.
To build the image directly:

    docker build -t quillmq:latest .

Behind a TLS-intercepting corporate proxy, pass your CA or, as a last resort:

    docker build -t quillmq:latest \
      --build-arg UV_SYNC_FLAGS="--allow-insecure-host pypi.org --allow-insecure-host files.pythonhosted.org" .

## Licence

Apache-2.0.
