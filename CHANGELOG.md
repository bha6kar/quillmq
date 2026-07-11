# Changelog

All notable changes to QuillMQ are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-07-11

### Added

- Broker core with direct, fanout, and topic exchanges.
- Work queues with competing consumers, round-robin dispatch, and prefetch.
- Pub/sub fan-out and topic routing (`*` and `#` patterns).
- RPC request/reply helpers with correlation ids.
- At-least-once delivery: manual ack/nack, redelivery, disconnect requeue.
- SQLite WAL durability with recovery of durable queues and messages on restart.
- Asyncio TCP server with an optional shared-secret auth token.
- Async client library (`connect`, `Channel`, `Message`) and `quillmq` CLI
  (`serve`, `publish`, `stats`, `queues`, `tail`).
- msgspec-based wire serialisation with strict validation of inbound frames.
- Operational hardening: structured logging (text or JSON), graceful shutdown on
  SIGTERM/SIGINT, idle-connection heartbeat timeout with client keepalives, and
  HELLO handshake enforcement.
- Message reliability: configurable max-delivery-count with a dead-letter queue
  for poison messages, and optional per-message TTL.
- Optional TLS transport via a `quills://` URL.
- Prometheus `/metrics` endpoint (stdlib, no extra dependency) with broker
  counters and queue-depth gauges.
- Environment-variable configuration for every `quillmq serve` option.
- Docker image and compose file.
- Examples for every pattern, including a self-contained `demo_all.py`.

[Unreleased]: https://github.com/bha6kar/quillmq/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/bha6kar/quillmq/releases/tag/v0.1.0
