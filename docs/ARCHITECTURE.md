# QuillMQ architecture

QuillMQ is a single-node message broker built in layers, each testable in
isolation. The layers below the network edge are pure and synchronous where
possible, which keeps the concurrency reasoning contained to the server and
client.

## Layers

```
client.py         Connection / Channel / Message   (what applications import)
   |  quill(s):// TCP (optional TLS), length-prefixed JSON frames
server.py         asyncio TCP server, handshake, auth, idle-heartbeat timeout
broker.py         routing, delivery, durability, dead-letter, TTL  (no sockets)
   |-- exchange.py    direct / fanout / topic routing and the topic matcher
   |-- queue.py       in-memory ready deque, consumers, round-robin, prefetch
   |-- store.py       SQLite WAL persistence and startup recovery
   |-- metrics.py     broker counters and the Prometheus /metrics endpoint
protocol.py       msgspec frame codec shared by client and server
logging_config.py structured logging (text or JSON)
```

## Concepts

- **Exchange**: a routing point. `direct` matches the routing key exactly,
  `fanout` sends to all bound queues, `topic` matches patterns (`*` is one word,
  `#` is zero or more). A default nameless exchange routes by queue name.
- **Queue**: an ordered store, `durable` (SQLite-backed) or `transient`
  (in-memory).
- **Binding**: connects an exchange to a queue by routing key or pattern.
- **Consumer**: a subscription. Several consumers on one queue compete, giving
  work-queue load balancing.

## Delivery and durability

Delivery is at-least-once. A consumer acknowledges each message; an unacked
message is redelivered on nack or on consumer disconnect. For durable queues a
message is written to SQLite before the publish is confirmed, and deleted on
ack. On startup the broker reloads durable exchanges, queues, bindings, and
messages, so nothing durable is lost across a restart.

A message that exceeds the configured max-delivery-count, is rejected
(`nack` without requeue), or has an expired TTL is routed to a dead-letter queue
if one is configured, or dropped with a warning otherwise.

## Operations

The server enforces a HELLO handshake and closes connections that send nothing
within the heartbeat window; clients send periodic heartbeats to stay alive.
The broker exposes counters and queue-depth gauges over a Prometheus `/metrics`
endpoint, logs in text or JSON, and shuts down gracefully on SIGTERM/SIGINT.

## Wire protocol

Each frame is a four-byte big-endian length prefix followed by a JSON body
encoded with msgspec, which validates the decoded frame is a well-formed object
before it reaches the broker. Requests carry a request id that the broker echoes
on its `ok` or `error`
reply, so the client can correlate responses; deliveries are pushed
asynchronously and routed on the client by queue name.

## Deliberate non-goals

Single node only: no clustering, replication, or failover. No AMQP
compatibility, no headers exchanges, message priorities, or delayed delivery.
These keep the core small; see the README and COMPARISON.md for where that
trade-off fits.
