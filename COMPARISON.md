# QuillMQ compared to RabbitMQ

Short version: QuillMQ is to RabbitMQ what SQLite is to PostgreSQL. RabbitMQ is
the right tool when you need a clustered, highly available, multi-protocol
message broker with years of production hardening. QuillMQ is the right tool
when you want messaging without running and operating that infrastructure: a
small, embeddable, single-node broker you can read in an afternoon and own
outright.

So "better" here means "a better fit for a specific set of jobs", not "better at
everything". Below is where each one wins, stated plainly.

## Where QuillMQ is the better choice

- **Zero infrastructure.** No Erlang/BEAM runtime, no separate server to install
  and operate. Two small, focused dependencies (`aiosqlite` for durability and
  `msgspec` for wire serialisation) and the Python you already have. RabbitMQ
  needs an Erlang node running and managed.
- **Embeddable.** You can start a broker in-process, inside your own application
  or test suite, on an ephemeral port, in milliseconds. The bundled
  `examples/demo_all.py` does exactly this. With RabbitMQ you stand up a real
  node or a container first.
- **Small and hackable.** The whole broker, client, RPC layer and CLI are about
  950 lines of straightforward Python. You can read it end to end, understand
  every delivery decision, and change it. RabbitMQ is a large Erlang codebase.
- **Python-native async API.** `await ch.publish(...)` and `async for msg in
  ch.consume(...)` drop straight into an asyncio application. No separate client
  library plus a server to coordinate.
- **Fast local development and CI.** Tests spin the broker up and tear it down
  per test with no external service, so the suite runs in about a second.
- **Transparent durability.** Durable messages live in a single SQLite WAL file
  you can open and inspect with any SQLite tool.
- **You own the IP.** Clean-room, Apache-2.0, no third-party broker to depend on
  or track licences for.

## Where RabbitMQ is the better choice

Being honest about this is the point:

- **Clustering, high availability and failover.** QuillMQ is single-node by
  design. RabbitMQ offers clustering, quorum queues and mirrored queues.
- **Scale and hardening.** RabbitMQ has years of production use at high
  throughput. QuillMQ is new and single-process.
- **Protocol interop.** RabbitMQ speaks AMQP 0-9-1 and 1.0, plus MQTT and STOMP
  via plugins, with mature clients in every language. QuillMQ speaks its own
  compact protocol with a Python client only.
- **Operations tooling.** RabbitMQ ships a management UI, federation, shovel and
  a large plugin ecosystem. QuillMQ offers a Prometheus `/metrics` endpoint,
  `stats`, `tail` and structured logs, but no management UI or plugins.
- **Security and multitenancy.** RabbitMQ has vhosts, fine-grained authorisation
  and RBAC. QuillMQ has optional TLS and a single shared-secret token, but no
  vhosts or per-user permissions.
- **Advanced routing features.** Headers exchanges, message priorities and
  delayed delivery are built in to RabbitMQ. QuillMQ covers direct, fanout and
  topic exchanges plus dead-letter queues and per-message TTL, but not headers
  exchanges, priorities or delayed delivery.

## What is different under the hood

The mental model is deliberately the same, so concepts transfer directly, but
the implementations differ.

| Aspect            | QuillMQ                                  | RabbitMQ                                       |
|-------------------|------------------------------------------|------------------------------------------------|
| Runtime           | Python asyncio                           | Erlang/OTP on the BEAM VM                       |
| Wire protocol     | Length-prefixed JSON frames over TCP, optional TLS | AMQP 0-9-1 binary framing (and more)  |
| Topology model    | Exchanges, queues, bindings              | Exchanges, queues, bindings (same idea)        |
| Exchange types    | direct, fanout, topic                    | direct, fanout, topic, headers                 |
| Delivery          | At-least-once, ack/nack, redelivery, dead-letter, TTL | At-least-once, plus richer QoS       |
| Persistence       | Single SQLite WAL file                   | Built-in message store, Mnesia/Khepri metadata |
| Topology scope    | Single node                              | Single node or cluster                          |
| Client libraries  | Python (async)                           | Every major language                           |
| Footprint         | About 950 lines, two small dependencies  | Large codebase, Erlang runtime                 |
| Licence           | Apache-2.0                               | Mozilla Public License 2.0                      |

## Rule of thumb

- Reach for **QuillMQ** when you want simple messaging between a handful of
  services or processes, prefer one small dependency over a managed broker,
  value being able to read and own the code, and do not need clustering or
  multi-language clients.
- Reach for **RabbitMQ** when you need horizontal scale, high availability,
  AMQP interop, mature operational tooling, or clients in many languages.

If your system outgrows QuillMQ, the exchange/queue/binding model maps cleanly
onto RabbitMQ, so the migration is mostly swapping the client, not rethinking
the design.
