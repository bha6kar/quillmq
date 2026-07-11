# QuillMQ examples

Runnable examples for every QuillMQ pattern. All commands assume the repo root
and an environment set up with `uv sync --extra dev`.

## Fastest way to see everything

One self-contained script starts an embedded broker and walks through work
queues, pub/sub, topic routing, RPC, a Prometheus metrics scrape, and durability
across a restart, with narrated output. No separate broker needed:

    uv run python examples/demo_all.py

Read the printed steps alongside `demo_all.py` to see how each pattern is wired.
Section 6 prints live `quillmq_*_total` counters scraped from the broker's
`/metrics` endpoint, so the demo doubles as an observability check.

## The two-process examples

These mirror real usage: a broker process plus separate producer and consumer
processes. Start the broker once in its own terminal:

    uv run quillmq serve --port 5555

Then run the pairs below in other terminals.

### Work queue (load balancing, exactly-once)

Each task goes to exactly one worker. Run several workers to see them share the
load; an unacked task is redelivered if a worker dies.

    uv run python examples/worker.py        # terminal A
    uv run python examples/worker.py        # terminal B
    uv run python examples/producer.py      # terminal C

### Pub/Sub fan-out (broadcast)

Every subscriber gets its own copy. Start the subscribers first so their queues
are bound before you publish.

    uv run python examples/pubsub_subscriber.py audit       # terminal A
    uv run python examples/pubsub_subscriber.py notifier    # terminal B
    uv run python examples/pubsub_publisher.py              # terminal C

### RPC request/reply

A call that returns a value, using a reply queue and correlation id under the
hood.

    uv run python examples/rpc_server.py    # terminal A
    uv run python examples/rpc_client.py    # terminal B

## Inspecting a running broker

    uv run quillmq stats --url quill://127.0.0.1:5555
    uv run quillmq tail tasks --url quill://127.0.0.1:5555
    uv run quillmq publish "" tasks '{"job": 1}' --url quill://127.0.0.1:5555

## Observability (Prometheus metrics)

Start the broker with a metrics port, generate some traffic, then scrape it:

    uv run quillmq serve --port 5555 --metrics-port 9095          # terminal A
    uv run python examples/producer.py                            # terminal B
    curl http://127.0.0.1:9095/metrics                            # terminal C

You will see `quillmq_published_total`, `quillmq_delivered_total`,
`quillmq_acked_total`, and per-queue `quillmq_queue_depth` gauges. The demo
above (`demo_all.py`, section 6) shows the same scrape in-process.

## TLS

Generate a self-signed certificate, serve with TLS, and connect over `quills://`:

    openssl req -x509 -newkey rsa:2048 -nodes -keyout key.pem -out cert.pem \
        -days 365 -subj "/CN=127.0.0.1" -addext "subjectAltName=IP:127.0.0.1"
    uv run quillmq serve --port 5556 --tls-cert cert.pem --tls-key key.pem
    uv run python examples/tls_client.py

`tls_client.py` trusts that certificate and publishes/consumes over the
encrypted connection.

## Topic routing

Topic routing (bindings with `*` and `#`) is shown in section 3 of
`demo_all.py`: `*` matches exactly one dot-delimited word, `#` matches zero or
more. For example a queue bound to `auth.#` receives `auth.login` and
`auth.mfa.failed`, while `*.error` receives `auth.error` and `payment.error`.
