# syntax=docker/dockerfile:1.7
# QuillMQ broker image. Self-contained and clean-room: official Python base
# plus uv, no private registries or build secrets required.

# Pinned by digest for reproducible builds; Dependabot keeps it current.
FROM python:3.13-slim-bookworm@sha256:fcbd8dfc2605ba7c2eca646846c5e892b2931e41f6227985154a596f26ab8ed7

# uv, copied from its official distroless image (pinned major version).
COPY --from=ghcr.io/astral-sh/uv:0.11 /uv /uvx /bin/

WORKDIR /app

# Install dependencies first for better layer caching, using the committed lock.
# UV_SYNC_FLAGS is empty by default (standard, secure build). Behind a
# TLS-intercepting corporate proxy, pass e.g. "--native-tls" after adding your
# root CA, or "--allow-insecure-host pypi.org".
ARG UV_SYNC_FLAGS=""
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
RUN uv sync --frozen --no-dev ${UV_SYNC_FLAGS}

# Run the installed console script straight from the venv: no uv at runtime,
# so no cache/home is needed and shutdown signals reach the broker directly.
ENV PATH="/app/.venv/bin:${PATH}"
ENV QUILLMQ_DATA=/data

# Durable data lives in a volume so it survives restarts; owned by a non-root user.
RUN mkdir -p /data && useradd --system --uid 10001 quillmq && chown quillmq /data
USER quillmq
VOLUME ["/data"]

EXPOSE 5555

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD quillmq stats --url quill://127.0.0.1:5555 || exit 1

# Durable broker by default. Override the command for an in-memory broker.
CMD ["sh", "-c", "exec quillmq serve --host 0.0.0.0 --port 5555 --data ${QUILLMQ_DATA}/quill.db"]
