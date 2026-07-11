# Contributing to QuillMQ

Thanks for your interest in improving QuillMQ. This is a small, focused project,
so contributions that keep it lean and well-tested are very welcome.

## Development setup

QuillMQ uses [uv](https://docs.astral.sh/uv/) for all tooling.

    git clone https://github.com/bha6kar/quillmq
    cd quillmq
    uv sync --extra dev

## Running the tests

    uv run pytest

The suite includes a coverage gate: the build fails below 95% coverage. New code
should come with tests. The `tests/` directory holds fast unit tests and
`tests/integration/` holds end-to-end tests that start a broker over real
sockets.

## Trying it out

Run the narrated demo, which starts an embedded broker and exercises every
pattern:

    uv run python examples/demo_all.py

## Guidelines

- Keep the core small. New features should earn their complexity; see the
  non-goals in the README before proposing large additions.
- Follow the existing style: type hints, `dataclasses` for models, no new
  runtime dependencies beyond `aiosqlite` and `msgspec` without discussion first.
- Every source file carries an `SPDX-License-Identifier: Apache-2.0` header.
- Write UK or US English consistently within a file; existing prose uses UK.
- Update `CHANGELOG.md` under `[Unreleased]` for user-visible changes.

## Pull requests

1. Fork and create a branch from `main`.
2. Add tests and make sure `uv run pytest` passes.
3. Keep the PR focused on one change; open an issue first for larger work.
4. CI runs the suite on Python 3.11, 3.12, and 3.13; all must pass.

## Reporting bugs

Open an issue using the bug report template with a minimal reproduction, the
expected behaviour, and what happened instead.
