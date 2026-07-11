# SPDX-License-Identifier: Apache-2.0
"""Length-prefixed frame codec for the QuillMQ wire protocol.

Frames are a 4-byte big-endian length prefix followed by a msgspec-encoded JSON
body. msgspec is used rather than the stdlib json module because it is fast,
validates untrusted input strictly, and gives a clean path to msgpack later.
"""

from __future__ import annotations

import asyncio

import msgspec

PROTOCOL_VERSION = 1
MAX_FRAME_SIZE = 16 * 1024 * 1024
_HEADER = 4

HELLO = "hello"
OK = "ok"
ERROR = "error"
DECLARE_EXCHANGE = "declare_exchange"
DECLARE_QUEUE = "declare_queue"
BIND = "bind"
PUBLISH = "publish"
CONSUME = "consume"
DELIVER = "deliver"
ACK = "ack"
NACK = "nack"
STATS = "stats"
HEARTBEAT = "heartbeat"


class FrameError(Exception):
    """Raised on a malformed or oversized frame."""


_encoder = msgspec.json.Encoder()
_decoder = msgspec.json.Decoder()


def encode_frame(obj: dict) -> bytes:
    body = _encoder.encode(obj)
    if len(body) > MAX_FRAME_SIZE:
        raise FrameError(f"frame too large: {len(body)} bytes")
    return len(body).to_bytes(_HEADER, "big") + body


def _decode_body(body: bytes) -> dict:
    try:
        frame = _decoder.decode(body)
    except msgspec.DecodeError as exc:
        raise FrameError(f"malformed frame: {exc}") from exc
    if not isinstance(frame, dict) or not isinstance(frame.get("type"), str):
        raise FrameError("frame must be an object with a string 'type'")
    return frame


class FrameDecoder:
    """Incremental decoder tolerant of partial and coalesced reads."""

    def __init__(self, max_frame_size: int = MAX_FRAME_SIZE) -> None:
        self._buf = bytearray()
        self._max = max_frame_size

    def feed(self, data: bytes) -> None:
        self._buf.extend(data)

    def __iter__(self):
        while True:
            if len(self._buf) < _HEADER:
                return
            n = int.from_bytes(self._buf[:_HEADER], "big")
            if n > self._max:
                raise FrameError(f"frame too large: {n} bytes")
            if len(self._buf) < _HEADER + n:
                return
            body = bytes(self._buf[_HEADER : _HEADER + n])
            del self._buf[: _HEADER + n]
            yield _decode_body(body)


async def read_frame(reader: asyncio.StreamReader) -> dict | None:
    try:
        header = await reader.readexactly(_HEADER)
    except asyncio.IncompleteReadError:
        return None
    n = int.from_bytes(header, "big")
    if n > MAX_FRAME_SIZE:
        raise FrameError(f"frame too large: {n} bytes")
    try:
        body = await reader.readexactly(n)
    except asyncio.IncompleteReadError:  # pragma: no cover - truncated mid-frame
        return None
    return _decode_body(body)
