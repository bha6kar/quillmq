import asyncio

import pytest

from quillmq import protocol as p


def test_roundtrip_single_frame():
    frame = {"type": p.PUBLISH, "exchange": "", "routing_key": "q", "body": {"n": 1}}
    dec = p.FrameDecoder()
    dec.feed(p.encode_frame(frame))
    assert list(dec) == [frame]


def test_partial_reads_reassemble():
    frame = {"type": p.HELLO, "version": 1}
    raw = p.encode_frame(frame)
    dec = p.FrameDecoder()
    dec.feed(raw[:3])
    assert list(dec) == []  # header incomplete
    dec.feed(raw[3:7])
    assert list(dec) == []  # body incomplete
    dec.feed(raw[7:])
    assert list(dec) == [frame]


def test_multiple_frames_in_one_feed():
    a = {"type": p.ACK, "delivery_tag": 1}
    b = {"type": p.ACK, "delivery_tag": 2}
    dec = p.FrameDecoder()
    dec.feed(p.encode_frame(a) + p.encode_frame(b))
    assert list(dec) == [a, b]


def test_oversized_frame_rejected_in_decoder():
    dec = p.FrameDecoder(max_frame_size=8)
    dec.feed((9).to_bytes(4, "big") + b"123456789")
    with pytest.raises(p.FrameError):
        list(dec)


def test_encode_rejects_oversized(monkeypatch):
    monkeypatch.setattr(p, "MAX_FRAME_SIZE", 4)
    with pytest.raises(p.FrameError):
        p.encode_frame({"type": "x", "big": "payload"})


async def test_read_frame_from_stream_and_eof():
    a = {"type": p.OK, "rid": 5}
    reader = asyncio.StreamReader()
    reader.feed_data(p.encode_frame(a))
    reader.feed_eof()
    assert await p.read_frame(reader) == a
    assert await p.read_frame(reader) is None


def test_malformed_json_rejected():
    dec = p.FrameDecoder()
    raw = b"{not json"
    dec.feed(len(raw).to_bytes(4, "big") + raw)
    with pytest.raises(p.FrameError):
        list(dec)


def test_non_object_frame_rejected():
    dec = p.FrameDecoder()
    raw = p._encoder.encode([1, 2, 3])  # a JSON array, not an object
    dec.feed(len(raw).to_bytes(4, "big") + raw)
    with pytest.raises(p.FrameError):
        list(dec)


def test_frame_without_string_type_rejected():
    dec = p.FrameDecoder()
    raw = p._encoder.encode({"no": "type field"})
    dec.feed(len(raw).to_bytes(4, "big") + raw)
    with pytest.raises(p.FrameError):
        list(dec)


async def test_read_frame_rejects_oversized_header():
    reader = asyncio.StreamReader()
    reader.feed_data((p.MAX_FRAME_SIZE + 1).to_bytes(4, "big"))
    reader.feed_eof()
    with pytest.raises(p.FrameError):
        await p.read_frame(reader)
