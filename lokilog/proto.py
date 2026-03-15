import struct
from datetime import datetime, timezone
from typing import Sequence


def _encode_varint(value: int) -> bytes:
    bits = []
    while True:
        group = value & 0x7F
        value >>= 7
        if value:
            bits.append(group | 0x80)
        else:
            bits.append(group)
            break
    return bytes(bits)


def _field_tag(field_number: int, wire_type: int) -> bytes:
    return _encode_varint((field_number << 3) | wire_type)


_WT_VARINT = 0
_WT_64BIT  = 1
_WT_LEN    = 2
_WT_32BIT  = 5


def _field_bytes(field_number: int, data: bytes) -> bytes:
    return _field_tag(field_number, _WT_LEN) + _encode_varint(len(data)) + data


def _field_string(field_number: int, value: str) -> bytes:
    return _field_bytes(field_number, value.encode("utf-8"))


def _field_int64(field_number: int, value: int) -> bytes:
    if value < 0:
        value += (1 << 64)
    return _field_tag(field_number, _WT_VARINT) + _encode_varint(value)


def _field_int32(field_number: int, value: int) -> bytes:
    if value < 0:
        value += (1 << 32)
    return _field_tag(field_number, _WT_VARINT) + _encode_varint(value)


def encode_label_pair(name: str, value: str) -> bytes:
    return _field_string(1, name) + _field_string(2, value)


def encode_timestamp(unix_ns: int) -> bytes:
    seconds = unix_ns // 1_000_000_000
    nanos   = unix_ns % 1_000_000_000
    buf  = _field_int64(1, seconds)
    buf += _field_int32(2, nanos)
    return buf


def encode_entry(
    timestamp_ns: int,
    line: str,
    metadata: Sequence[tuple[str, str]] | None = None,
) -> bytes:
    buf  = _field_bytes(1, encode_timestamp(timestamp_ns))
    buf += _field_string(2, line)
    if metadata:
        for name, value in metadata:
            buf += _field_bytes(3, encode_label_pair(name, value))
    return buf


def encode_stream(labels: str, entries: list[bytes]) -> bytes:
    buf = _field_string(1, labels)
    for entry_bytes in entries:
        buf += _field_bytes(2, entry_bytes)
    return buf


def encode_push_request(streams: list[bytes]) -> bytes:
    buf = b""
    for stream_bytes in streams:
        buf += _field_bytes(1, stream_bytes)
    return buf
