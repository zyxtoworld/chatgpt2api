from __future__ import annotations

import struct
import zlib


FIXTURE_NAMES = (
    "chery_studio.png",
    "image.png",
    "image_edit.png",
)

_FIXTURE_COLORS = {
    "chery_studio.png": (38, 92, 140),
    "image.png": (142, 74, 116),
    "image_edit.png": (72, 128, 82),
}


def _png_chunk(kind: bytes, payload: bytes) -> bytes:
    return (
        struct.pack(">I", len(payload))
        + kind
        + payload
        + struct.pack(">I", zlib.crc32(kind + payload) & 0xFFFFFFFF)
    )


def image_fixture_bytes(name: str) -> bytes:
    """Return a small deterministic RGB PNG for the named integration input."""
    if name not in _FIXTURE_COLORS:
        raise ValueError(f"unknown image fixture: {name}")

    width = 16
    height = 16
    base_r, base_g, base_b = _FIXTURE_COLORS[name]
    rows = []
    for y in range(height):
        row = bytearray([0])
        for x in range(width):
            offset = (x + y) % 16
            row.extend((base_r + offset, base_g + offset, base_b + offset))
        rows.append(bytes(row))

    return b"\x89PNG\r\n\x1a\n" + b"".join(
        (
            _png_chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0)),
            _png_chunk(b"IDAT", zlib.compress(b"".join(rows), level=9)),
            _png_chunk(b"IEND", b""),
        )
    )
