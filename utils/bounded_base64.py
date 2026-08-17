from __future__ import annotations

import base64


def decode_bounded_base64(value: object, *, max_bytes: int) -> bytes | None:
    """Decode canonical base64 only after rejecting impossible sizes."""
    if not isinstance(value, str):
        return None
    encoded = value.strip()
    max_chars = ((max_bytes + 2) // 3) * 4
    if not encoded or len(encoded) > max_chars or len(encoded) % 4:
        return None
    padding = len(encoded) - len(encoded.rstrip("="))
    decoded_size = (len(encoded) // 4) * 3 - padding
    if decoded_size > max_bytes:
        return None
    try:
        decoded = base64.b64decode(encoded, validate=True)
    except (TypeError, ValueError):
        return None
    return decoded if len(decoded) <= max_bytes else None
