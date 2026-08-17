from __future__ import annotations


MODEL_TEXT_MAX_LENGTH = 256


def parse_model_text(value: object, *, default: str = "") -> str:
    if not isinstance(value, str):
        return default
    text = value.strip()
    if not text or len(text) > MODEL_TEXT_MAX_LENGTH:
        return default
    return text
