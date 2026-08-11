from __future__ import annotations

from typing import Any


_CONVERSATION_EFFORTS = {
    "none": "auto",
    "auto": "auto",
    "low": "low",
    "medium": "medium",
    "high": "high",
    "xhigh": "xhigh",
    "extended": "extended",
    "standard": "standard",
    "max": "max",
}


_CONVERSATION_EFFORT_STRENGTH = {
    "auto": 0,
    "minimal": 1,
    "min": 1,
    "low": 2,
    "medium": 3,
    "standard": 4,
    "high": 5,
    "extended": 6,
    "xhigh": 7,
    "max": 8,
    "ultra": 9,
}


_FALLBACK_CONVERSATION_EFFORTS = {
    "low": "low",
    "medium": "medium",
    "high": "high",
    "standard": "standard",
    "extended": "extended",
    "xhigh": "extended",
    "max": "max",
}


def canonical_conversation_effort(value: object) -> str:
    if value is None:
        return ""
    if not isinstance(value, str):
        raise ValueError("reasoning effort must be a string")
    normalized = value.strip().lower()
    if not normalized:
        return ""
    return _CONVERSATION_EFFORTS.get(normalized, normalized)


def strongest_conversation_effort(values: list[str] | tuple[str, ...]) -> str:
    normalized = [canonical_conversation_effort(value) for value in values]
    normalized = [value for value in normalized if value]
    if not normalized:
        return ""
    return max(
        enumerate(normalized),
        key=lambda item: (_CONVERSATION_EFFORT_STRENGTH.get(item[1], -1), item[0]),
    )[1]


def fallback_conversation_effort(value: object) -> str:
    normalized = canonical_conversation_effort(value)
    return _FALLBACK_CONVERSATION_EFFORTS.get(normalized, "")


def normalize_conversation_effort(value: object) -> str:
    """Normalize aliases while deferring model support checks to the upstream catalog."""
    return canonical_conversation_effort(value)


def conversation_effort_from_body(body: dict[str, Any]) -> str:
    """Extract exactly one effort source; reject fields this backend cannot honor."""
    candidates: list[object] = []
    if "reasoning" in body and body.get("reasoning") is not None:
        reasoning = body.get("reasoning")
        if not isinstance(reasoning, dict):
            raise ValueError("reasoning must be an object")
        if any(key != "effort" for key in reasoning):
            raise ValueError("reasoning parameter is not supported by the upstream text backend")
        if reasoning.get("effort") is not None:
            candidates.append(reasoning.get("effort"))
    for key in ("reasoning_effort", "thinking_effort"):
        if key in body and body.get(key) is not None:
            candidates.append(body.get(key))
    if len(candidates) > 1:
        raise ValueError("reasoning effort must be specified once")
    return normalize_conversation_effort(candidates[0] if candidates else None)


def codex_effort_from_body(body: dict[str, Any]) -> str:
    candidates = [
        body.get(key)
        for key in ("reasoning_effort", "thinking_effort")
        if key in body and body.get(key) is not None
    ]
    if len(candidates) > 1:
        raise ValueError("reasoning effort must be specified once")
    if not candidates:
        return ""
    value = candidates[0]
    if not isinstance(value, str):
        raise ValueError("reasoning effort must be a string")
    normalized = value.strip().lower()
    if not normalized:
        raise ValueError("reasoning effort must not be empty")
    return normalized
