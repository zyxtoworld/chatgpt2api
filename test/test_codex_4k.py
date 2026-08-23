"""Opt-in live contract for one Codex image-generation response."""

from __future__ import annotations

import json
from typing import Any

import pytest
import requests

from test.live_contract_support import bearer_headers, explicit_url
from test.utils import decode_image_payload, iter_sse_data, require_http_response


def _find_image_values(value: object) -> list[str]:
    if isinstance(value, dict):
        if value.get("type") == "image_generation_call" and isinstance(value.get("result"), str):
            return [value["result"]]
        return [item for child in value.values() for item in _find_image_values(child)]
    if isinstance(value, list):
        return [item for child in value for item in _find_image_values(child)]
    return []


def _read_response_events(response: requests.Response) -> tuple[list[dict[str, Any]], list[str]]:
    require_http_response(response)
    events: list[dict[str, Any]] = []
    parse_errors: list[str] = []
    try:
        content_type = response.headers.get("content-type", "")
        if "application/json" in content_type:
            payload = response.json()
            if isinstance(payload, dict):
                events.append(payload)
            else:
                parse_errors.append("non_object_json")
            return events, parse_errors
        for payload_text in iter_sse_data(response):
            if payload_text == "[DONE]":
                continue
            try:
                payload = json.loads(payload_text)
            except Exception:
                parse_errors.append("invalid_json")
                continue
            if isinstance(payload, dict):
                events.append(payload)
            else:
                parse_errors.append("non_object_event")
    finally:
        response.close()
    return events, parse_errors


@pytest.mark.live_upstream
def test_codex_4k_image_stream_has_completed_terminal_and_decodable_result():
    response = requests.post(
        f"{explicit_url('CHATGPT2API_LIVE_CODEX_BASE_URL')}/backend-api/codex/responses",
        headers={**bearer_headers(), "Content-Type": "application/json"},
        json={
            "model": "gpt-5.5",
            "instructions": "Use the image_generation tool to create exactly one image.",
            "store": False,
            "input": [{"role": "user", "content": [{"type": "input_text", "text": "A quiet futuristic library at sunrise."}]}],
            "tools": [{
                "type": "image_generation",
                "model": "gpt-image-2",
                "action": "generate",
                "size": "3840x2160",
                "quality": "auto",
                "output_format": "png",
            }],
            "tool_choice": {"type": "image_generation"},
            "stream": True,
        },
        stream=True,
        timeout=1200,
    )
    events, parse_errors = _read_response_events(response)
    event_types = [str(event.get("type") or "") for event in events]
    errors = [event for event in events if event.get("type") == "error" or event.get("error") is not None]
    images = [value for event in events for value in _find_image_values(event)]
    assert "response.completed" in event_types
    assert not errors, [str(event.get("type") or "error") for event in errors]
    assert not parse_errors, parse_errors
    assert images
    for image in images:
        decode_image_payload(image)
