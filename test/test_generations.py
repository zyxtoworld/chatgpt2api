"""Opt-in live contract for the OpenAI image generations endpoint."""

from __future__ import annotations

import json

import pytest
import requests

from test.live_contract_support import bearer_headers, live_base_url
from test.utils import decode_image_payload, iter_sse_data, require_http_response, require_stream_response


@pytest.mark.live_upstream
def test_image_generation_non_stream_returns_decodable_images_in_memory():
    response = requests.post(
        f"{live_base_url()}/v1/images/generations",
        headers=bearer_headers(),
        json={
            "prompt": "A cute orange cat sitting on a chair",
            "model": "gpt-image-2",
            "n": 1,
            "response_format": "b64_json",
        },
        timeout=300,
    )
    require_http_response(response, content_type="application/json")
    try:
        payload = response.json()
    finally:
        response.close()
    assert isinstance(payload, dict)
    assert payload.get("object") in {"list", "image_generation"}
    data = payload.get("data")
    assert isinstance(data, list) and data
    decoded = [decode_image_payload(str(item.get("b64_json") or "")) for item in data if isinstance(item, dict)]
    assert decoded


@pytest.mark.live_upstream
def test_image_generation_stream_has_terminal_and_decodable_images_in_memory():
    response = requests.post(
        f"{live_base_url()}/v1/images/generations",
        headers=bearer_headers(),
        json={
            "prompt": "A cute orange cat sitting on a chair",
            "model": "gpt-image-2",
            "n": 1,
            "response_format": "b64_json",
            "stream": True,
        },
        stream=True,
        timeout=300,
    )
    require_stream_response(response)
    event_types: list[str] = []
    images: list[str] = []
    parse_errors: list[str] = []
    errors: list[str] = []
    try:
        for payload_text in iter_sse_data(response):
            if payload_text == "[DONE]":
                continue
            try:
                payload = json.loads(payload_text)
            except Exception:
                parse_errors.append("invalid_json")
                continue
            if not isinstance(payload, dict):
                parse_errors.append("non_object_event")
                continue
            event_type = payload.get("type")
            if isinstance(event_type, str):
                event_types.append(event_type)
            if event_type == "error" or payload.get("error") is not None:
                errors.append(str(event_type or "error"))
            if event_type == "image_generation.completed" and payload.get("b64_json"):
                images.append(str(payload["b64_json"]))
    finally:
        response.close()
    assert "image_generation.completed" in event_types
    assert not errors, errors
    assert not parse_errors, parse_errors
    assert images
    for image in images:
        decode_image_payload(image)
