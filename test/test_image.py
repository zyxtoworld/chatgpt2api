"""Opt-in live image smoke replacing the old cwd-writing helper script."""

from __future__ import annotations

import pytest
import requests

from test.live_contract_support import bearer_headers, live_base_url
from test.utils import decode_image_payload, require_http_response


@pytest.mark.live_upstream
def test_image_generation_legacy_model_returns_decodable_image_in_memory():
    response = requests.post(
        f"{live_base_url()}/v1/images/generations",
        headers=bearer_headers(),
        json={
            "prompt": "一只橘猫坐在窗台上，午后阳光，写实摄影",
            "model": "gpt-5-3",
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
    data = payload.get("data")
    assert isinstance(data, list) and data
    decoded = [decode_image_payload(str(item.get("b64_json") or "")) for item in data if isinstance(item, dict)]
    assert decoded
