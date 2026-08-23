"""Opt-in live contract for image URL output and token accounting."""

from __future__ import annotations

import pytest
import requests

from test.live_contract_support import bearer_headers, live_base_url
from test.utils import require_http_response


@pytest.mark.live_upstream
def test_image_generation_url_response_is_successful_and_nonempty():
    response = requests.post(
        f"{live_base_url()}/v1/images/generations",
        headers=bearer_headers(),
        json={
            "prompt": "一只橘猫坐在窗台上，午后阳光，写实摄影",
            "model": "gpt-image-2",
            "n": 1,
            "response_format": "url",
        },
        timeout=300,
    )
    require_http_response(response, content_type="application/json")
    try:
        payload = response.json()
    finally:
        response.close()
    assert isinstance(payload, dict)
    assert isinstance(payload.get("usage"), dict)
    data = payload.get("data")
    assert isinstance(data, list) and data
    urls = [str(item.get("url") or "") for item in data if isinstance(item, dict)]
    assert urls and all(url.startswith(("http://", "https://", "/")) for url in urls)
