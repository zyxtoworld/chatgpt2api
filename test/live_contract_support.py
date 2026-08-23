"""Shared, fail-closed helpers for explicit live-upstream smoke tests."""

from __future__ import annotations

import os
from typing import Any
from urllib.parse import quote

import requests

from test.utils import require_http_response


def live_base_url() -> str:
    value = os.getenv("CHATGPT2API_LIVE_BASE_URL", "").strip().rstrip("/")
    if not value:
        raise AssertionError("CHATGPT2API_LIVE_BASE_URL is required for live_upstream")
    return value


def live_api_key() -> str:
    value = os.getenv("CHATGPT2API_LIVE_API_KEY", "").strip()
    if not value:
        raise AssertionError("CHATGPT2API_LIVE_API_KEY is required for live_upstream")
    return value


def explicit_url(env_name: str) -> str:
    value = os.getenv(env_name, "").strip().rstrip("/")
    if not value:
        raise AssertionError(f"{env_name} is required for this live_upstream test")
    return value


def bearer_headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {live_api_key()}"}


def request_json(
    method: str,
    path: str,
    payload: dict[str, Any] | None = None,
    *,
    timeout: float = 60,
    base_url: str | None = None,
) -> dict[str, Any]:
    response = requests.request(
        method,
        f"{(base_url or live_base_url()).rstrip('/')}{path}",
        headers={**bearer_headers(), "Content-Type": "application/json"},
        json=payload,
        timeout=timeout,
    )
    require_http_response(response, content_type="application/json")
    try:
        value = response.json()
    finally:
        response.close()
    if not isinstance(value, dict):
        raise AssertionError("live JSON response must be an object")
    return value


def task_status(task_id: str) -> dict[str, Any]:
    return request_json(
        "GET",
        f"/v1/editable-file-tasks?ids={quote(task_id, safe='')}",
        timeout=60,
    )
