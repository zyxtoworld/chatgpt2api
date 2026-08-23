"""Pytest policy for deterministic and live-upstream test layers."""

from __future__ import annotations

import os
from urllib.parse import urlsplit

import pytest


_LIVE_UPSTREAM_ENV = "CHATGPT2API_LIVE_UPSTREAM"


def _live_upstream_enabled() -> bool:
    return os.getenv(_LIVE_UPSTREAM_ENV, "").strip().lower() in {"1", "true", "yes"}


def pytest_sessionstart(session: pytest.Session) -> None:
    del session
    if not _live_upstream_enabled():
        return
    base_url = os.getenv("CHATGPT2API_LIVE_BASE_URL", "").strip()
    codex_base_url = os.getenv("CHATGPT2API_LIVE_CODEX_BASE_URL", "").strip()
    api_key = os.getenv("CHATGPT2API_LIVE_API_KEY", "").strip()
    parsed = urlsplit(base_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise pytest.UsageError(
            "CHATGPT2API_LIVE_BASE_URL must be an explicit http(s) URL when live_upstream is enabled"
        )
    parsed_codex = urlsplit(codex_base_url)
    if parsed_codex.scheme not in {"http", "https"} or not parsed_codex.netloc:
        raise pytest.UsageError(
            "CHATGPT2API_LIVE_CODEX_BASE_URL must be an explicit http(s) URL when live_upstream is enabled"
        )
    if not api_key:
        raise pytest.UsageError(
            "CHATGPT2API_LIVE_API_KEY must be set when live_upstream is enabled"
        )


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    del config
    if _live_upstream_enabled():
        return
    skip = pytest.mark.skip(
        reason=(
            "live_upstream disabled; set CHATGPT2API_LIVE_UPSTREAM=1 and run the "
            "explicit live smoke command"
        )
    )
    for item in items:
        if "live_upstream" in item.keywords:
            item.add_marker(skip)
