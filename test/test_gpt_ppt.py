"""Opt-in live contract for the PPT editable-file lifecycle."""

from __future__ import annotations

import os
import time
from uuid import uuid4

import pytest

from test.live_contract_support import live_base_url, request_json, task_status


PROMPT = "生成一份 2026 年 Q2 电商运营复盘 PPT，8 页以内，商务科技风，包含销售、用户、渠道、广告、618 活动和 Q3 规划。"
TASK_TIMEOUT_SECS = float(os.getenv("CHATGPT2API_LIVE_TASK_TIMEOUT_SECS", "600"))
POLL_INTERVAL_SECS = float(os.getenv("CHATGPT2API_LIVE_TASK_POLL_SECS", "5"))


def _wait_for_success(task_id: str) -> dict[str, object]:
    deadline = time.monotonic() + TASK_TIMEOUT_SECS
    while time.monotonic() < deadline:
        status_payload = task_status(task_id)
        items = status_payload.get("items")
        assert isinstance(items, list) and items, "editable task status returned no item"
        item = items[0]
        assert isinstance(item, dict)
        state = item.get("status")
        if state == "error":
            raise AssertionError("PPT editable task reached error terminal state")
        if state == "success":
            result = item.get("result")
            assert isinstance(result, dict)
            assert isinstance(result.get("primary_url"), str) and result["primary_url"]
            assert isinstance(result.get("zip_url"), str) and result["zip_url"]
            return item
        assert state in {"queued", "running"}, f"unexpected PPT task status: {state!r}"
        time.sleep(POLL_INTERVAL_SECS)
    raise AssertionError("PPT editable task timed out")


@pytest.mark.live_upstream
def test_ppt_generation_has_success_terminal_and_download_urls():
    task = request_json(
        "POST",
        "/v1/ppt/generations",
        {
            "client_task_id": f"live-ppt-{uuid4().hex}",
            "prompt": PROMPT,
            "base64_images": [],
        },
        base_url=live_base_url(),
        timeout=60,
    )
    task_id = str(task.get("taskId") or task.get("id") or "")
    assert task_id
    _wait_for_success(task_id)
