"""Opt-in live contract for the PSD editable-file lifecycle."""

from __future__ import annotations

import base64
import os
import time
from uuid import uuid4

import pytest

from test.fixtures.image_inputs import image_fixture_bytes
from test.live_contract_support import live_base_url, request_json, task_status


PROMPT = "按原图位置拆分海报元素并合成可编辑 PSD，同时输出每个图层素材 zip。"
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
            raise AssertionError("PSD editable task reached error terminal state")
        if state == "success":
            result = item.get("result")
            assert isinstance(result, dict)
            assert isinstance(result.get("primary_url"), str) and result["primary_url"]
            assert isinstance(result.get("zip_url"), str) and result["zip_url"]
            return item
        assert state in {"queued", "running"}, f"unexpected PSD task status: {state!r}"
        time.sleep(POLL_INTERVAL_SECS)
    raise AssertionError("PSD editable task timed out")


@pytest.mark.live_upstream
def test_psd_generation_has_success_terminal_and_download_urls():
    image_data_url = "data:image/png;base64," + base64.b64encode(image_fixture_bytes("image.png")).decode("ascii")
    task = request_json(
        "POST",
        "/v1/psd/generations",
        {
            "client_task_id": f"live-psd-{uuid4().hex}",
            "prompt": PROMPT,
            "base64_images": [image_data_url],
        },
        base_url=live_base_url(),
        timeout=60,
    )
    task_id = str(task.get("taskId") or task.get("id") or "")
    assert task_id
    _wait_for_success(task_id)
