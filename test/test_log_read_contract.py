from __future__ import annotations

import json
from pathlib import Path
from unittest import mock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import api.system as system_module
from api.system import create_router as create_system_router
from services.log_service import LogService


SECRET = "legacy-log-access-token opaque-secret"


def test_legacy_log_reads_do_not_return_credential_fields(tmp_path: Path) -> None:
    path = tmp_path / "logs.jsonl"
    path.write_text(
        json.dumps(
            {
                "id": "legacy-1",
                "time": "2026-08-09 00:00:00",
                "type": "call",
                "summary": "legacy",
                "detail": {
                    "token": SECRET,
                    "access_token": SECRET,
                    "email": "owner@example.com",
                    "error": SECRET,
                    "nested": {"refresh_token": SECRET},
                },
            },
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )

    payload = LogService(path).list()

    serialized = json.dumps(payload, ensure_ascii=False)
    assert SECRET not in serialized
    assert "owner@example.com" not in serialized


def test_logs_api_uses_the_sanitized_legacy_log_projection(tmp_path: Path) -> None:
    path = tmp_path / "logs.jsonl"
    path.write_text(
        json.dumps(
            {
                "id": "legacy-api-1",
                "time": "2026-08-09 00:00:00",
                "type": "call",
                "summary": "legacy",
                "detail": {"error": SECRET, "token": SECRET},
            }
        )
        + "\n",
        encoding="utf-8",
    )
    app = FastAPI()
    app.include_router(create_system_router("test"))

    with (
        mock.patch.object(system_module, "require_admin_async", return_value={"role": "admin"}),
        mock.patch.object(system_module, "log_service", LogService(path)),
    ):
        response = TestClient(app).get("/api/logs")

    assert response.status_code == 200, response.text
    assert SECRET not in response.text


def test_log_retention_bounds_file_growth_and_keeps_newest_entries(tmp_path: Path) -> None:
    path = tmp_path / "logs.jsonl"
    service = LogService(path, max_bytes=1024, retain_bytes=512)

    for index in range(100):
        service.add("call", f"item-{index}", {"payload": "x" * 64})

    assert path.stat().st_size <= 1024
    items = service.list(limit=200)
    assert items
    assert items[0]["summary"] == "item-99"
    assert len(items) < 100


def test_log_list_does_not_load_the_entire_file_with_path_read_text(tmp_path: Path) -> None:
    path = tmp_path / "logs.jsonl"
    service = LogService(path)
    for index in range(250):
        service.add("call", f"item-{index}")

    with mock.patch.object(Path, "read_text", side_effect=AssertionError("whole-file read")):
        items = service.list(limit=10)

    assert [item["summary"] for item in items] == [f"item-{index}" for index in range(249, 239, -1)]


def test_legacy_log_id_returned_by_tail_reader_can_be_deleted(tmp_path: Path) -> None:
    path = tmp_path / "logs.jsonl"
    path.write_text(
        json.dumps({"time": "2026-08-09 00:00:00", "type": "call", "summary": "第一条"}, ensure_ascii=False)
        + "\n"
        + json.dumps({"time": "2026-08-09 00:00:01", "type": "call", "summary": "second"})
        + "\n",
        encoding="utf-8",
    )
    service = LogService(path)
    listed = service.list()
    second_id = next(item["id"] for item in listed if item["summary"] == "second")

    result = service.delete([second_id])

    assert result == {"removed": 1}
    assert [item["summary"] for item in service.list()] == ["第一条"]


def test_log_compaction_replace_failure_preserves_existing_file(tmp_path: Path) -> None:
    path = tmp_path / "logs.jsonl"
    line = (
        json.dumps(
            {"id": "old", "time": "2026-08-09 00:00:00", "type": "call", "summary": "old"}
        )
        + "\n"
    ).encode("utf-8")
    original = line * 20
    path.write_bytes(original)
    service = LogService(path, max_bytes=1024, retain_bytes=512)

    with (
        mock.patch("services.log_service.os.replace", side_effect=OSError("replace failed")),
        pytest.raises(OSError, match="replace failed"),
    ):
        service.add("call", "new")

    assert path.read_bytes() == original
    assert list(tmp_path.glob(".logs.jsonl.*.tmp")) == []


def test_log_tail_reader_handles_one_line_larger_than_the_read_chunk(tmp_path: Path) -> None:
    path = tmp_path / "logs.jsonl"
    service = LogService(path)
    long_summary = "长" * 70_000
    service.add("call", long_summary)
    service.add("call", "newest")

    items = service.list(limit=2)

    assert [item["summary"] for item in items] == ["newest", long_summary]
