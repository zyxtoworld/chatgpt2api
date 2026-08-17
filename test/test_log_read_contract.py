from __future__ import annotations

import json
import os
from pathlib import Path
from unittest import mock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import api.system as system_module
import services.log_service as log_module
import services.secure_file as secure_file
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


def test_legacy_log_reads_drop_unknown_top_level_fields(tmp_path: Path) -> None:
    canary = "legacy-top-level-log-secret owner@example.com"
    path = tmp_path / "logs.jsonl"
    path.write_text(
        json.dumps(
            {
                "id": "legacy-top-level-1",
                "time": "2026-08-09 00:00:00",
                "type": "call",
                "summary": "legacy",
                "detail": {},
                "access_token": canary,
                "internal_metadata": {"secret": canary},
            },
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )

    payload = LogService(path).list()

    serialized = json.dumps(payload, ensure_ascii=False)
    assert canary not in serialized
    assert "access_token" not in payload[0]
    assert "internal_metadata" not in payload[0]


def test_legacy_log_reads_drop_unknown_nested_detail_fields(tmp_path: Path) -> None:
    canary = "legacy-nested-log-secret owner@example.com"
    path = tmp_path / "logs.jsonl"
    path.write_text(
        json.dumps(
            {
                "id": "legacy-nested-1",
                "time": "2026-08-09 00:00:00",
                "type": "call",
                "summary": "legacy",
                "detail": {
                    "endpoint": "/v1/models",
                    "internal_metadata": {"secret": canary},
                    "nested": {"opaque": canary},
                },
            },
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )

    payload = LogService(path).list()

    serialized = json.dumps(payload, ensure_ascii=False)
    assert canary not in serialized
    assert payload[0]["detail"] == {"endpoint": "/v1/models"}


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


def test_log_compaction_handles_single_entry_larger_than_retention_budget(tmp_path: Path) -> None:
    path = tmp_path / "logs.jsonl"
    service = LogService(path, max_bytes=512, retain_bytes=0)
    path.write_bytes(b"oversized legacy payload\n" + b"x" * 900)

    service.add("call", "x" * 10_000)

    assert path.stat().st_size <= 512
    items = service.list(limit=2)
    assert len(items) == 1
    assert items[0]["summary"] == "entry truncated"


def test_log_add_commits_compaction_and_new_entry_as_one_replace(tmp_path: Path) -> None:
    path = tmp_path / "logs.jsonl"
    service = LogService(path, max_bytes=300, retain_bytes=100)
    original = b"legacy-line\n" * 40
    path.write_bytes(original)

    with mock.patch.object(log_module, "append_checked_file_bytes", side_effect=OSError("append failed")) as append:
        service.add("call", "new-entry")

    append.assert_not_called()
    assert path.read_bytes() != original
    assert service.list(limit=1)[0]["summary"] == "new-entry"


def test_log_add_keeps_original_snapshot_when_atomic_commit_fails(tmp_path: Path) -> None:
    path = tmp_path / "logs.jsonl"
    service = LogService(path, max_bytes=300, retain_bytes=100)
    original = b"legacy-line\n" * 40
    path.write_bytes(original)

    with mock.patch.object(log_module, "atomic_write_bytes", side_effect=OSError("replace failed")):
        with pytest.raises(OSError, match="replace failed"):
            service.add("call", "new-entry")

    assert path.read_bytes() == original


def test_log_list_does_not_load_the_entire_file_with_path_read_text(tmp_path: Path) -> None:
    path = tmp_path / "logs.jsonl"
    service = LogService(path)
    for index in range(250):
        service.add("call", f"item-{index}")

    with mock.patch.object(Path, "read_text", side_effect=AssertionError("whole-file read")):
        items = service.list(limit=10)

    assert [item["summary"] for item in items] == [f"item-{index}" for index in range(249, 239, -1)]


def test_log_list_does_not_read_replaced_snapshot_path(tmp_path: Path) -> None:
    path = tmp_path / "logs.jsonl"
    replacement = tmp_path / "replacement.jsonl"
    displaced = tmp_path / "displaced.jsonl"
    path.write_text(
        json.dumps({"id": "original", "time": "2026-08-09 00:00:00", "type": "call", "summary": "original"}) + "\n",
        encoding="utf-8",
    )
    replacement.write_text(
        json.dumps({"id": "replaced", "time": "2026-08-09 00:00:00", "type": "call", "summary": "replaced"}) + "\n",
        encoding="utf-8",
    )
    original_open = Path.open

    def replace_before_open(path_obj, *args, **kwargs):
        if path_obj == path:
            path.replace(displaced)
            replacement.replace(path)
        return original_open(path_obj, *args, **kwargs)

    with mock.patch.object(Path, "open", autospec=True, side_effect=replace_before_open):
        items = LogService(path).list()

    assert [item["summary"] for item in items] == ["original"]


def test_log_list_uses_fixed_handle_after_path_replacement(tmp_path: Path) -> None:
    path = tmp_path / "logs.jsonl"
    replacement = tmp_path / "replacement.jsonl"
    displaced = tmp_path / "displaced.jsonl"
    path.write_text(
        json.dumps({"id": "original", "time": "2026-08-09 00:00:00", "type": "call", "summary": "original"}) + "\n",
        encoding="utf-8",
    )
    replacement.write_text(
        json.dumps({"id": "replaced", "time": "2026-08-09 00:00:00", "type": "call", "summary": "replaced"}) + "\n",
        encoding="utf-8",
    )
    original_open = log_module.open_checked_file

    def replace_after_open(path_obj, *args, **kwargs):
        opened = original_open(path_obj, *args, **kwargs)
        if path_obj == path:
            path.replace(displaced)
            replacement.replace(path)
        return opened

    with mock.patch.object(log_module, "open_checked_file", side_effect=replace_after_open):
        items = LogService(path).list()

    assert [item["summary"] for item in items] == ["original"]


def test_log_list_rejects_path_replacement_before_open(tmp_path: Path) -> None:
    path = tmp_path / "logs.jsonl"
    replacement = tmp_path / "replacement.jsonl"
    displaced = tmp_path / "displaced.jsonl"
    path.write_text(
        json.dumps({"id": "original", "time": "2026-08-09 00:00:00", "type": "call", "summary": "original"})
        + "\n",
        encoding="utf-8",
    )
    replacement.write_text(
        json.dumps({"id": "foreign", "time": "2026-08-09 00:00:00", "type": "call", "summary": "foreign"})
        + "\n",
        encoding="utf-8",
    )
    original_open = log_module.open_checked_file

    def replace_before_open(path_obj, *args, **kwargs):
        if path_obj == path:
            path.replace(displaced)
            replacement.replace(path)
        return original_open(path_obj, *args, **kwargs)

    with (
        mock.patch.object(log_module, "open_checked_file", side_effect=replace_before_open),
        pytest.raises(OSError, match="log file changed"),
    ):
        LogService(path).list()

    assert "foreign" in path.read_text(encoding="utf-8")
    assert "original" in displaced.read_text(encoding="utf-8")


def test_log_list_fails_closed_when_parent_directory_is_rebound_before_open(
    tmp_path: Path,
) -> None:
    original_dir = tmp_path / "logs"
    original_dir.mkdir()
    path = original_dir / "logs.jsonl"
    path.write_text(
        json.dumps({"id": "original", "time": "2026-08-09 00:00:00", "type": "call", "summary": "original"})
        + "\n",
        encoding="utf-8",
    )
    moved_dir = tmp_path / "moved-logs"
    service = LogService(path)
    original_authorized_root = secure_file.authorized_root
    rebound = False

    def rebind_after_root_authorization(root: Path) -> Path:
        nonlocal rebound
        authorized = original_authorized_root(root)
        if not rebound and root == original_dir:
            rebound = True
            original_dir.rename(moved_dir)
            original_dir.mkdir()
            (original_dir / "logs.jsonl").write_text(
                json.dumps({"id": "foreign", "time": "2026-08-09 00:00:00", "type": "call", "summary": "foreign"})
                + "\n",
                encoding="utf-8",
            )
        return authorized

    with (
        mock.patch.object(secure_file, "authorized_root", side_effect=rebind_after_root_authorization),
        pytest.raises(OSError, match="log directory changed"),
    ):
        service.list()

    assert rebound
    assert "original" in (moved_dir / "logs.jsonl").read_text(encoding="utf-8")


def test_log_add_does_not_append_to_replaced_snapshot_path(tmp_path: Path) -> None:
    path = tmp_path / "logs.jsonl"
    replacement = tmp_path / "replacement.jsonl"
    displaced = tmp_path / "displaced.jsonl"
    path.write_text(
        json.dumps({"id": "original", "time": "2026-08-09 00:00:00", "type": "call", "summary": "original"}) + "\n",
        encoding="utf-8",
    )
    replacement.write_text(
        json.dumps({"id": "foreign", "time": "2026-08-09 00:00:00", "type": "call", "summary": "foreign"}) + "\n",
        encoding="utf-8",
    )
    original_open = Path.open

    def replace_before_open(path_obj, *args, **kwargs):
        if path_obj == path:
            path.replace(displaced)
            replacement.replace(path)
        return original_open(path_obj, *args, **kwargs)

    with mock.patch.object(Path, "open", autospec=True, side_effect=replace_before_open):
        LogService(path).add("call", "new")

    assert "foreign" not in path.read_text(encoding="utf-8")


def test_log_add_writes_fixed_handle_after_path_replacement(tmp_path: Path) -> None:
    path = tmp_path / "logs.jsonl"
    replacement = tmp_path / "replacement.jsonl"
    displaced = tmp_path / "displaced.jsonl"
    path.write_text(
        json.dumps({"id": "original", "time": "2026-08-09 00:00:00", "type": "call", "summary": "original"}) + "\n",
        encoding="utf-8",
    )
    replacement.write_text(
        json.dumps({"id": "foreign", "time": "2026-08-09 00:00:00", "type": "call", "summary": "foreign"}) + "\n",
        encoding="utf-8",
    )
    helper_name = "_open_windows_append_file" if os.name == "nt" else "_open_posix_append_file"
    original_open = getattr(secure_file, helper_name)

    def replace_after_open(path_obj, *args, **kwargs):
        opened = original_open(path_obj, *args, **kwargs)
        if path_obj == path:
            path.replace(displaced)
            replacement.replace(path)
        return opened

    with mock.patch.object(secure_file, helper_name, side_effect=replace_after_open):
        LogService(path).add("call", "new")

    assert "foreign" in path.read_text(encoding="utf-8")
    assert "new" in displaced.read_text(encoding="utf-8")


def test_log_add_fails_closed_when_parent_directory_is_rebound_before_append(tmp_path: Path) -> None:
    original_dir = tmp_path / "logs"
    original_dir.mkdir()
    path = original_dir / "logs.jsonl"
    path.write_text(
        json.dumps({"id": "old", "time": "2026-01-01 00:00:00", "type": "call", "summary": "old"})
        + "\n",
        encoding="utf-8",
    )
    moved_dir = tmp_path / "moved-logs"
    service = LogService(path)
    original_append = log_module.append_checked_file_bytes
    rebound = False

    def rebind_parent(path_obj: Path, root: Path, payload: bytes, **kwargs: object) -> None:
        nonlocal rebound
        original_dir.rename(moved_dir)
        rebound = True
        original_dir.mkdir()
        path.write_text(
            json.dumps({"id": "foreign", "time": "2026-01-01 00:00:00", "type": "call", "summary": "foreign"})
            + "\n",
            encoding="utf-8",
        )
        original_append(path_obj, root, payload, **kwargs)

    with (
        mock.patch.object(log_module, "append_checked_file_bytes", rebind_parent),
        pytest.raises(OSError),
    ):
        service.add("call", "new")

    if rebound:
        assert "foreign" in path.read_text(encoding="utf-8")
        assert "old" in (moved_dir / "logs.jsonl").read_text(encoding="utf-8")
    else:
        assert "old" in path.read_text(encoding="utf-8")


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


def test_log_delete_does_not_read_replaced_snapshot_path(tmp_path: Path) -> None:
    path = tmp_path / "logs.jsonl"
    replacement = tmp_path / "replacement.jsonl"
    displaced = tmp_path / "displaced.jsonl"
    path.write_text(
        json.dumps({"id": "original", "time": "2026-08-09 00:00:00", "type": "call", "summary": "original"}) + "\n",
        encoding="utf-8",
    )
    replacement.write_text(
        json.dumps({"id": "victim", "time": "2026-08-09 00:00:00", "type": "call", "summary": "victim"}) + "\n",
        encoding="utf-8",
    )
    original_open = Path.open

    def replace_before_open(path_obj, *args, **kwargs):
        if path_obj == path:
            path.replace(displaced)
            replacement.replace(path)
        return original_open(path_obj, *args, **kwargs)

    with mock.patch.object(Path, "open", autospec=True, side_effect=replace_before_open):
        LogService(path).delete(["victim"])

    assert "original" in path.read_text(encoding="utf-8")


def test_log_delete_fails_closed_after_fixed_handle_path_replacement(tmp_path: Path) -> None:
    path = tmp_path / "logs.jsonl"
    replacement = tmp_path / "replacement.jsonl"
    displaced = tmp_path / "displaced.jsonl"
    path.write_text(
        json.dumps({"id": "original", "time": "2026-08-09 00:00:00", "type": "call", "summary": "original"}) + "\n",
        encoding="utf-8",
    )
    replacement.write_text(
        json.dumps({"id": "victim", "time": "2026-08-09 00:00:00", "type": "call", "summary": "victim"}) + "\n",
        encoding="utf-8",
    )
    original_open = log_module.open_checked_file

    def replace_after_open(path_obj, *args, **kwargs):
        opened = original_open(path_obj, *args, **kwargs)
        if path_obj == path:
            path.replace(displaced)
            replacement.replace(path)
        return opened

    with (
        mock.patch.object(log_module, "open_checked_file", side_effect=replace_after_open),
        pytest.raises(OSError, match="log file changed"),
    ):
        LogService(path).delete(["victim"])

    assert "victim" in path.read_text(encoding="utf-8")


def test_log_delete_rejects_path_replacement_before_open(tmp_path: Path) -> None:
    path = tmp_path / "logs.jsonl"
    replacement = tmp_path / "replacement.jsonl"
    displaced = tmp_path / "displaced.jsonl"
    path.write_text(
        json.dumps({"id": "victim", "time": "2026-01-01 00:00:00", "type": "call", "summary": "victim"})
        + "\n",
        encoding="utf-8",
    )
    replacement.write_text(
        json.dumps({"id": "foreign", "time": "2026-01-01 00:00:00", "type": "call", "summary": "foreign"})
        + "\n",
        encoding="utf-8",
    )
    original_open = log_module.open_checked_file

    def replace_before_open(path_obj, *args, **kwargs):
        if path_obj == path:
            path.replace(displaced)
            replacement.replace(path)
        return original_open(path_obj, *args, **kwargs)

    with (
        mock.patch.object(log_module, "open_checked_file", side_effect=replace_before_open),
        pytest.raises(OSError, match="log file changed"),
    ):
        LogService(path).delete(["victim"])

    assert "foreign" in path.read_text(encoding="utf-8")
    assert "victim" in displaced.read_text(encoding="utf-8")


def test_log_delete_fails_closed_when_parent_directory_is_rebound_before_replace(tmp_path: Path) -> None:
    original_dir = tmp_path / "logs"
    original_dir.mkdir()
    path = original_dir / "logs.jsonl"
    path.write_text(
        json.dumps({"id": "victim", "time": "2026-01-01 00:00:00", "type": "call", "summary": "victim"})
        + "\n",
        encoding="utf-8",
    )
    moved_dir = tmp_path / "moved-logs"
    service = LogService(path)
    original_replace = log_module.LogService._atomic_replace_bytes_locked
    rebound = False

    def rebind_parent(self: LogService, payload: bytes) -> None:
        nonlocal rebound
        original_dir.rename(moved_dir)
        rebound = True
        original_dir.mkdir()
        path.write_text(
            json.dumps({"id": "foreign", "time": "2026-01-01 00:00:00", "type": "call", "summary": "foreign"})
            + "\n",
            encoding="utf-8",
        )
        original_replace(self, payload)

    with (
        mock.patch.object(log_module.LogService, "_atomic_replace_bytes_locked", rebind_parent),
        pytest.raises(OSError),
    ):
        service.delete(["victim"])

    if rebound:
        assert "foreign" in path.read_text(encoding="utf-8")
        assert "victim" in (moved_dir / "logs.jsonl").read_text(encoding="utf-8")
    else:
        assert "victim" in path.read_text(encoding="utf-8")


def test_log_delete_does_not_replace_a_new_file_after_identity_check(tmp_path: Path) -> None:
    path = tmp_path / "logs.jsonl"
    displaced = tmp_path / "displaced.jsonl"
    path.write_text(
        json.dumps({"id": "victim", "time": "2026-01-01 00:00:00", "type": "call", "summary": "victim"})
        + "\n",
        encoding="utf-8",
    )
    service = LogService(path)
    original_assert = log_module._assert_log_path_unchanged
    rebound = False

    def rebind_after_identity_check(path_obj: Path, opened: object) -> None:
        nonlocal rebound
        original_assert(path_obj, opened)
        if not rebound:
            rebound = True
            path.replace(displaced)
            path.write_text(
                json.dumps(
                    {"id": "foreign", "time": "2026-01-01 00:00:00", "type": "call", "summary": "foreign"}
                )
                + "\n",
                encoding="utf-8",
            )

    with mock.patch.object(log_module, "_assert_log_path_unchanged", side_effect=rebind_after_identity_check):
        with pytest.raises(OSError, match="log file changed"):
            service.delete(["victim"])

    assert rebound
    assert "foreign" in path.read_text(encoding="utf-8")
    assert "victim" in displaced.read_text(encoding="utf-8")


def test_log_compaction_fails_closed_after_fixed_handle_path_replacement(tmp_path: Path) -> None:
    path = tmp_path / "logs.jsonl"
    replacement = tmp_path / "replacement.jsonl"
    displaced = tmp_path / "displaced.jsonl"
    original_lines = "".join(
        json.dumps({"id": str(index), "time": "2026-08-09 00:00:00", "type": "call", "summary": "original"}) + "\n"
        for index in range(20)
    )
    path.write_text(original_lines, encoding="utf-8")
    replacement.write_text(
        json.dumps({"id": "foreign", "time": "2026-08-09 00:00:00", "type": "call", "summary": "foreign"}) + "\n",
        encoding="utf-8",
    )
    original_open = log_module.open_checked_file

    def replace_after_open(path_obj, *args, **kwargs):
        opened = original_open(path_obj, *args, **kwargs)
        if path_obj == path:
            path.replace(displaced)
            replacement.replace(path)
        return opened

    with (
        mock.patch.object(log_module, "open_checked_file", side_effect=replace_after_open),
        pytest.raises(OSError, match="log file changed"),
    ):
        LogService(path, max_bytes=200, retain_bytes=100).add("call", "new")

    assert "foreign" in path.read_text(encoding="utf-8")


def test_log_compaction_rejects_path_replacement_before_open(tmp_path: Path) -> None:
    path = tmp_path / "logs.jsonl"
    replacement = tmp_path / "replacement.jsonl"
    displaced = tmp_path / "displaced.jsonl"
    original_lines = "".join(
        json.dumps({"id": str(index), "time": "2026-08-09 00:00:00", "type": "call", "summary": "original"})
        + "\n"
        for index in range(20)
    )
    path.write_text(original_lines, encoding="utf-8")
    replacement.write_text(
        json.dumps({"id": "foreign", "time": "2026-08-09 00:00:00", "type": "call", "summary": "foreign"})
        + "\n",
        encoding="utf-8",
    )
    original_open = log_module.open_checked_file

    def replace_before_open(path_obj, *args, **kwargs):
        if path_obj == path:
            path.replace(displaced)
            replacement.replace(path)
        return original_open(path_obj, *args, **kwargs)

    with (
        mock.patch.object(log_module, "open_checked_file", side_effect=replace_before_open),
        pytest.raises(OSError, match="log file changed"),
    ):
        LogService(path, max_bytes=200, retain_bytes=100).add("call", "new")

    assert "foreign" in path.read_text(encoding="utf-8")
    assert "original" in displaced.read_text(encoding="utf-8")


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
        mock.patch.object(log_module, "atomic_write_bytes", side_effect=OSError("replace failed")),
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
