from __future__ import annotations

import json
import multiprocessing as mp
import os
from datetime import UTC, datetime
from pathlib import Path
from unittest import mock

import services.backup_service as backup_module
import services.config as config_module
from services.backup_service import BackupError, BackupService


_ROLE = ""
_A_UPLOADED = None
_B_FINISHED = None


class LocalObjectClient:
    def __init__(self, settings: dict[str, object]) -> None:
        self.root = Path(str(settings["object_root"]))
        self.prefix = str(settings.get("prefix") or "backups")

    def validate(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)

    def upload_bytes(self, key: str, payload: bytes, *, content_type: str, metadata: dict[str, str]):
        path = self.root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
        if _ROLE == "A" and _A_UPLOADED is not None:
            _A_UPLOADED.set()
        return {"key": key, "etag": "local"}

    def list_objects(self):
        items = []
        if not self.root.exists():
            return items
        for path in self.root.rglob("backup-*.tar.gz"):
            items.append({"key": path.relative_to(self.root).as_posix(), "size": path.stat().st_size})
        return items

    def delete_object(self, key: str) -> None:
        path = self.root / key
        if path.exists():
            path.unlink()

    def close(self) -> None:
        return None


def _configure_child(root: str, role: str, key: str | None):
    global _ROLE
    _ROLE = role
    state_path = Path(root) / "data" / "backup_state.json"
    object_root = Path(root) / "objects"
    config_module.BACKUP_STATE_FILE = state_path
    backup_module.config.get_backup_settings = lambda: {
        "prefix": "backups",
        "encrypt": False,
        "rotation_keep": 1,
        "object_root": str(object_root),
    }
    backup_module._utc_now = lambda: datetime(2026, 8, 17, tzinfo=UTC)
    if key is None:
        class FixedUuid:
            def __init__(self, value: str) -> None:
                self.hex = value

        operation_tag = role.lower() * 32
        backup_module.uuid.uuid4 = lambda: FixedUuid(operation_tag)
    else:
        backup_module._new_backup_object_key = lambda _settings: key
    backup_module.CloudflareR2Client = LocalObjectClient
    return object_root


def _concurrent_worker(
    root: str,
    role: str,
    key: str | None,
    a_uploaded,
    b_finished,
    result_queue,
) -> None:
    global _A_UPLOADED, _B_FINISHED
    _A_UPLOADED = a_uploaded
    _B_FINISHED = b_finished
    _configure_child(root, role, key)
    service = BackupService()
    service._build_backup_archive = lambda _settings, trigger: b"payload"
    if role == "A":
        original_rotation = service._apply_rotation

        def fail_after_rotation(client, keep, *, settings, current_object_key):
            if not b_finished.wait(10):
                raise AssertionError("B did not finish")
            original_rotation(
                client,
                keep,
                settings=settings,
                current_object_key=current_object_key,
            )
            raise BackupError("late A failure", code="r2_upload_failed", status_code=503)

        service._apply_rotation = fail_after_rotation
    try:
        service.run_backup()
        result_queue.put((role, "success"))
    except BaseException as exc:
        result_queue.put((role, type(exc).__name__, getattr(exc, "code", "")))


def _crash_worker(root: str, key: str) -> None:
    _configure_child(root, "A", key)
    service = BackupService()
    service._build_backup_archive = lambda _settings, trigger: b"payload"

    def crash_after_upload(client, keep, *, settings, current_object_key):
        os._exit(0)

    service._apply_rotation = crash_after_upload
    service.run_backup()


def _recover_worker(root: str, key: str, result_queue) -> None:
    _configure_child(root, "B", key)
    service = BackupService()
    service._build_backup_archive = lambda _settings, trigger: b"payload"
    try:
        service.run_backup()
        result_queue.put("success")
    except BaseException as exc:
        result_queue.put((type(exc).__name__, getattr(exc, "code", "")))


def test_spawn_processes_use_distinct_keys_and_stale_a_cannot_overwrite_b(tmp_path: Path) -> None:
    ctx = mp.get_context("spawn")
    a_uploaded = ctx.Event()
    b_finished = ctx.Event()
    result_queue = ctx.Queue()
    root = str(tmp_path)
    process_a = ctx.Process(
        target=_concurrent_worker,
        args=(root, "A", None, a_uploaded, b_finished, result_queue),
    )
    process_b = ctx.Process(
        target=_concurrent_worker,
        args=(root, "B", None, a_uploaded, b_finished, result_queue),
    )
    process_a.start()
    assert a_uploaded.wait(10)
    process_b.start()
    process_b.join(10)
    assert not process_b.is_alive()
    b_finished.set()
    process_a.join(10)
    assert not process_a.is_alive()
    results = {}
    for _ in range(2):
        result = result_queue.get(timeout=2)
        results[result[0]] = result[1:]
    assert results["B"] == ("success",)
    assert results["A"] == ("BackupError", "r2_upload_failed")
    state = json.loads((tmp_path / "data" / "backup_state.json").read_text(encoding="utf-8"))
    assert state["last_status"] == "success"
    assert state["last_object_key"].endswith("b" * 32 + ".tar.gz")
    object_keys = sorted(
        path.relative_to(tmp_path / "objects").as_posix()
        for path in (tmp_path / "objects").rglob("backup-*.tar.gz")
    )
    assert len(object_keys) == 2
    assert object_keys[0] != object_keys[1]
    assert state["last_object_key"] in object_keys


def test_spawn_crash_recovery_gets_new_owner_without_reusing_stale_key(tmp_path: Path) -> None:
    ctx = mp.get_context("spawn")
    root = str(tmp_path)
    stale_key = "backups/backup-stale-a.tar.gz"
    process_a = ctx.Process(target=_crash_worker, args=(root, stale_key))
    process_a.start()
    process_a.join(10)
    assert not process_a.is_alive()
    assert process_a.exitcode == 0

    result_queue = ctx.Queue()
    process_b = ctx.Process(
        target=_recover_worker,
        args=(root, "backups/backup-recovered-b.tar.gz", result_queue),
    )
    process_b.start()
    process_b.join(10)
    assert not process_b.is_alive()
    assert result_queue.get(timeout=2) == "success"
    state = json.loads((tmp_path / "data" / "backup_state.json").read_text(encoding="utf-8"))
    assert state["last_object_key"].endswith("recovered-b.tar.gz")
    assert state["last_object_key"] != stale_key
    assert (tmp_path / "objects" / stale_key).exists()

    process_c = ctx.Process(
        target=_recover_worker,
        args=(root, "backups/backup-recovered-c.tar.gz", result_queue),
    )
    process_c.start()
    process_c.join(10)
    assert not process_c.is_alive()
    assert result_queue.get(timeout=2) == "success"
    assert not (tmp_path / "objects" / stale_key).exists()
