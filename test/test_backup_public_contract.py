from __future__ import annotations

import io
import json
import tarfile
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

from fastapi import FastAPI
from fastapi.testclient import TestClient

import api.system as system_module
import services.backup_service as backup_module
import services.config as config_module
import services.secure_file as secure_file
from api.system import create_router
from services.backup_service import BackupError, BackupService


AUTH_HEADERS = {"Authorization": "Bearer chatgpt2api"}


class BackupPublicErrorContractTests(unittest.TestCase):
    def test_backup_archive_over_download_budget_is_rejected_before_upload(self) -> None:
        service = BackupService()
        uploaded: list[bytes] = []

        class FakeClient:
            prefix = "backups"

            def validate(self) -> None:
                return None

            def upload_bytes(self, _key, payload, *, content_type, metadata):
                uploaded.append(payload)
                return {"key": "backups/backup-test.tar.gz"}

            def close(self) -> None:
                return None

        with (
            mock.patch.object(
                backup_module.config,
                "get_backup_settings",
                return_value={"prefix": "backups", "encrypt": False},
            ),
            mock.patch.object(backup_module, "CloudflareR2Client", return_value=FakeClient()),
            mock.patch.object(service, "_build_backup_archive", return_value=b"12345"),
            mock.patch.object(backup_module, "_MAX_R2_DOWNLOAD_BYTES", 4),
        ):
            with self.assertRaises(BackupError) as raised:
                service._run_backup_once(trigger="test")

        self.assertEqual(raised.exception.code, "backup_archive_too_large")
        self.assertEqual(uploaded, [])

    def test_backup_archive_builder_is_capped_before_returning_bytes(self) -> None:
        service = BackupService()
        with mock.patch.object(backup_module, "_MAX_R2_DOWNLOAD_BYTES", 4):
            with self.assertRaises(BackupError) as raised:
                service._build_backup_archive({"include": {}}, trigger="test")
        self.assertEqual(raised.exception.code, "backup_archive_too_large")

    def test_backup_run_uses_one_settings_snapshot_when_encryption_changes(self) -> None:
        service = BackupService()
        object_key = "backups/backup-consistent.tar.gz"
        settings = {"prefix": "backups", "encrypt": False, "passphrase": "stable-passphrase"}
        state: dict[str, object] = {}
        entered_settings_read = threading.Event()
        release_settings_read = threading.Event()
        uploaded: dict[str, object] = {}
        def get_settings() -> dict[str, object]:
            return dict(settings)

        class FakeClient:
            prefix = "backups"

            def validate(self) -> None:
                return None

            def upload_bytes(self, key, payload, *, content_type, metadata):
                uploaded.update(key=key, payload=payload, metadata=metadata)
                return {"key": key}

            def close(self) -> None:
                return None

        service._apply_rotation = mock.Mock()
        original_run_once = service._run_backup_once

        def gated_run_once(*, trigger: str, object_key: str | None = None) -> dict[str, object]:
            entered_settings_read.set()
            self.assertTrue(release_settings_read.wait(2))
            return original_run_once(trigger=trigger, object_key=object_key)

        service._run_backup_once = gated_run_once
        worker_errors: list[BaseException] = []

        def run() -> None:
            try:
                service.run_backup()
            except BaseException as exc:
                worker_errors.append(exc)

        with (
            mock.patch.object(backup_module.config, "get_backup_settings", side_effect=get_settings),
            mock.patch.object(backup_module, "_new_backup_object_key", return_value=object_key),
            mock.patch.object(backup_module, "load_backup_state", side_effect=lambda: dict(state)),
            mock.patch.object(backup_module, "save_backup_state", side_effect=lambda payload: state.update(payload)),
            mock.patch.object(backup_module, "CloudflareR2Client", return_value=FakeClient()),
            mock.patch.object(backup_module, "_openssl_encrypt", return_value=b"cipher-payload"),
        ):
            service._build_backup_archive = mock.Mock(return_value=b"plain-payload")
            worker = threading.Thread(target=run, name="backup-settings-test")
            worker.start()
            self.assertTrue(entered_settings_read.wait(2))
            settings["encrypt"] = True
            release_settings_read.set()
            worker.join(timeout=2)

        self.assertFalse(worker.is_alive())
        self.assertEqual(worker_errors, [])
        self.assertEqual(uploaded["key"], object_key)
        self.assertEqual(uploaded["payload"], b"plain-payload")
        self.assertEqual(uploaded["metadata"]["encrypted"], "false")

    def test_backup_rotation_uses_operation_settings_after_configuration_changes(self) -> None:
        service = BackupService()
        object_key = "old-prefix/backup-current.tar.gz"
        settings = {
            "prefix": "old-prefix",
            "encrypt": False,
            "rotation_keep": 1,
        }
        state: dict[str, object] = {}
        deleted: list[str] = []

        class FakeClient:
            prefix = "old-prefix"

            def validate(self) -> None:
                return None

            def upload_bytes(self, key, payload, *, content_type, metadata):
                settings["prefix"] = "new-prefix"
                settings["encrypt"] = True
                return {"key": key}

            def list_objects(self):
                return [
                    {"key": object_key, "size": 1, "updated_at": "2026-08-16T00:00:02Z"},
                    {"key": "old-prefix/backup-old.tar.gz", "size": 1, "updated_at": "2026-08-16T00:00:01Z"},
                ]

            def delete_object(self, key: str) -> None:
                deleted.append(key)

            def close(self) -> None:
                return None

        with (
            mock.patch.object(backup_module.config, "get_backup_settings", side_effect=lambda: dict(settings)),
            mock.patch.object(backup_module, "_new_backup_object_key", return_value=object_key),
            mock.patch.object(backup_module, "load_backup_state", side_effect=lambda: dict(state)),
            mock.patch.object(backup_module, "save_backup_state", side_effect=lambda payload: state.update(payload)),
            mock.patch.object(backup_module, "CloudflareR2Client", return_value=FakeClient()),
        ):
            service._build_backup_archive = mock.Mock(return_value=b"plain-payload")
            result = service.run_backup()

        self.assertEqual(result["key"], object_key)
        self.assertEqual(deleted, ["old-prefix/backup-old.tar.gz"])

    def test_backup_retry_rejects_pending_key_after_target_settings_change(self) -> None:
        service = BackupService()
        settings = {
            "account_id": "old-account",
            "access_key_id": "old-access",
            "secret_access_key": "old-secret",
            "bucket": "old-bucket",
            "prefix": "backups",
            "encrypt": False,
            "passphrase": "old-passphrase",
            "rotation_keep": 1,
        }
        state: dict[str, object] = {}
        uploaded: list[tuple[str, str]] = []
        fail_upload = True

        class FakeConfig:
            def get_backup_settings(self) -> dict[str, object]:
                return dict(settings)

            def update(self, payload: dict[str, object]) -> dict[str, object]:
                settings.update(dict(payload.get("backup") or {}))
                return {"backup": dict(settings)}

        class FakeClient:
            prefix = "backups"

            def __init__(self, client_settings: dict[str, object]) -> None:
                self.client_settings = dict(client_settings)

            def validate(self) -> None:
                return None

            def upload_bytes(self, key, payload, *, content_type, metadata):
                nonlocal fail_upload
                uploaded.append((str(self.client_settings["bucket"]), key))
                if fail_upload:
                    fail_upload = False
                    raise BackupError("upload failed", code="r2_upload_failed", status_code=503)
                return {"key": key}

            def close(self) -> None:
                return None

        object_key = "backups/backup-20260817T000000Z-pending.tar.gz"
        fake_config = FakeConfig()
        app = FastAPI()
        app.include_router(create_router("test"))
        with (
            mock.patch.object(backup_module, "config", fake_config),
            mock.patch.object(system_module, "config", fake_config),
            mock.patch.object(
                system_module,
                "require_admin_async",
                new=mock.AsyncMock(return_value={"id": "admin", "role": "admin"}),
            ),
            mock.patch.object(backup_module, "_new_backup_object_key", return_value=object_key),
            mock.patch.object(backup_module, "load_backup_state", side_effect=lambda: dict(state)),
            mock.patch.object(backup_module, "save_backup_state", side_effect=lambda payload: state.update(payload)),
            mock.patch.object(backup_module, "CloudflareR2Client", FakeClient),
            mock.patch.object(service, "_build_backup_archive", return_value=b"payload"),
            mock.patch.object(service, "_apply_rotation"),
        ):
            with self.assertRaises(BackupError):
                service.run_backup()

            response = TestClient(app).post(
                "/api/settings",
                headers=AUTH_HEADERS,
                json={"backup": {"bucket": "new-bucket", "access_key_id": "new-access"}},
            )
            self.assertEqual(response.status_code, 200, response.text)

            with self.assertRaises(BackupError) as raised:
                service.run_backup()

        self.assertEqual(raised.exception.code, "backup_state_invalid")
        self.assertEqual(uploaded, [("old-bucket", object_key)])

    def test_concurrent_instances_do_not_reuse_pending_key_or_overwrite_success_state(self) -> None:
        service_a = BackupService()
        service_b = BackupService()
        object_key_a = "backups/backup-20260817T000000Z-0001.tar.gz"
        object_key_b = "backups/backup-20260817T000000Z-0002.tar.gz"
        state: dict[str, object] = {}
        a_started = threading.Event()
        release_a = threading.Event()
        a_errors: list[BaseException] = []
        b_errors: list[BaseException] = []
        b_result: list[dict[str, object]] = []

        def run_a(*, trigger: str, object_key: str | None = None) -> dict[str, object]:
            a_started.set()
            if not release_a.wait(2):
                raise AssertionError("first backup was not released")
            raise BackupError("upload failed", code="r2_upload_failed", status_code=503)

        def run_b(*, trigger: str, object_key: str | None = None) -> dict[str, object]:
            b_result.append({"key": object_key or "missing"})
            return {"key": object_key or "missing", "size": 1, "encrypted": False}

        def save_state(payload: dict[str, object]) -> None:
            state.clear()
            state.update(payload)

        service_a._run_backup_once = mock.Mock(side_effect=run_a)
        service_b._run_backup_once = mock.Mock(side_effect=run_b)

        def run(service: BackupService, errors: list[BaseException]) -> None:
            try:
                service.run_backup()
            except BaseException as exc:
                errors.append(exc)

        with (
            mock.patch.object(
                backup_module,
                "_new_backup_object_key",
                side_effect=[object_key_a, object_key_b],
            ),
            mock.patch.object(
                backup_module.config,
                "get_backup_settings",
                return_value={"prefix": "backups", "encrypt": False},
            ),
            mock.patch.object(backup_module, "load_backup_state", side_effect=lambda: dict(state)),
            mock.patch.object(backup_module, "save_backup_state", side_effect=save_state),
        ):
            thread_a = threading.Thread(target=run, args=(service_a, a_errors))
            thread_a.start()
            self.assertTrue(a_started.wait(2))

            thread_b = threading.Thread(target=run, args=(service_b, b_errors))
            thread_b.start()
            thread_b.join(timeout=2)
            self.assertFalse(thread_b.is_alive())
            release_a.set()
            thread_a.join(timeout=2)
            self.assertFalse(thread_a.is_alive())

        self.assertEqual(len(a_errors), 1)
        self.assertIsInstance(a_errors[0], BackupError)
        self.assertEqual(b_errors, [])
        self.assertEqual(b_result, [{"key": object_key_b}])
        self.assertEqual(state.get("last_status"), "success")

    def test_rotation_does_not_delete_newer_pending_or_successful_owner(self) -> None:
        service = BackupService()
        current_key = "backups/backup-20260817T000000Z-current.tar.gz"
        newer_key = "backups/backup-20260817T000000Z-newer.tar.gz"
        old_key = "backups/backup-20260816T000000Z-old.tar.gz"
        deleted: list[str] = []

        class FakeClient:
            prefix = "backups"

            def validate(self) -> None:
                return None

            def upload_bytes(self, key, payload, *, content_type, metadata):
                return {"key": key}

            def list_objects(self):
                return [
                    {"key": current_key, "size": 1, "updated_at": "2026-08-17T00:00:03Z"},
                    {"key": newer_key, "size": 1, "updated_at": "2026-08-17T00:00:02Z"},
                    {"key": old_key, "size": 1, "updated_at": "2026-08-16T00:00:01Z"},
                ]

            def delete_object(self, key: str) -> None:
                deleted.append(key)

            def close(self) -> None:
                return None

        with (
            mock.patch.object(
                backup_module.config,
                "get_backup_settings",
                return_value={"prefix": "backups", "encrypt": False, "rotation_keep": 1},
            ),
            mock.patch.object(
                backup_module,
                "load_backup_state",
                return_value={"pending_object_key": newer_key, "last_object_key": newer_key},
            ),
            mock.patch.object(backup_module, "CloudflareR2Client", return_value=FakeClient()),
            mock.patch.object(service, "_build_backup_archive", return_value=b"payload"),
        ):
            result = service._run_backup_once(trigger="test", object_key=current_key)

        self.assertEqual(result["key"], current_key)
        self.assertEqual(deleted, [old_key])

    def test_cross_instance_delete_cannot_remove_running_backup_owner(self) -> None:
        service_a = BackupService()
        service_b = BackupService()
        object_key = "backups/backup-20260817T000000Z-running.tar.gz"
        state: dict[str, object] = {}
        entered = threading.Event()
        release = threading.Event()
        run_errors: list[BaseException] = []
        delete_calls: list[str] = []

        def run_once(*, trigger: str, object_key: str | None = None) -> dict[str, object]:
            entered.set()
            if not release.wait(2):
                raise AssertionError("running backup was not released")
            return {"key": object_key or "missing", "size": 1, "encrypted": False}

        class FakeClient:
            def validate(self) -> None:
                return None

            def delete_object(self, key: str) -> None:
                delete_calls.append(key)

            def close(self) -> None:
                return None

        service_a._run_backup_once = mock.Mock(side_effect=run_once)

        def run() -> None:
            try:
                service_a.run_backup()
            except BaseException as exc:
                run_errors.append(exc)

        def save_state(payload: dict[str, object]) -> None:
            state.clear()
            state.update(payload)

        with (
            mock.patch.object(backup_module, "_new_backup_object_key", return_value=object_key),
            mock.patch.object(
                backup_module.config,
                "get_backup_settings",
                return_value={"prefix": "backups", "encrypt": False},
            ),
            mock.patch.object(backup_module, "load_backup_state", side_effect=lambda: dict(state)),
            mock.patch.object(backup_module, "save_backup_state", side_effect=save_state),
            mock.patch.object(backup_module, "CloudflareR2Client", return_value=FakeClient()),
        ):
            worker = threading.Thread(target=run)
            worker.start()
            self.assertTrue(entered.wait(2))
            with self.assertRaises(BackupError) as raised:
                service_b.delete_backup(object_key)
            self.assertEqual(raised.exception.code, "backup_busy")
            release.set()
            worker.join(timeout=2)

        self.assertFalse(worker.is_alive())
        self.assertEqual(run_errors, [])
        self.assertEqual(delete_calls, [])

    def test_delete_cannot_remove_object_owned_by_running_backup(self) -> None:
        service = BackupService()
        object_key = "backups/backup-running.tar.gz"
        entered = threading.Event()
        release = threading.Event()
        state: dict[str, object] = {}

        def save_state(payload: dict[str, object]) -> None:
            state.clear()
            state.update(payload)

        def run_once(*, trigger: str, object_key: str | None = None) -> dict[str, object]:
            self.assertEqual(object_key, object_key_for_test)
            entered.set()
            self.assertTrue(release.wait(2))
            return {"key": object_key_for_test, "size": 1, "encrypted": False}

        object_key_for_test = object_key
        service._run_backup_once = mock.Mock(side_effect=run_once)
        with (
            mock.patch.object(backup_module, "_new_backup_object_key", return_value=object_key),
            mock.patch.object(
                backup_module.config,
                "get_backup_settings",
                return_value={"prefix": "backups", "encrypt": False},
            ),
            mock.patch.object(backup_module, "load_backup_state", side_effect=lambda: dict(state)),
            mock.patch.object(backup_module, "save_backup_state", side_effect=save_state),
            mock.patch.object(backup_module, "CloudflareR2Client") as client_cls,
        ):
            worker = threading.Thread(target=service.run_backup, name="backup-test")
            worker.start()
            self.assertTrue(entered.wait(2))

            try:
                with self.assertRaises(BackupError) as raised:
                    service.delete_backup(object_key)
                self.assertEqual(raised.exception.code, "backup_busy")
                client_cls.assert_not_called()
            finally:
                release.set()
                worker.join(timeout=2)
            self.assertFalse(worker.is_alive())
            self.assertEqual(state.get("last_status"), "success")

    def test_delete_admission_blocks_pending_backup_reusing_same_object_key(self) -> None:
        service = BackupService()
        object_key = "backups/backup-pending-race.tar.gz"
        delete_validating = threading.Event()
        allow_delete = threading.Event()
        delete_calls: list[str] = []
        run_result: list[object] = []
        state = {"pending_object_key": object_key}

        class FakeClient:
            def validate(self) -> None:
                delete_validating.set()
                self.assert_wait()

            def assert_wait(self) -> None:
                if not allow_delete.wait(2):
                    raise AssertionError("delete validation barrier was not released")

            def delete_object(self, key: str) -> None:
                delete_calls.append(key)

            def close(self) -> None:
                return None

        service._run_backup_once = mock.Mock(return_value={"key": object_key})

        def run_backup() -> None:
            try:
                service.run_backup()
            except BaseException as exc:
                run_result.append(exc)

        with (
            mock.patch.object(backup_module, "CloudflareR2Client", return_value=FakeClient()),
            mock.patch.object(
                backup_module.config,
                "get_backup_settings",
                return_value={"prefix": "backups", "encrypt": False},
            ),
            mock.patch.object(backup_module, "load_backup_state", side_effect=lambda: dict(state)),
            mock.patch.object(backup_module, "save_backup_state", side_effect=lambda payload: state.update(payload)),
        ):
            delete_thread = threading.Thread(target=service.delete_backup, args=(object_key,))
            delete_thread.start()
            self.assertTrue(delete_validating.wait(2))

            run_thread = threading.Thread(target=run_backup)
            run_thread.start()
            run_thread.join(timeout=2)
            self.assertFalse(run_thread.is_alive())

            allow_delete.set()
            delete_thread.join(timeout=2)
            self.assertFalse(delete_thread.is_alive())

        self.assertEqual(len(run_result), 1)
        self.assertIsInstance(run_result[0], BackupError)
        self.assertEqual(run_result[0].code, "backup_busy")
        service._run_backup_once.assert_not_called()
        self.assertEqual(delete_calls, [object_key])

    def test_scheduled_backup_failure_is_logged_without_exception_text(self) -> None:
        canary = "backup-secret-path-token"
        service = BackupService()
        service.run_scheduled_backup_if_needed = mock.Mock(side_effect=RuntimeError(canary))

        with mock.patch.object(backup_module, "logger", create=True) as logger:
            service._run_scheduled_backup_once()

        logger.warning.assert_called_once()
        event = logger.warning.call_args.args[0]
        self.assertEqual(event["event"], "scheduled_backup_failed")
        self.assertEqual(event["error_type"], "RuntimeError")
        self.assertNotIn(canary, repr(event))

    def test_initial_backup_state_failure_does_not_leave_service_busy(self) -> None:
        service = BackupService()
        service._run_backup_once = mock.Mock(return_value={"key": "backup-key"})

        with mock.patch.object(
            backup_module,
            "save_backup_state",
            side_effect=[OSError("state write failed"), None, None],
        ), mock.patch.object(
            backup_module,
            "load_backup_state",
            return_value={},
        ):
            with self.assertRaises(OSError):
                service.run_backup()

            result = service.run_backup()

        self.assertEqual(result["key"], "backup-key")
        service._run_backup_once.assert_called_once()
        self.assertEqual(service._run_backup_once.call_args.kwargs["trigger"], "manual")
        self.assertTrue(service._run_backup_once.call_args.kwargs["object_key"])

    def test_backup_retry_reuses_operation_key_after_success_state_failure(self) -> None:
        service = BackupService()
        state: dict[str, object] = {}
        save_calls: list[dict[str, object]] = []
        fail_success_write = True

        def save_state(payload: dict[str, object]) -> None:
            nonlocal fail_success_write
            save_calls.append(dict(payload))
            if payload.get("last_status") == "success" and fail_success_write:
                fail_success_write = False
                raise OSError("state write failed")
            state.clear()
            state.update(payload)

        generated_keys: list[str] = []

        def run_once(*, trigger: str, object_key: str | None = None) -> dict[str, object]:
            key = object_key or f"generated-{len(generated_keys) + 1}"
            generated_keys.append(key)
            return {"key": key}

        service._run_backup_once = mock.Mock(side_effect=run_once)
        with (
            mock.patch.object(backup_module, "load_backup_state", side_effect=lambda: dict(state)),
            mock.patch.object(backup_module, "save_backup_state", side_effect=save_state),
        ):
            with self.assertRaises(OSError):
                service.run_backup()
            service.run_backup()

        self.assertEqual(generated_keys, [generated_keys[0], generated_keys[0]])
        self.assertTrue(any(item.get("pending_object_key") for item in save_calls))

    def test_pending_backup_key_round_trips_and_is_cleared_after_success(self) -> None:
        state_path = Path(tempfile.mkdtemp()) / "backup_state.json"
        with mock.patch.object(config_module, "BACKUP_STATE_FILE", state_path):
            config_module.save_backup_state({
                "last_status": "idle",
                "pending_object_key": "backups/backup-20260816T000000Z-0001.tar.gz",
            })
            pending = config_module.load_backup_state()
            self.assertEqual(
                pending["pending_object_key"],
                "backups/backup-20260816T000000Z-0001.tar.gz",
            )

            config_module.save_backup_state({
                "last_status": "success",
                "last_object_key": pending["pending_object_key"],
                "pending_object_key": None,
            })
            self.assertIsNone(config_module.load_backup_state()["pending_object_key"])

    def test_pending_backup_key_encoding_mismatch_fails_before_upload(self) -> None:
        cases = (
            ("backups/backup-20260816T000000Z-0001.tar.gz.enc", False),
            ("backups/backup-20260816T000000Z-0001.tar.gz", True),
        )
        for pending_key, encrypt in cases:
            with self.subTest(pending_key=pending_key, encrypt=encrypt):
                service = BackupService()
                service._run_backup_once = mock.Mock(return_value={"key": pending_key})
                settings = {
                    "prefix": "backups",
                    "encrypt": encrypt,
                }
                with (
                    mock.patch.object(
                        backup_module,
                        "load_backup_state",
                        return_value={"pending_object_key": pending_key},
                    ),
                    mock.patch.object(
                        backup_module.config,
                        "get_backup_settings",
                        return_value=settings,
                    ),
                    mock.patch.object(backup_module, "save_backup_state") as save_state,
                ):
                    with self.assertRaises(BackupError) as raised:
                        service.run_backup()

                self.assertEqual(raised.exception.code, "backup_state_invalid")
                service._run_backup_once.assert_not_called()
                save_state.assert_not_called()
                self.assertFalse(service._running)

    def test_backup_state_read_does_not_follow_path_replacement(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            state_path = Path(tmp_dir) / "backup_state.json"
            replacement = Path(tmp_dir) / "replacement.json"
            state_path.write_text(json.dumps({"last_status": "success"}), encoding="utf-8")
            replacement.write_text(json.dumps({"last_status": "error", "last_error_code": "backup_failed"}), encoding="utf-8")
            original_read_text = Path.read_text

            def replace_before_read(path_obj, *args, **kwargs):
                if path_obj == state_path:
                    state_path.replace(Path(tmp_dir) / "displaced.json")
                    replacement.replace(state_path)
                return original_read_text(path_obj, *args, **kwargs)

            with (
                mock.patch.object(config_module, "BACKUP_STATE_FILE", state_path),
                mock.patch.object(Path, "read_text", autospec=True, side_effect=replace_before_read),
            ):
                state = config_module.load_backup_state()

            self.assertEqual(state["last_status"], "success")

    def test_backup_state_reads_fixed_handle_after_path_replacement(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            state_path = Path(tmp_dir) / "backup_state.json"
            replacement = Path(tmp_dir) / "replacement.json"
            displaced = Path(tmp_dir) / "displaced.json"
            state_path.write_text(json.dumps({"last_status": "success"}), encoding="utf-8")
            replacement.write_text(json.dumps({"last_status": "error", "last_error_code": "backup_failed"}), encoding="utf-8")
            original_open = secure_file.open_no_follow_file

            def replace_after_open(path_obj, *args, **kwargs):
                opened = original_open(path_obj, *args, **kwargs)
                if path_obj == state_path:
                    state_path.replace(displaced)
                    replacement.replace(state_path)
                return opened

            with (
                mock.patch.object(config_module, "BACKUP_STATE_FILE", state_path),
                mock.patch.object(secure_file, "open_no_follow_file", side_effect=replace_after_open),
            ):
                state = config_module.load_backup_state()

            self.assertEqual(state["last_status"], "success")

    def test_backup_object_operations_reject_keys_outside_generated_backup_scope(self) -> None:
        service = BackupService()
        settings = {
            "account_id": "account",
            "access_key_id": "access",
            "secret_access_key": "secret",
            "bucket": "bucket",
            "prefix": "backups",
        }
        client = mock.Mock()

        with (
            mock.patch.object(backup_module.config, "get_backup_settings", return_value=settings),
            mock.patch.object(backup_module, "CloudflareR2Client", return_value=client),
        ):
            for operation in (service.delete_backup, service.download_backup, service.get_backup_detail):
                with self.subTest(operation=operation.__name__):
                    with self.assertRaises(BackupError) as raised:
                        operation("other/backup-20260813.tar.gz")
                    self.assertEqual(raised.exception.code, "backup_key_invalid")

        client.delete_object.assert_not_called()
        client.download_bytes.assert_not_called()

    def test_backup_object_operations_validate_r2_config_before_network(self) -> None:
        settings = {
            "account_id": "",
            "access_key_id": "access",
            "secret_access_key": "secret",
            "bucket": "bucket",
            "prefix": "backups",
        }
        client = mock.Mock()
        client.download_bytes.return_value = b""
        client.validate.side_effect = BackupError(
            "R2 配置不完整",
            code="r2_config_incomplete",
        )

        with (
            mock.patch.object(backup_module.config, "get_backup_settings", return_value=settings),
            mock.patch.object(backup_module, "CloudflareR2Client", return_value=client),
        ):
            for operation in (service := BackupService()).delete_backup, service.download_backup, service.get_backup_detail:
                with self.subTest(operation=operation.__name__):
                    with self.assertRaises(BackupError) as raised:
                        operation("backups/backup-20260813.tar.gz")
                    self.assertEqual(raised.exception.code, "r2_config_incomplete")
                    client.validate.assert_called_once_with()
                    client.close.assert_called_once_with()
                    client.delete_object.assert_not_called()
                    client.download_bytes.assert_not_called()
                    client.reset_mock(side_effect=False)
                    client.validate.side_effect = BackupError(
                        "R2 配置不完整",
                        code="r2_config_incomplete",
                    )

    def test_r2_list_rejects_noncanonical_object_size(self) -> None:
        client = backup_module.CloudflareR2Client.__new__(backup_module.CloudflareR2Client)
        client.prefix = "backups"
        response = mock.Mock(
            status_code=200,
            headers={},
            iter_content=mock.Mock(return_value=[
                b"<ListBucketResult><Contents><Key>backups/backup.tar.gz</Key>",
                b"<Size>1.5</Size><LastModified>2026-08-13T00:00:00Z</LastModified>",
                b"</Contents><IsTruncated>false</IsTruncated></ListBucketResult>",
            ]),
            text=(
                "<ListBucketResult><Contents><Key>backups/backup.tar.gz</Key>"
                "<Size>1.5</Size><LastModified>2026-08-13T00:00:00Z</LastModified>"
                "</Contents><IsTruncated>false</IsTruncated></ListBucketResult>"
            ),
        )

        with mock.patch.object(client, "_request", return_value=response):
            with self.assertRaises(BackupError) as raised:
                client.list_objects()
        self.assertEqual(raised.exception.code, "r2_list_payload_invalid")
        self.assertEqual(str(raised.exception), "备份列表格式无效")
        response.close.assert_called_once_with()

    def test_r2_list_rejects_overlong_object_size(self) -> None:
        client = backup_module.CloudflareR2Client.__new__(backup_module.CloudflareR2Client)
        client.prefix = "backups"
        response = mock.Mock(
            status_code=200,
            headers={},
            iter_content=mock.Mock(return_value=[
                b"<ListBucketResult><Contents><Key>backups/backup.tar.gz</Key>",
                (b"<Size>" + b"9" * 5000 + b"</Size>"),
                b"<LastModified>2026-08-13T00:00:00Z</LastModified></Contents>",
                b"<IsTruncated>false</IsTruncated></ListBucketResult>",
            ]),
            text=(
                "<ListBucketResult><Contents><Key>backups/backup.tar.gz</Key>"
                f"<Size>{'9' * 5000}</Size>"
                "<LastModified>2026-08-13T00:00:00Z</LastModified>"
                "</Contents><IsTruncated>false</IsTruncated></ListBucketResult>"
            ),
        )

        with mock.patch.object(client, "_request", return_value=response):
            with self.assertRaises(BackupError) as raised:
                client.list_objects()
        self.assertEqual(raised.exception.code, "r2_list_payload_invalid")
        self.assertEqual(str(raised.exception), "备份列表格式无效")
        response.close.assert_called_once_with()

    def test_r2_list_rejects_object_size_above_download_contract(self) -> None:
        client = backup_module.CloudflareR2Client.__new__(backup_module.CloudflareR2Client)
        client.prefix = "backups"
        oversized = backup_module._MAX_R2_DOWNLOAD_BYTES + 1
        response = mock.Mock(
            status_code=200,
            headers={},
            iter_content=mock.Mock(return_value=[
                b"<ListBucketResult><Contents><Key>backups/backup.tar.gz</Key>",
                f"<Size>{oversized}</Size><LastModified>2026-08-13T00:00:00Z</LastModified>".encode(),
                b"</Contents><IsTruncated>false</IsTruncated></ListBucketResult>",
            ]),
            text=(
                "<ListBucketResult><Contents><Key>backups/backup.tar.gz</Key>"
                f"<Size>{oversized}</Size><LastModified>2026-08-13T00:00:00Z</LastModified>"
                "</Contents><IsTruncated>false</IsTruncated></ListBucketResult>"
            ),
        )

        with mock.patch.object(client, "_request", return_value=response):
            with self.assertRaises(BackupError) as raised:
                client.list_objects()
        self.assertEqual(raised.exception.code, "r2_list_payload_invalid")
        self.assertEqual(str(raised.exception), "备份列表格式无效")
        response.close.assert_called_once_with()

    def test_r2_download_closes_response_after_body_read(self) -> None:
        client = backup_module.CloudflareR2Client.__new__(backup_module.CloudflareR2Client)
        response = mock.Mock(
            status_code=200,
            headers={"content-length": "14"},
            iter_content=mock.Mock(return_value=[b"backup-", b"payload"]),
        )

        with mock.patch.object(client, "_request", return_value=response) as request_mock:
            self.assertEqual(client.download_bytes("backups/backup.tar.gz"), b"backup-payload")

        request_mock.assert_called_once_with("GET", "backups/backup.tar.gz", timeout=60.0, stream=True)
        response.close.assert_called_once_with()

    def test_r2_download_rejects_oversized_content_length_before_reading(self) -> None:
        client = backup_module.CloudflareR2Client.__new__(backup_module.CloudflareR2Client)
        response = mock.Mock(
            status_code=200,
            headers={"content-length": str(backup_module._MAX_R2_DOWNLOAD_BYTES + 1)},
            iter_content=mock.Mock(return_value=[b"should-not-read"]),
        )

        with mock.patch.object(client, "_request", return_value=response):
            with self.assertRaises(BackupError) as raised:
                client.download_bytes("backups/backup.tar.gz")

        self.assertEqual(raised.exception.code, "r2_read_payload_invalid")
        response.iter_content.assert_not_called()
        response.close.assert_called_once_with()

    def test_r2_download_rejects_stream_overflow_and_closes_response(self) -> None:
        client = backup_module.CloudflareR2Client.__new__(backup_module.CloudflareR2Client)
        response = mock.Mock(
            status_code=200,
            headers={},
            iter_content=mock.Mock(return_value=[b"1234", b"5"]),
        )

        with (
            mock.patch.object(backup_module, "_MAX_R2_DOWNLOAD_BYTES", 4),
            mock.patch.object(client, "_request", return_value=response),
        ):
            with self.assertRaises(BackupError) as raised:
                client.download_bytes("backups/backup.tar.gz")

        self.assertEqual(raised.exception.code, "r2_read_payload_invalid")
        response.close.assert_called_once_with()

    def test_r2_upload_streams_and_closes_response_without_reading_body(self) -> None:
        client = backup_module.CloudflareR2Client.__new__(backup_module.CloudflareR2Client)
        response = mock.Mock(status_code=200, headers={"etag": '"etag-value"'})

        with mock.patch.object(client, "_request", return_value=response) as request_mock:
            result = client.upload_bytes("backups/backup.tar.gz", b"payload", content_type="application/gzip")

        self.assertEqual(result, {"key": "backups/backup.tar.gz", "etag": "etag-value"})
        request_mock.assert_called_once_with(
            "PUT",
            "backups/backup.tar.gz",
            body=b"payload",
            extra_headers={"content-type": "application/gzip"},
            stream=True,
        )
        response.close.assert_called_once_with()

    def test_r2_delete_streams_and_closes_response_without_reading_body(self) -> None:
        client = backup_module.CloudflareR2Client.__new__(backup_module.CloudflareR2Client)
        response = mock.Mock(status_code=204)

        with mock.patch.object(client, "_request", return_value=response) as request_mock:
            client.delete_object("backups/backup.tar.gz")

        request_mock.assert_called_once_with("DELETE", "backups/backup.tar.gz", timeout=30.0, stream=True)
        response.close.assert_called_once_with()

    def test_r2_list_rejects_oversized_xml_body_and_closes_response(self) -> None:
        client = backup_module.CloudflareR2Client.__new__(backup_module.CloudflareR2Client)
        client.prefix = "backups"
        response = mock.Mock(
            status_code=200,
            headers={},
            iter_content=mock.Mock(return_value=[b"x" * 5, b"y" * 5]),
        )

        with (
            mock.patch.object(backup_module, "_MAX_R2_LIST_RESPONSE_BYTES", 8),
            mock.patch.object(client, "_request", return_value=response),
        ):
            with self.assertRaises(BackupError) as raised:
                client.list_objects()

        self.assertEqual(raised.exception.code, "r2_list_payload_invalid")
        response.close.assert_called_once_with()

    def test_r2_list_rejects_unbounded_continuation_pages(self) -> None:
        client = backup_module.CloudflareR2Client.__new__(backup_module.CloudflareR2Client)
        client.prefix = "backups"
        responses = []
        for index in range(4):
            truncated = "true" if index < 3 else "false"
            token = f"token-{index}"
            response = mock.Mock(
                status_code=200,
                headers={},
                iter_content=mock.Mock(return_value=[(
                    "<ListBucketResult>"
                    f"<Contents><Key>backups/backup-{index}.tar.gz</Key>"
                    "<Size>1</Size><LastModified>2026-08-14T00:00:00Z</LastModified></Contents>"
                    f"<IsTruncated>{truncated}</IsTruncated>"
                    f"<NextContinuationToken>{token}</NextContinuationToken>"
                    "</ListBucketResult>"
                ).encode("utf-8")]),
            )
            responses.append(response)

        with (
            mock.patch.object(backup_module, "_MAX_R2_LIST_PAGES", 3, create=True),
            mock.patch.object(client, "_request", side_effect=responses) as request_mock,
        ):
            with self.assertRaises(BackupError) as raised:
                client.list_objects()

        self.assertEqual(raised.exception.code, "r2_list_limit_exceeded")
        self.assertEqual(request_mock.call_count, 3)
        for response in responses[:3]:
            response.close.assert_called_once_with()

    def test_r2_list_rejects_page_that_exceeds_object_budget(self) -> None:
        client = backup_module.CloudflareR2Client.__new__(backup_module.CloudflareR2Client)
        client.prefix = "backups"
        contents = "".join(
            (
                f"<Contents><Key>backups/backup-{index}.tar.gz</Key>"
                "<Size>1</Size><LastModified>2026-08-14T00:00:00Z</LastModified></Contents>"
            )
            for index in range(3)
        )
        response = mock.Mock(
            status_code=200,
            headers={},
            iter_content=mock.Mock(return_value=[(
                f"<ListBucketResult>{contents}<IsTruncated>false</IsTruncated></ListBucketResult>"
            ).encode("utf-8")]),
        )

        with (
            mock.patch.object(backup_module, "_MAX_R2_LIST_OBJECTS", 2),
            mock.patch.object(client, "_request", return_value=response),
        ):
            with self.assertRaises(BackupError) as raised:
                client.list_objects()

        self.assertEqual(raised.exception.code, "r2_list_limit_exceeded")
        response.close.assert_called_once_with()

    def test_backup_detail_does_not_project_untrusted_metadata_containers(self) -> None:
        canary = "backup-detail-canary owner@example.com https://secret.example"
        metadata = {
            "created_at": {"secret": canary},
            "trigger": [canary],
            "app_version": {"secret": canary},
            "storage_backend": {
                "type": "json",
                "file_path": canary,
                "repo_url": canary,
                "nested": {"secret": canary},
            },
        }
        archive_buffer = io.BytesIO()
        with tarfile.open(fileobj=archive_buffer, mode="w:gz") as archive:
            payload = json.dumps(metadata, ensure_ascii=False).encode("utf-8")
            info = tarfile.TarInfo("backup-metadata.json")
            info.size = len(payload)
            archive.addfile(info, io.BytesIO(payload))

        detail = BackupService()._decode_archive_detail(archive_buffer.getvalue())

        self.assertEqual(detail["created_at"], None)
        self.assertEqual(detail["trigger"], None)
        self.assertEqual(detail["app_version"], None)
        self.assertEqual(detail["storage_backend"], {"type": "json"})
        self.assertNotIn(canary, json.dumps(detail, ensure_ascii=False))

    def test_backup_detail_preserves_valid_metadata_projection(self) -> None:
        archive_buffer = io.BytesIO()
        with tarfile.open(fileobj=archive_buffer, mode="w:gz") as archive:
            payload = json.dumps(
                {
                    "created_at": "2026-08-14T00:00:00Z",
                    "trigger": "manual",
                    "app_version": "1.8.0",
                    "storage_backend": {"type": "json", "file_path": "private/config.json"},
                }
            ).encode("utf-8")
            info = tarfile.TarInfo("backup-metadata.json")
            info.size = len(payload)
            archive.addfile(info, io.BytesIO(payload))

        detail = BackupService()._decode_archive_detail(archive_buffer.getvalue())

        self.assertEqual(detail["created_at"], "2026-08-14T00:00:00Z")
        self.assertEqual(detail["trigger"], "manual")
        self.assertEqual(detail["app_version"], "1.8.0")
        self.assertEqual(detail["storage_backend"], {"type": "json"})

    def test_backup_detail_rejects_malformed_snapshot_json(self) -> None:
        archive_buffer = io.BytesIO()
        with tarfile.open(fileobj=archive_buffer, mode="w:gz") as archive:
            payload = b"{malformed-snapshot"
            info = tarfile.TarInfo("snapshots/accounts.json")
            info.size = len(payload)
            archive.addfile(info, io.BytesIO(payload))

        with self.assertRaises(BackupError) as raised:
            BackupService()._decode_archive_detail(archive_buffer.getvalue())

        self.assertEqual(raised.exception.code, "backup_archive_invalid")

    def test_get_backup_detail_rejects_malformed_snapshot_from_r2(self) -> None:
        archive_buffer = io.BytesIO()
        with tarfile.open(fileobj=archive_buffer, mode="w:gz") as archive:
            payload = b"{malformed-snapshot"
            info = tarfile.TarInfo("snapshots/accounts.json")
            info.size = len(payload)
            archive.addfile(info, io.BytesIO(payload))

        class FakeClient:
            def validate(self) -> None:
                return None

            def download_bytes(self, _key: str) -> bytes:
                return archive_buffer.getvalue()

            def close(self) -> None:
                return None

        with (
            mock.patch.object(
                backup_module.config,
                "get_backup_settings",
                return_value={"prefix": "backups", "encrypt": False},
            ),
            mock.patch.object(backup_module, "CloudflareR2Client", return_value=FakeClient()),
        ):
            with self.assertRaises(BackupError) as raised:
                BackupService().get_backup_detail("backups/backup-20260816T000000Z-0001.tar.gz")

        self.assertEqual(raised.exception.code, "backup_archive_invalid")

    def test_backup_detail_drops_untrusted_archive_member_names(self) -> None:
        canary = "backup-member-canary owner@example.com upstream-body"
        archive_buffer = io.BytesIO()
        with tarfile.open(fileobj=archive_buffer, mode="w:gz") as archive:
            for name in (f"data/images/{canary}", f"snapshots/{canary}.json"):
                info = tarfile.TarInfo(name)
                info.size = 1
                archive.addfile(info, io.BytesIO(b"x"))

        detail = BackupService()._decode_archive_detail(archive_buffer.getvalue())

        self.assertEqual(detail["files"], [])
        self.assertEqual(detail["snapshots"], [])
        self.assertNotIn(canary, json.dumps(detail, ensure_ascii=False))

    def test_backup_list_projects_untrusted_updated_at_before_public_response(self) -> None:
        canary = "r2-updated-at-canary owner@example.com upstream-body"
        settings = {
            "account_id": "account",
            "access_key_id": "access",
            "secret_access_key": "secret",
            "bucket": "bucket",
            "prefix": "backups",
        }
        client = mock.Mock()
        client.list_objects.return_value = [{
            "key": "backups/backup-20260814T000000Z-0001.tar.gz",
            "size": 1,
            "updated_at": canary,
        }]
        app = FastAPI()
        app.include_router(create_router("test"))

        with (
            mock.patch.object(backup_module.config, "get_backup_settings", return_value=settings),
            mock.patch.object(backup_module, "CloudflareR2Client", return_value=client),
        ):
            response = TestClient(app).get("/api/backups", headers=AUTH_HEADERS)

        self.assertEqual(response.status_code, 200, response.text)
        body = response.json()
        self.assertIsNone(body["items"][0]["updated_at"])
        self.assertNotIn(canary, response.text)

    def test_backup_detail_rejects_oversized_uncompressed_member(self) -> None:
        archive_buffer = io.BytesIO()
        with tarfile.open(fileobj=archive_buffer, mode="w:gz") as archive:
            payload = b"0123456789"
            info = tarfile.TarInfo("data/large.bin")
            info.size = len(payload)
            archive.addfile(info, io.BytesIO(payload))

        with mock.patch.object(backup_module, "_MAX_BACKUP_DETAIL_MEMBER_BYTES", 4):
            with self.assertRaises(BackupError) as raised:
                BackupService()._decode_archive_detail(archive_buffer.getvalue())

        self.assertEqual(raised.exception.code, "backup_archive_invalid")

    def test_backup_detail_rejects_too_many_archive_members(self) -> None:
        archive_buffer = io.BytesIO()
        with tarfile.open(fileobj=archive_buffer, mode="w:gz") as archive:
            for index in range(3):
                payload = f"member-{index}".encode("ascii")
                info = tarfile.TarInfo(f"data/member-{index}.txt")
                info.size = len(payload)
                archive.addfile(info, io.BytesIO(payload))

        with mock.patch.object(backup_module, "_MAX_BACKUP_DETAIL_MEMBERS", 2, create=True):
            with self.assertRaises(BackupError) as raised:
                BackupService()._decode_archive_detail(archive_buffer.getvalue())

        self.assertEqual(raised.exception.code, "backup_archive_invalid")

    def test_backup_detail_streams_member_metadata_instead_of_getmembers_list(self) -> None:
        archive_buffer = io.BytesIO()
        with tarfile.open(fileobj=archive_buffer, mode="w:gz") as archive:
            payload = b"member"
            info = tarfile.TarInfo("data/member.txt")
            info.size = len(payload)
            archive.addfile(info, io.BytesIO(payload))

        with mock.patch.object(tarfile.TarFile, "getmembers", side_effect=AssertionError("unbounded member list")):
            detail = BackupService()._decode_archive_detail(archive_buffer.getvalue())

        self.assertEqual(detail["files"][0]["name"], "data/member.txt")

    def test_encrypted_backup_download_rejects_oversized_decrypted_payload(self) -> None:
        service = BackupService()
        client = mock.Mock()
        settings = {
            "account_id": "account",
            "access_key_id": "access",
            "secret_access_key": "secret",
            "bucket": "bucket",
            "prefix": "backups",
            "passphrase": "passphrase",
        }

        with (
            mock.patch.object(backup_module.config, "get_backup_settings", return_value=settings),
            mock.patch.object(backup_module, "CloudflareR2Client", return_value=client),
            mock.patch.object(client, "download_bytes", return_value=b"encrypted"),
            mock.patch.object(backup_module, "_openssl_decrypt", return_value=b"12345"),
            mock.patch.object(backup_module, "_MAX_R2_DOWNLOAD_BYTES", 4),
        ):
            with self.assertRaises(BackupError) as raised:
                service.download_backup("backups/backup-20260814T000000Z-0001.tar.gz.enc")

        self.assertEqual(raised.exception.code, "r2_read_payload_invalid")

    def test_legacy_backup_state_error_is_not_returned(self) -> None:
        secret = "legacy-upstream-secret owner@example.com"
        state_path = Path(tempfile.mkdtemp()) / "backup_state.json"
        state_path.write_text(
            json.dumps({
                "last_status": "error",
                "last_error": secret,
            }),
            encoding="utf-8",
        )

        with mock.patch.object(config_module, "BACKUP_STATE_FILE", state_path):
            state = BackupService().get_status()

        self.assertEqual(state["last_error"], "备份执行失败，请稍后重试")
        self.assertNotIn(secret, json.dumps(state, ensure_ascii=False))

    def test_forged_legacy_public_marker_does_not_trust_error_text(self) -> None:
        secret = "forged-state-secret owner@example.com"
        state_path = Path(tempfile.mkdtemp()) / "backup_state.json"
        state_path.write_text(
            json.dumps({
                "last_status": "error",
                "last_error": secret,
                "_last_error_public": True,
            }),
            encoding="utf-8",
        )

        with mock.patch.object(config_module, "BACKUP_STATE_FILE", state_path):
            state = BackupService().get_status()

        self.assertEqual(state["last_error"], "备份执行失败，请稍后重试")
        self.assertNotIn(secret, json.dumps(state, ensure_ascii=False))

    def test_forged_marker_and_error_body_are_not_persisted(self) -> None:
        secret = "forged-persisted-secret owner@example.com"
        state_path = Path(tempfile.mkdtemp()) / "backup_state.json"

        with mock.patch.object(config_module, "BACKUP_STATE_FILE", state_path):
            config_module.save_backup_state(
                {
                    "last_status": "error",
                    "last_error": secret,
                    "_last_error_public": True,
                }
            )
            raw_state = state_path.read_text(encoding="utf-8")
            state = BackupService().get_status()

        self.assertNotIn(secret, raw_state)
        self.assertNotIn("_last_error_public", raw_state)
        self.assertNotIn('"last_error"', raw_state)
        self.assertEqual(state["last_error"], "备份执行失败，请稍后重试")

    def test_backup_state_rebuilds_error_from_allowlisted_code_and_status(self) -> None:
        secret = "persisted-error-body owner@example.com"
        state_path = Path(tempfile.mkdtemp()) / "backup_state.json"
        state_path.write_text(
            json.dumps({
                "last_status": "error",
                "last_error": secret,
                "last_error_code": "r2_connection_failed",
                "last_error_status": 503,
            }),
            encoding="utf-8",
        )

        with mock.patch.object(config_module, "BACKUP_STATE_FILE", state_path):
            state = BackupService().get_status()

        self.assertEqual(state["last_error"], "连接 R2 失败：HTTP 503")
        self.assertNotIn(secret, json.dumps(state, ensure_ascii=False))

    def test_unknown_backup_state_code_or_status_falls_back(self) -> None:
        cases = (
            {"last_error_code": "unknown-code", "last_error_status": 503},
            {"last_error_code": "r2_connection_failed", "last_error_status": 700},
            {"last_error_code": "r2_config_incomplete", "last_error_status": "503"},
        )
        for fields in cases:
            with self.subTest(fields=fields), tempfile.TemporaryDirectory() as tmp_dir:
                state_path = Path(tmp_dir) / "backup_state.json"
                state_path.write_text(json.dumps(fields), encoding="utf-8")
                with mock.patch.object(config_module, "BACKUP_STATE_FILE", state_path):
                    state = BackupService().get_status()
                self.assertEqual(state["last_error"], "备份执行失败，请稍后重试")

    def test_backup_state_drops_container_values_from_public_fields(self) -> None:
        secret = "backup-state-container-canary owner@example.com"
        state_path = Path(tempfile.mkdtemp()) / "backup_state.json"
        state_path.write_text(
            json.dumps(
                {
                    "last_started_at": {"secret": secret},
                    "last_finished_at": [secret],
                    "last_status": {"secret": secret},
                    "last_object_key": {"secret": secret},
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

        with mock.patch.object(config_module, "BACKUP_STATE_FILE", state_path):
            state = BackupService().get_status()

        self.assertEqual(state["last_started_at"], None)
        self.assertEqual(state["last_finished_at"], None)
        self.assertEqual(state["last_status"], "idle")
        self.assertEqual(state["last_object_key"], None)
        self.assertNotIn(secret, json.dumps(state, ensure_ascii=False))

    def test_invalid_status_is_canonicalized_to_fallback_when_saved(self) -> None:
        state_path = Path(tempfile.mkdtemp()) / "backup_state.json"
        with mock.patch.object(config_module, "BACKUP_STATE_FILE", state_path):
            config_module.save_backup_state(
                {
                    "last_status": "error",
                    "last_error_code": "r2_config_incomplete",
                    "last_error_status": "503",
                }
            )
            raw_state = json.loads(state_path.read_text(encoding="utf-8"))
            state = BackupService().get_status()

        self.assertEqual(raw_state["last_error_code"], "backup_failed")
        self.assertNotIn("last_error_status", raw_state)
        self.assertEqual(state["last_error"], "备份执行失败，请稍后重试")

    def test_backup_state_replace_failure_preserves_previous_snapshot(self) -> None:
        state_path = Path(tempfile.mkdtemp()) / "backup_state.json"
        original = json.dumps({"last_status": "idle", "last_object_key": "old"}, ensure_ascii=False) + "\n"
        state_path.write_text(original, encoding="utf-8")

        with (
            mock.patch.object(config_module, "BACKUP_STATE_FILE", state_path),
            mock.patch.object(config_module, "atomic_write_bytes", side_effect=OSError("replace failed")),
        ):
            with self.assertRaises(OSError):
                config_module.save_backup_state({"last_status": "success", "last_object_key": "new"})

        self.assertEqual(state_path.read_text(encoding="utf-8"), original)
        self.assertEqual(list(state_path.parent.glob("backup_state.json.*.tmp")), [])

    def test_backup_state_write_fails_closed_when_parent_directory_is_rebound(self) -> None:
        original_dir = Path(tempfile.mkdtemp()) / "state"
        original_dir.mkdir()
        state_path = original_dir / "backup_state.json"
        original = json.dumps({"last_status": "idle", "last_object_key": "old"}, ensure_ascii=False) + "\n"
        state_path.write_text(original, encoding="utf-8")
        moved_dir = original_dir.parent / "moved-state"
        original_write = config_module.atomic_write_bytes

        def rebind_parent(path: Path, root: Path, payload: bytes, **kwargs: object) -> None:
            original_dir.rename(moved_dir)
            original_dir.mkdir()
            state_path.write_text(
                json.dumps({"last_status": "foreign", "last_object_key": "foreign"}, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )
            original_write(path, root, payload, **kwargs)

        with (
            mock.patch.object(config_module, "BACKUP_STATE_FILE", state_path),
            mock.patch.object(config_module, "atomic_write_bytes", rebind_parent),
            self.assertRaises(OSError, msg="backup state parent rebind must fail closed"),
        ):
            config_module.save_backup_state({"last_status": "success", "last_object_key": "new"})

        self.assertEqual(json.loads(state_path.read_text(encoding="utf-8"))["last_status"], "foreign")
        self.assertEqual(json.loads((moved_dir / "backup_state.json").read_text(encoding="utf-8"))["last_status"], "idle")

    def test_unknown_backup_error_is_not_persisted_or_returned(self) -> None:
        secret = "opaque-backup-token owner@example.com upstream fragment"
        state_path = Path(tempfile.mkdtemp()) / "backup_state.json"
        service = BackupService()
        service._run_backup_once = mock.Mock(side_effect=RuntimeError(secret))

        with mock.patch.object(config_module, "BACKUP_STATE_FILE", state_path):
            with self.assertRaises(RuntimeError):
                service.run_backup()

            state = service.get_status()
            raw_state = state_path.read_text(encoding="utf-8")
            self.assertEqual(state["last_error"], "备份执行失败，请稍后重试")
            self.assertNotIn(secret, raw_state)

            app = FastAPI()
            app.include_router(create_router("test"))
            with (
                mock.patch.object(system_module.backup_service, "list_backups", return_value=[]),
                mock.patch.object(system_module.backup_service, "get_settings", return_value={}),
            ):
                response = TestClient(app).get("/api/backups", headers=AUTH_HEADERS)

            self.assertEqual(response.status_code, 200, response.text)
            self.assertNotIn(secret, json.dumps(response.json(), ensure_ascii=False))

    def test_explicit_backup_error_keeps_its_controlled_message(self) -> None:
        state_path = Path(tempfile.mkdtemp()) / "backup_state.json"
        service = BackupService()
        service._run_backup_once = mock.Mock(
            side_effect=BackupError(
                "连接 R2 失败：HTTP 503",
                code="r2_connection_failed",
                status_code=503,
            )
        )

        with mock.patch.object(config_module, "BACKUP_STATE_FILE", state_path):
            with self.assertRaises(BackupError):
                service.run_backup()
            self.assertEqual(service.get_status()["last_error"], "连接 R2 失败：HTTP 503")

    def test_backup_state_write_failure_does_not_replace_controlled_backup_error(self) -> None:
        service = BackupService()
        original = BackupError(
            "连接 R2 失败：HTTP 503",
            code="r2_connection_failed",
            status_code=503,
        )
        service._run_backup_once = mock.Mock(side_effect=original)

        with mock.patch.object(
            backup_module,
            "save_backup_state",
            side_effect=[None, OSError("state write failed")],
        ), mock.patch.object(backup_module, "load_backup_state", return_value={}):
            with self.assertRaises(BackupError) as raised:
                service.run_backup()

        self.assertIs(raised.exception, original)
        self.assertFalse(service._running)


if __name__ == "__main__":
    unittest.main()
