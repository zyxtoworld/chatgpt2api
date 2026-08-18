from __future__ import annotations

import json
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import services.editable_file_task_service as editable_module
import services.secure_file as secure_file
import services.task_executor as task_executor_module
from services.account_service import AccountService
from services.editable_file_task_service import EditableFileTaskService
from services.protocol.error_response import PublicSafeError
from services.storage.base import StorageDataError
from services.storage.json_storage import JSONStorageBackend


OWNER = {"id": "owner-1", "name": "Owner", "role": "admin"}


def wait_for_task(service: EditableFileTaskService, task_id: str, status: str) -> dict[str, object]:
    deadline = time.time() + 2
    while time.time() < deadline:
        result = service.list_tasks(OWNER, [task_id])
        if result["items"] and result["items"][0]["status"] == status:
            return result["items"][0]
        time.sleep(0.01)
    raise AssertionError(f"task {task_id!r} did not reach {status!r}")


class EditableFileTaskErrorContractTests(unittest.TestCase):
    def test_cross_instance_duplicate_submit_returns_authoritative_task(self) -> None:
        submitted: list[tuple[object, tuple[object, ...]]] = []

        class FakeReservation:
            def submit(self, target, *args, **_kwargs) -> None:
                submitted.append((target, args))

            def cancel(self) -> None:
                pass

        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "editable_file_tasks.json"
            service_a = EditableFileTaskService(path)
            service_b = EditableFileTaskService(path)
            with mock.patch.object(editable_module, "reserve_background_task", side_effect=FakeReservation):
                first = service_a.submit_ppt(OWNER, client_task_id="same-task", prompt="A")
                second = service_b.submit_ppt(OWNER, client_task_id="same-task", prompt="B")

            self.assertEqual(first["id"], second["id"])
            self.assertEqual(first["status"], editable_module.TASK_STATUS_QUEUED)
            self.assertEqual(second["status"], editable_module.TASK_STATUS_QUEUED)
            self.assertEqual(len(submitted), 1)

    def test_cross_instance_error_transition_preserves_other_terminal_task(self) -> None:
        class FakeReservation:
            def submit(self, *_args, **_kwargs) -> None:
                pass

            def cancel(self) -> None:
                pass

        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "editable_file_tasks.json"
            service_a = EditableFileTaskService(path)
            with mock.patch.object(editable_module, "reserve_background_task", side_effect=FakeReservation):
                service_a.submit_ppt(OWNER, client_task_id="error-a", prompt="A")
            key_a = "owner-1:error-a"
            service_a._update_task(key_a, status=editable_module.TASK_STATUS_RUNNING, error="")

            with mock.patch.object(
                editable_module.EditableFileTaskService,
                "_recover_unfinished_locked",
                return_value=False,
            ):
                service_b = EditableFileTaskService(path)
            with mock.patch.object(editable_module, "reserve_background_task", side_effect=FakeReservation):
                service_b.submit_ppt(OWNER, client_task_id="error-b", prompt="B")
            key_b = "owner-1:error-b"
            service_b._update_task(
                key_b,
                status=editable_module.TASK_STATUS_ERROR,
                error="editable file task failed",
                ended_ts=time.time(),
            )
            service_a._update_task(
                key_a,
                status=editable_module.TASK_STATUS_ERROR,
                error="editable file task failed",
                ended_ts=time.time(),
            )

            with mock.patch.object(
                editable_module.EditableFileTaskService,
                "_recover_unfinished_locked",
                return_value=False,
            ):
                reloaded = EditableFileTaskService(path)
            by_id = {item["id"]: item for item in reloaded.list_tasks(OWNER, [])['items']}
            self.assertEqual(by_id["error-a"]["status"], editable_module.TASK_STATUS_ERROR)
            self.assertEqual(by_id["error-b"]["status"], editable_module.TASK_STATUS_ERROR)

    def test_cross_instance_task_completion_does_not_lose_own_task_or_artifacts(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        submitted: list[tuple[object, tuple[object, ...]]] = []

        class FakeReservation:
            def submit(self, target, *args, **_kwargs) -> None:
                submitted.append((target, args))

            def cancel(self) -> None:
                pass

        class BlockingBackend:
            def __init__(self, _access_token: str) -> None:
                pass

            def export_ppt_zip(self, _images, _prompt, output_dir):
                entered.set()
                if not release.wait(5):
                    raise AssertionError("editable task did not receive release")
                output_dir.mkdir(parents=True, exist_ok=True)
                primary = output_dir / "primary.pptx"
                archive = output_dir / "assets.zip"
                primary.write_bytes(b"ppt")
                archive.write_bytes(b"zip")
                return SimpleNamespace(
                    conversation_id="conversation-a",
                    primary_path=primary,
                    zip_path=archive,
                )

            def close(self) -> None:
                pass

        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "editable_file_tasks.json"
            root = Path(temp_dir) / "files"
            service_a = EditableFileTaskService(path)
            fake_account = SimpleNamespace(
                get_text_access_token=lambda **_kwargs: "editable-token",
                _get_account_lease=lambda _token: ("editable-token", {"access_token": "editable-token"}),
                mark_text_used=lambda *_args, **_kwargs: None,
            )
            with (
                mock.patch.object(editable_module, "reserve_background_task", side_effect=FakeReservation),
                mock.patch.object(editable_module, "EDITABLE_FILE_ROOT", root),
                mock.patch.object(editable_module, "account_service", fake_account),
                mock.patch.object(editable_module, "OpenAIBackendAPI", BlockingBackend),
                mock.patch.object(service_a, "_log_call"),
                mock.patch.object(editable_module.EditableFileTaskService, "_recover_unfinished_locked", return_value=False),
            ):
                service_a.submit_ppt(OWNER, client_task_id="task-a", prompt="A")
                self.assertEqual(len(submitted), 1)
                target_a, args_a = submitted.pop()

                worker = threading.Thread(
                    target=lambda: target_a(*args_a),
                )
                worker.start()
                self.assertTrue(entered.wait(5))

                service_b = EditableFileTaskService(path)
                service_b.submit_ppt(OWNER, client_task_id="task-b", prompt="B")

                release.set()
                worker.join(5)
                self.assertFalse(worker.is_alive())
                output_dir = editable_module._editable_output_dir("owner-1", "ppt", "task-a")
                self.assertTrue(output_dir.exists())
                self.assertEqual((output_dir / "primary.pptx").read_bytes(), b"ppt")
                self.assertEqual((output_dir / "assets.zip").read_bytes(), b"zip")

            with mock.patch.object(
                editable_module.EditableFileTaskService,
                "_recover_unfinished_locked",
                return_value=False,
            ):
                reloaded = EditableFileTaskService(path)
            items = reloaded.list_tasks(OWNER, [])['items']
            by_id = {item['id']: item for item in items}
            self.assertEqual(by_id['task-a']['status'], editable_module.TASK_STATUS_SUCCESS)
            self.assertEqual(by_id['task-b']['status'], editable_module.TASK_STATUS_QUEUED)

    def test_capability_generation_failure_releases_background_reservation(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = EditableFileTaskService(Path(temp_dir) / "editable_file_tasks.json")
            capacity = threading.BoundedSemaphore(1)
            with (
                mock.patch.object(task_executor_module, "_CAPACITY", capacity),
                mock.patch.object(
                    editable_module,
                    "_new_download_capability",
                    side_effect=RuntimeError("capability generation failed"),
                ),
            ):
                with self.assertRaisesRegex(RuntimeError, "capability generation failed"):
                    service.submit_ppt(OWNER, client_task_id="capability-failure")

                reservation = task_executor_module.reserve_background_task()
                reservation.cancel()

            self.assertEqual(service.list_tasks(OWNER, ["capability-failure"])["items"], [])

    def test_export_artifacts_are_removed_when_success_state_commit_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "editable_file_tasks.json"
            root = Path(temp_dir) / "files"
            service = EditableFileTaskService(path)
            task_id = "commit-failure"
            key = f"owner-1:{task_id}"
            now = editable_module._now_iso()
            service._tasks[key] = {
                "id": task_id,
                "owner_id": "owner-1",
                "status": editable_module.TASK_STATUS_QUEUED,
                "kind": "ppt",
                "model": editable_module.EDITABLE_FILE_MODEL,
                "created_at": now,
                "updated_at": now,
                "created_ts": time.time(),
                "updated_ts": time.time(),
            }

            class ExportBackend:
                def __init__(self, _access_token: str) -> None:
                    pass

                def export_ppt_zip(self, _images, _prompt, output_dir):
                    output_dir.mkdir(parents=True, exist_ok=True)
                    primary = output_dir / "primary.pptx"
                    archive = output_dir / "assets.zip"
                    primary.write_bytes(b"ppt")
                    archive.write_bytes(b"zip")
                    return SimpleNamespace(
                        conversation_id="conversation-1",
                        primary_path=primary,
                        zip_path=archive,
                    )

                def close(self) -> None:
                    pass

            original_update = service._update_task

            def fail_success_commit(task_key: str, **updates):
                if updates.get("status") == editable_module.TASK_STATUS_SUCCESS:
                    raise OSError("task success commit failed")
                return original_update(task_key, **updates)

            account = SimpleNamespace(
                get_text_access_token=lambda **_kwargs: "editable-token",
                mark_text_used=lambda *_args, **_kwargs: None,
            )
            with (
                mock.patch.object(editable_module, "EDITABLE_FILE_ROOT", root),
                mock.patch.object(editable_module, "account_service", account),
                mock.patch.object(editable_module, "OpenAIBackendAPI", ExportBackend),
                mock.patch.object(service, "_update_task", side_effect=fail_success_commit),
                mock.patch.object(service, "_log_call"),
            ):
                service._run_task(
                    key,
                    "ppt",
                    "prompt",
                    [],
                    OWNER,
                    "",
                    "download-capability",
                )
                output_dir = editable_module._editable_output_dir("owner-1", "ppt", task_id)
                self.assertFalse(output_dir.exists())
                self.assertEqual(
                    service.list_tasks(OWNER, [task_id])["items"][0]["status"],
                    editable_module.TASK_STATUS_ERROR,
                )

    def test_late_editable_usage_does_not_mutate_replaced_same_token_account(self) -> None:
        entered = threading.Event()
        release = threading.Event()

        class BlockingBackend:
            def __init__(self, _access_token: str) -> None:
                pass

            def export_ppt_zip(self, _images, _prompt, output_dir):
                entered.set()
                if not release.wait(5):
                    raise AssertionError("editable task did not receive release")
                output_dir.mkdir(parents=True, exist_ok=True)
                primary = output_dir / "primary.pptx"
                archive = output_dir / "assets.zip"
                primary.write_bytes(b"ppt")
                archive.write_bytes(b"zip")
                return SimpleNamespace(
                    conversation_id="conversation-1",
                    primary_path=primary,
                    zip_path=archive,
                )

            def close(self) -> None:
                pass

        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "editable_file_tasks.json"
            root = Path(temp_dir) / "files"
            service = EditableFileTaskService(path)
            account = AccountService(JSONStorageBackend(Path(temp_dir) / "accounts.json"))
            account.add_account_items([{"access_token": "editable-token", "type": "Pro", "status": "正常"}])
            key = "owner-1:late-usage"
            now = editable_module._now_iso()
            service._tasks[key] = {
                "id": "late-usage",
                "owner_id": "owner-1",
                "status": editable_module.TASK_STATUS_QUEUED,
                "kind": "ppt",
                "model": editable_module.EDITABLE_FILE_MODEL,
                "created_at": now,
                "updated_at": now,
                "created_ts": time.time(),
                "updated_ts": time.time(),
            }

            errors: list[BaseException] = []

            def run_task() -> None:
                try:
                    service._run_task(
                        key,
                        "ppt",
                        "prompt",
                        [],
                        OWNER,
                        "",
                        "download-capability",
                    )
                except BaseException as exc:
                    errors.append(exc)

            with (
                mock.patch.object(editable_module, "account_service", account),
                mock.patch.object(editable_module, "EDITABLE_FILE_ROOT", root),
                mock.patch.object(editable_module, "_editable_access_token", return_value="editable-token"),
                mock.patch.object(editable_module, "OpenAIBackendAPI", BlockingBackend),
                mock.patch.object(service, "_log_call"),
            ):
                worker = threading.Thread(target=run_task)
                worker.start()
                self.assertTrue(entered.wait(5))
                account.update_account(
                    "editable-token",
                    {"last_used_at": "2000-01-01 00:00:00", "success": 99},
                )
                release.set()
                worker.join(5)

            self.assertFalse(worker.is_alive())
            self.assertEqual(errors, [])
            current = account.get_account("editable-token")
            self.assertIsNotNone(current)
            self.assertEqual(current["last_used_at"], "2000-01-01 00:00:00")
            self.assertEqual(current["success"], 99)

    def test_editable_lease_capture_failure_does_not_create_backend(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "editable_file_tasks.json"
            service = EditableFileTaskService(path)
            account = AccountService(JSONStorageBackend(Path(temp_dir) / "accounts.json"))
            account.add_account_items([{"access_token": "editable-token", "type": "Pro", "status": "正常"}])
            key = "owner-1:lease-failure"
            now = editable_module._now_iso()
            service._tasks[key] = {
                "id": "lease-failure",
                "owner_id": "owner-1",
                "status": editable_module.TASK_STATUS_QUEUED,
                "kind": "ppt",
                "model": editable_module.EDITABLE_FILE_MODEL,
                "created_at": now,
                "updated_at": now,
                "created_ts": time.time(),
                "updated_ts": time.time(),
            }
            with (
                mock.patch.object(editable_module, "account_service", account),
                mock.patch.object(editable_module, "_editable_access_token", return_value="editable-token"),
                mock.patch.object(
                    account,
                    "_get_account_lease",
                    side_effect=RuntimeError("lease capture failed"),
                ),
                mock.patch.object(editable_module, "OpenAIBackendAPI") as backend,
                mock.patch.object(service, "_log_call"),
            ):
                service._run_task(
                    key,
                    "ppt",
                    "prompt",
                    [],
                    OWNER,
                    "",
                    "download-capability",
                )

            backend.assert_not_called()
            task = service.list_tasks(OWNER, ["lease-failure"])["items"][0]
            self.assertEqual(task["status"], editable_module.TASK_STATUS_ERROR)

    def test_editable_access_token_skips_rate_limited_accounts(self) -> None:
        with mock.patch.object(editable_module.account_service, "get_text_access_token", return_value="ready-token") as select:
            self.assertEqual(editable_module._editable_access_token(), "ready-token")
        select.assert_called_once_with(
            plan_types=editable_module.EDITABLE_FILE_PLAN_TYPES,
            backend_capability="web",
        )

    def test_editable_access_token_rejects_unknown_web_source(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            service = AccountService(JSONStorageBackend(Path(directory) / "accounts.json"))
            service.add_account_items([
                {
                    "access_token": "future-only",
                    "type": "Pro",
                    "source_type": "future-incompatible",
                    "status": "正常",
                },
            ])
            service.refresh_access_token = lambda token, **_kwargs: token
            with mock.patch.object(editable_module, "account_service", service):
                with self.assertRaisesRegex(RuntimeError, "no available plus/team/pro account"):
                    editable_module._editable_access_token()

    def test_editable_access_token_rechecks_account_after_refresh(self) -> None:
        from services.model_service import ModelUnavailableError

        with mock.patch.object(
            editable_module.account_service,
            "get_text_access_token",
            side_effect=ModelUnavailableError("account was revoked during refresh"),
        ):
            with self.assertRaisesRegex(RuntimeError, "no available plus/team/pro account"):
                editable_module._editable_access_token()

    def test_snapshot_read_does_not_follow_path_replacement(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "editable_file_tasks.json"
            replacement = Path(tmp_dir) / "replacement.json"
            displaced = Path(tmp_dir) / "displaced.json"
            original_snapshot = {"tasks": [{
                "id": "original-task",
                "owner_id": "owner-1",
                "status": "success",
                "kind": "ppt",
                "created_at": "2026-08-13 12:00:00",
                "updated_at": "2026-08-13 12:00:00",
                "created_ts": 1,
                "updated_ts": 2,
            }]}
            replacement_snapshot = {"tasks": [{
                **original_snapshot["tasks"][0],
                "id": "replaced-task",
            }]}
            path.write_text(json.dumps(original_snapshot), encoding="utf-8")
            replacement.write_text(json.dumps(replacement_snapshot), encoding="utf-8")
            original_read_text = Path.read_text

            def replace_before_read(path_obj, *args, **kwargs):
                if path_obj == path:
                    path.write_bytes(replacement.read_bytes())
                return original_read_text(path_obj, *args, **kwargs)

            with mock.patch.object(Path, "read_text", autospec=True, side_effect=replace_before_read):
                service = EditableFileTaskService(path)

            self.assertIn("owner-1:original-task", service._tasks)
            self.assertNotIn("owner-1:replaced-task", service._tasks)

    def test_snapshot_read_uses_fixed_handle_after_path_replacement(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "editable_file_tasks.json"
            replacement = Path(tmp_dir) / "replacement.json"
            displaced = Path(tmp_dir) / "displaced.json"
            original_snapshot = {"tasks": [{
                "id": "original-task",
                "owner_id": "owner-1",
                "status": "success",
                "kind": "ppt",
                "created_at": "2026-08-13 12:00:00",
                "updated_at": "2026-08-13 12:00:00",
                "created_ts": 1,
                "updated_ts": 2,
            }]}
            replacement_snapshot = {"tasks": [{
                **original_snapshot["tasks"][0],
                "id": "replaced-task",
            }]}
            path.write_text(json.dumps(original_snapshot), encoding="utf-8")
            replacement.write_text(json.dumps(replacement_snapshot), encoding="utf-8")
            original_open = secure_file.open_no_follow_file

            def replace_after_open(path_obj, *args, **kwargs):
                opened = original_open(path_obj, *args, **kwargs)
                if path_obj == path:
                    path.replace(displaced)
                    replacement.replace(path)
                return opened

            with mock.patch.object(secure_file, "open_no_follow_file", side_effect=replace_after_open):
                service = EditableFileTaskService(path)

            self.assertIn("owner-1:original-task", service._tasks)
            self.assertNotIn("owner-1:replaced-task", service._tasks)

    def test_running_transition_failure_finishes_task_as_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = EditableFileTaskService(Path(tmp_dir) / "editable_file_tasks.json")
            key = "owner-1:running-transition-failure"
            service._tasks[key] = {
                "id": "running-transition-failure",
                "owner_id": "owner-1",
                "status": editable_module.TASK_STATUS_QUEUED,
                "kind": "ppt",
                "model": editable_module.EDITABLE_FILE_MODEL,
                "created_at": "2026-08-16 12:00:00",
                "updated_at": "2026-08-16 12:00:00",
                "created_ts": 1,
                "updated_ts": 1,
            }
            with (
                mock.patch.object(service, "_save_locked", side_effect=[StorageDataError(), None]),
                mock.patch.object(service, "_log_call"),
            ):
                service._run_task(
                    key,
                    "ppt",
                    "prompt",
                    [],
                    OWNER,
                    "",
                    "download-capability",
                )

            task = service.list_tasks(OWNER, ["running-transition-failure"])["items"][0]
            self.assertEqual(task["status"], editable_module.TASK_STATUS_ERROR)
            self.assertEqual(task["error"], "editable file task failed")

    def test_terminal_error_write_failure_does_not_leave_task_running(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = EditableFileTaskService(Path(tmp_dir) / "editable_file_tasks.json")
            key = "owner-1:terminal-write-failure"
            service._tasks[key] = {
                "id": "terminal-write-failure",
                "owner_id": "owner-1",
                "status": editable_module.TASK_STATUS_QUEUED,
                "kind": "ppt",
                "model": editable_module.EDITABLE_FILE_MODEL,
                "created_at": "2026-08-16 12:00:00",
                "updated_at": "2026-08-16 12:00:00",
                "created_ts": 1,
                "updated_ts": 1,
            }
            with (
                mock.patch.object(service, "_save_locked", side_effect=[None, StorageDataError()]),
                mock.patch.object(editable_module, "_editable_access_token", side_effect=RuntimeError("no account")),
                mock.patch.object(service, "_log_call"),
            ):
                service._run_task(
                    key,
                    "ppt",
                    "prompt",
                    [],
                    OWNER,
                    "",
                    "download-capability",
                )

            task = service.list_tasks(OWNER, ["terminal-write-failure"])["items"][0]
            self.assertEqual(task["status"], editable_module.TASK_STATUS_ERROR)
            self.assertEqual(task["error"], "editable file task failed")

    def test_submit_failure_preserves_primary_error_when_rollback_save_fails(self) -> None:
        import services.task_executor as task_executor_module

        with tempfile.TemporaryDirectory() as tmp_dir:
            service = EditableFileTaskService(Path(tmp_dir) / "editable_file_tasks.json")
            with (
                mock.patch.object(
                    task_executor_module._EXECUTOR,
                    "submit",
                    side_effect=RuntimeError("executor submit failed"),
                ),
                mock.patch.object(
                    service,
                    "_save_locked",
                    side_effect=[None, StorageDataError()],
                ),
            ):
                with self.assertRaisesRegex(RuntimeError, "executor submit failed"):
                    service.submit_ppt(
                        OWNER,
                        client_task_id="submit-rollback-failure",
                        prompt="make a deck",
                    )

    def test_corrupt_result_container_fails_closed_without_rewriting_snapshot(self) -> None:
        canary = "editable-result-container-canary"
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "editable_file_tasks.json"
            snapshot = {
                "tasks": [{
                    "id": "corrupt-result",
                    "owner_id": "owner-1",
                    "status": "success",
                    "kind": "ppt",
                    "created_at": "2026-08-13 12:00:00",
                    "updated_at": "2026-08-13 12:00:00",
                    "created_ts": 1,
                    "updated_ts": 2,
                    "result": {"primary_url": "/files/ppt/corrupt-result/primary.pptx", "secret": canary},
                }],
            }
            path.write_text(json.dumps(snapshot), encoding="utf-8")
            original = path.read_bytes()

            with self.assertRaises(StorageDataError):
                EditableFileTaskService(path)

            self.assertEqual(path.read_bytes(), original)

    def test_corrupt_result_url_fails_closed_without_rewriting_snapshot(self) -> None:
        canary = "editable-result-url-canary owner@example.com token"
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "editable_file_tasks.json"
            snapshot = {
                "tasks": [{
                    "id": "corrupt-result-url",
                    "owner_id": "owner-1",
                    "status": "success",
                    "kind": "ppt",
                    "created_at": "2026-08-13 12:00:00",
                    "updated_at": "2026-08-13 12:00:00",
                    "created_ts": 1,
                    "updated_ts": 2,
                    "result": {
                        "conversation_id": "conversation-1",
                        "primary_url": f"https://files.example.test/{canary}",
                    },
                }],
            }
            path.write_text(json.dumps(snapshot), encoding="utf-8")
            original = path.read_bytes()

            with self.assertRaises(StorageDataError):
                EditableFileTaskService(path)

            self.assertEqual(path.read_bytes(), original)

    def test_protocol_relative_result_url_fails_closed_without_public_redirect(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "editable_file_tasks.json"
            snapshot = {
                "tasks": [{
                    "id": "protocol-relative-result",
                    "owner_id": "owner-1",
                    "status": "success",
                    "kind": "ppt",
                    "created_at": "2026-08-13 12:00:00",
                    "updated_at": "2026-08-13 12:00:00",
                    "created_ts": 1,
                    "updated_ts": 2,
                    "result": {
                        "primary_url": "//attacker.example/files/ppt/protocol-relative-result/primary.pptx",
                    },
                }],
            }
            path.write_text(json.dumps(snapshot), encoding="utf-8")
            original = path.read_bytes()

            with self.assertRaises(StorageDataError):
                EditableFileTaskService(path)

            self.assertEqual(path.read_bytes(), original)

    def test_corrupt_error_text_fails_closed_without_public_query_or_rewrite(self) -> None:
        canary = "editable-error-canary owner@example.com token"
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "editable_file_tasks.json"
            snapshot = {
                "tasks": [{
                    "id": "corrupt-error",
                    "owner_id": "owner-1",
                    "status": "error",
                    "kind": "ppt",
                    "created_at": "2026-08-13 12:00:00",
                    "updated_at": "2026-08-13 12:00:00",
                    "created_ts": 1,
                    "updated_ts": 2,
                    "error": canary,
                }],
            }
            path.write_text(json.dumps(snapshot), encoding="utf-8")
            original = path.read_bytes()

            with self.assertRaises(StorageDataError):
                EditableFileTaskService(path)

            self.assertEqual(path.read_bytes(), original)

    def test_corrupt_task_timestamp_text_fails_closed_without_public_leak(self) -> None:
        canary = "editable-timestamp-canary owner@example.com"
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "editable_file_tasks.json"
            snapshot = {
                "tasks": [{
                    "id": "corrupt-timestamp",
                    "owner_id": "owner-1",
                    "status": "success",
                    "kind": "ppt",
                    "created_at": canary,
                    "updated_at": canary,
                    "created_ts": 1,
                    "updated_ts": 2,
                }],
            }
            path.write_text(json.dumps(snapshot), encoding="utf-8")
            original = path.read_bytes()

            with self.assertRaises(StorageDataError):
                EditableFileTaskService(path)

            self.assertEqual(path.read_bytes(), original)

    def test_missing_task_timestamps_fail_closed_without_synthesizing_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "editable_file_tasks.json"
            snapshot = {
                "tasks": [{
                    "id": "missing-timestamps",
                    "owner_id": "owner-1",
                    "status": "success",
                    "kind": "ppt",
                    "created_ts": 1,
                    "updated_ts": 2,
                }],
            }
            path.write_text(json.dumps(snapshot), encoding="utf-8")
            original = path.read_bytes()

            with self.assertRaises(StorageDataError):
                EditableFileTaskService(path)

            self.assertEqual(path.read_bytes(), original)

    def test_recovered_unfinished_tasks_round_trip_across_two_restarts(self) -> None:
        recovery_error = "服务已重启，未完成的任务已中断"
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "editable_file_tasks.json"
            snapshot = {"tasks": [
                {
                    "id": "queued-restart",
                    "owner_id": "owner-1",
                    "status": "queued",
                    "kind": "ppt",
                    "created_at": "2026-08-13 12:00:00",
                    "updated_at": "2026-08-13 12:00:00",
                    "created_ts": 1,
                    "updated_ts": 2,
                },
                {
                    "id": "running-restart",
                    "owner_id": "owner-1",
                    "status": "running",
                    "kind": "psd",
                    "created_at": "2026-08-13 12:00:00",
                    "updated_at": "2026-08-13 12:00:00",
                    "created_ts": 1,
                    "updated_ts": 2,
                    "started_ts": 2,
                },
            ]}
            path.write_text(json.dumps(snapshot, ensure_ascii=False), encoding="utf-8")

            first = EditableFileTaskService(path)
            first_items = first.list_tasks(OWNER, ["queued-restart", "running-restart"])["items"]
            self.assertEqual([item["status"] for item in first_items], ["error", "error"])
            self.assertTrue(all(item["error"] == recovery_error for item in first_items))

            second = EditableFileTaskService(path)
            second_items = second.list_tasks(OWNER, ["queued-restart", "running-restart"])["items"]
            self.assertEqual([item["status"] for item in second_items], ["error", "error"])
            self.assertTrue(all(item["error"] == recovery_error for item in second_items))

    def test_unknown_backend_error_is_not_exposed_or_persisted(self) -> None:
        secret = "opaque-editable-token owner@example.com upstream fragment"
        path = Path(tempfile.mkdtemp()) / "editable_file_tasks.json"

        class FakeBackend:
            def __init__(self, _access_token: str) -> None:
                pass

            def export_ppt_zip(self, _images, _prompt, _output_dir):
                raise RuntimeError(secret)

            def close(self) -> None:
                pass

        service = EditableFileTaskService(path)
        with (
            mock.patch.object(editable_module, "_editable_access_token", return_value="token-editable"),
            mock.patch.object(editable_module, "OpenAIBackendAPI", FakeBackend),
            mock.patch.object(editable_module.log_service, "add") as add,
        ):
            service.submit_ppt(OWNER, client_task_id="secret-error", prompt="make a deck")
            task = wait_for_task(service, "secret-error", "error")

        self.assertNotIn(secret, json.dumps(task, ensure_ascii=False))
        self.assertNotIn(secret, path.read_text(encoding="utf-8"))
        self.assertNotIn(secret, repr(add.call_args_list))

        reloaded = EditableFileTaskService(path)
        reloaded_task = reloaded.list_tasks(OWNER, ["secret-error"])["items"][0]
        self.assertEqual(reloaded_task["status"], "error")
        self.assertEqual(reloaded_task["error"], "editable file task failed")

    def test_successful_editable_task_reports_log_persistence_failure_without_losing_result(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir) / "files"
            path = Path(tmp_dir) / "editable_file_tasks.json"

            class FakeBackend:
                def __init__(self, _access_token: str) -> None:
                    pass

                def export_ppt_zip(self, _images, _prompt, output_dir):
                    output_dir.mkdir(parents=True, exist_ok=True)
                    primary = output_dir / "primary.pptx"
                    archive = output_dir / "assets.zip"
                    primary.write_bytes(b"ppt")
                    archive.write_bytes(b"zip")
                    return SimpleNamespace(
                        conversation_id="conversation-1",
                        primary_path=primary,
                        zip_path=archive,
                    )

                def close(self) -> None:
                    pass

            service = EditableFileTaskService(path)
            with (
                mock.patch.object(editable_module, "EDITABLE_FILE_ROOT", root),
                mock.patch.object(editable_module, "_editable_access_token", return_value="token-editable"),
                mock.patch.object(editable_module, "OpenAIBackendAPI", FakeBackend),
                mock.patch.object(editable_module.log_service, "add", side_effect=OSError("log unavailable")),
                mock.patch.object(editable_module._EDITABLE_TASK_LOGGER, "error") as fallback_logger,
            ):
                service.submit_ppt(OWNER, client_task_id="log-failure-success", prompt="make a deck")
                task = wait_for_task(service, "log-failure-success", "success")

        self.assertEqual(task["status"], "success")
        fallback_logger.assert_called_once_with("editable task log persistence failed")

    def test_unlisted_public_safe_error_is_persisted_as_fixed_fallback(self) -> None:
        canary = "unlisted-editable-safe-canary"

        class FakeBackend:
            def __init__(self, _access_token: str) -> None:
                pass

            def export_ppt_zip(self, _images, _prompt, _output_dir):
                raise PublicSafeError(canary)

            def close(self) -> None:
                pass

        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "editable_file_tasks.json"
            service = EditableFileTaskService(path)
            with (
                mock.patch.object(editable_module, "_editable_access_token", return_value="token-editable"),
                mock.patch.object(editable_module, "OpenAIBackendAPI", FakeBackend),
            ):
                service.submit_ppt(OWNER, client_task_id="safe-fallback", prompt="make a deck")
                task = wait_for_task(service, "safe-fallback", "error")

            self.assertEqual(task["error"], "editable file task failed")
            self.assertNotIn(canary, path.read_text(encoding="utf-8"))

    def test_empty_psd_input_keeps_a_controlled_public_message(self) -> None:
        path = Path(tempfile.mkdtemp()) / "editable_file_tasks.json"
        service = EditableFileTaskService(path)
        with mock.patch.object(editable_module.log_service, "add"):
            task = service.submit_psd(OWNER, client_task_id="empty-psd", prompt="make a psd", base64_images=[])
            task = wait_for_task(service, "empty-psd", "error")

        self.assertEqual(task["error"], "PSD 任务需要至少一张图片")

    def test_client_task_id_cannot_escape_editable_file_root(self) -> None:
        task_ids = (
            "../outside",
            "..\\outside",
            ".",
            "..",
            "/outside",
            r"C:\outside",
            "task,one",
        )
        for task_id in task_ids:
            with self.subTest(task_id=task_id), tempfile.TemporaryDirectory() as tmp_dir:
                service = EditableFileTaskService(Path(tmp_dir) / "editable_file_tasks.json")
                with mock.patch.object(editable_module, "reserve_background_task") as reserve:
                    with self.assertRaises(ValueError):
                        service.submit_ppt(OWNER, client_task_id=task_id, prompt="make a deck")

                    reserve.assert_not_called()

    def test_editable_output_dir_has_root_invariant(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir) / "files"
            with mock.patch.object(editable_module, "EDITABLE_FILE_ROOT", root):
                output_dir = editable_module._editable_output_dir("owner-1", "ppt", "safe-task")
                self.assertEqual(
                    output_dir,
                    (root / editable_module._owner_storage_segment("owner-1") / "ppt" / "safe-task").resolve(),
                )
                with self.assertRaises(ValueError):
                    editable_module._editable_output_dir("owner-1", "ppt", "../../outside")


if __name__ == "__main__":
    unittest.main()
