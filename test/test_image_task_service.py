from __future__ import annotations

import json
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

import services.protocol.conversation as conversation_module
import services.image_task_service as service_module
import services.secure_file as secure_file
from services.account_service import AccountService
from services.image_task_service import ImageTaskService
from services.openai_backend_api import ImagePollTimeoutError
from services.protocol.conversation import ConversationRequest, ImageGenerationError, ImageOutput
from services.protocol.error_response import PublicSafeValueError
from services.storage.json_storage import JSONStorageBackend
from services.storage.base import StorageDataError
from test.fixtures.image_inputs import image_fixture_bytes


OWNER = {"id": "owner-1", "name": "Owner", "role": "admin"}
OTHER_OWNER = {"id": "owner-2", "name": "Other", "role": "user"}


def wait_for_task(service: ImageTaskService, identity: dict[str, object], task_id: str, status: str, timeout: float = 2.0):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        result = service.list_tasks(identity, [task_id])
        last = (result.get("items") or [None])[0]
        if last and last.get("status") == status:
            return last
        time.sleep(0.02)
    raise AssertionError(f"task {task_id} did not reach {status}, last={last}")


class InlineReservation:
    def submit(self, target, *args, **kwargs):
        target(*args, **kwargs)

    def cancel(self):
        pass


class ImageTaskServiceTests(unittest.TestCase):
    def make_service(self, path: Path, handler=None) -> ImageTaskService:
        return ImageTaskService(
            path,
            generation_handler=handler or (lambda _payload: {"data": [{"url": "http://example.test/image.png"}]}),
            edit_handler=handler or (lambda _payload: {"data": [{"url": "http://example.test/edit.png"}]}),
            retention_days_getter=lambda: 30,
        )

    def test_client_task_id_is_bounded_before_reservation(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = self.make_service(Path(tmp_dir) / "image_tasks.json")

            class NoopReservation:
                def submit(self, *_args, **_kwargs):
                    return None

                def cancel(self):
                    return None

            with mock.patch.object(
                service_module,
                "reserve_background_task",
                return_value=NoopReservation(),
            ) as reserve:
                with self.assertRaises(ValueError):
                    service.submit_generation(
                        OWNER,
                        client_task_id="x" * 257,
                        prompt="cat",
                        model="gpt-image-2",
                        size=None,
                    )

                reserve.assert_not_called()

            with mock.patch.object(
                service_module,
                "reserve_background_task",
                return_value=NoopReservation(),
            ) as reserve:
                with self.assertRaises(ValueError):
                    service.submit_generation(
                        OWNER,
                        client_task_id="task,one",
                        prompt="cat",
                        model="gpt-image-2",
                        size=None,
                    )

            reserve.assert_not_called()

    def test_snapshot_read_does_not_follow_path_replacement(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "image_tasks.json"
            replacement = Path(tmp_dir) / "replacement.json"
            displaced = Path(tmp_dir) / "displaced.json"
            original_snapshot = {"tasks": [{
                "id": "original-task",
                "owner_id": "owner-1",
                "status": "success",
                "mode": "generate",
                "model": "gpt-image-2",
                "size": "1024x1024",
                "quality": "auto",
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
                service = self.make_service(path)

            self.assertIn("owner-1:original-task", service._tasks)
            self.assertNotIn("owner-1:replaced-task", service._tasks)

    def test_snapshot_read_uses_fixed_handle_after_path_replacement(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "image_tasks.json"
            replacement = Path(tmp_dir) / "replacement.json"
            displaced = Path(tmp_dir) / "displaced.json"
            original_snapshot = {"tasks": [{
                "id": "original-task",
                "owner_id": "owner-1",
                "status": "success",
                "mode": "generate",
                "model": "gpt-image-2",
                "size": "1024x1024",
                "quality": "auto",
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
                service = self.make_service(path)

            self.assertIn("owner-1:original-task", service._tasks)
            self.assertNotIn("owner-1:replaced-task", service._tasks)

    def test_invalid_image_options_are_rejected_before_task_persistence_or_start(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "image_tasks.json"
            service = self.make_service(path)

            requests = (
                (
                    service.submit_generation,
                    {
                        "client_task_id": "invalid-quality",
                        "prompt": "cat",
                        "model": "gpt-image-2",
                        "size": None,
                        "quality": "ultra",
                        "base_url": "http://local.test",
                    },
                ),
                (
                    service.submit_edit,
                    {
                        "client_task_id": "invalid-edit-size",
                        "prompt": "cat",
                        "model": "gpt-image-2",
                        "size": "1536x864",
                        "quality": "auto",
                        "base_url": "http://local.test",
                    },
                ),
            )

            with mock.patch("services.image_task_service.reserve_background_task") as reserve:
                for submit, kwargs in requests:
                    with self.subTest(task_id=kwargs["client_task_id"]):
                        with self.assertRaises(PublicSafeValueError):
                            submit(OWNER, **kwargs)

            reserve.assert_not_called()
            self.assertFalse(path.exists())
            self.assertEqual(service.list_tasks(OWNER, []), {"items": [], "missing_ids": []})

    def test_duplicate_submit_uses_existing_task(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            calls = 0

            def handler(_payload):
                nonlocal calls
                calls += 1
                time.sleep(0.05)
                return {"data": [{"url": "http://example.test/image.png"}]}

            service = self.make_service(Path(tmp_dir) / "image_tasks.json", handler)
            first = service.submit_generation(
                OWNER,
                client_task_id="task-1",
                prompt="cat",
                model="gpt-image-2",
                size=None,
                base_url="http://local.test",
            )
            second = service.submit_generation(
                OWNER,
                client_task_id="task-1",
                prompt="cat",
                model="gpt-image-2",
                size=None,
                base_url="http://local.test",
            )

            self.assertEqual(first["id"], "task-1")
            self.assertEqual(second["id"], "task-1")
            task = wait_for_task(service, OWNER, "task-1", "success")
            self.assertEqual(task["data"][0]["url"], "http://example.test/image.png")
            self.assertEqual(calls, 1)

    def test_submit_failure_preserves_primary_error_when_rollback_save_fails(self):
        import services.task_executor as task_executor_module

        with tempfile.TemporaryDirectory() as tmp_dir:
            service = self.make_service(Path(tmp_dir) / "image_tasks.json")
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
                    service.submit_generation(
                        OWNER,
                        client_task_id="submit-rollback-failure",
                        prompt="cat",
                        model="gpt-image-2",
                        size=None,
                    )

    def test_terminal_error_write_failure_does_not_leave_task_running(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = self.make_service(Path(tmp_dir) / "image_tasks.json")
            key = "owner-1:terminal-write-failure"
            service._tasks[key] = {
                "id": "terminal-write-failure",
                "owner_id": "owner-1",
                "status": service_module.TASK_STATUS_QUEUED,
                "mode": "generate",
                "model": "gpt-image-2",
                "created_at": "2026-08-16 12:00:00",
                "updated_at": "2026-08-16 12:00:00",
                "created_ts": 1,
                "updated_ts": 1,
            }
            with (
                mock.patch.object(service, "_save_locked", side_effect=[None, StorageDataError()]),
                mock.patch.object(service_module, "_IMAGE_TASK_LOGGER") as fallback_logger,
                mock.patch.object(service, "_log_call"),
            ):
                service._run_task(
                    key,
                    "generate",
                    {"prompt": "cat"},
                    OWNER,
                    "gpt-image-2",
                )

            task = service.list_tasks(OWNER, ["terminal-write-failure"])["items"][0]
            self.assertEqual(task["status"], service_module.TASK_STATUS_ERROR)
            self.assertEqual(task["error"], "image task failed")
            fallback_logger.error.assert_called_once_with("image task terminal state persistence failed")

    def test_initial_running_state_write_failure_does_not_leave_task_queued(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = self.make_service(Path(tmp_dir) / "image_tasks.json")
            key = "owner-1:initial-write-failure"
            service._tasks[key] = {
                "id": "initial-write-failure",
                "owner_id": "owner-1",
                "status": service_module.TASK_STATUS_QUEUED,
                "mode": "generate",
                "model": "gpt-image-2",
                "created_at": "2026-08-16 12:00:00",
                "updated_at": "2026-08-16 12:00:00",
                "created_ts": 1,
                "updated_ts": 1,
            }
            with mock.patch.object(service, "_update_task", side_effect=StorageDataError()):
                service._run_task(
                    key,
                    "generate",
                    {"prompt": "cat"},
                    OWNER,
                    "gpt-image-2",
                )

            task = service.list_tasks(OWNER, ["initial-write-failure"])["items"][0]
            self.assertEqual(task["status"], service_module.TASK_STATUS_ERROR)
            self.assertEqual(task["error"], "image task failed")

    def test_resume_terminal_error_write_failure_does_not_leave_task_running(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = self.make_service(Path(tmp_dir) / "image_tasks.json")
            key = "owner-1:resume-terminal-write-failure"
            service._tasks[key] = {
                "id": "resume-terminal-write-failure",
                "owner_id": "owner-1",
                "status": service_module.TASK_STATUS_RUNNING,
                "mode": "generate",
                "model": "gpt-image-2",
                "conversation_id": "conversation-1",
                "created_at": "2026-08-16 12:00:00",
                "updated_at": "2026-08-16 12:00:00",
                "created_ts": 1,
                "updated_ts": 1,
            }
            service._resume_credentials[key] = "token-resume"
            with (
                mock.patch.object(service, "_save_locked", side_effect=StorageDataError()),
                mock.patch.object(service_module.account_service, "refresh_access_token", return_value="token-resume"),
                mock.patch("services.openai_backend_api.OpenAIBackendAPI", side_effect=RuntimeError("backend failed")),
                mock.patch.object(service_module, "_IMAGE_TASK_LOGGER") as fallback_logger,
                mock.patch.object(service, "_log_call"),
            ):
                service._run_resume_poll(
                    key,
                    "conversation-1",
                    30,
                    "token-resume",
                    OWNER,
                    "generate",
                    "gpt-image-2",
                )

            task = service.list_tasks(OWNER, ["resume-terminal-write-failure"])["items"][0]
            self.assertEqual(task["status"], service_module.TASK_STATUS_ERROR)
            self.assertEqual(task["error"], "image task failed")
            fallback_logger.error.assert_called_once_with("image task terminal state persistence failed")

    def test_different_owner_cannot_query_task(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = self.make_service(Path(tmp_dir) / "image_tasks.json")
            service.submit_generation(
                OWNER,
                client_task_id="private-task",
                prompt="cat",
                model="gpt-image-2",
                size=None,
                base_url="http://local.test",
            )

            wait_for_task(service, OWNER, "private-task", "success")
            result = service.list_tasks(OTHER_OWNER, ["private-task"])

            self.assertEqual(result["items"], [])
            self.assertEqual(result["missing_ids"], ["private-task"])

    def test_success_task_persists_to_new_service_instance(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "image_tasks.json"
            service = self.make_service(path)
            service.submit_generation(
                OWNER,
                client_task_id="persisted-task",
                prompt="cat",
                model="gpt-image-2",
                size=None,
                base_url="http://local.test",
            )
            wait_for_task(service, OWNER, "persisted-task", "success")

            reloaded = self.make_service(path)
            result = reloaded.list_tasks(OWNER, ["persisted-task"])

            self.assertEqual(result["missing_ids"], [])
            self.assertEqual(result["items"][0]["status"], "success")
            self.assertEqual(result["items"][0]["data"][0]["url"], "http://example.test/image.png")

    def test_startup_marks_unfinished_tasks_as_error(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "image_tasks.json"
            path.write_text(
                json.dumps(
                    {
                        "tasks": [
                            {
                                "id": "queued-task",
                                "owner_id": "owner-1",
                                "status": "queued",
                                "mode": "generate",
                                "model": "gpt-image-2",
                                "created_at": "2099-01-01 00:00:00",
                                "updated_at": "2099-01-01 00:00:00",
                            },
                            {
                                "id": "running-task",
                                "owner_id": "owner-1",
                                "status": "running",
                                "mode": "generate",
                                "model": "gpt-image-2",
                                "created_at": "2099-01-01 00:00:00",
                                "updated_at": "2099-01-01 00:00:00",
                            },
                        ]
                    }
                ),
                encoding="utf-8",
            )

            service = self.make_service(path)
            result = service.list_tasks(OWNER, ["queued-task", "running-task"])

            self.assertEqual([item["status"] for item in result["items"]], ["error", "error"])
            self.assertTrue(all("已中断" in item.get("error", "") for item in result["items"]))

    def test_corrupt_task_scalar_container_fails_closed_without_public_stringification(self):
        canary = "image-task-scalar-container-canary"
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "image_tasks.json"
            snapshot = {
                "tasks": [{
                    "id": "corrupt-scalar",
                    "owner_id": "owner-1",
                    "status": "success",
                    "mode": "generate",
                    "model": {"secret": canary},
                    "size": "",
                    "quality": "auto",
                    "created_at": "2026-08-13 12:00:00",
                    "updated_at": "2026-08-13 12:00:00",
                }]
            }
            path.write_text(json.dumps(snapshot), encoding="utf-8")
            original = path.read_bytes()

            with self.assertRaises(StorageDataError):
                ImageTaskService(path)

            self.assertEqual(path.read_bytes(), original)

    def test_corrupt_task_timestamp_text_fails_closed_without_public_leak(self):
        canary = "image-timestamp-canary owner@example.com"
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "image_tasks.json"
            snapshot = {
                "tasks": [{
                    "id": "corrupt-timestamp",
                    "owner_id": "owner-1",
                    "status": "success",
                    "mode": "generate",
                    "created_at": canary,
                    "updated_at": canary,
                    "created_ts": 1,
                    "updated_ts": 2,
                }],
            }
            path.write_text(json.dumps(snapshot), encoding="utf-8")
            original = path.read_bytes()

            with self.assertRaises(StorageDataError):
                ImageTaskService(path)

            self.assertEqual(path.read_bytes(), original)

    def test_missing_or_empty_task_timestamps_fail_closed_without_rewriting_snapshot(self):
        for field_name, field_value in (
            ("created_at", None),
            ("created_at", ""),
            ("updated_at", None),
            ("updated_at", ""),
        ):
            with self.subTest(field_name=field_name, field_value=field_value):
                with tempfile.TemporaryDirectory() as tmp_dir:
                    path = Path(tmp_dir) / "image_tasks.json"
                    task = {
                        "id": "missing-timestamp",
                        "owner_id": "owner-1",
                        "status": "success",
                        "mode": "generate",
                        "model": "gpt-image-2",
                        "created_at": "2026-08-13 12:00:00",
                        "updated_at": "2026-08-13 12:00:00",
                        "created_ts": 1,
                        "updated_ts": 2,
                    }
                    task[field_name] = field_value
                    snapshot = {"tasks": [task]}
                    path.write_text(json.dumps(snapshot), encoding="utf-8")
                    original = path.read_bytes()

                    with self.assertRaises(StorageDataError):
                        ImageTaskService(path)

                    self.assertEqual(path.read_bytes(), original)

    def test_corrupt_task_data_usage_fields_fail_closed_without_rewriting_snapshot(self):
        canary = "image-task-data-usage-canary"
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "image_tasks.json"
            snapshot = {
                "tasks": [{
                    "id": "corrupt-data-usage",
                    "owner_id": "owner-1",
                    "status": "success",
                    "mode": "generate",
                    "model": "gpt-image-2",
                    "size": "",
                    "quality": "auto",
                    "created_at": "2026-08-13 12:00:00",
                    "updated_at": "2026-08-13 12:00:00",
                    "data": [{"url": "/images/x", "internal": canary}],
                    "usage": {"input_tokens": 1, "internal": canary},
                }],
            }
            path.write_text(json.dumps(snapshot), encoding="utf-8")
            original = path.read_bytes()

            with self.assertRaises(StorageDataError):
                ImageTaskService(path)

            self.assertEqual(path.read_bytes(), original)

    def test_corrupt_task_image_url_scheme_fails_closed_without_rewriting_snapshot(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "image_tasks.json"
            snapshot = {
                "tasks": [{
                    "id": "corrupt-image-url",
                    "owner_id": "owner-1",
                    "status": "success",
                    "mode": "generate",
                    "model": "gpt-image-2",
                    "size": "",
                    "quality": "auto",
                    "created_at": "2026-08-13 12:00:00",
                    "updated_at": "2026-08-13 12:00:00",
                    "created_ts": 1,
                    "updated_ts": 2,
                    "data": [{"url": "javascript:alert('image-url-canary')"}],
                }],
            }
            path.write_text(json.dumps(snapshot), encoding="utf-8")
            original = path.read_bytes()

            with self.assertRaises(StorageDataError):
                ImageTaskService(path)

            self.assertEqual(path.read_bytes(), original)

    def test_corrupt_task_numeric_fields_fail_closed_without_rewriting_snapshot(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "image_tasks.json"
            snapshot = {
                "tasks": [{
                    "id": "corrupt-numeric",
                    "owner_id": "owner-1",
                    "status": "success",
                    "mode": "generate",
                    "model": "gpt-image-2",
                    "size": "",
                    "quality": "auto",
                    "created_at": "2026-08-13 12:00:00",
                    "updated_at": "2026-08-13 12:00:00",
                    "created_ts": float("nan"),
                    "updated_ts": 1,
                    "started_ts": 1,
                    "duration_ms": {"secret": "duration-canary"},
                }],
            }
            path.write_text(json.dumps(snapshot, allow_nan=True), encoding="utf-8")
            original = path.read_bytes()

            with self.assertRaises(StorageDataError):
                ImageTaskService(path)

            self.assertEqual(path.read_bytes(), original)

    def test_corrupt_task_progress_fails_closed_without_rewriting_snapshot(self):
        canary = "image-progress-container-canary"
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "image_tasks.json"
            snapshot = {
                "tasks": [{
                    "id": "corrupt-progress",
                    "owner_id": "owner-1",
                    "status": "running",
                    "mode": "generate",
                    "model": "gpt-image-2",
                    "size": "",
                    "quality": "auto",
                    "created_at": "2026-08-13 12:00:00",
                    "updated_at": "2026-08-13 12:00:00",
                    "created_ts": 1,
                    "updated_ts": 2,
                    "started_ts": 2,
                    "progress": {"secret": canary},
                }],
            }
            path.write_text(json.dumps(snapshot), encoding="utf-8")
            original = path.read_bytes()

            with self.assertRaises(StorageDataError):
                ImageTaskService(path)

            self.assertEqual(path.read_bytes(), original)

    def test_corrupt_task_conversation_id_fails_closed_without_rewriting_snapshot(self):
        canary = "image-conversation-canary owner@example.com"
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "image_tasks.json"
            snapshot = {
                "tasks": [{
                    "id": "corrupt-conversation",
                    "owner_id": "owner-1",
                    "status": "error",
                    "mode": "generate",
                    "model": "gpt-image-2",
                    "created_at": "2026-08-13 12:00:00",
                    "updated_at": "2026-08-13 12:00:00",
                    "created_ts": 1,
                    "updated_ts": 2,
                    "conversation_id": canary,
                }],
            }
            path.write_text(json.dumps(snapshot), encoding="utf-8")
            original = path.read_bytes()

            with self.assertRaises(StorageDataError):
                ImageTaskService(path)

            self.assertEqual(path.read_bytes(), original)

    def test_recovered_unfinished_tasks_round_trip_across_two_restarts(self):
        recovery_error = "服务已重启，未完成的图片任务已中断"
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "image_tasks.json"
            snapshot = {"tasks": [
                {
                    "id": "queued-restart",
                    "owner_id": "owner-1",
                    "status": "queued",
                    "mode": "generate",
                    "model": "gpt-image-2",
                    "created_at": "2026-08-13 12:00:00",
                    "updated_at": "2026-08-13 12:00:00",
                    "created_ts": 1,
                    "updated_ts": 2,
                },
                {
                    "id": "running-restart",
                    "owner_id": "owner-1",
                    "status": "running",
                    "mode": "generate",
                    "model": "gpt-image-2",
                    "created_at": "2026-08-13 12:00:00",
                    "updated_at": "2026-08-13 12:00:00",
                    "created_ts": 1,
                    "updated_ts": 2,
                    "started_ts": 2,
                },
            ]}
            path.write_text(json.dumps(snapshot), encoding="utf-8")

            first = ImageTaskService(path)
            first_items = first.list_tasks(OWNER, ["queued-restart", "running-restart"])["items"]
            self.assertEqual([item["status"] for item in first_items], ["error", "error"])
            self.assertTrue(all(item["error"] == recovery_error for item in first_items))

            persisted = json.loads(path.read_text(encoding="utf-8"))
            self.assertTrue(all(
                item.get("error") == recovery_error
                and item.get("error_code") == "image_task_interrupted_on_restart"
                for item in persisted["tasks"]
            ))

            second = ImageTaskService(path)
            second_items = second.list_tasks(OWNER, ["queued-restart", "running-restart"])["items"]
            self.assertEqual([item["status"] for item in second_items], ["error", "error"])
            self.assertTrue(all(item["error"] == recovery_error for item in second_items))

    def test_unknown_task_error_text_is_safely_projected_without_rewriting_snapshot(self):
        canary = "image-error-canary owner@example.com token"
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "image_tasks.json"
            snapshot = {
                "tasks": [{
                    "id": "corrupt-error",
                    "owner_id": "owner-1",
                    "status": "error",
                    "mode": "generate",
                    "model": "gpt-image-2",
                    "size": "",
                    "quality": "auto",
                    "created_at": "2026-08-13 12:00:00",
                    "updated_at": "2026-08-13 12:00:00",
                    "error": canary,
                }],
            }
            path.write_text(json.dumps(snapshot), encoding="utf-8")
            original = path.read_bytes()

            service = ImageTaskService(path, retention_days_getter=lambda: 30)
            item = service.list_tasks(OWNER, ["corrupt-error"])["items"][0]
            self.assertEqual(item["error"], "image task failed")
            self.assertNotIn(canary, repr(item))
            self.assertEqual(path.read_bytes(), original)

    def test_resume_poll_uses_the_generation_account_token(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            def timeout_handler(_payload):
                error = ImagePollTimeoutError.from_timeout(30, "conversation-1")
                error.access_token = "token-owner-1"
                raise error

            path = Path(tmp_dir) / "image_tasks.json"
            service = self.make_service(path, timeout_handler)
            service._log_call = mock.Mock()
            success_transition_credentials = []
            original_save = service._save_locked

            def save_probe():
                task = service._tasks.get("owner-1:resume-task")
                if task and task.get("status") == "success" and not success_transition_credentials:
                    success_transition_credentials.append(
                        service._resume_credentials.get("owner-1:resume-task")
                    )
                original_save()

            service._save_locked = save_probe
            service.submit_generation(
                OWNER,
                client_task_id="resume-task",
                prompt="cat",
                model="gpt-image-2",
                size=None,
                base_url="http://local.test",
            )
            wait_for_task(service, OWNER, "resume-task", "error")
            self.assertNotIn("token-owner-1", path.read_text(encoding="utf-8"))

            class FakeBackend:
                instances = []

                def __init__(self, *, access_token):
                    self.access_token = access_token
                    self.__class__.instances.append(self)

                def _poll_image_results(self, conversation_id, timeout_secs):
                    self.poll_args = (conversation_id, timeout_secs)
                    return ["file-1"], []

                def resolve_conversation_image_urls(self, conversation_id, file_ids, sediment_ids, *, poll):
                    self.resolve_args = (conversation_id, file_ids, sediment_ids, poll)
                    return ["https://files.example/image.png"]

                def download_image_bytes(self, image_urls):
                    self.download_args = image_urls
                    return [image_fixture_bytes("image.png")]

                def close(self):
                    self.closed = True

            with (
                mock.patch("services.image_task_service.account_service.refresh_access_token", return_value="token-refreshed") as refresh,
                mock.patch("services.openai_backend_api.OpenAIBackendAPI", FakeBackend),
                mock.patch("services.image_task_service.reserve_background_task", return_value=InlineReservation()),
            ):
                service.resume_poll(OWNER, "resume-task", 30)
                wait_for_task(service, OWNER, "resume-task", "success")

            result = service.list_tasks(OWNER, ["resume-task"])
            self.assertEqual(result["items"][0]["status"], "success")
            self.assertTrue(FakeBackend.instances[0].closed)
            refresh.assert_called_once_with("token-owner-1", event="image_resume_poll")
            self.assertEqual(FakeBackend.instances[0].access_token, "token-refreshed")
            self.assertEqual(FakeBackend.instances[0].poll_args, ("conversation-1", 30))
            self.assertEqual(success_transition_credentials, [None])
            self.assertEqual(service._resume_credentials, {})
            public_task = service.list_tasks(OWNER, ["resume-task"])["items"][0]
            self.assertNotIn("token-refreshed", json.dumps(public_task, ensure_ascii=False))
            self.assertNotIn("token-refreshed", repr(service._log_call.call_args_list))

    def test_initial_timeout_publishes_resume_credential_before_error(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            handler_finished = threading.Event()
            transition_marked = threading.Event()
            observer_finished = threading.Event()
            observed: dict[str, object] = {}
            service = self.make_service(Path(tmp_dir) / "image_tasks.json")
            service._log_call = mock.Mock()
            service._run_resume_poll = mock.Mock()

            def timeout_handler(_payload):
                error = ImagePollTimeoutError.from_timeout(30, "conversation-atomic")
                error.access_token = "token-atomic"
                handler_finished.set()
                raise error

            service.generation_handler = timeout_handler
            armed = True

            def save_probe():
                nonlocal armed
                if not armed:
                    return original_save()
                task = service._tasks.get("owner-1:atomic-timeout")
                if task and task.get("status") == "error":
                    armed = False
                    transition_marked.set()
                original_save()

            original_save = service._save_locked
            service._save_locked = save_probe

            def observe_and_resume():
                transition_marked.wait(2)
                try:
                    observed["result"] = service.resume_poll(OWNER, "atomic-timeout", 30)
                except Exception as exc:
                    observed["error"] = exc
                finally:
                    observer_finished.set()

            observer = threading.Thread(target=observe_and_resume, daemon=True)
            observer.start()
            service.submit_generation(
                OWNER,
                client_task_id="atomic-timeout",
                prompt="cat",
                model="gpt-image-2",
                size=None,
                base_url="http://local.test",
            )

            self.assertTrue(handler_finished.wait(2))
            self.assertTrue(transition_marked.wait(2))
            self.assertTrue(observer_finished.wait(2))
            observer.join(timeout=2)
            self.assertNotIn("error", observed)
            self.assertEqual(observed["result"]["status"], "running")
            self.assertEqual(service._resume_credentials["owner-1:atomic-timeout"], "token-atomic")

    def test_resume_timeout_keeps_refreshed_credential_for_a_second_resume(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            def timeout_handler(_payload):
                error = ImagePollTimeoutError.from_timeout(30, "conversation-1")
                error.access_token = "token-owner-1"
                raise error

            path = Path(tmp_dir) / "image_tasks.json"
            service = self.make_service(path, timeout_handler)
            service._log_call = mock.Mock()
            service.submit_generation(
                OWNER,
                client_task_id="retry-resume-task",
                prompt="cat",
                model="gpt-image-2",
                size=None,
                base_url="http://local.test",
            )
            wait_for_task(service, OWNER, "retry-resume-task", "error")

            transition_credentials = {}
            original_save = service._save_locked

            def save_probe():
                task = service._tasks.get("owner-1:retry-resume-task")
                if task and task.get("status") == "error" and "error" not in transition_credentials:
                    transition_credentials["error"] = service._resume_credentials.get(
                        "owner-1:retry-resume-task"
                    )
                if task and task.get("status") == "success" and "success" not in transition_credentials:
                    transition_credentials["success"] = service._resume_credentials.get(
                        "owner-1:retry-resume-task"
                    )
                original_save()

            service._save_locked = save_probe

            class FakeBackend:
                instances = []
                poll_calls = 0
                first_timeout_closed = threading.Event()

                def __init__(self, *, access_token):
                    self.access_token = access_token
                    self.__class__.instances.append(self)

                def _poll_image_results(self, conversation_id, timeout_secs):
                    self.__class__.poll_calls += 1
                    if self.__class__.poll_calls == 1:
                        self.timed_out = True
                        raise ImagePollTimeoutError.from_timeout(30, conversation_id)
                    return ["file-1"], []

                def resolve_conversation_image_urls(self, conversation_id, file_ids, sediment_ids, *, poll):
                    return ["https://files.example/image.png"]

                def download_image_bytes(self, image_urls):
                    return [image_fixture_bytes("image.png")]

                def close(self):
                    self.closed = True
                    if getattr(self, "timed_out", False):
                        self.__class__.first_timeout_closed.set()

            with (
                mock.patch(
                    "services.image_task_service.account_service.refresh_access_token",
                    side_effect=["token-resume-1", "token-resume-2"],
                ) as refresh,
                mock.patch("services.openai_backend_api.OpenAIBackendAPI", FakeBackend),
                mock.patch("services.image_task_service.reserve_background_task", return_value=InlineReservation()),
            ):
                service.resume_poll(OWNER, "retry-resume-task", 30)
                self.assertTrue(FakeBackend.first_timeout_closed.wait(2))
                wait_for_task(service, OWNER, "retry-resume-task", "error")
                self.assertEqual(service._resume_credentials["owner-1:retry-resume-task"], "token-resume-1")

                service.resume_poll(OWNER, "retry-resume-task", 30)
                wait_for_task(service, OWNER, "retry-resume-task", "success")

            self.assertEqual(refresh.call_args_list, [
                mock.call("token-owner-1", event="image_resume_poll"),
                mock.call("token-resume-1", event="image_resume_poll"),
            ])
            self.assertEqual([item.access_token for item in FakeBackend.instances], ["token-resume-1", "token-resume-2"])
            self.assertEqual(transition_credentials, {"error": "token-resume-1", "success": None})
            self.assertEqual(service._resume_credentials, {})
            self.assertNotIn("token-resume-1", path.read_text(encoding="utf-8"))
            self.assertNotIn("token-resume-2", path.read_text(encoding="utf-8"))
            public_task = service.list_tasks(OWNER, ["retry-resume-task"])["items"][0]
            self.assertNotIn("token-resume-2", json.dumps(public_task, ensure_ascii=False))
            self.assertNotIn("token-resume-2", repr(service._log_call.call_args_list))

    def test_resume_success_merges_unrelated_snapshot_update(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            def timeout_handler(_payload):
                error = ImagePollTimeoutError.from_timeout(30, "conversation-merge")
                error.access_token = "token-owner-1"
                raise error

            path = Path(tmp_dir) / "image_tasks.json"
            service = self.make_service(path, timeout_handler)
            service._log_call = mock.Mock()
            service.submit_generation(
                OWNER,
                client_task_id="resume-merge-task",
                prompt="cat",
                model="gpt-image-2",
                size=None,
                base_url="http://local.test",
            )
            wait_for_task(service, OWNER, "resume-merge-task", "error")
            worker_closed = threading.Event()

            class FakeBackend:
                def __init__(self, *, access_token):
                    self.access_token = access_token

                def _poll_image_results(self, conversation_id, timeout_secs):
                    snapshot = json.loads(path.read_text(encoding="utf-8"))
                    snapshot["tasks"].append(
                        {
                            "id": "unrelated-task",
                            "owner_id": "owner-2",
                            "status": "success",
                            "mode": "generate",
                            "model": "gpt-image-2",
                            "size": "",
                            "quality": "auto",
                            "created_at": "2026-08-09 00:00:00",
                            "updated_at": "2026-08-09 00:00:00",
                            "data": [{"url": "http://example.test/unrelated.png"}],
                        }
                    )
                    replacement = path.with_suffix(".external.tmp")
                    replacement.write_text(
                        json.dumps(snapshot, ensure_ascii=False, indent=2) + "\n",
                        encoding="utf-8",
                    )
                    replacement.replace(path)
                    return ["file-1"], []

                def resolve_conversation_image_urls(self, conversation_id, file_ids, sediment_ids, *, poll):
                    return ["https://files.example/image.png"]

                def download_image_bytes(self, image_urls):
                    return [image_fixture_bytes("image.png")]

                def close(self):
                    worker_closed.set()

            with (
                mock.patch(
                    "services.image_task_service.account_service.refresh_access_token",
                    return_value="token-refreshed",
                ),
                mock.patch("services.openai_backend_api.OpenAIBackendAPI", FakeBackend),
                mock.patch("services.image_task_service.reserve_background_task", return_value=InlineReservation()),
            ):
                service.resume_poll(OWNER, "resume-merge-task", 30)
                task = wait_for_task(service, OWNER, "resume-merge-task", "success")
                self.assertTrue(worker_closed.wait(5))

            self.assertEqual(task["status"], "success")
            persisted = json.loads(path.read_text(encoding="utf-8"))["tasks"]
            persisted_keys = {(item["owner_id"], item["id"]) for item in persisted}
            self.assertEqual(
                persisted_keys,
                {("owner-1", "resume-merge-task"), ("owner-2", "unrelated-task")},
            )
            self.assertEqual(service._resume_credentials, {})

    def test_resume_does_not_overwrite_newer_transition_of_same_task(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            def timeout_handler(_payload):
                error = ImagePollTimeoutError.from_timeout(30, "conversation-winner")
                error.access_token = "token-owner-1"
                raise error

            path = Path(tmp_dir) / "image_tasks.json"
            service = self.make_service(path, timeout_handler)
            service._log_call = mock.Mock()
            service.submit_generation(
                OWNER,
                client_task_id="resume-winner-task",
                prompt="cat",
                model="gpt-image-2",
                size=None,
                base_url="http://local.test",
            )
            wait_for_task(service, OWNER, "resume-winner-task", "error")
            worker_closed = threading.Event()

            class FakeBackend:
                def __init__(self, *, access_token):
                    self.access_token = access_token

                def _poll_image_results(self, conversation_id, timeout_secs):
                    snapshot = json.loads(path.read_text(encoding="utf-8"))
                    task = next(item for item in snapshot["tasks"] if item["id"] == "resume-winner-task")
                    task.update(
                        status="success",
                        data=[{"url": "http://example.test/external-winner.png"}],
                        error="",
                        updated_at="2026-08-09 00:00:01",
                        updated_ts=float(task.get("updated_ts") or 0) + 1,
                    )
                    replacement = path.with_suffix(".external.tmp")
                    replacement.write_text(
                        json.dumps(snapshot, ensure_ascii=False, indent=2) + "\n",
                        encoding="utf-8",
                    )
                    replacement.replace(path)
                    return ["file-1"], []

                def resolve_conversation_image_urls(self, conversation_id, file_ids, sediment_ids, *, poll):
                    return ["https://files.example/image.png"]

                def download_image_bytes(self, image_urls):
                    return [image_fixture_bytes("image.png")]

                def close(self):
                    worker_closed.set()

            with (
                mock.patch(
                    "services.image_task_service.account_service.refresh_access_token",
                    return_value="token-refreshed",
                ),
                mock.patch("services.openai_backend_api.OpenAIBackendAPI", FakeBackend),
                mock.patch("services.image_task_service.reserve_background_task", return_value=InlineReservation()),
            ):
                service.resume_poll(OWNER, "resume-winner-task", 30)
                self.assertTrue(worker_closed.wait(5))

            task = service.list_tasks(OWNER, ["resume-winner-task"])["items"][0]
            self.assertEqual(task["status"], "success")
            self.assertEqual(task["data"], [{"url": "http://example.test/external-winner.png"}])
            persisted = json.loads(path.read_text(encoding="utf-8"))["tasks"][0]
            self.assertEqual(persisted["status"], "success")
            self.assertEqual(persisted["data"], [{"url": "http://example.test/external-winner.png"}])
            self.assertEqual(service._resume_credentials, {})

    def test_cleanup_removes_expired_resume_credential(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "image_tasks.json"
            service = self.make_service(path)
            key = "owner-1:expired-task"
            with service._lock:
                service._tasks[key] = {
                    "id": "expired-task",
                    "owner_id": "owner-1",
                    "status": "error",
                    "mode": "generate",
                    "model": "gpt-image-2",
                    "size": "",
                    "quality": "auto",
                    "created_at": "2000-01-01 00:00:00",
                    "updated_at": "2000-01-01 00:00:00",
                    "conversation_id": "conversation-expired",
                    "error": "ChatGPT 生图超时",
                }
                service._resume_credentials[key] = "token-expired"

            result = service.list_tasks(OWNER, ["expired-task"])

            self.assertEqual(result["items"], [])
            self.assertEqual(result["missing_ids"], ["expired-task"])
            self.assertEqual(service._resume_credentials, {})
            self.assertNotIn("token-expired", path.read_text(encoding="utf-8"))

    def test_generation_timeout_carries_the_current_account_token(self):
        class FakeBackend:
            instances = []

            def __init__(self, *, access_token):
                self.access_token = access_token
                self.__class__.instances.append(self)

            def close(self):
                self.closed = True

        def timeout_stream(_backend, _request, *_args):
            yield ImageOutput(
                kind="progress",
                model="gpt-image-2",
                index=1,
                total=1,
                conversation_id="conversation-1",
            )
            raise ImagePollTimeoutError.from_timeout(30, "conversation-1")

        with (
            mock.patch.object(conversation_module.account_service, "get_available_access_token", return_value="token-generated"),
            mock.patch.object(conversation_module.account_service, "get_account", return_value={"email": "owner@example.test"}),
            mock.patch.object(conversation_module.account_service, "mark_image_result"),
            mock.patch.object(conversation_module, "OpenAIBackendAPI", FakeBackend),
            mock.patch.object(conversation_module, "stream_image_outputs", timeout_stream),
            mock.patch.object(conversation_module, "_remove_image_conversation_later"),
        ):
            with self.assertRaises(ImagePollTimeoutError) as raised:
                conversation_module._generate_single_image(
                    ConversationRequest(model="gpt-image-2", prompt="cat"),
                    1,
                    1,
                )

        self.assertEqual(raised.exception.access_token, "token-generated")
        self.assertEqual(FakeBackend.instances[0].access_token, "token-generated")
        self.assertTrue(FakeBackend.instances[0].closed)

    def test_generation_without_image_result_marks_failure_once(self):
        class FakeBackend:
            def __init__(self, *, access_token):
                self.access_token = access_token

            def close(self):
                pass

        def progress_only_stream(_backend, _request, *_args):
            yield ImageOutput(
                kind="progress",
                model="gpt-image-2",
                index=1,
                total=1,
                conversation_id="conversation-1",
            )

        with (
            mock.patch.object(conversation_module.account_service, "get_available_access_token", return_value="token-generated"),
            mock.patch.object(conversation_module.account_service, "get_account", return_value={"email": "owner@example.test"}),
            mock.patch.object(conversation_module.account_service, "mark_image_result") as mark_result,
            mock.patch.object(conversation_module.account_service, "release_image_slot") as release_slot,
            mock.patch.object(conversation_module, "OpenAIBackendAPI", FakeBackend),
            mock.patch.object(conversation_module, "stream_image_outputs", progress_only_stream),
            mock.patch.object(conversation_module, "_remove_image_conversation_later"),
        ):
            with self.assertRaisesRegex(ImageGenerationError, "without generating images"):
                conversation_module._generate_single_image(
                    ConversationRequest(model="gpt-image-2", prompt="cat"),
                    1,
                    1,
                )

        mark_result.assert_called_once_with("token-generated", False)
        release_slot.assert_called_once_with("token-generated")

    def test_generation_releases_image_slot_on_success_error_and_timeout(self):
        class FakeBackend:
            def __init__(self, *, access_token):
                self.access_token = access_token

            def close(self):
                pass

        def success_stream(_backend, _request, *_args):
            yield ImageOutput(kind="result", model="gpt-image-2", index=1, total=1)

        def error_stream(_backend, _request, *_args):
            raise RuntimeError("upstream failed")

        def timeout_stream(_backend, _request, *_args):
            yield ImageOutput(
                kind="progress",
                model="gpt-image-2",
                index=1,
                total=1,
                conversation_id="conversation-1",
            )
            raise ImagePollTimeoutError.from_timeout(30, "conversation-1")

        with (
            mock.patch.object(conversation_module.account_service, "get_available_access_token", return_value="token-generated"),
            mock.patch.object(conversation_module.account_service, "get_account", return_value={"email": "owner@example.test"}),
            mock.patch.object(conversation_module.account_service, "mark_image_result"),
            mock.patch.object(conversation_module.account_service, "release_image_slot") as release_slot,
            mock.patch.object(conversation_module, "OpenAIBackendAPI", FakeBackend),
            mock.patch.object(conversation_module, "_remove_image_conversation_later"),
        ):
            for stream, expected_exception in (
                (success_stream, None),
                (error_stream, ImageGenerationError),
                (timeout_stream, ImagePollTimeoutError),
            ):
                with self.subTest(stream=stream.__name__):
                    with mock.patch.object(conversation_module, "stream_image_outputs", stream):
                        if expected_exception is None:
                            outputs = conversation_module._generate_single_image(
                                ConversationRequest(model="gpt-image-2", prompt="cat"),
                                1,
                                1,
                            )
                            self.assertEqual(len(outputs), 1)
                        else:
                            with self.assertRaises(expected_exception):
                                conversation_module._generate_single_image(
                                    ConversationRequest(model="gpt-image-2", prompt="cat"),
                                    1,
                                    1,
                                )

        self.assertEqual(release_slot.call_count, 3)
        self.assertEqual(release_slot.call_args_list, [mock.call("token-generated")] * 3)

    def test_generator_owned_release_happens_exactly_once_after_marking_result(self):
        class FakeBackend:
            def __init__(self, *, access_token):
                self.access_token = access_token

            def close(self):
                pass

        def result_stream(_backend, _request, *_args):
            yield ImageOutput(kind="result", model="gpt-image-2", index=1, total=1)

        with tempfile.TemporaryDirectory() as tmp_dir:
            service = AccountService(JSONStorageBackend(Path(tmp_dir) / "accounts.json"))
            service.add_account_items([{
                "access_token": "token-generated",
                "status": "正常",
                "quota": 3,
            }])
            service._image_inflight["token-generated"] = 2
            with (
                mock.patch.object(service, "get_available_access_token", return_value="token-generated"),
                mock.patch.object(conversation_module, "account_service", service),
                mock.patch.object(conversation_module, "OpenAIBackendAPI", FakeBackend),
                mock.patch.object(conversation_module, "stream_image_outputs", result_stream),
                mock.patch.object(conversation_module, "_remove_image_conversation_later"),
            ):
                outputs = conversation_module._generate_single_image(
                    ConversationRequest(model="gpt-image-2", prompt="cat"),
                    1,
                    1,
                )

            self.assertEqual(len(outputs), 1)
            self.assertEqual(service._image_inflight.get("token-generated"), 1)

    def test_late_image_result_cannot_mutate_replaced_same_token_account(self):
        class FakeBackend:
            def __init__(self, *, access_token):
                self.access_token = access_token

            def close(self):
                pass

        entered = threading.Event()
        release = threading.Event()

        def blocked_result_stream(_backend, _request, *_args):
            entered.set()
            if not release.wait(5):
                raise AssertionError("image stream did not receive release")
            yield ImageOutput(kind="result", model="gpt-image-2", index=1, total=1)

        with tempfile.TemporaryDirectory() as tmp_dir:
            service = AccountService(JSONStorageBackend(Path(tmp_dir) / "accounts.json"))
            service.add_account_items([{
                "access_token": "token-replaced",
                "status": "正常",
                "quota": 3,
                "success": 0,
            }])
            service._image_inflight["token-replaced"] = 1
            errors: list[BaseException] = []
            outputs: list[ImageOutput] = []

            def run_generation() -> None:
                try:
                    outputs.extend(
                        conversation_module._generate_single_image(
                            ConversationRequest(model="gpt-image-2", prompt="cat"),
                            1,
                            1,
                        )
                    )
                except BaseException as exc:
                    errors.append(exc)

            with (
                mock.patch.object(service, "get_available_access_token", return_value="token-replaced"),
                mock.patch.object(conversation_module, "account_service", service),
                mock.patch.object(conversation_module, "OpenAIBackendAPI", FakeBackend),
                mock.patch.object(conversation_module, "stream_image_outputs", blocked_result_stream),
                mock.patch.object(conversation_module, "_remove_image_conversation_later"),
            ):
                worker = threading.Thread(target=run_generation)
                worker.start()
                self.assertTrue(entered.wait(5))

                service.update_account(
                    "token-replaced",
                    {"status": "正常", "quota": 9, "success": 40},
                )
                release.set()
                worker.join(5)

            self.assertFalse(worker.is_alive())
            self.assertEqual(errors, [])
            self.assertEqual(len(outputs), 1)
            current = service.get_account("token-replaced")
            self.assertIsNotNone(current)
            self.assertEqual(current["quota"], 9)
            self.assertEqual(current["success"], 40)
            self.assertNotIn("token-replaced", service._image_inflight)

    def test_late_image_failure_cannot_disable_replaced_same_token_account(self):
        class FakeBackend:
            def __init__(self, *, access_token):
                self.access_token = access_token

            def close(self):
                pass

        entered = threading.Event()
        release = threading.Event()

        def blocked_invalid_stream(_backend, _request, *_args):
            entered.set()
            if not release.wait(5):
                raise AssertionError("image stream did not receive release")
            raise RuntimeError("token_invalidated")

        with tempfile.TemporaryDirectory() as tmp_dir:
            service = AccountService(JSONStorageBackend(Path(tmp_dir) / "accounts.json"))
            service.add_account_items([{
                "access_token": "token-replaced",
                "status": "正常",
                "quota": 3,
                "fail": 0,
            }])
            service._image_inflight["token-replaced"] = 1
            errors: list[BaseException] = []

            def run_generation() -> None:
                try:
                    conversation_module._generate_single_image(
                        ConversationRequest(model="gpt-image-2", prompt="cat"),
                        1,
                        1,
                    )
                except BaseException as exc:
                    errors.append(exc)

            with (
                mock.patch.object(service, "get_available_access_token", side_effect=[
                    "token-replaced",
                    RuntimeError("no available image quota"),
                ]),
                mock.patch.object(service, "refresh_access_token", return_value=""),
                mock.patch.object(conversation_module, "account_service", service),
                mock.patch.object(conversation_module, "OpenAIBackendAPI", FakeBackend),
                mock.patch.object(conversation_module, "stream_image_outputs", blocked_invalid_stream),
                mock.patch.object(conversation_module, "_remove_image_conversation_later"),
            ):
                worker = threading.Thread(target=run_generation)
                worker.start()
                self.assertTrue(entered.wait(5))

                service.update_account(
                    "token-replaced",
                    {"status": "正常", "quota": 9, "fail": 40},
                )
                release.set()
                worker.join(5)

            self.assertFalse(worker.is_alive())
            self.assertEqual(len(errors), 1)
            self.assertIsInstance(errors[0], ImageGenerationError)
            current = service.get_account("token-replaced")
            self.assertIsNotNone(current)
            self.assertEqual(current["status"], "正常")
            self.assertEqual(current["quota"], 9)
            self.assertEqual(current["fail"], 40)
            self.assertNotIn("token-replaced", service._image_inflight)

    def test_generation_releases_the_token_acquired_before_refresh_retry(self):
        class FakeBackend:
            def __init__(self, *, access_token):
                self.access_token = access_token

            def close(self):
                pass

        stream_calls = 0

        def retrying_stream(_backend, _request, *_args):
            nonlocal stream_calls
            stream_calls += 1
            if stream_calls == 1:
                raise RuntimeError("token_invalidated")
            yield ImageOutput(kind="result", model="gpt-image-2", index=1, total=1)

        with (
            mock.patch.object(
                conversation_module.account_service,
                "get_available_access_token",
                side_effect=["token-one", "token-two"],
            ),
            mock.patch.object(
                conversation_module.account_service,
                "get_account",
                return_value={"email": "owner@example.test"},
            ),
            mock.patch.object(
                conversation_module.account_service,
                "refresh_access_token",
                return_value="token-two",
            ),
            mock.patch.object(conversation_module.account_service, "mark_image_result"),
            mock.patch.object(conversation_module.account_service, "release_image_slot") as release_slot,
            mock.patch.object(conversation_module, "OpenAIBackendAPI", FakeBackend),
            mock.patch.object(conversation_module, "stream_image_outputs", retrying_stream),
            mock.patch.object(conversation_module, "_remove_image_conversation_later"),
        ):
            outputs = conversation_module._generate_single_image(
                ConversationRequest(model="gpt-image-2", prompt="cat"),
                1,
                1,
            )

        self.assertEqual(len(outputs), 1)
        self.assertEqual(
            release_slot.call_args_list,
            [mock.call("token-one"), mock.call("token-two")],
        )

    def test_generation_does_not_release_when_token_acquisition_fails(self):
        with (
            mock.patch.object(
                conversation_module.account_service,
                "get_available_access_token",
                side_effect=RuntimeError("no available image quota"),
            ),
            mock.patch.object(conversation_module.account_service, "release_image_slot") as release_slot,
        ):
            with self.assertRaises(ImageGenerationError):
                conversation_module._generate_single_image(
                    ConversationRequest(model="gpt-image-2", prompt="cat"),
                    1,
                    1,
                )

        release_slot.assert_not_called()

    def test_generation_releases_when_backend_constructor_fails(self):
        with (
            mock.patch.object(
                conversation_module.account_service,
                "get_available_access_token",
                return_value="token-generated",
            ),
            mock.patch.object(
                conversation_module.account_service,
                "get_account",
                return_value={"email": "owner@example.test"},
            ),
            mock.patch.object(conversation_module.account_service, "mark_image_result"),
            mock.patch.object(conversation_module.account_service, "release_image_slot") as release_slot,
            mock.patch.object(
                conversation_module,
                "OpenAIBackendAPI",
                side_effect=RuntimeError("backend constructor failed"),
            ),
        ):
            with self.assertRaises(ImageGenerationError):
                conversation_module._generate_single_image(
                    ConversationRequest(model="gpt-image-2", prompt="cat"),
                    1,
                    1,
                )

        release_slot.assert_called_once_with("token-generated")

    def test_generation_releases_when_account_lookup_fails(self):
        with (
            mock.patch.object(
                conversation_module.account_service,
                "get_available_access_token",
                return_value="token-generated",
            ),
            mock.patch.object(
                conversation_module.account_service,
                "get_account",
                side_effect=RuntimeError("account lookup failed"),
            ),
            mock.patch.object(conversation_module.account_service, "mark_image_result"),
            mock.patch.object(conversation_module.account_service, "release_image_slot") as release_slot,
        ):
            with self.assertRaises(ImageGenerationError):
                conversation_module._generate_single_image(
                    ConversationRequest(model="gpt-image-2", prompt="cat"),
                    1,
                    1,
                )

        release_slot.assert_called_once_with("token-generated")

    def test_generation_releases_when_account_lease_capture_fails(self):
        with (
            mock.patch.object(
                conversation_module.account_service,
                "get_available_access_token",
                return_value="token-generated",
            ),
            mock.patch.object(
                conversation_module.account_service,
                "_get_account_lease",
                side_effect=RuntimeError("account lease capture failed"),
            ),
            mock.patch.object(conversation_module.account_service, "mark_image_result"),
            mock.patch.object(conversation_module.account_service, "release_image_slot") as release_slot,
        ):
            with self.assertRaises(ImageGenerationError):
                conversation_module._generate_single_image(
                    ConversationRequest(model="gpt-image-2", prompt="cat"),
                    1,
                    1,
                )

        release_slot.assert_called_once_with("token-generated")

    def test_generation_releases_once_when_backend_close_fails(self):
        class ClosingBackend:
            def __init__(self, *, access_token):
                self.access_token = access_token

            def close(self):
                raise RuntimeError("backend close failed")

        def result_stream(_backend, _request, *_args):
            yield ImageOutput(kind="result", model="gpt-image-2", index=1, total=1)

        with (
            mock.patch.object(
                conversation_module.account_service,
                "get_available_access_token",
                return_value="token-generated",
            ),
            mock.patch.object(
                conversation_module.account_service,
                "get_account",
                return_value={"email": "owner@example.test"},
            ),
            mock.patch.object(conversation_module.account_service, "mark_image_result"),
            mock.patch.object(conversation_module.account_service, "release_image_slot") as release_slot,
            mock.patch.object(conversation_module, "OpenAIBackendAPI", ClosingBackend),
            mock.patch.object(conversation_module, "stream_image_outputs", result_stream),
            mock.patch.object(conversation_module, "_remove_image_conversation_later"),
        ):
            with self.assertRaisesRegex(RuntimeError, "backend close failed"):
                conversation_module._generate_single_image(
                    ConversationRequest(model="gpt-image-2", prompt="cat"),
                    1,
                    1,
                )

        release_slot.assert_called_once_with("token-generated")

    def test_generation_does_not_mark_the_same_lease_twice_when_accounting_fails(self):
        class FakeBackend:
            def __init__(self, *, access_token):
                self.access_token = access_token

            def close(self):
                pass

        def result_stream(_backend, _request, *_args):
            yield ImageOutput(kind="result", model="gpt-image-2", index=1, total=1)

        with (
            mock.patch.object(
                conversation_module.account_service,
                "get_available_access_token",
                return_value="token-generated",
            ),
            mock.patch.object(
                conversation_module.account_service,
                "get_account",
                return_value={"email": "owner@example.test"},
            ),
            mock.patch.object(
                conversation_module.account_service,
                "mark_image_result",
                side_effect=RuntimeError("account snapshot write failed"),
            ) as mark_result,
            mock.patch.object(conversation_module.account_service, "release_image_slot") as release_slot,
            mock.patch.object(conversation_module, "OpenAIBackendAPI", FakeBackend),
            mock.patch.object(conversation_module, "stream_image_outputs", result_stream),
            mock.patch.object(conversation_module, "_remove_image_conversation_later"),
        ):
            with self.assertRaises(Exception):
                conversation_module._generate_single_image(
                    ConversationRequest(model="gpt-image-2", prompt="cat"),
                    1,
                    1,
                )

        mark_result.assert_called_once_with("token-generated", True)
        release_slot.assert_called_once_with("token-generated")

    def test_resume_credentials_are_isolated_by_owner(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            def timeout_handler(_payload):
                error = ImagePollTimeoutError.from_timeout(30, "conversation-1")
                error.access_token = "token-owner-1"
                raise error

            service = self.make_service(Path(tmp_dir) / "image_tasks.json", timeout_handler)
            service.submit_generation(
                OWNER,
                client_task_id="private-resume-task",
                prompt="cat",
                model="gpt-image-2",
                size=None,
                base_url="http://local.test",
            )
            wait_for_task(service, OWNER, "private-resume-task", "error")

            with self.assertRaisesRegex(ValueError, "task not found"):
                service.resume_poll(OTHER_OWNER, "private-resume-task", 30)
            self.assertEqual(set(service._resume_credentials), {"owner-1:private-resume-task"})

    def test_non_timeout_failure_does_not_keep_resume_credentials(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            def failure_handler(_payload):
                error = RuntimeError("ordinary generation failure")
                error.conversation_id = "conversation-ordinary"
                error.access_token = "token-ordinary"
                raise error

            path = Path(tmp_dir) / "image_tasks.json"
            service = self.make_service(path, failure_handler)
            service.submit_generation(
                OWNER,
                client_task_id="ordinary-failure",
                prompt="cat",
                model="gpt-image-2",
                size=None,
                base_url="http://local.test",
            )
            wait_for_task(service, OWNER, "ordinary-failure", "error")

            self.assertEqual(service._resume_credentials, {})
            self.assertNotIn("token-ordinary", path.read_text(encoding="utf-8"))

            reloaded = self.make_service(path)
            reloaded_task = reloaded.list_tasks(OWNER, ["ordinary-failure"])["items"][0]
            self.assertEqual(reloaded_task["status"], "error")
            self.assertEqual(reloaded_task["error"], "image task failed")

    def test_self_produced_resume_failure_snapshot_round_trips(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "image_tasks.json"

            def timeout_handler(_payload):
                error = ImagePollTimeoutError.from_timeout(30, "conversation-round-trip")
                error.access_token = "token-round-trip"
                raise error

            service = self.make_service(path, timeout_handler)
            service.submit_generation(
                OWNER,
                client_task_id="resume-round-trip",
                prompt="cat",
                model="gpt-image-2",
                size=None,
                base_url="http://local.test",
            )
            wait_for_task(service, OWNER, "resume-round-trip", "error")

            class FakeBackend:
                def __init__(self, *, access_token):
                    self.access_token = access_token

                def _poll_image_results(self, _conversation_id, _timeout_secs):
                    raise RuntimeError("opaque upstream resume failure")

                def close(self):
                    pass

            with (
                mock.patch(
                    "services.image_task_service.account_service.refresh_access_token",
                    return_value="token-round-trip",
                ),
                mock.patch("services.openai_backend_api.OpenAIBackendAPI", FakeBackend),
                mock.patch("services.image_task_service.reserve_background_task", return_value=InlineReservation()),
            ):
                service.resume_poll(OWNER, "resume-round-trip", 30)
                resumed = wait_for_task(service, OWNER, "resume-round-trip", "error")

            self.assertEqual(resumed["error"], "image task failed")
            reloaded = self.make_service(path)
            reloaded_task = reloaded.list_tasks(OWNER, ["resume-round-trip"])["items"][0]
            self.assertEqual(reloaded_task["status"], "error")
            self.assertEqual(reloaded_task["error"], "image task failed")

    def test_untrusted_result_message_is_not_exposed_or_logged(self):
        secret = "opaque-result-token owner@example.com upstream fragment"

        def handler(_payload):
            return {"data": [], "message": secret}

        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "image_tasks.json"
            service = self.make_service(path, handler)
            with mock.patch("services.image_task_service.log_service.add") as add:
                service.submit_generation(
                    OWNER,
                    client_task_id="untrusted-message",
                    prompt="cat",
                    model="gpt-image-2",
                    size=None,
                    base_url="http://local.test",
                )
                task = wait_for_task(service, OWNER, "untrusted-message", "error")

            self.assertNotIn(secret, json.dumps(task, ensure_ascii=False))
            self.assertNotIn(secret, path.read_text(encoding="utf-8"))
        self.assertNotIn(secret, repr(add.call_args_list))

    def test_successful_image_task_reports_log_persistence_failure_without_losing_result(self):
        def handler(_payload):
            return {"data": [{"url": "https://example.test/image.png"}]}

        with tempfile.TemporaryDirectory() as tmp_dir:
            service = self.make_service(Path(tmp_dir) / "image_tasks.json", handler)
            with (
                mock.patch.object(service_module.log_service, "add", side_effect=OSError("log unavailable")),
                mock.patch.object(service_module._IMAGE_TASK_LOGGER, "error") as fallback_logger,
            ):
                service.submit_generation(
                    OWNER,
                    client_task_id="log-failure-success",
                    prompt="cat",
                    model="gpt-image-2",
                    size=None,
                    base_url="http://local.test",
                )
                task = wait_for_task(service, OWNER, "log-failure-success", "success")

        self.assertEqual(task["status"], "success")
        fallback_logger.assert_called_once_with("image task log persistence failed")

    def test_public_task_projects_image_data_and_usage_fields(self):
        secret = "image-task-public-canary"

        def handler(_payload):
            return {
                "data": [
                    {
                        "b64_json": "ZmFrZQ==",
                        "url": "https://example.test/image.png",
                        "revised_prompt": "a safe prompt",
                        "internal_metadata": {"secret": secret},
                    }
                ],
                "usage": {
                    "input_tokens": 3,
                    "output_tokens": 7,
                    "total_tokens": 10,
                    "input_tokens_details": {
                        "text_tokens": 3,
                        "image_tokens": 0,
                        "cached_tokens": 0,
                        "secret": secret,
                    },
                    "output_tokens_details": {
                        "text_tokens": 0,
                        "image_tokens": 7,
                        "reasoning_tokens": 0,
                        "secret": secret,
                    },
                    "internal_metadata": {"secret": secret},
                },
            }

        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "image_tasks.json"
            service = self.make_service(path, handler)
            service.submit_generation(
                OWNER,
                client_task_id="public-projection",
                prompt="cat",
                model="gpt-image-2",
                size=None,
                base_url="http://local.test",
            )
            task = wait_for_task(service, OWNER, "public-projection", "success")
            persisted = path.read_text(encoding="utf-8")

        serialized = json.dumps(task, ensure_ascii=False)
        self.assertNotIn(secret, serialized)
        self.assertNotIn(secret, persisted)
        self.assertEqual(task["data"], [{
            "b64_json": "ZmFrZQ==",
            "url": "https://example.test/image.png",
            "revised_prompt": "a safe prompt",
        }])
        self.assertEqual(task["usage"], {
            "input_tokens": 3,
            "output_tokens": 7,
            "total_tokens": 10,
            "input_tokens_details": {
                "text_tokens": 3,
                "image_tokens": 0,
                "cached_tokens": 0,
            },
            "output_tokens_details": {
                "text_tokens": 0,
                "image_tokens": 7,
                "reasoning_tokens": 0,
            },
        })

    def test_image_result_url_query_is_not_written_to_call_log(self):
        canary = "signed-image-query-canary"

        def handler(_payload):
            return {"data": [{"url": f"https://cdn.example.test/image.png?sig={canary}"}]}

        with tempfile.TemporaryDirectory() as tmp_dir:
            service = self.make_service(Path(tmp_dir) / "image_tasks.json", handler)
            with mock.patch.object(service_module.log_service, "add") as add:
                service.submit_generation(
                    OWNER,
                    client_task_id="signed-image-url",
                    prompt="cat",
                    model="gpt-image-2",
                    size=None,
                    base_url="http://local.test",
                )
                wait_for_task(service, OWNER, "signed-image-url", "success")

        self.assertNotIn(canary, repr(add.call_args_list))

    def test_resume_runtime_error_is_not_exposed_or_logged(self):
        secret = "opaque-resume-token owner@example.com upstream fragment"

        def timeout_handler(_payload):
            error = ImagePollTimeoutError.from_timeout(30, "conversation-1")
            error.access_token = "token-owner-1"
            raise error

        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "image_tasks.json"
            service = self.make_service(path, timeout_handler)
            with mock.patch("services.image_task_service.log_service.add") as add:
                service.submit_generation(
                    OWNER,
                    client_task_id="resume-runtime-error",
                    prompt="cat",
                    model="gpt-image-2",
                    size=None,
                    base_url="http://local.test",
                )
                wait_for_task(service, OWNER, "resume-runtime-error", "error")

                transition_credentials = []
                original_save = service._save_locked

                def save_probe():
                    task = service._tasks.get("owner-1:resume-runtime-error")
                    if task and task.get("status") == "error" and not transition_credentials:
                        transition_credentials.append(
                            service._resume_credentials.get("owner-1:resume-runtime-error")
                        )
                    original_save()

                service._save_locked = save_probe

                class FakeBackend:
                    def __init__(self, *, access_token):
                        self.access_token = access_token

                    def _poll_image_results(self, conversation_id, timeout_secs):
                        raise RuntimeError(secret)

                    def close(self):
                        pass

                with (
                    mock.patch(
                        "services.image_task_service.account_service.refresh_access_token",
                        return_value="token-owner-1",
                    ),
                    mock.patch("services.openai_backend_api.OpenAIBackendAPI", FakeBackend),
                    mock.patch("services.image_task_service.reserve_background_task", return_value=InlineReservation()),
                ):
                    service.resume_poll(OWNER, "resume-runtime-error", 30)
                    task = wait_for_task(service, OWNER, "resume-runtime-error", "error")

            self.assertNotIn(secret, json.dumps(task, ensure_ascii=False))
            self.assertNotIn(secret, path.read_text(encoding="utf-8"))
            self.assertNotIn(secret, repr(add.call_args_list))
            self.assertEqual(transition_credentials, [None])


if __name__ == "__main__":
    unittest.main()
