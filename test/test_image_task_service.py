from __future__ import annotations

import json
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

import services.protocol.conversation as conversation_module
from services.image_task_service import ImageTaskService
from services.openai_backend_api import ImagePollTimeoutError
from services.protocol.conversation import ConversationRequest, ImageOutput
from services.protocol.error_response import PublicSafeValueError
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
