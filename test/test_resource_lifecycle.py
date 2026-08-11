from __future__ import annotations

import asyncio
import hashlib
import json
import threading
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from fastapi import FastAPI
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

import api.accounts as accounts_api_module
import api.ai as ai_api_module
from api.errors import install_exception_handlers
import services.protocol.conversation as conversation_module
import services.protocol.web_search_tool as web_search_module
import services.backup_service as backup_module
import services.content_filter as content_filter_module
import services.image_storage_service as image_storage_module
import services.account_service as account_module
import services.auth_service as auth_module
import services.editable_file_task_service as editable_task_module
import services.image_task_service as image_task_module
import services.openai_backend_api as backend_module
from services.log_service import LoggedCall, iterate_ai_chunks
from services.protocol.conversation import ConversationRequest, stream_text_deltas
from services.backup_service import BackupError, BackupService
from services.storage.base import StorageDataError
from services.storage.json_storage import JSONStorageBackend


class _SequencedGateLock:
    def __init__(self, expected_waiters: int) -> None:
        self._condition = threading.Condition()
        self._entered = 0
        self._expected_waiters = expected_waiters
        self._released = False
        self._next_ticket = 0

    def __enter__(self):
        with self._condition:
            ticket = self._entered
            self._entered += 1
            self._condition.notify_all()
            self._condition.wait_for(
                lambda: self._released and ticket == self._next_ticket,
            )
        return self

    def __exit__(self, *_args) -> None:
        with self._condition:
            self._next_ticket += 1
            self._condition.notify_all()

    def wait_until_queued(self) -> None:
        with self._condition:
            if not self._condition.wait_for(
                lambda: self._entered == self._expected_waiters,
                timeout=5,
            ):
                raise AssertionError("authentication workers did not reach the storage gate")

    def wait_for_entered(self, count: int) -> None:
        with self._condition:
            if not self._condition.wait_for(lambda: self._entered >= count, timeout=5):
                raise AssertionError("authentication worker did not reach the storage gate")

    def release(self) -> None:
        with self._condition:
            self._released = True
            self._condition.notify_all()


class ResourceLifecycleTests(unittest.TestCase):
    def test_concurrent_authentication_wave_reuses_one_current_snapshot_read(self) -> None:
        worker_count = 8
        raw_key = "concurrent-auth-key"
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            accounts_path = root / "accounts.json"
            auth_keys_path = root / "auth_keys.json"
            accounts_path.write_text("[]", encoding="utf-8")
            auth_keys_path.write_text(
                json.dumps({
                    "items": [{
                        "id": "base-key",
                        "name": "base",
                        "role": "user",
                        "key_hash": hashlib.sha256(raw_key.encode("utf-8")).hexdigest(),
                        "enabled": True,
                    }],
                }),
                encoding="utf-8",
            )
            backend = JSONStorageBackend(accounts_path, auth_keys_path)
            service = auth_module.AuthService(backend)
            # Keep this test on the read path; durable last_used_at flushing has
            # an independent CAS contract.
            service._last_used_flush_at["base-key"] = auth_module.datetime.now(auth_module.timezone.utc)
            gate = _SequencedGateLock(worker_count)
            service._lock = gate

            with (
                mock.patch.object(
                    backend,
                    "load_auth_keys_snapshot",
                    wraps=backend.load_auth_keys_snapshot,
                ) as load_snapshot,
                ThreadPoolExecutor(max_workers=worker_count) as executor,
            ):
                futures = [executor.submit(service.authenticate, raw_key) for _ in range(worker_count)]
                try:
                    gate.wait_until_queued()
                finally:
                    gate.release()
                identities = [future.result(timeout=5) for future in futures]

        self.assertTrue(all(identity is not None for identity in identities))
        self.assertEqual(load_snapshot.call_count, 1)

    def test_authentication_wave_invalidates_shared_snapshot_after_corrupt_audit_write(self) -> None:
        owner_key = "auth-owner-key"
        waiter_key = "auth-waiter-key"
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            accounts_path = root / "accounts.json"
            auth_keys_path = root / "auth_keys.json"
            accounts_path.write_text("[]", encoding="utf-8")
            auth_keys_path.write_text(
                json.dumps({
                    "items": [
                        {
                            "id": "owner-key",
                            "name": "owner",
                            "role": "user",
                            "key_hash": hashlib.sha256(owner_key.encode("utf-8")).hexdigest(),
                            "enabled": True,
                        },
                        {
                            "id": "waiter-key",
                            "name": "waiter",
                            "role": "user",
                            "key_hash": hashlib.sha256(waiter_key.encode("utf-8")).hexdigest(),
                            "enabled": True,
                        },
                    ],
                }),
                encoding="utf-8",
            )
            backend = JSONStorageBackend(accounts_path, auth_keys_path)
            service = auth_module.AuthService(backend)
            service._last_used_flush_at["waiter-key"] = auth_module.datetime.now(auth_module.timezone.utc)
            gate = _SequencedGateLock(2)
            service._lock = gate
            original_save = backend.save_auth_keys_if_revision

            def corrupt_before_audit_write(expected, auth_keys):
                auth_keys_path.write_text("{broken", encoding="utf-8")
                return original_save(expected, auth_keys)

            with (
                mock.patch.object(
                    backend,
                    "save_auth_keys_if_revision",
                    side_effect=corrupt_before_audit_write,
                ),
                ThreadPoolExecutor(max_workers=2) as executor,
            ):
                owner = executor.submit(service.authenticate, owner_key)
                gate.wait_for_entered(1)
                waiter = executor.submit(service.authenticate, waiter_key)
                try:
                    gate.wait_until_queued()
                finally:
                    gate.release()

                with self.assertRaises(StorageDataError):
                    owner.result(timeout=5)
                with self.assertRaises(StorageDataError):
                    waiter.result(timeout=5)

            self.assertEqual(auth_keys_path.read_text(encoding="utf-8"), "{broken")

    def test_production_container_disables_per_request_access_logging(self) -> None:
        dockerfile = Path(__file__).resolve().parents[1] / "Dockerfile"
        command_line = next(
            line.strip()
            for line in dockerfile.read_text(encoding="utf-8").splitlines()
            if line.strip().startswith("CMD ")
        )
        command = json.loads(command_line.removeprefix("CMD "))

        self.assertNotIn("--access-log", command)
        self.assertIn("--no-access-log", command)

    def test_responses_websocket_connections_are_globally_bounded(self) -> None:
        class FakeWebSocket:
            def __init__(self, release: asyncio.Event) -> None:
                self.headers = {"authorization": "Bearer key"}
                self.release = release
                self.accepted = False
                self.received = False
                self.sent: list[dict] = []
                self.closed: list[tuple[int, str]] = []

            async def accept(self) -> None:
                self.accepted = True

            async def receive_json(self):
                self.received = True
                await self.release.wait()
                raise WebSocketDisconnect()

            async def send_json(self, payload: dict) -> None:
                self.sent.append(payload)

            async def close(self, code: int = 1000, reason: str = "") -> None:
                self.closed.append((code, reason))

        async def scenario() -> FakeWebSocket:
            endpoint = next(
                route.endpoint
                for route in ai_api_module.create_router().routes
                if getattr(route, "path", "") == "/v1/responses"
                and "websocket" in type(route).__name__.lower()
            )
            release = asyncio.Event()
            holders = [FakeWebSocket(release) for _ in range(64)]
            tasks = [asyncio.create_task(endpoint(websocket)) for websocket in holders]
            try:
                while not all(websocket.accepted for websocket in holders):
                    await asyncio.sleep(0)
                overflow_release = asyncio.Event()
                overflow_release.set()
                overflow = FakeWebSocket(overflow_release)
                await endpoint(overflow)
                return overflow
            finally:
                release.set()
                await asyncio.gather(*tasks)

        with mock.patch.object(ai_api_module, "require_identity_async", new=mock.AsyncMock(return_value={"id": "owner"})):
            overflow = asyncio.run(scenario())

        self.assertTrue(overflow.accepted)
        self.assertFalse(overflow.received)
        self.assertEqual(overflow.sent[0]["error"]["code"], "websocket_connection_capacity_reached")
        self.assertEqual(overflow.closed[0][0], 1013)

    def test_ai_review_closes_upstream_response(self) -> None:
        class FakeResponse:
            status_code = 200

            def __init__(self) -> None:
                self.closed = False

            def json(self):
                return {"choices": [{"message": {"content": "ALLOW"}}]}

            def close(self) -> None:
                self.closed = True

        response = FakeResponse()
        review_config = {
            "enabled": True,
            "base_url": "https://review.example.test",
            "api_key": "review-key",
            "model": "review-model",
        }
        with (
            mock.patch.object(
                content_filter_module,
                "config",
                SimpleNamespace(sensitive_words=[], ai_review=review_config),
            ),
            mock.patch.object(content_filter_module.requests, "post", return_value=response),
        ):
            content_filter_module.check_request("review this request")

        self.assertTrue(response.closed)

    def test_account_refresh_reuses_one_process_executor_with_worker_sized_batches(self) -> None:
        observations: list[int] = []

        class FakeFuture:
            def result(self):
                return {}

        class RecordingExecutor:
            def __init__(self, *, max_workers: int) -> None:
                self.max_workers = max_workers
                self.submitted: list[FakeFuture] = []

            def submit(self, *_args, **_kwargs):
                future = FakeFuture()
                self.submitted.append(future)
                return future

        def recording_as_completed(futures):
            observations.append(len(futures))
            return iter(futures)

        service = account_module.AccountService.__new__(account_module.AccountService)
        service._lock = account_module.Lock()
        service._accounts = {}
        service._token_aliases = {}
        service.list_accounts = mock.Mock(return_value=[])
        executor = RecordingExecutor(max_workers=10)

        with (
            mock.patch.object(account_module, "_ACCOUNT_REFRESH_EXECUTOR", executor, create=True),
            mock.patch.object(
                account_module,
                "ThreadPoolExecutor",
                side_effect=AssertionError("account refresh must not create a per-call executor"),
            ),
            mock.patch.object(account_module, "as_completed", side_effect=recording_as_completed),
            mock.patch.object(account_module, "config", SimpleNamespace(auto_relogin_after_refresh=False)),
        ):
            first = service.refresh_accounts([f"token-{index}" for index in range(25)])
            second = service.refresh_accounts([f"second-token-{index}" for index in range(25)])

        self.assertEqual(first["refreshed"], 25)
        self.assertEqual(second["refreshed"], 25)
        self.assertGreater(len(observations), 1)
        self.assertLessEqual(max(observations), 10)
        self.assertEqual(len(executor.submitted), 50)

    def test_account_info_reuses_one_process_executor(self) -> None:
        class ImmediateFuture:
            def __init__(self, function) -> None:
                self._function = function

            def result(self):
                return self._function()

            def cancel(self) -> None:
                pass

        class RecordingExecutor:
            def __init__(self) -> None:
                self.submitted = []

            def submit(self, function):
                self.submitted.append(function)
                return ImmediateFuture(function)

        executor = RecordingExecutor()
        backend = backend_module.OpenAIBackendAPI.__new__(backend_module.OpenAIBackendAPI)
        backend.access_token = "access-token"
        backend._get_me = mock.Mock(return_value={"email": "owner@example.test", "id": "user-1"})
        backend._get_conversation_init = mock.Mock(
            return_value={"limits_progress": [], "default_model_slug": "gpt-5.5"}
        )
        backend._get_default_account = mock.Mock(return_value={"plan_type": "plus"})

        with (
            mock.patch.object(backend_module, "_ACCOUNT_INFO_EXECUTOR", executor, create=True),
            mock.patch.object(
                backend_module,
                "ThreadPoolExecutor",
                side_effect=AssertionError("get_user_info must not create a per-call executor"),
            ),
        ):
            first = backend.get_user_info()
            second = backend.get_user_info()

        self.assertEqual(first["type"], "plus")
        self.assertEqual(second["default_model_slug"], "gpt-5.5")
        self.assertEqual(len(executor.submitted), 6)

    def test_account_maintenance_background_queue_is_bounded(self) -> None:
        release = threading.Event()
        condition = threading.Condition()
        finished = 0

        async def block_management_io(*_args, **_kwargs):
            nonlocal finished
            try:
                while not release.is_set():
                    await asyncio.sleep(0.01)
            finally:
                with condition:
                    finished += 1
                    condition.notify_all()

        app = FastAPI()
        install_exception_handlers(app)
        app.include_router(accounts_api_module.create_router())
        try:
            with (
                mock.patch.object(accounts_api_module, "require_admin_async", new=mock.AsyncMock()),
                mock.patch.object(accounts_api_module, "run_management_io", side_effect=block_management_io),
                TestClient(app, raise_server_exceptions=False) as client,
            ):
                for index in range(32):
                    response = client.post(
                        "/api/accounts/refresh",
                        headers={"Authorization": "Bearer admin"},
                        json={"access_tokens": [f"token-{index}"]},
                    )
                    self.assertEqual(response.status_code, 200, response.text)

                overflow = client.post(
                    "/api/accounts/re-login",
                    headers={"Authorization": "Bearer admin"},
                    json={"access_tokens": ["overflow-token"]},
                )
                self.assertEqual(overflow.status_code, 429, overflow.text)
                self.assertEqual(overflow.headers.get("Retry-After"), "1")
        finally:
            release.set()

        with condition:
            self.assertTrue(condition.wait_for(lambda: finished >= 32, timeout=3))

    def test_bulk_account_relogin_uses_bounded_background_capacity(self) -> None:
        release = threading.Event()
        condition = threading.Condition()
        active = 0
        max_active = 0
        finished = 0
        progress_id = "bounded-account-relogin"
        tokens = [f"relogin-token-{index}" for index in range(33)]

        service = account_module.AccountService.__new__(account_module.AccountService)
        service._relogin_progress = {}
        service._relogin_progress_lock = account_module.Lock()

        def block_relogin(
            token: str,
            _email: str,
            _password: str,
            _event: str,
            task_progress_id: str | None = None,
        ) -> None:
            nonlocal active, max_active, finished
            with condition:
                active += 1
                max_active = max(max_active, active)
                condition.notify_all()
            try:
                if not release.wait(timeout=5):
                    raise AssertionError("relogin worker was not released")
                if task_progress_id:
                    service.update_relogin_progress(task_progress_id, token, "成功")
            finally:
                with condition:
                    active -= 1
                    finished += 1
                    condition.notify_all()

        with (
            mock.patch.object(
                service,
                "get_account",
                return_value={"email": "owner@example.test", "password": "password"},
            ),
            mock.patch.object(service, "list_accounts", return_value=[]),
            mock.patch.object(service, "_password_re_login_thread", side_effect=block_relogin),
        ):
            result = service.re_login_accounts(tokens, progress_id)
            try:
                with condition:
                    self.assertTrue(condition.wait_for(lambda: active >= 16, timeout=3))
                self.assertEqual(result["relogined"], 32)
                self.assertEqual(result["skipped"], 1)
                self.assertLessEqual(max_active, 16)
            finally:
                release.set()
                with condition:
                    self.assertTrue(
                        condition.wait_for(lambda: finished >= result["relogined"], timeout=5),
                        "accepted relogin workers did not finish",
                    )

        progress = service.get_relogin_progress(progress_id)
        self.assertIsNotNone(progress)
        assert progress is not None
        self.assertTrue(progress["done"])
        self.assertEqual(progress["processed"], len(tokens))
        service.clean_relogin_progress(progress_id)

    def test_public_background_tasks_have_bounded_workers_and_queue(self) -> None:
        condition = threading.Condition()
        release = threading.Event()
        all_finished = threading.Event()
        active = 0
        max_active = 0
        finished = 0
        accepted = 32

        def block_task(*_args, **_kwargs) -> None:
            nonlocal active, max_active, finished
            with condition:
                active += 1
                max_active = max(max_active, active)
                condition.notify_all()
            try:
                if not release.wait(timeout=5):
                    raise AssertionError("background task was not released")
            finally:
                with condition:
                    active -= 1
                    finished += 1
                    if finished == accepted:
                        all_finished.set()
                    condition.notify_all()

        owner = {"id": "bounded-task-owner", "name": "test", "role": "user"}
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            image_service = image_task_module.ImageTaskService(root / "image_tasks.json")
            editable_service = editable_task_module.EditableFileTaskService(root / "editable_tasks.json")
            with (
                mock.patch.object(image_service, "_run_task", side_effect=block_task),
                mock.patch.object(editable_service, "_run_task", side_effect=block_task),
            ):
                try:
                    for index in range(16):
                        image_service.submit_generation(
                            owner,
                            client_task_id=f"image-{index}",
                            prompt="draw",
                            model="gpt-image-2",
                            size="1024x1024",
                        )
                    for index in range(16):
                        editable_service.submit_ppt(
                            owner,
                            client_task_id=f"ppt-{index}",
                            prompt="build",
                        )

                    with condition:
                        self.assertTrue(
                            condition.wait_for(lambda: max_active >= 16, timeout=3),
                            "background workers did not reach the test baseline",
                        )
                        self.assertLessEqual(max_active, 16)

                    with self.assertRaises(RuntimeError):
                        image_service.submit_generation(
                            owner,
                            client_task_id="overflow",
                            prompt="draw",
                            model="gpt-image-2",
                            size="1024x1024",
                        )
                    self.assertEqual(
                        image_service.list_tasks(owner, ["overflow"])["missing_ids"],
                        ["overflow"],
                    )
                finally:
                    release.set()
                    self.assertTrue(all_finished.wait(timeout=5), "background tasks did not finish")

    def test_http_stream_bridge_closes_source_iterator_off_event_loop(self) -> None:
        class CloseTrackingIterator:
            def __init__(self) -> None:
                self.closed = False
                self.close_thread_id: int | None = None

            def __iter__(self):
                return self

            def __next__(self):
                return b"chunk"

            def close(self) -> None:
                self.closed = True
                self.close_thread_id = threading.get_ident()

        source = CloseTrackingIterator()

        async def consume_one_chunk() -> int:
            event_loop_thread_id = threading.get_ident()
            stream = iterate_ai_chunks(source)
            self.assertEqual(await anext(stream), b"chunk")
            await stream.aclose()
            return event_loop_thread_id

        event_loop_thread_id = asyncio.run(consume_one_chunk())

        self.assertTrue(source.closed)
        self.assertIsNotNone(source.close_thread_id)
        self.assertNotEqual(source.close_thread_id, event_loop_thread_id)

    def test_http_stream_disconnect_closes_logged_protocol_iterator(self) -> None:
        closed = threading.Event()

        def protocol_events():
            try:
                yield {"type": "response.created", "response": {"id": "resp_1"}}
                yield {"type": "response.completed", "response": {"id": "resp_1"}}
            finally:
                closed.set()

        events = protocol_events()
        call = LoggedCall(
            {"id": "key-1", "name": "test", "role": "user"},
            "/v1/responses",
            "gpt-5",
            "Responses",
        )

        async def disconnect_after_first_chunk() -> None:
            with mock.patch.object(call, "log"):
                response = await call.run(lambda: events, sse="responses")
                await anext(response.body_iterator)
                await response.body_iterator.aclose()

        asyncio.run(disconnect_after_first_chunk())

        self.assertTrue(closed.is_set())

    def test_public_account_progress_hides_all_internal_timestamps(self) -> None:
        self.assertEqual(
            account_module._public_progress(
                {
                    "done": True,
                    "_created_ts": 1.0,
                    "_last_activity_ts": 2.0,
                    "_finished_ts": 3.0,
                }
            ),
            {"done": True},
        )

    def test_in_progress_account_activity_refreshes_unfinished_ttl(self) -> None:
        service = account_module.AccountService.__new__(account_module.AccountService)
        service._refresh_progress = {}
        service._accounts = {"access-token": {"status": "正常", "quota": 0}}
        service._token_aliases = {}
        service._lock = account_module.Lock()
        progress_id = "refresh-progress-activity"
        try:
            with mock.patch.object(
                account_module.time,
                "monotonic",
                side_effect=[0.0, 3599.0, 7198.0, 10797.0, 10799.0],
            ):
                service.init_refresh_progress(progress_id, 3)
                service.update_refresh_progress(progress_id, "access-token")
                service.update_refresh_progress(progress_id, "access-token")

                # The total age is over one hour, but every update is less than
                # the unfinished retention window from the previous activity.
                self.assertIsNotNone(service.get_refresh_progress(progress_id))
                self.assertIsNone(service.get_refresh_progress(progress_id))
        finally:
            service.clean_refresh_progress(progress_id)

    def test_legacy_progress_timestamps_are_persisted_and_eventually_expire(self) -> None:
        service = account_module.AccountService.__new__(account_module.AccountService)
        service._refresh_progress = {
            "legacy-progress": {
                "total": 1,
                "processed": 0,
                "done": False,
            }
        }
        service._refresh_progress_lock = account_module.Lock()
        try:
            with mock.patch.object(
                account_module.time,
                "monotonic",
                side_effect=[100.0, 3699.0, 3701.0],
            ):
                first = service.get_refresh_progress("legacy-progress")
                self.assertIsNotNone(first)
                self.assertNotIn("_created_ts", first)
                self.assertNotIn("_last_activity_ts", first)
                self.assertIn("_created_ts", service._refresh_progress["legacy-progress"])
                self.assertIn("_last_activity_ts", service._refresh_progress["legacy-progress"])

                self.assertIsNotNone(service.get_refresh_progress("legacy-progress"))
                self.assertIsNone(service.get_refresh_progress("legacy-progress"))
        finally:
            service.clean_refresh_progress("legacy-progress")

    def test_relogin_activity_refreshes_unfinished_ttl(self) -> None:
        service = account_module.AccountService.__new__(account_module.AccountService)
        service._relogin_progress = {}
        service._relogin_progress_lock = account_module.Lock()
        progress_id = "relogin-progress-activity"
        try:
            with mock.patch.object(
                account_module.time,
                "monotonic",
                side_effect=[0.0, 3599.0, 7198.0, 10797.0, 10799.0],
            ):
                service.init_relogin_progress(progress_id, 3)
                service.update_relogin_progress(progress_id, "opaque-token", "正常")
                service.update_relogin_progress(progress_id, "opaque-token", "正常")

                self.assertIsNotNone(service.get_relogin_progress(progress_id))
                self.assertIsNone(service.get_relogin_progress(progress_id))
        finally:
            service.clean_relogin_progress(progress_id)

    def test_completed_account_progress_is_expired_from_memory(self) -> None:
        service = account_module.AccountService.__new__(account_module.AccountService)
        refresh_id = "refresh-progress-expiry"
        relogin_id = "relogin-progress-expiry"
        try:
            with mock.patch.object(account_module.time, "monotonic", side_effect=[0.0, 0.0, 16 * 60]):
                service.init_refresh_progress(refresh_id, 1)
                service.finish_refresh_progress(refresh_id, result={"items": [{"access_token": "secret"}]})
                self.assertIsNone(service.get_refresh_progress(refresh_id))

            with mock.patch.object(account_module.time, "monotonic", side_effect=[0.0, 0.0, 16 * 60]):
                service.init_relogin_progress(relogin_id, 1)
                service.update_relogin_progress(relogin_id, "opaque-token", "异常", "relogin_failed")
                self.assertIsNone(service.get_relogin_progress(relogin_id))
        finally:
            service.clean_refresh_progress(refresh_id)
            service.clean_relogin_progress(relogin_id)

    def test_web_search_closes_backend_after_success(self) -> None:
        instances: list[object] = []

        class FakeBackend:
            def __init__(self, access_token: str) -> None:
                self.access_token = access_token
                self.closed = False
                instances.append(self)

            def search(self, query: str) -> dict[str, object]:
                return {"answer": query}

            def close(self) -> None:
                self.closed = True

        with (
            mock.patch.object(web_search_module.account_service, "get_text_access_token", return_value="token"),
            mock.patch.object(web_search_module.account_service, "mark_text_used"),
            mock.patch.object(web_search_module, "OpenAIBackendAPI", FakeBackend),
        ):
            result = web_search_module.run_web_search("query")

        self.assertEqual(result, {"answer": "query"})
        self.assertEqual(len(instances), 1)
        self.assertTrue(instances[0].closed)

    def test_text_stream_closes_token_source_backend(self) -> None:
        class SourceBackend:
            access_token = "source-token"

            def __init__(self) -> None:
                self.closed = False

            def close(self) -> None:
                self.closed = True

        source_backend = SourceBackend()
        active_backends: list[object] = []

        class FakeBackend:
            def __init__(self, access_token: str) -> None:
                self.access_token = access_token
                self.closed = False
                active_backends.append(self)

            def close(self) -> None:
                self.closed = True

        with (
            mock.patch.object(conversation_module, "OpenAIBackendAPI", FakeBackend),
            mock.patch.object(
                conversation_module,
                "conversation_events",
                return_value=iter([{"type": "conversation.delta", "delta": "ok"}]),
            ),
            mock.patch.object(conversation_module.account_service, "mark_text_used"),
        ):
            result = list(stream_text_deltas(source_backend, ConversationRequest(model="auto", messages=[])))

        self.assertEqual(result, ["ok"])
        self.assertEqual(len(active_backends), 1)
        self.assertTrue(active_backends[0].closed)
        self.assertTrue(source_backend.closed)

    def test_backup_client_closes_when_validation_fails(self) -> None:
        instances: list[object] = []

        class FakeClient:
            def __init__(self, _settings: dict[str, object]) -> None:
                self.closed = False
                instances.append(self)

            def validate(self) -> None:
                raise BackupError("invalid backup settings")

            def close(self) -> None:
                self.closed = True

        service = BackupService()
        with (
            mock.patch.object(backup_module.config, "get_backup_settings", return_value={}),
            mock.patch.object(backup_module, "CloudflareR2Client", FakeClient),
        ):
            with self.assertRaises(BackupError):
                service._run_backup_once(trigger="test")

        self.assertEqual(len(instances), 1)
        self.assertTrue(instances[0].closed)

    def test_backup_scheduler_does_not_restart_while_stopped_thread_is_alive(self) -> None:
        threads: list[object] = []

        class BlockedThread:
            def __init__(self, *args, **kwargs) -> None:
                self.started = False
                self.join_timeouts: list[float | None] = []
                threads.append(self)

            def start(self) -> None:
                self.started = True

            def is_alive(self) -> bool:
                return self.started

            def join(self, timeout: float | None = None) -> None:
                self.join_timeouts.append(timeout)

        service = BackupService()
        with mock.patch.object(backup_module.threading, "Thread", BlockedThread):
            service.start()
            service.stop()
            service.start()

        self.assertEqual(len(threads), 1)
        self.assertEqual(threads[0].join_timeouts, [2])
        self.assertTrue(service._stop_event.is_set())

    def test_webdav_test_closes_session_on_invalid_settings(self) -> None:
        sessions: list[object] = []

        class FakeSession:
            def __init__(self) -> None:
                self.closed = False
                sessions.append(self)

            def close(self) -> None:
                self.closed = True

        with mock.patch.object(image_storage_module.requests, "Session", FakeSession):
            for url in ("", "ftp://webdav.example.test/root"):
                with self.subTest(url=url):
                    result = image_storage_module.WebDAVClient({"webdav_url": url}).test()
                    self.assertFalse(result["ok"])

        self.assertEqual(len(sessions), 2)
        self.assertTrue(all(session.closed for session in sessions))


if __name__ == "__main__":
    unittest.main()
