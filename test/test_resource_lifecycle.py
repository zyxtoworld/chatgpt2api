from __future__ import annotations

import asyncio
import hashlib
import io
import json
import threading
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

import api.accounts as accounts_api_module
import api.ai as ai_api_module
import api.support as support_api_module
import api.system as system_api_module
from api.errors import install_exception_handlers
import services.protocol.conversation as conversation_module
import services.protocol.openai_v1_chat_complete as openai_chat_module
import services.protocol.openai_v1_response as openai_response_module
import services.protocol.web_search_tool as web_search_module
import services.backup_service as backup_module
import services.content_filter as content_filter_module
import services.image_storage_service as image_storage_module
import services.image_service as image_service_module
import services.account_service as account_module
import services.auth_service as auth_module
import services.editable_file_task_service as editable_task_module
import services.image_task_service as image_task_module
import services.openai_backend_api as backend_module
import services.log_service as log_service_module
import services.task_executor as task_executor_module
from services.log_service import LoggedCall, iterate_ai_chunks
from services.protocol.conversation import ConversationRequest, stream_text_deltas
from services.backup_service import BackupError, BackupService
from services.storage.base import StorageDataError
from services.storage.json_storage import JSONStorageBackend
import utils.helper as helper_module
from utils.helper import iter_sse_payloads


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
    def test_image_zip_download_rejects_too_many_paths_before_io(self) -> None:
        app = FastAPI()
        app.include_router(system_api_module.create_router("test"))
        paths = [f"2026/08/16/{index}.png" for index in range(5001)]

        with (
            mock.patch.object(system_api_module, "require_admin_async", return_value={"role": "admin"}),
            mock.patch.object(system_api_module, "download_images_zip") as download,
            TestClient(app) as client,
        ):
            response = client.post("/api/images/download", json={"paths": paths})

        self.assertEqual(response.status_code, 413, response.text)
        download.assert_not_called()

    def test_image_zip_download_closes_owned_buffer_after_response(self) -> None:
        app = FastAPI()
        app.include_router(system_api_module.create_router("test"))
        buffer = io.BytesIO(b"zip-payload")

        with (
            mock.patch.object(system_api_module, "require_admin_async", return_value={"role": "admin"}),
            mock.patch.object(system_api_module, "download_images_zip", return_value=buffer),
            TestClient(app) as client,
        ):
            response = client.post("/api/images/download", json={"paths": []})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, b"zip-payload")
        self.assertTrue(buffer.closed)

    def test_image_zip_download_closes_buffer_when_generation_fails(self) -> None:
        created: list[io.BytesIO] = []
        original_bytes_io = io.BytesIO

        def make_buffer() -> io.BytesIO:
            buffer = original_bytes_io()
            created.append(buffer)
            return buffer

        with (
            mock.patch.object(image_service_module.io, "BytesIO", side_effect=make_buffer),
            mock.patch.object(image_service_module.image_storage_service, "open_local", return_value=None),
            mock.patch.object(
                image_service_module.image_storage_service,
                "get_bytes",
                side_effect=RuntimeError("read failed"),
            ),
        ):
            with self.assertRaises(HTTPException):
                image_service_module.download_images_zip(["missing.png"])

        self.assertEqual(len(created), 1)
        self.assertTrue(created[0].closed)

    def test_sse_payload_rejects_an_oversized_single_line_before_json_use(self) -> None:
        class Response:
            def iter_lines(self):
                yield b"data: " + (b"x" * 9)

        with mock.patch.object(helper_module, "MAX_SSE_LINE_BYTES", 8, create=True):
            with self.assertRaisesRegex(RuntimeError, "SSE line is too large"):
                list(iter_sse_payloads(Response()))

    def test_sse_payload_parser_handles_chunk_boundaries_with_a_fixed_read_size(self) -> None:
        class Response:
            def iter_content(self, chunk_size=None):
                self.chunk_size = chunk_size
                yield b"data: fir"
                yield b"st\n: comment\n"
                yield b"data: second\n"

        response = Response()
        self.assertEqual(list(iter_sse_payloads(response)), ["first", "second"])
        self.assertEqual(response.chunk_size, helper_module.SSE_READ_CHUNK_BYTES)

    def test_sse_payload_rejects_oversized_chunk_before_copying_it(self) -> None:
        class OversizedChunk(bytes):
            def __new__(cls):
                return super().__new__(cls, b"data: safe\n")

            def __len__(self):
                return helper_module.MAX_SSE_LINE_BYTES + 1

        class Response:
            def iter_content(self, chunk_size=None):
                yield OversizedChunk()

        with self.assertRaisesRegex(RuntimeError, "SSE chunk is too large"):
            list(iter_sse_payloads(Response()))

    def test_account_watcher_does_not_submit_after_stop_during_reservation(self) -> None:
        stop_event = threading.Event()
        reservation_ready = threading.Event()
        release_reservation = threading.Event()
        stop_returned = threading.Event()
        submitted: list[object] = []
        cancelled: list[bool] = []

        class ImmediateFuture:
            def done(self) -> bool:
                return True

            def result(self):
                return None

        class Reservation:
            def cancel(self) -> None:
                cancelled.append(True)

            def submit(self, function, *args, **kwargs):
                submitted.append((function, args, kwargs))
                return ImmediateFuture()

        def reserve():
            reservation_ready.set()
            if not release_reservation.wait(5):
                raise AssertionError("reservation was not released")
            return Reservation()

        old_interval = support_api_module.config.data.get("refresh_account_interval_minute")
        support_api_module.config.data["refresh_account_interval_minute"] = 0
        try:
            with (
                mock.patch.object(support_api_module.account_service, "list_limited_tokens", return_value=["token"]),
                mock.patch.object(support_api_module.account_service, "list_normal_tokens", return_value=[]),
                mock.patch.object(support_api_module.account_service, "list_expiring_access_tokens", return_value=[]),
                mock.patch.object(support_api_module.account_service, "list_refresh_token_keepalive_tokens", return_value=[]),
                mock.patch.object(support_api_module, "reserve_background_task", side_effect=reserve),
            ):
                thread = support_api_module.start_limited_account_watcher(stop_event)
                self.assertTrue(reservation_ready.wait(1))

                def stop() -> None:
                    support_api_module.stop_limited_account_watcher(stop_event, thread)
                    stop_returned.set()

                stopper = threading.Thread(target=stop)
                stopper.start()
                self.assertTrue(stop_event.wait(1))
                release_reservation.set()
                stopper.join(2)
                thread.join(2)

            self.assertTrue(stop_returned.is_set())
            self.assertEqual(submitted, [])
            self.assertEqual(cancelled, [True])
        finally:
            release_reservation.set()
            if old_interval is None:
                support_api_module.config.data.pop("refresh_account_interval_minute", None)
            else:
                support_api_module.config.data["refresh_account_interval_minute"] = old_interval

    def test_account_watcher_stop_does_not_leave_refresh_running(self) -> None:
        stop_event = threading.Event()
        started = threading.Event()
        release = threading.Event()
        finished = threading.Event()

        def blocked_refresh(*_args, **_kwargs):
            started.set()
            release.wait(5)
            finished.set()

        old_interval = support_api_module.config.data.get("refresh_account_interval_minute")
        support_api_module.config.data["refresh_account_interval_minute"] = 0.001
        try:
            with (
                mock.patch.object(support_api_module.account_service, "list_limited_tokens", return_value=["token"]),
                mock.patch.object(support_api_module.account_service, "list_normal_tokens", return_value=[]),
                mock.patch.object(support_api_module.account_service, "list_expiring_access_tokens", return_value=[]),
                mock.patch.object(support_api_module.account_service, "list_refresh_token_keepalive_tokens", return_value=[]),
                mock.patch.object(support_api_module.account_service, "refresh_accounts", side_effect=blocked_refresh),
            ):
                thread = support_api_module.start_limited_account_watcher(stop_event)
                self.assertTrue(started.wait(1))
                support_api_module.stop_limited_account_watcher(stop_event, thread)
                self.assertFalse(thread.is_alive())
                self.assertFalse(finished.is_set())
                release.set()
                task_executor_module.wait_for_background_tasks()
                self.assertTrue(finished.is_set())
        finally:
            release.set()
            if old_interval is None:
                support_api_module.config.data.pop("refresh_account_interval_minute", None)
            else:
                support_api_module.config.data["refresh_account_interval_minute"] = old_interval

    def test_stopped_watcher_cannot_delete_after_invalid_token_owner_check(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = account_module.AccountService(
                JSONStorageBackend(Path(tmp_dir) / "accounts.json")
            )
            service.add_account_items([{
                "access_token": "watcher-token",
                "status": "正常",
                "quota": 1,
            }])
            owner = service.begin_watcher_refresh()

            def precheck_succeeds(_candidate) -> bool:
                return True

            original_delete = service.delete_accounts
            delete_entered = threading.Event()
            release_delete = threading.Event()
            worker_errors: list[BaseException] = []

            def paused_delete(tokens, *, watcher_owner=None):
                delete_entered.set()
                if not release_delete.wait(5):
                    raise AssertionError("delete wrapper did not receive release")
                return original_delete(tokens, watcher_owner=watcher_owner)

            def remove_in_worker() -> None:
                try:
                    service.remove_invalid_token(
                        "watcher-token",
                        "watcher-test",
                        watcher_owner=owner,
                    )
                except BaseException as exc:
                    worker_errors.append(exc)

            with (
                mock.patch.dict(account_module.config.data, {"auto_remove_invalid_accounts": True}),
                mock.patch.object(
                    service,
                    "_watcher_refresh_is_current",
                    side_effect=precheck_succeeds,
                ),
                mock.patch.object(service, "delete_accounts", side_effect=paused_delete),
            ):
                worker = threading.Thread(target=remove_in_worker)
                worker.start()
                self.assertTrue(delete_entered.wait(5))
                service.invalidate_all_watcher_refreshes()
                release_delete.set()
                worker.join(5)
                self.assertFalse(worker.is_alive())

            self.assertEqual(worker_errors, [])

            self.assertIsNotNone(service.get_account("watcher-token"))

            # Mutation check: bypassing the final owner gate makes the same
            # interleaving delete the account, proving the red condition.
            service.add_account_items([{
                "access_token": "watcher-token",
                "status": "正常",
                "quota": 1,
            }])
            new_owner = service.begin_watcher_refresh()

            def unguarded_delete(tokens, *, watcher_owner=None):
                # Model removing the final owner argument: the real delete
                # operation still runs, but no watcher provenance reaches its
                # lock-protected commit boundary.
                return original_delete(tokens)

            with (
                mock.patch.dict(account_module.config.data, {"auto_remove_invalid_accounts": True}),
                mock.patch.object(service, "_watcher_refresh_is_current", return_value=True),
                mock.patch.object(service, "delete_accounts", side_effect=unguarded_delete),
            ):
                service.invalidate_all_watcher_refreshes()
                service.remove_invalid_token(
                    "watcher-token",
                    "watcher-test",
                    watcher_owner=new_owner,
                )
            self.assertIsNone(service.get_account("watcher-token"))

    def test_old_watcher_result_cannot_overwrite_new_account_generation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = account_module.AccountService(
                JSONStorageBackend(Path(tmp_dir) / "accounts.json")
            )
            service.add_account_items([{
                "access_token": "watcher-token",
                "status": "正常",
                "quota": 1,
            }])
            old_owner = service.begin_watcher_refresh()
            backend_started = threading.Event()
            release_backend = threading.Event()
            finished = threading.Event()

            class FakeBackend:
                def __init__(self, _token: str) -> None:
                    pass

                def get_user_info(self, **_kwargs):
                    backend_started.set()
                    release_backend.wait(5)
                    return {"type": "Pro", "quota": 99, "status": "正常"}

                def close(self) -> None:
                    pass

            def fetch() -> None:
                try:
                    service.fetch_remote_info("watcher-token", watcher_owner=old_owner)
                finally:
                    finished.set()

            with (
                mock.patch.object(service, "refresh_access_token", return_value="watcher-token"),
                mock.patch("services.openai_backend_api.OpenAIBackendAPI", FakeBackend),
            ):
                thread = threading.Thread(target=fetch)
                thread.start()
                self.assertTrue(backend_started.wait(1))
                service.invalidate_all_watcher_refreshes()
                new_owner = service.begin_watcher_refresh()
                self.assertNotEqual(old_owner, new_owner)
                service.update_account("watcher-token", {"status": "禁用"}, quiet=True)
                release_backend.set()
                thread.join(2)

            self.assertTrue(finished.is_set())
            self.assertEqual(service.get_account("watcher-token").get("status"), "禁用")

    def test_stopped_watcher_keepalive_cannot_commit_late_token_rotation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = account_module.AccountService(
                JSONStorageBackend(Path(tmp_dir) / "accounts.json")
            )
            service.add_account_items([{
                "access_token": "watcher-token",
                "refresh_token": "watcher-refresh",
                "status": "正常",
                "quota": 1,
            }])
            owner = service.begin_watcher_refresh()
            request_started = threading.Event()
            release_request = threading.Event()
            worker_errors: list[BaseException] = []

            def blocked_refresh(*_args, **_kwargs):
                request_started.set()
                if not release_request.wait(5):
                    raise AssertionError("refresh request did not receive release")
                return {
                    "access_token": "rotated-token",
                    "refresh_token": "rotated-refresh",
                    "id_token": "rotated-id",
                }

            def run_keepalive() -> None:
                try:
                    service.keepalive_refresh_tokens(
                        ["watcher-token"],
                        watcher_owner=owner,
                    )
                except BaseException as exc:
                    worker_errors.append(exc)

            with mock.patch.object(service, "_request_access_token_refresh", side_effect=blocked_refresh):
                worker = threading.Thread(target=run_keepalive)
                worker.start()
                self.assertTrue(request_started.wait(5))
                service.invalidate_all_watcher_refreshes()
                release_request.set()
                worker.join(5)

            self.assertFalse(worker.is_alive())
            self.assertEqual(worker_errors, [])
            self.assertIsNotNone(service.get_account("watcher-token"))
            self.assertIsNone(service.get_account("rotated-token"))

    def test_stopped_watcher_password_relogin_failures_cannot_commit(self) -> None:
        outcomes = (
            (
                "account_deactivated",
                {
                    "ok": False,
                    "error": "password_verify_failed_403",
                    "detail": {"error": {"code": "account_deactivated"}},
                },
                "正常",
            ),
            (
                "permanent_failure",
                {
                    "ok": False,
                    "error": "password_verify_failed_403",
                    "detail": {"error": {"code": "invalid_password"}},
                },
                "正常",
            ),
            ("exception", RuntimeError("late relogin failure"), "正常"),
        )

        for case_name, login_result, expected_status in outcomes:
            with self.subTest(case_name=case_name), tempfile.TemporaryDirectory() as tmp_dir:
                service = account_module.AccountService(
                    JSONStorageBackend(Path(tmp_dir) / "accounts.json")
                )
                service.add_account_items([{
                    "access_token": "watcher-token",
                    "email": "watcher@example.test",
                    "password": "password",
                    "status": "正常",
                    "quota": 1,
                }])
                progress_id = f"watcher-relogin-{case_name}"
                service.init_relogin_progress(progress_id, 1)
                owner = service.begin_watcher_refresh()
                login_started = threading.Event()
                release_login = threading.Event()
                worker_errors: list[BaseException] = []

                def blocked_login(*_args, **_kwargs):
                    login_started.set()
                    if not release_login.wait(5):
                        raise AssertionError("password login did not receive release")
                    if isinstance(login_result, BaseException):
                        raise login_result
                    return login_result

                def run_relogin() -> None:
                    try:
                        service._password_re_login_thread(
                            "watcher-token",
                            "watcher@example.test",
                            "password",
                            "watcher-test",
                            progress_id,
                            owner,
                        )
                    except BaseException as exc:
                        worker_errors.append(exc)

                with (
                    mock.patch.dict(account_module.config.data, {"auto_remove_invalid_accounts": True}),
                    mock.patch.object(service, "_login_with_password", side_effect=blocked_login),
                    mock.patch.object(account_module.log_service, "add") as log_add,
                ):
                    worker = threading.Thread(target=run_relogin)
                    worker.start()
                    self.assertTrue(login_started.wait(5))
                    service.invalidate_all_watcher_refreshes()
                    release_login.set()
                    worker.join(5)

                self.assertFalse(worker.is_alive())
                self.assertEqual(worker_errors, [])
                account = service.get_account("watcher-token")
                self.assertIsNotNone(account)
                self.assertEqual(account.get("status"), expected_status)
                progress = service.get_relogin_progress(progress_id)
                self.assertIsNotNone(progress)
                self.assertEqual(progress.get("processed"), 0)
                log_add.assert_not_called()

    def test_manual_relogin_late_success_cannot_rotate_deleted_and_recreated_account(self) -> None:
        class FakeFuture:
            def __init__(self) -> None:
                self.callbacks = []

            def add_done_callback(self, callback):
                self.callbacks.append(callback)

            def cancel(self):
                return False

        class DeferredReservation:
            def __init__(self) -> None:
                self.calls = []
                self.future = None

            def submit(self, function, *args, **kwargs):
                self.calls.append((function, args, kwargs))
                self.future = FakeFuture()
                return self.future

            def cancel(self):
                return None

        with tempfile.TemporaryDirectory() as tmp_dir:
            service = account_module.AccountService(
                JSONStorageBackend(Path(tmp_dir) / "accounts.json")
            )
            service.add_account_items([{
                "access_token": "manual-aba-token",
                "email": "old@example.test",
                "password": "old-password",
                "status": "正常",
                "quota": 1,
            }])
            reservation = DeferredReservation()
            with mock.patch.object(account_module, "reserve_background_task", return_value=reservation):
                service.re_login_accounts(["manual-aba-token"], "manual-aba-progress")

            service.delete_accounts(["manual-aba-token"])
            service.add_account_items([{
                "access_token": "manual-aba-token",
                "email": "new@example.test",
                "password": "new-password",
                "status": "正常",
                "quota": 9,
                "source_type": "new-generation",
            }])
            with mock.patch.object(
                service,
                "_login_with_password",
                return_value={
                    "ok": True,
                    "access_token": "manual-aba-rotated",
                    "refresh_token": "manual-refresh",
                    "id_token": "manual-id",
                },
            ):
                function, args, kwargs = reservation.calls[0]
                function(*args, **kwargs)
                reservation.future.callbacks[0](reservation.future)

            account = service.get_account("manual-aba-token")
            self.assertIsNotNone(account)
            assert account is not None
            self.assertEqual(account["access_token"], "manual-aba-token")
            self.assertEqual(account["source_type"], "new-generation")
            self.assertEqual(account["quota"], 9)
            self.assertIsNone(service.get_account("manual-aba-rotated"))

    def test_manual_relogin_cancel_fences_late_success_and_progress(self) -> None:
        class FakeFuture:
            def __init__(self) -> None:
                self.callbacks = []

            def add_done_callback(self, callback):
                self.callbacks.append(callback)

            def cancel(self):
                return False

        class DeferredReservation:
            def __init__(self) -> None:
                self.calls = []
                self.future = None

            def submit(self, function, *args, **kwargs):
                self.calls.append((function, args, kwargs))
                self.future = FakeFuture()
                return self.future

            def cancel(self):
                return None

        with tempfile.TemporaryDirectory() as tmp_dir:
            service = account_module.AccountService(
                JSONStorageBackend(Path(tmp_dir) / "accounts.json")
            )
            service.add_account_items([{
                "access_token": "manual-cancel-token",
                "email": "cancel@example.test",
                "password": "password",
                "status": "正常",
                "quota": 2,
            }])
            reservation = DeferredReservation()
            progress_id = "manual-cancel-progress"
            with mock.patch.object(account_module, "reserve_background_task", return_value=reservation):
                service.re_login_accounts(["manual-cancel-token"], progress_id)

            service.finish_relogin_progress(progress_id, error="重新登录任务已取消")
            with mock.patch.object(
                service,
                "_login_with_password",
                return_value={
                    "ok": True,
                    "access_token": "manual-cancel-rotated",
                    "refresh_token": "manual-refresh",
                    "id_token": "manual-id",
                },
            ):
                function, args, kwargs = reservation.calls[0]
                function(*args, **kwargs)
                reservation.future.callbacks[0](reservation.future)

            account = service.get_account("manual-cancel-token")
            self.assertIsNotNone(account)
            assert account is not None
            self.assertEqual(account["access_token"], "manual-cancel-token")
            self.assertIsNone(service.get_account("manual-cancel-rotated"))
            progress = service.get_relogin_progress(progress_id)
            self.assertIsNotNone(progress)
            assert progress is not None
            self.assertTrue(progress["done"])
            self.assertEqual(progress["error"], "重新登录任务已取消")

    def test_manual_relogin_rotation_persists_status_and_source_in_one_commit(self) -> None:
        class FakeFuture:
            def __init__(self) -> None:
                self.callbacks = []

            def add_done_callback(self, callback):
                self.callbacks.append(callback)

            def cancel(self):
                return False

        class DeferredReservation:
            def __init__(self) -> None:
                self.calls = []
                self.future = None

            def submit(self, function, *args, **kwargs):
                self.calls.append((function, args, kwargs))
                self.future = FakeFuture()
                return self.future

            def cancel(self):
                return None

        with tempfile.TemporaryDirectory() as tmp_dir:
            service = account_module.AccountService(
                JSONStorageBackend(Path(tmp_dir) / "accounts.json")
            )
            service.add_account_items([{
                "access_token": "manual-atomic-token",
                "email": "atomic@example.test",
                "password": "password",
                "status": "异常",
                "source_type": "old-source",
            }])
            reservation = DeferredReservation()
            with mock.patch.object(account_module, "reserve_background_task", return_value=reservation):
                service.re_login_accounts(["manual-atomic-token"], "manual-atomic-progress")

            with mock.patch.object(
                service,
                "_login_with_password",
                return_value={
                    "ok": True,
                    "access_token": "manual-atomic-rotated",
                    "refresh_token": "atomic-refresh",
                    "id_token": "atomic-id",
                    "source_type": "password-oauth",
                },
            ):
                function, args, kwargs = reservation.calls[0]
                function(*args, **kwargs)
                reservation.future.callbacks[0](reservation.future)

            new_account = service.get_account("manual-atomic-rotated")
            self.assertIsNotNone(new_account)
            assert new_account is not None
            self.assertEqual(new_account["status"], "正常")
            self.assertEqual(new_account["source_type"], "password-oauth")
            with service._lock:
                self.assertNotIn("manual-atomic-token", service._accounts)
                self.assertIn("manual-atomic-rotated", service._accounts)

            reloaded = account_module.AccountService(
                JSONStorageBackend(Path(tmp_dir) / "accounts.json")
            ).get_account("manual-atomic-rotated")
            self.assertIsNotNone(reloaded)
            assert reloaded is not None
            self.assertEqual(reloaded["status"], "正常")
            self.assertEqual(reloaded["source_type"], "password-oauth")

    def test_manual_relogin_same_token_replacement_rejects_late_success(self) -> None:
        class FakeFuture:
            def __init__(self) -> None:
                self.callbacks = []

            def add_done_callback(self, callback):
                self.callbacks.append(callback)

            def cancel(self):
                return False

        class DeferredReservation:
            def __init__(self) -> None:
                self.calls = []
                self.future = None

            def submit(self, function, *args, **kwargs):
                self.calls.append((function, args, kwargs))
                self.future = FakeFuture()
                return self.future

            def cancel(self):
                return None

        with tempfile.TemporaryDirectory() as tmp_dir:
            service = account_module.AccountService(
                JSONStorageBackend(Path(tmp_dir) / "accounts.json")
            )
            service.add_account_items([{
                "access_token": "manual-replaced-token",
                "email": "before@example.test",
                "password": "password",
                "status": "正常",
                "source_type": "before",
            }])
            reservation = DeferredReservation()
            with mock.patch.object(account_module, "reserve_background_task", return_value=reservation):
                service.re_login_accounts(["manual-replaced-token"], "manual-replaced-progress")

            service.update_account(
                "manual-replaced-token",
                {"email": "after@example.test", "source_type": "replacement"},
            )
            with mock.patch.object(
                service,
                "_login_with_password",
                return_value={
                    "ok": True,
                    "access_token": "manual-replaced-rotated",
                    "refresh_token": "replaced-refresh",
                    "id_token": "replaced-id",
                },
            ):
                function, args, kwargs = reservation.calls[0]
                function(*args, **kwargs)
                reservation.future.callbacks[0](reservation.future)

            account = service.get_account("manual-replaced-token")
            self.assertIsNotNone(account)
            assert account is not None
            self.assertEqual(account["email"], "after@example.test")
            self.assertEqual(account["source_type"], "replacement")
            self.assertIsNone(service.get_account("manual-replaced-rotated"))
            progress = service.get_relogin_progress("manual-replaced-progress")
            self.assertIsNotNone(progress)
            assert progress is not None
            self.assertNotIn("成功", [item.get("status") for item in progress["results"]])

    def test_manual_relogin_new_progress_generation_rejects_old_thread(self) -> None:
        class FakeFuture:
            def __init__(self) -> None:
                self.callbacks = []

            def add_done_callback(self, callback):
                self.callbacks.append(callback)

            def cancel(self):
                return False

        class DeferredReservation:
            def __init__(self) -> None:
                self.calls = []
                self.future = None

            def submit(self, function, *args, **kwargs):
                self.calls.append((function, args, kwargs))
                self.future = FakeFuture()
                return self.future

            def cancel(self):
                return None

        with tempfile.TemporaryDirectory() as tmp_dir:
            service = account_module.AccountService(
                JSONStorageBackend(Path(tmp_dir) / "accounts.json")
            )
            service.add_account_items([{
                "access_token": "manual-generation-token",
                "email": "generation@example.test",
                "password": "password",
                "status": "正常",
                "quota": 1,
            }])
            first = DeferredReservation()
            second = DeferredReservation()
            with mock.patch.object(
                account_module,
                "reserve_background_task",
                side_effect=[first, second],
            ):
                service.re_login_accounts(["manual-generation-token"], "reused-progress")
                service.re_login_accounts(["manual-generation-token"], "reused-progress")

            result = {
                "ok": True,
                "access_token": "manual-generation-rotated",
                "refresh_token": "manual-refresh",
                "id_token": "manual-id",
            }
            with mock.patch.object(service, "_login_with_password", return_value=result):
                old_function, old_args, old_kwargs = first.calls[0]
                old_function(*old_args, **old_kwargs)
                first.future.callbacks[0](first.future)

            self.assertIsNotNone(service.get_account("manual-generation-token"))
            self.assertIsNone(service.get_account("manual-generation-rotated"))

            with mock.patch.object(service, "_login_with_password", return_value=result):
                new_function, new_args, new_kwargs = second.calls[0]
                new_function(*new_args, **new_kwargs)
                second.future.callbacks[0](second.future)

            with service._lock:
                self.assertNotIn("manual-generation-token", service._accounts)
                self.assertIn("manual-generation-rotated", service._accounts)

    def test_manual_relogin_partial_queue_failure_finishes_after_running_thread(self) -> None:
        class FakeFuture:
            def __init__(self) -> None:
                self.callbacks = []

            def add_done_callback(self, callback):
                self.callbacks.append(callback)

            def cancel(self):
                return False

        class DeferredReservation:
            def __init__(self) -> None:
                self.calls = []
                self.future = None

            def submit(self, function, *args, **kwargs):
                self.calls.append((function, args, kwargs))
                self.future = FakeFuture()
                return self.future

            def cancel(self):
                return None

        with tempfile.TemporaryDirectory() as tmp_dir:
            service = account_module.AccountService(
                JSONStorageBackend(Path(tmp_dir) / "accounts.json")
            )
            service.add_account_items([
                {
                    "access_token": "partial-one",
                    "email": "one@example.test",
                    "password": "password",
                    "status": "正常",
                },
                {
                    "access_token": "partial-two",
                    "email": "two@example.test",
                    "password": "password",
                    "status": "正常",
                },
            ])
            reservation = DeferredReservation()
            with mock.patch.object(
                account_module,
                "reserve_background_task",
                side_effect=[reservation, account_module.BackgroundTaskQueueFullError("queue full")],
            ):
                result = service.re_login_accounts(["partial-one", "partial-two"], "partial-progress")

            self.assertEqual(result["relogined"], 1)
            self.assertEqual(result["skipped"], 1)
            progress = service.get_relogin_progress("partial-progress")
            self.assertIsNotNone(progress)
            assert progress is not None
            self.assertFalse(progress["done"])
            self.assertEqual(progress["processed"], 1)

            with mock.patch.object(
                service,
                "_login_with_password",
                return_value={
                    "ok": True,
                    "access_token": "partial-one-rotated",
                    "refresh_token": "partial-refresh",
                    "id_token": "partial-id",
                },
            ):
                function, args, kwargs = reservation.calls[0]
                function(*args, **kwargs)
                reservation.future.callbacks[0](reservation.future)

            progress = service.get_relogin_progress("partial-progress")
            self.assertIsNotNone(progress)
            assert progress is not None
            self.assertTrue(progress["done"])
            self.assertEqual(progress["processed"], 2)

    def test_manual_relogin_submit_failure_finishes_progress(self) -> None:
        class FailingReservation:
            def submit(self, *_args, **_kwargs):
                raise RuntimeError("submit failed")

            def cancel(self):
                return None

        with tempfile.TemporaryDirectory() as tmp_dir:
            service = account_module.AccountService(
                JSONStorageBackend(Path(tmp_dir) / "accounts.json")
            )
            service.add_account_items([{
                "access_token": "submit-failure-token",
                "email": "submit@example.test",
                "password": "password",
                "status": "正常",
            }])
            with mock.patch.object(
                account_module,
                "reserve_background_task",
                return_value=FailingReservation(),
            ):
                result = service.re_login_accounts(["submit-failure-token"], "submit-failure-progress")

            self.assertEqual(result["relogined"], 0)
            self.assertEqual(result["skipped"], 1)
            progress = service.get_relogin_progress("submit-failure-progress")
            self.assertIsNotNone(progress)
            assert progress is not None
            self.assertTrue(progress["done"])
            self.assertEqual(progress["processed"], 1)

    def test_manual_relogin_generic_submit_failure_cancels_reservation(self) -> None:
        class FailingReservation:
            def __init__(self) -> None:
                self.cancelled = False

            def submit(self, *_args, **_kwargs):
                raise RuntimeError("submit failed")

            def cancel(self):
                self.cancelled = True

        reservation = FailingReservation()
        service = account_module.AccountService.__new__(account_module.AccountService)
        service._lock = account_module.Lock()
        service._watcher_generation = 0
        service._accounts = {}
        service._token_aliases = {}
        with mock.patch.object(account_module, "reserve_background_task", return_value=reservation):
            self.assertFalse(
                service._schedule_password_relogin(
                    "submit-token",
                    "submit@example.test",
                    "password",
                    "manual_relogin",
                )
            )
        self.assertTrue(reservation.cancelled)

    def test_stopped_watcher_refresh_cannot_commit_late_progress(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = account_module.AccountService(
                JSONStorageBackend(Path(tmp_dir) / "accounts.json")
            )
            service.add_account_items([{
                "access_token": "watcher-token",
                "status": "正常",
                "quota": 1,
            }])
            owner = service.begin_watcher_refresh()
            progress_id = "watcher-refresh-progress"
            progress_entered = threading.Event()
            release_progress = threading.Event()
            original_update = service.update_refresh_progress

            def blocked_update(
                progress,
                token,
                *,
                watcher_owner=None,
                progress_generation=None,
                account_snapshot=None,
            ):
                progress_entered.set()
                if not release_progress.wait(5):
                    raise AssertionError("refresh progress did not receive release")
                if watcher_owner is None:
                    return original_update(
                        progress,
                        token,
                        progress_generation=progress_generation,
                        account_snapshot=account_snapshot,
                    )
                return original_update(
                    progress,
                    token,
                    watcher_owner=watcher_owner,
                    progress_generation=progress_generation,
                    account_snapshot=account_snapshot,
                )

            with (
                mock.patch.object(service, "fetch_remote_info", return_value=service.get_account("watcher-token")),
                mock.patch.object(service, "update_refresh_progress", side_effect=blocked_update),
            ):
                worker = threading.Thread(
                    target=service.refresh_accounts,
                    args=(["watcher-token"], progress_id),
                    kwargs={"watcher_owner": owner},
                )
                worker.start()
                self.assertTrue(progress_entered.wait(5))
                service.invalidate_all_watcher_refreshes()
                release_progress.set()
                worker.join(5)

            self.assertFalse(worker.is_alive())
            progress = service.get_refresh_progress(progress_id)
            self.assertIsNotNone(progress)
            self.assertEqual(progress.get("processed"), 0)

    def test_refresh_progress_isolated_between_service_instances(self) -> None:
        with tempfile.TemporaryDirectory() as first_dir, tempfile.TemporaryDirectory() as second_dir:
            first = account_module.AccountService(
                JSONStorageBackend(Path(first_dir) / "accounts.json")
            )
            second = account_module.AccountService(
                JSONStorageBackend(Path(second_dir) / "accounts.json")
            )
            progress_id = "refresh-progress-instance-isolation"
            first.init_refresh_progress(progress_id, 1)
            second.init_refresh_progress(progress_id, 2)
            first.update_refresh_progress(progress_id, "missing-token")

            first_progress = first.get_refresh_progress(progress_id)
            second_progress = second.get_refresh_progress(progress_id)

        self.assertIsNotNone(first_progress)
        self.assertIsNotNone(second_progress)
        assert first_progress is not None
        assert second_progress is not None
        self.assertEqual(first_progress["total"], 1)
        self.assertEqual(first_progress["processed"], 1)
        self.assertEqual(second_progress["total"], 2)
        self.assertEqual(second_progress["processed"], 0)

    def test_late_refresh_batch_cannot_finish_reused_progress_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = account_module.AccountService(
                JSONStorageBackend(Path(tmp_dir) / "accounts.json")
            )
            service.add_account_items([{
                "access_token": "refresh-progress-token",
                "status": "正常",
                "quota": 1,
            }])
            progress_id = "refresh-progress-generation"
            fetch_started = threading.Event()
            release_fetch = threading.Event()

            def deferred_fetch(*_args, **_kwargs):
                fetch_started.set()
                if not release_fetch.wait(5):
                    raise AssertionError("refresh worker did not receive release")
                return service.get_account("refresh-progress-token")

            with mock.patch.object(service, "fetch_remote_info", side_effect=deferred_fetch):
                worker = threading.Thread(
                    target=service.refresh_accounts,
                    args=(["refresh-progress-token"], progress_id),
                )
                worker.start()
                self.assertTrue(fetch_started.wait(5))
                service.init_refresh_progress(progress_id, 1)
                release_fetch.set()
                worker.join(5)

            self.assertFalse(worker.is_alive())
            progress = service.get_refresh_progress(progress_id)

        self.assertIsNotNone(progress)
        assert progress is not None
        self.assertEqual(progress["processed"], 0)
        self.assertFalse(progress["done"])

    def test_refresh_timeout_fences_late_watcher_account_commit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = account_module.AccountService(
                JSONStorageBackend(Path(tmp_dir) / "accounts.json")
            )
            service.add_account_items([{
                "access_token": "refresh-timeout-token",
                "status": "正常",
                "quota": 1,
            }])
            owner = service.begin_watcher_refresh()
            fetch_started = threading.Event()
            release_fetch = threading.Event()
            refresh_done = threading.Event()
            worker_errors: list[BaseException] = []

            def late_fetch(*_args, **kwargs):
                fetch_started.set()
                if not release_fetch.wait(5):
                    raise AssertionError("refresh worker did not receive release")
                current = service._accounts["refresh-timeout-token"]
                return service.update_account(
                    "refresh-timeout-token",
                    {"email": "late-commit@example.test"},
                    watcher_owner=kwargs.get("watcher_owner"),
                    expected_account=current,
                )

            def timeout_as_completed(_futures, **_kwargs):
                if not fetch_started.wait(5):
                    raise AssertionError("refresh worker did not start")
                raise account_module.FuturesTimeoutError("batch timeout")

            def run_refresh() -> None:
                try:
                    service.refresh_accounts(["refresh-timeout-token"], watcher_owner=owner)
                except BaseException as exc:
                    worker_errors.append(exc)
                finally:
                    refresh_done.set()

            with (
                mock.patch.object(service, "fetch_remote_info", side_effect=late_fetch),
                mock.patch.object(account_module, "as_completed", side_effect=timeout_as_completed),
            ):
                worker = threading.Thread(target=run_refresh)
                worker.start()
                self.assertTrue(refresh_done.wait(5))
                release_fetch.set()
                worker.join(5)

            self.assertFalse(worker.is_alive())
            self.assertEqual(len(worker_errors), 1)
            self.assertIsInstance(worker_errors[0], TimeoutError)
            current = service.get_account("refresh-timeout-token")

        self.assertIsNotNone(current)
        assert current is not None
        self.assertNotEqual(current.get("email"), "late-commit@example.test")

    def test_concurrent_refresh_batch_failure_only_invalidates_its_operation(self) -> None:
        for failing_batch in ("newer", "older"):
            with self.subTest(failing_batch=failing_batch), tempfile.TemporaryDirectory() as tmp_dir:
                service = account_module.AccountService(
                    JSONStorageBackend(Path(tmp_dir) / "accounts.json")
                )
                service.add_account_items([
                    {"access_token": "refresh-old-batch", "status": "正常", "quota": 1},
                    {"access_token": "refresh-new-batch", "status": "正常", "quota": 1},
                ])
                fetch_started = {
                    "older": threading.Event(),
                    "newer": threading.Event(),
                }
                release_fetch = {
                    "older": threading.Event(),
                    "newer": threading.Event(),
                }
                finished = {
                    "older": threading.Event(),
                    "newer": threading.Event(),
                }
                worker_errors: dict[str, BaseException] = {}
                progress_ids = {
                    "older": f"refresh-concurrent-older-{failing_batch}",
                    "newer": f"refresh-concurrent-newer-{failing_batch}",
                }

                def deferred_fetch(token: str, *_args, **_kwargs):
                    batch = "older" if token == "refresh-old-batch" else "newer"
                    fetch_started[batch].set()
                    if not release_fetch[batch].wait(5):
                        raise AssertionError(f"{batch} fetch did not receive release")
                    return service.get_account(token)

                def failing_progress(_count: int) -> None:
                    raise RuntimeError("progress callback failed")

                def run_batch(batch: str) -> None:
                    token = "refresh-old-batch" if batch == "older" else "refresh-new-batch"
                    callback = failing_progress if batch == failing_batch else None
                    try:
                        service.refresh_accounts(
                            [token],
                            progress_ids[batch],
                            on_progress=callback,
                        )
                    except BaseException as exc:
                        worker_errors[batch] = exc
                    finally:
                        finished[batch].set()

                with mock.patch.object(service, "fetch_remote_info", side_effect=deferred_fetch):
                    older = threading.Thread(target=run_batch, args=("older",))
                    newer = threading.Thread(target=run_batch, args=("newer",))
                    older.start()
                    self.assertTrue(fetch_started["older"].wait(5))
                    newer.start()
                    self.assertTrue(fetch_started["newer"].wait(5))

                    first_release = "newer" if failing_batch == "newer" else "older"
                    second_release = "older" if first_release == "newer" else "newer"
                    release_fetch[first_release].set()
                    self.assertTrue(finished[first_release].wait(5))
                    release_fetch[second_release].set()
                    self.assertTrue(finished[second_release].wait(5))
                    older.join(5)
                    newer.join(5)

                self.assertFalse(older.is_alive())
                self.assertFalse(newer.is_alive())
                expected_success = "older" if failing_batch == "newer" else "newer"
                successful_progress = service.get_refresh_progress(progress_ids[expected_success])
                self.assertIsNotNone(successful_progress)
                assert successful_progress is not None
                self.assertEqual(successful_progress["processed"], 1)
                self.assertTrue(successful_progress["done"])
                self.assertIn(failing_batch, worker_errors)
                self.assertEqual(service._refresh_operations, {})

    def test_stopped_watcher_invalid_removal_does_not_log_after_commit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = account_module.AccountService(
                JSONStorageBackend(Path(tmp_dir) / "accounts.json")
            )
            service.add_account_items([{
                "access_token": "watcher-token",
                "status": "异常",
                "quota": 0,
            }])
            owner = service.begin_watcher_refresh()
            original_delete = service.delete_accounts

            def delete_then_stop(tokens, *, watcher_owner=None):
                result = original_delete(tokens, watcher_owner=watcher_owner)
                service.invalidate_all_watcher_refreshes()
                return result

            with (
                mock.patch.dict(account_module.config.data, {"auto_remove_invalid_accounts": True}),
                mock.patch.object(service, "delete_accounts", side_effect=delete_then_stop),
                mock.patch.object(account_module.log_service, "add") as log_add,
            ):
                self.assertTrue(
                    service.remove_invalid_token(
                        "watcher-token",
                        "watcher-test",
                        watcher_owner=owner,
                    )
                )

            # The real delete commit logs once while holding the account lock;
            # the stale post-commit watcher-specific log must not be emitted.
            self.assertEqual(log_add.call_count, 1)
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
        lines = dockerfile.read_text(encoding="utf-8").splitlines()
        app_start = next(
            index
            for index, line in enumerate(lines)
            if line.strip().lower().startswith("from ")
            and line.strip().lower().endswith(" as app")
        )
        app_end = next(
            (
                index
                for index in range(app_start + 1, len(lines))
                if lines[index].strip().lower().startswith("from ")
            ),
            len(lines),
        )
        command_lines = [
            line.strip()
            for line in lines[app_start + 1 : app_end]
            if line.strip().startswith("CMD ")
        ]
        self.assertEqual(len(command_lines), 1)
        command_line = command_lines[0]
        command = json.loads(command_line.removeprefix("CMD "))

        self.assertNotIn("--access-log", command)
        self.assertEqual(
            command,
            [
                "uv",
                "run",
                "--no-sync",
                "uvicorn",
                "main:app",
                "--host",
                "0.0.0.0",
                "--port",
                "80",
                "--no-access-log",
            ],
        )

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
                self.chunk_size = None

            def iter_content(self, chunk_size=None):
                self.chunk_size = chunk_size
                yield b'{"choices":[{"message":{"content":"ALLOW"}}]}'

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
            mock.patch.object(content_filter_module.requests, "post", return_value=response) as post,
        ):
            content_filter_module.check_request("review this request")

        self.assertTrue(response.closed)
        self.assertIsNotNone(response.chunk_size)
        self.assertTrue(post.call_args.kwargs["stream"])

    def test_ai_review_requires_tls_verification_even_when_proxy_runtime_skips_it(self) -> None:
        class FakeResponse:
            status_code = 200

            def iter_content(self, chunk_size=None):
                yield b'{"choices":[{"message":{"content":"ALLOW"}}]}'

            def close(self) -> None:
                pass

        review_config = {
            "enabled": True,
            "base_url": "https://review.example.test",
            "api_key": "review-key",
            "model": "review-model",
        }
        observed: dict[str, object] = {}

        def build_session_kwargs(**kwargs: object) -> dict[str, object]:
            observed.update(kwargs)
            return {"verify": bool(kwargs.get("require_tls_verification"))}

        with (
            mock.patch.object(
                content_filter_module,
                "config",
                SimpleNamespace(sensitive_words=[], ai_review=review_config),
            ),
            mock.patch.object(
                content_filter_module.proxy_settings,
                "build_session_kwargs",
                side_effect=build_session_kwargs,
            ),
            mock.patch.object(content_filter_module.requests, "post", return_value=FakeResponse()) as post,
        ):
            content_filter_module.check_request("review this request")

        self.assertTrue(observed.get("require_tls_verification"))
        self.assertTrue(post.call_args.kwargs["verify"])

    def test_ai_review_oversized_stream_fails_closed_and_closes_response(self) -> None:
        class OversizedResponse:
            status_code = 200

            def __init__(self) -> None:
                self.closed = False

            def iter_content(self, chunk_size=None):
                yield b"x" * (content_filter_module._MAX_REVIEW_RESPONSE_BYTES + 1)

            def close(self) -> None:
                self.closed = True

        response = OversizedResponse()
        review_config = {
            "enabled": True,
            "base_url": "https://review.example.test",
            "api_key": "review-key",
            "model": "review-model",
            "fail_open": False,
        }
        with (
            mock.patch.object(
                content_filter_module,
                "config",
                SimpleNamespace(sensitive_words=[], ai_review=review_config),
            ),
            mock.patch.object(content_filter_module.requests, "post", return_value=response),
            self.assertRaises(HTTPException) as raised,
        ):
            content_filter_module.check_request("review this request")

        self.assertEqual(raised.exception.status_code, 503)
        self.assertTrue(response.closed)

    def test_ai_review_http_error_is_not_reclassified_as_parse_error(self) -> None:
        class FailedResponse:
            status_code = 503

            def __init__(self) -> None:
                self.closed = False

            def close(self) -> None:
                self.closed = True

        response = FailedResponse()
        review_config = {
            "enabled": True,
            "base_url": "https://review.example.test",
            "api_key": "review-key",
            "model": "review-model",
            "fail_open": False,
        }
        warnings: list[dict[str, object]] = []

        with (
            mock.patch.object(
                content_filter_module,
                "config",
                SimpleNamespace(sensitive_words=[], ai_review=review_config),
            ),
            mock.patch.object(content_filter_module.requests, "post", return_value=response),
            mock.patch.object(
                content_filter_module.logger,
                "warning",
                side_effect=lambda payload: warnings.append(payload),
            ),
            self.assertRaises(HTTPException) as raised,
        ):
            content_filter_module.check_request("review this request")

        self.assertEqual(raised.exception.status_code, 503)
        self.assertTrue(response.closed)
        self.assertEqual([item.get("event") for item in warnings], ["ai_review_response_http_error"])

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

        def recording_as_completed(futures, **_kwargs):
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

    def test_account_refresh_cancels_siblings_when_batch_submit_fails(self) -> None:
        class ControlledFuture:
            def __init__(self) -> None:
                self.cancel_calls = 0

            def cancel(self) -> bool:
                self.cancel_calls += 1
                return True

        first = ControlledFuture()
        second = ControlledFuture()

        class FailingExecutor:
            def __init__(self) -> None:
                self.calls = 0

            def submit(self, *_args, **_kwargs):
                self.calls += 1
                if self.calls == 1:
                    return first
                if self.calls == 2:
                    return second
                raise RuntimeError("account refresh submit failed")

        service = account_module.AccountService.__new__(account_module.AccountService)
        service._lock = account_module.Lock()
        service._accounts = {}
        service._token_aliases = {}
        service.list_accounts = mock.Mock(return_value=[])

        with (
            mock.patch.object(account_module, "_ACCOUNT_REFRESH_EXECUTOR", FailingExecutor(), create=True),
            mock.patch.object(account_module, "config", SimpleNamespace(auto_relogin_after_refresh=False)),
        ):
            with self.assertRaisesRegex(RuntimeError, "account refresh submit failed"):
                service.refresh_accounts(["token-1", "token-2", "token-3"])

        self.assertEqual(first.cancel_calls, 1)
        self.assertEqual(second.cancel_calls, 1)

    def test_account_refresh_deadline_uses_full_remaining_budget_for_batch_wait(self) -> None:
        observed_timeouts: list[float | None] = []

        class FakeFuture:
            def result(self):
                return {}

        class RecordingExecutor:
            def submit(self, *_args, **_kwargs):
                return FakeFuture()

        def recording_as_completed(futures, **kwargs):
            observed_timeouts.append(kwargs.get("timeout"))
            return iter(futures)

        service = account_module.AccountService.__new__(account_module.AccountService)
        service._lock = account_module.Lock()
        service._accounts = {}
        service._token_aliases = {}
        service.list_accounts = mock.Mock(return_value=[])

        with (
            mock.patch.object(account_module, "_ACCOUNT_REFRESH_EXECUTOR", RecordingExecutor()),
            mock.patch.object(account_module, "as_completed", side_effect=recording_as_completed),
            mock.patch.object(account_module.time, "monotonic", return_value=1000.0),
            mock.patch.object(account_module, "config", SimpleNamespace(auto_relogin_after_refresh=False)),
        ):
            result = service.refresh_accounts(["access-token"], deadline=2800.0)

        self.assertEqual(result["refreshed"], 1)
        self.assertEqual(observed_timeouts, [1800.0])

    def test_account_refresh_does_not_schedule_auto_relogin_after_deadline(self) -> None:
        clock = [1000.0]

        class FakeFuture:
            def result(self):
                return {}

        class RecordingExecutor:
            def submit(self, *_args, **_kwargs):
                return FakeFuture()

        def recording_as_completed(futures, **kwargs):
            self.assertEqual(kwargs.get("timeout"), 1.0)
            clock[0] = 1001.0
            return iter(futures)

        service = account_module.AccountService.__new__(account_module.AccountService)
        service._lock = account_module.Lock()
        service._accounts = {
            "access-token": {
                "access_token": "access-token",
                "status": "异常",
                "email": "owner@example.test",
                "password": "password",
            }
        }
        service._token_aliases = {}
        service.list_accounts = mock.Mock(return_value=[])
        schedule_relogin = mock.Mock(return_value=True)
        service._schedule_password_relogin = schedule_relogin

        with (
            mock.patch.object(account_module, "_ACCOUNT_REFRESH_EXECUTOR", RecordingExecutor()),
            mock.patch.object(account_module, "as_completed", side_effect=recording_as_completed),
            mock.patch.object(account_module.time, "monotonic", side_effect=lambda: clock[0]),
            mock.patch.object(account_module, "config", SimpleNamespace(auto_relogin_after_refresh=True)),
        ):
            with self.assertRaises(TimeoutError):
                service.refresh_accounts(["access-token"], deadline=1001.0)

        schedule_relogin.assert_not_called()

    def test_account_refresh_does_not_complete_after_deadline_without_auto_relogin(self) -> None:
        clock = [1000.0]

        class FakeFuture:
            def result(self):
                return {}

        class RecordingExecutor:
            def submit(self, *_args, **_kwargs):
                return FakeFuture()

        def recording_as_completed(futures, **kwargs):
            self.assertEqual(kwargs.get("timeout"), 1.0)
            clock[0] = 1001.0
            return iter(futures)

        service = account_module.AccountService.__new__(account_module.AccountService)
        service._lock = account_module.Lock()
        service._accounts = {}
        service._token_aliases = {}
        service.list_accounts = mock.Mock(return_value=[])
        service._refresh_progress = {}
        service._refresh_progress_lock = account_module.Lock()
        progress_id = "refresh-progress-deadline-boundary"

        with (
            mock.patch.object(account_module, "_ACCOUNT_REFRESH_EXECUTOR", RecordingExecutor()),
            mock.patch.object(account_module, "as_completed", side_effect=recording_as_completed),
            mock.patch.object(account_module.time, "monotonic", side_effect=lambda: clock[0]),
            mock.patch.object(account_module, "config", SimpleNamespace(auto_relogin_after_refresh=False)),
        ):
            with self.assertRaises(TimeoutError):
                service.refresh_accounts(["access-token"], progress_id, deadline=1001.0)
            progress = service.get_refresh_progress(progress_id)
        self.assertIsNotNone(progress)
        assert progress is not None
        self.assertTrue(progress["done"])
        self.assertEqual(progress["error"], "refresh timed out")

    def test_account_refresh_deadline_during_auto_relogin_finishes_owner(self) -> None:
        clock = [1000.0]

        class FakeFuture:
            def result(self):
                return {}

        class RecordingExecutor:
            def submit(self, *_args, **_kwargs):
                return FakeFuture()

        def recording_as_completed(futures, **_kwargs):
            return iter(futures)

        service = account_module.AccountService.__new__(account_module.AccountService)
        service._lock = account_module.Lock()
        service._accounts = {
            "relogin-deadline-token": {
                "access_token": "relogin-deadline-token",
                "status": "异常",
                "email": "owner@example.test",
                "password": "password",
            }
        }
        service._token_aliases = {}
        service.list_accounts = mock.Mock(return_value=[])
        service.get_account = mock.Mock(
            side_effect=lambda _token: (
                clock.__setitem__(0, 1002.0)
                or service._accounts["relogin-deadline-token"]
            )
        )
        service._refresh_progress = {}
        service._refresh_progress_lock = account_module.Lock()
        progress_id = "refresh-progress-relogin-deadline"

        with (
            mock.patch.object(account_module, "_ACCOUNT_REFRESH_EXECUTOR", RecordingExecutor()),
            mock.patch.object(account_module, "as_completed", side_effect=recording_as_completed),
            mock.patch.object(account_module.time, "monotonic", side_effect=lambda: clock[0]),
            mock.patch.object(account_module, "config", SimpleNamespace(auto_relogin_after_refresh=True)),
            mock.patch.object(service, "_schedule_password_relogin", return_value=True),
        ):
            with self.assertRaises(TimeoutError):
                service.refresh_accounts(
                    ["relogin-deadline-token"],
                    progress_id,
                    deadline=1002.0,
                )
            progress = service.get_refresh_progress(progress_id)

        self.assertIsNotNone(progress)
        assert progress is not None
        self.assertTrue(progress["done"])
        self.assertEqual(progress["error"], "refresh timed out")
        self.assertEqual(service._refresh_operations, {})

    def test_account_refresh_auto_relogin_failure_finishes_owner(self) -> None:
        class FakeFuture:
            def result(self):
                return {}

        class RecordingExecutor:
            def submit(self, *_args, **_kwargs):
                return FakeFuture()

        service = account_module.AccountService.__new__(account_module.AccountService)
        service._lock = account_module.Lock()
        service._accounts = {
            "relogin-failure-token": {
                "access_token": "relogin-failure-token",
                "status": "异常",
                "email": "owner@example.test",
                "password": "password",
            }
        }
        service._token_aliases = {}
        service.list_accounts = mock.Mock(return_value=[])
        service.get_account = mock.Mock(return_value=service._accounts["relogin-failure-token"])
        service._refresh_progress = {}
        service._refresh_progress_lock = account_module.Lock()
        progress_id = "refresh-progress-relogin-failure"

        with (
            mock.patch.object(account_module, "_ACCOUNT_REFRESH_EXECUTOR", RecordingExecutor()),
            mock.patch.object(account_module, "as_completed", side_effect=lambda futures, **_kwargs: iter(futures)),
            mock.patch.object(account_module, "config", SimpleNamespace(auto_relogin_after_refresh=True)),
            mock.patch.object(
                service,
                "_schedule_password_relogin",
                side_effect=RuntimeError("relogin scheduling failed"),
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "relogin scheduling failed"):
                service.refresh_accounts(["relogin-failure-token"], progress_id)
            progress = service.get_refresh_progress(progress_id)

        self.assertIsNotNone(progress)
        assert progress is not None
        self.assertTrue(progress["done"])
        self.assertEqual(progress["error"], "refresh failed")
        self.assertEqual(service._refresh_operations, {})

    def test_account_refresh_empty_batch_finalizer_failure_releases_owner(self) -> None:
        service = account_module.AccountService.__new__(account_module.AccountService)
        service._lock = account_module.Lock()
        service._accounts = {}
        service._token_aliases = {}
        service._refresh_progress = {}
        service._refresh_progress_lock = account_module.Lock()
        progress_id = "refresh-progress-empty-finalizer-failure"

        with mock.patch.object(
            service,
            "finish_refresh_progress",
            side_effect=RuntimeError("progress finalizer failed"),
        ):
            with self.assertRaisesRegex(RuntimeError, "progress finalizer failed"):
                service.refresh_accounts([], progress_id)

        self.assertEqual(service._refresh_operations, {})

    def test_account_refresh_failure_preserves_primary_error_and_running_future_fence(self) -> None:
        callbacks: list = []

        class FakeFuture:
            def result(self):
                return {}

            def add_done_callback(self, callback):
                callbacks.append(callback)

            def cancel(self):
                return False

        class RecordingExecutor:
            def submit(self, *_args, **_kwargs):
                return FakeFuture()

        service = account_module.AccountService.__new__(account_module.AccountService)
        service._lock = account_module.Lock()
        service._accounts = {
            "callback-failure-token": {
                "access_token": "callback-failure-token",
                "status": "正常",
                "quota": 1,
            }
        }
        service._token_aliases = {}
        service.list_accounts = mock.Mock(return_value=[])
        service._refresh_progress = {}
        service._refresh_progress_lock = account_module.Lock()
        progress_id = "refresh-progress-callback-finalizer-failure"

        def fail_on_progress(_processed: int) -> None:
            raise RuntimeError("callback failed")

        with (
            mock.patch.object(account_module, "_ACCOUNT_REFRESH_EXECUTOR", RecordingExecutor()),
            mock.patch.object(account_module, "as_completed", side_effect=lambda futures, **_kwargs: iter(futures)),
            mock.patch.object(
                service,
                "finish_refresh_progress",
                side_effect=RuntimeError("progress finalizer failed"),
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "callback failed"):
                service.refresh_accounts(
                    ["callback-failure-token"],
                    progress_id,
                    on_progress=fail_on_progress,
                )

        self.assertEqual(len(callbacks), 1)
        self.assertEqual(len(service._refresh_operations), 1)
        operation = next(iter(service._refresh_operations.values()))
        self.assertFalse(operation.valid)
        callbacks[0](None)
        self.assertEqual(service._refresh_operations, {})

    def test_account_refresh_does_not_schedule_relogin_if_deadline_expires_after_account_read(self) -> None:
        clock = [1000.0]

        class FakeFuture:
            def result(self):
                return {}

        class RecordingExecutor:
            def submit(self, *_args, **_kwargs):
                return FakeFuture()

        service = account_module.AccountService.__new__(account_module.AccountService)
        service._lock = account_module.Lock()
        service._accounts = {
            "access-token": {
                "access_token": "access-token",
                "status": "异常",
                "email": "owner@example.test",
                "password": "password",
            }
        }
        service._token_aliases = {}
        service.list_accounts = mock.Mock(return_value=[])
        service.get_account = mock.Mock(
            side_effect=lambda _token: (
                clock.__setitem__(0, 1001.0)
                or service._accounts["access-token"]
            )
        )
        schedule_relogin = mock.Mock(return_value=True)
        service._schedule_password_relogin = schedule_relogin

        with (
            mock.patch.object(account_module, "_ACCOUNT_REFRESH_EXECUTOR", RecordingExecutor()),
            mock.patch.object(account_module, "as_completed", side_effect=lambda futures, **_kwargs: iter(futures)),
            mock.patch.object(account_module.time, "monotonic", side_effect=lambda: clock[0]),
            mock.patch.object(account_module, "config", SimpleNamespace(auto_relogin_after_refresh=True)),
        ):
            with self.assertRaises(TimeoutError):
                service.refresh_accounts(["access-token"], deadline=1001.0)

        schedule_relogin.assert_not_called()

    def test_deadline_bound_relogin_worker_drops_late_task_without_side_effects(self) -> None:
        service = account_module.AccountService.__new__(account_module.AccountService)
        service._watcher_refresh_is_current = mock.Mock(return_value=True)
        service._login_with_password = mock.Mock(
            return_value={
                "ok": True,
                "access_token": "new-access-token",
                "refresh_token": "new-refresh-token",
                "id_token": "new-id-token",
            }
        )
        service._apply_refreshed_tokens = mock.Mock()
        service.update_account = mock.Mock()
        service.remove_invalid_token = mock.Mock()

        with mock.patch.object(account_module.time, "monotonic", return_value=1001.0):
            service._password_re_login_thread(
                "old-access-token",
                "owner@example.test",
                "password",
                "deadline-test",
                deadline=1000.0,
            )

        service._login_with_password.assert_not_called()
        service._apply_refreshed_tokens.assert_not_called()
        service.update_account.assert_not_called()
        service.remove_invalid_token.assert_not_called()

    def test_token_refresh_does_not_schedule_password_relogin_after_deadline(self) -> None:
        clock = [1000.0]
        service = account_module.AccountService.__new__(account_module.AccountService)
        service._lock = threading.RLock()
        service._token_refresh_condition = threading.Condition(threading.Lock())
        service._active_token_refreshes = set()
        service._accounts = {
            "access-token": {
                "access_token": "access-token",
                "refresh_token": "refresh-token",
                "email": "owner@example.test",
                "password": "password",
            }
        }
        service._token_aliases = {}
        schedule_relogin = mock.Mock(return_value=True)
        service._schedule_password_relogin = schedule_relogin

        def request_refresh(*_args, **_kwargs):
            clock[0] = 1001.0
            raise account_module.TokenRefreshError("app_session_terminated")

        with (
            mock.patch.object(account_module, "time", SimpleNamespace(monotonic=lambda: clock[0])),
            mock.patch.object(service, "_token_needs_refresh", return_value=True),
            mock.patch.object(service, "_request_access_token_refresh", side_effect=request_refresh),
            mock.patch.object(service, "_record_token_refresh_error"),
        ):
            result = service.refresh_access_token("access-token", deadline=1001.0)

        self.assertEqual(result, "access-token")
        schedule_relogin.assert_not_called()

    def test_token_refresh_does_not_commit_after_deadline_expires_before_save(self) -> None:
        clock = [1000.0]
        with tempfile.TemporaryDirectory() as temp_dir:
            service = account_module.AccountService(
                JSONStorageBackend(Path(temp_dir) / "accounts.json")
            )
            service.add_accounts(["access-token"])
            service._accounts["access-token"]["refresh_token"] = "refresh-token"

            def request_refresh(*_args, **_kwargs):
                clock[0] = 1001.0
                return {
                    "access_token": "new-access-token",
                    "refresh_token": "new-refresh-token",
                }

            with (
                mock.patch.object(account_module.time, "monotonic", side_effect=lambda: clock[0]),
                mock.patch.object(service, "_token_needs_refresh", return_value=True),
                mock.patch.object(service, "_request_access_token_refresh", side_effect=request_refresh),
            ):
                with self.assertRaises(TimeoutError):
                    service.refresh_access_token("access-token", deadline=1001.0)

            self.assertIsNotNone(service.get_account("access-token"))
            self.assertIsNone(service.get_account("new-access-token"))

    def test_account_refresh_reports_cumulative_success_count_after_each_result(self) -> None:
        class FakeFuture:
            def result(self):
                return {}

        class RecordingExecutor:
            def submit(self, *_args, **_kwargs):
                return FakeFuture()

        service = account_module.AccountService.__new__(account_module.AccountService)
        service._lock = account_module.Lock()
        service._accounts = {}
        service._token_aliases = {}
        service.list_accounts = mock.Mock(return_value=[])
        progress: list[int] = []

        with (
            mock.patch.object(account_module, "_ACCOUNT_REFRESH_EXECUTOR", RecordingExecutor()),
            mock.patch.object(account_module, "as_completed", side_effect=lambda futures, **_kwargs: iter(futures)),
            mock.patch.object(account_module, "config", SimpleNamespace(auto_relogin_after_refresh=False)),
        ):
            result = service.refresh_accounts(
                ["access-token-1", "access-token-2", "access-token-3"],
                on_progress=progress.append,
            )

        self.assertEqual(result["refreshed"], 3)
        self.assertEqual(progress, [1, 2, 3])

    def test_account_refresh_callback_failure_finishes_progress(self) -> None:
        class FakeFuture:
            def result(self):
                return {}

            def cancel(self):
                return True

        class ImmediateExecutor:
            def submit(self, *_args, **_kwargs):
                return FakeFuture()

        service = account_module.AccountService.__new__(account_module.AccountService)
        service._lock = account_module.Lock()
        service._accounts = {}
        service._token_aliases = {}
        service.list_accounts = mock.Mock(return_value=[])
        service.fetch_remote_info = mock.Mock(return_value={})
        progress_id = "refresh-progress-callback-failure"

        def fail_callback(_count: int) -> None:
            raise RuntimeError("callback failed")

        with (
            mock.patch.object(account_module, "_ACCOUNT_REFRESH_EXECUTOR", ImmediateExecutor()),
            mock.patch.object(account_module, "as_completed", side_effect=lambda futures, **_kwargs: iter(futures)),
            mock.patch.object(account_module, "config", SimpleNamespace(auto_relogin_after_refresh=False)),
        ):
            with self.assertRaisesRegex(RuntimeError, "callback failed"):
                service.refresh_accounts(["callback-token"], progress_id, on_progress=fail_callback)

        progress = service.get_refresh_progress(progress_id)
        self.assertIsNotNone(progress)
        assert progress is not None
        self.assertTrue(progress["done"])
        self.assertEqual(progress["error"], "refresh failed")

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

    def test_account_info_close_cannot_race_before_future_is_registered(self) -> None:
        submit_entered = threading.Event()
        release_submit = threading.Event()
        future_returned = threading.Event()
        callback_holder: list[object] = []
        close_overlapped_request: list[bool] = []

        class ControlledFuture:
            def add_done_callback(self, callback) -> None:
                callback_holder.append(callback)

            def finish(self) -> None:
                callback_holder[0](self)

        future = ControlledFuture()

        class BlockingExecutor:
            def submit(self, _function, **_kwargs):
                submit_entered.set()
                if not release_submit.wait(2):
                    raise AssertionError("submit was not released")
                future_returned.set()
                return future

        class Session:
            def close(self) -> None:
                close_overlapped_request.append(backend._account_info_pending > 0)

        backend = backend_module.OpenAIBackendAPI.__new__(backend_module.OpenAIBackendAPI)
        backend._account_info_state_lock = threading.Lock()
        backend._account_info_pending = 0
        backend._account_info_close_requested = False
        backend._account_info_session_closed = False
        backend.session = Session()
        holder: list[object] = []

        with mock.patch.object(backend_module, "_ACCOUNT_INFO_EXECUTOR", BlockingExecutor(), create=True):
            worker = threading.Thread(
                target=lambda: holder.append(backend._submit_account_info_future(lambda: None, None))
            )
            worker.start()
            self.assertTrue(submit_entered.wait(1))
            backend.close()
            self.assertEqual(close_overlapped_request, [])
            release_submit.set()
            worker.join(1)

        self.assertTrue(future_returned.is_set())
        self.assertEqual(backend._account_info_pending, 1)
        future.finish()
        self.assertEqual(close_overlapped_request, [False])
        self.assertEqual(backend._account_info_pending, 0)

    def test_account_info_does_not_submit_after_close(self) -> None:
        executor = mock.Mock()
        backend = backend_module.OpenAIBackendAPI.__new__(backend_module.OpenAIBackendAPI)
        backend._account_info_state_lock = threading.Lock()
        backend._account_info_pending = 0
        backend._account_info_close_requested = False
        backend._account_info_session_closed = False
        backend.session = mock.Mock()

        with mock.patch.object(backend_module, "_ACCOUNT_INFO_EXECUTOR", executor, create=True):
            backend.close()
            with self.assertRaisesRegex(RuntimeError, "closed"):
                backend._submit_account_info_future(lambda: None, None)

        executor.submit.assert_not_called()
        backend.session.close.assert_called_once_with()
        self.assertEqual(backend._account_info_pending, 0)

    def test_account_info_close_failure_remains_retryable(self) -> None:
        close_calls: list[int] = []

        class FlakySession:
            def close(self) -> None:
                close_calls.append(1)
                if len(close_calls) == 1:
                    raise OSError("close failed")

        backend = backend_module.OpenAIBackendAPI.__new__(backend_module.OpenAIBackendAPI)
        backend._account_info_state_lock = threading.Lock()
        backend._account_info_pending = 0
        backend._account_info_close_requested = False
        backend._account_info_session_closed = False
        backend.session = FlakySession()

        backend.close()
        self.assertFalse(backend._account_info_session_closed)

        backend.close()
        self.assertEqual(close_calls, [1, 1])
        self.assertTrue(backend._account_info_session_closed)

    def test_get_user_info_cancels_siblings_when_later_submit_fails(self) -> None:
        class ControlledFuture:
            def __init__(self) -> None:
                self.cancel_calls = 0
                self._callbacks = []

            def add_done_callback(self, callback) -> None:
                self._callbacks.append(callback)

            def cancel(self) -> bool:
                self.cancel_calls += 1
                for callback in self._callbacks:
                    callback(self)
                return True

        first = ControlledFuture()
        second = ControlledFuture()

        class FailingExecutor:
            def __init__(self) -> None:
                self.calls = 0

            def submit(self, _function, **_kwargs):
                self.calls += 1
                if self.calls == 1:
                    return first
                if self.calls == 2:
                    return second
                raise RuntimeError("account info submit failed")

        backend = backend_module.OpenAIBackendAPI.__new__(backend_module.OpenAIBackendAPI)
        backend.access_token = "access-token"
        backend._account_info_state_lock = threading.Lock()
        backend._account_info_pending = 0
        backend._account_info_close_requested = False
        backend._account_info_session_closed = False

        with mock.patch.object(backend_module, "_ACCOUNT_INFO_EXECUTOR", FailingExecutor(), create=True):
            with self.assertRaisesRegex(RuntimeError, "account info submit failed"):
                backend.get_user_info()

        self.assertEqual(first.cancel_calls, 1)
        self.assertEqual(second.cancel_calls, 1)
        self.assertEqual(backend._account_info_pending, 0)

    def test_get_user_info_drains_running_sibling_requests_before_returning_error(self) -> None:
        release_sibling = threading.Event()
        second_result_called = threading.Event()
        sibling_result_called = threading.Event()
        finished = threading.Event()
        errors: list[BaseException] = []

        class ControlledFuture:
            def __init__(self, value=None, error: BaseException | None = None, wait_for_release: bool = False) -> None:
                self.value = value
                self.error = error
                self.wait_for_release = wait_for_release

            def result(self):
                if self.error is not None:
                    second_result_called.set()
                    raise self.error
                if self.wait_for_release:
                    sibling_result_called.set()
                    if not release_sibling.wait(5):
                        raise AssertionError("sibling future was not released")
                return self.value

            def cancel(self) -> bool:
                return False

        futures = [
            ControlledFuture({"email": "owner@example.test", "id": "user-1"}),
            ControlledFuture(error=RuntimeError("account info failed")),
            ControlledFuture({"plan_type": "plus"}, wait_for_release=True),
        ]

        class ControlledExecutor:
            def __init__(self) -> None:
                self.index = 0

            def submit(self, *_args, **_kwargs):
                future = futures[self.index]
                self.index += 1
                return future

        backend = backend_module.OpenAIBackendAPI.__new__(backend_module.OpenAIBackendAPI)
        backend.access_token = "access-token"
        backend._get_me = mock.Mock()
        backend._get_conversation_init = mock.Mock()
        backend._get_default_account = mock.Mock()

        def invoke() -> None:
            try:
                backend.get_user_info()
            except BaseException as exc:
                errors.append(exc)
            finally:
                finished.set()

        with mock.patch.object(backend_module, "_ACCOUNT_INFO_EXECUTOR", ControlledExecutor(), create=True):
            thread = threading.Thread(target=invoke)
            thread.start()
            self.assertTrue(second_result_called.wait(1))
            self.assertFalse(finished.wait(0.1))
            self.assertTrue(sibling_result_called.wait(1))
            release_sibling.set()
            thread.join(2)

        self.assertTrue(finished.is_set())
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], RuntimeError)

    def test_fetch_remote_info_defers_shared_session_close_until_siblings_finish(self) -> None:
        release_siblings = threading.Event()
        sibling_started = threading.Event()
        siblings_finished = threading.Event()
        fetch_finished = threading.Event()
        close_called = threading.Event()
        close_overlapped_request: list[bool] = []
        state_lock = threading.Lock()
        active_requests = 0
        started_count = 0

        class Response:
            status_code = 200
            ok = True

            def __init__(self, payload: dict) -> None:
                self.payload = payload

            def iter_content(self, *, chunk_size: int):
                del chunk_size
                yield json.dumps(self.payload).encode("utf-8")

            def close(self) -> None:
                pass

        class SharedSession:
            def __init__(self) -> None:
                self.headers: dict[str, str] = {}

            def _sibling_response(self, payload: dict) -> Response:
                nonlocal active_requests, started_count
                with state_lock:
                    active_requests += 1
                    started_count += 1
                    if started_count == 2:
                        sibling_started.set()
                try:
                    if not release_siblings.wait(5):
                        raise AssertionError("sibling request was not released")
                    return Response(payload)
                finally:
                    with state_lock:
                        active_requests -= 1
                        if active_requests == 0:
                            siblings_finished.set()

            def get(self, url: str, **_kwargs):
                if url.endswith("/backend-api/me"):
                    raise TimeoutError("account info request timed out")
                return self._sibling_response({"accounts": {"default": {"account": {"plan_type": "free"}}}})

            def post(self, _url: str, **_kwargs):
                return self._sibling_response({"limits_progress": []})

            def close(self) -> None:
                with state_lock:
                    close_overlapped_request.append(active_requests > 0)
                close_called.set()

        session = SharedSession()
        backend = backend_module.OpenAIBackendAPI.__new__(backend_module.OpenAIBackendAPI)
        backend.access_token = "watcher-token"
        backend.base_url = "https://chatgpt.com"
        backend.session = session

        with tempfile.TemporaryDirectory() as tmp_dir:
            service = account_module.AccountService(JSONStorageBackend(Path(tmp_dir) / "accounts.json"))
            service.add_accounts(["watcher-token"])
            errors: list[BaseException] = []
            executor = ThreadPoolExecutor(max_workers=3)

            def fetch() -> None:
                try:
                    service.fetch_remote_info(
                        "watcher-token",
                        deadline=time.monotonic() + 5.0,
                    )
                except BaseException as exc:
                    errors.append(exc)
                finally:
                    fetch_finished.set()

            try:
                with (
                    mock.patch.object(backend_module, "_ACCOUNT_INFO_EXECUTOR", executor),
                    mock.patch.object(
                        backend_module.account_service,
                        "get_account",
                        return_value={"access_token": "watcher-token", "source_type": "web"},
                    ),
                    mock.patch.object(service, "refresh_access_token", return_value="watcher-token"),
                    mock.patch("services.openai_backend_api.OpenAIBackendAPI", return_value=backend),
                ):
                    worker = threading.Thread(target=fetch)
                    worker.start()
                    self.assertTrue(sibling_started.wait(1))
                    self.assertTrue(fetch_finished.wait(1))
                    self.assertFalse(close_called.is_set())
                    self.assertTrue(errors)
                    self.assertIsInstance(errors[0], TimeoutError)

                    release_siblings.set()
                    self.assertTrue(siblings_finished.wait(2))
                    self.assertTrue(close_called.wait(1))
                    self.assertEqual(getattr(backend, "_account_info_pending", -1), 0)
                    worker.join(1)
            finally:
                release_siblings.set()
                executor.shutdown(wait=True, cancel_futures=True)

        self.assertEqual(close_overlapped_request, [False])

    def test_conversation_recovery_ignores_non_scalar_upstream_metadata(self) -> None:
        backend = backend_module.OpenAIBackendAPI.__new__(backend_module.OpenAIBackendAPI)
        backend._list_recent_conversations = mock.Mock(return_value=[
            {
                "id": "conversation-1",
                "updated_at": {"secret": "conversation-metadata-canary"},
                "title": ["conversation-metadata-canary"],
            },
            ["conversation-metadata-canary"],
        ])

        result = backend.find_conversation_by_prompt("make an image", started_at=1.0)

        self.assertEqual(result, "")

    def test_conversation_recovery_returns_recent_item_without_title_match(self) -> None:
        backend = backend_module.OpenAIBackendAPI.__new__(backend_module.OpenAIBackendAPI)
        backend._list_recent_conversations = mock.Mock(return_value=[
            {
                "id": "recent-conversation",
                "updated_at": 100.0,
                "title": "unrelated title",
            },
        ])

        result = backend.find_conversation_by_prompt("make an image", started_at=100.0)

        self.assertEqual(result, "recent-conversation")

    def test_editable_artifact_recovery_ignores_non_scalar_message_metadata(self) -> None:
        backend = backend_module.OpenAIBackendAPI.__new__(backend_module.OpenAIBackendAPI)
        conversation = {
            "mapping": {
                "malformed": {
                    "message": {
                        "id": {"canary": "editable-message-canary"},
                        "author": {"role": ["editable-message-canary"]},
                        "create_time": {"canary": "editable-message-canary"},
                    },
                },
                "not-a-node": ["editable-message-canary"],
                "malformed-metadata": {
                    "message": {
                        "id": "message-1",
                        "author": {"role": "assistant"},
                        "create_time": 1.0,
                        "metadata": ["editable-message-canary"],
                    },
                },
            },
        }

        artifacts = backend._extract_editable_artifacts(
            conversation,
            backend_module.EDITABLE_PPT_EXPORT_FILE_RE,
        )

        self.assertEqual(artifacts, [])
        self.assertNotIn("editable-message-canary", repr(artifacts))

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

            # Keep the portal alive while accepted tasks observe the release.
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
        service._lock = account_module.Lock()
        service._relogin_progress = {}
        service._relogin_progress_lock = account_module.Lock()

        def block_relogin(
            token: str,
            _email: str,
            _password: str,
            _event: str,
            task_progress_id: str | None = None,
            **_kwargs,
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
            mock.patch.object(
                service,
                "_get_account_lease",
                side_effect=lambda token: (
                    token,
                    {"email": "owner@example.test", "password": "password"},
                ),
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

    def test_chat_completion_stream_close_closes_retained_delta_iterator(self) -> None:
        closed = threading.Event()

        def deltas():
            try:
                yield "first"
                yield "second"
            finally:
                closed.set()

        retained = deltas()
        with mock.patch.object(openai_chat_module, "stream_text_deltas", return_value=retained):
            stream = openai_chat_module.stream_text_chat_completion(
                object(),
                [{"role": "user", "content": "hello"}],
                "auto",
            )
            next(stream)
            stream.close()

        self.assertTrue(closed.is_set())

    def test_http_stream_sender_iter_failure_closes_source_iterators(self) -> None:
        class SourceIterator:
            def __init__(self) -> None:
                self.closed = False
                self._yielded = False

            def __iter__(self):
                return self

            def __next__(self):
                if self._yielded:
                    raise StopIteration
                self._yielded = True
                return {"data": "first"}

            def close(self) -> None:
                self.closed = True

        class SenderIterable:
            def __iter__(self):
                raise RuntimeError("sender iterator construction failed")

        source = SourceIterator()
        call = LoggedCall(
            {"id": "key-1", "name": "test", "role": "user"},
            "/v1/chat/completions",
            "test-model",
            "测试",
        )
        call.log_async = mock.AsyncMock()

        with mock.patch.object(log_service_module, "sse_json_stream", return_value=SenderIterable()):
            response = asyncio.run(call.run(lambda: source))

        async def consume() -> None:
            with self.assertRaisesRegex(RuntimeError, "sender iterator construction failed"):
                await anext(response.body_iterator)

        asyncio.run(consume())
        self.assertTrue(source.closed)

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

    def test_closable_sync_body_closes_source_when_iterator_close_fails(self) -> None:
        class Source:
            def __init__(self) -> None:
                self.closed = False

            def close(self) -> None:
                self.closed = True

        class FailingIterator:
            async def aclose(self) -> None:
                raise RuntimeError("iterator close failed")

        source = Source()
        body = system_api_module._ClosableSyncBody(source)
        body._iterator = FailingIterator()

        with self.assertRaisesRegex(RuntimeError, "iterator close failed"):
            asyncio.run(body.aclose())

        self.assertTrue(source.closed)

    def test_closable_sync_body_closes_source_when_iteration_fails(self) -> None:
        class Source:
            def __init__(self) -> None:
                self.closed = False

            def close(self) -> None:
                self.closed = True

        class FailingIterator:
            async def __anext__(self) -> object:
                raise RuntimeError("iteration failed")

            async def aclose(self) -> None:
                return None

        source = Source()
        body = system_api_module._ClosableSyncBody(source)
        body._iterator = FailingIterator()

        with self.assertRaisesRegex(RuntimeError, "iteration failed"):
            asyncio.run(body.__anext__())

        self.assertTrue(source.closed)

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

    def test_late_invalid_remote_result_cannot_mutate_rotated_account(self) -> None:
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        service = account_module.AccountService(
            JSONStorageBackend(Path(temp_dir.name) / "accounts.json")
        )
        service.add_account_items([
            {
                "access_token": "old-token",
                "refresh_token": "refresh-token",
                "status": "正常",
                "quota": 10,
            }
        ])
        service.refresh_access_token = mock.Mock(return_value="old-token")
        request_started = threading.Event()
        release_request = threading.Event()

        class DeferredBackend:
            def __init__(self, access_token: str) -> None:
                self.access_token = access_token

            def get_user_info(self, **_kwargs: object) -> dict[str, object]:
                request_started.set()
                if not release_request.wait(timeout=5):
                    raise AssertionError("deferred request was not released")
                raise backend_module.InvalidAccessTokenError("late invalid token")

            def close(self) -> None:
                return None

        with (
            mock.patch.object(backend_module, "OpenAIBackendAPI", DeferredBackend),
            ThreadPoolExecutor(max_workers=1) as executor,
        ):
            future = executor.submit(
                service.fetch_remote_info,
                "old-token",
                "late-invalid",
                False,
            )
            self.assertTrue(request_started.wait(timeout=5))
            service._apply_refreshed_tokens(
                "old-token",
                {"access_token": "new-token", "refresh_token": "new-refresh-token"},
                "test-rotation",
            )
            release_request.set()
            with self.assertRaises(backend_module.InvalidAccessTokenError):
                future.result(timeout=5)

        rotated = service.get_account("new-token")
        self.assertIsNotNone(rotated)
        self.assertEqual(rotated["status"], "正常")
        self.assertEqual(rotated["quota"], 10)
        self.assertEqual(rotated["invalid_count"], 0)

    def test_logged_call_closes_stream_when_first_item_fails(self) -> None:
        class FailingIterator:
            def __init__(self) -> None:
                self.closed = False

            def __iter__(self):
                return self

            def __next__(self):
                raise RuntimeError("first item failed")

            def close(self) -> None:
                self.closed = True

        stream = FailingIterator()
        call = LoggedCall(
            {"id": "key-id", "name": "test", "role": "user"},
            "/v1/chat/completions",
            "test-model",
            "测试",
        )
        call.log_async = mock.AsyncMock()

        response = asyncio.run(call.run(lambda: stream))

        self.assertEqual(response.status_code, 502)
        self.assertTrue(stream.closed)

    def test_logged_call_closes_empty_stream_before_returning_response(self) -> None:
        class EmptyIterator:
            def __init__(self) -> None:
                self.closed = False

            def __iter__(self):
                return self

            def __next__(self):
                raise StopIteration

            def close(self) -> None:
                self.closed = True

        stream = EmptyIterator()
        call = LoggedCall(
            {"id": "key-id", "name": "test", "role": "user"},
            "/v1/chat/completions",
            "test-model",
            "测试",
        )
        call.log_async = mock.AsyncMock()

        response = asyncio.run(call.run(lambda: stream))

        self.assertEqual(response.status_code, 200)
        self.assertTrue(stream.closed)

    def test_logged_call_closes_prefetched_stream_when_body_is_closed_before_first_consume(self) -> None:
        class PrefetchedIterator:
            def __init__(self) -> None:
                self.closed = False
                self._yielded = False

            def __iter__(self):
                return self

            def __next__(self):
                if self._yielded:
                    raise StopIteration
                self._yielded = True
                return {"data": "prefetched"}

            def close(self) -> None:
                self.closed = True

        stream = PrefetchedIterator()
        call = LoggedCall(
            {"id": "key-id", "name": "test", "role": "user"},
            "/v1/chat/completions",
            "test-model",
            "测试",
        )
        call.log_async = mock.AsyncMock()

        async def close_before_consuming() -> None:
            response = await call.run(lambda: stream)
            await response.body_iterator.aclose()

        asyncio.run(close_before_consuming())
        self.assertTrue(stream.closed)

    def test_responses_prefetched_event_closes_text_backend_before_body_consume(self) -> None:
        class Backend:
            def __init__(self) -> None:
                self.closed = False

            def close(self) -> None:
                self.closed = True

        backend = Backend()
        call = LoggedCall(
            {"id": "key-id", "name": "test", "role": "user"},
            "/v1/responses",
            "test-model",
            "测试",
        )
        call.log_async = mock.AsyncMock()

        async def close_before_consuming() -> None:
            events = openai_response_module.stream_text_response(
                backend,
                {"model": "test-model", "input": "hello"},
                [],
            )
            response = await call.run(lambda: events, sse="responses")
            await response.body_iterator.aclose()

        asyncio.run(close_before_consuming())
        self.assertTrue(backend.closed)

    def test_image_download_route_closes_buffer_before_body_consume(self) -> None:
        buffer = io.BytesIO(b"zip-data")
        router = system_api_module.create_router("test")
        route = next(route for route in router.routes if route.path == "/api/images/download")

        async def close_before_consuming() -> None:
            with (
                mock.patch.object(
                    system_api_module,
                    "require_admin_async",
                    new=mock.AsyncMock(return_value={"role": "admin"}),
                ),
                mock.patch.object(system_api_module, "download_images_zip", return_value=buffer),
            ):
                response = await route.endpoint(system_api_module.ImageDownloadRequest(paths=[]), None)
            await response.body_iterator.aclose()

        asyncio.run(close_before_consuming())
        self.assertTrue(buffer.closed)

    def test_logged_call_closes_prefetched_stream_if_response_creation_fails(self) -> None:
        class OneItemIterator:
            def __init__(self) -> None:
                self.closed = False

            def __iter__(self):
                return self

            def __next__(self):
                if self.closed:
                    raise StopIteration
                return {"data": "first"}

            def close(self) -> None:
                self.closed = True

        stream = OneItemIterator()
        call = LoggedCall(
            {"id": "key-id", "name": "test", "role": "user"},
            "/v1/chat/completions",
            "test-model",
            "测试",
        )
        call.log_async = mock.AsyncMock()

        with mock.patch.object(log_service_module, "sse_json_stream", side_effect=RuntimeError("sender failed")):
            response = asyncio.run(call.run(lambda: stream))

        self.assertEqual(response.status_code, 502)
        self.assertTrue(stream.closed)

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

    def test_backup_scheduler_does_not_submit_after_stop_during_reservation(self) -> None:
        service = BackupService()
        reservation_ready = threading.Event()
        release_reservation = threading.Event()
        submitted: list[object] = []

        class ImmediateFuture:
            def done(self) -> bool:
                return True

        class Reservation:
            def cancel(self) -> None:
                return None

            def submit(self, function, *args, **kwargs):
                submitted.append((function, args, kwargs))
                return ImmediateFuture()

        def reserve():
            reservation_ready.set()
            if not release_reservation.wait(5):
                raise AssertionError("reservation was not released")
            return Reservation()

        with mock.patch.object(backup_module, "reserve_background_task", side_effect=reserve):
            service.start()
            thread = service._thread
            self.assertIsNotNone(thread)
            self.assertTrue(reservation_ready.wait(1))
            service.stop()
            release_reservation.set()
            thread.join(timeout=2)

        self.assertEqual(submitted, [])

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
