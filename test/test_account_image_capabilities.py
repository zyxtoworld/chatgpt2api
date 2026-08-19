from __future__ import annotations

import os
import json
import threading
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import Mock, patch

os.environ.setdefault("CHATGPT2API_AUTH_KEY", "chatgpt2api")

import services.account_service as account_module
from services.account_service import AccountService
from services.auth_service import AuthService
from services.config import config
from services.openai_backend_api import InvalidAccessTokenError, OpenAIBackendAPI
from services.protocol.error_response import PublicSafeValueError
from services.storage.json_storage import JSONStorageBackend
from services.storage.base import StorageDataError
from utils.helper import anonymize_token, split_image_model


class AccountCapabilityTests(unittest.TestCase):
    def setUp(self) -> None:
        self._web_backend_identity = patch.object(
            account_module.account_service,
            "get_account",
            side_effect=lambda token: {"access_token": token, "source_type": "web"},
        )
        self._web_backend_identity.start()
        self.addCleanup(self._web_backend_identity.stop)

    def test_backend_model_catalog_rejects_container_slug_and_owner(self) -> None:
        canary = "backend-model-container-canary"
        backend = OpenAIBackendAPI(access_token="token-1")

        class Response:
            status_code = 200
            ok = True

            def __init__(self, payload: dict) -> None:
                self._payload = json.dumps(payload).encode("utf-8")
                self.closed = False

            def iter_content(self, chunk_size: int = 0):
                yield self._payload

            def close(self) -> None:
                self.closed = True

        response = Response({
            "models": [
                {"slug": {"secret": canary}, "owned_by": "chatgpt"},
                {"slug": "valid-model", "owned_by": [canary]},
            ],
        })
        try:
            with (
                patch.object(backend, "_bootstrap"),
                patch.object(backend.session, "get", return_value=response),
            ):
                result = backend.list_models()
        finally:
            backend.close()

        self.assertEqual([item["id"] for item in result["data"]], ["valid-model"])
        self.assertNotIn(canary, json.dumps(result, ensure_ascii=False))
        self.assertTrue(response.closed)

    def test_image_accounts_require_positive_quota(self) -> None:
        self.assertFalse(
            AccountService._is_image_account_available(
                {"status": "限流", "quota": 1}
            )
        )
        self.assertFalse(
            AccountService._is_image_account_available(
                {"status": "正常", "quota": 0}
            )
        )
        self.assertTrue(AccountService._is_image_account_available({"status": "正常", "quota": 1}))

    def test_prolite_variants_are_normalized(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = AccountService(JSONStorageBackend(Path(tmp_dir) / "accounts.json"))
            self.assertEqual(service._normalize_account_type("prolite"), "ProLite")
            self.assertEqual(service._normalize_account_type("pro_lite"), "ProLite")

    def test_account_type_normalization_rejects_container_values(self) -> None:
        self.assertIsNone(AccountService._normalize_account_type({"plan": "Pro"}))
        self.assertIsNone(AccountService._normalize_account_type(["Team"]))

    def test_user_info_preserves_team_and_rejects_container_plan_types(self) -> None:
        for plan_type, expected in (("team", "team"), ({"plan": "canary"}, "free"), (["pro"], "free")):
            backend = OpenAIBackendAPI(access_token="token-1")
            try:
                with (
                    patch.object(backend, "_get_me", return_value={"email": "user@example.test", "id": "user-1"}),
                    patch.object(backend, "_get_conversation_init", return_value={"limits_progress": []}),
                    patch.object(backend, "_get_default_account", return_value={"plan_type": plan_type}),
                ):
                    result = backend.get_user_info()
            finally:
                backend.close()
            self.assertEqual(result["type"], expected)
            self.assertNotIn("canary", str(result))

    def test_user_info_rejects_container_quota_fields(self) -> None:
        backend = OpenAIBackendAPI(access_token="token-1")
        try:
            with (
                patch.object(backend, "_get_me", return_value={"email": "user@example.test", "id": "user-1"}),
                patch.object(
                    backend,
                    "_get_conversation_init",
                    return_value={
                        "limits_progress": [{
                            "feature_name": "image_gen",
                            "remaining": {"secret": "canary"},
                            "reset_after": ["canary"],
                        }],
                    },
                ),
                patch.object(backend, "_get_default_account", return_value={"plan_type": "Team"}),
            ):
                result = backend.get_user_info()
        finally:
            backend.close()
        self.assertEqual(result["quota"], 0)
        self.assertIsNone(result["restore_at"])
        self.assertNotIn("canary", str(result))

    def test_user_info_rejects_container_identity_and_model_fields(self) -> None:
        backend = OpenAIBackendAPI(access_token="token-1")
        try:
            with (
                patch.object(
                    backend,
                    "_get_me",
                    return_value={
                        "email": {"secret": "canary"},
                        "id": ["canary"],
                    },
                ),
                patch.object(
                    backend,
                    "_get_conversation_init",
                    return_value={
                        "limits_progress": [],
                        "default_model_slug": {"secret": "canary"},
                    },
                ),
                patch.object(backend, "_get_default_account", return_value={"plan_type": "Team"}),
            ):
                result = backend.get_user_info()
        finally:
            backend.close()
        self.assertIsNone(result["email"])
        self.assertIsNone(result["user_id"])
        self.assertIsNone(result["default_model_slug"])
        self.assertNotIn("canary", str(result))

    def test_default_account_debug_log_does_not_include_container_metadata(self) -> None:
        canary = "default-account-log-canary"
        payload = {
            "accounts": {
                "default": {
                    "account": {
                        "plan_type": {"secret": canary},
                        "account_user_role": [canary],
                        "account_id": {"secret": canary},
                    },
                    "entitlement": {
                        "has_active_subscription": {"secret": canary},
                        "subscription_plan": [canary],
                    },
                },
            },
        }
        response = Mock(
            status_code=200,
            iter_content=None,
            content=json.dumps(payload).encode("utf-8"),
            json=lambda: payload,
        )
        backend = OpenAIBackendAPI(access_token="token-1")
        try:
            with (
                patch.object(backend.session, "get", return_value=response),
                patch("services.openai_backend_api.logger.debug") as debug,
            ):
                backend._get_default_account()
        finally:
            backend.close()
        self.assertNotIn(canary, repr(debug.call_args_list))

    def test_default_account_debug_log_does_not_include_untrusted_text(self) -> None:
        canary = "default-account-text-canary owner@example.com opaque-token"
        payload = {
            "accounts": {
                "default": {
                    "account": {
                        "plan_type": canary,
                        "account_user_role": canary,
                        "account_id": canary,
                    },
                    "entitlement": {
                        "has_active_subscription": True,
                        "subscription_plan": canary,
                    },
                },
            },
        }
        response = Mock(
            status_code=200,
            iter_content=None,
            content=json.dumps(payload).encode("utf-8"),
            json=lambda: payload,
        )
        backend = OpenAIBackendAPI(access_token="token-1")
        try:
            with (
                patch.object(backend.session, "get", return_value=response),
                patch("services.openai_backend_api.logger.debug") as debug,
            ):
                backend._get_default_account()
        finally:
            backend.close()
        self.assertNotIn(canary, repr(debug.call_args_list))

    def test_user_info_result_debug_log_does_not_include_untrusted_text(self) -> None:
        canary = "user-info-result-text-canary owner@example.com opaque-token"
        backend = OpenAIBackendAPI(access_token="token-1")
        try:
            with (
                patch.object(backend, "_get_me", return_value={"email": canary, "id": canary}),
                patch.object(
                    backend,
                    "_get_conversation_init",
                    return_value={"limits_progress": [], "default_model_slug": canary},
                ),
                patch.object(backend, "_get_default_account", return_value={"plan_type": canary}),
                patch("services.openai_backend_api.logger.debug") as debug,
            ):
                backend.get_user_info()
        finally:
            backend.close()
        self.assertNotIn(canary, repr(debug.call_args_list))

    def test_bounded_user_info_does_not_wait_for_sibling_after_timeout(self) -> None:
        release = threading.Event()
        sibling_started = threading.Event()
        finished = threading.Event()
        errors: list[BaseException] = []

        def timed_out_request(**_kwargs: object) -> dict:
            raise TimeoutError("account info deadline exceeded")

        def blocked_request(**_kwargs: object) -> dict:
            sibling_started.set()
            release.wait(5)
            return {}

        backend = OpenAIBackendAPI(access_token="token-1")
        executor = ThreadPoolExecutor(max_workers=3)
        try:
            with (
                patch("services.openai_backend_api._ACCOUNT_INFO_EXECUTOR", executor),
                patch.object(backend, "_get_me", side_effect=timed_out_request),
                patch.object(backend, "_get_conversation_init", side_effect=blocked_request),
                patch.object(backend, "_get_default_account", side_effect=blocked_request),
            ):
                def run() -> None:
                    try:
                        backend.get_user_info(deadline=1_000_000.0)
                    except BaseException as exc:
                        errors.append(exc)
                    finally:
                        finished.set()

                worker = threading.Thread(target=run)
                worker.start()
                self.assertTrue(sibling_started.wait(1))
                self.assertTrue(finished.wait(0.2))
                worker.join(1)
        finally:
            release.set()
            backend.close()
            executor.shutdown(wait=True, cancel_futures=True)
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], TimeoutError)

    def test_bounded_user_info_deadline_interrupts_first_blocked_future(self) -> None:
        first_started = threading.Event()
        release_first = threading.Event()
        finished = threading.Event()
        errors: list[BaseException] = []

        def blocked_request(**_kwargs: object) -> dict:
            first_started.set()
            release_first.wait(5)
            return {"email": "owner@example.test", "id": "user-1"}

        backend = OpenAIBackendAPI(access_token="token-1")
        executor = ThreadPoolExecutor(max_workers=3)
        try:
            with (
                patch("services.openai_backend_api._ACCOUNT_INFO_EXECUTOR", executor),
                patch.object(backend, "_get_me", side_effect=blocked_request),
                patch.object(
                    backend,
                    "_get_conversation_init",
                    return_value={"limits_progress": []},
                ),
                patch.object(
                    backend,
                    "_get_default_account",
                    return_value={"plan_type": "free"},
                ),
            ):
                def run() -> None:
                    try:
                        backend.get_user_info(deadline=time.monotonic() + 0.05)
                    except BaseException as exc:
                        errors.append(exc)
                    finally:
                        finished.set()

                worker = threading.Thread(target=run)
                worker.start()
                self.assertTrue(first_started.wait(1))
                self.assertTrue(
                    finished.wait(0.2),
                    "account info collection must honor its absolute deadline",
                )
                worker.join(1)
        finally:
            release_first.set()
            backend.close()
            executor.shutdown(wait=True, cancel_futures=True)
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], TimeoutError)

    def test_fetch_remote_info_does_not_commit_result_after_deadline(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = AccountService(JSONStorageBackend(Path(tmp_dir) / "accounts.json"))
            service.add_account_items([{
                "access_token": "token-1",
                "type": "free",
                "status": "正常",
                "quota": 1,
            }])

            class FakeBackend:
                def __init__(self, _access_token: str) -> None:
                    pass

                def get_user_info(self, **_kwargs: object) -> dict:
                    return {"type": "Pro", "status": "正常", "quota": 99}

                def close(self) -> None:
                    pass

            with (
                patch.object(service, "refresh_access_token", return_value="token-1"),
                patch("services.openai_backend_api.OpenAIBackendAPI", FakeBackend),
                patch.object(account_module.time, "monotonic", return_value=1001.0),
            ):
                with self.assertRaises(TimeoutError):
                    service.fetch_remote_info("token-1", deadline=1000.5)

            self.assertEqual(service.get_account("token-1")["type"], "free")

    def test_text_refresh_rejects_late_result_after_same_token_account_replacement(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = AccountService(JSONStorageBackend(Path(tmp_dir) / "accounts.json"))
            service.add_account_items([{
                "access_token": "token-1",
                "refresh_token": "refresh-1",
                "type": "free",
                "status": "正常",
                "quota": 1,
            }])
            refresh_started = threading.Event()
            release_refresh = threading.Event()
            result: list[str] = []
            errors: list[BaseException] = []

            def delayed_refresh(*_args: object, **_kwargs: object) -> dict[str, str]:
                refresh_started.set()
                if not release_refresh.wait(2):
                    raise AssertionError("refresh was not released")
                return {
                    "access_token": "token-rotated-by-stale-refresh",
                    "refresh_token": "refresh-rotated-by-stale-refresh",
                    "id_token": "id-rotated-by-stale-refresh",
                }

            def select_token() -> None:
                try:
                    result.append(service.get_text_access_token())
                except BaseException as exc:
                    errors.append(exc)

            with (
                patch.object(service, "_token_needs_refresh", return_value=True),
                patch.object(service, "_request_access_token_refresh", side_effect=delayed_refresh),
            ):
                worker = threading.Thread(target=select_token)
                worker.start()
                self.assertTrue(refresh_started.wait(1))
                service.update_account(
                    "token-1",
                    {"quota": 7, "email": "replacement@example.test"},
                )
                release_refresh.set()
                worker.join(2)

            self.assertEqual(errors, [])
            self.assertEqual(result, ["token-1"])
            current = service.get_account("token-1")
            self.assertIsNotNone(current)
            self.assertEqual(current["quota"], 7)
            self.assertEqual(current["email"], "replacement@example.test")
            self.assertEqual(current["refresh_token"], "refresh-1")

    def test_fetch_remote_info_rechecks_deadline_at_account_commit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = AccountService(JSONStorageBackend(Path(tmp_dir) / "accounts.json"))
            service.add_accounts(["token-1"])
            service.update_account(
                "token-1",
                {"type": "free", "status": "正常", "quota": 1},
            )

            class FakeBackend:
                def __init__(self, _access_token: str) -> None:
                    pass

                def get_user_info(self, **_kwargs: object) -> dict:
                    return {"type": "Pro", "status": "正常", "quota": 99}

                def close(self) -> None:
                    pass

            clock = iter((1000.0, 1001.0))
            with (
                patch.object(service, "refresh_access_token", return_value="token-1"),
                patch("services.openai_backend_api.OpenAIBackendAPI", FakeBackend),
                patch.object(account_module.time, "monotonic", side_effect=lambda: next(clock)),
            ):
                with self.assertRaises(TimeoutError):
                    service.fetch_remote_info("token-1", deadline=1000.5)

            self.assertEqual(service.get_account("token-1")["type"], "free")

    def test_fetch_remote_info_success_clears_all_refresh_state_fields_and_round_trips(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "accounts.json"
            service = AccountService(JSONStorageBackend(path))
            service.add_account_items(
                [{
                    "access_token": "token-1",
                    "type": "free",
                    "status": "异常",
                    "quota": 1,
                    "invalid_count": 2,
                    "last_invalid_at": "2026-08-15T00:00:00+00:00",
                    "last_refresh_error": "账号访问令牌无效",
                    "last_refresh_error_at": "2026-08-15T00:01:00+00:00",
                }]
            )

            class FakeBackend:
                def __init__(self, _access_token: str) -> None:
                    pass

                def get_user_info(self, **_kwargs: object) -> dict:
                    return {"type": "Pro", "status": "正常", "quota": 99}

                def close(self) -> None:
                    pass

            with (
                patch.object(service, "refresh_access_token", return_value="token-1"),
                patch("services.openai_backend_api.OpenAIBackendAPI", FakeBackend),
            ):
                updated = service.fetch_remote_info("token-1")

            self.assertIsNotNone(updated)
            for field, expected in (
                ("invalid_count", 0),
                ("last_invalid_at", None),
                ("last_refresh_error", None),
                ("last_refresh_error_at", None),
            ):
                self.assertEqual(updated[field], expected)

            reloaded = AccountService(JSONStorageBackend(path))
            persisted = reloaded.get_account("token-1")
            self.assertIsNotNone(persisted)
            for field, expected in (
                ("invalid_count", 0),
                ("last_invalid_at", None),
                ("last_refresh_error", None),
                ("last_refresh_error_at", None),
            ):
                self.assertEqual(persisted[field], expected)

    def test_fetch_remote_info_save_failure_rolls_back_all_refresh_state_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "accounts.json"
            service = AccountService(JSONStorageBackend(path))
            service.add_account_items(
                [{
                    "access_token": "token-1",
                    "type": "free",
                    "status": "异常",
                    "quota": 1,
                    "invalid_count": 2,
                    "last_invalid_at": "2026-08-15T00:00:00+00:00",
                    "last_refresh_error": "账号访问令牌无效",
                    "last_refresh_error_at": "2026-08-15T00:01:00+00:00",
                }]
            )
            before = service.get_account("token-1")
            before_bytes = path.read_bytes()

            class FakeBackend:
                def __init__(self, _access_token: str) -> None:
                    pass

                def get_user_info(self, **_kwargs: object) -> dict:
                    return {"type": "Pro", "status": "正常", "quota": 99}

                def close(self) -> None:
                    pass

            with (
                patch.object(service, "refresh_access_token", return_value="token-1"),
                patch("services.openai_backend_api.OpenAIBackendAPI", FakeBackend),
                patch.object(service, "_save_accounts", side_effect=StorageDataError()),
                self.assertRaises(StorageDataError),
            ):
                service.fetch_remote_info("token-1")

            self.assertEqual(service.get_account("token-1"), before)
            self.assertEqual(path.read_bytes(), before_bytes)

    def test_backend_account_json_response_is_streamed_and_closed(self) -> None:
        class Response:
            status_code = 200
            ok = True
            iter_content = None

            def __init__(self) -> None:
                self.closed = False
                self.content = b'{"email":"user@example.test","id":"user-1"}'

            def json(self):
                return {"email": "user@example.test", "id": "user-1"}

            def close(self) -> None:
                self.closed = True

        response = Response()
        backend = OpenAIBackendAPI(access_token="token-1")
        try:
            with patch.object(backend.session, "get", return_value=response) as get:
                result = backend._get_me()
        finally:
            backend.close()

        self.assertEqual(result, {"email": "user@example.test", "id": "user-1"})
        self.assertTrue(response.closed)
        self.assertTrue(get.call_args.kwargs["stream"])

    def test_search_account_type_ignores_unrelated_scalar_values(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = AccountService(JSONStorageBackend(Path(tmp_dir) / "accounts.json"))
            self.assertIsNone(
                service._search_account_type(
                    {
                        "amr": ["pwd", "otp", "mfa"],
                        "chatgpt_compute_residency": "no_constraint",
                        "chatgpt_data_residency": "no_constraint",
                        "user_id": "user-I52GFfLGFM0dokFk2dBiKEBn",
                    }
                )
            )

    def test_mark_image_result_consumes_quota(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = AccountService(JSONStorageBackend(Path(tmp_dir) / "accounts.json"))
            service.add_accounts(["token-1"])
            service.update_account(
                "token-1",
                {
                    "status": "正常",
                    "quota": 1,
                },
            )

            updated = service.mark_image_result("token-1", success=True)

            self.assertIsNotNone(updated)
            self.assertEqual(updated["quota"], 0)
            self.assertEqual(updated["status"], "限流")

    def test_mark_image_result_does_not_release_generator_lease(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = AccountService(JSONStorageBackend(Path(tmp_dir) / "accounts.json"))
            service.add_accounts(["token-1"])
            service.update_account(
                "token-1",
                {
                    "status": "正常",
                    "quota": 2,
                },
            )
            service._image_inflight["token-1"] = 2

            updated = service.mark_image_result("token-1", success=True)

            self.assertIsNotNone(updated)
            self.assertEqual(updated["quota"], 1)
            self.assertEqual(service._image_inflight["token-1"], 2)
            service.release_image_slot("token-1")
            self.assertEqual(service._image_inflight["token-1"], 1)

    def test_mark_image_result_rejects_replaced_account_identity(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = AccountService(JSONStorageBackend(Path(tmp_dir) / "accounts.json"))
            service.add_account_items([{
                "access_token": "token-1",
                "status": "正常",
                "quota": 3,
                "success": 4,
                "fail": 5,
            }])
            _, leased_account = service._get_account_lease("token-1")
            self.assertIsNotNone(leased_account)

            service.update_account(
                "token-1",
                {"status": "正常", "quota": 9, "success": 40, "fail": 50},
            )
            for success in (True, False):
                with self.subTest(success=success):
                    self.assertIsNone(
                        service.mark_image_result(
                            "token-1",
                            success,
                            expected_account=leased_account,
                        )
                    )

            current = service.get_account("token-1")
            self.assertIsNotNone(current)
            self.assertEqual(current["quota"], 9)
            self.assertEqual(current["success"], 40)
            self.assertEqual(current["fail"], 50)

    def test_mark_text_used_does_not_persist_the_whole_account_snapshot_per_request(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            backend = JSONStorageBackend(Path(tmp_dir) / "accounts.json")
            service = AccountService(backend)
            service.add_accounts(["token-1"])

            with patch.object(service, "_save_accounts") as save_accounts:
                for _ in range(25):
                    service.mark_text_used("token-1")

            save_accounts.assert_not_called()
            last_used_at = service.get_account("token-1")["last_used_at"]
            self.assertIsNotNone(last_used_at)

            # Usage metadata remains part of the in-memory snapshot and is
            # persisted by the next real account mutation.
            service.update_account("token-1", {"status": "正常"})
            reloaded = AccountService(backend)
            self.assertEqual(reloaded.get_account("token-1")["last_used_at"], last_used_at)

    def test_split_image_model_supports_plan_type_prefix(self) -> None:
        self.assertEqual(split_image_model("gpt-image-2"), (None, "gpt-image-2"))
        self.assertEqual(split_image_model("plus-codex-gpt-image-2"), ("plus", "codex-gpt-image-2"))
        self.assertEqual(split_image_model("team-codex-gpt-image-2"), ("team", "codex-gpt-image-2"))
        self.assertEqual(split_image_model("pro-codex-gpt-image-2"), ("pro", "codex-gpt-image-2"))
        self.assertEqual(split_image_model("plus-gpt-image-2"), (None, None))
        self.assertEqual(split_image_model("unknown-image-model"), (None, None))

    def test_get_available_access_token_filters_by_plan_type(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = AccountService(JSONStorageBackend(Path(tmp_dir) / "accounts.json"))
            service.add_account_items(
                [
                    {"access_token": "token-plus", "type": "Plus", "status": "正常", "quota": 3},
                    {"access_token": "token-pro", "type": "Pro", "status": "正常", "quota": 3},
                ]
            )

            service.fetch_remote_info = lambda access_token, event="fetch_remote_info": service.get_account(access_token)

            plus_token = service.get_available_access_token(plan_type="plus")
            pro_token = service.get_available_access_token(plan_type="pro")
            service.release_image_slot(plus_token)
            service.release_image_slot(pro_token)

            self.assertEqual(plus_token, "token-plus")
            self.assertEqual(pro_token, "token-pro")

    def test_remote_account_check_failure_is_not_reported_as_quota_exhaustion(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = AccountService(JSONStorageBackend(Path(tmp_dir) / "accounts.json"))
            service.add_account_items([
                {"access_token": "remote-check-token", "type": "Plus", "status": "正常", "quota": 3},
            ])
            service.fetch_remote_info = Mock(
                side_effect=TimeoutError("upstream timeout with secret payload")
            )

            with self.assertRaisesRegex(RuntimeError, "image account remote check failed") as raised:
                service.get_available_access_token(plan_type="plus")
            self.assertIsNone(raised.exception.__cause__)
            self.assertNotIn("secret payload", repr(raised.exception))

    def test_refresh_accounts_can_remove_invalid_token_without_confirmation_delay(self) -> None:
        original_value = config.data.get("auto_remove_invalid_accounts")
        config.data["auto_remove_invalid_accounts"] = True
        try:
            with tempfile.TemporaryDirectory() as tmp_dir:
                service = AccountService(JSONStorageBackend(Path(tmp_dir) / "accounts.json"))
                service.add_account_items([{"access_token": "invalid-token", "status": "正常"}])

                with patch(
                    "services.openai_backend_api.OpenAIBackendAPI.get_user_info",
                    side_effect=InvalidAccessTokenError("token invalidated (/backend-api/me)"),
                ):
                    result = service.refresh_accounts(["invalid-token"], defer_invalid_removal=False)

                self.assertEqual(result["refreshed"], 0)
                self.assertEqual(len(result["errors"]), 1)
                self.assertEqual(result["items"], [])
                self.assertIsNone(service.get_account("invalid-token"))
        finally:
            if original_value is None:
                config.data.pop("auto_remove_invalid_accounts", None)
            else:
                config.data["auto_remove_invalid_accounts"] = original_value

    def test_refresh_accounts_defers_invalid_token_removal_by_default(self) -> None:
        original_value = config.data.get("auto_remove_invalid_accounts")
        config.data["auto_remove_invalid_accounts"] = True
        try:
            with tempfile.TemporaryDirectory() as tmp_dir:
                service = AccountService(JSONStorageBackend(Path(tmp_dir) / "accounts.json"))
                service.add_account_items([{"access_token": "invalid-token", "status": "正常"}])

                with patch(
                    "services.openai_backend_api.OpenAIBackendAPI.get_user_info",
                    side_effect=InvalidAccessTokenError("token invalidated (/backend-api/me)"),
                ):
                    result = service.refresh_accounts(["invalid-token"])

                account = service.get_account("invalid-token")
                self.assertEqual(result["refreshed"], 0)
                self.assertEqual(len(result["errors"]), 1)
                self.assertIsNotNone(account)
                self.assertEqual(account["invalid_count"], 1)
        finally:
            if original_value is None:
                config.data.pop("auto_remove_invalid_accounts", None)
            else:
                config.data["auto_remove_invalid_accounts"] = original_value


class TokenLogTests(unittest.TestCase):
    def test_anonymize_token_hides_raw_value(self) -> None:
        token = "super-secret-token"
        token_ref = anonymize_token(token)

        self.assertTrue(token_ref.startswith("token:"))
        self.assertNotIn(token, token_ref)


class AuthServiceTests(unittest.TestCase):
    def test_auth_snapshot_rejects_container_name_without_stringifying_it(self) -> None:
        canary = "auth-name-container-canary owner@example.com"
        with tempfile.TemporaryDirectory() as tmp_dir:
            auth_keys_path = Path(tmp_dir) / "auth_keys.json"
            original = (
                '[{"id":"key-1","role":"user","key_hash":"hash",'
                f'"name":{{"secret":"{canary}"}},"enabled":true,'
                '"created_at":"2026-08-14T00:00:00+00:00","last_used_at":null}]\n'
            )
            auth_keys_path.write_text(original, encoding="utf-8")

            with self.assertRaises(StorageDataError):
                AuthService(JSONStorageBackend(Path(tmp_dir) / "accounts.json", auth_keys_path))

            self.assertEqual(auth_keys_path.read_text(encoding="utf-8"), original)

    def test_create_authenticate_disable_and_delete_user_key(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = AuthService(JSONStorageBackend(Path(tmp_dir) / "accounts.json", Path(tmp_dir) / "auth_keys.json"))

            item, raw_key = service.create_key(role="user", name="Alice")

            self.assertEqual(item["role"], "user")
            self.assertEqual(item["name"], "Alice")
            self.assertTrue(item["enabled"])
            self.assertTrue(raw_key.startswith("sk-"))

            authed = service.authenticate(raw_key)
            self.assertIsNotNone(authed)
            self.assertEqual(authed["id"], item["id"])
            self.assertEqual(authed["role"], "user")
            self.assertIsNotNone(authed["last_used_at"])

            updated = service.update_key(item["id"], {"enabled": False}, role="user")
            self.assertIsNotNone(updated)
            self.assertFalse(updated["enabled"])
            self.assertIsNone(service.authenticate(raw_key))

            self.assertTrue(service.delete_key(item["id"], role="user"))
            self.assertFalse(service.delete_key(item["id"], role="user"))
            self.assertEqual(service.list_keys(role="user"), [])

    def test_authenticate_ignores_last_used_save_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = AuthService(JSONStorageBackend(Path(tmp_dir) / "accounts.json", Path(tmp_dir) / "auth_keys.json"))
            item, raw_key = service.create_key(role="user", name="Alice")

            def fail_save() -> None:
                raise OSError("disk unavailable")

            service._save = fail_save

            authed = service.authenticate(raw_key)

            self.assertIsNotNone(authed)
            self.assertEqual(authed["id"], item["id"])
            self.assertIsNotNone(authed["last_used_at"])
            self.assertIsNone(
                next(current for current in service._items if current["id"] == item["id"])["last_used_at"]
            )

    def test_auth_key_name_length_is_rejected_before_persistence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            accounts_path = Path(tmp_dir) / "accounts.json"
            auth_keys_path = Path(tmp_dir) / "auth_keys.json"
            service = AuthService(JSONStorageBackend(accounts_path, auth_keys_path))
            oversized_name = "x" * 257

            with self.assertRaises(PublicSafeValueError):
                service.create_key(role="user", name=oversized_name)
            self.assertFalse(auth_keys_path.exists())

            item, _ = service.create_key(role="user", name="Alice")
            before = auth_keys_path.read_bytes()
            with self.assertRaises(PublicSafeValueError):
                service.update_key(item["id"], {"name": oversized_name}, role="user")
            self.assertEqual(auth_keys_path.read_bytes(), before)
            self.assertEqual(service.list_keys(role="user")[0]["name"], "Alice")

    def test_update_user_key_replaces_raw_key(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = AuthService(JSONStorageBackend(Path(tmp_dir) / "accounts.json", Path(tmp_dir) / "auth_keys.json"))
            item, raw_key = service.create_key(role="user", name="Alice")

            updated = service.update_key(item["id"], {"key": "sk-user-custom-key"}, role="user")

            self.assertIsNotNone(updated)
            self.assertIsNone(service.authenticate(raw_key))

            authed = service.authenticate("sk-user-custom-key")
            self.assertIsNotNone(authed)
            self.assertEqual(authed["id"], item["id"])

    def test_user_key_name_must_be_unique(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = AuthService(JSONStorageBackend(Path(tmp_dir) / "accounts.json", Path(tmp_dir) / "auth_keys.json"))
            first, _ = service.create_key(role="user", name="Alice")
            second, _ = service.create_key(role="user", name="Bob")

            with self.assertRaisesRegex(ValueError, "这个名称已经在使用中了"):
                service.create_key(role="user", name="Alice")

            with self.assertRaisesRegex(ValueError, "这个名称已经在使用中了"):
                service.update_key(second["id"], {"name": "Alice"}, role="user")

            updated = service.update_key(first["id"], {"name": "Alice"}, role="user")
            self.assertIsNotNone(updated)
            self.assertEqual(updated["name"], "Alice")


if __name__ == "__main__":
    unittest.main()
