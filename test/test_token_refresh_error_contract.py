from __future__ import annotations

import copy
import base64
import io
import json
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import redirect_stdout
from threading import Event
from types import SimpleNamespace
from unittest import mock

import api.support as support_module
import services.account_service as account_module
from services.account_service import AccountService, TokenRefreshError


SECRET = "opaque-refresh-secret user:password@auth.example owner@example.com"


class MemoryStorage:
    def __init__(self, accounts: list[dict[str, object]]) -> None:
        self.accounts = copy.deepcopy(accounts)

    def load_accounts(self) -> list[dict[str, object]]:
        return copy.deepcopy(self.accounts)

    def save_accounts(self, accounts: list[dict[str, object]]) -> None:
        self.accounts = copy.deepcopy(accounts)


class FailingSaveStorage(MemoryStorage):
    def __init__(self, accounts: list[dict[str, object]]) -> None:
        super().__init__(accounts)
        self.fail_saves = False

    def save_accounts(self, accounts: list[dict[str, object]]) -> None:
        if self.fail_saves:
            raise OSError("save failed")
        super().save_accounts(accounts)


def make_service() -> tuple[AccountService, MemoryStorage]:
    storage = MemoryStorage(
        [
            {
                "access_token": "access-token",
                "refresh_token": "refresh-token",
                "status": "正常",
                "type": "free",
                "quota": 1,
            }
        ]
    )
    return AccountService(storage), storage


class TokenRefreshErrorContractTests(unittest.TestCase):
    def test_refresh_deadline_covers_waiting_token_refresh_slot(self) -> None:
        service, _ = make_service()
        with service._token_refresh_slot("access-token"):
            with ThreadPoolExecutor(max_workers=1) as executor:
                pending = executor.submit(
                    service.refresh_access_token,
                    "access-token",
                    force=True,
                    deadline=time.monotonic() + 0.05,
                )
                with self.assertRaises(TimeoutError):
                    pending.result(timeout=1)

    def test_refresh_request_rejects_an_expired_absolute_deadline_before_network_io(self) -> None:
        service, _ = make_service()
        post = mock.Mock()

        class Session:
            def post(self, *args, **kwargs):
                post(*args, **kwargs)
                raise AssertionError("expired refresh must not start network I/O")

            def close(self):
                pass

        with (
            mock.patch("curl_cffi.requests.Session", return_value=Session()),
            mock.patch("services.proxy_service.proxy_settings.build_session_kwargs", return_value={}),
            mock.patch.object(account_module.time, "monotonic", return_value=100.0),
        ):
            with self.assertRaises(TimeoutError):
                service._request_access_token_refresh("refresh-token", deadline=100.0)

        post.assert_not_called()

    def test_password_verify_malformed_error_shape_returns_fixed_failure(self) -> None:
        secret = "password-error-shape-secret owner@example.com"
        session_options: dict[str, object] = {}

        class Response:
            def __init__(self, status_code: int, payload: object, url: str = "") -> None:
                self.status_code = status_code
                self.url = url
                self.closed = False
                self._body = json.dumps(payload).encode("utf-8")

            def iter_content(self, *, chunk_size: int):
                yield self._body

            def close(self) -> None:
                self.closed = True

        class Session:
            def __init__(self, *args, **kwargs) -> None:
                session_options.update(kwargs)
                self.cookies = mock.Mock()
                self.responses: list[Response] = []

            def get(self, url: str, *args, **kwargs) -> Response:
                response = Response(
                    200,
                    {},
                    "https://platform.openai.com/auth/callback?code=login-code",
                )
                self.responses.append(response)
                return response

            def post(self, url: str, *args, **kwargs) -> Response:
                response = Response(
                    400,
                    {"error": [secret], "message": secret},
                )
                self.responses.append(response)
                return response

            def close(self) -> None:
                pass

        service = AccountService.__new__(AccountService)
        with (
            mock.patch("curl_cffi.requests.Session", Session),
            mock.patch.object(account_module.config, "get_proxy_settings", return_value=""),
            mock.patch("utils.sentinel.build_sentinel_token", return_value=("", "")),
        ):
            result = service._login_with_password("owner@example.com", "password")

        self.assertEqual(result, {
            "ok": False,
            "error": "password_verify_failed_400",
            "detail": {},
        })
        self.assertTrue(session_options["verify"])
        self.assertNotIn(secret, json.dumps(result, ensure_ascii=False, default=str))

    def test_password_verify_success_with_container_payload_returns_fixed_failure(self) -> None:
        secret = "password-success-shape-secret"

        class Response:
            def __init__(self, status_code: int, payload: object, url: str = "") -> None:
                self.status_code = status_code
                self.url = url
                self._body = json.dumps(payload).encode("utf-8")

            def iter_content(self, *, chunk_size: int):
                yield self._body

            def close(self) -> None:
                pass

        class Session:
            def __init__(self, *args, **kwargs) -> None:
                self.cookies = mock.Mock()

            def get(self, url: str, *args, **kwargs) -> Response:
                return Response(
                    200,
                    {},
                    "https://platform.openai.com/auth/callback?code=login-code",
                )

            def post(self, url: str, *args, **kwargs) -> Response:
                return Response(200, [secret])

            def close(self) -> None:
                pass

        service = AccountService.__new__(AccountService)
        with (
            mock.patch("curl_cffi.requests.Session", Session),
            mock.patch.object(account_module.config, "get_proxy_settings", return_value=""),
            mock.patch("utils.sentinel.build_sentinel_token", return_value=("", "")),
        ):
            result = service._login_with_password("owner@example.com", "password")

        self.assertEqual(result, {
            "ok": False,
            "error": "password_verify_invalid_response",
            "detail": {},
        })
        self.assertNotIn(secret, json.dumps(result, ensure_ascii=False, default=str))

    def test_authorize_error_payload_is_not_returned_to_password_login_caller(self) -> None:
        secret = "authorize-error-payload-secret owner@example.com"
        encoded_payload = base64.b64encode(
            json.dumps({"errorCode": secret, "message": secret}).encode("utf-8")
        ).decode("ascii")

        class Response:
            status_code = 200

            def __init__(self) -> None:
                self.url = f"https://auth.openai.com/error?payload={encoded_payload}"

            def close(self) -> None:
                pass

        class Session:
            def __init__(self, *args, **kwargs) -> None:
                self.cookies = mock.Mock()

            def get(self, *args, **kwargs) -> Response:
                return Response()

            def close(self) -> None:
                pass

        service = AccountService.__new__(AccountService)
        with (
            mock.patch("curl_cffi.requests.Session", Session),
            mock.patch.object(account_module.config, "get_proxy_settings", return_value=""),
        ):
            result = service._login_with_password("owner@example.com", "password")

        self.assertEqual(result, {
            "ok": False,
            "error": "authorize_redirect_error",
            "detail": {},
        })
        self.assertNotIn(secret, json.dumps(result, ensure_ascii=False, default=str))

    def test_password_verify_without_code_does_not_return_upstream_detail(self) -> None:
        secret = "password-no-code-detail-secret owner@example.com"

        class Response:
            def __init__(self, payload: object, url: str = "") -> None:
                self.status_code = 200
                self.url = url
                self._body = json.dumps(payload).encode("utf-8")

            def iter_content(self, *, chunk_size: int):
                yield self._body

            def close(self) -> None:
                pass

        class Session:
            def __init__(self, *args, **kwargs) -> None:
                self.cookies = mock.Mock()

            def get(self, *args, **kwargs) -> Response:
                return Response(
                    {},
                    "https://platform.openai.com/auth/callback?code=login-code",
                )

            def post(self, *args, **kwargs) -> Response:
                return Response({"message": secret, "nested": {"secret": secret}})

            def close(self) -> None:
                pass

        service = AccountService.__new__(AccountService)
        with (
            mock.patch("curl_cffi.requests.Session", Session),
            mock.patch.object(account_module.config, "get_proxy_settings", return_value=""),
            mock.patch("utils.sentinel.build_sentinel_token", return_value=("", "")),
        ):
            result = service._login_with_password("owner@example.com", "password")

        self.assertEqual(result, {
            "ok": False,
            "error": "no_auth_code",
            "detail": {},
        })
        self.assertNotIn(secret, json.dumps(result, ensure_ascii=False, default=str))

    def test_password_login_malformed_profile_body_does_not_abort_valid_token_login(self) -> None:
        canary = "profile-container-canary"

        class Response:
            def __init__(self, status_code: int, payload: object, url: str = "") -> None:
                self.status_code = status_code
                self.url = url
                self._body = json.dumps(payload).encode("utf-8")

            def iter_content(self, *, chunk_size: int):
                yield self._body

            def close(self) -> None:
                pass

        class Session:
            def __init__(self, *args, **kwargs) -> None:
                self.cookies = mock.Mock()
                self.posts = 0

            def get(self, url: str, *args, **kwargs) -> Response:
                if url.endswith("/me"):
                    return Response(200, [canary])
                return Response(
                    200,
                    {},
                    "https://platform.openai.com/auth/callback?code=login-code",
                )

            def post(self, url: str, *args, **kwargs) -> Response:
                self.posts += 1
                if self.posts == 1:
                    return Response(200, {"continue_url": "https://platform.openai.com/auth/callback?code=login-code"})
                return Response(200, {
                    "access_token": "access-token",
                    "refresh_token": "refresh-token",
                    "id_token": "id-token",
                })

            def close(self) -> None:
                pass

        service = AccountService.__new__(AccountService)
        with (
            mock.patch("curl_cffi.requests.Session", Session),
            mock.patch.object(account_module.config, "get_proxy_settings", return_value=""),
            mock.patch.object(service, "_decode_jwt_payload", return_value={}),
            mock.patch.object(
                account_module,
                "parse_json_response",
                side_effect=[
                    {"continue_url": "https://platform.openai.com/auth/callback?code=login-code"},
                    {"access_token": "access-token", "refresh_token": "refresh-token", "id_token": "id-token"},
                    [canary],
                ],
            ),
        ):
            result = service._login_with_password("owner@example.com", "password")

        self.assertTrue(result["ok"])
        self.assertEqual(result["access_token"], "access-token")
        self.assertNotIn(canary, json.dumps(result, ensure_ascii=False, default=str))

    def test_password_login_close_failure_does_not_replace_successful_result(self) -> None:
        class Response:
            def __init__(self, status_code: int, url: str = "") -> None:
                self.status_code = status_code
                self.url = url

            def close(self) -> None:
                pass

        class Session:
            def __init__(self, *args, **kwargs) -> None:
                self.cookies = mock.Mock()
                self.posts = 0

            def get(self, url: str, *args, **kwargs) -> Response:
                if url.endswith("/me"):
                    return Response(200)
                return Response(200, "https://platform.openai.com/auth/callback?code=login-code")

            def post(self, url: str, *args, **kwargs) -> Response:
                self.posts += 1
                return Response(200)

            def close(self) -> None:
                raise RuntimeError("session close failed")

        service = AccountService.__new__(AccountService)
        with (
            mock.patch("curl_cffi.requests.Session", Session),
            mock.patch.object(account_module.config, "get_proxy_settings", return_value=""),
            mock.patch("utils.sentinel.build_sentinel_token", return_value=("", "")),
            mock.patch.object(service, "_decode_jwt_payload", return_value={}),
            mock.patch.object(
                account_module,
                "parse_json_response",
                side_effect=[
                    {"continue_url": "https://platform.openai.com/auth/callback?code=login-code"},
                    {"access_token": "access-token", "refresh_token": "refresh-token", "id_token": "id-token"},
                    {"account": {}},
                ],
            ),
        ):
            result = service._login_with_password("owner@example.com", "password")

        self.assertTrue(result["ok"])
        self.assertEqual(result["access_token"], "access-token")

    def test_slow_refresh_for_one_account_does_not_block_another_valid_account(self) -> None:
        storage = MemoryStorage(
            [
                {
                    "access_token": "slow-access-token",
                    "refresh_token": "slow-refresh-token",
                    "status": "正常",
                    "type": "free",
                    "quota": 1,
                },
                {
                    "access_token": "fast-access-token",
                    "refresh_token": "fast-refresh-token",
                    "status": "正常",
                    "type": "free",
                    "quota": 1,
                },
            ]
        )
        service = AccountService(storage)
        refresh_started = Event()
        release_refresh = Event()
        refresh_calls: list[str] = []

        def request_refresh(
            refresh_token: str,
            _account: dict | None = None,
            **_kwargs,
        ) -> dict[str, str]:
            refresh_calls.append(refresh_token)
            refresh_started.set()
            if not release_refresh.wait(timeout=2):
                raise TimeoutError("test did not release token refresh")
            return {
                "access_token": "slow-access-token",
                "refresh_token": "slow-refresh-token",
            }

        real_hash = hash

        def colliding_refresh_hash(value: object) -> int:
            if value in {"slow-access-token", "fast-access-token"}:
                return 0
            return real_hash(value)

        with (
            mock.patch("builtins.hash", side_effect=colliding_refresh_hash),
            mock.patch.object(service, "_request_access_token_refresh", side_effect=request_refresh),
            ThreadPoolExecutor(max_workers=2) as executor,
        ):
            slow = executor.submit(
                service.refresh_access_token,
                "slow-access-token",
                force=True,
            )
            self.assertTrue(refresh_started.wait(timeout=1))
            fast = executor.submit(service.refresh_access_token, "fast-access-token")
            try:
                fast_token = fast.result(timeout=0.2)
            finally:
                release_refresh.set()
            slow_token = slow.result(timeout=2)

        self.assertEqual(fast_token, "fast-access-token")
        self.assertEqual(slow_token, "slow-access-token")
        self.assertEqual(refresh_calls, ["slow-refresh-token"])

    def test_concurrent_refreshes_for_the_same_account_remain_single_flight(self) -> None:
        service, _ = make_service()
        refresh_started = Event()
        release_refresh = Event()
        refresh_calls: list[str] = []

        def request_refresh(
            refresh_token: str,
            _account: dict | None = None,
            **_kwargs,
        ) -> dict[str, str]:
            refresh_calls.append(refresh_token)
            refresh_started.set()
            if not release_refresh.wait(timeout=2):
                raise TimeoutError("test did not release token refresh")
            return {
                "access_token": "access-token",
                "refresh_token": "refresh-token",
            }

        with (
            mock.patch.object(
                service,
                "_token_needs_refresh",
                side_effect=lambda _token, force=False: force or not refresh_calls,
            ),
            mock.patch.object(service, "_request_access_token_refresh", side_effect=request_refresh),
            ThreadPoolExecutor(max_workers=2) as executor,
        ):
            first = executor.submit(service.refresh_access_token, "access-token")
            self.assertTrue(refresh_started.wait(timeout=1))
            second = executor.submit(service.refresh_access_token, "access-token")
            self.assertFalse(second.done())
            release_refresh.set()
            self.assertEqual(first.result(timeout=2), "access-token")
            self.assertEqual(second.result(timeout=2), "access-token")

        self.assertEqual(refresh_calls, ["refresh-token"])

    def test_rotation_keeps_new_token_on_the_same_refresh_single_flight(self) -> None:
        service, _ = make_service()
        service._accounts = {
            "old-token": {
                "access_token": "old-token",
                "refresh_token": "old-refresh",
                "status": "正常",
                "type": "free",
                "quota": 1,
            }
        }
        service._token_aliases = {}
        service._token_needs_refresh = lambda *_args, **_kwargs: True
        first_request_started = Event()
        rotation_committed = Event()
        release_first_owner = Event()
        refresh_calls: list[str] = []
        apply_calls = 0

        def request_refresh(refresh_token: str, _account: dict | None = None, **_kwargs) -> dict[str, str]:
            refresh_calls.append(refresh_token)
            if len(refresh_calls) == 1:
                first_request_started.set()
                return {
                    "access_token": "new-token",
                    "refresh_token": "new-refresh",
                }
            return {
                "access_token": "new-token",
                "refresh_token": "new-refresh",
            }

        real_apply = service._apply_refreshed_tokens

        def block_after_rotation(*args, **kwargs) -> str:
            nonlocal apply_calls
            result = real_apply(*args, **kwargs)
            apply_calls += 1
            if apply_calls == 1:
                rotation_committed.set()
                if not release_first_owner.wait(timeout=2):
                    raise AssertionError("first refresh owner was not released")
            return result

        with (
            mock.patch.object(service, "_request_access_token_refresh", side_effect=request_refresh),
            mock.patch.object(service, "_apply_refreshed_tokens", side_effect=block_after_rotation),
            ThreadPoolExecutor(max_workers=3) as executor,
        ):
            first = executor.submit(service.refresh_access_token, "old-token", force=True)
            self.assertTrue(first_request_started.wait(timeout=1))
            self.assertTrue(rotation_committed.wait(timeout=1))

            second = executor.submit(service.refresh_access_token, "new-token", force=True)
            alias_waiter = executor.submit(service.refresh_access_token, "old-token", force=True)
            self.assertFalse(second.done())
            self.assertFalse(alias_waiter.done())
            release_first_owner.set()
            first.result(timeout=2)
            second.result(timeout=2)
            self.assertEqual(alias_waiter.result(timeout=2), "new-token")

        self.assertEqual(refresh_calls, ["old-refresh"])

    def test_refresh_via_rotated_alias_uses_canonical_owner_lease(self) -> None:
        service, _ = make_service()
        service._accounts = {
            "alias-a": {
                "access_token": "alias-a",
                "refresh_token": "alias-refresh",
                "status": "正常",
                "type": "free",
                "quota": 1,
            }
        }
        service._token_aliases = {}
        service._apply_refreshed_tokens(
            "alias-a",
            {"access_token": "canonical-b", "refresh_token": "canonical-refresh"},
            "test_rotation_setup",
        )
        service._token_needs_refresh = lambda *_args, **_kwargs: True
        refresh_calls: list[str] = []

        def request_refresh(refresh_token: str, _account: dict | None = None, **_kwargs) -> dict[str, str]:
            refresh_calls.append(refresh_token)
            return {"access_token": "canonical-c", "refresh_token": "next-refresh"}

        with mock.patch.object(service, "_request_access_token_refresh", side_effect=request_refresh):
            refreshed = service.refresh_access_token("alias-a", force=True)

        self.assertEqual(refreshed, "canonical-c")
        self.assertEqual(refresh_calls, ["canonical-refresh"])
        self.assertEqual(service.resolve_access_token("canonical-b"), "canonical-c")
        self.assertEqual(service.resolve_access_token("alias-a"), "canonical-c")

    def test_refresh_via_alias_accepts_the_current_canonical_expected_account(self) -> None:
        service, _ = make_service()
        service._accounts = {
            "alias-a": {
                "access_token": "alias-a",
                "refresh_token": "alias-refresh",
                "status": "正常",
                "type": "free",
                "quota": 1,
            }
        }
        service._token_aliases = {}
        service._apply_refreshed_tokens(
            "alias-a",
            {"access_token": "canonical-b", "refresh_token": "canonical-refresh"},
            "test_rotation_setup",
        )
        _, expected_account = service._get_account_lease("alias-a")
        assert expected_account is not None
        service._token_needs_refresh = lambda *_args, **_kwargs: True

        with mock.patch.object(
            service,
            "_request_access_token_refresh",
            return_value={"access_token": "canonical-c", "refresh_token": "next-refresh"},
        ):
            refreshed = service.refresh_access_token(
                "alias-a",
                force=True,
                expected_account=expected_account,
            )

        self.assertEqual(refreshed, "canonical-c")

    def test_late_refresh_result_cannot_overwrite_same_token_account_rebuilt_during_refresh(self) -> None:
        service, _ = make_service()
        service._token_needs_refresh = lambda *_args, **_kwargs: True
        refresh_started = Event()
        release_refresh = Event()

        def deferred_refresh(*_args, **_kwargs) -> dict[str, str]:
            refresh_started.set()
            if not release_refresh.wait(timeout=2):
                raise AssertionError("refresh was not released")
            return {
                "access_token": "access-token",
                "refresh_token": "stale-refresh-token",
            }

        with (
            mock.patch.object(service, "_request_access_token_refresh", side_effect=deferred_refresh),
            ThreadPoolExecutor(max_workers=1) as executor,
        ):
            pending = executor.submit(service.refresh_access_token, "access-token", force=True)
            self.assertTrue(refresh_started.wait(timeout=1))
            service.update_account(
                "access-token",
                {"refresh_token": "rebuilt-refresh-token", "email": "rebuilt@example.test"},
            )
            release_refresh.set()
            pending.result(timeout=2)

        current = service.get_account("access-token")
        self.assertIsNotNone(current)
        assert current is not None
        self.assertEqual(current.get("refresh_token"), "rebuilt-refresh-token")
        self.assertEqual(current.get("email"), "rebuilt@example.test")

    def test_fetch_remote_info_does_not_rebind_stale_refresh_to_recreated_account(self) -> None:
        service, _ = make_service()
        service._token_needs_refresh = lambda *_args, **_kwargs: True
        refresh_started = Event()
        release_refresh = Event()
        backend_calls: list[str] = []

        def deferred_refresh(*_args, **_kwargs) -> dict[str, str]:
            refresh_started.set()
            if not release_refresh.wait(timeout=2):
                raise AssertionError("refresh was not released")
            return {
                "access_token": "access-token",
                "refresh_token": "stale-refresh-token",
            }

        class FakeBackend:
            def __init__(self, access_token: str) -> None:
                backend_calls.append(access_token)

            def get_user_info(self, **_kwargs) -> dict[str, str]:
                return {"email": "late-remote@example.test"}

            def close(self) -> None:
                return None

        with (
            mock.patch.object(service, "_request_access_token_refresh", side_effect=deferred_refresh),
            mock.patch("services.openai_backend_api.OpenAIBackendAPI", FakeBackend),
            ThreadPoolExecutor(max_workers=1) as executor,
        ):
            pending = executor.submit(
                service.fetch_remote_info,
                "access-token",
                "refresh_accounts",
                False,
            )
            self.assertTrue(refresh_started.wait(timeout=1))
            service.delete_accounts(["access-token"])
            service.add_account_items([
                {
                    "access_token": "access-token",
                    "refresh_token": "replacement-refresh-token",
                    "email": "replacement@example.test",
                    "status": "正常",
                    "quota": 9,
                }
            ])
            release_refresh.set()
            self.assertIsNone(pending.result(timeout=2))

        self.assertEqual(backend_calls, [])
        current = service.get_account("access-token")
        self.assertIsNotNone(current)
        assert current is not None
        self.assertEqual(current.get("email"), "replacement@example.test")
        self.assertEqual(current.get("refresh_token"), "replacement-refresh-token")

    def test_refresh_batch_progress_does_not_account_for_recreated_account(self) -> None:
        service, _ = make_service()
        service._token_needs_refresh = lambda *_args, **_kwargs: True
        refresh_started = Event()
        release_refresh = Event()

        def deferred_refresh(*_args, **_kwargs) -> dict[str, str]:
            refresh_started.set()
            if not release_refresh.wait(timeout=2):
                raise AssertionError("refresh was not released")
            return {
                "access_token": "access-token",
                "refresh_token": "stale-refresh-token",
            }

        class FakeBackend:
            def __init__(self, _access_token: str) -> None:
                pass

            def get_user_info(self, **_kwargs) -> dict[str, str]:
                return {"email": "late-remote@example.test"}

            def close(self) -> None:
                return None

        with (
            mock.patch.object(service, "_request_access_token_refresh", side_effect=deferred_refresh),
            mock.patch("services.openai_backend_api.OpenAIBackendAPI", FakeBackend),
            ThreadPoolExecutor(max_workers=1) as executor,
        ):
            pending = executor.submit(
                service.refresh_accounts,
                ["access-token"],
                "recreated-progress",
                False,
            )
            self.assertTrue(refresh_started.wait(timeout=1))
            service.delete_accounts(["access-token"])
            service.add_account_items([
                {
                    "access_token": "access-token",
                    "refresh_token": "replacement-refresh-token",
                    "email": "replacement@example.test",
                    "status": "正常",
                    "quota": 9,
                }
            ])
            release_refresh.set()
            pending.result(timeout=3)

        progress = service.get_refresh_progress("recreated-progress")
        self.assertIsNotNone(progress)
        assert progress is not None
        self.assertEqual(progress["processed"], 1)
        self.assertEqual(
            progress["status_counts"],
            {"正常": 0, "限流": 0, "异常": 0, "禁用": 0},
        )
        self.assertEqual(progress["total_quota"], 0)

    def test_queued_refresh_does_not_claim_recreated_account(self) -> None:
        service, _ = make_service()
        service._token_needs_refresh = lambda *_args, **_kwargs: True
        blocker_started = Event()
        release_blocker = Event()
        refresh_submitted = Event()
        request_started = Event()

        def blocker() -> None:
            blocker_started.set()
            if not release_blocker.wait(timeout=2):
                raise AssertionError("executor blocker was not released")

        def request_refresh(*_args, **_kwargs) -> dict[str, str]:
            request_started.set()
            return {
                "access_token": "access-token",
                "refresh_token": "stale-refresh-token",
            }

        class FakeBackend:
            def __init__(self, _access_token: str) -> None:
                pass

            def get_user_info(self, **_kwargs) -> dict[str, str]:
                return {"email": "late-remote@example.test"}

            def close(self) -> None:
                return None

        with ThreadPoolExecutor(max_workers=1) as account_executor:
            blocker_future = account_executor.submit(blocker)
            self.assertTrue(blocker_started.wait(timeout=1))
            original_submit = account_executor.submit

            def submit_and_record(function, *args, **kwargs):
                future = original_submit(function, *args, **kwargs)
                refresh_submitted.set()
                return future

            with (
                mock.patch.object(account_module, "_ACCOUNT_REFRESH_EXECUTOR", account_executor),
                mock.patch.object(account_executor, "submit", side_effect=submit_and_record),
                mock.patch.object(service, "_request_access_token_refresh", side_effect=request_refresh),
                mock.patch("services.openai_backend_api.OpenAIBackendAPI", FakeBackend),
                ThreadPoolExecutor(max_workers=1) as caller_executor,
            ):
                pending = caller_executor.submit(
                    service.refresh_accounts,
                    ["access-token"],
                    "queued-recreated-progress",
                    False,
                )
                self.assertTrue(refresh_submitted.wait(timeout=1))
                service.delete_accounts(["access-token"])
                service.add_account_items([
                    {
                        "access_token": "access-token",
                        "refresh_token": "replacement-refresh-token",
                        "email": "replacement@example.test",
                        "status": "正常",
                        "quota": 9,
                    }
                ])
                release_blocker.set()
                blocker_future.result(timeout=2)
                pending.result(timeout=3)

        self.assertFalse(request_started.is_set())
        current = service.get_account("access-token")
        self.assertIsNotNone(current)
        assert current is not None
        self.assertEqual(current.get("email"), "replacement@example.test")
        self.assertEqual(current.get("refresh_token"), "replacement-refresh-token")

    def test_refresh_progress_uses_completed_snapshot_not_live_replacement(self) -> None:
        service, _ = make_service()
        progress_entered = Event()
        release_progress = Event()
        original_update = service.update_refresh_progress

        def completed_refresh(*_args, **_kwargs) -> dict[str, object]:
            return {
                "access_token": "rotated-access-token",
                "status": "正常",
                "quota": 1,
            }

        def blocked_update(progress_id: str, token: str, **kwargs) -> None:
            progress_entered.set()
            if not release_progress.wait(timeout=2):
                raise AssertionError("progress update was not released")
            original_update(progress_id, token, **kwargs)

        with (
            mock.patch.object(service, "fetch_remote_info", side_effect=completed_refresh),
            mock.patch.object(service, "update_refresh_progress", side_effect=blocked_update),
            ThreadPoolExecutor(max_workers=1) as executor,
        ):
            pending = executor.submit(
                service.refresh_accounts,
                ["access-token"],
                "snapshot-progress",
                False,
            )
            self.assertTrue(progress_entered.wait(timeout=1))
            service.delete_accounts(["access-token"])
            service.add_account_items([
                {
                    "access_token": "access-token",
                    "refresh_token": "replacement-refresh-token",
                    "email": "replacement@example.test",
                    "status": "正常",
                    "quota": 9,
                }
            ])
            release_progress.set()
            pending.result(timeout=3)

        progress = service.get_refresh_progress("snapshot-progress")
        self.assertIsNotNone(progress)
        assert progress is not None
        self.assertEqual(
            progress["status_counts"],
            {"正常": 1, "限流": 0, "异常": 0, "禁用": 0},
        )
        self.assertEqual(progress["total_quota"], 1)

    def test_rotation_save_failure_releases_old_and_new_refresh_slots(self) -> None:
        storage = FailingSaveStorage(
            [
                {
                    "access_token": "old-token",
                    "refresh_token": "old-refresh",
                    "status": "正常",
                    "type": "free",
                    "quota": 1,
                }
            ]
        )
        service = AccountService(storage)
        service._token_needs_refresh = lambda *_args, **_kwargs: True
        storage.fail_saves = True

        with mock.patch.object(
            service,
            "_request_access_token_refresh",
            return_value={"access_token": "new-token", "refresh_token": "new-refresh"},
        ):
            with self.assertRaises(OSError):
                service.refresh_access_token("old-token", force=True)

        self.assertEqual(service._active_token_refreshes, set())
        self.assertIsNotNone(service.get_account("old-token"))
        self.assertIsNone(service.get_account("new-token"))

    def test_refresh_exception_and_timeout_release_slot_for_retry(self) -> None:
        for first_error in (RuntimeError("provider failed"), TimeoutError("provider timed out")):
            with self.subTest(error=type(first_error).__name__):
                service, _ = make_service()
                service._token_needs_refresh = lambda *_args, **_kwargs: True
                calls = 0

                def request_refresh(*_args, **_kwargs) -> dict[str, str]:
                    nonlocal calls
                    calls += 1
                    if calls == 1:
                        raise first_error
                    return {
                        "access_token": "access-token",
                        "refresh_token": "refresh-token",
                    }

                with mock.patch.object(service, "_request_access_token_refresh", side_effect=request_refresh):
                    if isinstance(first_error, TimeoutError):
                        with self.assertRaises(TimeoutError):
                            service.refresh_access_token("access-token", force=True)
                    else:
                        self.assertEqual(service.refresh_access_token("access-token", force=True), "access-token")

                    self.assertEqual(service._active_token_refreshes, set())
                    self.assertEqual(service._token_refresh_leases, {})
                    self.assertEqual(service.refresh_access_token("access-token", force=True), "access-token")

                self.assertEqual(calls, 2)

    def test_legacy_invalid_token_error_is_migrated_to_a_safe_message(self) -> None:
        storage = MemoryStorage(
            [
                {
                    "access_token": "access-token",
                    "refresh_token": "refresh-token",
                    "last_refresh_error": SECRET,
                }
            ]
        )
        service = AccountService(storage)

        persisted = json.dumps(storage.accounts, ensure_ascii=False, default=str)
        returned = json.dumps(service.list_accounts(), ensure_ascii=False, default=str)
        self.assertNotIn(SECRET, persisted)
        self.assertNotIn(SECRET, returned)

    def test_legacy_persisted_refresh_error_is_migrated_to_a_safe_code(self) -> None:
        storage = MemoryStorage(
            [
                {
                    "access_token": "access-token",
                    "refresh_token": "refresh-token",
                    "last_token_refresh_error": SECRET,
                }
            ]
        )
        AccountService(storage)

        self.assertNotIn(SECRET, json.dumps(storage.accounts, ensure_ascii=False, default=str))

    def test_refresh_request_maps_network_and_payload_failures_to_fixed_codes(self) -> None:
        service, _ = make_service()

        class FailingSession:
            def post(self, *args, **kwargs):
                raise RuntimeError(SECRET)

            def close(self):
                pass

        with (
            mock.patch("curl_cffi.requests.Session", return_value=FailingSession()),
            mock.patch("services.proxy_service.proxy_settings.build_session_kwargs", return_value={}),
        ):
            with self.assertRaises(TokenRefreshError) as raised:
                service._request_access_token_refresh("refresh-token")
        self.assertEqual(raised.exception.code, "network_error")
        self.assertNotIn(SECRET, str(raised.exception))

    def test_refresh_request_preserves_only_structured_app_session_code(self) -> None:
        service, _ = make_service()

        class Response:
            status_code = 401
            text = '{"error":"invalid_grant","error_description":"app_session_terminated"}'

            def __init__(self) -> None:
                self.closed = False

            @staticmethod
            def json():
                return {"error": "invalid_grant", "error_description": "app_session_terminated"}

            def close(self) -> None:
                self.closed = True

        class Session:
            response = None
            kwargs = None

            def post(self, *args, **kwargs):
                type(self).kwargs = kwargs
                type(self).response = Response()
                return type(self).response

            def close(self):
                pass

        with (
            mock.patch("curl_cffi.requests.Session", return_value=Session()),
            mock.patch("services.proxy_service.proxy_settings.build_session_kwargs", return_value={}),
        ):
            with self.assertRaises(TokenRefreshError) as raised:
                service._request_access_token_refresh("refresh-token")
        self.assertEqual(raised.exception.code, "app_session_terminated")
        self.assertTrue(Session.response.closed)
        self.assertTrue(Session.kwargs["stream"])

    def test_refresh_request_closes_success_response(self) -> None:
        service, _ = make_service()

        class Response:
            status_code = 200
            text = '{"access_token":"new-access","refresh_token":"new-refresh","id_token":"new-id"}'

            def __init__(self) -> None:
                self.closed = False

            @staticmethod
            def json():
                return {
                    "access_token": "new-access",
                    "refresh_token": "new-refresh",
                    "id_token": "new-id",
                }

            def close(self) -> None:
                self.closed = True

        class Session:
            response = None
            kwargs = None

            def post(self, *args, **kwargs):
                type(self).kwargs = kwargs
                type(self).response = Response()
                return type(self).response

            def close(self):
                pass

        with (
            mock.patch("curl_cffi.requests.Session", return_value=Session()),
            mock.patch("services.proxy_service.proxy_settings.build_session_kwargs", return_value={}),
        ):
            result = service._request_access_token_refresh("refresh-token")
        self.assertEqual(result["access_token"], "new-access")
        self.assertTrue(Session.response.closed)
        self.assertTrue(Session.kwargs["stream"])

    def test_refresh_request_rejects_container_token_fields(self) -> None:
        service, _ = make_service()

        class Response:
            status_code = 200
            text = '{"access_token": {"secret": "refresh-token-canary"}}'

            @staticmethod
            def json():
                return {
                    "access_token": {"secret": "refresh-token-canary"},
                    "refresh_token": ["refresh-token-canary"],
                    "id_token": {"secret": "refresh-token-canary"},
                }

        class Session:
            def post(self, *args, **kwargs):
                return Response()

            def close(self):
                pass

        with (
            mock.patch("curl_cffi.requests.Session", return_value=Session()),
            mock.patch("services.proxy_service.proxy_settings.build_session_kwargs", return_value={}),
        ):
            with self.assertRaises(TokenRefreshError) as raised:
                service._request_access_token_refresh("refresh-token")
        self.assertEqual(raised.exception.code, "invalid_response")
        self.assertNotIn("refresh-token-canary", str(raised.exception))

    def test_apply_refreshed_tokens_rejects_container_token_fields(self) -> None:
        service, storage = make_service()
        result = service._apply_refreshed_tokens(
            "access-token",
            {
                "access_token": {"secret": "apply-token-canary"},
                "refresh_token": ["apply-token-canary"],
                "id_token": {"secret": "apply-token-canary"},
            },
            "test",
        )
        self.assertEqual(result, "access-token")
        self.assertIsNotNone(service.get_account("access-token"))
        self.assertNotIn("apply-token-canary", json.dumps(storage.accounts, default=str))

    def test_password_relogin_rejects_container_oauth_tokens(self) -> None:
        service, _ = make_service()
        canary = "password-oauth-token-canary"
        sessions: list[object] = []

        class Response:
            def __init__(self, status_code: int, payload: object, url: str = "") -> None:
                self.status_code = status_code
                self._payload = payload
                self.text = json.dumps(payload) if payload else ""
                self.url = url
                self.closed = False

            def json(self):
                return self._payload

            def close(self) -> None:
                self.closed = True

        class Session:
            def __init__(self, *args, **kwargs) -> None:
                self.cookies = mock.Mock()
                self.posts = 0
                self.responses = []
                self.stream_calls = []
                self.post_verify_values = []
                sessions.append(self)

            def get(self, url: str, *args, **kwargs):
                self.stream_calls.append(kwargs.get("stream"))
                if "authorize" in url:
                    response = Response(200, {}, "https://platform.openai.com/auth/callback?code=oauth-code")
                else:
                    response = Response(500, {})
                self.responses.append(response)
                return response

            def post(self, url: str, *args, **kwargs):
                self.posts += 1
                self.stream_calls.append(kwargs.get("stream"))
                self.post_verify_values.append(kwargs.get("verify"))
                if self.posts == 1:
                    response = Response(200, {"continue_url": "https://platform.openai.com/auth/callback?code=oauth-code"})
                else:
                    response = Response(
                    200,
                    {
                        "access_token": {"secret": canary},
                        "refresh_token": [canary],
                        "id_token": {"secret": canary},
                    },
                )
                self.responses.append(response)
                return response

            def close(self) -> None:
                pass

        with (
            mock.patch("curl_cffi.requests.Session", side_effect=Session),
            mock.patch("utils.sentinel.build_sentinel_token", return_value=("", "")),
            mock.patch("services.proxy_service.proxy_settings.build_session_kwargs", return_value={}),
        ):
            result = service._login_with_password("user@example.test", "password")
        session = sessions[0]
        self.assertFalse(result["ok"])
        self.assertEqual(result["error"], "token_exchange_failed")
        self.assertNotIn(canary, repr(result))
        self.assertEqual(session.stream_calls, [True, True, True])
        self.assertEqual(session.post_verify_values, [None, True])
        self.assertTrue(all(response.closed for response in session.responses))

    def test_refresh_failure_does_not_persist_log_or_return_secret(self) -> None:
        service, storage = make_service()
        with (
            mock.patch.object(service, "_request_access_token_refresh", side_effect=RuntimeError(SECRET)),
            mock.patch.object(account_module.log_service, "add") as add,
        ):
            service.refresh_access_token("access-token", force=True)
            result = service.keepalive_refresh_tokens(["access-token"])

        persisted = json.dumps(storage.accounts, ensure_ascii=False, default=str)
        logged = json.dumps(add.call_args_list, ensure_ascii=False, default=str)
        returned = json.dumps(result, ensure_ascii=False, default=str)
        for payload in (persisted, logged, returned):
            self.assertNotIn(SECRET, payload)

    def test_account_watcher_does_not_print_keepalive_error_details(self) -> None:
        stop_event = Event()

        class FakeAccountService:
            @staticmethod
            def list_limited_tokens() -> list[str]:
                return []

            @staticmethod
            def list_normal_tokens() -> list[str]:
                return []

            @staticmethod
            def list_expiring_access_tokens() -> list[str]:
                return []

            @staticmethod
            def list_refresh_token_keepalive_tokens() -> list[str]:
                return ["access-token"]

            @staticmethod
            def keepalive_refresh_tokens(_tokens: list[str]) -> dict[str, object]:
                stop_event.set()
                return {"errors": [{"error": SECRET}]}

        output = io.StringIO()
        with (
            mock.patch.object(support_module, "account_service", FakeAccountService()),
            mock.patch.object(support_module, "config", SimpleNamespace(refresh_account_interval_minute=0)),
            redirect_stdout(output),
        ):
            thread = support_module.start_limited_account_watcher(stop_event)
            thread.join(timeout=2)

        self.assertFalse(thread.is_alive())
        self.assertNotIn(SECRET, output.getvalue())


if __name__ == "__main__":
    unittest.main()
