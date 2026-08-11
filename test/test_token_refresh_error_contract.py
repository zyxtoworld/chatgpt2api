from __future__ import annotations

import copy
import io
import json
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

        def request_refresh(refresh_token: str, _account: dict | None = None) -> dict[str, str]:
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

        def request_refresh(refresh_token: str, _account: dict | None = None) -> dict[str, str]:
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

            @staticmethod
            def json():
                return {"error": "invalid_grant", "error_description": "app_session_terminated"}

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
        self.assertEqual(raised.exception.code, "app_session_terminated")

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
