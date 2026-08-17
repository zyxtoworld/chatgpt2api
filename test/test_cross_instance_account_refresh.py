from __future__ import annotations

import tempfile
import threading
from pathlib import Path
from unittest import mock

import services.account_service as account_module
from services.account_service import AccountService, TokenRefreshError
from services.storage.base import StorageConflictError
from services.storage.json_storage import JSONStorageBackend


def _service(path: Path) -> AccountService:
    return AccountService(JSONStorageBackend(path))


def test_late_refresh_from_second_instance_cannot_overwrite_first_commit() -> None:
    outcomes = ("old_success", "token_error", "unexpected_error")
    for outcome in outcomes:
        with tempfile.TemporaryDirectory() as tmp_dir, mock.patch.object(
            account_module, "_account_log", return_value=None
        ):
            path = Path(tmp_dir) / "accounts.json"
            seed = _service(path)
            seed.add_account_items([{
                "access_token": "old-token",
                "refresh_token": "old-refresh",
                "id_token": "old-id",
                "status": "正常",
                "quota": 7,
            }])
            service_a = _service(path)
            service_b = _service(path)
            a_started = threading.Event()
            release_a = threading.Event()
            a_errors: list[BaseException] = []
            b_result: list[str] = []

            def delayed_a(*_args, **_kwargs):
                a_started.set()
                assert release_a.wait(5), "A refresh was not released"
                if outcome == "old_success":
                    return {
                        "access_token": "old-token",
                        "refresh_token": "old-refresh-late",
                        "id_token": "old-id-late",
                    }
                if outcome == "token_error":
                    raise TokenRefreshError("app_session_terminated")
                raise RuntimeError("late refresh failure")

            def fast_b(*_args, **_kwargs):
                return {
                    "access_token": "new-token",
                    "refresh_token": "new-refresh",
                    "id_token": "new-id",
                }

            service_a._request_access_token_refresh = delayed_a
            service_b._request_access_token_refresh = fast_b

            def run_a() -> None:
                try:
                    service_a.refresh_access_token("old-token", force=True, event="instance-a")
                except BaseException as exc:
                    a_errors.append(exc)

            def run_b() -> None:
                b_result.append(
                    service_b.refresh_access_token("old-token", force=True, event="instance-b")
                )

            thread_a = threading.Thread(target=run_a)
            thread_a.start()
            assert a_started.wait(5), "A did not enter the refresh request"
            thread_b = threading.Thread(target=run_b)
            thread_b.start()
            thread_b.join(5)
            assert not thread_b.is_alive()
            assert b_result == ["new-token"]

            release_a.set()
            thread_a.join(5)
            assert not thread_a.is_alive()
            assert a_errors
            assert isinstance(a_errors[0], StorageConflictError)

            reloaded = _service(path)
            current = reloaded.get_account("new-token")
            assert current is not None
            assert current["refresh_token"] == "new-refresh"
            assert current["id_token"] == "new-id"
            assert current["status"] == "正常"
            assert current["quota"] == 7
            assert reloaded.get_account("old-token") is None
