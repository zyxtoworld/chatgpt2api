from __future__ import annotations

import asyncio
import gc
import subprocess
import sys
import threading
import time
import unittest
import weakref
from queue import Empty, Queue
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import anyio
import httpx
from fastapi.concurrency import run_in_threadpool

from api.app import create_app
import api.accounts as accounts_api_module
import api.ai as ai_module
import api.image_inputs as image_inputs_module
import api.image_tasks as image_tasks_module
import api.support as support_module
import api.system as system_module
import services.account_service as account_module
import services.content_filter as content_filter_module
import services.log_service as log_service_module
import services.opened_file_response as opened_file_response_module
from services.log_service import LoggedCall


async def _release_and_drain_health_probes(release: threading.Event) -> None:
    """Release blocked probes and drain their loop-owned futures before loop exit."""
    release.set()
    drained = await system_module.wait_for_health_probe_tasks(timeout=1.0)
    if not drained:
        raise AssertionError("health probe workers were not drained after release")


class _BlockingAfterFirst:
    def __init__(self, entered: list[int], entered_lock: threading.Lock, all_entered: threading.Event, release: threading.Event) -> None:
        self._index = 0
        self._entered = entered
        self._entered_lock = entered_lock
        self._all_entered = all_entered
        self._release = release

    def __iter__(self):
        return self

    def __next__(self):
        if self._index == 0:
            self._index += 1
            return {"type": "response.output_text.delta", "delta": "ready"}
        if self._index == 1:
            self._index += 1
            with self._entered_lock:
                self._entered[0] += 1
                if self._entered[0] == 2:
                    self._all_entered.set()
            self._release.wait(2)
        raise StopIteration


class AIThreadPoolIsolationContractTests(unittest.TestCase):

    def test_health_probe_blocker_cannot_hold_interpreter_shutdown(self) -> None:
        child = r'''
import asyncio
import threading
from types import SimpleNamespace

import api.system as system_module

entered = threading.Event()

def blocked_stats():
    entered.set()
    threading.Event().wait()

async def main():
    result = await system_module._account_stats_async(
        SimpleNamespace(get_stats=blocked_stats)
    )
    assert result is None
    assert entered.is_set(), "health probe worker did not start"
    await system_module.wait_for_health_probe_tasks(timeout=0.02)
    print("HEALTH_PROBE_READY", flush=True)

asyncio.run(main())
'''
        process = subprocess.Popen(
            [sys.executable, "-c", child],
            cwd=str(Path(__file__).resolve().parent.parent),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        output = Queue()

        def read_stdout() -> None:
            assert process.stdout is not None
            for line in process.stdout:
                output.put(line)

        reader = threading.Thread(target=read_stdout, daemon=True)
        reader.start()
        try:
            ready = False
            startup_deadline = time.monotonic() + 10.0
            while time.monotonic() < startup_deadline:
                try:
                    if output.get(timeout=0.05).strip() == "HEALTH_PROBE_READY":
                        ready = True
                        break
                except Empty:
                    if process.poll() is not None:
                        break
            self.assertTrue(ready, "health probe child did not reach the shutdown checkpoint")
            process.wait(timeout=2.0)
        except subprocess.TimeoutExpired:
            process.kill()
            _stdout, stderr = process.communicate()
            self.fail(
                "blocked health probe held interpreter shutdown: "
                f"stderr={stderr[-2000:]!r}"
            )
        finally:
            if process.poll() is None:
                process.kill()
            _stdout, stderr = process.communicate()
        self.assertEqual(process.returncode, 0, stderr[-2000:])

    def test_blocked_remote_image_downloads_do_not_exhaust_default_threadpool(self) -> None:
        async def scenario() -> None:
            default_limiter = anyio.to_thread.current_default_thread_limiter()
            original_tokens = default_limiter.total_tokens
            default_limiter.total_tokens = 2
            entered = [0]
            entered_lock = threading.Lock()
            all_entered = threading.Event()
            release = threading.Event()

            def download(url: str):
                with entered_lock:
                    entered[0] += 1
                    if entered[0] == 2:
                        all_entered.set()
                release.wait(2)
                return (b"image", f"{url.rsplit('/', 1)[-1]}.png", "image/png")

            async def read(index: int) -> None:
                result = await image_inputs_module.read_image_sources(
                    [f"https://images.example.test/{index}"]
                )
                self.assertEqual(result[0][0], b"image")

            quick_result = None
            timed_out = False
            try:
                with mock.patch.object(
                    image_inputs_module,
                    "_download_image_url",
                    side_effect=download,
                ):
                    async with anyio.create_task_group() as task_group:
                        task_group.start_soon(read, 1)
                        task_group.start_soon(read, 2)
                        with anyio.fail_after(2):
                            while not all_entered.is_set():
                                await anyio.sleep(0.01)
                        try:
                            with anyio.fail_after(0.25):
                                quick_result = await run_in_threadpool(
                                    lambda: "default-pool-ready"
                                )
                        except TimeoutError:
                            timed_out = True
                        finally:
                            release.set()

                self.assertFalse(
                    timed_out,
                    "remote image downloads exhausted AnyIO's shared default threadpool",
                )
                self.assertEqual(quick_result, "default-pool-ready")
            finally:
                release.set()
                default_limiter.total_tokens = original_tokens

        anyio.run(scenario)

    def test_user_key_storage_does_not_run_on_the_asgi_event_loop(self) -> None:
        def create_key(*, role: str, name: str):
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                pass
            else:
                raise AssertionError("auth key storage ran on the ASGI event loop")
            return ({"id": "key-1", "role": role, "name": name}, "raw-key")

        async def scenario() -> None:
            app = create_app()
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
                response = await client.post(
                    "/api/auth/users",
                    headers={"Authorization": "Bearer chatgpt2api"},
                    json={"name": "worker-key"},
                )
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(response.json()["key"], "raw-key")

        with (
            mock.patch.object(accounts_api_module.auth_service, "create_key", side_effect=create_key),
            mock.patch.object(accounts_api_module.auth_service, "list_keys", return_value=[]),
        ):
            anyio.run(scenario)

    def test_delete_accounts_storage_does_not_run_on_the_asgi_event_loop(self) -> None:
        def delete_accounts(_tokens: list[str]) -> dict[str, object]:
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                pass
            else:
                raise AssertionError("account deletion storage ran on the ASGI event loop")
            return {"removed": 1, "items": []}

        async def scenario() -> None:
            app = create_app()
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
                response = await client.request(
                    "DELETE",
                    "/api/accounts",
                    headers={"Authorization": "Bearer chatgpt2api"},
                    json={"tokens": ["account-token"]},
                )
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(response.json()["removed"], 1)

        with mock.patch.object(
            accounts_api_module.account_service,
            "delete_accounts",
            side_effect=delete_accounts,
        ):
            anyio.run(scenario)

    def test_create_accounts_storage_and_refresh_do_not_run_on_the_asgi_event_loop(self) -> None:
        def assert_worker_thread() -> None:
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                return
            raise AssertionError("account storage or refresh ran on the ASGI event loop")

        def add_accounts(_tokens: list[str]) -> dict[str, object]:
            assert_worker_thread()
            return {"added": 1, "skipped": 0, "items": []}

        def refresh_accounts(_tokens: list[str]) -> dict[str, object]:
            assert_worker_thread()
            return {"refreshed": 1, "errors": [], "items": []}

        async def scenario() -> None:
            app = create_app()
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
                response = await client.post(
                    "/api/accounts",
                    headers={"Authorization": "Bearer chatgpt2api"},
                    json={"tokens": ["account-token"]},
                )
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(response.json()["refreshed"], 1)

        with (
            mock.patch.object(accounts_api_module.account_service, "add_accounts", side_effect=add_accounts),
            mock.patch.object(accounts_api_module.account_service, "refresh_accounts", side_effect=refresh_accounts),
        ):
            anyio.run(scenario)

    def test_account_export_build_and_compression_do_not_run_on_the_asgi_event_loop(self) -> None:
        def assert_worker_thread() -> None:
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                return
            raise AssertionError("account export ran on the ASGI event loop")

        def build_export_items(_tokens: list[str]) -> list[dict[str, str]]:
            assert_worker_thread()
            return [{
                "type": "codex",
                "access_token": "access-token",
                "refresh_token": "refresh-token",
                "id_token": "id-token",
            }]

        def account_zip_bytes(_items: list[dict[str, str]]) -> bytes:
            assert_worker_thread()
            return b"zip-payload"

        async def scenario() -> None:
            app = create_app()
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
                response = await client.post(
                    "/api/accounts/export",
                    headers={"Authorization": "Bearer chatgpt2api"},
                    json={"access_tokens": ["access-token"], "format": "zip"},
                )
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(response.content, b"zip-payload")

        with (
            mock.patch.object(
                accounts_api_module.account_service,
                "build_export_items",
                side_effect=build_export_items,
            ),
            mock.patch.object(
                accounts_api_module,
                "_account_zip_bytes",
                side_effect=account_zip_bytes,
            ),
        ):
            anyio.run(scenario)

    def test_blocked_account_management_does_not_exhaust_default_threadpool(self) -> None:
        async def scenario() -> None:
            default_limiter = anyio.to_thread.current_default_thread_limiter()
            original_tokens = default_limiter.total_tokens
            default_limiter.total_tokens = 2
            entered = [0]
            entered_lock = threading.Lock()
            all_entered = threading.Event()
            release = threading.Event()

            def add_accounts(_tokens: list[str]) -> dict[str, object]:
                with entered_lock:
                    entered[0] += 1
                    if entered[0] == 2:
                        all_entered.set()
                release.wait(2)
                return {"added": 1, "skipped": 0, "items": []}

            def refresh_accounts(_tokens: list[str]) -> dict[str, object]:
                return {"refreshed": 1, "errors": [], "items": []}

            app = create_app()
            transport = httpx.ASGITransport(app=app)
            quick_result = None
            timed_out = False
            try:
                with (
                    mock.patch.object(accounts_api_module.account_service, "add_accounts", side_effect=add_accounts),
                    mock.patch.object(accounts_api_module.account_service, "refresh_accounts", side_effect=refresh_accounts),
                ):
                    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
                        async def create_account(index: int) -> None:
                            response = await client.post(
                                "/api/accounts",
                                headers={"Authorization": "Bearer chatgpt2api"},
                                json={"tokens": [f"account-token-{index}"]},
                            )
                            self.assertEqual(response.status_code, 200, response.text)

                        async with anyio.create_task_group() as task_group:
                            task_group.start_soon(create_account, 1)
                            task_group.start_soon(create_account, 2)
                            with anyio.fail_after(2):
                                while not all_entered.is_set():
                                    await anyio.sleep(0.01)
                            try:
                                with anyio.fail_after(0.25):
                                    quick_result = await run_in_threadpool(lambda: "default-pool-ready")
                            except TimeoutError:
                                timed_out = True
                            finally:
                                release.set()

                self.assertFalse(timed_out, "account management exhausted AnyIO's shared default threadpool")
                self.assertEqual(quick_result, "default-pool-ready")
            finally:
                release.set()
                default_limiter.total_tokens = original_tokens

        anyio.run(scenario)

    def test_refresh_all_accounts_loads_tokens_off_the_asgi_event_loop(self) -> None:
        def list_tokens() -> list[str]:
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                return ["account-token"]
            raise AssertionError("account token storage ran on the ASGI event loop")

        async def scenario() -> None:
            app = create_app()
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
                response = await client.post(
                    "/api/accounts/refresh",
                    headers={"Authorization": "Bearer chatgpt2api"},
                    json={"access_tokens": []},
                )
            self.assertEqual(response.status_code, 200, response.text)
            self.assertIn("progress_id", response.json())

        with (
            mock.patch.object(accounts_api_module.account_service, "list_tokens", side_effect=list_tokens),
            mock.patch.object(accounts_api_module, "_schedule_management_task"),
        ):
            anyio.run(scenario)

    def test_account_read_routes_load_state_off_the_asgi_event_loop(self) -> None:
        def assert_worker_thread(*_args: object, **_kwargs: object) -> None:
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                return
            raise AssertionError("account state storage ran on the ASGI event loop")

        async def scenario() -> None:
            app = create_app()
            transport = httpx.ASGITransport(app=app)
            progress = {"total": 0, "completed": 0, "added": 0, "skipped": 0, "refreshed": 0, "failed": 0}
            async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
                responses = []
                with (
                    mock.patch.object(
                        accounts_api_module.account_service,
                        "list_accounts",
                        side_effect=lambda: (assert_worker_thread(), [])[1],
                    ),
                    mock.patch.object(
                        accounts_api_module.account_service,
                        "get_refresh_progress",
                        side_effect=lambda _progress_id: (assert_worker_thread(), progress)[1],
                    ),
                    mock.patch.object(
                        accounts_api_module.account_service,
                        "get_relogin_progress",
                        side_effect=lambda _progress_id: (assert_worker_thread(), progress)[1],
                    ),
                ):
                    responses.append(await client.get("/api/accounts", headers={"Authorization": "Bearer chatgpt2api"}))
                    responses.append(
                        await client.get(
                            "/api/accounts/refresh/progress/progress-1",
                            headers={"Authorization": "Bearer chatgpt2api"},
                        )
                    )
                    responses.append(
                        await client.get(
                            "/api/accounts/re-login/progress/progress-2",
                            headers={"Authorization": "Bearer chatgpt2api"},
                        )
                    )
            self.assertTrue(all(response.status_code == 200 for response in responses))

        anyio.run(scenario)

    def test_storage_backend_initialization_stays_off_the_asgi_event_loop(self) -> None:
        def get_storage_backend():
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                return SimpleNamespace(
                    health_check=lambda: {"status": "healthy", "backend": "json"},
                    get_backend_info=lambda: {"type": "json"},
                )
            raise AssertionError("storage backend initialization ran on the ASGI event loop")

        async def scenario() -> None:
            app = create_app()
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
                with (
                    mock.patch.object(system_module.config, "get_storage_backend", side_effect=get_storage_backend),
                    mock.patch.object(
                        system_module,
                        "_account_stats_async",
                        new=mock.AsyncMock(return_value={"active": 1}),
                    ),
                ):
                    health = await client.get("/health?format=json")
                    info = await client.get(
                        "/api/storage/info",
                        headers={"Authorization": "Bearer chatgpt2api"},
                    )
            self.assertEqual(health.status_code, 200, health.text)
            self.assertEqual(info.status_code, 200, info.text)

        anyio.run(scenario)

    def test_backup_state_and_settings_reads_stay_off_the_asgi_event_loop(self) -> None:
        def assert_worker_thread(*_args: object, **_kwargs: object) -> dict[str, object]:
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                return {}
            raise AssertionError("backup state/settings storage ran on the ASGI event loop")

        async def scenario() -> None:
            app = create_app()
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
                with (
                    mock.patch.object(
                        system_module,
                        "require_admin_async",
                        new=mock.AsyncMock(return_value={"id": "admin", "role": "admin"}),
                    ),
                    mock.patch.object(system_module.backup_service, "list_backups", return_value=[]),
                    mock.patch.object(
                        system_module.backup_service,
                        "get_status",
                        side_effect=assert_worker_thread,
                    ),
                    mock.patch.object(
                        system_module.backup_service,
                        "get_settings",
                        side_effect=assert_worker_thread,
                    ),
                ):
                    response = await client.get(
                        "/api/backups",
                        headers={"Authorization": "Bearer admin-key"},
                    )
            self.assertEqual(response.status_code, 200, response.text)

        anyio.run(scenario)

    def test_progress_failure_finalization_stays_off_the_asgi_event_loop(self) -> None:
        def assert_worker_thread(*_args: object, **_kwargs: object) -> None:
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                return
            raise AssertionError("progress persistence ran on the ASGI event loop")

        async def scenario() -> None:
            app = create_app()
            transport = httpx.ASGITransport(app=app)
            factories: list[object] = []

            def capture(factory) -> None:
                factories.append(factory)

            async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
                with (
                    mock.patch.object(accounts_api_module, "_schedule_management_task", side_effect=capture),
                    mock.patch.object(
                        accounts_api_module.account_service,
                        "refresh_accounts",
                        side_effect=RuntimeError("refresh failed"),
                    ),
                    mock.patch.object(
                        accounts_api_module.account_service,
                        "finish_refresh_progress",
                        side_effect=assert_worker_thread,
                    ),
                ):
                    response = await client.post(
                        "/api/accounts/refresh",
                        headers={"Authorization": "Bearer chatgpt2api"},
                        json={"access_tokens": ["account-token"]},
                    )
                    self.assertEqual(response.status_code, 200, response.text)
                    await factories[-1]()

                factories.clear()
                with (
                    mock.patch.object(accounts_api_module, "_schedule_management_task", side_effect=capture),
                    mock.patch.object(
                        accounts_api_module.account_service,
                        "re_login_accounts",
                        side_effect=RuntimeError("re-login failed"),
                    ),
                    mock.patch.object(
                        accounts_api_module.account_service,
                        "finish_relogin_progress",
                        side_effect=assert_worker_thread,
                    ),
                ):
                    response = await client.post(
                        "/api/accounts/re-login",
                        headers={"Authorization": "Bearer chatgpt2api"},
                        json={"access_tokens": ["account-token"]},
                    )
                    self.assertEqual(response.status_code, 200, response.text)
                    await factories[-1]()

        anyio.run(scenario)

    def test_cancelled_refresh_finalizes_progress_before_propagating(self) -> None:
        async def scenario() -> None:
            app = create_app()
            transport = httpx.ASGITransport(app=app)
            factories: list[object] = []
            finalized: list[dict[str, object]] = []

            def capture(factory) -> None:
                factories.append(factory)

            def cancel_refresh(*_args: object, **_kwargs: object) -> None:
                raise asyncio.CancelledError()

            def finish_progress(*_args: object, **kwargs: object) -> None:
                finalized.append(kwargs)

            async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
                with (
                    mock.patch.object(accounts_api_module, "_schedule_management_task", side_effect=capture),
                    mock.patch.object(
                        accounts_api_module.account_service,
                        "refresh_accounts",
                        side_effect=cancel_refresh,
                    ),
                    mock.patch.object(
                        accounts_api_module.account_service,
                        "finish_refresh_progress",
                        side_effect=finish_progress,
                    ),
                ):
                    response = await client.post(
                        "/api/accounts/refresh",
                        headers={"Authorization": "Bearer chatgpt2api"},
                        json={"access_tokens": ["account-token"]},
                    )
                    self.assertEqual(response.status_code, 200, response.text)
                    with self.assertRaises(asyncio.CancelledError):
                        await factories[-1]()

                factories.clear()
                with (
                    mock.patch.object(accounts_api_module, "_schedule_management_task", side_effect=capture),
                    mock.patch.object(
                        accounts_api_module.account_service,
                        "re_login_accounts",
                        side_effect=cancel_refresh,
                    ),
                    mock.patch.object(
                        accounts_api_module.account_service,
                        "finish_relogin_progress",
                        side_effect=finish_progress,
                    ),
                ):
                    response = await client.post(
                        "/api/accounts/re-login",
                        headers={"Authorization": "Bearer chatgpt2api"},
                        json={"access_tokens": ["account-token"]},
                    )
                    self.assertEqual(response.status_code, 200, response.text)
                    with self.assertRaises(asyncio.CancelledError):
                        await factories[-1]()

            self.assertEqual(
                finalized,
                [{"error": "刷新任务已取消"}, {"error": "重新登录任务已取消"}],
            )

        anyio.run(scenario)

    def test_non_stream_call_log_io_does_not_run_on_the_asgi_event_loop(self) -> None:
        def add_log(*_args, **_kwargs) -> None:
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                return
            raise AssertionError("call log I/O ran on the ASGI event loop")

        async def scenario() -> None:
            call = LoggedCall(
                {"id": "user-1", "name": "user", "role": "user"},
                "/v1/responses",
                "gpt-5.4",
                "Responses",
            )
            result = await call.run(lambda: {"id": "resp-1", "object": "response"})
            self.assertEqual(result["id"], "resp-1")

        with mock.patch.object(log_service_module.log_service, "add", side_effect=add_log):
            anyio.run(scenario)

    def test_public_health_account_stats_do_not_run_on_the_asgi_event_loop(self) -> None:
        def get_stats() -> dict[str, object]:
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                pass
            else:
                raise AssertionError("account stats ran on the ASGI event loop")
            return {
                "total": 1,
                "cumulative_total": 1,
                "active": 1,
                "limited": 0,
                "abnormal": 0,
                "disabled": 0,
                "total_quota": 0,
                "total_success": 0,
                "total_fail": 0,
                "by_type": {"free": 1},
            }

        class HealthyStorage:
            def health_check(self) -> dict[str, object]:
                return {"status": "healthy"}

            def get_backend_info(self) -> dict[str, object]:
                return {"type": "json"}

        async def scenario() -> None:
            app = create_app()
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
                response = await client.get("/health?format=json")
            self.assertEqual(response.status_code, 200)
            self.assertTrue(response.json()["healthy"])

        with (
            mock.patch.object(account_module.account_service, "get_stats", side_effect=get_stats),
            mock.patch.object(system_module.config, "get_storage_backend", return_value=HealthyStorage()),
            mock.patch.object(
                system_module.proxy_settings,
                "get_runtime_status",
                return_value={"enabled": False, "clearance_enabled": False},
            ),
        ):
            anyio.run(scenario)

    def test_public_health_returns_degraded_when_account_stats_is_blocked(self) -> None:
        entered = threading.Event()
        release = threading.Event()

        def blocked_stats() -> dict[str, object]:
            entered.set()
            release.wait(3)
            return {"active": 1}

        class HealthyStorage:
            def health_check(self) -> dict[str, object]:
                return {"status": "healthy"}

            def get_backend_info(self) -> dict[str, object]:
                return {"type": "json"}

        async def scenario() -> tuple[object, float]:
            app = create_app()
            transport = httpx.ASGITransport(app=app)
            request_started = time.monotonic()
            try:
                async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
                    response = await client.get("/health?format=json")
                self.assertEqual(response.status_code, 200, response.text)
                request_elapsed = time.monotonic() - request_started
                return response, request_elapsed
            finally:
                await _release_and_drain_health_probes(release)

        with (
            mock.patch.object(account_module.account_service, "get_stats", side_effect=blocked_stats),
            mock.patch.object(system_module.config, "get_storage_backend", return_value=HealthyStorage()),
            mock.patch.object(
                system_module.proxy_settings,
                "get_runtime_status",
                return_value={"enabled": False, "clearance_enabled": False},
            ),
        ):
            response, elapsed = anyio.run(scenario)
        self.assertLess(elapsed, 2.0, "blocked account stats kept the health route open")
        self.assertFalse(response.json()["healthy"])

    def test_public_health_bounds_each_blocking_subcheck(self) -> None:
        for stage in ("account", "storage", "proxy"):
            release = threading.Event()

            def blocked() -> dict[str, object]:
                release.wait(3)
                return {}

            class HealthyStorage:
                def health_check(self) -> dict[str, object]:
                    return blocked() if stage == "storage" else {"status": "healthy"}

                def get_backend_info(self) -> dict[str, object]:
                    return {"type": "json"}

            async def scenario() -> object:
                app = create_app()
                transport = httpx.ASGITransport(app=app)
                try:
                    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
                        started = time.monotonic()
                        response = await client.get("/health?format=json")
                        return response, time.monotonic() - started
                finally:
                    await _release_and_drain_health_probes(release)

            with (
                mock.patch.object(
                    account_module.account_service,
                    "get_stats",
                    side_effect=blocked if stage == "account" else lambda: {"active": 1},
                ),
                mock.patch.object(system_module.config, "get_storage_backend", return_value=HealthyStorage()),
                mock.patch.object(
                    system_module.proxy_settings,
                    "get_runtime_status",
                    side_effect=blocked if stage == "proxy" else lambda: {
                        "enabled": False,
                        "clearance_enabled": False,
                    },
                ),
            ):
                response, elapsed = anyio.run(scenario)
            self.assertLess(elapsed, 2.0, f"{stage} health subcheck exceeded its bound")
            self.assertEqual(response.status_code, 200, response.text)
            if stage != "proxy":
                self.assertFalse(response.json()["healthy"])

    def test_public_health_has_one_overall_budget_for_all_blocking_subchecks(self) -> None:
        release = threading.Event()

        def blocked() -> dict[str, object]:
            release.wait(3)
            return {}

        class BlockingStorage:
            def health_check(self) -> dict[str, object]:
                return blocked()

            def get_backend_info(self) -> dict[str, object]:
                return blocked()

        async def scenario() -> tuple[object, float]:
            app = create_app()
            transport = httpx.ASGITransport(app=app)
            request_started = time.monotonic()
            try:
                async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
                    response = await client.get("/health?format=json")
                request_elapsed = time.monotonic() - request_started
                return response, request_elapsed
            finally:
                await _release_and_drain_health_probes(release)

        with (
            mock.patch.object(account_module.account_service, "get_stats", side_effect=blocked),
            mock.patch.object(system_module.config, "get_storage_backend", return_value=BlockingStorage()),
            mock.patch.object(system_module.proxy_settings, "get_runtime_status", side_effect=blocked),
        ):
            response, elapsed = anyio.run(scenario)
        self.assertLess(elapsed, 2.5, "health subchecks exceeded the production liveness budget")
        self.assertEqual(response.status_code, 200, response.text)
        self.assertFalse(response.json()["healthy"])

    def test_public_health_keeps_storage_and_proxy_results_during_repeated_concurrent_probes(self) -> None:
        class HealthyStorage:
            def __init__(self) -> None:
                self._factory_barrier = threading.Barrier(2)
                self._details_barrier = threading.Barrier(2)

            def health_check(self) -> dict[str, object]:
                self._details_barrier.wait(2)
                return {"status": "healthy"}

            def get_backend_info(self) -> dict[str, object]:
                self._details_barrier.wait(2)
                return {"type": "json"}

        storage = HealthyStorage()

        def get_storage_backend() -> HealthyStorage:
            storage._factory_barrier.wait(2)
            return storage

        def get_runtime_status() -> dict[str, object]:
            storage._factory_barrier.wait(2)
            return {"enabled": True, "clearance_enabled": True}

        async def scenario() -> None:
            app = create_app()
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
                for _ in range(5):
                    response = await client.get("/health?format=json")
                    self.assertEqual(response.status_code, 200, response.text)
                    payload = response.json()
                    self.assertTrue(payload["healthy"], payload)
                    self.assertEqual(payload["storage"], {"backend": "json", "health": {"status": "healthy"}})
                    self.assertEqual(
                        payload["proxy_runtime"],
                        {"enabled": True, "clearance_enabled": True},
                    )

        with (
            mock.patch.object(account_module.account_service, "get_stats", return_value={"active": 1}),
            mock.patch.object(system_module.config, "get_storage_backend", side_effect=get_storage_backend),
            mock.patch.object(system_module.proxy_settings, "get_runtime_status", side_effect=get_runtime_status),
        ):
            anyio.run(scenario)

    def test_health_probe_shutdown_is_bounded_and_loop_scoped(self) -> None:
        owner_refs: list[weakref.ReferenceType[object]] = []

        async def scenario() -> None:
            owner_refs.append(weakref.ref(system_module._health_probe_owner()))
            entered = threading.Event()
            release = threading.Event()

            def blocked_stats() -> dict[str, object]:
                entered.set()
                release.wait(3)
                return {"active": 1}

            probe_task = asyncio.create_task(
                system_module._account_stats_async(SimpleNamespace(get_stats=blocked_stats))
            )
            with anyio.fail_after(1):
                await anyio.to_thread.run_sync(entered.wait, 1)

            started = time.monotonic()
            drained = await system_module.wait_for_health_probe_tasks(timeout=0.05)
            elapsed = time.monotonic() - started
            self.assertFalse(drained)
            self.assertLess(elapsed, 0.5)

            release.set()
            self.assertEqual(await probe_task, {"active": 1})
            self.assertTrue(await system_module.wait_for_health_probe_tasks(timeout=0.5))

        anyio.run(scenario)
        anyio.run(scenario)
        gc.collect()
        self.assertTrue(all(reference() is None for reference in owner_refs))

    def test_expired_health_probe_deadline_does_not_leak_stage_guard(self) -> None:
        async def scenario() -> None:
            service = SimpleNamespace(get_stats=lambda: {"active": 1})
            self.assertIsNone(
                await system_module._account_stats_async(
                    service,
                    deadline=anyio.current_time() - 1,
                )
            )
            self.assertEqual(
                await system_module._account_stats_async(
                    service,
                    deadline=anyio.current_time() + 1,
                ),
                {"active": 1},
            )

        anyio.run(scenario)

    def test_public_image_reads_and_thumbnail_generation_do_not_run_on_the_asgi_event_loop(self) -> None:
        def image_response(_path: str):
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                pass
            else:
                raise AssertionError("image I/O ran on the ASGI event loop")
            return system_module.Response(content=b"image", media_type="image/png")

        async def scenario() -> None:
            app = create_app()
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
                image = await client.get("/images/2026/08/image.png")
                thumbnail = await client.get("/image-thumbnails/2026/08/image.png")
            self.assertEqual(image.status_code, 200)
            self.assertEqual(thumbnail.status_code, 200)

        with (
            mock.patch.object(system_module, "get_image_response", side_effect=image_response),
            mock.patch.object(system_module, "get_thumbnail_response", side_effect=image_response),
        ):
            anyio.run(scenario)

    def test_blocked_public_image_reads_do_not_exhaust_default_threadpool(self) -> None:
        async def scenario() -> None:
            default_limiter = anyio.to_thread.current_default_thread_limiter()
            original_tokens = default_limiter.total_tokens
            default_limiter.total_tokens = 2
            entered = [0]
            entered_lock = threading.Lock()
            all_entered = threading.Event()
            release = threading.Event()

            def image_response(_path: str):
                with entered_lock:
                    entered[0] += 1
                    if entered[0] == 2:
                        all_entered.set()
                release.wait(2)
                return system_module.Response(content=b"image", media_type="image/png")

            async def read(client: httpx.AsyncClient, index: int) -> None:
                response = await client.get(f"/images/2026/08/image-{index}.png")
                self.assertEqual(response.status_code, 200)

            quick_result = None
            timed_out = False
            try:
                with mock.patch.object(system_module, "get_image_response", side_effect=image_response):
                    app = create_app()
                    transport = httpx.ASGITransport(app=app)
                    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
                        async with anyio.create_task_group() as task_group:
                            task_group.start_soon(read, client, 1)
                            task_group.start_soon(read, client, 2)
                            with anyio.fail_after(2):
                                while not all_entered.is_set():
                                    await anyio.sleep(0.01)
                            try:
                                with anyio.fail_after(0.25):
                                    quick_result = await run_in_threadpool(
                                        lambda: "default-pool-ready"
                                    )
                            except TimeoutError:
                                timed_out = True
                            finally:
                                release.set()

                self.assertFalse(
                    timed_out,
                    "image reads exhausted AnyIO's shared default threadpool",
                )
                self.assertEqual(quick_result, "default-pool-ready")
            finally:
                release.set()
                default_limiter.total_tokens = original_tokens

        with (
            mock.patch.object(system_module, "_IMAGE_IO_THREAD_CAPACITY", 2),
            mock.patch.object(system_module, "_IMAGE_IO_THREAD_STATE", threading.local()),
        ):
            anyio.run(scenario)

    def test_blocked_backup_network_calls_do_not_exhaust_default_threadpool(self) -> None:
        async def scenario() -> None:
            default_limiter = anyio.to_thread.current_default_thread_limiter()
            original_tokens = default_limiter.total_tokens
            default_limiter.total_tokens = 2
            entered = [0]
            entered_lock = threading.Lock()
            all_entered = threading.Event()
            release = threading.Event()

            def test_connection() -> dict[str, object]:
                with entered_lock:
                    entered[0] += 1
                    if entered[0] == 2:
                        all_entered.set()
                release.wait(2)
                return {"ok": True}

            async def request_backup_test(client: httpx.AsyncClient) -> None:
                response = await client.post(
                    "/api/backup/test",
                    headers={"Authorization": "Bearer admin-key"},
                )
                self.assertEqual(response.status_code, 200, response.text)

            quick_result = None
            timed_out = False
            try:
                with (
                    mock.patch.object(
                        system_module,
                        "require_admin_async",
                        new=mock.AsyncMock(return_value={"id": "admin", "role": "admin"}),
                    ),
                    mock.patch.object(
                        system_module.backup_service,
                        "test_connection",
                        side_effect=test_connection,
                    ),
                ):
                    app = create_app()
                    transport = httpx.ASGITransport(app=app)
                    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
                        async with anyio.create_task_group() as task_group:
                            task_group.start_soon(request_backup_test, client)
                            task_group.start_soon(request_backup_test, client)
                            with anyio.fail_after(2):
                                while not all_entered.is_set():
                                    await anyio.sleep(0.01)
                            try:
                                with anyio.fail_after(0.25):
                                    quick_result = await run_in_threadpool(
                                        lambda: "default-pool-ready"
                                    )
                            except TimeoutError:
                                timed_out = True
                            finally:
                                release.set()

                self.assertFalse(
                    timed_out,
                    "backup network calls exhausted AnyIO's shared default threadpool",
                )
                self.assertEqual(quick_result, "default-pool-ready")
            finally:
                release.set()
                default_limiter.total_tokens = original_tokens

        anyio.run(scenario)

    def test_settings_storage_does_not_run_on_the_asgi_event_loop(self) -> None:
        def update_config(_payload: dict[str, object]) -> dict[str, object]:
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                return {}
            raise AssertionError("config storage ran on the ASGI event loop")

        async def scenario() -> None:
            app = create_app()
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
                response = await client.post(
                    "/api/settings",
                    headers={"Authorization": "Bearer admin-key"},
                    json={"image_retention_days": 30},
                )
            self.assertEqual(response.status_code, 200, response.text)

        with (
            mock.patch.object(
                system_module,
                "require_admin_async",
                new=mock.AsyncMock(return_value={"id": "admin", "role": "admin"}),
            ),
            mock.patch.object(system_module.config, "update", side_effect=update_config),
        ):
            anyio.run(scenario)

    def test_settings_read_does_not_run_on_the_asgi_event_loop(self) -> None:
        def get_config() -> dict[str, object]:
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                return {}
            raise AssertionError("config storage ran on the ASGI event loop")

        async def scenario() -> None:
            app = create_app()
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
                response = await client.get(
                    "/api/settings",
                    headers={"Authorization": "Bearer admin-key"},
                )
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(response.json(), {"config": {}})

        with (
            mock.patch.object(
                system_module,
                "require_admin_async",
                new=mock.AsyncMock(return_value={"id": "admin", "role": "admin"}),
            ),
            mock.patch.object(system_module.config, "get", side_effect=get_config),
        ):
            anyio.run(scenario)

    def test_blocked_image_task_polling_does_not_exhaust_default_threadpool(self) -> None:
        async def scenario() -> None:
            default_limiter = anyio.to_thread.current_default_thread_limiter()
            original_tokens = default_limiter.total_tokens
            default_limiter.total_tokens = 2
            entered = [0]
            entered_lock = threading.Lock()
            all_entered = threading.Event()
            release = threading.Event()

            def list_tasks(_identity, _task_ids) -> dict[str, object]:
                with entered_lock:
                    entered[0] += 1
                    if entered[0] == 2:
                        all_entered.set()
                release.wait(2)
                return {"items": [], "missing_ids": []}

            async def poll_tasks(client: httpx.AsyncClient) -> None:
                response = await client.get(
                    "/api/image-tasks",
                    headers={"Authorization": "Bearer user-key"},
                )
                self.assertEqual(response.status_code, 200, response.text)

            quick_result = None
            timed_out = False
            try:
                with (
                    mock.patch.object(
                        image_tasks_module,
                        "require_identity_async",
                        new=mock.AsyncMock(return_value={"id": "user-1", "role": "user"}),
                    ),
                    mock.patch.object(
                        image_tasks_module.image_task_service,
                        "list_tasks",
                        side_effect=list_tasks,
                    ),
                ):
                    app = create_app()
                    transport = httpx.ASGITransport(app=app)
                    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
                        async with anyio.create_task_group() as task_group:
                            task_group.start_soon(poll_tasks, client)
                            task_group.start_soon(poll_tasks, client)
                            with anyio.fail_after(2):
                                while not all_entered.is_set():
                                    await anyio.sleep(0.01)
                            try:
                                with anyio.fail_after(0.25):
                                    quick_result = await run_in_threadpool(
                                        lambda: "default-pool-ready"
                                    )
                            except TimeoutError:
                                timed_out = True
                            finally:
                                release.set()

                self.assertFalse(
                    timed_out,
                    "image task polling exhausted AnyIO's shared default threadpool",
                )
                self.assertEqual(quick_result, "default-pool-ready")
            finally:
                release.set()
                default_limiter.total_tokens = original_tokens

        anyio.run(scenario)

    def test_blocked_editable_task_polling_does_not_exhaust_default_threadpool(self) -> None:
        async def scenario() -> None:
            default_limiter = anyio.to_thread.current_default_thread_limiter()
            original_tokens = default_limiter.total_tokens
            default_limiter.total_tokens = 2
            entered = [0]
            entered_lock = threading.Lock()
            all_entered = threading.Event()
            release = threading.Event()

            def list_tasks(_identity, _task_ids) -> dict[str, object]:
                with entered_lock:
                    entered[0] += 1
                    if entered[0] == 2:
                        all_entered.set()
                release.wait(2)
                return {"items": [], "missing_ids": []}

            async def poll_tasks(client: httpx.AsyncClient) -> None:
                response = await client.get(
                    "/v1/editable-file-tasks",
                    headers={"Authorization": "Bearer user-key"},
                )
                self.assertEqual(response.status_code, 200, response.text)

            quick_result = None
            timed_out = False
            try:
                with (
                    mock.patch.object(
                        ai_module,
                        "require_identity_async",
                        new=mock.AsyncMock(return_value={"id": "user-1", "role": "user"}),
                    ),
                    mock.patch.object(
                        ai_module.editable_file_task_service,
                        "list_tasks",
                        side_effect=list_tasks,
                    ),
                ):
                    app = create_app()
                    transport = httpx.ASGITransport(app=app)
                    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
                        async with anyio.create_task_group() as task_group:
                            task_group.start_soon(poll_tasks, client)
                            task_group.start_soon(poll_tasks, client)
                            with anyio.fail_after(2):
                                while not all_entered.is_set():
                                    await anyio.sleep(0.01)
                            try:
                                with anyio.fail_after(0.25):
                                    quick_result = await run_in_threadpool(
                                        lambda: "default-pool-ready"
                                    )
                            except TimeoutError:
                                timed_out = True
                            finally:
                                release.set()

                self.assertFalse(
                    timed_out,
                    "editable task polling exhausted AnyIO's shared default threadpool",
                )
                self.assertEqual(quick_result, "default-pool-ready")
            finally:
                release.set()
                default_limiter.total_tokens = original_tokens

        anyio.run(scenario)

    def test_blocked_opened_file_reads_do_not_exhaust_default_threadpool(self) -> None:
        async def scenario() -> None:
            default_limiter = anyio.to_thread.current_default_thread_limiter()
            original_tokens = default_limiter.total_tokens
            default_limiter.total_tokens = 2
            entered = [0]
            entered_lock = threading.Lock()
            all_entered = threading.Event()
            release = threading.Event()

            class BlockingFile:
                def read(self, _size: int) -> bytes:
                    with entered_lock:
                        entered[0] += 1
                        if entered[0] == 2:
                            all_entered.set()
                    release.wait(2)
                    return b""

            async def read_file() -> None:
                response = opened_file_response_module.OpenedFileResponse.__new__(
                    opened_file_response_module.OpenedFileResponse
                )
                response._opened_file = BlockingFile()
                await response._read_opened_file(1)

            quick_result = None
            timed_out = False
            try:
                async with anyio.create_task_group() as task_group:
                    task_group.start_soon(read_file)
                    task_group.start_soon(read_file)
                    with anyio.fail_after(2):
                        while not all_entered.is_set():
                            await anyio.sleep(0.01)
                    try:
                        with anyio.fail_after(0.25):
                            quick_result = await run_in_threadpool(
                                lambda: "default-pool-ready"
                            )
                    except TimeoutError:
                        timed_out = True
                    finally:
                        release.set()

                self.assertFalse(
                    timed_out,
                    "opened file reads exhausted AnyIO's shared default threadpool",
                )
                self.assertEqual(quick_result, "default-pool-ready")
            finally:
                release.set()
                default_limiter.total_tokens = original_tokens

        anyio.run(scenario)

    def test_blocked_content_reviews_do_not_exhaust_default_threadpool(self) -> None:
        async def scenario() -> None:
            default_limiter = anyio.to_thread.current_default_thread_limiter()
            original_tokens = default_limiter.total_tokens
            default_limiter.total_tokens = 2
            entered = [0]
            entered_lock = threading.Lock()
            all_entered = threading.Event()
            release = threading.Event()

            class ReviewResponse:
                status_code = 200

                @staticmethod
                def json():
                    return {"choices": [{"message": {"content": "ALLOW"}}]}

            def review_request(*_args, **_kwargs):
                with entered_lock:
                    entered[0] += 1
                    if entered[0] == 2:
                        all_entered.set()
                release.wait(2)
                return ReviewResponse()

            async def filter_request(index: int) -> None:
                call = LoggedCall(
                    {"id": f"user-{index}", "name": "user", "role": "user"},
                    "/v1/responses",
                    "auto",
                    "并发审核隔离",
                )
                await ai_module.filter_or_log(call, "review me")

            quick_result = None
            timed_out = False
            try:
                with (
                    mock.patch.object(
                        content_filter_module,
                        "config",
                        SimpleNamespace(
                            sensitive_words=[],
                            ai_review={
                                "enabled": True,
                                "base_url": "https://review.example.test",
                                "api_key": "review-key",
                                "model": "review-model",
                            },
                        ),
                    ),
                    mock.patch.object(content_filter_module.requests, "post", side_effect=review_request),
                    mock.patch.object(
                        content_filter_module.proxy_settings,
                        "build_session_kwargs",
                        return_value={},
                    ),
                ):
                    async with anyio.create_task_group() as task_group:
                        task_group.start_soon(filter_request, 1)
                        task_group.start_soon(filter_request, 2)
                        with anyio.fail_after(2):
                            while not all_entered.is_set():
                                await anyio.sleep(0.01)
                        try:
                            with anyio.fail_after(0.25):
                                quick_result = await run_in_threadpool(lambda: "default-pool-ready")
                        except TimeoutError:
                            timed_out = True
                        finally:
                            release.set()

                self.assertFalse(timed_out, "blocked content reviews exhausted AnyIO's shared default threadpool")
                self.assertEqual(quick_result, "default-pool-ready")
            finally:
                release.set()
                default_limiter.total_tokens = original_tokens

        anyio.run(scenario)

    def test_async_api_authentication_does_not_run_storage_on_event_loop(self) -> None:
        def authenticate(_: str) -> dict[str, object]:
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                pass
            else:
                raise AssertionError("authentication storage I/O ran on the ASGI event loop")
            return {"id": "user-1", "name": "user", "role": "user"}

        async def scenario() -> None:
            app = create_app()
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
                response = await client.get(
                    "/v1/models",
                    headers={"Authorization": "Bearer user-key"},
                )
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json(), {"object": "list", "data": []})

        with (
            mock.patch.object(support_module, "config", SimpleNamespace(auth_key="")),
            mock.patch.object(support_module.auth_service, "authenticate", side_effect=authenticate),
            mock.patch.object(ai_module.openai_v1_models, "list_models", return_value={"object": "list", "data": []}),
        ):
            anyio.run(scenario)

    def test_blocked_authentication_does_not_exhaust_default_threadpool(self) -> None:
        async def scenario() -> None:
            default_limiter = anyio.to_thread.current_default_thread_limiter()
            original_tokens = default_limiter.total_tokens
            default_limiter.total_tokens = 2
            entered = [0]
            entered_lock = threading.Lock()
            all_entered = threading.Event()
            release = threading.Event()

            def authenticate(_: str) -> dict[str, object]:
                with entered_lock:
                    entered[0] += 1
                    if entered[0] == 2:
                        all_entered.set()
                release.wait(2)
                return {"id": "user-1", "name": "user", "role": "user"}

            async def authenticate_request() -> None:
                await support_module.require_identity_async("Bearer user-key")

            quick_result = None
            timed_out = False
            try:
                with (
                    mock.patch.object(support_module, "config", SimpleNamespace(auth_key="")),
                    mock.patch.object(support_module.auth_service, "authenticate", side_effect=authenticate),
                ):
                    async with anyio.create_task_group() as task_group:
                        task_group.start_soon(authenticate_request)
                        task_group.start_soon(authenticate_request)
                        with anyio.fail_after(2):
                            while not all_entered.is_set():
                                await anyio.sleep(0.01)
                        try:
                            with anyio.fail_after(0.25):
                                quick_result = await run_in_threadpool(lambda: "default-pool-ready")
                        except TimeoutError:
                            timed_out = True
                        finally:
                            release.set()

                self.assertFalse(timed_out, "blocked authentication exhausted AnyIO's shared default threadpool")
                self.assertEqual(quick_result, "default-pool-ready")
            finally:
                release.set()
                default_limiter.total_tokens = original_tokens

        anyio.run(scenario)

    def test_blocked_storage_health_does_not_exhaust_default_threadpool(self) -> None:
        async def scenario() -> None:
            default_limiter = anyio.to_thread.current_default_thread_limiter()
            original_tokens = default_limiter.total_tokens
            default_limiter.total_tokens = 1
            entered = threading.Event()
            release = threading.Event()

            class BlockingStorage:
                def health_check(self):
                    entered.set()
                    release.wait(2)
                    return {"status": "healthy"}

            async def run_health() -> None:
                await system_module._storage_health_async(BlockingStorage())

            quick_result = None
            timed_out = False
            try:
                async with anyio.create_task_group() as task_group:
                    task_group.start_soon(run_health)
                    with anyio.fail_after(2):
                        while not entered.is_set():
                            await anyio.sleep(0.01)
                    try:
                        with anyio.fail_after(0.25):
                            quick_result = await run_in_threadpool(lambda: "default-pool-ready")
                    except TimeoutError:
                        timed_out = True
                    finally:
                        release.set()

                self.assertFalse(timed_out, "storage health exhausted AnyIO's shared default threadpool")
                self.assertEqual(quick_result, "default-pool-ready")
            finally:
                release.set()
                default_limiter.total_tokens = original_tokens

        anyio.run(scenario)

    def test_blocked_model_catalog_requests_do_not_exhaust_default_threadpool(self) -> None:
        async def scenario() -> None:
            default_limiter = anyio.to_thread.current_default_thread_limiter()
            original_tokens = default_limiter.total_tokens
            default_limiter.total_tokens = 2
            entered = [0]
            entered_lock = threading.Lock()
            all_entered = threading.Event()
            release = threading.Event()

            def list_models() -> dict[str, object]:
                with entered_lock:
                    entered[0] += 1
                    if entered[0] == 2:
                        all_entered.set()
                release.wait(2)
                return {"object": "list", "data": []}

            async def request_models(client: httpx.AsyncClient) -> None:
                response = await client.get(
                    "/v1/models",
                    headers={"Authorization": "Bearer user-key"},
                )
                self.assertEqual(response.status_code, 200, response.text)

            app = create_app()
            transport = httpx.ASGITransport(app=app)
            quick_result = None
            timed_out = False
            try:
                with (
                    mock.patch.object(support_module, "config", SimpleNamespace(auth_key="")),
                    mock.patch.object(
                        support_module.auth_service,
                        "authenticate",
                        return_value={"id": "user-1", "name": "user", "role": "user"},
                    ),
                    mock.patch.object(ai_module.openai_v1_models, "list_models", side_effect=list_models),
                ):
                    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
                        async with anyio.create_task_group() as task_group:
                            task_group.start_soon(request_models, client)
                            task_group.start_soon(request_models, client)
                            with anyio.fail_after(2):
                                while not all_entered.is_set():
                                    await anyio.sleep(0.01)
                            try:
                                with anyio.fail_after(0.25):
                                    quick_result = await run_in_threadpool(
                                        lambda: "default-pool-ready"
                                    )
                            except TimeoutError:
                                timed_out = True
                            finally:
                                release.set()

                self.assertFalse(
                    timed_out,
                    "blocked model catalog requests exhausted AnyIO's default threadpool",
                )
                self.assertEqual(quick_result, "default-pool-ready")
            finally:
                release.set()
                default_limiter.total_tokens = original_tokens

        anyio.run(scenario)

    def test_blocked_ai_streams_do_not_exhaust_the_default_threadpool(self) -> None:
        async def scenario() -> None:
            default_limiter = anyio.to_thread.current_default_thread_limiter()
            original_tokens = default_limiter.total_tokens
            default_limiter.total_tokens = 2
            entered = [0]
            entered_lock = threading.Lock()
            all_entered = threading.Event()
            release = threading.Event()

            async def make_response(iterator: _BlockingAfterFirst):
                call = LoggedCall(
                    {"id": "user-1", "name": "user", "role": "user"},
                    "/v1/responses",
                    "auto",
                    "并发隔离",
                )
                return await call.run(lambda: iterator, sse="responses")

            async def consume(response) -> None:
                async for _chunk in response.body_iterator:
                    pass

            try:
                streams = [
                    _BlockingAfterFirst(entered, entered_lock, all_entered, release)
                    for _ in range(2)
                ]
                responses = [await make_response(stream) for stream in streams]
                quick_result = None
                timed_out = False
                async with anyio.create_task_group() as task_group:
                    for response in responses:
                        task_group.start_soon(consume, response)
                    with anyio.fail_after(2):
                        while not all_entered.is_set():
                            await anyio.sleep(0.01)
                    try:
                        with anyio.fail_after(0.25):
                            quick_result = await run_in_threadpool(lambda: "default-pool-ready")
                    except TimeoutError:
                        timed_out = True
                    finally:
                        release.set()

                self.assertFalse(timed_out, "blocked AI streams exhausted AnyIO's shared default threadpool")
                self.assertEqual(quick_result, "default-pool-ready")
            finally:
                release.set()
                default_limiter.total_tokens = original_tokens

        anyio.run(scenario)

    def test_blocked_http_ai_streams_do_not_starve_non_stream_calls(self) -> None:
        async def scenario() -> None:
            entered = [0]
            entered_lock = threading.Lock()
            all_entered = threading.Event()
            release = threading.Event()

            async def make_response(iterator: _BlockingAfterFirst):
                call = LoggedCall(
                    {"id": "user-1", "name": "user", "role": "user"},
                    "/v1/responses",
                    "auto",
                    "流式并发隔离",
                )
                return await call.run(lambda: iterator, sse="responses")

            async def consume(response) -> None:
                async for _chunk in response.body_iterator:
                    pass

            quick_result = None
            timed_out = False
            try:
                streams = [
                    _BlockingAfterFirst(entered, entered_lock, all_entered, release)
                    for _ in range(2)
                ]
                responses = [await make_response(stream) for stream in streams]
                async with anyio.create_task_group() as task_group:
                    for response in responses:
                        task_group.start_soon(consume, response)
                    with anyio.fail_after(2):
                        while not all_entered.is_set():
                            await anyio.sleep(0.01)
                    try:
                        with anyio.fail_after(0.25):
                            quick_call = LoggedCall(
                                {"id": "user-2", "name": "user", "role": "user"},
                                "/v1/responses",
                                "auto",
                                "非流式并发隔离",
                            )
                            quick_result = await quick_call.run(
                                lambda: {"id": "response-ready"}
                            )
                    except TimeoutError:
                        timed_out = True
                    finally:
                        release.set()

                self.assertFalse(
                    timed_out,
                    "blocked HTTP AI streams exhausted the non-stream AI worker capacity",
                )
                self.assertEqual(quick_result["id"], "response-ready")
            finally:
                release.set()

        with (
            mock.patch.object(log_service_module, "_AI_THREAD_CAPACITY", 2),
            mock.patch.object(log_service_module, "_AI_THREAD_STATE", threading.local()),
            mock.patch.object(log_service_module, "_AI_STREAM_THREAD_CAPACITY", 2, create=True),
            mock.patch.object(log_service_module, "_AI_STREAM_THREAD_STATE", threading.local(), create=True),
        ):
            anyio.run(scenario)

    def test_blocked_websocket_turns_do_not_starve_http_ai_workers(self) -> None:
        async def scenario() -> None:
            entered = [0]
            entered_lock = threading.Lock()
            all_entered = threading.Event()
            release = threading.Event()
            websocket_iterate = getattr(
                log_service_module,
                "iterate_ws_ai_chunks",
                log_service_module.iterate_ai_chunks,
            )

            async def consume(iterator: _BlockingAfterFirst) -> None:
                async for _chunk in websocket_iterate(iterator):
                    pass

            quick_result = None
            timed_out = False
            try:
                streams = [
                    _BlockingAfterFirst(entered, entered_lock, all_entered, release)
                    for _ in range(2)
                ]
                async with anyio.create_task_group() as task_group:
                    for stream in streams:
                        task_group.start_soon(consume, stream)
                    with anyio.fail_after(2):
                        while not all_entered.is_set():
                            await anyio.sleep(0.01)
                    try:
                        with anyio.fail_after(0.25):
                            quick_result = await log_service_module.run_ai_in_threadpool(
                                lambda: "http-ai-ready"
                            )
                    except TimeoutError:
                        timed_out = True
                    finally:
                        release.set()

                self.assertFalse(
                    timed_out,
                    "blocked websocket turns exhausted the HTTP AI worker capacity",
                )
                self.assertEqual(quick_result, "http-ai-ready")
            finally:
                release.set()

        with (
            mock.patch.object(log_service_module, "_AI_THREAD_CAPACITY", 2),
            mock.patch.object(log_service_module, "_AI_THREAD_STATE", threading.local()),
            mock.patch.object(log_service_module, "_WS_AI_THREAD_CAPACITY", 2, create=True),
            mock.patch.object(log_service_module, "_WS_AI_THREAD_STATE", threading.local(), create=True),
        ):
            anyio.run(scenario)


if __name__ == "__main__":
    unittest.main()
