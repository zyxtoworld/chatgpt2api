from __future__ import annotations

import copy
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from fastapi import FastAPI
from fastapi.testclient import TestClient

import api.system as system_module
from services.config import DEFAULT_PROXY_RUNTIME
from services.storage.json_storage import JSONStorageBackend


AUTH_HEADERS = {"Authorization": "Bearer chatgpt2api"}


class FakeStorage:
    def get_backend_info(self) -> dict[str, object]:
        return {"type": "json"}

    def health_check(self) -> dict[str, object]:
        return {"status": "healthy"}


class FakeConfig:
    def __init__(self) -> None:
        self.data: dict[str, object] = {
            "proxy": "",
            "proxy_runtime": copy.deepcopy(DEFAULT_PROXY_RUNTIME),
        }

    def get(self) -> dict[str, object]:
        return copy.deepcopy(self.data)

    def update(self, updates: dict[str, object]) -> dict[str, object]:
        self.data.update(copy.deepcopy(updates))
        return self.get()

    def get_proxy_settings(self) -> str:
        return str(self.data.get("proxy") or "").strip()

    def get_proxy_runtime_settings(self) -> dict[str, object]:
        return copy.deepcopy(self.data["proxy_runtime"])  # type: ignore[index]

    def get_public_proxy_runtime_settings(self) -> dict[str, object]:
        return self.get_proxy_runtime_settings()

    def get_storage_backend(self) -> FakeStorage:
        return FakeStorage()


class FakeProxySettings:
    def __init__(self) -> None:
        self.runtime_calls = 0

    def get_runtime_status(self) -> dict[str, object]:
        self.runtime_calls += 1
        return {
            "enabled": True,
            "egress_mode": "single_proxy",
            "proxy_source": "proxy_runtime",
            "has_proxy": True,
            "clearance_enabled": True,
            "clearance_mode": "flaresolverr",
            "has_clearance_bundle": False,
            "cached_clearance_hosts": ["internal-target.sentinel.example"],
            "proxy_url": "http://proxy-runtime.sentinel.example:8080",
            "cookies": "opaque-cookie-sentinel",
            "user_agent": "opaque-user-agent-sentinel",
            "error": "opaque-error-sentinel",
            "future_field": "proxy-runtime-future-sentinel",
        }


class FakeAccountService:
    def get_stats(self) -> dict[str, object]:
        return {
            "total": 1,
            "cumulative_total": 1,
            "active": 1,
            "total_quota": 1,
            "limited": 0,
            "abnormal": 0,
            "disabled": 0,
            "total_success": 0,
            "total_fail": 0,
            "by_type": {"web": 1},
        }


class ProxyRuntimeApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.fake_config = FakeConfig()
        self.fake_proxy_settings = FakeProxySettings()
        self.test_proxy_calls: list[str] = []
        self.test_clearance_calls: list[str] = []

        def fake_test_proxy(url: str = "") -> dict[str, object]:
            self.test_proxy_calls.append(url)
            return {
                "ok": True,
                "status": 204,
                "latency_ms": 12,
                "error": None,
                "proxy_source": "proxy_runtime" if not url else "input",
                "has_proxy": True,
            }

        def fake_test_clearance(target_url: str = "https://chatgpt.com") -> dict[str, object]:
            self.test_clearance_calls.append(target_url)
            return {
                "ok": True,
                "status": "ok",
                "latency_ms": 34,
                "has_cookies": True,
                "user_agent": "Flare UA",
                "error": None,
                "runtime": self.fake_proxy_settings.get_runtime_status(),
            }

        self.patchers = [
            mock.patch.object(system_module, "config", self.fake_config),
            mock.patch.object(
                system_module,
                "require_admin_async",
                mock.AsyncMock(return_value={"role": "admin"}),
            ),
            mock.patch.object(system_module, "test_proxy", fake_test_proxy),
            mock.patch.object(system_module, "test_clearance", fake_test_clearance, create=True),
            mock.patch.object(system_module, "proxy_settings", self.fake_proxy_settings, create=True),
            mock.patch("services.account_service.account_service", FakeAccountService()),
        ]
        for patcher in self.patchers:
            patcher.start()
            self.addCleanup(patcher.stop)
        app = FastAPI()
        app.include_router(system_module.create_router("9.9.9-test"))
        self.client = TestClient(app)

    def test_proxy_test_can_use_active_runtime_when_url_is_empty(self) -> None:
        response = self.client.post("/api/proxy/test", headers=AUTH_HEADERS, json={})

        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json()["result"]
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["proxy_source"], "proxy_runtime")
        self.assertEqual(self.test_proxy_calls, [""])

    def test_proxy_runtime_endpoint_reads_and_updates_runtime_config(self) -> None:
        get_response = self.client.get("/api/proxy/runtime", headers=AUTH_HEADERS)
        self.assertEqual(get_response.status_code, 200, get_response.text)
        self.assertEqual(get_response.json()["runtime"]["enabled"], False)
        self.assertEqual(get_response.json()["status"]["proxy_source"], "proxy_runtime")

        runtime = copy.deepcopy(DEFAULT_PROXY_RUNTIME)
        runtime["enabled"] = True
        runtime["egress_mode"] = "single_proxy"
        runtime["proxy_url"] = "http://privoxy:8118"
        runtime["clearance"]["enabled"] = True  # type: ignore[index]
        runtime["clearance"]["mode"] = "flaresolverr"  # type: ignore[index]
        runtime["clearance"]["flaresolverr_url"] = "http://flaresolverr:8191"  # type: ignore[index]

        post_response = self.client.post("/api/proxy/runtime", headers=AUTH_HEADERS, json=runtime)

        self.assertEqual(post_response.status_code, 200, post_response.text)
        self.assertTrue(post_response.json()["runtime"]["enabled"])
        self.assertEqual(post_response.json()["status"]["cached_clearance_hosts"], [])
        for sentinel in (
            "internal-target.sentinel.example",
            "proxy-runtime.sentinel.example",
            "opaque-cookie-sentinel",
            "opaque-user-agent-sentinel",
            "opaque-error-sentinel",
            "proxy-runtime-future-sentinel",
        ):
            self.assertNotIn(sentinel, post_response.text)
        self.assertEqual(self.fake_config.data["proxy_runtime"], runtime)

    def test_proxy_runtime_endpoint_drops_operational_status_details(self) -> None:
        response = self.client.get("/api/proxy/runtime", headers=AUTH_HEADERS)

        self.assertEqual(response.status_code, 200, response.text)
        status = response.json()["status"]
        self.assertEqual(status["cached_clearance_hosts"], [])
        for sentinel in (
            "internal-target.sentinel.example",
            "proxy-runtime.sentinel.example",
            "opaque-cookie-sentinel",
            "opaque-user-agent-sentinel",
            "opaque-error-sentinel",
            "proxy-runtime-future-sentinel",
        ):
            self.assertNotIn(sentinel, response.text)

    def test_storage_info_does_not_expose_real_backend_paths(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            canary = "storage-path-canary"
            root = Path(temp_dir) / canary
            backend = JSONStorageBackend(root / "accounts.json", root / "auth_keys.json")
            with mock.patch.object(self.fake_config, "get_storage_backend", return_value=backend):
                response = self.client.get("/api/storage/info", headers=AUTH_HEADERS)

        self.assertEqual(response.status_code, 200, response.text)
        serialized = response.text
        self.assertNotIn(canary, serialized)
        self.assertNotIn("file_path", response.json()["backend"])
        self.assertNotIn("auth_keys_file_path", response.json()["health"])

    def test_storage_info_projects_real_backend_failure_to_fixed_status(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "storage-failure-canary"
            accounts_path = root / "accounts.json"
            accounts_path.parent.mkdir(parents=True)
            accounts_path.write_text("not-json", encoding="utf-8")
            backend = JSONStorageBackend(accounts_path, root / "auth_keys.json")
            with mock.patch.object(self.fake_config, "get_storage_backend", return_value=backend):
                response = self.client.get("/api/storage/info", headers=AUTH_HEADERS)

        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json(), {
            "backend": {"type": "json"},
            "health": {"status": "unhealthy", "error": "存储后端健康检查失败"},
        })
        self.assertNotIn("storage-failure-canary", response.text)

    def test_clearance_test_endpoint_runs_clearance_refresh_without_returning_cookie_values(self) -> None:
        response = self.client.post(
            "/api/proxy/clearance/test",
            headers=AUTH_HEADERS,
            json={"target_url": "https://chatgpt.com/backend-api/models"},
        )

        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json()["result"]
        self.assertTrue(payload["ok"])
        self.assertTrue(payload["has_cookies"])
        self.assertNotIn("cf_clearance", response.text)
        for sentinel in (
            "internal-target.sentinel.example",
            "proxy-runtime.sentinel.example",
            "opaque-cookie-sentinel",
            "opaque-user-agent-sentinel",
            "opaque-error-sentinel",
            "proxy-runtime-future-sentinel",
        ):
            self.assertNotIn(sentinel, response.text)
        self.assertEqual(self.test_clearance_calls, ["https://chatgpt.com/backend-api/models"])

    def test_clearance_test_projects_malformed_status_values_without_500(self) -> None:
        canary = "proxy-clearance-malformed-canary"
        malformed = {
            "ok": True,
            "status": {canary: "status"},
            "latency_ms": [canary],
            "has_cookies": {canary: True},
            "error": {canary: "error"},
            "runtime": {
                "proxy_source": [canary],
                "egress_mode": {canary: "egress"},
                "clearance_mode": [canary],
            },
        }
        with mock.patch.object(system_module, "test_clearance", return_value=malformed):
            response = self.client.post(
                "/api/proxy/clearance/test",
                headers=AUTH_HEADERS,
                json={"target_url": "https://chatgpt.com/backend-api/models"},
            )

        self.assertEqual(response.status_code, 200, response.text)
        result = response.json()["result"]
        self.assertEqual(result["status"], "error")
        self.assertEqual(result["latency_ms"], 0)
        self.assertFalse(result["has_cookies"])
        self.assertEqual(result["runtime"]["proxy_source"], "unknown")
        self.assertNotIn(canary, response.text)

    def test_health_json_includes_proxy_runtime_status(self) -> None:
        response = self.client.get("/health?format=json")

        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json()
        self.assertEqual(payload["version"], "9.9.9-test")
        self.assertEqual(
            payload["proxy_runtime"],
            {"enabled": True, "clearance_enabled": True},
        )

    def test_public_health_projects_proxy_runtime_without_operational_details(self) -> None:
        self.fake_proxy_settings.runtime_calls = 0
        json_response = self.client.get("/health?format=json")

        self.assertEqual(json_response.status_code, 200, json_response.text)
        self.assertEqual(self.fake_proxy_settings.runtime_calls, 1)
        payload = json_response.json()
        self.assertEqual(payload["status"], "ok")
        self.assertTrue(payload["healthy"])
        self.assertEqual(
            payload["proxy_runtime"],
            {"enabled": True, "clearance_enabled": True},
        )
        for sentinel in (
            "internal-target.sentinel.example",
            "proxy-runtime.sentinel.example",
            "opaque-cookie-sentinel",
            "opaque-user-agent-sentinel",
            "opaque-error-sentinel",
            "proxy-runtime-future-sentinel",
            "proxy_source",
            "cached_clearance_hosts",
            "future_field",
        ):
            self.assertNotIn(sentinel, json_response.text)

        self.fake_proxy_settings.runtime_calls = 0
        html_response = self.client.get("/health")

        self.assertEqual(html_response.status_code, 200, html_response.text)
        self.assertEqual(self.fake_proxy_settings.runtime_calls, 1)
        for sentinel in (
            "internal-target.sentinel.example",
            "proxy-runtime.sentinel.example",
            "opaque-cookie-sentinel",
            "opaque-user-agent-sentinel",
            "opaque-error-sentinel",
            "proxy-runtime-future-sentinel",
            "proxy_source",
            "cached_clearance_hosts",
            "future_field",
        ):
            self.assertNotIn(sentinel, html_response.text)

    def test_health_stays_available_when_proxy_runtime_probe_fails(self) -> None:
        with mock.patch.object(
            self.fake_proxy_settings,
            "get_runtime_status",
            side_effect=RuntimeError("runtime probe failed"),
        ):
            response = self.client.get("/health?format=json")

        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(
            response.json()["proxy_runtime"],
            {"enabled": False, "clearance_enabled": False},
        )


if __name__ == "__main__":
    unittest.main()
