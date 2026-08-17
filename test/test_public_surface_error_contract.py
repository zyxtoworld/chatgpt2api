from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import services.proxy_service as proxy_module
from services.proxy_service import test_clearance as run_clearance_test
from services.proxy_service import test_proxy as run_proxy_test
from services.storage.database_storage import DatabaseStorageBackend
from services.storage.git_storage import GitStorageBackend
from services.storage.json_storage import JSONStorageBackend


SECRET = "opaque-proxy-token user:password@proxy.example owner@example.com"


class PublicSurfaceErrorContractTests(unittest.TestCase):
    def test_clearance_test_does_not_project_container_user_agent(self) -> None:
        canary = "clearance-user-agent-container-canary"
        status = {"clearance_enabled": True, "clearance_mode": "flaresolverr"}
        bundle = SimpleNamespace(cookies={}, user_agent={"secret": canary})
        with (
            mock.patch.object(proxy_module.proxy_settings, "get_runtime_status", return_value=status),
            mock.patch.object(proxy_module.proxy_settings, "refresh_clearance", return_value=bundle),
        ):
            result = run_clearance_test()

        self.assertEqual(result["user_agent"], "")
        self.assertNotIn(canary, json.dumps(result, ensure_ascii=False))

    def test_proxy_test_does_not_return_raw_exception_text(self) -> None:
        class FailedSession:
            def __init__(self, *args, **kwargs):
                pass

            def get(self, *args, **kwargs):
                raise RuntimeError(SECRET)

            def close(self):
                pass

        profile = SimpleNamespace(proxy_url="http://proxy.example:8080", proxy_source="proxy_runtime")
        with (
            mock.patch.object(proxy_module.proxy_settings, "get_profile", return_value=profile),
            mock.patch.object(proxy_module, "Session", FailedSession),
        ):
            result = run_proxy_test()

        self.assertEqual(result["error"], "代理测试失败，请稍后重试")
        self.assertNotIn(SECRET, json.dumps(result, ensure_ascii=False))

    def test_clearance_test_does_not_return_raw_exception_text(self) -> None:
        status = {"clearance_enabled": True, "clearance_mode": "flaresolverr"}
        with (
            mock.patch.object(proxy_module.proxy_settings, "get_runtime_status", return_value=status),
            mock.patch.object(proxy_module.proxy_settings, "refresh_clearance", side_effect=RuntimeError(SECRET)),
        ):
            result = run_clearance_test()

        self.assertEqual(result["error"], "通关测试失败，请稍后重试")
        self.assertNotIn(SECRET, json.dumps(result, ensure_ascii=False))

    def test_storage_health_checks_do_not_return_raw_exception_text(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            json_backend = JSONStorageBackend(Path(temp_dir) / "accounts.json")

            class FailingPath:
                def exists(self) -> bool:
                    return True

                def read_text(self, **kwargs):
                    raise RuntimeError(SECRET)

            json_backend.file_path = FailingPath()  # type: ignore[assignment]
            json_result = json_backend.health_check()

            database_backend = DatabaseStorageBackend(f"sqlite:///{Path(temp_dir) / 'accounts.db'}")
            database_backend.Session = mock.Mock(side_effect=RuntimeError(SECRET))
            database_result = database_backend.health_check()

            git_backend = GitStorageBackend(
                "https://git.example/repo.git",
                "git-secret",
                local_cache_dir=Path(temp_dir) / "git-cache",
            )
            with mock.patch.object(git_backend, "_clone_or_pull", side_effect=RuntimeError(SECRET)):
                git_result = git_backend.health_check()
            database_backend.engine.dispose()

        for result in (json_result, database_result, git_result):
            with self.subTest(backend=result.get("backend")):
                self.assertEqual(result["error"], "存储后端健康检查失败")
                self.assertNotIn(SECRET, json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    unittest.main()
