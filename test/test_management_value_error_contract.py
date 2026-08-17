from __future__ import annotations

import json
import unittest
from unittest import mock

from fastapi import FastAPI
from fastapi.testclient import TestClient

import api.accounts as accounts_module
import api.system as system_module
from api.accounts import create_router as create_accounts_router
from api.system import create_router as create_system_router


SECRET = "opaque-value-error-secret owner@example.com"


class _FakeCPAConfig:
    def get_pool(self, pool_id: str):
        return {"id": pool_id}


class _FakeSub2APIConfig:
    def get_server(self, server_id: str):
        return {"id": server_id}


class _BusyCPAConfig:
    def delete_pool(self, _pool_id: str):
        raise accounts_module.ImportJobActiveError("import is already running")


class _BusySub2APIConfig:
    def delete_server(self, _server_id: str):
        raise accounts_module.ImportJobActiveError("import is already running")


class _BusyCCLoadConfig:
    def delete_server(self, _server_id: str):
        raise accounts_module.ImportJobActiveError("import is already running")


class _InvalidCPAConfig:
    def delete_pool(self, _pool_id: str):
        raise accounts_module.PublicSafeValueError(SECRET)


class _InvalidSub2APIConfig:
    def delete_server(self, _server_id: str):
        raise accounts_module.PublicSafeValueError(SECRET)


class _InvalidCCLoadConfig:
    def delete_server(self, _server_id: str):
        raise accounts_module.PublicSafeValueError(SECRET)


class ManagementValueErrorContractTests(unittest.TestCase):
    def test_delete_connection_reports_active_import_as_conflict(self) -> None:
        app = FastAPI()
        app.include_router(create_accounts_router())
        with (
            mock.patch.object(accounts_module, "require_admin_async", return_value={"role": "admin"}),
            mock.patch.object(accounts_module, "cpa_config", _BusyCPAConfig()),
            mock.patch.object(accounts_module, "sub2api_config", _BusySub2APIConfig()),
            mock.patch.object(accounts_module, "ccload_config", _BusyCCLoadConfig()),
        ):
            client = TestClient(app)
            responses = [
                client.delete("/api/cpa/pools/pool-1"),
                client.delete("/api/sub2api/servers/server-1"),
                client.delete("/api/ccload/servers/server-1"),
            ]

        for response in responses:
            with self.subTest(path=response.request.url):
                self.assertEqual(response.status_code, 409, response.text)
                self.assertEqual(response.json(), {"detail": {"error": "import is already running"}})

    def test_generic_safe_value_error_is_not_misclassified_as_conflict(self) -> None:
        app = FastAPI()
        app.include_router(create_accounts_router())
        with (
            mock.patch.object(accounts_module, "require_admin_async", return_value={"role": "admin"}),
            mock.patch.object(accounts_module, "cpa_config", _InvalidCPAConfig()),
            mock.patch.object(accounts_module, "sub2api_config", _InvalidSub2APIConfig()),
            mock.patch.object(accounts_module, "ccload_config", _InvalidCCLoadConfig()),
        ):
            client = TestClient(app, raise_server_exceptions=False)
            responses = [
                client.delete("/api/cpa/pools/pool-1"),
                client.delete("/api/sub2api/servers/server-1"),
                client.delete("/api/ccload/servers/server-1"),
            ]

        for response in responses:
            with self.subTest(path=response.request.url):
                self.assertEqual(response.status_code, 500, response.text)
                self.assertNotIn(SECRET, response.text)

    def test_delete_missing_connection_preserves_not_found_contract(self) -> None:
        class _MissingCPAConfig:
            def delete_pool(self, _pool_id: str):
                return False

        class _MissingSub2APIConfig:
            def delete_server(self, _server_id: str):
                return False

        class _MissingCCLoadConfig:
            def delete_server(self, _server_id: str):
                return False

        app = FastAPI()
        app.include_router(create_accounts_router())
        with (
            mock.patch.object(accounts_module, "require_admin_async", return_value={"role": "admin"}),
            mock.patch.object(accounts_module, "cpa_config", _MissingCPAConfig()),
            mock.patch.object(accounts_module, "sub2api_config", _MissingSub2APIConfig()),
            mock.patch.object(accounts_module, "ccload_config", _MissingCCLoadConfig()),
        ):
            client = TestClient(app)
            responses = [
                client.delete("/api/cpa/pools/missing"),
                client.delete("/api/sub2api/servers/missing"),
                client.delete("/api/ccload/servers/missing"),
            ]

        for response in responses:
            with self.subTest(path=response.request.url):
                self.assertEqual(response.status_code, 404, response.text)

    def test_generic_value_errors_are_not_reflected_by_management_routes(self) -> None:
        app = FastAPI()
        app.include_router(create_accounts_router())
        app.include_router(create_system_router("test"))

        auth_service = mock.Mock()
        auth_service.create_key.side_effect = ValueError(SECRET)
        cpa_import_service = mock.Mock()
        cpa_import_service.start_import.side_effect = ValueError(SECRET)
        sub2api_import_service = mock.Mock()
        sub2api_import_service.start_import.side_effect = ValueError(SECRET)
        config = mock.Mock()
        config.update.side_effect = ValueError(SECRET)

        with (
            mock.patch.object(accounts_module, "require_admin_async", return_value={"role": "admin"}),
            mock.patch.object(system_module, "require_admin_async", return_value={"role": "admin"}),
            mock.patch.object(accounts_module, "auth_service", auth_service),
            mock.patch.object(accounts_module, "cpa_config", _FakeCPAConfig()),
            mock.patch.object(accounts_module, "cpa_import_service", cpa_import_service),
            mock.patch.object(accounts_module, "sub2api_config", _FakeSub2APIConfig()),
            mock.patch.object(accounts_module, "sub2api_import_service", sub2api_import_service),
            mock.patch.object(system_module, "config", config),
        ):
            client = TestClient(app)
            responses = [
                client.post("/api/auth/users", json={}),
                client.post("/api/cpa/pools/pool-1/import", json={"names": ["file.json"]}),
                client.post("/api/sub2api/servers/server-1/import", json={"account_ids": ["account-1"]}),
                client.post("/api/settings", json={"proxy": "https://proxy.example"}),
                client.post("/api/proxy/runtime", json={"enabled": True}),
            ]

        for response in responses:
            with self.subTest(path=response.request.url):
                self.assertEqual(response.status_code, 400, response.text)
                self.assertNotIn(SECRET, response.text)
                self.assertNotIn("owner@example.com", response.text)
                self.assertNotIn("opaque-value-error-secret", json.dumps(response.json(), ensure_ascii=False))


if __name__ == "__main__":
    unittest.main()
