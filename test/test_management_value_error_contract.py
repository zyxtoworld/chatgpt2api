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


class ManagementValueErrorContractTests(unittest.TestCase):
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
