from __future__ import annotations

import unittest
from unittest import mock

from fastapi import FastAPI
from fastapi.testclient import TestClient

import api.accounts as accounts_module
from api.accounts import create_router


class _FakeCPAConfig:
    def list_pools(self):
        return [{
            "id": "pool-1",
            "name": "CPA",
            "base_url": "https://cpa-user:cpa-url-secret@cpa.example/api",
            "secret_key": "cpa-secret-key",
        }]


class _FakeSub2APIConfig:
    def list_servers(self):
        return [{
            "id": "server-1",
            "name": "Sub2API",
            "base_url": "https://sub-user:sub-url-secret@sub.example/api",
            "email": "owner@example.com",
            "password": "sub-password",
            "api_key": "sub-api-key",
        }]


class ManagementURLContractTests(unittest.TestCase):
    def test_management_lists_do_not_return_url_userinfo(self) -> None:
        app = FastAPI()
        app.include_router(create_router())
        with (
            mock.patch.object(accounts_module, "require_admin_async", return_value={"role": "admin"}),
            mock.patch.object(accounts_module, "cpa_config", _FakeCPAConfig()),
            mock.patch.object(accounts_module, "sub2api_config", _FakeSub2APIConfig()),
        ):
            client = TestClient(app)
            cpa_response = client.get("/api/cpa/pools")
            sub_response = client.get("/api/sub2api/servers")

        self.assertEqual(cpa_response.status_code, 200, cpa_response.text)
        self.assertEqual(sub_response.status_code, 200, sub_response.text)
        self.assertNotIn("cpa-url-secret", cpa_response.text)
        self.assertNotIn("sub-url-secret", sub_response.text)


if __name__ == "__main__":
    unittest.main()
