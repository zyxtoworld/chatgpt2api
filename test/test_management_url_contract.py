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


class _FakeCCLoadConfig:
    def list_servers(self):
        return [{
            "id": "ccload-1",
            "name": "ccLoad",
            "base_url": "https://ccload.example/api",
            "password": "ccload-password",
            "internal_metadata": {"secret": "management-projector-canary"},
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

    def test_management_lists_drop_base_url_query_and_fragment(self) -> None:
        secret = "management-query-secret"
        cpa = _FakeCPAConfig()
        cpa.list_pools = lambda: [{
            "id": "pool-1",
            "name": "CPA",
            "base_url": f"https://cpa.example/api?token={secret}#fragment-secret",
        }]
        sub2api = _FakeSub2APIConfig()
        sub2api.list_servers = lambda: [{
            "id": "server-1",
            "name": "Sub2API",
            "base_url": f"https://sub2api.example/api?token={secret}#fragment-secret",
        }]
        ccload = _FakeCCLoadConfig()
        ccload.list_servers = lambda: [{
            "id": "ccload-1",
            "name": "ccLoad",
            "base_url": f"https://ccload.example/api?token={secret}#fragment-secret",
        }]
        app = FastAPI()
        app.include_router(create_router())
        with (
            mock.patch.object(accounts_module, "require_admin_async", return_value={"role": "admin"}),
            mock.patch.object(accounts_module, "cpa_config", cpa),
            mock.patch.object(accounts_module, "sub2api_config", sub2api),
            mock.patch.object(accounts_module, "ccload_config", ccload),
        ):
            responses = [
                TestClient(app).get("/api/cpa/pools"),
                TestClient(app).get("/api/sub2api/servers"),
                TestClient(app).get("/api/ccload/servers"),
            ]

        for response in responses:
            self.assertEqual(response.status_code, 200, response.text)
            self.assertNotIn(secret, response.text)
            self.assertNotIn("fragment-secret", response.text)

    def test_management_lists_drop_unknown_nested_fields(self) -> None:
        canary = "management-projector-canary"
        cpa = _FakeCPAConfig()
        cpa.list_pools = lambda: [{
            "id": "pool-1",
            "name": "CPA",
            "base_url": "https://cpa.example/api",
            "secret_key": "secret",
            "internal_metadata": {"secret": canary},
            "import_job": {"status": "idle", "internal": canary},
        }]
        sub2api = _FakeSub2APIConfig()
        sub2api.list_servers = lambda: [{
            "id": "server-1",
            "name": "Sub2API",
            "base_url": "https://sub2api.example/api",
            "email": "owner@example.com",
            "password": "password",
            "api_key": "key",
            "internal_metadata": {"secret": canary},
            "import_job": {"status": "idle", "internal": canary},
        }]
        ccload = _FakeCCLoadConfig()
        with (
            mock.patch.object(accounts_module, "require_admin_async", return_value={"role": "admin"}),
            mock.patch.object(accounts_module, "cpa_config", cpa),
            mock.patch.object(accounts_module, "sub2api_config", sub2api),
            mock.patch.object(accounts_module, "ccload_config", ccload),
        ):
            app = FastAPI()
            app.include_router(create_router())
            client = TestClient(app)
            responses = [
                client.get("/api/cpa/pools"),
                client.get("/api/sub2api/servers"),
                client.get("/api/ccload/servers"),
            ]

        for response in responses:
            self.assertEqual(response.status_code, 200, response.text)
            self.assertNotIn(canary, response.text)

    def test_management_import_job_unknown_status_is_fixed(self) -> None:
        canary = "import-job-status-canary owner@example.com"
        cpa = _FakeCPAConfig()
        cpa.list_pools = lambda: [{
            "id": "pool-1",
            "name": "CPA",
            "base_url": "https://cpa.example/api",
            "import_job": {"job_id": "job-1", "status": canary, "errors": []},
        }]
        app = FastAPI()
        app.include_router(create_router())
        with (
            mock.patch.object(accounts_module, "require_admin_async", return_value={"role": "admin"}),
            mock.patch.object(accounts_module, "cpa_config", cpa),
        ):
            response = TestClient(app).get("/api/cpa/pools")

        assert response.status_code == 200, response.text
        assert canary not in response.text
        assert response.json()["pools"][0]["import_job"]["status"] == "failed"

    def test_management_credential_flags_reject_container_values(self) -> None:
        sub2api = _FakeSub2APIConfig()
        sub2api.list_servers = lambda: [{
            "id": "server-1",
            "name": "Sub2API",
            "base_url": "https://sub2api.example/api",
            "api_key": {"secret": "sub-api-key-canary"},
        }]
        ccload = _FakeCCLoadConfig()
        ccload.list_servers = lambda: [{
            "id": "ccload-1",
            "name": "ccLoad",
            "base_url": "https://ccload.example/api",
            "password": ["ccload-password-canary"],
        }]
        app = FastAPI()
        app.include_router(create_router())
        with (
            mock.patch.object(accounts_module, "require_admin_async", return_value={"role": "admin"}),
            mock.patch.object(accounts_module, "sub2api_config", sub2api),
            mock.patch.object(accounts_module, "ccload_config", ccload),
        ):
            sub_response = TestClient(app).get("/api/sub2api/servers")
            ccload_response = TestClient(app).get("/api/ccload/servers")

        assert sub_response.status_code == 200, sub_response.text
        assert ccload_response.status_code == 200, ccload_response.text
        assert sub_response.json()["servers"][0]["has_api_key"] is False
        assert ccload_response.json()["servers"][0]["has_password"] is False


if __name__ == "__main__":
    unittest.main()
