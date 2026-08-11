from __future__ import annotations

import json
import unittest
from unittest import mock

from fastapi import FastAPI
from fastapi.testclient import TestClient

import api.accounts as accounts_module


AUTH_HEADERS = {"Authorization": "Bearer chatgpt2api"}


class _FakeCCLoadConfig:
    def __init__(self) -> None:
        self.server = {
            "id": "server-1",
            "name": "preview",
            "base_url": "https://user:url-secret@ccload.example.test",
            "password": "admin-password-secret",
            "import_job": None,
        }

    def list_servers(self) -> list[dict]:
        return [dict(self.server)]

    def get_server(self, server_id: str) -> dict | None:
        return dict(self.server) if server_id == self.server["id"] else None

    def add_server(self, **values) -> dict:
        self.server.update(values)
        return dict(self.server)

    def update_server(self, server_id: str, updates: dict) -> dict | None:
        if server_id != self.server["id"]:
            return None
        self.server.update(updates)
        return dict(self.server)

    def delete_server(self, server_id: str) -> bool:
        return server_id == self.server["id"]


class _FakeImportService:
    def start_import(self, _server: dict, channel_ids: list[str]) -> dict:
        return {"status": "pending", "total": len(channel_ids), "errors": []}


class CCLoadAPIContractTests(unittest.TestCase):
    def setUp(self) -> None:
        app = FastAPI()
        app.include_router(accounts_module.create_router())
        self.client = TestClient(app)
        self.config = _FakeCCLoadConfig()

    def test_all_ccload_routes_require_admin(self) -> None:
        requests = (
            ("GET", "/api/ccload/servers", None),
            ("POST", "/api/ccload/servers", {"base_url": "https://x.test", "password": "secret"}),
            ("POST", "/api/ccload/servers/server-1", {"name": "new"}),
            ("DELETE", "/api/ccload/servers/server-1", None),
            ("GET", "/api/ccload/servers/server-1/channels", None),
            ("POST", "/api/ccload/servers/server-1/import", {"channel_ids": ["7"]}),
            ("GET", "/api/ccload/servers/server-1/import", None),
        )
        for method, path, body in requests:
            with self.subTest(method=method, path=path):
                response = self.client.request(method, path, json=body)
                self.assertIn(response.status_code, {401, 403}, response.text)

    def test_server_listing_never_returns_password_or_url_userinfo(self) -> None:
        with mock.patch.object(accounts_module, "ccload_config", self.config):
            response = self.client.get("/api/ccload/servers", headers=AUTH_HEADERS)

        self.assertEqual(response.status_code, 200, response.text)
        serialized = json.dumps(response.json(), ensure_ascii=False)
        self.assertNotIn("admin-password-secret", serialized)
        self.assertNotIn("url-secret", serialized)
        self.assertNotIn("user@", serialized)
        self.assertTrue(response.json()["servers"][0]["has_password"])

    def test_channel_listing_projects_unknown_upstream_error_to_fixed_502(self) -> None:
        opaque_secret = "opaque-upstream-secret user@example.test"
        with (
            mock.patch.object(accounts_module, "ccload_config", self.config),
            mock.patch.object(accounts_module, "ccload_list_remote_channels", side_effect=RuntimeError(opaque_secret)),
        ):
            response = self.client.get(
                "/api/ccload/servers/server-1/channels",
                headers=AUTH_HEADERS,
            )

        self.assertEqual(response.status_code, 502, response.text)
        self.assertNotIn(opaque_secret, response.text)
        self.assertNotIn("user@example.test", response.text)

    def test_import_route_returns_only_job_metadata(self) -> None:
        import_service = _FakeImportService()
        with (
            mock.patch.object(accounts_module, "ccload_config", self.config),
            mock.patch.object(accounts_module, "ccload_import_service", import_service),
        ):
            response = self.client.post(
                "/api/ccload/servers/server-1/import",
                headers=AUTH_HEADERS,
                json={"channel_ids": ["7"]},
            )

        self.assertEqual(response.status_code, 200, response.text)
        serialized = json.dumps(response.json(), ensure_ascii=False)
        self.assertNotIn("admin-password-secret", serialized)
        self.assertNotIn("access_token", serialized)


if __name__ == "__main__":
    unittest.main()
