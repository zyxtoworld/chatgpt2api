from __future__ import annotations

import unittest
from unittest import mock

from fastapi import FastAPI
from fastapi.testclient import TestClient

import api.accounts as accounts_module


AUTH_HEADERS = {"Authorization": "Bearer chatgpt2api"}


class CPAAPIPublicErrorTests(unittest.TestCase):
    def test_file_listing_projects_upstream_failure_to_fixed_502(self) -> None:
        secret = "opaque-cpa-upstream-secret owner@example.com"
        app = FastAPI()
        app.include_router(accounts_module.create_router())
        pool = {
            "id": "pool-1",
            "name": "CPA",
            "base_url": "https://cpa.example.test",
            "secret_key": "management-secret",
        }

        with (
            mock.patch.object(accounts_module.cpa_config, "get_pool", return_value=pool),
            mock.patch.object(accounts_module, "list_remote_files", side_effect=RuntimeError(secret)),
        ):
            response = TestClient(app, raise_server_exceptions=False).get(
                "/api/cpa/pools/pool-1/files",
                headers=AUTH_HEADERS,
            )

        self.assertEqual(response.status_code, 502, response.text)
        self.assertNotIn(secret, response.text)
        self.assertNotIn("owner@example.com", response.text)


if __name__ == "__main__":
    unittest.main()
