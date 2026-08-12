from __future__ import annotations

import json
import unittest
from unittest import mock

from fastapi import FastAPI
from fastapi.testclient import TestClient

import api.accounts as accounts_module


AUTH_HEADERS = {"Authorization": "Bearer chatgpt2api"}
ACCESS_TOKEN = "access-token-used-as-account-id"
PRIVATE_ACCOUNT = {
    "access_token": ACCESS_TOKEN,
    "refresh_token": "refresh-token-must-not-be-public",
    "id_token": "id-token-must-not-be-public",
    "password": "password-must-not-be-public",
    "proxy": "http://proxy.example.test:8080",
    "type": "free",
    "status": "正常",
    "quota": 1,
    "success": 0,
    "fail": 0,
}


class AccountPublicContractTests(unittest.TestCase):
    def setUp(self) -> None:
        app = FastAPI()
        app.include_router(accounts_module.create_router())
        self.client = TestClient(app)

    def test_account_list_keeps_selection_id_but_drops_persisted_credentials(self) -> None:
        with mock.patch.object(accounts_module.account_service, "list_accounts", return_value=[dict(PRIVATE_ACCOUNT)]):
            response = self.client.get("/api/accounts", headers=AUTH_HEADERS)

        self.assertEqual(response.status_code, 200, response.text)
        serialized = json.dumps(response.json(), ensure_ascii=False)
        self.assertEqual(response.json()["items"][0]["access_token"], ACCESS_TOKEN)
        self.assertEqual(response.json()["items"][0]["proxy"], "http://proxy.example.test:8080")
        self.assertNotIn("refresh-token-must-not-be-public", serialized)
        self.assertNotIn("id-token-must-not-be-public", serialized)
        self.assertNotIn("password-must-not-be-public", serialized)

    def test_refresh_progress_drops_credentials_from_nested_account_items(self) -> None:
        progress = {
            "total": 1,
            "processed": 1,
            "done": True,
            "error": None,
            "result": {"items": [dict(PRIVATE_ACCOUNT)], "refreshed": 1, "errors": []},
        }
        with mock.patch.object(accounts_module.account_service, "get_refresh_progress", return_value=progress):
            response = self.client.get("/api/accounts/refresh/progress/progress-1", headers=AUTH_HEADERS)

        self.assertEqual(response.status_code, 200, response.text)
        serialized = json.dumps(response.json(), ensure_ascii=False)
        self.assertEqual(response.json()["result"]["items"][0]["access_token"], ACCESS_TOKEN)
        self.assertEqual(response.json()["result"]["items"][0]["proxy"], "http://proxy.example.test:8080")
        self.assertNotIn("refresh-token-must-not-be-public", serialized)
        self.assertNotIn("id-token-must-not-be-public", serialized)
        self.assertNotIn("password-must-not-be-public", serialized)

    def test_create_response_projects_nested_account_items(self) -> None:
        with (
            mock.patch.object(
                accounts_module.account_service,
                "add_account_items",
                return_value={"added": 1, "skipped": 0, "items": [dict(PRIVATE_ACCOUNT)]},
            ),
            mock.patch.object(
                accounts_module.account_service,
                "refresh_accounts",
                return_value={"refreshed": 1, "errors": [], "items": [dict(PRIVATE_ACCOUNT)]},
            ),
        ):
            response = self.client.post(
                "/api/accounts",
                headers=AUTH_HEADERS,
                json={"tokens": [ACCESS_TOKEN]},
            )

        self.assertEqual(response.status_code, 200, response.text)
        serialized = json.dumps(response.json(), ensure_ascii=False)
        self.assertEqual(response.json()["items"][0]["access_token"], ACCESS_TOKEN)
        self.assertEqual(response.json()["items"][0]["proxy"], "http://proxy.example.test:8080")
        self.assertNotIn("refresh-token-must-not-be-public", serialized)
        self.assertNotIn("id-token-must-not-be-public", serialized)
        self.assertNotIn("password-must-not-be-public", serialized)

    def test_relogin_progress_projects_nested_account_items(self) -> None:
        progress = {
            "total": 1,
            "processed": 1,
            "done": True,
            "error": None,
            "result": {"items": [dict(PRIVATE_ACCOUNT)], "relogined": 1, "errors": []},
        }
        with mock.patch.object(accounts_module.account_service, "get_relogin_progress", return_value=progress):
            response = self.client.get("/api/accounts/re-login/progress/progress-2", headers=AUTH_HEADERS)

        self.assertEqual(response.status_code, 200, response.text)
        serialized = json.dumps(response.json(), ensure_ascii=False)
        self.assertEqual(response.json()["result"]["items"][0]["access_token"], ACCESS_TOKEN)
        self.assertEqual(response.json()["result"]["items"][0]["proxy"], "http://proxy.example.test:8080")
        self.assertNotIn("refresh-token-must-not-be-public", serialized)
        self.assertNotIn("id-token-must-not-be-public", serialized)
        self.assertNotIn("password-must-not-be-public", serialized)

    def test_update_response_projects_item_and_items(self) -> None:
        with (
            mock.patch.object(accounts_module.account_service, "update_account", return_value=dict(PRIVATE_ACCOUNT)),
            mock.patch.object(accounts_module.account_service, "list_accounts", return_value=[dict(PRIVATE_ACCOUNT)]),
        ):
            response = self.client.post(
                "/api/accounts/update",
                headers=AUTH_HEADERS,
                json={"access_token": ACCESS_TOKEN, "proxy": "http://proxy.example.test:8080"},
            )

        self.assertEqual(response.status_code, 200, response.text)
        serialized = json.dumps(response.json(), ensure_ascii=False)
        self.assertEqual(response.json()["item"]["access_token"], ACCESS_TOKEN)
        self.assertEqual(response.json()["item"]["proxy"], "http://proxy.example.test:8080")
        self.assertEqual(response.json()["items"][0]["access_token"], ACCESS_TOKEN)
        self.assertNotIn("refresh-token-must-not-be-public", serialized)
        self.assertNotIn("id-token-must-not-be-public", serialized)
        self.assertNotIn("password-must-not-be-public", serialized)

    def test_oauth_finish_projects_account_items(self) -> None:
        oauth_tokens = {
            "access_token": ACCESS_TOKEN,
            "refresh_token": "oauth-refresh-token",
            "id_token": "oauth-id-token",
        }
        with (
            mock.patch.object(accounts_module.oauth_login_service, "finish", return_value=oauth_tokens),
            mock.patch.object(
                accounts_module.account_service,
                "add_account_items",
                return_value={"added": 1, "skipped": 0, "items": [dict(PRIVATE_ACCOUNT)]},
            ),
            mock.patch.object(
                accounts_module.account_service,
                "refresh_accounts",
                return_value={"refreshed": 1, "errors": [], "items": [dict(PRIVATE_ACCOUNT)]},
            ),
        ):
            response = self.client.post(
                "/api/accounts/oauth/finish",
                headers=AUTH_HEADERS,
                json={"session_id": "session-1", "callback": "code-1"},
            )

        self.assertEqual(response.status_code, 200, response.text)
        serialized = json.dumps(response.json(), ensure_ascii=False)
        self.assertEqual(response.json()["items"][0]["access_token"], ACCESS_TOKEN)
        self.assertEqual(response.json()["items"][0]["proxy"], "http://proxy.example.test:8080")
        self.assertNotIn("refresh-token-must-not-be-public", serialized)
        self.assertNotIn("id-token-must-not-be-public", serialized)
        self.assertNotIn("password-must-not-be-public", serialized)


if __name__ == "__main__":
    unittest.main()
