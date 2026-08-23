from __future__ import annotations

import json
import unittest
from unittest import mock
from unittest.mock import AsyncMock

from fastapi import FastAPI
from fastapi.testclient import TestClient

import api.accounts as accounts_module
from utils.helper import anonymize_token


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

    def test_account_list_drops_container_values_from_limits_progress(self) -> None:
        canary = "limits-progress-container-canary"
        account = dict(PRIVATE_ACCOUNT)
        account["limits_progress"] = [{
            "feature_name": {"secret": canary},
            "remaining": [canary],
            "reset_after": {"secret": canary},
        }]
        with mock.patch.object(accounts_module.account_service, "list_accounts", return_value=[account]):
            response = self.client.get("/api/accounts", headers=AUTH_HEADERS)

        self.assertEqual(response.status_code, 200, response.text)
        serialized = json.dumps(response.json(), ensure_ascii=False)
        self.assertNotIn(canary, serialized)
        self.assertEqual(response.json()["items"][0]["limits_progress"], [])

    def test_account_list_drops_container_values_from_public_fields(self) -> None:
        canary = "account-public-container-canary"
        account = dict(PRIVATE_ACCOUNT)
        account.update({
            "proxy": {"secret": canary},
            "type": [canary],
            "email": {"secret": canary},
            "quota": {"secret": canary},
            "success": [canary],
            "fail": {"secret": canary},
            "image_inflight": {"secret": canary},
        })
        with mock.patch.object(accounts_module.account_service, "list_accounts", return_value=[account]):
            response = self.client.get("/api/accounts", headers=AUTH_HEADERS)

        self.assertEqual(response.status_code, 200, response.text)
        item = response.json()["items"][0]
        serialized = json.dumps(response.json(), ensure_ascii=False)
        self.assertNotIn(canary, serialized)
        for field in ("proxy", "type", "quota", "success", "fail", "image_inflight"):
            self.assertNotIn(field, item)

    def test_account_list_drops_string_quota_instead_of_treating_it_as_text(self) -> None:
        canary = "account-quota-text-canary owner@example.com"
        account = dict(PRIVATE_ACCOUNT)
        account["quota"] = canary
        with mock.patch.object(accounts_module.account_service, "list_accounts", return_value=[account]):
            response = self.client.get("/api/accounts", headers=AUTH_HEADERS)

        self.assertEqual(response.status_code, 200, response.text)
        self.assertNotIn(canary, response.text)
        self.assertNotIn("quota", response.json()["items"][0])

    def test_account_list_preserves_typed_limits_progress_fields(self) -> None:
        account = dict(PRIVATE_ACCOUNT)
        account["limits_progress"] = [{
            "feature_name": "image_gen",
            "remaining": 3,
            "reset_after": "2026-08-13T12:00:00Z",
            "internal": "dropped",
        }]
        with mock.patch.object(accounts_module.account_service, "list_accounts", return_value=[account]):
            response = self.client.get("/api/accounts", headers=AUTH_HEADERS)

        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["items"][0]["limits_progress"], [{
            "feature_name": "image_gen",
            "remaining": 3,
            "reset_after": "2026-08-13T12:00:00Z",
        }])

    def test_account_list_bounds_limits_progress_to_upstream_contract(self) -> None:
        account = dict(PRIVATE_ACCOUNT)
        account["limits_progress"] = [
            {"feature_name": f"feature-{index}", "remaining": index}
            for index in range(101)
        ]
        with mock.patch.object(accounts_module.account_service, "list_accounts", return_value=[account]):
            response = self.client.get("/api/accounts", headers=AUTH_HEADERS)

        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(len(response.json()["items"][0]["limits_progress"]), 100)

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
                return_value={
                    "added": 1,
                    "skipped": 0,
                    "items": [dict(PRIVATE_ACCOUNT)],
                    "internal_result_canary": "account-result-secret",
                },
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
        self.assertNotIn("internal_result_canary", response.json())

    def test_mixed_account_and_token_create_uses_one_atomic_add_batch(self) -> None:
        batches: list[list[dict]] = []

        def add_account_items(items: list[dict]) -> dict:
            batches.append(items)
            return {
                "added": len(items),
                "skipped": 0,
                "items": [
                    {"access_token": item["access_token"], "type": "free", "status": "正常"}
                    for item in items
                    if isinstance(item, dict) and isinstance(item.get("access_token"), str)
                ],
            }

        with (
            mock.patch.object(accounts_module.account_service, "add_account_items", side_effect=add_account_items),
            mock.patch.object(
                accounts_module.account_service,
                "add_accounts",
                side_effect=AssertionError("mixed create must not split into a second write"),
            ) as add_accounts,
            mock.patch.object(
                accounts_module.account_service,
                "refresh_accounts",
                return_value={"refreshed": 2, "errors": [], "items": []},
            ),
        ):
            response = self.client.post(
                "/api/accounts",
                headers=AUTH_HEADERS,
                json={
                    "tokens": ["token-extra"],
                    "accounts": [{"access_token": "token-structured", "type": "free"}],
                },
            )

        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(len(batches), 1)
        self.assertCountEqual(
            [item["access_token"] for item in batches[0]],
            ["token-structured", "token-extra"],
        )
        add_accounts.assert_not_called()

    def test_create_validates_element_types_and_keeps_python_token_contract(self) -> None:
        with (
            mock.patch.object(accounts_module.account_service, "add_account_items") as add_items,
            mock.patch.object(accounts_module.account_service, "add_accounts") as add_accounts,
            mock.patch.object(accounts_module.account_service, "refresh_accounts") as refresh_accounts,
        ):
            for body in (
                {"tokens": ["valid-token", 42]},
                {"accounts": [{"access_token": "valid-token"}, "not-an-object"]},
            ):
                with self.subTest(body=body):
                    response = self.client.post(
                        "/api/accounts",
                        headers=AUTH_HEADERS,
                        json=body,
                    )
                    self.assertEqual(response.status_code, 422, response.text)
            add_items.assert_not_called()
            add_accounts.assert_not_called()
            refresh_accounts.assert_not_called()

    def test_create_accepts_access_token_alias_ignores_token_field_and_refreshes_tokens_first(self) -> None:
        batches: list[list[dict]] = []
        refresh_calls: list[list[str]] = []

        def add_account_items(items: list[dict]) -> dict:
            batches.append(items)
            normalized_items = []
            for item in items:
                token = item.get("access_token")
                if not isinstance(token, str) or not token.strip():
                    token = item.get("accessToken")
                if not isinstance(token, str) or not token.strip():
                    continue
                normalized_items.append(
                    {"access_token": token.strip(), "type": "free", "status": "正常"}
                )
            return {
                "added": len(items),
                "skipped": 0,
                "items": normalized_items,
            }

        def refresh_accounts(tokens: list[str]) -> dict:
            refresh_calls.append(tokens)
            return {"refreshed": 0, "errors": [], "items": []}

        with (
            mock.patch.object(accounts_module.account_service, "add_account_items", side_effect=add_account_items),
            mock.patch.object(accounts_module.account_service, "refresh_accounts", side_effect=refresh_accounts),
            mock.patch.object(
                accounts_module.account_service,
                "add_accounts",
                side_effect=AssertionError("mixed create must use one account-item batch"),
            ),
        ):
            response = self.client.post(
                "/api/accounts",
                headers=AUTH_HEADERS,
                json={
                    "tokens": ["token-extra", "token-shared", "token-extra"],
                    "accounts": [
                        {"access_token": "token-shared", "type": "free"},
                        {"accessToken": "token-alias", "type": "plus"},
                        {"token": "must-be-ignored", "type": "pro"},
                    ],
                },
            )

        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(
            [item.get("access_token") for item in batches[0]],
            ["token-shared", None, None, "token-extra"],
        )
        self.assertEqual(batches[0][1]["accessToken"], "token-alias")
        self.assertEqual(batches[0][2]["token"], "must-be-ignored")
        self.assertEqual(refresh_calls, [["token-extra", "token-shared", "token-alias"]])

    def test_create_rejects_container_account_token_before_service_calls(self) -> None:
        canary = "api-account-token-container-canary"
        with (
            mock.patch.object(accounts_module.account_service, "add_account_items") as add_items,
            mock.patch.object(accounts_module.account_service, "add_accounts") as add_accounts,
            mock.patch.object(accounts_module.account_service, "refresh_accounts") as refresh_accounts,
        ):
            response = self.client.post(
                "/api/accounts",
                headers=AUTH_HEADERS,
                json={"accounts": [{"access_token": {"secret": canary}}]},
            )

        self.assertEqual(response.status_code, 400, response.text)
        self.assertNotIn(canary, response.text)
        add_items.assert_not_called()
        add_accounts.assert_not_called()
        refresh_accounts.assert_not_called()

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

    def test_relogin_progress_projects_result_entries(self) -> None:
        canary = "relogin-result-container-canary"
        progress = {
            "total": 1,
            "processed": 1,
            "done": True,
            "error": None,
            "results": [{
                "token": "token:0123456789",
                "status": "成功",
                "error": None,
                "account": {"secret": canary},
                "internal_metadata": canary,
            }],
        }
        with mock.patch.object(accounts_module.account_service, "get_relogin_progress", return_value=progress):
            response = self.client.get("/api/accounts/re-login/progress/progress-3", headers=AUTH_HEADERS)

        self.assertEqual(response.status_code, 200, response.text)
        self.assertNotIn(canary, response.text)
        self.assertEqual(response.json()["results"], [{
            "token": "token:0123456789",
            "status": "成功",
        }])

    def test_progress_result_entries_reanonymize_token_and_bound_status_error(self) -> None:
        secret = "raw-progress-access-token"
        progress = {
            "total": 1,
            "processed": 1,
            "done": True,
            "result": {
                "error": "nested-secret-error",
                "errors": [{"token": secret, "error": "nested-error-canary"}],
            },
            "results": [{
                "token": secret,
                "status": "untrusted-status-canary",
                "error": "untrusted-error-canary",
                "metadata": secret,
            }, {
                "token": {"secret": secret},
                "status": [secret],
                "error": {"secret": secret},
            }],
        }
        with mock.patch.object(accounts_module.account_service, "get_relogin_progress", return_value=progress):
            response = self.client.get("/api/accounts/re-login/progress/progress-5", headers=AUTH_HEADERS)

        self.assertEqual(response.status_code, 200, response.text)
        self.assertNotIn(secret, response.text)
        self.assertNotIn("untrusted-status-canary", response.text)
        self.assertNotIn("untrusted-error-canary", response.text)
        self.assertEqual(response.json()["results"], [{
            "token": anonymize_token(secret),
            "status": "失败",
            "error": "account operation failed",
        }])

    def test_progress_drops_non_object_nested_result(self) -> None:
        canary = "nested-result-container-canary@example.test"
        progress = {
            "total": 1,
            "processed": 1,
            "done": True,
            "result": [canary, {"secret": canary}],
        }
        with mock.patch.object(accounts_module.account_service, "get_refresh_progress", return_value=progress):
            response = self.client.get(
                "/api/accounts/refresh/progress/non-object-result",
                headers=AUTH_HEADERS,
            )

        self.assertEqual(response.status_code, 200, response.text)
        self.assertNotIn(canary, response.text)
        self.assertNotIn("result", response.json())

    def test_account_progress_projects_counters_and_errors(self) -> None:
        canary = "account-progress-container-canary"
        progress = {
            "total": 1,
            "processed": 1,
            "done": True,
            "status_counts": {"正常": 1, "internal": {"secret": canary}},
            "total_quota": {"secret": canary},
            "errors": [{"token": "token:0123456789", "error": canary, "internal": canary}],
        }
        with mock.patch.object(accounts_module.account_service, "get_refresh_progress", return_value=progress):
            response = self.client.get("/api/accounts/refresh/progress/progress-4", headers=AUTH_HEADERS)

        self.assertEqual(response.status_code, 200, response.text)
        self.assertNotIn(canary, response.text)
        self.assertEqual(response.json()["status_counts"], {"正常": 1})
        self.assertNotIn("total_quota", response.json())
        self.assertEqual(response.json()["errors"], [{
            "token": "token:0123456789",
            "error": "account operation failed",
        }])

    def test_progress_status_counts_drop_unknown_status_keys(self) -> None:
        canary = "status-count-canary@example.test"
        progress = {
            "total": 1,
            "processed": 1,
            "done": True,
            "status_counts": {
                "正常": 1,
                canary: 7,
                "异常": {"secret": canary},
            },
        }
        with mock.patch.object(accounts_module.account_service, "get_refresh_progress", return_value=progress):
            response = self.client.get("/api/accounts/refresh/progress/status-counts", headers=AUTH_HEADERS)

        self.assertEqual(response.status_code, 200, response.text)
        self.assertNotIn(canary, response.text)
        self.assertEqual(response.json()["status_counts"], {"正常": 1})

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

    def test_update_rejects_boolean_quota_before_service_call(self) -> None:
        with (
            mock.patch.object(accounts_module, "require_admin_async", new=AsyncMock()),
            mock.patch.object(accounts_module.account_service, "update_account") as update_account,
        ):
            response = self.client.post(
                "/api/accounts/update",
                headers=AUTH_HEADERS,
                json={"access_token": ACCESS_TOKEN, "quota": True},
            )

        self.assertEqual(response.status_code, 422, response.text)
        update_account.assert_not_called()

    def test_update_user_key_rejects_string_enabled_before_service_call(self) -> None:
        with (
            mock.patch.object(accounts_module, "require_admin_async", new=AsyncMock()),
            mock.patch.object(accounts_module.auth_service, "update_key") as update_key,
        ):
            response = self.client.post(
                "/api/auth/users/key-1",
                headers=AUTH_HEADERS,
                json={"enabled": "false"},
            )

        self.assertEqual(response.status_code, 422, response.text)
        update_key.assert_not_called()

    def test_oauth_finish_projects_account_items(self) -> None:
        oauth_tokens = {
            "access_token": ACCESS_TOKEN,
            "refresh_token": "oauth-refresh-token",
            "id_token": "oauth-id-token",
        }
        with (
            mock.patch.object(accounts_module.oauth_login_service, "finish", return_value=oauth_tokens),
            mock.patch.object(accounts_module.oauth_login_service, "commit_finish"),
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
