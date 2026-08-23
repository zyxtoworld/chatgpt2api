from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from fastapi import FastAPI
from fastapi.testclient import TestClient

import api.ai as ai_module
from services.account_service import AccountService
from services.model_service import ModelCatalogPendingError, ModelCatalogService
from services.openai_backend_api import InvalidAccessTokenError, OpenAIBackendAPI
from services.storage.json_storage import JSONStorageBackend


class ModelPublicProjectionContractTests(unittest.TestCase):
    def test_models_route_drops_invalid_pro_representative_without_blocking_free(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            accounts = AccountService(
                JSONStorageBackend(Path(temp_dir) / "accounts.json")
            )
            accounts.add_account_items([
                {
                    "access_token": "free-live",
                    "type": "free",
                    "source_type": "codex",
                    "status": "正常",
                },
                {
                    "access_token": "pro-invalid",
                    "type": "Pro",
                    "source_type": "codex",
                    "status": "正常",
                },
            ])
            accounts.refresh_access_token = lambda token, **_kwargs: token

            def model_list(model_id: str) -> dict[str, object]:
                return {
                    "object": "list",
                    "data": [{
                        "id": model_id,
                        "object": "model",
                        "created": 0,
                        "owned_by": "chatgpt",
                        "permission": [],
                        "root": model_id,
                        "parent": None,
                    }],
                }

            class Backend:
                def __init__(self, access_token: str = "") -> None:
                    self.access_token = access_token

                def list_models(self, **_kwargs: object) -> dict[str, object]:
                    if self.access_token == "pro-invalid":
                        raise InvalidAccessTokenError("fixture token invalid")
                    if self.access_token == "free-live":
                        return model_list("free-live-model")
                    return model_list("anonymous-model")

                def close(self) -> None:
                    pass

            catalog = ModelCatalogService(accounts, backend_factory=Backend)
            app = FastAPI()
            app.include_router(ai_module.create_router())

            with (
                mock.patch.object(
                    ai_module,
                    "require_identity_async",
                    new=mock.AsyncMock(return_value="test-identity"),
                ),
                mock.patch.object(ai_module.openai_v1_models, "model_catalog_service", catalog),
                mock.patch.object(ai_module.openai_v1_models, "account_service", accounts),
            ):
                response = TestClient(app, raise_server_exceptions=False).get(
                    "/v1/models",
                    headers={"Authorization": "Bearer test-key"},
                )

            self.assertEqual(response.status_code, 200, response.text)
            model_ids = {item["id"] for item in response.json()["data"]}
            self.assertIn("free-live-model", model_ids)
            self.assertIn("anonymous-model", model_ids)
            self.assertNotIn("pro-invalid-model", model_ids)
            self.assertTrue(all(
                "pro" not in item.get("supported_account_types", [])
                for item in response.json()["data"]
            ))
            # A failed representative closes only that account type's model
            # capability.  It must remain eligible for bounded retry/fallback
            # and must not mutate the live account into an invalid state.
            self.assertEqual(
                catalog._active_accounts_by_group()["Pro"],
                ["pro-invalid"],
            )
            self.assertEqual(
                catalog.route_for_model("free-live-model").access_tokens,
                frozenset({"free-live"}),
            )

    def test_models_route_returns_pending_for_cold_catalog_without_ready_snapshot(self) -> None:
        calls: list[bool] = []

        class ColdCatalog:
            def list_models(self, *, wait_for_cold: bool = True) -> dict[str, object]:
                calls.append(wait_for_cold)
                if wait_for_cold:
                    raise ModelCatalogPendingError("full catalog scan is still warming up")
                return {"object": "list", "data": []}

        app = FastAPI()
        app.include_router(ai_module.create_router())

        with (
            mock.patch.object(
                ai_module,
                "require_identity_async",
                new=mock.AsyncMock(return_value="test-identity"),
            ),
            mock.patch.object(ai_module.openai_v1_models, "model_catalog_service", ColdCatalog()),
            mock.patch.object(ai_module.openai_v1_models.account_service, "list_accounts", return_value=[]),
        ):
            response = TestClient(app, raise_server_exceptions=False).get(
                "/v1/models",
                headers={"Authorization": "Bearer test-key"},
            )

        self.assertEqual(response.status_code, 503, response.text)
        self.assertEqual(response.json()["error"]["code"], "upstream_error")
        self.assertIn("warming up", response.json()["error"]["message"])
        self.assertEqual(response.headers.get("retry-after"), "5")
        self.assertEqual(calls, [True, False])

    def test_backend_catalog_route_rejects_container_model_fields(self) -> None:
        secret = "backend-model-container-canary"

        class StreamingResponse:
            status_code = 200

            def __init__(self) -> None:
                self.closed = False

            def iter_content(self, chunk_size=None):
                yield json.dumps({
                    "models": [
                        {"slug": {"secret": secret}, "owned_by": "chatgpt"},
                        {"slug": "gpt-valid", "owned_by": [secret]},
                    ]
                }).encode("utf-8")

            def close(self) -> None:
                self.closed = True

        upstream_response = StreamingResponse()
        backend = object.__new__(OpenAIBackendAPI)
        backend.access_token = ""
        backend.base_url = "https://chatgpt.com"
        backend.session = mock.Mock()
        backend.session.get.return_value = upstream_response
        backend._bootstrap = mock.Mock()
        backend._headers = mock.Mock(return_value={})
        backend._search_remaining = mock.Mock(return_value=30.0)
        backend.close = mock.Mock()

        class EmptyAccounts:
            def list_accounts(self) -> list[dict[str, object]]:
                return []

            def _normalize_account_type(self, value: object) -> str:
                return str(value or "")

        catalog = ModelCatalogService(
            EmptyAccounts(),
            backend_factory=lambda **_: backend,
            cache_ttl_seconds=300,
        )
        app = FastAPI()
        app.include_router(ai_module.create_router())

        with (
            mock.patch.object(
                ai_module,
                "require_identity_async",
                new=mock.AsyncMock(return_value="test-identity"),
            ),
            mock.patch.object(
                ai_module.openai_v1_models,
                "model_catalog_service",
                catalog,
            ),
        ):
            response = TestClient(app, raise_server_exceptions=False).get(
                "/v1/models",
                headers={"Authorization": "Bearer test-key"},
            )

        self.assertEqual(response.status_code, 200, response.text)
        self.assertTrue(catalog._refresh_done.wait(timeout=5))

        with (
            mock.patch.object(
                ai_module,
                "require_identity_async",
                new=mock.AsyncMock(return_value="test-identity"),
            ),
            mock.patch.object(
                ai_module.openai_v1_models,
                "model_catalog_service",
                catalog,
            ),
        ):
            response = TestClient(app, raise_server_exceptions=False).get(
                "/v1/models",
                headers={"Authorization": "Bearer test-key"},
            )

        self.assertEqual(response.status_code, 200, response.text)
        self.assertNotIn(secret, response.text)
        self.assertTrue(upstream_response.closed)
        self.assertEqual([item["id"] for item in response.json()["data"]], ["gpt-valid"])
        self.assertEqual(response.json()["data"][0]["owned_by"], "chatgpt")

    def test_models_route_drops_unknown_fields_and_normalizes_malformed_schema(self) -> None:
        secret = "model-catalog-access-token-canary"
        catalog = {
            "object": "list",
            "internal_response_metadata": {"secret": secret},
            "data": [{
                "id": "gpt-public",
                "object": "model",
                "created": "not-a-timestamp",
                "owned_by": {"secret": secret},
                "permission": {"secret": secret},
                "root": [secret],
                "parent": {"secret": secret},
                "supported_reasoning_efforts": ["extended", {"secret": secret}],
                "access_token": secret,
                "internal_metadata": {"secret": secret},
            }],
        }
        app = FastAPI()
        app.include_router(ai_module.create_router())

        with (
            mock.patch.object(
                ai_module,
                "require_identity_async",
                new=mock.AsyncMock(return_value="test-identity"),
            ),
            mock.patch.object(
                ai_module.openai_v1_models.model_catalog_service,
                "list_models",
                return_value=catalog,
            ),
        ):
            response = TestClient(app, raise_server_exceptions=False).get(
                "/v1/models",
                headers={"Authorization": "Bearer test-key"},
            )

        self.assertEqual(response.status_code, 200, response.text)
        self.assertNotIn(secret, response.text)
        model = response.json()["data"][0]
        self.assertEqual(model["id"], "gpt-public")
        self.assertEqual(model["object"], "model")
        self.assertEqual(model["created"], 0)
        self.assertEqual(model["owned_by"], "chatgpt")
        self.assertEqual(model["permission"], [])
        self.assertEqual(model["root"], "gpt-public")
        self.assertIsNone(model["parent"])
        self.assertEqual(model["supported_reasoning_efforts"], ["extended"])
        self.assertNotIn("access_token", model)
        self.assertNotIn("internal_metadata", model)
        self.assertNotIn("internal_response_metadata", response.json())


if __name__ == "__main__":
    unittest.main()
