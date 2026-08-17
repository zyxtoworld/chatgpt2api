from __future__ import annotations

import json
import unittest
from unittest import mock

from fastapi import FastAPI
from fastapi.testclient import TestClient

import api.ai as ai_module
from services.model_service import ModelCatalogService
from services.openai_backend_api import OpenAIBackendAPI


class ModelPublicProjectionContractTests(unittest.TestCase):
    def test_models_route_uses_partial_catalog_without_waiting_for_cold_scan(self) -> None:
        calls: list[bool] = []

        class ColdCatalog:
            def list_models(self, *, wait_for_cold: bool = True) -> dict[str, object]:
                calls.append(wait_for_cold)
                if wait_for_cold:
                    raise RuntimeError("full catalog scan is still warming up")
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

        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json(), {"object": "list", "data": []})
        self.assertEqual(calls, [False])

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
