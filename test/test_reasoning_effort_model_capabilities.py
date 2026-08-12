from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest import mock

from services.account_service import AccountService
from services.model_service import ModelCatalogService
from services.openai_backend_api import OpenAIBackendAPI
from services.protocol import openai_v1_chat_complete, openai_v1_response
from services.protocol.chat_completion_cache import chat_completion_cache
from services.protocol.reasoning_effort import normalize_conversation_effort
from services.storage.json_storage import JSONStorageBackend


class UpstreamModelEffortContractTests(unittest.TestCase):
    def test_backend_model_list_preserves_only_normalized_effort_capabilities(self) -> None:
        response = mock.Mock(status_code=200)
        response.json.return_value = {
            "models": [
                {
                    "slug": "gpt-capable",
                    "thinking_efforts": [
                        {"thinking_effort": "min", "description": "private detail"},
                        {"thinking_effort": "standard"},
                        {"thinking_effort": "extended"},
                        {"thinking_effort": "max"},
                    ],
                    "opaque_upstream_field": "must-not-be-forwarded",
                }
            ]
        }
        backend = object.__new__(OpenAIBackendAPI)
        backend.access_token = "account-token"
        backend.base_url = "https://chatgpt.com"
        backend.session = mock.Mock()
        backend.session.get.return_value = response
        backend._bootstrap = mock.Mock()
        backend._headers = mock.Mock(return_value={})

        result = backend.list_models()

        self.assertEqual(
            result["data"][0]["supported_reasoning_efforts"],
            ["min", "standard", "extended", "max"],
        )
        self.assertNotIn("opaque_upstream_field", result["data"][0])

    def test_model_catalog_requests_full_account_catalog_without_changing_conversation_history_mode(self) -> None:
        limited_response = mock.Mock(status_code=200)
        limited_response.json.return_value = {"models": [{"slug": "gpt-basic"}]}
        full_response = mock.Mock(status_code=200)
        full_response.json.return_value = {
            "models": [
                {"slug": "gpt-basic"},
                {"slug": "gpt-pro-extra"},
            ],
        }
        backend = object.__new__(OpenAIBackendAPI)
        backend.access_token = "account-token"
        backend.base_url = "https://chatgpt.com"
        backend.session = mock.Mock()
        backend.session.get.side_effect = lambda url, **_kwargs: (
            full_response if url.endswith("history_and_training_disabled=false") else limited_response
        )
        backend._bootstrap = mock.Mock()
        backend._headers = mock.Mock(return_value={})

        with mock.patch(
            "services.model_service.model_catalog_service.normalize_reasoning_effort",
            return_value="",
        ):
            payload = backend._conversation_payload(
                [{"role": "user", "content": "hello"}],
                "gpt-capable",
                "Asia/Shanghai",
            )
        result = backend.list_models()

        self.assertIs(payload["history_and_training_disabled"], True)
        self.assertEqual(
            [item["id"] for item in result["data"]],
            ["gpt-basic", "gpt-pro-extra"],
        )
        self.assertEqual(
            backend.session.get.call_args.args[0],
            "https://chatgpt.com/backend-api/models?history_and_training_disabled=false",
        )

    def test_invalid_or_unsupported_effort_uses_selected_models_strongest_level(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            accounts = AccountService(
                JSONStorageBackend(Path(temp_dir) / "accounts.json")
            )
            accounts.add_account_items(
                [{"access_token": "pro-token", "type": "Pro", "status": "正常"}]
            )
            accounts.refresh_access_token = lambda token, **_kwargs: token

            class Backend:
                def __init__(self, access_token: str = "") -> None:
                    self.access_token = access_token

                def list_models(self) -> dict:
                    efforts = ["min", "standard"] if not self.access_token else [
                        "min",
                        "standard",
                        "extended",
                        "max",
                        "ultra",
                    ]
                    return {
                        "object": "list",
                        "data": [{
                            "id": "gpt-capable",
                            "supported_reasoning_efforts": efforts,
                        }],
                    }

                def close(self) -> None:
                    pass

            catalog = ModelCatalogService(
                accounts,
                backend_factory=Backend,
                cache_ttl_seconds=300,
            )

            self.assertEqual(
                catalog.normalize_reasoning_effort(
                    "gpt-capable",
                    "not-a-real-effort",
                    access_token="pro-token",
                ),
                "ultra",
            )
            self.assertEqual(
                catalog.normalize_reasoning_effort(
                    "gpt-capable",
                    "high",
                    access_token="pro-token",
                ),
                "ultra",
            )
            self.assertEqual(
                catalog.normalize_reasoning_effort(
                    "gpt-capable",
                    "extended",
                    access_token="pro-token",
                ),
                "extended",
            )
            self.assertEqual(
                catalog.normalize_reasoning_effort(
                    "gpt-capable",
                    "xhigh",
                    access_token="pro-token",
                ),
                "extended",
            )
            self.assertEqual(
                catalog.normalize_reasoning_effort(
                    "gpt-capable",
                    "minimal",
                    access_token="pro-token",
                ),
                "min",
            )
            self.assertEqual(
                catalog.normalize_reasoning_effort(
                    "gpt-capable",
                    "not-a-real-effort",
                    access_token="",
                ),
                "standard",
            )

            with mock.patch.object(
                catalog,
                "supported_reasoning_efforts",
                side_effect=AssertionError("auto must not refresh the upstream catalog"),
            ):
                self.assertEqual(
                    catalog.normalize_reasoning_effort(
                        "gpt-capable",
                        "auto",
                        access_token="pro-token",
                    ),
                    "",
                )

    def test_unknown_public_effort_survives_until_model_capability_resolution(self) -> None:
        self.assertEqual(
            normalize_conversation_effort("model-defined-effort"),
            "model-defined-effort",
        )

    def test_conversation_payload_resolves_effort_for_selected_model_and_account(self) -> None:
        backend = object.__new__(OpenAIBackendAPI)
        backend.access_token = "selected-account-token"
        with mock.patch(
            "services.model_service.model_catalog_service.normalize_reasoning_effort",
            return_value="max",
        ) as normalize:
            payload = backend._conversation_payload(
                [{"role": "user", "content": "hello"}],
                "gpt-capable",
                "Asia/Shanghai",
                thinking_effort="wrong-for-this-model",
            )

        self.assertEqual(payload["thinking_effort"], "max")
        normalize.assert_called_once_with(
            "gpt-capable",
            "wrong-for-this-model",
            access_token="selected-account-token",
        )

    def test_chat_and_responses_defer_unknown_effort_to_selected_backend(self) -> None:
        captured: list[str] = []
        chat_completion_cache.clear()

        def collect_text(_backend, request) -> str:
            captured.append(request.thinking_effort)
            return "ok"

        def stream_text(_backend, request):
            captured.append(request.thinking_effort)
            yield "ok"

        with (
            mock.patch.object(openai_v1_chat_complete, "text_backend", return_value=object()),
            mock.patch.object(openai_v1_chat_complete, "collect_text", side_effect=collect_text),
        ):
            openai_v1_chat_complete.handle({
                "model": "gpt-capable",
                "messages": [{"role": "user", "content": "chat effort fallback"}],
                "reasoning_effort": "wrong-for-this-model",
            })

        with (
            mock.patch.object(openai_v1_response, "text_backend", return_value=object()),
            mock.patch.object(openai_v1_response, "stream_text_deltas", side_effect=stream_text),
        ):
            openai_v1_response.handle({
                "model": "gpt-capable",
                "input": "response effort fallback",
                "reasoning": {"effort": "wrong-for-this-model"},
            })

        self.assertEqual(captured, ["wrong-for-this-model", "wrong-for-this-model"])


if __name__ == "__main__":
    unittest.main()
