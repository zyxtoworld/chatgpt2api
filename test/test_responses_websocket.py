from __future__ import annotations

import asyncio
import gc
import json
import tempfile
import threading
import unittest
import uuid
import weakref
from pathlib import Path
from unittest import mock

from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect
from websockets.datastructures import Headers
from websockets.exceptions import InvalidStatus
from websockets.http11 import Response

import api.ai as ai_module
import services.protocol.responses_websocket as responses_websocket_module
from services.account_service import AccountService
from services.model_service import ModelCatalogPendingError
from services.protocol.responses_websocket import (
    CodexResponsesWebSocketProtocolError,
    CodexResponsesWebSocketTransport,
    CodexResponsesWebSocketUnavailable,
    ResponsesWebSocketRequestError,
    ResponsesWebSocketSession,
)
from services.storage.json_storage import JSONStorageBackend


class _FakeUpstreamWebSocket:
    def __init__(self) -> None:
        self.closed = False
        self.sent: list[dict[str, object]] = []
        self._events: list[str] = []

    def send(self, message: str) -> None:
        request = json.loads(message)
        self.sent.append(request)
        response_id = f"resp_{len(self.sent)}"
        self._events.extend(
            [
                json.dumps({"type": "response.created", "response": {"id": response_id}}),
                json.dumps(
                    {
                        "type": "response.completed",
                        "response": {
                            "id": response_id,
                            "status": "completed",
                            "output": [
                                {
                                    "type": "message",
                                    "role": "assistant",
                                    "content": [
                                        {
                                            "type": "output_text",
                                            "text": f"reply-{len(self.sent)}",
                                            "annotations": [],
                                        }
                                    ],
                                }
                            ],
                        },
                    }
                ),
            ]
        )

    def recv(self, timeout: float | None = None) -> str:
        del timeout
        return self._events.pop(0)

    def close(self) -> None:
        self.closed = True


class ResponsesWebSocketContractTests(unittest.TestCase):
    def test_late_websocket_usage_does_not_mutate_replaced_same_token_account(self) -> None:
        entered = threading.Event()
        release = threading.Event()

        class BlockingConnection:
            def __init__(self) -> None:
                self.events = [
                    json.dumps({"type": "response.created", "response": {"id": "resp_1"}}),
                    json.dumps(
                        {
                            "type": "response.completed",
                            "response": {
                                "id": "resp_1",
                                "status": "completed",
                                "output": [],
                            },
                        }
                    ),
                ]
                self.closed = False

            def send(self, _message: str) -> None:
                pass

            def recv(self, timeout: float | None = None) -> str:
                del timeout
                if len(self.events) == 1:
                    entered.set()
                    if not release.wait(5):
                        raise AssertionError("websocket did not receive release")
                return self.events.pop(0)

            def close(self) -> None:
                self.closed = True

        with tempfile.TemporaryDirectory() as temp_dir:
            service = AccountService(JSONStorageBackend(Path(temp_dir) / "accounts.json"))
            service.add_account_items([
                {
                    "access_token": "websocket-token",
                    "type": "Pro",
                    "source_type": "codex",
                    "account_id": "account-1",
                    "status": "正常",
                },
            ])
            service.get_text_access_token = lambda **_kwargs: "websocket-token"
            connection = BlockingConnection()
            transport = CodexResponsesWebSocketTransport(
                connector=lambda *_args, **_kwargs: connection,
            )
            turn = ResponsesWebSocketSession().prepare_turn(
                {"type": "response.create", "model": "auto", "input": "hello"}
            )
            result: list[dict[str, object]] = []
            errors: list[BaseException] = []

            def consume() -> None:
                try:
                    result.extend(transport.events(turn))
                except BaseException as exc:
                    errors.append(exc)

            with (
                mock.patch.object(responses_websocket_module, "account_service", service),
                mock.patch.object(
                    responses_websocket_module.proxy_settings,
                    "get_profile",
                    return_value=mock.Mock(proxy_url=""),
                ),
                mock.patch.object(responses_websocket_module, "resolve_codex_reasoning_effort"),
            ):
                worker = threading.Thread(target=consume)
                worker.start()
                self.assertTrue(entered.wait(5), errors)
                service.update_account(
                    "websocket-token",
                    {"last_used_at": "2000-01-01 00:00:00", "success": 99},
                )
                release.set()
                worker.join(5)

            self.assertFalse(worker.is_alive())
            self.assertEqual(errors, [])
            self.assertEqual([event["type"] for event in result], ["response.created", "response.completed"])
            current = service.get_account("websocket-token")
            self.assertIsNotNone(current)
            self.assertEqual(current["last_used_at"], "2000-01-01 00:00:00")
            self.assertEqual(current["success"], 99)
            transport.close()
            self.assertTrue(connection.closed)

    def test_websocket_lease_capture_failure_does_not_create_connection(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = AccountService(JSONStorageBackend(Path(temp_dir) / "accounts.json"))
            service.add_account_items([{"access_token": "websocket-token", "type": "Pro", "status": "正常"}])
            service.get_text_access_token = lambda **_kwargs: "websocket-token"
            connector = mock.Mock()
            transport = CodexResponsesWebSocketTransport(connector=connector)
            turn = ResponsesWebSocketSession().prepare_turn(
                {"type": "response.create", "model": "auto", "input": "hello"}
            )
            with (
                mock.patch.object(
                    service,
                    "_get_account_lease",
                    side_effect=RuntimeError("lease capture failed"),
                ),
                mock.patch.object(responses_websocket_module, "account_service", service),
                self.assertRaisesRegex(RuntimeError, "lease capture failed"),
            ):
                list(transport.events(turn))

            connector.assert_not_called()
    def test_public_event_rejects_container_event_type_without_raw_type_error(self) -> None:
        canary = "responses-event-type-container-canary owner@example.com"
        event = {
            "type": {"secret": canary},
            "response": {
                "id": "resp-safe",
                "status": "completed",
            },
        }

        with self.assertRaisesRegex(RuntimeError, "malformed public response event") as raised:
            responses_websocket_module.project_public_codex_response_event(event)

        self.assertNotIn(canary, str(raised.exception))

    def test_public_event_projection_drops_container_for_scalar_event_fields(self) -> None:
        canary = "responses-scalar-container-canary owner@example.com"
        event = {
            "type": "response.output_text.delta",
            "item_id": {"text": canary},
            "delta": [canary],
            "response": {
                "id": "resp-safe",
                "created_at": {"text": canary},
                "usage": {"input_tokens": {"text": canary}},
            },
        }

        with self.assertRaisesRegex(RuntimeError, "malformed public response event"):
            responses_websocket_module.project_public_codex_response_event(event)

    def test_public_event_projection_rejects_wrong_scalar_primitives(self) -> None:
        cases = [
            {"type": "response.output_text.delta", "delta": 123},
            {
                "type": "response.completed",
                "response": {
                    "id": "resp-safe",
                    "status": "completed",
                    "usage": {"input_tokens": "12", "output_tokens": 1, "total_tokens": 13},
                },
            },
            {
                "type": "response.completed",
                "response": {
                    "id": "resp-safe",
                    "status": "completed",
                    "parallel_tool_calls": 1,
                },
            },
        ]
        for event in cases:
            with self.subTest(event=event):
                with self.assertRaisesRegex(RuntimeError, "malformed"):
                    responses_websocket_module.project_public_codex_response_event(event)

    def test_public_event_rejects_malformed_incomplete_details(self) -> None:
        canary = "responses-incomplete-detail-canary owner@example.test"
        cases = [
            {
                "type": "response.incomplete",
                "response": {
                    "id": "resp-safe",
                    "status": "incomplete",
                    "incomplete_details": canary,
                },
            },
            {
                "type": "response.incomplete",
                "response": {
                    "id": "resp-safe",
                    "status": "incomplete",
                    "incomplete_details": {"reason": {"secret": canary}},
                },
            },
        ]
        for event in cases:
            with self.subTest(event=event):
                with self.assertRaisesRegex(RuntimeError, "malformed") as raised:
                    responses_websocket_module.project_public_codex_response_event(event)
                self.assertNotIn(canary, str(raised.exception))

    def test_public_event_projects_incomplete_reason_only(self) -> None:
        event = {
            "type": "response.incomplete",
            "response": {
                "id": "resp-safe",
                "status": "incomplete",
                "incomplete_details": {
                    "reason": "content_filter",
                    "internal_detail": "dropped",
                },
            },
        }

        projected = responses_websocket_module.project_public_codex_response_event(event)

        self.assertEqual(projected["response"]["incomplete_details"], {"reason": "content_filter"})
        self.assertNotIn("internal_detail", json.dumps(projected))

    def test_public_event_normalizes_lifecycle_and_preserves_nullable_fields(self) -> None:
        completed = responses_websocket_module.project_public_codex_response_event({
            "type": "response.completed",
            "response": {
                "id": "resp-completed-nullable",
                "error": None,
                "incomplete_details": None,
                "usage": None,
                "output": [],
            },
        })
        self.assertEqual(completed["response"]["status"], "completed")
        self.assertIsNone(completed["response"]["error"])
        self.assertIsNone(completed["response"]["incomplete_details"])
        self.assertIsNone(completed["response"]["usage"])

        incomplete = responses_websocket_module.project_public_codex_response_event({
            "type": "response.incomplete",
            "response": {
                "id": "resp-incomplete-nullable",
                "incomplete_details": {},
                "usage": None,
                "output": [],
            },
        })
        self.assertEqual(incomplete["response"]["status"], "incomplete")
        self.assertEqual(incomplete["response"]["incomplete_details"], {})
        self.assertIsNone(incomplete["response"]["usage"])

        active = responses_websocket_module.project_public_codex_response_event({
            "type": "response.created",
            "response": {
                "id": "resp-active-nullable",
                "error": None,
                "incomplete_details": None,
                "usage": None,
                "output": [],
            },
        })
        self.assertNotIn("status", active["response"])
        self.assertIsNone(active["response"]["error"])
        self.assertIsNone(active["response"]["incomplete_details"])
        self.assertIsNone(active["response"]["usage"])

        malformed = [
            {
                "type": "response.created",
                "response": {"id": "resp-active-status", "status": "completed"},
            },
            {
                "type": "response.created",
                "response": {
                    "id": "resp-active-error",
                    "error": {"type": "internal", "message": "private"},
                },
            },
            {
                "type": "response.created",
                "response": {
                    "id": "resp-active-incomplete-details",
                    "incomplete_details": {"reason": "content_filter"},
                },
            },
            {
                "type": "response.completed",
                "response": {
                    "id": "resp-terminal-error",
                    "error": {"type": "internal", "message": "private"},
                },
            },
            {
                "type": "response.completed",
                "response": {"id": "resp-terminal-status", "status": "incomplete"},
            },
        ]
        for event in malformed:
            with self.subTest(event=event):
                with self.assertRaisesRegex(RuntimeError, "malformed"):
                    responses_websocket_module.project_public_codex_response_event(event)

    def test_public_event_rejects_scalar_error_and_output_items(self) -> None:
        canary = "responses-error-output-canary owner@example.test"
        cases = [
            {
                "type": "response.failed",
                "response": {
                    "id": "resp-safe",
                    "status": "failed",
                    "error": canary,
                },
            },
            {
                "type": "response.completed",
                "response": {
                    "id": "resp-safe",
                    "status": "completed",
                    "output": [canary],
                },
            },
            {
                "type": "response.completed",
                "response": {
                    "id": "resp-safe",
                    "status": "completed",
                    "output": [{
                        "type": "message",
                        "content": [canary],
                    }],
                },
            },
        ]

        for event in cases:
            with self.subTest(event=event):
                with self.assertRaisesRegex(RuntimeError, "malformed"):
                    responses_websocket_module.project_public_codex_response_event(event)

    def test_public_event_projects_content_and_annotation_object_arrays(self) -> None:
        event = {
            "type": "response.completed",
            "response": {
                "id": "resp-safe",
                "status": "completed",
                "output": [{
                    "type": "message",
                    "role": "assistant",
                    "content": [{
                        "type": "output_text",
                        "text": "hello",
                        "annotations": [{
                            "type": "url_citation",
                            "url": "https://example.test",
                            "title": "Example",
                            "start_index": 0,
                            "end_index": 5,
                            "internal_secret": "dropped",
                        }],
                        "internal_secret": "dropped",
                    }],
                }],
            },
        }

        projected = responses_websocket_module.project_public_codex_response_event(event)

        self.assertEqual(
            projected["response"]["output"][0]["content"][0]["text"],
            "hello",
        )
        self.assertEqual(
            projected["response"]["output"][0]["content"][0]["annotations"][0]["url"],
            "https://example.test",
        )
        self.assertNotIn("internal_secret", json.dumps(projected))

    def test_public_event_uses_discriminated_item_projection(self) -> None:
        event = {
            "type": "response.completed",
            "response": {
                "id": "resp-discriminated",
                "status": "completed",
                "output": [
                    {
                        "type": "message",
                        "id": "message-1",
                        "role": "assistant",
                        "arguments": "message-arguments-private",
                        "result": "message-result-private",
                        "operation": {"type": "message-operation-private"},
                        "environment": {"type": "message-environment-private"},
                        "code": "message-code-private",
                        "content": [{"type": "output_text", "text": "hello"}],
                    },
                    {
                        "type": "shell_call",
                        "id": "shell-1",
                        "call_id": "shell-call-1",
                        "status": "completed",
                        "action": {"type": "shell", "query": "echo hi", "private": "action-private"},
                        "environment": {
                            "type": "container",
                            "text": "linux",
                            "private": "environment-private",
                        },
                        "operation": {"type": "wrong-item-private"},
                    },
                    {
                        "type": "computer_call",
                        "id": "computer-1",
                        "call_id": "computer-call-1",
                        "pending_safety_checks": [{
                            "type": "safety_check",
                            "code": "confirm",
                            "private": "safety-private",
                        }],
                        "status": "completed",
                    },
                    {
                        "type": "apply_patch_call",
                        "id": "patch-1",
                        "call_id": "patch-call-1",
                        "operation": {
                            "type": "patch",
                            "path": "README.md",
                            "patch": "@@ -1 +1 @@",
                            "private": "operation-private",
                        },
                        "status": "completed",
                    },
                    {
                        "type": "code_interpreter_call",
                        "id": "code-1",
                        "code": "print(1)",
                        "container_id": "container-1",
                        "outputs": [{"type": "logs", "text": "stdout", "private": "outputs-private"}],
                        "status": "completed",
                    },
                ],
            },
        }

        projected = responses_websocket_module.project_public_codex_response_event(event)
        output = projected["response"]["output"]
        self.assertEqual(output[0]["content"][0]["text"], "hello")
        for field in ("arguments", "result", "operation", "environment", "code"):
            self.assertNotIn(field, output[0])
        self.assertEqual(output[1]["environment"]["type"], "container")
        self.assertEqual(output[1]["environment"]["text"], "linux")
        self.assertNotIn("operation", output[1])
        self.assertEqual(output[2]["pending_safety_checks"][0]["code"], "confirm")
        self.assertEqual(output[3]["operation"]["path"], "README.md")
        self.assertEqual(output[3]["operation"]["patch"], "@@ -1 +1 @@")
        self.assertEqual(output[4]["outputs"][0]["text"], "stdout")
        rendered = json.dumps(projected)
        for canary in (
            "message-arguments-private",
            "message-result-private",
            "message-operation-private",
            "message-environment-private",
            "message-code-private",
            "action-private",
            "environment-private",
            "wrong-item-private",
            "safety-private",
            "operation-private",
            "outputs-private",
        ):
            self.assertNotIn(canary, rendered)

    def test_public_event_preserves_legal_tool_search_output_definitions(self) -> None:
        event = {
            "type": "response.completed",
            "response": {
                "id": "resp-tools",
                "status": "completed",
                "output": [{
                    "type": "tool_search_output",
                    "id": "tool-output-1",
                    "call_id": "search-1",
                    "status": "completed",
                    "execution": "client",
                    "tools": [{
                        "type": "function",
                        "name": "get_calendar",
                        "description": "Read calendar events",
                        "parameters": {
                            "type": "object",
                            "properties": {},
                            "internal_schema_secret": "drop-me-too",
                        },
                        "strict": True,
                        "defer_loading": False,
                        "internal_secret": "drop-me",
                    }],
                }],
            },
        }

        projected = responses_websocket_module.project_public_codex_response_event(event)

        tools = projected["response"]["output"][0]["tools"]
        self.assertEqual(tools[0]["type"], "function")
        self.assertEqual(tools[0]["name"], "get_calendar")
        self.assertEqual(tools[0]["description"], "Read calendar events")
        self.assertEqual(tools[0]["parameters"], {"type": "object", "properties": {}})
        self.assertTrue(tools[0]["strict"])
        self.assertFalse(tools[0]["defer_loading"])
        self.assertNotIn("internal_secret", json.dumps(projected))

    def test_public_event_rejects_untyped_tool_description_and_logprobs(self) -> None:
        canary = "responses-public-scalar-container-canary"
        cases = [
            {
                "type": "response.completed",
                "response": {
                    "id": "resp-tool-description",
                    "status": "completed",
                    "output": [{
                        "type": "tool_search_output",
                        "tools": [{"type": "function", "name": "lookup", "description": [canary]}],
                    }],
                },
            },
            {
                "type": "response.completed",
                "response": {
                    "id": "resp-logprobs",
                    "status": "completed",
                    "output": [{
                        "type": "message",
                        "content": [{
                            "type": "output_text",
                            "text": "answer",
                            "logprobs": [canary],
                        }],
                    }],
                },
            },
        ]

        for event in cases:
            with self.subTest(event=event):
                with self.assertRaisesRegex(RuntimeError, "malformed") as raised:
                    responses_websocket_module.project_public_codex_response_event(event)
                self.assertNotIn(canary, str(raised.exception))

    def test_public_event_projects_legal_logprobs(self) -> None:
        event = {
            "type": "response.completed",
            "response": {
                "id": "resp-logprobs-safe",
                "status": "completed",
                "output": [{
                    "type": "message",
                    "content": [{
                        "type": "output_text",
                        "text": "answer",
                        "logprobs": [{
                            "token": "answer",
                            "logprob": -0.25,
                            "bytes": [97, 110],
                            "top_logprobs": [],
                            "internal_secret": "drop-me",
                        }],
                    }],
                }],
            },
        }

        projected = responses_websocket_module.project_public_codex_response_event(event)
        self.assertEqual(
            projected["response"]["output"][0]["content"][0]["logprobs"],
            [{"token": "answer", "logprob": -0.25, "bytes": [97, 110], "top_logprobs": []}],
        )

    def test_public_event_rejects_non_string_web_search_queries(self) -> None:
        canary = "web-search-query-container-canary owner@example.com"
        event = {
            "type": "response.completed",
            "response": {
                "id": "resp-search",
                "status": "completed",
                "output": [{
                    "type": "web_search_call",
                    "id": "search-call-1",
                    "status": "completed",
                    "action": {
                        "type": "search",
                        "queries": [{"text": canary}],
                    },
                }],
            },
        }

        with self.assertRaisesRegex(RuntimeError, "malformed") as raised:
            responses_websocket_module.project_public_codex_response_event(event)
        self.assertNotIn(canary, str(raised.exception))

    def test_public_event_preserves_string_web_search_queries(self) -> None:
        event = {
            "type": "response.completed",
            "response": {
                "id": "resp-search-safe",
                "status": "completed",
                "output": [{
                    "type": "web_search_call",
                    "id": "search-call-safe",
                    "status": "completed",
                    "action": {
                        "type": "search",
                        "query": "latest release",
                        "queries": ["latest release", "project changelog"],
                    },
                }],
            },
        }

        projected = responses_websocket_module.project_public_codex_response_event(event)

        self.assertEqual(
            projected["response"]["output"][0]["action"]["queries"],
            ["latest release", "project changelog"],
        )

    def test_malformed_validation_error_detail_does_not_stringify_container(self) -> None:
        canary = "responses-validation-container-secret"
        validation_error = HTTPException(
            status_code=400,
            detail={"error": {"secret": canary}},
        )
        with mock.patch.object(
            responses_websocket_module,
            "validate_response_core_parameters",
            side_effect=validation_error,
        ):
            with self.assertRaises(ResponsesWebSocketRequestError) as raised:
                ResponsesWebSocketSession().prepare_turn({
                    "type": "response.create",
                    "model": "gpt-test",
                    "input": "hello",
                })

        self.assertEqual(raised.exception.message, "invalid Responses WebSocket request")
        self.assertNotIn(canary, raised.exception.message)

    def test_malformed_payload_error_detail_does_not_stringify_container(self) -> None:
        canary = "responses-payload-container-secret"
        validation_error = HTTPException(
            status_code=400,
            detail={"error": [canary]},
        )
        with mock.patch.object(
            responses_websocket_module,
            "codex_response_payload",
            side_effect=validation_error,
        ):
            with self.assertRaises(ResponsesWebSocketRequestError) as raised:
                ResponsesWebSocketSession().prepare_turn({
                    "type": "response.create",
                    "model": "gpt-test",
                    "input": "hello",
                })

        self.assertEqual(raised.exception.message, "invalid Responses WebSocket request")
        self.assertNotIn(canary, raised.exception.message)

    def test_context_management_selects_native_websocket_without_tools(self) -> None:
        terminal = {
            "type": "response.completed",
            "response": {"id": "resp_compacted", "status": "completed", "output": []},
        }
        transport = mock.Mock(is_connected=False)
        transport.events.return_value = iter([terminal])
        turn = ResponsesWebSocketSession().prepare_turn({
            "type": "response.create",
            "model": "gpt-5.3-codex",
            "input": "continue",
            "context_management": [{"type": "compaction", "compact_threshold": 200000}],
        })

        with mock.patch.object(
            ai_module.openai_v1_response,
            "response_events",
            side_effect=AssertionError("context_management must use the native websocket"),
        ) as http:
            events = list(ai_module._responses_websocket_turn_events(transport, turn))

        self.assertEqual(events, [terminal])
        transport.events.assert_called_once_with(turn)
        http.assert_not_called()

    def test_native_codex_websocket_forwards_context_management(self) -> None:
        connection = _FakeUpstreamWebSocket()
        context_management = [{"type": "compaction", "compact_threshold": 200000}]
        session = ResponsesWebSocketSession()
        transport = CodexResponsesWebSocketTransport(connector=lambda _uri, **_kwargs: connection)

        with (
            mock.patch.object(
                responses_websocket_module.account_service,
                "get_text_access_token",
                return_value="codex-access-token",
            ),
            mock.patch.object(
                responses_websocket_module.account_service,
                "get_account",
                return_value={"source_type": "codex", "access_token": "codex-access-token"},
            ),
            mock.patch.object(
                responses_websocket_module.proxy_settings,
                "get_profile",
                return_value=mock.Mock(proxy_url=""),
            ),
            mock.patch.object(responses_websocket_module.account_service, "mark_text_used"),
        ):
            turn = session.prepare_turn({
                "type": "response.create",
                "model": "gpt-5.3-codex",
                "input": "continue",
                "context_management": context_management,
            })
            list(transport.events(turn))

        self.assertEqual(connection.sent[0]["context_management"], context_management)
        transport.close()

    def test_native_codex_websocket_resolves_reasoning_effort_for_selected_account(self) -> None:
        connection = _FakeUpstreamWebSocket()
        session = ResponsesWebSocketSession()
        transport = CodexResponsesWebSocketTransport(connector=lambda _uri, **_kwargs: connection)

        with (
            mock.patch.object(
                responses_websocket_module.account_service,
                "get_text_access_token",
                return_value="selected-codex-token",
            ),
            mock.patch.object(
                responses_websocket_module.account_service,
                "get_account",
                return_value={"source_type": "codex", "access_token": "selected-codex-token"},
            ),
            mock.patch.object(
                responses_websocket_module.proxy_settings,
                "get_profile",
                return_value=mock.Mock(proxy_url=""),
            ),
            mock.patch.object(responses_websocket_module.account_service, "mark_text_used"),
            mock.patch(
                "services.model_service.model_catalog_service.normalize_reasoning_effort",
                return_value="max",
            ) as normalize,
        ):
            turn = session.prepare_turn({
                "type": "response.create",
                "model": "gpt-5.5",
                "input": "reason about this",
                "reasoning": {"effort": "wrong-for-this-model", "summary": "detailed"},
            })
            list(transport.events(turn))

        self.assertEqual(
            connection.sent[0]["reasoning"],
            {"effort": "max", "summary": "detailed"},
        )
        normalize.assert_called_once_with(
            "gpt-5.5",
            "wrong-for-this-model",
            access_token="selected-codex-token",
        )
        transport.close()

    def test_native_codex_warmup_forwards_generate_false_and_chains_response_id(self) -> None:
        connection = _FakeUpstreamWebSocket()
        stream_options = {"reasoning_summary_delivery": "sequential_cutoff"}
        session = ResponsesWebSocketSession()
        transport = CodexResponsesWebSocketTransport(connector=lambda _uri, **_kwargs: connection)

        with (
            mock.patch.object(
                responses_websocket_module.account_service,
                "get_text_access_token",
                return_value="codex-access-token",
            ),
            mock.patch.object(
                responses_websocket_module.account_service,
                "get_account",
                return_value={"source_type": "codex", "access_token": "codex-access-token"},
            ),
            mock.patch.object(
                responses_websocket_module.proxy_settings,
                "get_profile",
                return_value=mock.Mock(proxy_url=""),
            ),
            mock.patch.object(responses_websocket_module.account_service, "mark_text_used"),
        ):
            warmup = session.prepare_turn({
                "type": "response.create",
                "model": "gpt-5.5",
                "input": "prepare",
                "tools": [],
                "stream_options": stream_options,
                "generate": False,
            })
            warmup_events = list(transport.events(warmup))
            session.commit(warmup.replay_body, warmup_events[-1]["response"])

            turn = session.prepare_turn({
                "type": "response.create",
                "model": "gpt-5.5",
                "previous_response_id": "resp_1",
                "input": "run",
                "tools": [],
                "stream_options": stream_options,
            })
            list(transport.events(turn))

        self.assertIs(connection.sent[0]["generate"], False)
        self.assertEqual(connection.sent[0]["tools"], [])
        self.assertEqual(connection.sent[0]["stream_options"], stream_options)
        self.assertNotIn("generate", connection.sent[1])
        self.assertEqual(connection.sent[1]["previous_response_id"], "resp_1")
        self.assertEqual(connection.sent[1]["stream_options"], stream_options)
        transport.close()

    def test_websocket_warmup_and_follow_up_use_native_transport_without_tools(self) -> None:
        class TrackingTransport:
            def __init__(self) -> None:
                self.is_connected = False
                self.turns: list[PreparedResponsesWebSocketTurn] = []

            def events(self, turn: PreparedResponsesWebSocketTurn):
                self.turns.append(turn)
                self.is_connected = True
                yield {
                    "type": "response.completed",
                    "response": {
                        "id": f"resp_{len(self.turns)}",
                        "status": "completed",
                        "output": [],
                    },
                }

            def close(self) -> None:
                self.is_connected = False

        session = ResponsesWebSocketSession()
        transport = TrackingTransport()
        warmup = session.prepare_turn({
            "type": "response.create",
            "model": "gpt-5.5",
            "input": [],
            "tools": [],
            "generate": False,
        })
        with mock.patch.object(
            ai_module.openai_v1_response,
            "response_events",
            side_effect=AssertionError("warmup must not fall back to HTTP"),
        ) as http:
            warmup_events = list(ai_module._responses_websocket_turn_events(transport, warmup))
            session.commit(warmup.replay_body, warmup_events[-1]["response"])
            follow_up = session.prepare_turn({
                "type": "response.create",
                "model": "gpt-5.5",
                "previous_response_id": "resp_1",
                "input": [],
                "tools": [],
            })
            list(ai_module._responses_websocket_turn_events(transport, follow_up))

        self.assertEqual(transport.turns, [warmup, follow_up])
        http.assert_not_called()

    def test_websocket_rejects_generate_true_before_upstream_call(self) -> None:
        session = ResponsesWebSocketSession()

        for value in (True, 1, 0, "false", None):
            with self.subTest(value=value):
                with self.assertRaises(ResponsesWebSocketRequestError) as raised:
                    session.prepare_turn({
                        "type": "response.create",
                        "model": "gpt-5.5",
                        "input": [],
                        "tools": [],
                        "generate": value,
                    })
                self.assertEqual(raised.exception.code, "invalid_request_error")

    def test_native_codex_upstream_reuses_one_connection_for_two_turns(self) -> None:
        connection = _FakeUpstreamWebSocket()
        connect_calls: list[tuple[str, dict[str, object]]] = []

        def connect(uri: str, **kwargs):
            connect_calls.append((uri, kwargs))
            return connection

        tools = [
            {
                "type": "function",
                "name": "lookup",
                "description": "lookup a value",
                "parameters": {"type": "object", "properties": {}},
            }
        ]
        session = ResponsesWebSocketSession()
        transport = CodexResponsesWebSocketTransport(connector=connect)
        with (
            mock.patch.object(
                responses_websocket_module.account_service,
                "get_text_access_token",
                return_value="codex-access-token",
            ),
            mock.patch.object(
                responses_websocket_module.account_service,
                "get_account",
                return_value={
                    "source_type": "codex",
                    "account_id": "account-7",
                    "access_token": "codex-access-token",
                },
            ),
            mock.patch.object(
                responses_websocket_module.proxy_settings,
                "get_profile",
                return_value=mock.Mock(proxy_url=""),
            ),
            mock.patch.object(responses_websocket_module.account_service, "mark_text_used") as mark_used,
        ):
            first = session.prepare_turn(
                {"type": "response.create", "model": "gpt-5.5", "input": "first", "tools": tools}
            )
            first_events = list(transport.events(first))
            session.commit(first.replay_body, first_events[-1]["response"])

            second = session.prepare_turn(
                {
                    "type": "response.create",
                    "model": "gpt-5.5",
                    "previous_response_id": "resp_1",
                    "input": "second",
                    "tools": tools,
                }
            )
            second_events = list(transport.events(second))
            session.commit(second.replay_body, second_events[-1]["response"])

        self.assertEqual(len(connect_calls), 1)
        uri, kwargs = connect_calls[0]
        self.assertEqual(uri, "wss://chatgpt.com/backend-api/codex/responses")
        headers = kwargs["additional_headers"]
        self.assertEqual(headers["Authorization"], "Bearer codex-access-token")
        self.assertEqual(headers["ChatGPT-Account-ID"], "account-7")
        self.assertEqual(headers["Originator"], "codex-tui")
        self.assertEqual(headers["OpenAI-Beta"], "responses_websockets=2026-02-06")
        session_id = headers["session-id"]
        thread_id = headers["thread-id"]
        self.assertEqual(str(uuid.UUID(session_id)), session_id)
        self.assertEqual(str(uuid.UUID(thread_id)), thread_id)
        self.assertNotEqual(session_id, thread_id)
        self.assertNotIn("Session_id", headers)
        self.assertNotIn("Conversation_id", headers)
        self.assertNotIn("Content-Type", headers)
        self.assertNotIn("Accept", headers)
        self.assertEqual(connection.sent[0]["type"], "response.create")
        self.assertNotIn("previous_response_id", connection.sent[0])
        self.assertEqual(connection.sent[1]["type"], "response.create")
        self.assertEqual(connection.sent[1]["previous_response_id"], "resp_1")
        self.assertEqual(connection.sent[1]["input"][0]["content"][0]["text"], "second")
        self.assertNotIn("first", json.dumps(connection.sent[1]))
        self.assertEqual(mark_used.call_args_list, [mock.call("codex-access-token")] * 2)
        transport.close()
        self.assertTrue(connection.closed)

    def test_mark_text_used_failure_preserves_terminal_event_and_closes_connection(self) -> None:
        connection = _FakeUpstreamWebSocket()
        session = ResponsesWebSocketSession()
        transport = CodexResponsesWebSocketTransport(connector=lambda _uri, **_kwargs: connection)
        turn = session.prepare_turn(
            {"type": "response.create", "model": "gpt-5.5", "input": "hello"}
        )

        with (
            mock.patch.object(
                responses_websocket_module.account_service,
                "get_text_access_token",
                return_value="codex-access-token",
            ),
            mock.patch.object(
                responses_websocket_module.account_service,
                "get_account",
                return_value={"source_type": "codex", "account_id": "account-7"},
            ),
            mock.patch.object(
                responses_websocket_module.proxy_settings,
                "get_profile",
                return_value=mock.Mock(proxy_url=""),
            ),
            mock.patch.object(
                responses_websocket_module.account_service,
                "mark_text_used",
                side_effect=RuntimeError("telemetry failed"),
            ),
        ):
            events = list(transport.events(turn))

        self.assertEqual([event["type"] for event in events], ["response.created", "response.completed"])
        self.assertFalse(connection.closed)
        transport.close()
        self.assertTrue(connection.closed)

    def test_changed_request_properties_replay_full_transcript_without_cached_response_id(self) -> None:
        changed_properties = (
            ("model", "gpt-5.4"),
            ("instructions", "Use the changed instructions."),
            (
                "tools",
                [{"type": "function", "name": "lookup", "parameters": {"type": "object"}}],
            ),
            ("parallel_tool_calls", False),
            ("reasoning", {"effort": "high"}),
            (
                "include",
                ["reasoning.encrypted_content", "web_search_call.action.sources"],
            ),
            ("service_tier", "priority"),
            ("prompt_cache_key", "changed-cache-key"),
            (
                "text",
                {
                    "format": {
                        "type": "json_schema",
                        "name": "answer",
                        "schema": {"type": "object"},
                        "strict": True,
                    }
                },
            ),
            (
                "context_management",
                [{"type": "compaction", "compact_threshold": 200000}],
            ),
        )

        for field, value in changed_properties:
            with self.subTest(field=field):
                session = ResponsesWebSocketSession()
                first = session.prepare_turn({
                    "type": "response.create",
                    "model": "gpt-5.5",
                    "input": "first",
                })
                self.assertTrue(session.commit(
                    first.replay_body,
                    {
                        "id": "resp_1",
                        "output": [{
                            "type": "message",
                            "role": "assistant",
                            "content": [{"type": "output_text", "text": "reply"}],
                        }],
                    },
                ))

                follow_up = {
                    "type": "response.create",
                    "model": "gpt-5.5",
                    "previous_response_id": "resp_1",
                    "input": "second",
                    field: value,
                }
                turn = session.prepare_turn(follow_up)

                self.assertNotIn("previous_response_id", turn.incremental_body)
                self.assertEqual(turn.incremental_body, turn.replay_body)
                self.assertEqual(
                    [item.get("role") for item in turn.incremental_body["input"]],
                    ["user", "assistant", "user"],
                )

    def test_changed_request_properties_send_full_replay_on_reused_upstream_socket(self) -> None:
        connection = _FakeUpstreamWebSocket()
        session = ResponsesWebSocketSession()
        transport = CodexResponsesWebSocketTransport(
            connector=lambda _uri, **_kwargs: connection
        )
        with (
            mock.patch.object(
                responses_websocket_module.account_service,
                "get_text_access_token",
                return_value="codex-access-token",
            ),
            mock.patch.object(
                responses_websocket_module.account_service,
                "get_account",
                return_value={"source_type": "codex", "account_id": "account-7"},
            ),
            mock.patch.object(
                responses_websocket_module.proxy_settings,
                "get_profile",
                return_value=mock.Mock(proxy_url=""),
            ),
            mock.patch.object(responses_websocket_module.account_service, "mark_text_used"),
        ):
            first = session.prepare_turn({
                "type": "response.create",
                "model": "gpt-5.5",
                "instructions": "first instructions",
                "input": "first",
            })
            first_events = list(transport.events(first))
            self.assertTrue(session.commit(first.replay_body, first_events[-1]["response"]))

            second = session.prepare_turn({
                "type": "response.create",
                "model": "gpt-5.5",
                "instructions": "changed instructions",
                "previous_response_id": "resp_1",
                "input": "second",
            })
            list(transport.events(second))

        self.assertEqual(len(connection.sent), 2)
        self.assertNotIn("previous_response_id", connection.sent[1])
        self.assertEqual(connection.sent[1]["instructions"], "changed instructions")
        self.assertEqual(
            [item.get("role") for item in connection.sent[1]["input"]],
            ["user", "assistant", "user"],
        )
        self.assertEqual(connection.sent[1]["input"][0]["content"], "first")
        self.assertEqual(connection.sent[1]["input"][2]["content"], "second")

    def test_stream_options_change_keeps_cached_response_id(self) -> None:
        session = ResponsesWebSocketSession()
        first = session.prepare_turn({
            "type": "response.create",
            "model": "gpt-5.5",
            "input": "first",
        })
        self.assertTrue(session.commit(first.replay_body, {"id": "resp_1", "output": []}))

        turn = session.prepare_turn({
            "type": "response.create",
            "model": "gpt-5.5",
            "previous_response_id": "resp_1",
            "input": "second",
            "stream_options": {"reasoning_summary_delivery": "sequential_cutoff"},
        })

        self.assertEqual(turn.incremental_body["previous_response_id"], "resp_1")
        self.assertEqual(turn.incremental_body["input"], "second")

    def test_native_codex_token_change_closes_old_socket_and_replays_transcript(self) -> None:
        connections = [_FakeUpstreamWebSocket(), _FakeUpstreamWebSocket()]
        connect_calls: list[str] = []

        def connect(uri: str, **_kwargs):
            connect_calls.append(uri)
            return connections[len(connect_calls) - 1]

        tools = [{"type": "function", "name": "lookup", "parameters": {"type": "object"}}]
        session = ResponsesWebSocketSession()
        transport = CodexResponsesWebSocketTransport(connector=connect)
        with (
            mock.patch.object(
                responses_websocket_module.account_service,
                "get_text_access_token",
                side_effect=["token-1", "token-2"],
            ),
            mock.patch.object(
                responses_websocket_module.account_service,
                "get_account",
                side_effect=lambda token: {
                    "source_type": "codex",
                    "access_token": token,
                    "account_id": f"account-{token[-1]}",
                },
            ),
            mock.patch.object(
                responses_websocket_module.proxy_settings,
                "get_profile",
                return_value=mock.Mock(proxy_url=""),
            ),
            mock.patch.object(responses_websocket_module.account_service, "mark_text_used"),
        ):
            first = session.prepare_turn(
                {"type": "response.create", "model": "gpt-5.5", "input": "first", "tools": tools}
            )
            first_events = list(transport.events(first))
            session.commit(first.replay_body, first_events[-1]["response"])
            second = session.prepare_turn(
                {
                    "type": "response.create",
                    "model": "gpt-5.5",
                    "previous_response_id": "resp_1",
                    "input": "second",
                    "tools": tools,
                }
            )
            list(transport.events(second))

        self.assertEqual(len(connect_calls), 2)
        self.assertTrue(connections[0].closed)
        replay = connections[1].sent[0]
        self.assertNotIn("previous_response_id", replay)
        self.assertEqual([item["role"] for item in replay["input"]], ["user", "assistant", "user"])
        self.assertIn("first", json.dumps(replay))
        self.assertIn("second", json.dumps(replay))
        transport.close()

    def test_native_codex_reconnect_replays_completed_tool_call_and_matching_output(self) -> None:
        class ToolCallConnection(_FakeUpstreamWebSocket):
            def send(self, message: str) -> None:
                self.sent.append(json.loads(message))
                self._events.extend(
                    [
                        json.dumps(
                            {
                                "type": "response.output_item.done",
                                "output_index": 0,
                                "item": {
                                    "type": "function_call",
                                    "id": "fc-1",
                                    "call_id": "call-1",
                                    "name": "lookup",
                                    "arguments": "{}",
                                },
                            }
                        ),
                        json.dumps(
                            {
                                "type": "response.completed",
                                "response": {
                                    "id": "resp_tool",
                                    "status": "completed",
                                    "output": [
                                        {
                                            "type": "function_call",
                                            "id": "fc-1",
                                            "call_id": "call-1",
                                            "name": "lookup",
                                        }
                                    ],
                                },
                            }
                        ),
                    ]
                )

        connections = [ToolCallConnection(), _FakeUpstreamWebSocket()]
        connect_calls = 0

        def connect(_uri: str, **_kwargs):
            nonlocal connect_calls
            connection = connections[connect_calls]
            connect_calls += 1
            return connection

        tools = [{"type": "function", "name": "lookup", "parameters": {"type": "object"}}]
        session = ResponsesWebSocketSession()
        transport = CodexResponsesWebSocketTransport(connector=connect)
        with (
            mock.patch.object(
                responses_websocket_module.account_service,
                "get_text_access_token",
                side_effect=["token-1", "token-2"],
            ),
            mock.patch.object(
                responses_websocket_module.account_service,
                "get_account",
                side_effect=lambda token: {"source_type": "codex", "account_id": token},
            ),
            mock.patch.object(
                responses_websocket_module.proxy_settings,
                "get_profile",
                return_value=mock.Mock(proxy_url=""),
            ),
            mock.patch.object(responses_websocket_module.account_service, "mark_text_used"),
        ):
            first = session.prepare_turn(
                {"type": "response.create", "model": "gpt-5.5", "input": "lookup", "tools": tools}
            )
            first_events = list(transport.events(first))
            completed = first_events[-1]["response"]
            session.commit(first.replay_body, completed)

            second = session.prepare_turn(
                {
                    "type": "response.create",
                    "model": "gpt-5.5",
                    "previous_response_id": "resp_tool",
                    "input": [
                        {
                            "type": "function_call_output",
                            "call_id": "call-1",
                            "output": "42",
                        }
                    ],
                    "tools": tools,
                }
            )
            list(transport.events(second))

        self.assertEqual(completed["output"][0]["arguments"], "{}")
        replay_input = connections[1].sent[0]["input"]
        self.assertEqual([item.get("type", "message") for item in replay_input], [
            "message",
            "function_call",
            "function_call_output",
        ])
        self.assertEqual(replay_input[1]["call_id"], "call-1")
        self.assertEqual(replay_input[2]["call_id"], "call-1")
        transport.close()

    def test_native_codex_handshake_failure_is_disabled_and_http_fallback_is_safe(self) -> None:
        opaque_secret = "opaque-upstream-handshake-secret user@example.test"
        connect_calls = 0

        def fail_connect(_uri: str, **_kwargs):
            nonlocal connect_calls
            connect_calls += 1
            raise RuntimeError(opaque_secret)

        transport = CodexResponsesWebSocketTransport(connector=fail_connect)
        session = ResponsesWebSocketSession()
        turn = session.prepare_turn(
            {
                "type": "response.create",
                "model": "gpt-5.5",
                "input": "fallback",
                "tools": [{"type": "function", "name": "lookup", "parameters": {"type": "object"}}],
            }
        )
        completed = {
            "type": "response.completed",
            "response": {"id": "resp_http", "status": "completed", "output": []},
        }
        with (
            mock.patch.object(
                responses_websocket_module.account_service,
                "get_text_access_token",
                return_value="token-1",
            ),
            mock.patch.object(
                responses_websocket_module.account_service,
                "get_account",
                return_value={"source_type": "codex", "account_id": "account-1"},
            ),
            mock.patch.object(
                responses_websocket_module.proxy_settings,
                "get_profile",
                return_value=mock.Mock(proxy_url=""),
            ),
            mock.patch.object(ai_module.openai_v1_response, "response_events", return_value=iter([completed])) as http,
            mock.patch.object(responses_websocket_module.time, "sleep") as sleep,
            mock.patch.object(responses_websocket_module.logger, "warning") as warning,
        ):
            events = list(ai_module._responses_websocket_turn_events(transport, turn))
            with self.assertRaises(CodexResponsesWebSocketUnavailable) as second:
                list(transport.events(turn))

        self.assertEqual(events, [completed])
        http.assert_called_once_with(turn.replay_body)
        self.assertEqual(connect_calls, 3)
        self.assertEqual(sleep.call_args_list, [mock.call(0.2), mock.call(0.4)])
        self.assertNotIn(opaque_secret, str(second.exception))
        self.assertNotIn(opaque_secret, repr(warning.call_args_list))

    def test_catalog_pending_keeps_native_websocket_retryable_error_boundary(self) -> None:
        transport = CodexResponsesWebSocketTransport()
        turn = ResponsesWebSocketSession().prepare_turn({
            "type": "response.create",
            "model": "gpt-5.5",
            "input": "catalog pending",
        })

        with mock.patch.object(
            responses_websocket_module.account_service,
            "get_text_access_token",
            side_effect=ModelCatalogPendingError("private catalog details"),
        ):
            with self.assertRaises(CodexResponsesWebSocketUnavailable) as raised:
                list(transport.events(turn))

        self.assertEqual(str(raised.exception), "native codex websocket is unavailable")
        self.assertNotIn("private catalog details", str(raised.exception))

    def test_native_codex_transient_handshake_failure_retries_before_sending(self) -> None:
        connection = _FakeUpstreamWebSocket()
        connect_calls = 0

        def connect_after_two_failures(_uri: str, **_kwargs):
            nonlocal connect_calls
            connect_calls += 1
            if connect_calls < 3:
                raise OSError("transient handshake failure")
            return connection

        transport = CodexResponsesWebSocketTransport(connector=connect_after_two_failures)
        turn = ResponsesWebSocketSession().prepare_turn({
            "type": "response.create",
            "model": "gpt-5.5",
            "input": "retry handshake",
        })

        with (
            mock.patch.object(
                responses_websocket_module.account_service,
                "get_text_access_token",
                return_value="token-1",
            ),
            mock.patch.object(
                responses_websocket_module.account_service,
                "get_account",
                return_value={"source_type": "codex", "account_id": "account-1"},
            ),
            mock.patch.object(
                responses_websocket_module.proxy_settings,
                "get_profile",
                return_value=mock.Mock(proxy_url=""),
            ),
            mock.patch.object(responses_websocket_module.account_service, "mark_text_used"),
            mock.patch.object(responses_websocket_module.time, "sleep") as sleep,
        ):
            events = list(transport.events(turn))

        self.assertEqual(events[-1]["type"], "response.completed")
        self.assertEqual(connect_calls, 3)
        self.assertEqual(sleep.call_args_list, [mock.call(0.2), mock.call(0.4)])
        self.assertEqual(len(connection.sent), 1)
        transport.close()

    def test_native_codex_upgrade_required_falls_back_without_retry(self) -> None:
        connect_calls = 0

        def reject_upgrade(_uri: str, **_kwargs):
            nonlocal connect_calls
            connect_calls += 1
            raise InvalidStatus(Response(426, "Upgrade Required", Headers()))

        transport = CodexResponsesWebSocketTransport(connector=reject_upgrade)
        turn = ResponsesWebSocketSession().prepare_turn({
            "type": "response.create",
            "model": "gpt-5.5",
            "input": "fallback",
        })
        completed = {
            "type": "response.completed",
            "response": {"id": "resp_http", "status": "completed", "output": []},
        }

        with (
            mock.patch.object(
                responses_websocket_module.account_service,
                "get_text_access_token",
                return_value="token-1",
            ),
            mock.patch.object(
                responses_websocket_module.account_service,
                "get_account",
                return_value={"source_type": "codex", "account_id": "account-1"},
            ),
            mock.patch.object(
                responses_websocket_module.proxy_settings,
                "get_profile",
                return_value=mock.Mock(proxy_url=""),
            ),
            mock.patch.object(
                ai_module.openai_v1_response,
                "response_events",
                return_value=iter([completed]),
            ) as http,
            mock.patch.object(responses_websocket_module.time, "sleep") as sleep,
        ):
            events = list(ai_module._responses_websocket_turn_events(transport, turn))
            with self.assertRaises(CodexResponsesWebSocketUnavailable):
                list(transport.events(turn))

        self.assertEqual(events, [completed])
        http.assert_called_once_with(turn.replay_body)
        self.assertEqual(connect_calls, 1)
        sleep.assert_not_called()

    def test_native_codex_binary_frame_fails_closed_without_exposing_payload(self) -> None:
        opaque_secret = b"opaque-binary-secret user@example.test"

        class BinaryConnection(_FakeUpstreamWebSocket):
            def recv(self, timeout: float | None = None):
                del timeout
                return opaque_secret

        connection = BinaryConnection()
        transport = CodexResponsesWebSocketTransport(connector=lambda *_args, **_kwargs: connection)
        turn = ResponsesWebSocketSession().prepare_turn(
            {
                "type": "response.create",
                "model": "gpt-5.5",
                "input": "binary",
                "tools": [{"type": "function", "name": "lookup", "parameters": {"type": "object"}}],
            }
        )
        with (
            mock.patch.object(
                responses_websocket_module.account_service,
                "get_text_access_token",
                return_value="token-1",
            ),
            mock.patch.object(
                responses_websocket_module.account_service,
                "get_account",
                return_value={"source_type": "codex", "account_id": "account-1"},
            ),
            mock.patch.object(
                responses_websocket_module.proxy_settings,
                "get_profile",
                return_value=mock.Mock(proxy_url=""),
            ),
        ):
            with self.assertRaises(CodexResponsesWebSocketProtocolError) as raised:
                list(transport.events(turn))

        self.assertTrue(connection.closed)
        self.assertNotIn(opaque_secret.decode(), str(raised.exception))

    def test_native_codex_response_done_does_not_end_the_responses_turn(self) -> None:
        class DoneThenCompletedConnection(_FakeUpstreamWebSocket):
            def send(self, message: str) -> None:
                self.sent.append(json.loads(message))
                self._events.extend(
                    [
                        json.dumps({"type": "response.done", "response": {"id": "not-a-terminal"}}),
                        json.dumps(
                            {
                                "type": "response.completed",
                                "response": {
                                    "id": "resp_completed",
                                    "status": "completed",
                                    "output": [],
                                },
                            }
                        ),
                    ]
                )

        connection = DoneThenCompletedConnection()
        transport = CodexResponsesWebSocketTransport(connector=lambda *_args, **_kwargs: connection)
        turn = ResponsesWebSocketSession().prepare_turn(
            {"type": "response.create", "model": "gpt-5.5", "input": "hello"}
        )
        with (
            mock.patch.object(
                responses_websocket_module.account_service,
                "get_text_access_token",
                return_value="token-1",
            ),
            mock.patch.object(
                responses_websocket_module.account_service,
                "get_account",
                return_value={"source_type": "codex", "account_id": "account-1"},
            ),
            mock.patch.object(
                responses_websocket_module.proxy_settings,
                "get_profile",
                return_value=mock.Mock(proxy_url=""),
            ),
            mock.patch.object(responses_websocket_module.account_service, "mark_text_used") as mark_text_used,
        ):
            events = list(transport.events(turn))

        self.assertEqual([event["type"] for event in events], ["response.done", "response.completed"])
        self.assertEqual(events[-1]["response"]["id"], "resp_completed")
        mark_text_used.assert_called_once_with("token-1")
        transport.close()

    def test_native_codex_websocket_rejects_malformed_response_created(self) -> None:
        class MalformedCreatedConnection(_FakeUpstreamWebSocket):
            def send(self, message: str) -> None:
                self.sent.append(json.loads(message))
                self._events.extend(
                    [
                        json.dumps(
                            {
                                "type": "response.created",
                                "response": {
                                    "id": {"opaque": "must-not-be-coerced"},
                                    "status": "in_progress",
                                },
                            }
                        ),
                        json.dumps(
                            {
                                "type": "response.completed",
                                "response": {
                                    "id": "resp_completed",
                                    "status": "completed",
                                    "output": [],
                                },
                            }
                        ),
                    ]
                )

        connection = MalformedCreatedConnection()
        transport = CodexResponsesWebSocketTransport(connector=lambda *_args, **_kwargs: connection)
        turn = ResponsesWebSocketSession().prepare_turn(
            {"type": "response.create", "model": "gpt-5.5", "input": "hello"}
        )
        with (
            mock.patch.object(
                responses_websocket_module.account_service,
                "get_text_access_token",
                return_value="token-1",
            ),
            mock.patch.object(
                responses_websocket_module.account_service,
                "get_account",
                return_value={"source_type": "codex", "account_id": "account-1"},
            ),
            mock.patch.object(
                responses_websocket_module.proxy_settings,
                "get_profile",
                return_value=mock.Mock(proxy_url=""),
            ),
        ):
            with self.assertRaisesRegex(CodexResponsesWebSocketProtocolError, "invalid codex websocket event"):
                list(transport.events(turn))

        self.assertTrue(connection.closed)

    def test_native_codex_websocket_rejects_malformed_response_in_progress(self) -> None:
        class MalformedProgressConnection(_FakeUpstreamWebSocket):
            def send(self, message: str) -> None:
                self.sent.append(json.loads(message))
                self._events.extend(
                    [
                        json.dumps(
                            {
                                "type": "response.in_progress",
                                "response": {
                                    "id": "resp_in_progress",
                                    "status": "in_progress",
                                    "created_at": True,
                                },
                            }
                        ),
                        json.dumps(
                            {
                                "type": "response.completed",
                                "response": {
                                    "id": "resp_completed",
                                    "status": "completed",
                                    "output": [],
                                },
                            }
                        ),
                    ]
                )

        connection = MalformedProgressConnection()
        transport = CodexResponsesWebSocketTransport(connector=lambda *_args, **_kwargs: connection)
        turn = ResponsesWebSocketSession().prepare_turn(
            {"type": "response.create", "model": "gpt-5.5", "input": "hello"}
        )
        with (
            mock.patch.object(
                responses_websocket_module.account_service,
                "get_text_access_token",
                return_value="token-1",
            ),
            mock.patch.object(
                responses_websocket_module.account_service,
                "get_account",
                return_value={"source_type": "codex", "account_id": "account-1"},
            ),
            mock.patch.object(
                responses_websocket_module.proxy_settings,
                "get_profile",
                return_value=mock.Mock(proxy_url=""),
            ),
        ):
            with self.assertRaisesRegex(CodexResponsesWebSocketProtocolError, "invalid codex websocket event"):
                list(transport.events(turn))

        self.assertTrue(connection.closed)

    def test_native_codex_websocket_rejects_malformed_scalar_event_before_yield(self) -> None:
        canary = "responses-delta-container-canary"

        class MalformedScalarConnection(_FakeUpstreamWebSocket):
            def send(self, message: str) -> None:
                self.sent.append(json.loads(message))
                self._events.extend([
                    json.dumps({
                        "type": "response.output_text.delta",
                        "item_id": {"secret": canary},
                        "delta": [canary],
                    }),
                    json.dumps({
                        "type": "response.completed",
                        "response": {"id": "resp_completed", "status": "completed", "output": []},
                    }),
                ])

        connection = MalformedScalarConnection()
        transport = CodexResponsesWebSocketTransport(connector=lambda *_args, **_kwargs: connection)
        turn = ResponsesWebSocketSession().prepare_turn(
            {"type": "response.create", "model": "gpt-5.5", "input": "hello"}
        )
        with (
            mock.patch.object(
                responses_websocket_module.account_service,
                "get_text_access_token",
                return_value="token-1",
            ),
            mock.patch.object(
                responses_websocket_module.account_service,
                "get_account",
                return_value={"source_type": "codex", "account_id": "account-1"},
            ),
            mock.patch.object(
                responses_websocket_module.proxy_settings,
                "get_profile",
                return_value=mock.Mock(proxy_url=""),
            ),
        ):
            with self.assertRaisesRegex(CodexResponsesWebSocketProtocolError, "invalid codex websocket event"):
                list(transport.events(turn))

        self.assertTrue(connection.closed)

    def test_native_codex_websocket_rejects_malformed_incomplete_details_before_yield(self) -> None:
        canary = "responses-incomplete-details-transport-canary"

        class MalformedIncompleteConnection(_FakeUpstreamWebSocket):
            def send(self, message: str) -> None:
                self.sent.append(json.loads(message))
                self._events.append(json.dumps({
                    "type": "response.incomplete",
                    "response": {
                        "id": "resp_incomplete",
                        "status": "incomplete",
                        "incomplete_details": canary,
                    },
                }))

        connection = MalformedIncompleteConnection()
        transport = CodexResponsesWebSocketTransport(connector=lambda *_args, **_kwargs: connection)
        turn = ResponsesWebSocketSession().prepare_turn(
            {"type": "response.create", "model": "gpt-5.5", "input": "hello"}
        )
        with (
            mock.patch.object(
                responses_websocket_module.account_service,
                "get_text_access_token",
                return_value="token-1",
            ),
            mock.patch.object(
                responses_websocket_module.account_service,
                "get_account",
                return_value={"source_type": "codex", "account_id": "account-1"},
            ),
            mock.patch.object(
                responses_websocket_module.proxy_settings,
                "get_profile",
                return_value=mock.Mock(proxy_url=""),
            ),
        ):
            with self.assertRaisesRegex(CodexResponsesWebSocketProtocolError, "invalid codex websocket event") as raised:
                list(transport.events(turn))

        self.assertNotIn(canary, str(raised.exception))
        self.assertTrue(connection.closed)

    def test_plain_turn_prefers_native_codex_websocket(self) -> None:
        class TrackingTransport:
            def __init__(self) -> None:
                self.turns = []

            def events(self, turn):
                self.turns.append(turn)
                yield {
                    "type": "response.completed",
                    "response": {"id": "resp_native", "status": "completed", "output": []},
                }

            def close(self) -> None:
                pass

        transport = TrackingTransport()
        turn = ResponsesWebSocketSession().prepare_turn(
            {"type": "response.create", "model": "gpt-5.5", "input": "plain native turn"}
        )
        with mock.patch.object(
            ai_module.openai_v1_response,
            "response_events",
            side_effect=AssertionError("plain response turn must prefer the native Codex websocket"),
        ) as http:
            events = list(ai_module._responses_websocket_turn_events(transport, turn))

        self.assertEqual(events[-1]["response"]["id"], "resp_native")
        self.assertEqual(transport.turns, [turn])
        http.assert_not_called()

    def test_plain_turn_without_codex_account_falls_back_before_connect(self) -> None:
        connect = mock.Mock(side_effect=AssertionError("no account must not open an upstream socket"))
        transport = CodexResponsesWebSocketTransport(connector=connect)
        turn = ResponsesWebSocketSession().prepare_turn(
            {"type": "response.create", "model": "gpt-5.5", "input": "HTTP fallback"}
        )
        completed = {
            "type": "response.completed",
            "response": {"id": "resp_http", "status": "completed", "output": []},
        }
        with (
            mock.patch.object(
                responses_websocket_module.account_service,
                "get_text_access_token",
                side_effect=responses_websocket_module.ModelUnavailableError("no Codex account"),
            ),
            mock.patch.object(
                ai_module.openai_v1_response,
                "response_events",
                return_value=iter([completed]),
            ) as http,
        ):
            with self.assertRaises(CodexResponsesWebSocketUnavailable):
                list(transport.events(turn))
            events = list(ai_module._responses_websocket_turn_events(transport, turn))

        self.assertEqual(events, [completed])
        connect.assert_not_called()
        http.assert_called_once_with(turn.replay_body)

    def test_failed_terminal_closes_socket_and_retry_uses_a_fresh_connection(self) -> None:
        failed = _FakeUpstreamWebSocket()
        failed.send = lambda message: failed._events.append(
            json.dumps(
                {
                    "type": "response.failed",
                    "response": {"id": "resp_failed", "status": "failed"},
                }
            )
        )
        succeeded = _FakeUpstreamWebSocket()
        connections = [failed, succeeded]
        connect_calls = 0

        def connect(_uri: str, **_kwargs):
            nonlocal connect_calls
            connection = connections[connect_calls]
            connect_calls += 1
            return connection

        turn = ResponsesWebSocketSession().prepare_turn(
            {
                "type": "response.create",
                "model": "gpt-5.5",
                "input": "retryable turn",
                "tools": [{"type": "function", "name": "lookup", "parameters": {"type": "object"}}],
            }
        )
        transport = CodexResponsesWebSocketTransport(connector=connect)
        with (
            mock.patch.object(
                responses_websocket_module.account_service,
                "get_text_access_token",
                return_value="token-1",
            ),
            mock.patch.object(
                responses_websocket_module.account_service,
                "get_account",
                return_value={"source_type": "codex", "account_id": "account-1"},
            ),
            mock.patch.object(
                responses_websocket_module.proxy_settings,
                "get_profile",
                return_value=mock.Mock(proxy_url=""),
            ),
            mock.patch.object(responses_websocket_module.account_service, "mark_text_used") as mark_used,
        ):
            first_events = list(transport.events(turn))
            second_events = list(transport.events(turn))

        self.assertEqual(first_events[0]["type"], "response.failed")
        self.assertTrue(failed.closed)
        self.assertEqual(second_events[-1]["type"], "response.completed")
        self.assertEqual(connect_calls, 2)
        mark_used.assert_called_once_with("token-1")
        transport.close()

    def test_responses_websocket_route_uses_native_codex_transport(self) -> None:
        app = FastAPI()
        app.include_router(ai_module.create_router())
        instances: list[object] = []

        class FakeNativeTransport:
            def __init__(self) -> None:
                self.closed = False
                self.turns = []
                instances.append(self)

            def events(self, turn):
                self.turns.append(turn)
                yield {
                    "type": "response.completed",
                    "response": {"id": "resp_native", "status": "completed", "output": []},
                }

            def close(self) -> None:
                self.closed = True

        with (
            mock.patch.object(ai_module, "require_identity_async", return_value={"id": "user-1", "role": "user"}),
            mock.patch.object(ai_module, "CodexResponsesWebSocketTransport", FakeNativeTransport, create=True),
            mock.patch.object(
                ai_module.openai_v1_response,
                "response_events",
                side_effect=AssertionError("native Codex turn must not use HTTP fallback"),
            ),
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
        ):
            with TestClient(app).websocket_connect(
                "/v1/responses",
                headers={"Authorization": "Bearer user-key"},
            ) as websocket:
                websocket.send_json(
                    {
                        "type": "response.create",
                        "model": "gpt-5.5",
                        "input": "native turn",
                        "tools": [
                            {
                                "type": "function",
                                "name": "lookup",
                                "parameters": {"type": "object", "properties": {}},
                            }
                        ],
                    }
                )
                self.assertEqual(websocket.receive_json()["response"]["id"], "resp_native")

        self.assertEqual(len(instances), 1)
        self.assertEqual(len(instances[0].turns), 1)

    def test_post_terminal_usage_failure_does_not_emit_second_error_frame(self) -> None:
        app = FastAPI()
        app.include_router(ai_module.create_router())
        connections: list[_FakeUpstreamWebSocket] = []

        def transport_factory() -> CodexResponsesWebSocketTransport:
            connection = _FakeUpstreamWebSocket()
            connections.append(connection)
            return CodexResponsesWebSocketTransport(
                connector=lambda _uri, **_kwargs: connection,
            )

        with (
            mock.patch.object(ai_module, "require_identity_async", return_value={"id": "user-1", "role": "user"}),
            mock.patch.object(ai_module, "CodexResponsesWebSocketTransport", new=transport_factory),
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
            mock.patch.object(
                responses_websocket_module.account_service,
                "get_text_access_token",
                return_value="codex-access-token",
            ),
            mock.patch.object(
                responses_websocket_module.account_service,
                "get_account",
                return_value={"source_type": "codex", "account_id": "account-7"},
            ),
            mock.patch.object(
                responses_websocket_module.proxy_settings,
                "get_profile",
                return_value=mock.Mock(proxy_url=""),
            ),
            mock.patch.object(
                responses_websocket_module.account_service,
                "mark_text_used",
                side_effect=RuntimeError("usage accounting failed"),
            ) as mark_used,
            mock.patch.object(responses_websocket_module, "resolve_codex_reasoning_effort"),
        ):
            with TestClient(app).websocket_connect(
                "/v1/responses",
                headers={"Authorization": "Bearer user-key"},
            ) as websocket:
                websocket.send_json({"type": "response.create", "model": "gpt-test", "input": "first"})
                self.assertEqual(websocket.receive_json()["type"], "response.created")
                self.assertEqual(websocket.receive_json()["type"], "response.completed")

                websocket.send_json({"type": "response.create", "model": "gpt-test", "input": "second"})
                self.assertEqual(websocket.receive_json()["type"], "response.created")

        self.assertEqual(len(connections), 1)
        self.assertEqual(mark_used.call_count, 2)
        self.assertEqual(mark_used.call_args_list, [mock.call("codex-access-token")] * 2)

    def test_malformed_json_frame_is_recoverable_and_does_not_end_session(self) -> None:
        app = FastAPI()
        app.include_router(ai_module.create_router())

        class FakeNativeTransport:
            def events(self, _turn):
                yield {
                    "type": "response.completed",
                    "response": {"id": "resp_after_malformed", "status": "completed", "output": []},
                }

            def close(self) -> None:
                pass

        with (
            mock.patch.object(ai_module, "require_identity_async", return_value={"id": "user-1", "role": "user"}),
            mock.patch.object(ai_module, "CodexResponsesWebSocketTransport", FakeNativeTransport),
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
        ):
            with TestClient(app).websocket_connect(
                "/v1/responses",
                headers={"Authorization": "Bearer user-key"},
            ) as websocket:
                websocket.send_text("{malformed-json")
                error = websocket.receive_json()
                self.assertEqual(error["type"], "error")
                self.assertEqual(error["error"]["type"], "invalid_request_error")
                self.assertEqual(error["error"]["code"], "invalid_json")

                websocket.send_bytes(b"\xff")
                binary_error = websocket.receive_json()
                self.assertEqual(binary_error["type"], "error")
                self.assertEqual(binary_error["error"]["type"], "invalid_request_error")
                self.assertEqual(binary_error["error"]["code"], "invalid_json")

                websocket.send_json({"type": "response.create", "model": "gpt-test", "input": "valid"})
                self.assertEqual(websocket.receive_json()["type"], "response.completed")

    def test_content_filter_4xx_is_recoverable_before_any_upstream_call(self) -> None:
        app = FastAPI()
        app.include_router(ai_module.create_router())
        native_instances: list[object] = []
        native_turns: list[object] = []
        log_entries: list[tuple[str, dict[str, object]]] = []
        session_instances: list[object] = []

        class TrackingSlots:
            def __init__(self) -> None:
                self.acquire_calls = 0
                self.release_calls = 0

            def acquire(self, *, blocking: bool) -> bool:
                self.acquire_calls += 1
                self.blocking = blocking
                return True

            def release(self) -> None:
                self.release_calls += 1

        slots = TrackingSlots()

        class TrackingSession(ResponsesWebSocketSession):
            def __init__(self, *args, **kwargs) -> None:
                super().__init__(*args, **kwargs)
                self.closed = False
                session_instances.append(self)

            def close(self) -> None:
                self.closed = True
                super().close()

        class FakeNativeTransport:
            def __init__(self) -> None:
                self.closed = False
                native_instances.append(self)

            def events(self, turn):
                native_turns.append(turn)
                yield {
                    "type": "response.completed",
                    "response": {
                        "id": "resp_after_filter",
                        "status": "completed",
                        "output": [],
                    },
                }

            def close(self) -> None:
                self.closed = True

        class FakeLoggedCall:
            def __init__(self, *_args, **_kwargs) -> None:
                pass

            async def log_async(self, suffix: str, _result=None, **kwargs) -> None:
                log_entries.append((suffix, kwargs))

            def stream(self, items):
                return items

        check_request = mock.AsyncMock(
            side_effect=[
                HTTPException(
                    status_code=400,
                    detail={"error": "检测到敏感词，拒绝本次任务"},
                ),
                None,
            ]
        )
        with (
            mock.patch.object(ai_module, "require_identity_async", return_value={"id": "user-1", "role": "user"}),
            mock.patch.object(ai_module, "CodexResponsesWebSocketTransport", FakeNativeTransport),
            mock.patch.object(ai_module, "ResponsesWebSocketSession", TrackingSession),
            mock.patch.object(ai_module, "LoggedCall", FakeLoggedCall),
            mock.patch.object(ai_module, "_RESPONSES_WEBSOCKET_SLOTS", slots),
            mock.patch.object(ai_module, "check_request_async", new=check_request),
            mock.patch.object(
                ai_module.openai_v1_response,
                "response_events",
                side_effect=AssertionError("content-filtered turn must not call HTTP upstream"),
            ),
        ):
            with TestClient(app).websocket_connect(
                "/v1/responses",
                headers={"Authorization": "Bearer user-key"},
            ) as websocket:
                websocket.send_json({"type": "response.create", "model": "gpt-test", "input": "blocked"})
                error = websocket.receive_json()
                self.assertEqual(error["type"], "error")
                self.assertEqual(error["error"]["type"], "invalid_request_error")
                self.assertEqual(error["error"]["code"], "content_filter")
                self.assertEqual(error["error"]["message"], "检测到敏感词，拒绝本次任务")

                websocket.send_json({"type": "response.create", "model": "gpt-test", "input": "allowed"})
                self.assertEqual(websocket.receive_json()["type"], "response.completed")

        self.assertEqual(check_request.await_count, 2)
        self.assertEqual(len(native_instances), 1)
        self.assertEqual(len(native_turns), 1)
        self.assertEqual(native_turns[0].replay_body["input"], "allowed")
        self.assertEqual(log_entries[0][0], "调用失败")
        self.assertEqual(log_entries[0][1]["status"], "failed")
        self.assertEqual(log_entries[0][1]["error"], "HTTPException (status=400)")
        self.assertEqual(slots.acquire_calls, 1)
        self.assertEqual(slots.release_calls, 1)
        self.assertEqual(len(session_instances), 1)
        self.assertTrue(session_instances[0].closed)

    def test_content_filter_5xx_uses_public_server_error_contract(self) -> None:
        app = FastAPI()
        app.include_router(ai_module.create_router())
        secret = "opaque-moderation-upstream-secret owner@example.test"
        native_turns: list[object] = []
        log_entries: list[tuple[str, dict[str, object]]] = []

        class FakeNativeTransport:
            def events(self, turn):
                native_turns.append(turn)
                raise AssertionError("moderation failure must not call native upstream")

            def close(self) -> None:
                pass

        class FakeLoggedCall:
            def __init__(self, *_args, **_kwargs) -> None:
                pass

            async def log_async(self, suffix: str, _result=None, **kwargs) -> None:
                log_entries.append((suffix, kwargs))

        async def reject_with_upstream_failure(_text: str) -> None:
            raise HTTPException(status_code=503, detail={"error": secret})

        with (
            mock.patch.object(ai_module, "require_identity_async", return_value={"id": "user-1", "role": "user"}),
            mock.patch.object(ai_module, "CodexResponsesWebSocketTransport", FakeNativeTransport),
            mock.patch.object(ai_module, "LoggedCall", FakeLoggedCall),
            mock.patch.object(ai_module, "check_request_async", new=reject_with_upstream_failure),
            mock.patch.object(
                ai_module.openai_v1_response,
                "response_events",
                side_effect=AssertionError("moderation failure must not call HTTP upstream"),
            ),
        ):
            with TestClient(app).websocket_connect(
                "/v1/responses",
                headers={"Authorization": "Bearer user-key"},
            ) as websocket:
                websocket.send_json({"type": "response.create", "model": "gpt-test", "input": "blocked"})
                error = websocket.receive_json()

        self.assertEqual(error["type"], "error")
        self.assertEqual(error["error"]["type"], "server_error")
        self.assertEqual(error["error"]["code"], "upstream_error")
        self.assertEqual(error["error"]["message"], ai_module.PUBLIC_SERVER_ERROR_MESSAGE)
        self.assertEqual(native_turns, [])
        self.assertEqual(log_entries[0][0], "调用失败")
        self.assertEqual(log_entries[0][1]["status"], "failed")
        self.assertEqual(log_entries[0][1]["error"], "HTTPException (status=503)")
        serialized = json.dumps((error, log_entries), ensure_ascii=False, default=str)
        self.assertNotIn(secret, serialized)

    def test_websocket_projects_native_failed_event_without_upstream_secrets(self) -> None:
        app = FastAPI()
        app.include_router(ai_module.create_router())
        secret = "opaque-native-ws-secret owner@example.test"
        private_url = "https://user:private-password@upstream.invalid/private"
        private_response_id = "resp_privateToken98765"

        class FakeNativeTransport:
            def events(self, _turn):
                yield {
                    "type": "response.failed",
                    "sequence_number": 3,
                    "response": {
                        "id": private_response_id,
                        "object": "response",
                        "created_at": 1_765_000_000,
                        "status": "failed",
                        "model": "gpt-test",
                        "output": [],
                        "error": {
                            "code": "opaque_upstream_code",
                            "message": secret,
                            "url": private_url,
                        },
                    },
                }

            def close(self) -> None:
                pass

        with (
            mock.patch.object(ai_module, "require_identity_async", return_value={"id": "user-1", "role": "user"}),
            mock.patch.object(ai_module, "CodexResponsesWebSocketTransport", FakeNativeTransport, create=True),
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
            mock.patch("services.log_service.log_service.add") as add,
        ):
            with TestClient(app).websocket_connect(
                "/v1/responses",
                headers={"Authorization": "Bearer user-key"},
            ) as websocket:
                websocket.send_json({"type": "response.create", "model": "gpt-test", "input": "hello"})
                public_event = websocket.receive_json()

        serialized_event = json.dumps(public_event, ensure_ascii=False)
        serialized_log = json.dumps(add.call_args_list, ensure_ascii=False, default=str)
        self.assertEqual(public_event["type"], "response.failed")
        self.assertEqual(
            public_event["response"]["error"]["message"],
            ai_module.PUBLIC_SERVER_ERROR_MESSAGE,
        )
        for private_value in (
            secret,
            private_url,
            private_response_id,
            "private-password",
            "owner@example.test",
        ):
            self.assertNotIn(private_value, serialized_event)
            self.assertNotIn(private_value, serialized_log)

    def test_websocket_drops_internal_codex_metadata_before_public_output_and_logging(self) -> None:
        app = FastAPI()
        app.include_router(ai_module.create_router())
        secret = "opaque-native-ws-metadata-secret owner@example.test"
        private_url = "https://user:private-password@upstream.invalid/internal"
        public_item = {
            "id": "msg_public",
            "type": "message",
            "status": "completed",
            "role": "assistant",
            "content": [{"type": "output_text", "text": "hello", "annotations": []}],
        }
        private_item = {
            **public_item,
            "internal_chat_message_metadata_passthrough": {
                "turn_id": secret,
                "url": private_url,
            },
        }

        class ScriptedUpstreamWebSocket:
            def __init__(self) -> None:
                self.closed = False
                self.sent: list[dict[str, object]] = []
                self.events = [
                    json.dumps({
                        "type": "response.metadata",
                        "headers": {"location": private_url},
                        "metadata": {"turn_state": secret},
                    }),
                    json.dumps({
                        "type": "codex.response.metadata",
                        "metadata": {"private_debug": secret, "url": private_url},
                    }),
                    json.dumps({
                        "type": "responsesapi.websocket_timing",
                        "timing": {"private_debug": secret, "url": private_url},
                    }),
                    json.dumps({
                        "type": "response.output_text.delta",
                        "item_id": "msg_public",
                        "output_index": 0,
                        "content_index": 0,
                        "delta": "hello",
                        "headers": {"location": private_url},
                        "metadata": {"private_debug": secret},
                        "safety_buffering": {"private_debug": secret, "url": private_url},
                    }),
                    json.dumps({
                        "type": "response.output_item.done",
                        "output_index": 0,
                        "item": private_item,
                    }),
                    json.dumps({
                        "type": "response.completed",
                        "response": {
                            "id": "resp_public",
                            "object": "response",
                            "status": "completed",
                            "model": "gpt-test",
                            "output": [public_item],
                            "headers": {"location": private_url, "private_debug": secret},
                        },
                    }),
                ]

            def send(self, message: str) -> None:
                self.sent.append(json.loads(message))

            def recv(self, timeout: float | None = None) -> str:
                del timeout
                return self.events.pop(0)

            def close(self) -> None:
                self.closed = True

        upstream = ScriptedUpstreamWebSocket()

        def transport_factory() -> CodexResponsesWebSocketTransport:
            return CodexResponsesWebSocketTransport(connector=lambda _uri, **_kwargs: upstream)

        with (
            mock.patch.object(ai_module, "require_identity_async", return_value={"id": "user-1", "role": "user"}),
            mock.patch.object(ai_module, "CodexResponsesWebSocketTransport", new=transport_factory),
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
            mock.patch.object(
                responses_websocket_module.account_service,
                "get_text_access_token",
                return_value="codex-access-token",
            ),
            mock.patch.object(
                responses_websocket_module.account_service,
                "get_account",
                return_value={"source_type": "codex", "access_token": "codex-access-token"},
            ),
            mock.patch.object(
                responses_websocket_module.proxy_settings,
                "get_profile",
                return_value=mock.Mock(proxy_url=""),
            ),
            mock.patch.object(responses_websocket_module.account_service, "mark_text_used"),
            mock.patch("services.log_service.log_service.add") as add,
        ):
            with TestClient(app).websocket_connect(
                "/v1/responses",
                headers={"Authorization": "Bearer user-key"},
            ) as websocket:
                websocket.send_json({"type": "response.create", "model": "gpt-test", "input": "hello"})
                public_events = [websocket.receive_json(), websocket.receive_json(), websocket.receive_json()]

        self.assertEqual(
            [event["type"] for event in public_events],
            ["response.output_text.delta", "response.output_item.done", "response.completed"],
        )
        self.assertEqual(public_events[0]["delta"], "hello")
        self.assertNotIn("internal_chat_message_metadata_passthrough", public_events[1]["item"])
        self.assertNotIn(
            "internal_chat_message_metadata_passthrough",
            public_events[2]["response"]["output"][0],
        )
        serialized_event = json.dumps(public_events, ensure_ascii=False)
        serialized_log = json.dumps(add.call_args_list, ensure_ascii=False, default=str)
        for private_value in (secret, private_url, "private-password", "owner@example.test"):
            self.assertNotIn(private_value, serialized_event)
            self.assertNotIn(private_value, serialized_log)

    def test_completed_response_is_delivered_before_transcript_capacity_error(self) -> None:
        app = FastAPI()
        app.include_router(ai_module.create_router())
        completed = {
            "type": "response.completed",
            "response": {
                "id": "resp_large",
                "status": "completed",
                "output": [{"type": "message", "content": "x" * 200}],
            },
        }

        class FakeNativeTransport:
            def events(self, _turn):
                raise CodexResponsesWebSocketUnavailable("native websocket unavailable in capacity fixture")

            def close(self) -> None:
                pass

        with (
            mock.patch.object(ai_module, "require_identity_async", return_value={"id": "user-1", "role": "user"}),
            mock.patch.object(
                ai_module,
                "ResponsesWebSocketSession",
                side_effect=lambda: ResponsesWebSocketSession(max_transcript_bytes=100),
            ),
            mock.patch.object(ai_module, "CodexResponsesWebSocketTransport", FakeNativeTransport),
            mock.patch.object(ai_module.openai_v1_response, "response_events", return_value=iter([completed])) as events,
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
        ):
            with TestClient(app).websocket_connect(
                "/v1/responses",
                headers={"Authorization": "Bearer user-key"},
            ) as websocket:
                websocket.send_json({"type": "response.create", "model": "gpt-test", "input": "small"})
                self.assertEqual(websocket.receive_json(), completed)

                websocket.send_json({
                    "type": "response.create",
                    "model": "gpt-test",
                    "previous_response_id": "resp_large",
                    "input": "continue",
                })
                error = websocket.receive_json()

        self.assertEqual(error["type"], "error")
        self.assertEqual(error["error"]["code"], "websocket_session_too_large")
        events.assert_called_once()

    def test_invalid_authorization_is_rejected_before_upgrade(self) -> None:
        app = FastAPI()
        app.include_router(ai_module.create_router())
        with mock.patch.object(ai_module, "require_identity_async", side_effect=HTTPException(status_code=401)):
            with self.assertRaises(WebSocketDisconnect) as raised:
                with TestClient(app).websocket_connect(
                    "/v1/responses",
                    headers={"Authorization": "Bearer revoked"},
                ):
                    pass
        self.assertEqual(raised.exception.code, 1008)

    def test_invalid_codex_shape_is_rejected_before_native_transport(self) -> None:
        app = FastAPI()
        app.include_router(ai_module.create_router())
        native_calls: list[PreparedResponsesWebSocketTurn] = []

        class FakeNativeTransport:
            def events(self, turn: PreparedResponsesWebSocketTurn):
                native_calls.append(turn)
                yield {
                    "type": "response.completed",
                    "response": {"id": "resp_valid", "status": "completed", "output": []},
                }

            def close(self) -> None:
                pass

        with (
            mock.patch.object(
                ai_module,
                "require_identity_async",
                return_value={"id": "user-1", "role": "user"},
            ),
            mock.patch.object(ai_module, "CodexResponsesWebSocketTransport", FakeNativeTransport),
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
        ):
            with TestClient(app).websocket_connect(
                "/v1/responses",
                headers={"Authorization": "Bearer user-key"},
            ) as websocket:
                websocket.send_json({
                    "type": "response.create",
                    "model": "gpt-5.5",
                    "input": "invalid tool",
                    "tools": [{
                        "type": "function",
                        "name": "lookup",
                        "parameters": {"type": "object"},
                        "future_field": "must-not-reach-upstream",
                    }],
                })
                error = websocket.receive_json()

                websocket.send_json({
                    "type": "response.create",
                    "model": "gpt-5.5",
                    "input": "valid turn",
                })
                completed = websocket.receive_json()

        self.assertEqual(error["type"], "error")
        self.assertEqual(error["error"]["type"], "invalid_request_error")
        self.assertEqual(completed["type"], "response.completed")
        self.assertEqual(len(native_calls), 1)
        self.assertEqual(native_calls[0].replay_body["input"], "valid turn")

    def test_authenticated_connection_accepts_multiple_response_create_turns(self) -> None:
        app = FastAPI()
        app.include_router(ai_module.create_router())
        client = TestClient(app)
        seen_inputs: list[object] = []

        def fake_response_events(
            body: dict[str, object], *, cache_scope: str = "", authenticated: bool = False
        ):
            del cache_scope, authenticated
            seen_inputs.append(body.get("input"))
            response_id = f"resp_{len(seen_inputs)}"
            yield {
                "type": "response.created",
                "response": {"id": response_id, "status": "in_progress", "output": []},
            }
            yield {
                "type": "response.completed",
                "response": {
                    "id": response_id,
                    "status": "completed",
                    "output": [
                        {
                            "id": f"msg_{len(seen_inputs)}",
                            "type": "message",
                            "role": "assistant",
                            "status": "completed",
                            "content": [
                                {
                                    "type": "output_text",
                                    "text": f"reply-{len(seen_inputs)}",
                                    "annotations": [],
                                }
                            ],
                        }
                    ],
                },
            }

        with (
            mock.patch.object(ai_module, "require_identity_async", return_value={"id": "user-1", "role": "user"}) as require,
            mock.patch.object(ai_module.openai_v1_response, "response_events", side_effect=fake_response_events),
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
        ):
            with client.websocket_connect(
                "/v1/responses",
                headers={"Authorization": "Bearer user-key"},
            ) as websocket:
                websocket.send_json({"type": "response.create", "model": "gpt-test", "input": "first"})
                self.assertEqual(websocket.receive_json()["type"], "response.created")
                first_completed = websocket.receive_json()
                self.assertEqual(first_completed["type"], "response.completed")

                websocket.send_json({"type": "response.create", "model": "gpt-test", "input": "second"})
                self.assertEqual(websocket.receive_json()["type"], "response.created")
                second_completed = websocket.receive_json()
                self.assertEqual(second_completed["response"]["id"], "resp_2")

        self.assertEqual(require.call_args_list, [mock.call("Bearer user-key")] * 3)
        self.assertEqual(seen_inputs, ["first", "second"])

    def test_http_fallback_keeps_authenticated_cache_scope_per_websocket(self) -> None:
        app = FastAPI()
        app.include_router(ai_module.create_router())
        scopes: list[str] = []
        authenticated_values: list[bool] = []

        class UnavailableNativeTransport:
            def events(self, _turn):
                raise CodexResponsesWebSocketUnavailable("native transport unavailable")

            def close(self) -> None:
                pass

        def authenticate(authorization: str | None):
            return {"id": str(authorization or "").split()[-1], "role": "user"}

        def fake_response_events(
            body: dict[str, object], *, cache_scope: str = "", authenticated: bool = False
        ):
            del body
            scopes.append(cache_scope)
            authenticated_values.append(authenticated)
            yield {
                "type": "response.completed",
                "response": {
                    "id": f"resp-{cache_scope}",
                    "status": "completed",
                    "output": [],
                },
            }

        with (
            mock.patch.object(ai_module, "require_identity_async", side_effect=authenticate),
            mock.patch.object(ai_module, "CodexResponsesWebSocketTransport", UnavailableNativeTransport),
            mock.patch.object(ai_module.openai_v1_response, "response_events", side_effect=fake_response_events),
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
        ):
            client = TestClient(app)
            for token, expected_scope in (("alpha", "alpha"), ("beta", "beta")):
                with client.websocket_connect(
                    "/v1/responses",
                    headers={"Authorization": f"Bearer {token}"},
                ) as websocket:
                    websocket.send_json({
                        "type": "response.create",
                        "model": "gpt-test",
                        "input": "same request",
                    })
                    completed = websocket.receive_json()
                    self.assertEqual(completed["type"], "response.completed")
        self.assertEqual(completed["response"]["id"], f"resp-{expected_scope}")
        self.assertEqual(scopes, ["alpha", "beta"])
        self.assertEqual(authenticated_values, [True, True])

    def test_fallback_stops_consuming_a_turn_after_terminal_event(self) -> None:
        app = FastAPI()
        app.include_router(ai_module.create_router())
        completed_ids = ["resp-first", "resp-second"]

        def fallback(_body, **_kwargs):
            response_id = completed_ids.pop(0)
            yield {
                "type": "response.completed",
                "response": {"id": response_id, "status": "completed", "output": []},
            }
            if response_id == "resp-first":
                yield {
                    "type": "response.output_text.delta",
                    "item_id": "late-item",
                    "output_index": 0,
                    "content_index": 0,
                    "delta": "must-not-follow-terminal",
                }

        class UnavailableNativeTransport:
            def events(self, _turn):
                raise CodexResponsesWebSocketUnavailable("native websocket unavailable")

            def close(self) -> None:
                pass

        with (
            mock.patch.object(ai_module, "require_identity_async", return_value={"id": "user-1", "role": "user"}),
            mock.patch.object(ai_module, "CodexResponsesWebSocketTransport", UnavailableNativeTransport),
            mock.patch.object(ai_module.openai_v1_response, "response_events", side_effect=fallback) as response_events,
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
        ):
            with TestClient(app).websocket_connect(
                "/v1/responses",
                headers={"Authorization": "Bearer user-key"},
            ) as websocket:
                websocket.send_json({"type": "response.create", "model": "gpt-test", "input": "first"})
                self.assertEqual(websocket.receive_json()["response"]["id"], "resp-first")

                websocket.send_json({"type": "response.create", "model": "gpt-test", "input": "second"})
                self.assertEqual(websocket.receive_json()["response"]["id"], "resp-second")

        self.assertEqual(response_events.call_count, 2)

    def test_previous_response_id_reuses_only_the_current_connection_transcript(self) -> None:
        app = FastAPI()
        app.include_router(ai_module.create_router())
        client = TestClient(app)
        seen_inputs: list[object] = []

        def fake_response_events(
            body: dict[str, object], *, cache_scope: str = "", authenticated: bool = False
        ):
            del cache_scope, authenticated
            seen_inputs.append(body.get("input"))
            response_id = f"resp_{len(seen_inputs)}"
            yield {
                "type": "response.completed",
                "response": {
                    "id": response_id,
                    "status": "completed",
                    "output": [
                        {
                            "id": f"msg_{len(seen_inputs)}",
                            "type": "message",
                            "role": "assistant",
                            "status": "completed",
                            "content": [
                                {
                                    "type": "output_text",
                                    "text": f"reply-{len(seen_inputs)}",
                                    "annotations": [],
                                }
                            ],
                        }
                    ],
                },
            }

        with (
            mock.patch.object(ai_module, "require_identity_async", return_value={"id": "user-1", "role": "user"}),
            mock.patch.object(ai_module.openai_v1_response, "response_events", side_effect=fake_response_events),
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
        ):
            with client.websocket_connect(
                "/v1/responses",
                headers={"Authorization": "Bearer user-key"},
            ) as websocket:
                websocket.send_json({"type": "response.create", "model": "gpt-test", "input": "first"})
                self.assertEqual(websocket.receive_json()["response"]["id"], "resp_1")
                websocket.send_json(
                    {
                        "type": "response.create",
                        "model": "gpt-test",
                        "previous_response_id": "resp_1",
                        "input": "second",
                    }
                )
                self.assertEqual(websocket.receive_json()["response"]["id"], "resp_2")

        self.assertEqual(seen_inputs[0], "first")
        self.assertIsInstance(seen_inputs[1], list)
        transcript = seen_inputs[1]
        assert isinstance(transcript, list)
        self.assertEqual([item.get("role") for item in transcript], ["user", "assistant", "user"])
        self.assertEqual(transcript[0]["content"], "first")
        self.assertEqual(transcript[1]["content"][0]["text"], "reply-1")
        self.assertEqual(transcript[2]["content"], "second")

    def test_failed_continuation_evicts_the_referenced_response_id(self) -> None:
        app = FastAPI()
        app.include_router(ai_module.create_router())
        calls: list[dict[str, object]] = []

        def fake_response_events(
            body: dict[str, object], *, cache_scope: str = "", authenticated: bool = False
        ):
            del cache_scope, authenticated
            calls.append(body)
            if len(calls) == 1:
                yield {
                    "type": "response.completed",
                    "response": {"id": "resp_1", "status": "completed", "output": []},
                }
                return
            if len(calls) == 2:
                yield {
                    "type": "response.failed",
                    "response": {"id": "resp_failed", "status": "failed", "output": []},
                }
                return
            yield {
                "type": "response.completed",
                "response": {"id": "must-not-run", "status": "completed", "output": []},
            }

        with (
            mock.patch.object(ai_module, "require_identity_async", return_value={"id": "user-1", "role": "user"}),
            mock.patch.object(ai_module.openai_v1_response, "response_events", side_effect=fake_response_events),
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
        ):
            with TestClient(app).websocket_connect(
                "/v1/responses",
                headers={"Authorization": "Bearer user-key"},
            ) as websocket:
                websocket.send_json({"type": "response.create", "model": "gpt-test", "input": "first"})
                self.assertEqual(websocket.receive_json()["response"]["id"], "resp_1")

                websocket.send_json({
                    "type": "response.create",
                    "model": "gpt-test",
                    "previous_response_id": "resp_1",
                    "input": "continuation that fails",
                })
                self.assertEqual(websocket.receive_json()["type"], "response.failed")

                websocket.send_json({
                    "type": "response.create",
                    "model": "gpt-test",
                    "previous_response_id": "resp_1",
                    "input": "must be rejected before upstream",
                })
                error = websocket.receive_json()

        self.assertEqual(error["type"], "error")
        self.assertEqual(error["error"]["code"], "previous_response_not_found")
        self.assertEqual(len(calls), 2)

    def test_revoked_identity_is_rechecked_before_each_turn(self) -> None:
        app = FastAPI()
        app.include_router(ai_module.create_router())
        client = TestClient(app)
        auth_results: list[str] = []

        def authenticate(authorization: str | None):
            auth_results.append(str(authorization or ""))
            if len(auth_results) == 1:
                return {"id": "user-1", "role": "user"}
            raise HTTPException(status_code=401, detail={"error": "revoked"})

        with (
            mock.patch.object(ai_module, "require_identity_async", side_effect=authenticate),
            mock.patch.object(
                ai_module.openai_v1_response,
                "response_events",
                return_value=iter(
                    [
                        {
                            "type": "response.completed",
                            "response": {"id": "must-not-run", "status": "completed", "output": []},
                        }
                    ]
                ),
            ) as response_events,
        ):
            with client.websocket_connect(
                "/v1/responses",
                headers={"Authorization": "Bearer user-key"},
            ) as websocket:
                websocket.send_json({"type": "response.create", "model": "gpt-test", "input": "secret"})
                with self.assertRaises(WebSocketDisconnect) as raised:
                    websocket.receive_json()

        self.assertEqual(raised.exception.code, 1008)
        self.assertEqual(auth_results, ["Bearer user-key", "Bearer user-key"])
        response_events.assert_not_called()

    def test_bad_previous_response_id_is_a_recoverable_request_error(self) -> None:
        app = FastAPI()
        app.include_router(ai_module.create_router())
        completed = {
            "type": "response.completed",
            "response": {"id": "resp_ok", "status": "completed", "output": []},
        }
        with (
            mock.patch.object(ai_module, "require_identity_async", return_value={"id": "user-1", "role": "user"}),
            mock.patch.object(ai_module.openai_v1_response, "response_events", return_value=iter([completed])) as events,
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
        ):
            with TestClient(app).websocket_connect(
                "/v1/responses",
                headers={"Authorization": "Bearer user-key"},
            ) as websocket:
                websocket.send_json({
                    "type": "response.create",
                    "model": "gpt-test",
                    "previous_response_id": "resp_other_connection",
                    "input": "first",
                })
                error = websocket.receive_json()
                self.assertEqual(error["type"], "error")
                self.assertEqual(error["error"]["code"], "previous_response_not_found")
                websocket.send_json({"type": "response.create", "model": "gpt-test", "input": "valid"})
                self.assertEqual(websocket.receive_json(), completed)
        events.assert_called_once()

    def test_unknown_turn_exception_never_exposes_opaque_secret(self) -> None:
        app = FastAPI()
        app.include_router(ai_module.create_router())
        opaque_secret = "opaque-websocket-secret user@example.test"
        with (
            mock.patch.object(ai_module, "require_identity_async", return_value={"id": "user-1", "role": "user"}),
            mock.patch.object(ai_module.openai_v1_response, "response_events", side_effect=RuntimeError(opaque_secret)),
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
        ):
            with TestClient(app).websocket_connect(
                "/v1/responses",
                headers={"Authorization": "Bearer user-key"},
            ) as websocket:
                websocket.send_json({"type": "response.create", "model": "gpt-test", "input": "valid"})
                error = websocket.receive_json()
        self.assertEqual(error["type"], "error")
        self.assertEqual(error["error"]["message"], ai_module.PUBLIC_SERVER_ERROR_MESSAGE)
        self.assertNotIn(opaque_secret, repr(error))

    def test_disconnect_closes_raw_and_logged_iterators(self) -> None:
        class TrackingIterator:
            def __init__(self) -> None:
                self.closed = False
                self.source = None

            def __iter__(self):
                return self

            def __next__(self):
                if self.source is not None:
                    return next(self.source)
                return {"type": "response.created", "response": {"id": "resp_1"}}

            def close(self) -> None:
                self.closed = True

        raw = TrackingIterator()
        logged = TrackingIterator()
        native_instances: list[object] = []

        class FakeNativeTransport:
            def __init__(self) -> None:
                self.closed = False
                native_instances.append(self)

            def events(self, _turn):
                raise CodexResponsesWebSocketUnavailable("native websocket unavailable in HTTP cleanup fixture")

            def close(self) -> None:
                self.closed = True

        class FakeLoggedCall:
            def __init__(self, *_args, **_kwargs):
                pass

            def stream(self, _items):
                logged.source = iter(_items)
                return logged

        class FakeWebSocket:
            headers = {"authorization": "Bearer user-key"}

            async def accept(self) -> None:
                pass

            async def receive_json(self):
                return {"type": "response.create", "model": "gpt-test", "input": "valid"}

            async def send_json(self, _event) -> None:
                raise WebSocketDisconnect(code=1000)

            async def close(self, **_kwargs) -> None:
                pass

        router = ai_module.create_router()
        endpoint = next(route.endpoint for route in router.routes if getattr(route, "path", "") == "/v1/responses" and "websocket" in route.name)
        with (
            mock.patch.object(ai_module, "require_identity_async", return_value={"id": "user-1", "role": "user"}),
            mock.patch.object(ai_module.openai_v1_response, "response_events", return_value=raw),
            mock.patch.object(ai_module, "LoggedCall", FakeLoggedCall),
            mock.patch.object(ai_module, "CodexResponsesWebSocketTransport", FakeNativeTransport),
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
            mock.patch.object(ResponsesWebSocketSession, "close", autospec=True) as session_close,
        ):
            asyncio.run(endpoint(FakeWebSocket()))

        self.assertTrue(logged.closed)
        self.assertTrue(raw.closed)
        self.assertEqual(len(native_instances), 1)
        self.assertTrue(native_instances[0].closed)
        session_close.assert_called_once()

    def test_ws_reports_error_when_upstream_ends_without_terminal_event(self) -> None:
        sent: list[dict[str, object]] = []

        class FakeNativeTransport:
            def events(self, _turn):
                raise CodexResponsesWebSocketUnavailable("native websocket unavailable")

            def close(self) -> None:
                return None

        class FakeLoggedCall:
            def __init__(self, *_args, **_kwargs):
                pass

            def stream(self, items):
                return items

        class FakeWebSocket:
            headers = {"authorization": "Bearer user-key"}
            reads = 0

            async def accept(self) -> None:
                return None

            async def receive_json(self):
                self.reads += 1
                if self.reads == 1:
                    return {"type": "response.create", "model": "gpt-test", "input": "valid"}
                raise WebSocketDisconnect(code=1000)

            async def send_json(self, event) -> None:
                sent.append(event)

            async def close(self, **_kwargs) -> None:
                return None

        app = FastAPI()
        app.include_router(ai_module.create_router())
        with (
            mock.patch.object(ai_module, "require_identity_async", return_value={"id": "user-1", "role": "user"}),
            mock.patch.object(ai_module, "CodexResponsesWebSocketTransport", FakeNativeTransport),
            mock.patch.object(ai_module, "LoggedCall", FakeLoggedCall),
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
            mock.patch.object(
                ai_module.openai_v1_response,
                "response_events",
                return_value=[{"type": "response.created", "response": {"id": "resp-1"}}],
            ),
        ):
            asyncio.run(next(
                route.endpoint
                for route in app.router.routes
                if getattr(route, "path", "") == "/v1/responses" and "websocket" in route.name
            )(FakeWebSocket()))

        self.assertEqual([event["type"] for event in sent], ["response.created", "error"])
        self.assertEqual(sent[-1]["error"]["code"], "upstream_error")

    def test_connection_lifetime_limit_closes_before_reading_another_turn(self) -> None:
        sent: list[dict[str, object]] = []

        class ExpiredSession:
            closed = False

            def remaining_lifetime_seconds(self) -> float:
                return 0.0

            def close(self) -> None:
                self.closed = True

        class FakeNativeTransport:
            def close(self) -> None:
                pass

        class FakeWebSocket:
            headers = {"authorization": "Bearer user-key"}
            receive_calls = 0
            closed = False

            async def accept(self) -> None:
                pass

            async def receive_json(self):
                self.receive_calls += 1
                raise WebSocketDisconnect(code=1000)

            async def send_json(self, event) -> None:
                sent.append(event)

            async def close(self, **_kwargs) -> None:
                self.closed = True

        websocket = FakeWebSocket()
        session = ExpiredSession()
        router = ai_module.create_router()
        endpoint = next(
            route.endpoint
            for route in router.routes
            if getattr(route, "path", "") == "/v1/responses" and "websocket" in route.name
        )
        with (
            mock.patch.object(ai_module, "require_identity_async", return_value={"id": "user-1", "role": "user"}),
            mock.patch.object(ai_module, "ResponsesWebSocketSession", return_value=session),
            mock.patch.object(ai_module, "CodexResponsesWebSocketTransport", FakeNativeTransport),
        ):
            asyncio.run(endpoint(websocket))

        self.assertEqual(websocket.receive_calls, 0)
        self.assertTrue(websocket.closed)
        self.assertTrue(session.closed)
        self.assertEqual(sent[0]["type"], "error")
        self.assertEqual(sent[0]["error"]["code"], "websocket_connection_limit_reached")


class ResponsesWebSocketSessionContractTests(unittest.TestCase):
    def test_session_rejects_invalid_core_types_and_unsupported_background(self) -> None:
        invalid_turns = (
            {"type": "response.create", "model": {}, "input": "hello"},
            {"type": "response.create", "model": "auto", "instructions": {}, "input": "hello"},
            {"type": "response.create", "model": "auto", "input": ["discarded"]},
            {"type": "response.create", "model": "auto", "input": "hello", "stream": "true"},
            {"type": "response.create", "model": "auto", "input": "hello", "previous_response_id": 0},
            {"type": "response.create", "model": "auto", "input": "hello", "background": True},
        )

        for turn in invalid_turns:
            with self.subTest(turn=turn):
                with self.assertRaises(ResponsesWebSocketRequestError) as raised:
                    ResponsesWebSocketSession().prepare_turn(turn)
                self.assertEqual(raised.exception.code, "invalid_request_error")

    def test_session_rejects_oversized_state_before_upstream_call(self) -> None:
        session = ResponsesWebSocketSession(max_transcript_bytes=80)
        with self.assertRaises(ResponsesWebSocketRequestError) as raised:
            session.prepare({"type": "response.create", "input": "x" * 200})
        self.assertEqual(raised.exception.code, "websocket_session_too_large")

    def test_shared_transcript_budget_bounds_sessions_and_releases_on_close(self) -> None:
        budget = responses_websocket_module._ResponsesWebSocketTranscriptBudget(300)
        first = ResponsesWebSocketSession(
            max_transcript_bytes=1000,
            transcript_budget=budget,
        )
        second = ResponsesWebSocketSession(
            max_transcript_bytes=1000,
            transcript_budget=budget,
        )
        first_body = {"model": "gpt-test", "input": "x" * 200}
        second_body = {"model": "gpt-test", "input": "y" * 200}

        self.assertTrue(first.commit(first_body, {"id": "resp_first", "output": []}))
        self.assertFalse(second.commit(second_body, {"id": "resp_second", "output": []}))

        with self.assertRaises(ResponsesWebSocketRequestError) as raised:
            second.prepare_turn({
                "type": "response.create",
                "model": "gpt-test",
                "previous_response_id": "resp_second",
                "input": "continue",
            })
        self.assertEqual(raised.exception.code, "websocket_server_capacity_reached")

        first.close()
        self.assertTrue(second.commit(second_body, {"id": "resp_second", "output": []}))
        second.close()

    def test_failed_continuation_releases_shared_transcript_budget(self) -> None:
        budget = responses_websocket_module._ResponsesWebSocketTranscriptBudget(300)
        failed = ResponsesWebSocketSession(
            max_transcript_bytes=1000,
            transcript_budget=budget,
        )
        body = {"model": "gpt-test", "input": "x" * 200}
        self.assertTrue(failed.commit(body, {"id": "resp_first", "output": []}))
        turn = failed.prepare_turn({
            "type": "response.create",
            "model": "gpt-test",
            "previous_response_id": "resp_first",
            "input": "continue",
        })
        failed.fail(turn)

        replacement = ResponsesWebSocketSession(
            max_transcript_bytes=1000,
            transcript_budget=budget,
        )
        self.assertTrue(replacement.commit(body, {"id": "resp_second", "output": []}))
        replacement.close()

    def test_transcript_budget_releases_when_session_is_collected(self) -> None:
        budget = responses_websocket_module._ResponsesWebSocketTranscriptBudget(300)

        def reserve_one_session():
            session = ResponsesWebSocketSession(
                max_transcript_bytes=1000,
                transcript_budget=budget,
            )
            self.assertTrue(session.commit(
                {"model": "gpt-test", "input": "x" * 200},
                {"id": "resp_first", "output": []},
            ))
            return weakref.ref(session)

        session_ref = reserve_one_session()
        gc.collect()
        self.assertIsNone(session_ref())

        replacement = ResponsesWebSocketSession(
            max_transcript_bytes=1000,
            transcript_budget=budget,
        )
        self.assertTrue(replacement.commit(
            {"model": "gpt-test", "input": "y" * 200},
            {"id": "resp_second", "output": []},
        ))
        replacement.close()

    def test_oversized_committed_output_preserves_current_response_and_rejects_continuation(self) -> None:
        session = ResponsesWebSocketSession(max_transcript_bytes=100)
        committed = session.commit(
            {"model": "gpt-test", "input": "small"},
            {
                "id": "resp_large",
                "output": [{"type": "message", "content": "x" * 200}],
            },
        )
        self.assertFalse(committed)

        with self.assertRaises(ResponsesWebSocketRequestError) as raised:
            session.prepare_turn({
                "type": "response.create",
                "model": "gpt-test",
                "previous_response_id": "resp_large",
                "input": "continue",
            })
        self.assertEqual(raised.exception.code, "websocket_session_too_large")
        session.close()

    def test_session_lifetime_uses_monotonic_deadline(self) -> None:
        now = [100.0]
        session = ResponsesWebSocketSession(
            max_lifetime_seconds=3600,
            clock=lambda: now[0],
        )
        self.assertEqual(session.remaining_lifetime_seconds(), 3600.0)
        now[0] = 3699.5
        self.assertEqual(session.remaining_lifetime_seconds(), 0.5)
        now[0] = 3701.0
        self.assertEqual(session.remaining_lifetime_seconds(), 0.0)


if __name__ == "__main__":
    unittest.main()
