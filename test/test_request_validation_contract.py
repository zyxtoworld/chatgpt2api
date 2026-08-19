from __future__ import annotations

import base64
import unittest
from unittest import mock

from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

import api.ai as ai_module
from api.ai import AnthropicMessageRequest
from api.accounts import create_router
from api.errors import install_exception_handlers
from services.protocol import openai_v1_chat_complete, openai_v1_response
from services.protocol.conversation import ImageOutput
from test.fixtures.image_inputs import image_fixture_bytes


class RequestValidationContractTests(unittest.TestCase):
    @staticmethod
    def _consume_protocol_result(result: object) -> None:
        if not isinstance(result, dict):
            list(result)

    def test_anthropic_request_rejects_noncanonical_scalar_types(self) -> None:
        for field, value in (("model", 7), ("stream", "false")):
            with self.subTest(field=field):
                with self.assertRaises(ValueError):
                    AnthropicMessageRequest(**{
                        "messages": [{"role": "user", "content": "hello"}],
                        field: value,
                    })

    def test_responses_text_stream_emits_content_part_lifecycle(self) -> None:
        with mock.patch.object(
            openai_v1_response,
            "stream_text_deltas",
            return_value=iter(("Hi", " there")),
        ):
            events = list(openai_v1_response.stream_text_response(
                object(),
                {"model": "gpt-5", "input": "hello", "stream": True},
                [{"role": "user", "content": "hello"}],
            ))

        self.assertEqual(
            [event["type"] for event in events],
            [
                "response.created",
                "response.output_item.added",
                "response.content_part.added",
                "response.output_text.delta",
                "response.output_text.delta",
                "response.output_text.done",
                "response.content_part.done",
                "response.output_item.done",
                "response.completed",
            ],
        )
        self.assertEqual(events[1]["item"]["content"], [])
        self.assertEqual(events[2]["part"], {
            "type": "output_text",
            "text": "",
            "annotations": [],
        })
        self.assertEqual(events[6]["part"], {
            "type": "output_text",
            "text": "Hi there",
            "annotations": [],
        })

    def test_responses_image_text_fallback_emits_content_part_lifecycle(self) -> None:
        events = list(openai_v1_response.stream_image_response(
            [ImageOutput(
                kind="message",
                model="gpt-image-2",
                index=1,
                total=1,
                text="policy summary",
                public_safe_text=True,
            )],
            "draw a cat",
            "gpt-5",
            usage_model="gpt-image-2",
        ))

        self.assertEqual(
            [event["type"] for event in events],
            [
                "response.created",
                "response.output_item.added",
                "response.content_part.added",
                "response.output_text.delta",
                "response.output_text.done",
                "response.content_part.done",
                "response.output_item.done",
                "response.completed",
            ],
        )
        self.assertEqual(events[1]["item"]["content"], [])
        self.assertEqual(events[5]["part"]["text"], "policy summary")

    def test_chat_stream_rejects_non_boolean_values_before_backend_selection(self) -> None:
        requests = (
            {
                "model": "auto",
                "messages": [{"role": "user", "content": "hello"}],
            },
            {
                "model": "gpt-image-2",
                "messages": [{"role": "user", "content": "draw a cat"}],
                "modalities": ["image"],
            },
            {
                "model": "auto",
                "messages": [{"role": "user", "content": "call the tool"}],
                "tools": [{
                    "type": "function",
                    "function": {"name": "lookup", "parameters": {}},
                }],
            },
        )
        invalid_values = ("false", 0, 1, [], {})

        with (
            mock.patch.object(
                openai_v1_chat_complete,
                "text_backend",
                side_effect=AssertionError("invalid stream must not select a text backend"),
            ),
            mock.patch.object(
                openai_v1_chat_complete,
                "stream_image_outputs_with_pool",
                side_effect=AssertionError("invalid stream must not select an image backend"),
            ),
            mock.patch.object(
                openai_v1_chat_complete.openai_v1_response,
                "stream_codex_response",
                side_effect=AssertionError("invalid stream must not select a Codex backend"),
            ),
        ):
            for request in requests:
                for value in invalid_values:
                    with self.subTest(model=request["model"], has_tools="tools" in request, value=value):
                        with self.assertRaises(HTTPException) as raised:
                            result = openai_v1_chat_complete.handle({**request, "stream": value})
                            self._consume_protocol_result(result)
                        self.assertEqual(raised.exception.status_code, 400)

    def test_chat_core_fields_reject_json_coercion_before_backend_selection(self) -> None:
        requests = (
            {
                "model": {},
                "messages": [{"role": "user", "content": "hello"}],
            },
            {
                "model": "auto",
                "messages": [{"role": "user", "content": "hello", "name": "participant"}],
            },
            {
                "model": "auto",
                "messages": [{"role": "user", "content": "hello", "future_field": "discarded"}],
            },
            {
                "model": "auto",
                "messages": [{"role": "user", "content": "hello"}, "discarded"],
            },
            {
                "model": "auto",
                "prompt": {"text": "coerced"},
            },
            {
                "model": "gpt-image-2",
                "messages": [{"role": "user", "content": "draw a cat"}],
                "modalities": ["image"],
                "n": "2",
            },
            {
                "model": "gpt-image-2",
                "messages": [{"role": "user", "content": "draw a cat"}],
                "modalities": ["image"],
                "n": 2.0,
            },
            {
                "model": "gpt-image-2",
                "messages": [{"role": "user", "content": "draw a cat"}],
                "modalities": ["image"],
                "n": True,
            },
            {
                "model": "gpt-image-2",
                "messages": [{"role": "user", "content": "draw a cat"}],
                "modalities": ["image"],
                "n": 0,
            },
            {
                "model": "gpt-image-2",
                "messages": [{"role": "user", "content": "draw a cat"}],
                "modalities": ["image"],
                "n": 5,
            },
        )

        with (
            mock.patch.object(
                openai_v1_chat_complete,
                "text_backend",
                side_effect=AssertionError("invalid core fields must not select a text backend"),
            ),
            mock.patch.object(
                openai_v1_chat_complete,
                "stream_image_outputs_with_pool",
                side_effect=AssertionError("invalid core fields must not select an image backend"),
            ),
        ):
            for request in requests:
                with self.subTest(request=request):
                    with self.assertRaises(HTTPException) as raised:
                        self._consume_protocol_result(openai_v1_chat_complete.handle(request))
                    self.assertEqual(raised.exception.status_code, 400)

    def test_chat_messages_require_content_for_non_assistant_roles(self) -> None:
        for role in ("system", "developer", "user", "tool"):
            with self.subTest(role=role):
                with self.assertRaises(HTTPException) as raised:
                    openai_v1_chat_complete.validate_chat_core_parameters({
                        "model": "auto",
                        "messages": [{"role": role}],
                    })
                self.assertEqual(raised.exception.status_code, 400)

        # Assistant tool-call messages may omit content by contract.
        openai_v1_chat_complete.validate_chat_core_parameters({
            "model": "auto",
            "messages": [{
                "role": "assistant",
                "tool_calls": [{
                    "id": "call-1",
                    "type": "function",
                    "function": {"name": "lookup", "arguments": "{}"},
                }],
            }],
        })

    def test_responses_core_fields_reject_json_coercion_before_backend_selection(self) -> None:
        requests = (
            {"model": {}, "input": "hello"},
            {"model": "auto", "input": "hello", "instructions": {"text": "coerced"}},
            {
                "model": "auto",
                "input": [{"role": "user", "content": "hello"}, "discarded"],
            },
            {"model": "auto", "input": "hello", "stream": "false"},
            {
                "model": "gpt-image-2",
                "input": "draw a cat",
                "tools": [{"type": "image_generation"}],
                "stream": 1,
            },
            {
                "model": "auto",
                "input": ["discarded"],
                "tools": [{"type": "function", "name": "lookup", "parameters": {}}],
            },
            {
                "model": "auto",
                "input": [{"type": "future_item", "payload": "must-not-be-ignored"}],
                "reasoning": {"summary": "auto"},
            },
            {
                "model": "auto",
                "input": [{
                    "role": "user",
                    "content": [
                        {"type": "input_text", "text": "summarize the file"},
                        {
                            "type": "input_file",
                            "file_data": "data:text/plain;base64,aGVsbG8=",
                            "filename": "sample.txt",
                        },
                    ],
                }],
            },
            {
                "model": "auto",
                "input": [{
                    "role": "user",
                    "content": [
                        {"type": "input_text", "text": "hello"},
                        {"type": "future_content_part", "payload": "must-not-be-ignored"},
                    ],
                }],
            },
            {
                "model": "auto",
                "input": [{
                    "role": "user",
                    "content": [{"type": "input_text", "text": {"coerced": True}}],
                }],
            },
            {
                "model": "auto",
                "input": [{
                    "role": "user",
                    "content": [{
                        "type": "input_text",
                        "text": "hello",
                        "prompt_cache_breakpoint": {"mode": "explicit"},
                    }],
                }],
            },
            {
                "model": "auto",
                "input": [{
                    "role": "user",
                    "content": [{
                        "type": "input_image",
                        "image_url": "data:image/png;base64,aW1hZ2U=",
                        "detail": "ultra",
                    }],
                }],
            },
            {
                "model": "auto",
                "input": [{
                    "role": "user",
                    "content": [{
                        "type": "input_image",
                        "file_id": "file_unsupported",
                        "detail": "auto",
                    }],
                }],
            },
            {
                "model": "auto",
                "input": [{
                    "role": "user",
                    "content": [{"type": "input_audio", "audio_url": {"coerced": True}}],
                }],
            },
            {
                "model": "auto",
                "input": [{"role": {"coerced": True}, "content": "hello"}],
            },
            {
                "model": "auto",
                "input": [{"role": "future_role", "content": "hello"}],
            },
            {
                "model": "auto",
                "input": [{"type": "message", "role": "user", "content": "hello", "phase": "commentary"}],
            },
            {
                "model": "auto",
                "input": [{"type": "message", "role": "assistant", "content": "hello", "phase": "future_phase"}],
            },
            {
                "model": "auto",
                "input": [{"type": "message", "role": "user", "content": "hello", "status": "failed"}],
            },
            {
                "model": "auto",
                "input": [{
                    "type": "message",
                    "role": "user",
                    "content": "hello",
                    "future_field": "must-not-be-ignored",
                }],
            },
            {
                "model": "auto",
                "input": [{
                    "type": "message",
                    "role": "user",
                    "content": [{
                        "type": "text",
                        "text": "hello",
                        "future_field": "must-not-reach-codex",
                    }],
                }],
                "tools": [{"type": "function", "name": "lookup", "parameters": {}}],
            },
            {
                "model": "auto",
                "input": [{"type": "image", "image_url": "https://example.test/image.png"}],
                "tools": [{"type": "function", "name": "lookup", "parameters": {}}],
            },
            {
                "model": "auto",
                "input": [{"type": "image_url", "image_url": "https://example.test/image.png"}],
                "tools": [{"type": "function", "name": "lookup", "parameters": {}}],
            },
            {
                "model": "auto",
                "input": [{"image_url": "https://example.test/image.png"}],
                "tools": [{"type": "function", "name": "lookup", "parameters": {}}],
            },
        )

        with (
            mock.patch.object(
                openai_v1_response,
                "text_backend",
                side_effect=AssertionError("invalid core fields must not select a text backend"),
            ),
            mock.patch.object(
                openai_v1_response,
                "stream_image_outputs_with_pool",
                side_effect=AssertionError("invalid core fields must not select an image backend"),
            ),
            mock.patch.object(
                openai_v1_response,
                "stream_codex_response",
                side_effect=AssertionError("invalid core fields must not select a Codex backend"),
            ),
        ):
            for request in requests:
                with self.subTest(request=request):
                    with self.assertRaises(HTTPException) as raised:
                        self._consume_protocol_result(openai_v1_response.handle(request))
                    self.assertEqual(raised.exception.status_code, 400)

    def test_responses_enum_fields_reject_containers_as_bad_requests(self) -> None:
        cases = (
            ("status", "user"),
            ("phase", "assistant"),
        )
        for field, role in cases:
            with self.subTest(field=field):
                body = {
                    "model": "auto",
                    "input": [{
                        "type": "message",
                        "role": role,
                        "content": "hello",
                        field: {"enum_canary": "must-not-500"},
                    }],
                }
                with self.assertRaises(HTTPException) as raised:
                    openai_v1_response.validate_response_core_parameters(body)
                self.assertEqual(raised.exception.status_code, 400)

    def test_responses_nested_enum_containers_are_rejected_before_protocol_dispatch(self) -> None:
        cases = (
            {
                "model": "auto",
                "input": [{
                    "type": "function_call_output",
                    "call_id": "call-1",
                    "output": "ok",
                    "status": [],
                }],
            },
            {
                "model": "auto",
                "input": [{
                    "type": "function_call",
                    "call_id": "call-1",
                    "name": "lookup",
                    "arguments": "{}",
                    "status": {},
                }],
            },
            {
                "model": "auto",
                "input": [{
                    "type": "reasoning",
                    "summary": [],
                    "status": {"bad": True},
                }],
            },
            {
                "model": "auto",
                "input": [{
                    "type": "reasoning",
                    "summary": [],
                    "content": [{"type": {"bad": True}, "text": "detail"}],
                }],
            },
            {
                "model": "auto",
                "input": [{
                    "type": "image_generation_call",
                    "status": {"bad": True},
                    "result": "image-result",
                }],
            },
            {
                "model": "auto",
                "input": [{
                    "type": "web_search_call",
                    "status": {"bad": True},
                }],
            },
            {
                "model": "auto",
                "input": [{
                    "type": "tool_search_call",
                    "status": {"bad": True},
                    "execution": "server",
                    "arguments": "{}",
                }],
            },
            {
                "model": "auto",
                "input": [{
                    "type": "tool_search_output",
                    "status": {"bad": True},
                    "execution": "server",
                    "tools": [],
                }],
            },
            {
                "model": "auto",
                "input": [{
                    "type": "message",
                    "role": "user",
                    "content": [{
                        "type": "input_image",
                        "image_url": "https://example.test/image.png",
                        "detail": [],
                    }],
                }],
            },
            {
                "model": "auto",
                "input": [{
                    "type": "function_call_output",
                    "call_id": "call-1",
                    "output": [{"type": {"bad": True}}],
                }],
            },
        )
        for body in cases:
            with self.subTest(body=body):
                with self.assertRaises(HTTPException) as raised:
                    openai_v1_response.validate_response_core_parameters(body)
                self.assertEqual(raised.exception.status_code, 400)

        with self.assertRaises(HTTPException) as raised:
            openai_v1_response.codex_response_payload({
                "model": "auto",
                "input": "hello",
                "service_tier": {"bad": True},
            })
        self.assertEqual(raised.exception.status_code, 400)

    def test_public_json_routes_reject_scalar_type_coercion(self) -> None:
        app = FastAPI()
        install_exception_handlers(app)
        app.include_router(ai_module.create_router())
        client = TestClient(app)
        png_data_url = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9Y9ZlS8AAAAASUVORK5CYII="
        requests = (
            (
                "/v1/chat/completions",
                {
                    "model": "auto",
                    "messages": [{"role": "user", "content": "hello"}],
                    "stream": "false",
                },
                422,
            ),
            (
                "/v1/responses",
                {"model": "auto", "input": "hello", "stream": "false"},
                422,
            ),
            (
                "/v1/images/generations",
                {"prompt": "draw a cat", "n": "2"},
                422,
            ),
            (
                "/v1/images/generations",
                {"prompt": "draw a cat", "stream": "false"},
                422,
            ),
            (
                "/v1/images/generations",
                {"prompt": "draw a cat", "output_compression": "80"},
                422,
            ),
            (
                "/v1/images/generations",
                {"prompt": "draw a cat", "instructions": "custom image instructions"},
                422,
            ),
            (
                "/v1/images/edits",
                {
                    "prompt": "edit a cat",
                    "image": png_data_url,
                    "n": "2",
                },
                400,
            ),
            (
                "/v1/images/edits",
                {
                    "prompt": "edit a cat",
                    "image": png_data_url,
                    "stream": "false",
                },
                400,
            ),
            (
                "/v1/images/edits",
                {
                    "prompt": "edit a cat",
                    "image": png_data_url,
                    "output_compression": "80",
                },
                400,
            ),
        )

        with (
            mock.patch.object(
                ai_module,
                "require_identity_async",
                return_value={"id": "user-1", "role": "user"},
            ),
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
            mock.patch.object(
                ai_module.LoggedCall,
                "run",
                new=mock.AsyncMock(return_value={"accepted": True}),
            ) as run,
        ):
            for path, body, status_code in requests:
                with self.subTest(path=path):
                    response = client.post(path, json=body)
                    self.assertEqual(response.status_code, status_code, response.text)
        run.assert_not_awaited()

    def test_anthropic_route_requires_version_header_before_auth_or_backend(self) -> None:
        app = FastAPI()
        install_exception_handlers(app)
        app.include_router(ai_module.create_router())

        with (
            mock.patch.object(
                ai_module,
                "require_identity_async",
                new=mock.AsyncMock(return_value={"id": "user-1", "role": "user"}),
            ) as require_identity,
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
            mock.patch.object(
                ai_module.LoggedCall,
                "run",
                new=mock.AsyncMock(return_value={"accepted": True}),
            ) as run,
        ):
            response = TestClient(app).post(
                "/v1/messages",
                headers={"x-api-key": "fixture-key"},
                json={
                    "model": "auto",
                    "max_tokens": 8,
                    "messages": [{"role": "user", "content": "hello"}],
                },
            )

        self.assertEqual(response.status_code, 400, response.text)
        self.assertEqual(response.json()["type"], "error")
        self.assertEqual(response.json()["error"]["type"], "invalid_request_error")
        require_identity.assert_not_awaited()
        run.assert_not_awaited()

    def test_anthropic_route_rejects_unsupported_version_before_auth_or_backend(self) -> None:
        app = FastAPI()
        install_exception_handlers(app)
        app.include_router(ai_module.create_router())

        with (
            mock.patch.object(
                ai_module,
                "require_identity_async",
                new=mock.AsyncMock(return_value={"id": "user-1", "role": "user"}),
            ) as require_identity,
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
            mock.patch.object(
                ai_module.LoggedCall,
                "run",
                new=mock.AsyncMock(return_value={"accepted": True}),
            ) as run,
        ):
            response = TestClient(app).post(
                "/v1/messages",
                headers={
                    "x-api-key": "fixture-key",
                    "anthropic-version": "2099-01-01",
                },
                json={
                    "model": "auto",
                    "max_tokens": 8,
                    "messages": [{"role": "user", "content": "hello"}],
                },
            )

        self.assertEqual(response.status_code, 400, response.text)
        self.assertEqual(response.json()["type"], "error")
        self.assertEqual(response.json()["error"]["type"], "invalid_request_error")
        require_identity.assert_not_awaited()
        run.assert_not_awaited()

    def test_anthropic_route_rejects_non_array_messages_before_backend(self) -> None:
        app = FastAPI()
        install_exception_handlers(app)
        app.include_router(ai_module.create_router())

        async def invoke(_call, handler, payload, **_kwargs):
            return handler(payload)

        for messages in (None, {"secret": "message-container-canary"}):
            with self.subTest(messages=messages):
                with (
                    mock.patch.object(
                        ai_module,
                        "require_identity_async",
                        new=mock.AsyncMock(return_value={"id": "user-1", "role": "user"}),
                    ),
                    mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
                    mock.patch.object(ai_module.LoggedCall, "run", new=invoke),
                    mock.patch.object(
                        ai_module.anthropic_v1_messages.account_service,
                        "get_text_access_token",
                        return_value="fixture-token",
                    ) as selector,
                    mock.patch.object(ai_module.anthropic_v1_messages, "OpenAIBackendAPI") as backend,
                ):
                    response = TestClient(app).post(
                        "/v1/messages",
                        headers={"x-api-key": "fixture-key", "anthropic-version": "2023-06-01"},
                        json={"model": "auto", "messages": messages},
                    )

                self.assertEqual(response.status_code, 400, response.text)
                self.assertNotIn("fixture-token", response.text)
                self.assertNotIn("message-container-canary", response.text)
                selector.assert_not_called()
        backend.assert_not_called()

    def test_anthropic_route_rejects_known_unsupported_field_before_account_selection(self) -> None:
        app = FastAPI()
        install_exception_handlers(app)
        app.include_router(ai_module.create_router())

        async def invoke(_call, handler, payload, **_kwargs):
            return handler(payload)

        with (
            mock.patch.object(
                ai_module,
                "require_identity_async",
                new=mock.AsyncMock(return_value={"id": "user-1", "role": "user"}),
            ),
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
            mock.patch.object(ai_module.LoggedCall, "run", new=invoke),
            mock.patch.object(
                ai_module.anthropic_v1_messages.account_service,
                "get_text_access_token",
                return_value="fixture-token",
            ) as selector,
            mock.patch.object(ai_module.anthropic_v1_messages, "OpenAIBackendAPI") as backend,
        ):
            response = TestClient(app).post(
                "/v1/messages",
                headers={"x-api-key": "fixture-key", "anthropic-version": "2023-06-01"},
                json={
                    "model": "auto",
                    "temperature": 0.2,
                    "messages": [{"role": "user", "content": "hello"}],
                },
            )

        self.assertEqual(response.status_code, 400, response.text)
        self.assertIn("temperature", response.text)
        selector.assert_not_called()
        backend.assert_not_called()

    def test_anthropic_route_forwards_extra_body_to_adapter_without_pydantic_drop(self) -> None:
        app = FastAPI()
        install_exception_handlers(app)
        app.include_router(ai_module.create_router())

        async def invoke(_call, _handler, payload, **_kwargs):
            return {"forwarded": payload}

        body = {
            "model": "auto",
            "max_tokens": 32,
            "messages": [{"role": "user", "content": "hello"}],
            "tools": [{
                "name": "lookup",
                "description": "Look up a value",
                "input_schema": {"type": "object"},
            }],
            "metadata": {"user_id": "fixture-user"},
        }
        with (
            mock.patch.object(
                ai_module,
                "require_identity_async",
                new=mock.AsyncMock(return_value={"id": "user-1", "role": "user"}),
            ) as require_identity,
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
            mock.patch.object(ai_module.LoggedCall, "run", new=invoke),
        ):
            response = TestClient(app).post(
                "/v1/messages",
                headers={"x-api-key": "fixture-key", "anthropic-version": "2023-06-01"},
                json=body,
            )

        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json()["forwarded"]
        self.assertEqual(payload["max_tokens"], 32)
        self.assertEqual(payload["tools"], body["tools"])
        self.assertEqual(payload["metadata"], body["metadata"])
        require_identity.assert_awaited_once_with("Bearer fixture-key")

    def test_anthropic_route_accepts_sdk_shape_through_adapter(self) -> None:
        app = FastAPI()
        install_exception_handlers(app)
        app.include_router(ai_module.create_router())

        async def invoke(_call, handler, payload, **_kwargs):
            return handler(payload)

        backend = mock.Mock()
        with (
            mock.patch.object(
                ai_module,
                "require_identity_async",
                new=mock.AsyncMock(return_value={"id": "user-1", "role": "user"}),
            ),
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
            mock.patch.object(ai_module.LoggedCall, "run", new=invoke),
            mock.patch.object(
                ai_module.anthropic_v1_messages.account_service,
                "get_text_access_token",
                return_value="fixture-token",
            ),
            mock.patch.object(ai_module.anthropic_v1_messages, "OpenAIBackendAPI", return_value=backend),
            mock.patch.object(
                ai_module.anthropic_v1_messages,
                "stream_text_chat_completion",
                return_value=iter(({"choices": [{"delta": {"content": "hello"}, "finish_reason": "stop"}]},)),
            ),
        ):
            response = TestClient(app).post(
                "/v1/messages",
                headers={"x-api-key": "fixture-key", "anthropic-version": "2023-06-01"},
                json={
                    "model": "auto",
                    "max_tokens": 32,
                    "messages": [{"role": "user", "content": "hello"}],
                    "tools": [{
                        "name": "lookup",
                        "description": "Look up a value",
                        "input_schema": {"type": "object"},
                    }],
                },
            )

        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json()
        self.assertEqual(payload["type"], "message")
        self.assertEqual(payload["content"], [{"type": "text", "text": "hello"}])
        self.assertEqual(payload["stop_reason"], "end_turn")
        backend.close.assert_called_once_with()

    def test_anthropic_route_rejects_malformed_image_block_before_backend(self) -> None:
        app = FastAPI()
        install_exception_handlers(app)
        app.include_router(ai_module.create_router())

        async def invoke(_call, handler, payload, **_kwargs):
            return handler(payload)

        with (
            mock.patch.object(
                ai_module,
                "require_identity_async",
                new=mock.AsyncMock(return_value={"id": "user-1", "role": "user"}),
            ),
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
            mock.patch.object(ai_module.LoggedCall, "run", new=invoke),
            mock.patch.object(
                ai_module.anthropic_v1_messages.account_service,
                "get_text_access_token",
                return_value="fixture-token",
            ) as selector,
            mock.patch.object(ai_module.anthropic_v1_messages, "OpenAIBackendAPI") as backend,
        ):
            response = TestClient(app).post(
                "/v1/messages",
                headers={"x-api-key": "fixture-key", "anthropic-version": "2023-06-01"},
                json={
                    "model": "auto",
                    "messages": [{
                        "role": "user",
                        "content": [{
                            "type": "image",
                            "source": {
                                "type": "base64",
                                "media_type": "image/png",
                                "data": {"canary": "route-image-canary"},
                            },
                        }],
                    }],
                },
            )

        self.assertEqual(response.status_code, 400, response.text)
        self.assertNotIn("route-image-canary", response.text)
        selector.assert_not_called()
        backend.assert_not_called()

    def test_anthropic_route_rejects_malformed_content_tools_and_system_before_backend(self) -> None:
        app = FastAPI()
        install_exception_handlers(app)
        app.include_router(ai_module.create_router())

        async def invoke(_call, handler, payload, **_kwargs):
            return handler(payload)

        cases = (
            (
                {
                    "messages": [{
                        "role": "user",
                        "content": [{"type": "text", "text": "valid"}, "route-content-canary"],
                    }],
                },
                "route-content-canary",
            ),
            (
                {
                    "messages": [{"role": "user", "content": "use tools"}],
                    "tools": [{"name": "valid"}, "route-tool-canary"],
                },
                "route-tool-canary",
            ),
            (
                {
                    "messages": [{"role": "user", "content": "hello"}],
                    "system": ["route-system-canary"],
                },
                "route-system-canary",
            ),
            (
                {
                    "messages": [{"role": "user", "content": "hello"}],
                    "system": [{"type": "image", "source": {"url": "route-system-type-canary"}}],
                },
                "route-system-type-canary",
            ),
        )

        for body, canary in cases:
            with self.subTest(canary=canary):
                with (
                    mock.patch.object(
                        ai_module,
                        "require_identity_async",
                        new=mock.AsyncMock(return_value={"id": "user-1", "role": "user"}),
                    ),
                    mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
                    mock.patch.object(ai_module.LoggedCall, "run", new=invoke),
                    mock.patch.object(
                        ai_module.anthropic_v1_messages.account_service,
                        "get_text_access_token",
                        return_value="fixture-token",
                    ) as selector,
                    mock.patch.object(ai_module.anthropic_v1_messages, "OpenAIBackendAPI") as backend,
                ):
                    response = TestClient(app).post(
                        "/v1/messages",
                        headers={"x-api-key": "fixture-key", "anthropic-version": "2023-06-01"},
                        json={"model": "auto", **body},
                    )

                self.assertEqual(response.status_code, 400, response.text)
                self.assertNotIn(canary, response.text)
                self.assertNotIn("fixture-token", response.text)
                selector.assert_not_called()
                backend.assert_not_called()

    def test_anthropic_tool_result_content_array_requires_object_blocks(self) -> None:
        with self.assertRaises(HTTPException) as raised:
            ai_module.anthropic_v1_messages.message_request({
                "model": "auto",
                "messages": [{
                    "role": "user",
                    "content": [{
                        "type": "tool_result",
                        "tool_use_id": "toolu_fixture",
                        "content": [{"type": "text", "text": "valid"}, "tool-result-canary"],
                    }],
                }],
            })

        self.assertEqual(raised.exception.status_code, 400)
        self.assertNotIn("tool-result-canary", str(raised.exception.detail))

    def test_anthropic_tool_result_object_blocks_are_preserved(self) -> None:
        backend = mock.Mock()
        with (
            mock.patch.object(
                ai_module.anthropic_v1_messages.account_service,
                "get_text_access_token",
                return_value="fixture-token",
            ),
            mock.patch.object(
                ai_module.anthropic_v1_messages,
                "OpenAIBackendAPI",
                return_value=backend,
            ),
        ):
            request = ai_module.anthropic_v1_messages.message_request({
                "model": "auto",
                "messages": [{
                    "role": "user",
                    "content": [{
                        "type": "tool_result",
                        "tool_use_id": "toolu_fixture",
                        "content": [{"type": "text", "text": "valid result"}],
                    }],
                }],
            })

        self.assertIs(request.backend, backend)
        self.assertIn("valid result", request.messages[0]["content"])

    def test_public_responses_preserves_context_management(self) -> None:
        app = FastAPI()
        install_exception_handlers(app)
        app.include_router(ai_module.create_router())
        context_management = [{"type": "compaction", "compact_threshold": 200000}]

        with (
            mock.patch.object(
                ai_module,
                "require_identity_async",
                return_value={"id": "user-1", "role": "user"},
            ),
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
            mock.patch.object(
                ai_module.LoggedCall,
                "run",
                new=mock.AsyncMock(return_value={"id": "resp_compacted", "output": []}),
            ) as run,
        ):
            response = TestClient(app).post(
                "/v1/responses",
                json={
                    "model": "gpt-5.3-codex",
                    "input": "continue",
                    "context_management": context_management,
                },
            )

        self.assertEqual(response.status_code, 200, response.text)
        payload = run.await_args.args[1]
        self.assertEqual(payload["context_management"], context_management)

    def test_responses_image_tool_model_is_separate_from_the_response_model(self) -> None:
        for tool in (
            {"type": "image_generation"},
            {"type": "image_generation", "model": "gpt-image-2"},
        ):
            seen = []

            def image_outputs(request):
                seen.append(request)
                yield ImageOutput(
                    kind="result",
                    model=request.model,
                    index=1,
                    total=1,
                    data=[{"b64_json": "aW1hZ2U="}],
                )

            with (
                self.subTest(tool=tool),
                mock.patch.object(
                    openai_v1_response,
                    "stream_image_outputs_with_pool",
                    side_effect=image_outputs,
                ),
            ):
                response = openai_v1_response.handle({
                    "model": "gpt-5",
                    "input": "draw a cat",
                    "tools": [tool],
                })

            self.assertIsInstance(response, dict)
            self.assertEqual(response["model"], "gpt-5")
            self.assertEqual(seen[0].model, "gpt-image-2")

    def test_responses_image_tool_rejects_unsupported_model_before_backend_selection(self) -> None:
        for image_model in ("gpt-image-1", {"name": "gpt-image-2"}):
            with (
                self.subTest(image_model=image_model),
                mock.patch.object(openai_v1_response, "stream_image_outputs_with_pool") as backend,
                self.assertRaises(HTTPException) as raised,
            ):
                openai_v1_response.handle({
                    "model": "gpt-5",
                    "input": "draw a cat",
                    "tools": [{"type": "image_generation", "model": image_model}],
                })

            self.assertEqual(raised.exception.status_code, 400)
            backend.assert_not_called()

    def test_responses_image_tool_preserves_supported_rendering_options(self) -> None:
        seen = []

        def image_outputs(request):
            seen.append(request)
            yield ImageOutput(
                kind="result",
                model=request.model,
                index=1,
                total=1,
                data=[{"b64_json": "aW1hZ2U="}],
            )

        with mock.patch.object(
            openai_v1_response,
            "stream_image_outputs_with_pool",
            side_effect=image_outputs,
        ):
            response = openai_v1_response.handle({
                "model": "gpt-5",
                "input": "draw a cat",
                "tools": [{
                    "type": "image_generation",
                    "model": "gpt-image-2",
                    "action": "auto",
                    "background": "auto",
                    "moderation": "auto",
                    "output_format": "webp",
                    "output_compression": 55,
                    "partial_images": 0,
                    "quality": "high",
                    "size": "1920x1088",
                }],
            })

        self.assertIsInstance(response, dict)
        self.assertEqual(response["model"], "gpt-5")
        request = seen[0]
        self.assertEqual(request.model, "gpt-image-2")
        self.assertEqual(request.output_format, "webp")
        self.assertEqual(request.output_compression, 55)
        self.assertEqual(request.background, "auto")
        self.assertEqual(request.quality, "high")
        self.assertEqual(request.size, "1920x1088")

    def test_responses_generate_action_rejects_input_image_before_remote_download(self) -> None:
        with (
            mock.patch(
                "utils.helper.download_remote_image",
                side_effect=AssertionError("action mismatch must be rejected before image I/O"),
            ) as download,
            mock.patch.object(openai_v1_response, "stream_image_outputs_with_pool") as backend,
            self.assertRaises(HTTPException) as raised,
        ):
            openai_v1_response.handle({
                "model": "gpt-5",
                "input": [{
                    "role": "user",
                    "content": [
                        {"type": "input_text", "text": "draw a cat"},
                        {"type": "input_image", "image_url": "https://public.example/image.png"},
                    ],
                }],
                "tools": [{"type": "image_generation", "action": "generate"}],
            })

        self.assertEqual(raised.exception.status_code, 400)
        download.assert_not_called()
        backend.assert_not_called()

    def test_chat_and_responses_reject_invalid_local_images_before_backend_selection(self) -> None:
        invalid_image = "data:image/png;base64," + base64.b64encode(b"not-an-image").decode("ascii")
        requests = (
            (
                openai_v1_chat_complete,
                {
                    "model": "gpt-image-2",
                    "messages": [{
                        "role": "user",
                        "content": [
                            {"type": "text", "text": "edit cat"},
                            {"type": "image_url", "image_url": {"url": invalid_image}},
                        ],
                    }],
                },
            ),
            (
                openai_v1_response,
                {
                    "model": "gpt-5",
                    "input": [{
                        "role": "user",
                        "content": [
                            {"type": "input_text", "text": "edit cat"},
                            {"type": "input_image", "image_url": invalid_image},
                        ],
                    }],
                    "tools": [{"type": "image_generation", "action": "edit"}],
                },
            ),
        )

        for protocol, body in requests:
            with (
                self.subTest(protocol=protocol.__name__),
                mock.patch.object(
                    protocol,
                    "stream_image_outputs_with_pool",
                    side_effect=AssertionError("invalid image must not reach the backend"),
                ) as backend,
                self.assertRaises(HTTPException) as raised,
            ):
                protocol.handle(body)

            self.assertEqual(raised.exception.status_code, 400)
            backend.assert_not_called()

    def test_responses_edit_action_requires_and_routes_an_input_image(self) -> None:
        seen = []
        data_url = "data:image/png;base64," + base64.b64encode(
            image_fixture_bytes("image.png")
        ).decode("ascii")

        def image_outputs(request):
            seen.append(request)
            yield ImageOutput(
                kind="result",
                model=request.model,
                index=1,
                total=1,
                data=[{"b64_json": "aW1hZ2U="}],
            )

        with mock.patch.object(
            openai_v1_response,
            "stream_image_outputs_with_pool",
            side_effect=image_outputs,
        ):
            response = openai_v1_response.handle({
                "model": "gpt-5",
                "input": [{
                    "role": "user",
                    "content": [
                        {"type": "input_text", "text": "edit the cat"},
                        {"type": "input_image", "image_url": data_url},
                    ],
                }],
                "tools": [{"type": "image_generation", "action": "edit"}],
            })

        self.assertIsInstance(response, dict)
        self.assertTrue(seen[0].images)

        with (
            mock.patch.object(openai_v1_response, "stream_image_outputs_with_pool") as backend,
            self.assertRaises(HTTPException) as raised,
        ):
            openai_v1_response.handle({
                "model": "gpt-5",
                "input": "edit the cat",
                "tools": [{"type": "image_generation", "action": "edit"}],
            })

        self.assertEqual(raised.exception.status_code, 400)
        backend.assert_not_called()

    def test_responses_image_tool_rejects_options_the_backend_cannot_honor(self) -> None:
        unsupported_options = (
            {"input_fidelity": "high"},
            {"input_image_mask": {"image_url": "data:image/png;base64,aW1hZ2U="}},
            {"background": "transparent"},
            {"moderation": "low"},
            {"partial_images": 1},
            {"output_format": "png", "output_compression": 50},
        )
        for options in unsupported_options:
            with (
                self.subTest(options=options),
                mock.patch.object(openai_v1_response, "stream_image_outputs_with_pool") as backend,
                self.assertRaises(HTTPException) as raised,
            ):
                openai_v1_response.handle({
                    "model": "gpt-5",
                    "input": "draw a cat",
                    "tools": [{"type": "image_generation", **options}],
                })

            self.assertEqual(raised.exception.status_code, 400)
            backend.assert_not_called()

    def test_public_ai_routes_reject_unhonored_parameters_before_backend_selection(self) -> None:
        app = FastAPI()
        install_exception_handlers(app)
        app.include_router(ai_module.create_router())
        client = TestClient(app)
        requests = (
            ("/v1/responses", {"model": "auto", "input": "hello", "temperature": 0.2}),
            (
                "/v1/responses",
                {
                    "model": "gpt-image-2",
                    "input": "draw a cat",
                    "instructions": "custom image instructions",
                    "tools": [{"type": "image_generation"}],
                },
            ),
            (
                "/v1/responses",
                {
                    "model": "gpt-image-2",
                    "input": "draw a cat",
                    "tools": [
                        {
                            "type": "image_generation",
                            "quality": {"value": "high"},
                        }
                    ],
                },
            ),
            (
                "/v1/chat/completions",
                {
                    "model": "auto",
                    "messages": [{"role": "user", "content": "hello"}],
                    "max_completion_tokens": 32,
                },
            ),
            (
                "/v1/responses",
                {
                    "model": "auto",
                    "input": "hello",
                    "tools": [{"type": "function", "name": "answer", "parameters": {}}],
                    "tool_choice": {"type": "function", "name": "answer"},
                },
            ),
            (
                "/v1/chat/completions",
                {
                    "model": "auto",
                    "messages": [{"role": "user", "content": "hello"}],
                    "tools": [{
                        "type": "function",
                        "function": {"name": "answer", "parameters": {}},
                    }],
                    "tool_choice": {"type": "function", "function": {"name": "answer"}},
                },
            ),
        )

        with (
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
            mock.patch(
                "services.protocol.openai_v1_response.text_backend",
                side_effect=AssertionError("invalid parameters must not select an account"),
            ),
            mock.patch(
                "services.protocol.openai_v1_chat_complete.text_backend",
                side_effect=AssertionError("invalid parameters must not select an account"),
            ),
            mock.patch(
                "services.protocol.openai_v1_response.stream_image_outputs_with_pool",
                side_effect=AssertionError("invalid parameters must not select an image account"),
            ),
            mock.patch(
                "services.protocol.openai_v1_response.account_service.get_text_access_token",
                side_effect=AssertionError("invalid parameters must not select a Codex account"),
            ),
        ):
            for path, body in requests:
                with self.subTest(path=path):
                    response = client.post(
                        path,
                        headers={"Authorization": "Bearer chatgpt2api"},
                        json=body,
                    )
                    self.assertEqual(response.status_code, 400, response.text)

    def test_management_validation_does_not_echo_secret_input(self) -> None:
        secret = "validation-secret-opaque owner@example.com"
        app = FastAPI()
        install_exception_handlers(app)
        app.include_router(create_router())

        response = TestClient(app).post(
            "/api/accounts",
            json={"tokens": {"secret": secret}},
        )

        self.assertEqual(response.status_code, 422, response.text)
        self.assertNotIn(secret, response.text)
        self.assertNotIn('"input"', response.text)


if __name__ == "__main__":
    unittest.main()
