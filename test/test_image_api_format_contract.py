from __future__ import annotations

import asyncio
import base64
import io
import unittest
from unittest import mock

from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from PIL import Image
from starlette.datastructures import FormData, UploadFile
from starlette.requests import Request as StarletteRequest

import api.ai as ai_module
import api.image_inputs as image_inputs_module
import services.protocol.openai_v1_image_edit as image_edit_module
import services.protocol.openai_v1_image_generations as image_generation_module
import services.protocol.conversation as conversation_module
import utils.helper as helper_module
import utils.image_tokens as image_tokens_module
from services.protocol.conversation import (
    ImageGenerationError,
    ImageOutput,
    format_image_result,
    stream_image_events,
)
from services.protocol.openai_v1_response import image_output_items
from test.fixtures.image_inputs import image_fixture_bytes
from utils.helper import build_chat_image_markdown_content


AUTH_HEADERS = {"Authorization": "Bearer chatgpt2api"}
PNG_BYTES = image_fixture_bytes("image.png")
PNG_DATA_URL = "data:image/png;base64," + base64.b64encode(PNG_BYTES).decode("ascii")


def _image_data_url(*, mode: str, size: tuple[int, int], image_format: str) -> str:
    payload = io.BytesIO()
    color = (255, 0, 0, 128) if mode == "RGBA" else (255, 0, 0)
    Image.new(mode, size, color).save(payload, format=image_format)
    mime = "image/jpeg" if image_format == "JPEG" else "image/png"
    return f"data:{mime};base64,{base64.b64encode(payload.getvalue()).decode('ascii')}"


class ImageAPIFormatContractTests(unittest.TestCase):
    def test_multipart_parse_rejection_closes_uploaded_files(self) -> None:
        app = FastAPI()
        app.include_router(ai_module.create_router())
        client = TestClient(app, raise_server_exceptions=False)
        source = UploadFile(filename="image.png", file=io.BytesIO(PNG_BYTES))
        source.close = mock.AsyncMock()
        form = FormData([
            ("image", source),
            ("future_parameter", "rejected"),
        ])

        with (
            mock.patch.object(ai_module, "require_identity_async", new=mock.AsyncMock(return_value={"id": "test"})),
            mock.patch.object(StarletteRequest, "form", new=mock.AsyncMock(return_value=form)),
        ):
            response = client.post("/v1/images/edits", headers=AUTH_HEADERS)

        self.assertEqual(response.status_code, 400, response.text)
        source.close.assert_awaited_once_with()

    def test_edit_rejection_after_parse_closes_uploaded_files(self) -> None:
        app = FastAPI()
        app.include_router(ai_module.create_router())
        client = TestClient(app, raise_server_exceptions=False)
        source = UploadFile(filename="image.png", file=io.BytesIO(PNG_BYTES))
        source.close = mock.AsyncMock()
        form = FormData([
            ("image", source),
            ("prompt", "edit cat"),
            ("client_task_id", "not-supported-here"),
        ])

        with (
            mock.patch.object(ai_module, "require_identity_async", new=mock.AsyncMock(return_value={"id": "test"})),
            mock.patch.object(StarletteRequest, "form", new=mock.AsyncMock(return_value=form)),
        ):
            response = client.post("/v1/images/edits", headers=AUTH_HEADERS)

        self.assertEqual(response.status_code, 400, response.text)
        source.close.assert_awaited_once_with()

    def test_edit_filter_failure_closes_uploaded_files(self) -> None:
        app = FastAPI()
        app.include_router(ai_module.create_router())
        client = TestClient(app, raise_server_exceptions=False)
        source = UploadFile(filename="image.png", file=io.BytesIO(PNG_BYTES))
        source.close = mock.AsyncMock()
        form = FormData([
            ("image", source),
            ("prompt", "edit cat"),
        ])

        with (
            mock.patch.object(ai_module, "require_identity_async", new=mock.AsyncMock(return_value={"id": "test"})),
            mock.patch.object(StarletteRequest, "form", new=mock.AsyncMock(return_value=form)),
            mock.patch.object(
                ai_module,
                "filter_or_log",
                new=mock.AsyncMock(side_effect=HTTPException(status_code=400, detail={"error": "rejected"})),
            ),
        ):
            response = client.post("/v1/images/edits", headers=AUTH_HEADERS)

        self.assertEqual(response.status_code, 400, response.text)
        source.close.assert_awaited_once_with()

    def test_upload_close_failure_remains_retryable_for_outer_cleanup(self) -> None:
        source = UploadFile(filename="image.png", file=io.BytesIO(PNG_BYTES))
        source.close = mock.AsyncMock(side_effect=[RuntimeError("first close failed"), None])

        async def scenario() -> None:
            await image_inputs_module.read_image_sources([source])
            await image_inputs_module.close_image_sources([source])

        asyncio.run(scenario())
        self.assertEqual(source.close.await_count, 2)

    def test_upload_image_is_read_with_a_bounded_chunk_before_size_rejection(self) -> None:
        source = UploadFile(filename="huge.png", file=io.BytesIO())
        read_sizes: list[int] = []

        async def read(size: int = -1) -> bytes:
            read_sizes.append(size)
            return b"x" * (image_inputs_module.MAX_IMAGE_REFERENCE_BYTES + 1)

        source.read = read
        source.close = mock.AsyncMock()

        async def scenario() -> None:
            with self.assertRaises(HTTPException):
                await image_inputs_module.read_image_sources([source])

        asyncio.run(scenario())
        self.assertEqual(len(read_sizes), 1)
        self.assertGreater(read_sizes[0], 0)
        self.assertLessEqual(read_sizes[0], image_inputs_module.MAX_IMAGE_REFERENCE_BYTES + 1)
        source.close.assert_awaited_once_with()

    def test_upload_image_reads_valid_payload_in_bounded_chunks(self) -> None:
        source = UploadFile(filename="image.png", file=io.BytesIO())
        cursor = 0
        read_sizes: list[int] = []

        async def read(size: int = -1) -> bytes:
            nonlocal cursor
            read_sizes.append(size)
            if cursor >= len(PNG_BYTES):
                return b""
            chunk = PNG_BYTES[cursor:cursor + size]
            cursor += len(chunk)
            return chunk

        source.read = read
        source.close = mock.AsyncMock()

        async def scenario() -> list[image_inputs_module.ImageInput]:
            return await image_inputs_module.read_image_sources([source])

        result = asyncio.run(scenario())
        self.assertEqual(result, [(PNG_BYTES, "image.png", "image/png")])
        self.assertTrue(read_sizes)
        self.assertTrue(all(0 < size <= image_inputs_module.MAX_IMAGE_REFERENCE_BYTES + 1 for size in read_sizes))
        source.close.assert_awaited_once_with()

    def test_image_reference_expansion_rejects_count_and_depth_before_recursion(self) -> None:
        with self.assertRaises(HTTPException):
            image_inputs_module._sources_from_value([PNG_DATA_URL] * 17)

        deeply_nested: object = PNG_DATA_URL
        for _ in range(1000):
            deeply_nested = [deeply_nested]
        with self.assertRaises(HTTPException):
            image_inputs_module._sources_from_value(deeply_nested)

    def test_chat_image_input_rejects_predicted_overflow_before_decode(self) -> None:
        encoded = "A" * 8  # 6 decoded bytes; the patched contract limit is 4.

        with (
            mock.patch.object(helper_module, "MAX_JSON_IMAGE_BYTES", 4),
            mock.patch.object(
                image_tokens_module.base64,
                "b64decode",
                wraps=image_tokens_module.base64.b64decode,
            ) as decode,
        ):
            with self.assertRaises(HTTPException):
                helper_module.normalize_json_edit_images(image=encoded)

        decode.assert_not_called()

    def test_image_input_base64_paths_reject_predicted_overflow_before_decode(self) -> None:
        encoded = "A" * 8  # 6 decoded bytes; the patched contract limit is 4.

        with (
            mock.patch.object(image_inputs_module, "MAX_IMAGE_REFERENCE_BYTES", 4),
            mock.patch.object(
                image_tokens_module.base64,
                "b64decode",
                wraps=image_tokens_module.base64.b64decode,
            ) as decode,
        ):
            with self.assertRaises(HTTPException):
                image_inputs_module._decode_base64_image(encoded, "image.png", "image/png")
            with self.assertRaises(HTTPException):
                image_inputs_module._decode_data_url("data:image/png;base64," + encoded)

        decode.assert_not_called()

    def test_image_input_base64_rejects_container_without_stringifying_it(self) -> None:
        class ExplodingValue:
            def __str__(self):
                raise AssertionError("container must not be stringified")

        with mock.patch.object(image_tokens_module.base64, "b64decode") as decode:
            with self.assertRaises(HTTPException):
                image_inputs_module._decode_base64_image(ExplodingValue(), "image.png", "image/png")

        decode.assert_not_called()

    def test_image_input_base64_type_gate_does_not_call_string_conversion(self) -> None:
        class StringifyProbe:
            calls = 0

            def __str__(self):
                type(self).calls += 1
                return "QUJD"

        value = StringifyProbe()
        with mock.patch.object(image_tokens_module.base64, "b64decode") as decode:
            with self.assertRaises(HTTPException):
                image_inputs_module._decode_base64_image(value, "image.png", "image/png")

        self.assertEqual(StringifyProbe.calls, 0)
        decode.assert_not_called()

    def test_non_base64_data_url_preflights_decoded_size_before_unquote(self) -> None:
        cases = (
            ("raw exact", "abc", 3, True),
            ("percent exact", "%41%42", 2, True),
            ("percent utf8 exact", "%E4%BD%A0", 3, True),
            ("utf8 exact", "你", 3, True),
            ("raw overflow", "abcd", 3, False),
            ("percent overflow", "%41%42%43", 2, False),
            ("overflow before invalid percent", "abcd%G1", 3, False),
            ("invalid percent", "%G1", 8, False),
        )

        for label, payload, max_bytes, should_decode in cases:
            with self.subTest(label=label):
                with (
                    mock.patch.object(image_inputs_module, "MAX_IMAGE_REFERENCE_BYTES", max_bytes),
                    mock.patch.object(
                        image_inputs_module,
                        "_validated_image_input",
                        side_effect=lambda data, filename, mime: (data, filename, mime),
                    ),
                    mock.patch.object(
                        image_inputs_module,
                        "unquote_to_bytes",
                        wraps=image_inputs_module.unquote_to_bytes,
                    ) as unquote,
                ):
                    if should_decode:
                        result = image_inputs_module._decode_data_url("data:image/png," + payload)
                        expected = {
                            "你": "你".encode("utf-8"),
                            "%E4%BD%A0": "你".encode("utf-8"),
                        }.get(payload, payload.replace("%41", "A").replace("%42", "B").encode())
                        self.assertEqual(result[0], expected)
                        unquote.assert_called_once_with(payload)
                    else:
                        with self.assertRaises(HTTPException):
                            image_inputs_module._decode_data_url("data:image/png," + payload)
                        unquote.assert_not_called()

    def test_chat_image_markdown_does_not_stringify_container_b64(self) -> None:
        canary = "chat-markdown-container-secret"

        rendered = build_chat_image_markdown_content({
            "data": [{"b64_json": {"secret": canary}}],
        })

        self.assertNotIn(canary, rendered)
        self.assertEqual(rendered, "Image generation completed.")

    def test_stream_image_events_rejects_container_b64_without_serializing_it(self) -> None:
        canary = "image-stream-container-secret"
        output = ImageOutput(
            kind="result",
            model="gpt-image-2",
            index=1,
            total=1,
            data=[{"b64_json": {"secret": canary}}],
        )

        stream = stream_image_events([output], lambda _items: {"total_tokens": 1})
        with self.assertRaises(ImageGenerationError) as raised:
            next(stream)

        self.assertNotIn(canary, str(raised.exception))

    def test_stream_image_events_rejects_malformed_b64_before_emitting(self) -> None:
        output = ImageOutput(
            kind="result",
            model="gpt-image-2",
            index=1,
            total=1,
            data=[{"b64_json": "not-base64"}],
        )
        stream = stream_image_events([output], lambda _items: {"total_tokens": 1})

        with self.assertRaises(ImageGenerationError):
            next(stream)

    def test_malformed_upstream_base64_maps_to_safe_image_error(self) -> None:
        with self.assertRaises(ImageGenerationError) as raised:
            format_image_result(
                [{"b64_json": "not-base64-canary"}],
                "cat",
                "b64_json",
            )

        self.assertEqual(raised.exception.code, "upstream_error")
        self.assertNotIn("not-base64-canary", str(raised.exception))

    def test_upstream_base64_decoded_size_is_bounded_before_image_processing(self) -> None:
        with mock.patch.object(conversation_module, "_MAX_CODEX_IMAGE_BYTES", 4):
            with self.assertRaises(ImageGenerationError) as raised:
                format_image_result(
                    [{"b64_json": base64.b64encode(b"12345").decode("ascii")}],
                    "cat",
                    "b64_json",
                )

        self.assertEqual(raised.exception.code, "upstream_error")

    def test_upstream_base64_predicted_overflow_is_rejected_before_decode(self) -> None:
        with (
            mock.patch.object(conversation_module, "_MAX_CODEX_IMAGE_BYTES", 4),
            mock.patch.object(
                conversation_module.base64,
                "b64decode",
                wraps=conversation_module.base64.b64decode,
            ) as decode,
        ):
            with self.assertRaises(ImageGenerationError):
                format_image_result(
                    [{"b64_json": "A" * 8}],
                    "cat",
                    "b64_json",
                )

        decode.assert_not_called()

    def test_malformed_upstream_image_payload_is_not_stringified_into_public_output(self) -> None:
        canary = "image-output-container-secret"
        item = {"b64_json": {"secret": canary}}

        with mock.patch(
            "services.protocol.conversation.image_storage_service.save",
            side_effect=AssertionError("malformed image must not be stored"),
        ):
            result = format_image_result([item], "cat", "b64_json")

        self.assertEqual(result["data"], [])
        self.assertNotIn(canary, str(result))
        response_items = image_output_items("cat", [item])
        self.assertEqual(response_items, [])
        self.assertNotIn(canary, str(response_items))

    def test_malformed_revised_prompt_is_not_stringified_into_public_output(self) -> None:
        canary = "revised-prompt-container-secret"
        encoded = base64.b64encode(PNG_BYTES).decode("ascii")
        item = {"b64_json": encoded, "revised_prompt": {"secret": canary}}

        with mock.patch(
            "services.protocol.conversation.image_storage_service.save",
            return_value=type("Stored", (), {"url": "/images/generated.png"})(),
        ):
            result = format_image_result([item], "cat", "b64_json")

        response_items = image_output_items("cat", [item])
        self.assertNotIn(canary, str(result))
        self.assertNotIn(canary, str(response_items))

    def test_generation_stream_emits_official_completed_event_metadata(self) -> None:
        output = ImageOutput(
            kind="result",
            model="gpt-image-2",
            index=1,
            total=1,
            created=1_700_000_001,
            data=[{"b64_json": "ZmFrZQ=="}],
        )
        with mock.patch.object(
            image_generation_module,
            "stream_image_outputs_with_pool",
            return_value=iter([output]),
        ):
            events = list(image_generation_module.handle({
                "model": "gpt-image-2",
                "prompt": "cat",
                "stream": True,
                "size": "1024x1024",
                "quality": "high",
                "output_format": "webp",
                "background": "auto",
            }))

        self.assertEqual(len(events), 1)
        event = events[0]
        self.assertEqual(event["type"], "image_generation.completed")
        self.assertEqual(event["b64_json"], "ZmFrZQ==")
        self.assertEqual(event["background"], "auto")
        self.assertEqual(event["created_at"], 1_700_000_001)
        self.assertEqual(event["output_format"], "webp")
        self.assertEqual(event["quality"], "high")
        self.assertEqual(event["size"], "1024x1024")
        self.assertIsInstance(event["usage"], dict)

    def test_edit_stream_emits_official_edit_completed_event_metadata(self) -> None:
        output = ImageOutput(
            kind="result",
            model="gpt-image-2",
            index=1,
            total=1,
            created=1_700_000_002,
            data=[{"b64_json": "ZmFrZQ=="}],
        )
        with mock.patch.object(
            image_edit_module,
            "stream_image_outputs_with_pool",
            return_value=iter([output]),
        ):
            events = list(image_edit_module.handle({
                "model": "gpt-image-2",
                "prompt": "edit cat",
                "images": [(PNG_BYTES, "image.png", "image/png")],
                "stream": True,
                "size": "1536x1024",
                "quality": "medium",
                "output_format": "jpeg",
                "background": "auto",
            }))

        self.assertEqual(len(events), 1)
        event = events[0]
        self.assertEqual(event["type"], "image_edit.completed")
        self.assertEqual(event["b64_json"], "ZmFrZQ==")
        self.assertEqual(event["background"], "auto")
        self.assertEqual(event["created_at"], 1_700_000_002)
        self.assertEqual(event["output_format"], "jpeg")
        self.assertEqual(event["quality"], "medium")
        self.assertEqual(event["size"], "1536x1024")
        self.assertIsInstance(event["usage"], dict)

    def test_generation_and_edit_accept_official_ten_image_limit(self) -> None:
        app = FastAPI()
        app.include_router(ai_module.create_router())
        client = TestClient(app)

        with (
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
            mock.patch.object(
                ai_module.openai_v1_image_generations,
                "handle",
                return_value={"created": 1, "data": []},
            ) as generation_handler,
            mock.patch.object(
                ai_module.openai_v1_image_edit,
                "handle",
                return_value={"created": 1, "data": []},
            ) as edit_handler,
        ):
            generation_response = client.post(
                "/v1/images/generations",
                headers=AUTH_HEADERS,
                json={"model": "gpt-image-2", "prompt": "cat", "n": 10},
            )
            edit_response = client.post(
                "/v1/images/edits",
                headers=AUTH_HEADERS,
                json={
                    "model": "gpt-image-2",
                    "prompt": "edit cat",
                    "image": PNG_DATA_URL,
                    "n": 10,
                },
            )

        self.assertEqual(generation_response.status_code, 200, generation_response.text)
        self.assertEqual(edit_response.status_code, 200, edit_response.text)
        self.assertEqual(generation_handler.call_args.args[0]["n"], 10)
        self.assertEqual(edit_handler.call_args.args[0]["n"], 10)

    def test_edit_rejects_masks_that_violate_official_image_contract(self) -> None:
        app = FastAPI()
        app.include_router(ai_module.create_router())
        client = TestClient(app)
        image_png = _image_data_url(mode="RGBA", size=(4, 4), image_format="PNG")
        image_jpeg = _image_data_url(mode="RGB", size=(4, 4), image_format="JPEG")
        mask_png = _image_data_url(mode="RGBA", size=(4, 4), image_format="PNG")

        cases = {
            "different_size": (
                image_png,
                _image_data_url(mode="RGBA", size=(2, 2), image_format="PNG"),
            ),
            "different_format": (image_jpeg, mask_png),
            "missing_alpha": (
                image_png,
                _image_data_url(mode="RGB", size=(4, 4), image_format="PNG"),
            ),
        }
        with (
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
            mock.patch.object(image_edit_module, "stream_image_outputs_with_pool") as upstream,
        ):
            for name, (image_url, mask_url) in cases.items():
                with self.subTest(name=name):
                    response = client.post(
                        "/v1/images/edits",
                        headers=AUTH_HEADERS,
                        json={
                            "model": "gpt-image-2",
                            "prompt": "edit cat",
                            "images": [{"image_url": image_url}],
                            "mask": {"image_url": mask_url},
                        },
                    )

                    self.assertEqual(response.status_code, 400, response.text)
            upstream.assert_not_called()

    def test_edit_rejects_multiple_masks(self) -> None:
        app = FastAPI()
        app.include_router(ai_module.create_router())
        client = TestClient(app)
        image_url = _image_data_url(mode="RGBA", size=(4, 4), image_format="PNG")

        with (
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
            mock.patch.object(image_edit_module, "stream_image_outputs_with_pool") as upstream,
        ):
            response = client.post(
                "/v1/images/edits",
                headers=AUTH_HEADERS,
                json={
                    "model": "gpt-image-2",
                    "prompt": "edit cat",
                    "images": [{"image_url": image_url}],
                    "mask": [
                        {"image_url": image_url},
                        {"image_url": image_url},
                    ],
                },
            )

        self.assertEqual(response.status_code, 400, response.text)
        upstream.assert_not_called()

    def test_edit_rejects_more_than_sixteen_input_images(self) -> None:
        app = FastAPI()
        app.include_router(ai_module.create_router())
        client = TestClient(app)
        image_url = _image_data_url(mode="RGBA", size=(4, 4), image_format="PNG")

        with (
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
            mock.patch.object(image_edit_module, "stream_image_outputs_with_pool") as upstream,
        ):
            response = client.post(
                "/v1/images/edits",
                headers=AUTH_HEADERS,
                json={
                    "model": "gpt-image-2",
                    "prompt": "edit cat",
                    "images": [{"image_url": image_url} for _ in range(17)],
                },
            )

        self.assertEqual(response.status_code, 400, response.text)
        upstream.assert_not_called()

    def test_edit_rejects_invalid_local_image_payloads_before_backend_selection(self) -> None:
        app = FastAPI()
        app.include_router(ai_module.create_router())
        client = TestClient(app)
        invalid_data_url = "data:image/png;base64," + base64.b64encode(b"not-an-image").decode("ascii")
        jpeg_payload = base64.b64decode(
            _image_data_url(mode="RGB", size=(4, 4), image_format="JPEG").split(",", 1)[1]
        )
        mismatched_data_url = "data:image/png;base64," + base64.b64encode(jpeg_payload).decode("ascii")

        with (
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
            mock.patch.object(ai_module.openai_v1_image_edit, "handle", return_value={"created": 1, "data": []}) as handler,
        ):
            responses = (
                client.post(
                    "/v1/images/edits",
                    headers=AUTH_HEADERS,
                    json={"model": "gpt-image-2", "prompt": "edit cat", "image": invalid_data_url},
                ),
                client.post(
                    "/v1/images/edits",
                    headers=AUTH_HEADERS,
                    json={"model": "gpt-image-2", "prompt": "edit cat", "image": mismatched_data_url},
                ),
                client.post(
                    "/v1/images/edits",
                    headers=AUTH_HEADERS,
                    data={"model": "gpt-image-2", "prompt": "edit cat"},
                    files={"image": ("invalid.png", b"not-an-image", "image/png")},
                ),
            )

        for response in responses:
            with self.subTest(response=response):
                self.assertEqual(response.status_code, 400, response.text)
        handler.assert_not_called()

    def test_edit_rejects_non_integer_form_count_as_bad_request(self) -> None:
        app = FastAPI()
        app.include_router(ai_module.create_router())
        client = TestClient(app, raise_server_exceptions=False)

        with (
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
            mock.patch.object(ai_module.openai_v1_image_edit, "handle") as handler,
        ):
            response = client.post(
                "/v1/images/edits",
                headers=AUTH_HEADERS,
                data={"model": "gpt-image-2", "prompt": "edit cat", "n": "not-an-integer"},
                files={"image": ("image.png", PNG_BYTES, "image/png")},
            )

        self.assertEqual(response.status_code, 400, response.text)
        handler.assert_not_called()

    def test_edit_rejects_multipart_image_over_byte_limit_before_backend_selection(self) -> None:
        app = FastAPI()
        app.include_router(ai_module.create_router())
        client = TestClient(app)

        with (
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
            mock.patch.object(image_inputs_module, "MAX_IMAGE_REFERENCE_BYTES", 32),
            mock.patch.object(ai_module.openai_v1_image_edit, "handle", return_value={"created": 1, "data": []}) as handler,
        ):
            response = client.post(
                "/v1/images/edits",
                headers=AUTH_HEADERS,
                data={"model": "gpt-image-2", "prompt": "edit cat"},
                files={"image": ("image.png", PNG_BYTES, "image/png")},
            )

        self.assertEqual(response.status_code, 400, response.text)
        handler.assert_not_called()

    def test_edit_rejects_overlong_multipart_integer_options_before_backend_selection(self) -> None:
        app = FastAPI()
        app.include_router(ai_module.create_router())
        client = TestClient(app, raise_server_exceptions=False)

        with (
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
            mock.patch.object(ai_module.openai_v1_image_edit, "handle") as handler,
        ):
            responses = [
                client.post(
                    "/v1/images/edits",
                    headers=AUTH_HEADERS,
                    data={"model": "gpt-image-2", "prompt": "edit cat", field: "9" * 5000},
                    files={"image": ("image.png", PNG_BYTES, "image/png")},
                )
                for field in ("output_compression", "partial_images")
            ]

        for response in responses:
            with self.subTest(status=response.status_code):
                self.assertEqual(response.status_code, 400, response.text)
        handler.assert_not_called()

    def test_single_mask_is_applied_only_to_first_input_image(self) -> None:
        first = io.BytesIO()
        second = io.BytesIO()
        mask = io.BytesIO()
        Image.new("RGBA", (4, 4), (255, 0, 0, 255)).save(first, format="PNG")
        Image.new("RGBA", (4, 4), (0, 255, 0, 255)).save(second, format="PNG")
        Image.new("RGBA", (4, 4), (0, 0, 0, 0)).save(mask, format="PNG")
        second_input = (second.getvalue(), "second.png", "image/png")

        result = image_edit_module._composite_mask(
            [
                (first.getvalue(), "first.png", "image/png"),
                second_input,
            ],
            [(mask.getvalue(), "mask.png", "image/png")],
        )

        self.assertEqual(len(result), 2)
        self.assertEqual(result[1], second_input)

    def test_edit_rejects_unknown_json_and_multipart_parameters(self) -> None:
        app = FastAPI()
        app.include_router(ai_module.create_router())
        client = TestClient(app)

        with (
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
            mock.patch.object(ai_module.openai_v1_image_edit, "handle") as handler,
        ):
            json_response = client.post(
                "/v1/images/edits",
                headers=AUTH_HEADERS,
                json={
                    "model": "gpt-image-2",
                    "prompt": "edit cat",
                    "image": PNG_DATA_URL,
                    "future_parameter": "ignored",
                },
            )
            multipart_response = client.post(
                "/v1/images/edits",
                headers=AUTH_HEADERS,
                data={"prompt": "edit cat", "future_parameter": "ignored"},
                files={"image": ("image.png", PNG_BYTES, "image/png")},
            )

        self.assertEqual(json_response.status_code, 400, json_response.text)
        self.assertEqual(multipart_response.status_code, 400, multipart_response.text)
        handler.assert_not_called()

    def test_output_format_transcodes_bytes_and_reports_official_metadata(self) -> None:
        stored: list[bytes] = []

        class Stored:
            url = "/images/generated.jpg"

        with mock.patch(
            "services.protocol.conversation.image_storage_service.save",
            side_effect=lambda payload, _base_url: stored.append(payload) or Stored(),
        ):
            result = format_image_result(
                [{"b64_json": base64.b64encode(PNG_BYTES).decode("ascii")}],
                "cat",
                "b64_json",
                output_format="jpeg",
                output_compression=40,
                size="1024x1024",
                quality="high",
                background="opaque",
            )

        decoded = base64.b64decode(result["data"][0]["b64_json"])
        with Image.open(io.BytesIO(decoded)) as image:
            self.assertEqual(image.format, "JPEG")
        self.assertEqual(stored, [decoded])
        self.assertEqual(result["output_format"], "jpeg")
        self.assertEqual(result["size"], "1024x1024")
        self.assertEqual(result["quality"], "high")
        self.assertEqual(result["background"], "opaque")

    def test_generation_preserves_supported_official_image_fields(self) -> None:
        seen: list[dict[str, object]] = []
        app = FastAPI()
        app.include_router(ai_module.create_router())

        with (
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
            mock.patch.object(
                ai_module.openai_v1_image_generations,
                "handle",
                side_effect=lambda payload: seen.append(payload) or {"created": 1, "data": []},
            ),
        ):
            response = TestClient(app).post(
                "/v1/images/generations",
                headers=AUTH_HEADERS,
                json={
                    "model": "gpt-image-2",
                    "prompt": "cat",
                    "output_format": "webp",
                    "output_compression": 72,
                    "background": "auto",
                    "moderation": "auto",
                    "partial_images": 0,
                },
            )

        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(seen[0]["output_format"], "webp")
        self.assertEqual(seen[0]["output_compression"], 72)
        self.assertEqual(seen[0]["partial_images"], 0)

    def test_generation_validates_official_quality_and_arbitrary_size_contract(self) -> None:
        app = FastAPI()
        app.include_router(ai_module.create_router())
        client = TestClient(app)

        with (
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
            mock.patch.object(
                ai_module.openai_v1_image_generations,
                "handle",
                return_value={"created": 1, "data": []},
            ) as handler,
        ):
            valid = client.post(
                "/v1/images/generations",
                headers=AUTH_HEADERS,
                json={
                    "model": "gpt-image-2",
                    "prompt": "cat",
                    "quality": "high",
                    "size": "1920x1088",
                },
            )
            invalid_responses = [
                client.post(
                    "/v1/images/generations",
                    headers=AUTH_HEADERS,
                    json={"model": "gpt-image-2", "prompt": "cat", **invalid},
                )
                for invalid in (
                    {"quality": "ultra"},
                    {"size": "1000x1000"},
                    {"size": "3840x3840"},
                    {"size": f"{'9' * 5000}x16"},
                )
            ]

        self.assertEqual(valid.status_code, 200, valid.text)
        self.assertEqual(handler.call_args_list[0].args[0]["quality"], "high")
        self.assertEqual(handler.call_args_list[0].args[0]["size"], "1920x1088")
        for response in invalid_responses:
            with self.subTest(response=response.text):
                self.assertEqual(response.status_code, 400, response.text)
                self.assertNotIn("9999999999999999", response.text)
        self.assertEqual(handler.call_count, 1)

    def test_generation_accepts_official_gpt_image_2_max_landscape_and_portrait_sizes(self) -> None:
        app = FastAPI()
        app.include_router(ai_module.create_router())
        client = TestClient(app)

        with (
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
            mock.patch.object(
                ai_module.openai_v1_image_generations,
                "handle",
                return_value={"created": 1, "data": []},
            ) as handler,
        ):
            responses = [
                client.post(
                    "/v1/images/generations",
                    headers=AUTH_HEADERS,
                    json={"model": "gpt-image-2", "prompt": "cat", "size": size},
                )
                for size in ("3840x2160", "2160x3840")
            ]

        self.assertTrue(all(response.status_code == 200 for response in responses))
        self.assertEqual(
            [call.args[0]["size"] for call in handler.call_args_list],
            ["3840x2160", "2160x3840"],
        )

    def test_generation_rejects_non_ascii_dimension_digits_before_handler(self) -> None:
        app = FastAPI()
        app.include_router(ai_module.create_router())
        client = TestClient(app)

        with (
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
            mock.patch.object(
                ai_module.openai_v1_image_generations,
                "handle",
                return_value={"created": 1, "data": []},
            ) as handler,
        ):
            response = client.post(
                "/v1/images/generations",
                headers=AUTH_HEADERS,
                json={"model": "gpt-image-2", "prompt": "cat", "size": "102٤x1024"},
            )

        self.assertEqual(response.status_code, 400, response.text)
        handler.assert_not_called()

    def test_edit_preserves_supported_official_image_fields(self) -> None:
        seen: list[dict[str, object]] = []
        app = FastAPI()
        app.include_router(ai_module.create_router())

        with (
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
            mock.patch.object(
                ai_module.openai_v1_image_edit,
                "handle",
                side_effect=lambda payload: seen.append(payload) or {"created": 1, "data": []},
            ),
        ):
            response = TestClient(app).post(
                "/v1/images/edits",
                headers=AUTH_HEADERS,
                json={
                    "model": "gpt-image-2",
                    "prompt": "edit cat",
                    "image": PNG_DATA_URL,
                    "output_format": "jpeg",
                    "output_compression": 55,
                    "background": "auto",
                    "moderation": "auto",
                    "partial_images": 0,
                },
            )

        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(seen[0]["output_format"], "jpeg")
        self.assertEqual(seen[0]["output_compression"], 55)
        self.assertEqual(seen[0]["partial_images"], 0)

    def test_edit_rejects_nonstandard_output_size_before_backend_selection(self) -> None:
        app = FastAPI()
        app.include_router(ai_module.create_router())
        with (
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
            mock.patch.object(ai_module.openai_v1_image_edit, "handle") as handler,
        ):
            response = TestClient(app).post(
                "/v1/images/edits",
                headers=AUTH_HEADERS,
                json={
                    "model": "gpt-image-2",
                    "prompt": "edit cat",
                    "image": PNG_DATA_URL,
                    "size": "1536x864",
                },
            )

        self.assertEqual(response.status_code, 400, response.text)
        handler.assert_not_called()

    def test_unhonored_image_fields_fail_before_backend_selection(self) -> None:
        app = FastAPI()
        app.include_router(ai_module.create_router())
        client = TestClient(app)

        with (
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
            mock.patch.object(ai_module.openai_v1_image_generations, "handle") as generation_handler,
            mock.patch.object(ai_module.openai_v1_image_edit, "handle") as edit_handler,
        ):
            generation_user = client.post(
                "/v1/images/generations",
                headers=AUTH_HEADERS,
                json={"model": "gpt-image-2", "prompt": "cat", "user": "client-user-1"},
            )
            generation_history = client.post(
                "/v1/images/generations",
                headers=AUTH_HEADERS,
                json={"model": "gpt-image-2", "prompt": "cat", "history_disabled": False},
            )
            edit_user = client.post(
                "/v1/images/edits",
                headers=AUTH_HEADERS,
                json={
                    "model": "gpt-image-2",
                    "prompt": "edit cat",
                    "image": PNG_DATA_URL,
                    "user": "client-user-2",
                },
            )
            edit_task_id = client.post(
                "/v1/images/edits",
                headers=AUTH_HEADERS,
                json={
                    "model": "gpt-image-2",
                    "prompt": "edit cat",
                    "image": PNG_DATA_URL,
                    "client_task_id": "task-only-id",
                },
            )

        self.assertEqual(generation_user.status_code, 400, generation_user.text)
        self.assertEqual(generation_history.status_code, 422, generation_history.text)
        self.assertEqual(edit_user.status_code, 400, edit_user.text)
        self.assertEqual(edit_task_id.status_code, 400, edit_task_id.text)
        generation_handler.assert_not_called()
        edit_handler.assert_not_called()

    def test_partial_image_count_is_rejected_when_upstream_cannot_honor_it(self) -> None:
        app = FastAPI()
        app.include_router(ai_module.create_router())
        with mock.patch.object(ai_module.openai_v1_image_generations, "handle") as handler:
            response = TestClient(app).post(
                "/v1/images/generations",
                headers=AUTH_HEADERS,
                json={
                    "model": "gpt-image-2",
                    "prompt": "cat",
                    "stream": True,
                    "partial_images": 1,
                },
            )

        self.assertEqual(response.status_code, 400, response.text)
        handler.assert_not_called()


if __name__ == "__main__":
    unittest.main()
