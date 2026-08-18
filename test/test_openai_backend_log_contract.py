from __future__ import annotations

import json
import os
import re
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import services.openai_backend_api as backend_module
import services.secure_file as secure_file
from services.openai_backend_api import (
    EditableFileArtifact,
    ImagePollTimeoutError,
    InvalidAccessTokenError,
    OpenAIBackendAPI,
)
from services.remote_response import REMOTE_JSON_CHUNK_BYTES
from utils.helper import UpstreamHTTPError


SECRET = "opaque-upstream-token owner@example.com response fragment"


class _RawResponse:
    status = 400

    def __init__(self, body: str, content_type: str = "application/json") -> None:
        self.headers = {"content-type": content_type}
        self._body = body.encode("utf-8")
        self._offset = 0

    def read(self, size: int = -1) -> bytes:
        if self._offset >= len(self._body):
            return b""
        end = len(self._body) if size < 0 else min(len(self._body), self._offset + size)
        chunk = self._body[self._offset:end]
        self._offset = end
        return chunk

    def __iter__(self):
        yield from self._body.splitlines(keepends=True)


class _BoundedJSONResponse:
    """最小真实响应 double：生产 JSON 解析只能从有界迭代体读取。"""

    def __init__(self, payload: object, status_code: int = 200, url: str = "") -> None:
        self.status_code = status_code
        self.headers = {"content-type": "application/json"}
        self.url = url
        self._body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.closed = False

    def iter_content(self, chunk_size: int | None = None):
        yield self._body

    def close(self) -> None:
        self.closed = True


def logged_text(calls: object) -> str:
    return json.dumps(calls, ensure_ascii=False, default=str)


class OpenAIBackendLogContractTests(unittest.TestCase):
    def test_editable_artifact_deadline_uses_monotonic_clock(self) -> None:
        backend = object.__new__(OpenAIBackendAPI)
        clock = {"value": 0.0}

        def get_conversation(_conversation_id: str) -> dict[str, object]:
            clock["value"] = 2.0
            return {}

        backend._get_editable_conversation_detail = mock.Mock(side_effect=get_conversation)
        with (
            mock.patch.object(backend_module.time, "monotonic", side_effect=lambda: clock["value"]),
            mock.patch.object(backend_module.time, "time", side_effect=AssertionError("wall clock used")),
            mock.patch.object(backend_module.time, "sleep"),
            self.assertRaisesRegex(RuntimeError, "timed out waiting for ppt/zip outputs"),
        ):
            backend._wait_editable_output_artifacts(
                "conv-clock",
                "ppt",
                (".pptx",),
                {"application/vnd.openxmlformats-officedocument.presentationml.presentation"},
                (),
                re.compile(r"\.pptx$"),
                timeout_secs=1,
                poll_interval_secs=0,
            )

    def test_editable_artifact_poll_sleep_is_bounded_by_deadline(self) -> None:
        backend = object.__new__(OpenAIBackendAPI)
        clock = {"value": 0.0}
        sleep_calls: list[float] = []

        def get_conversation(_conversation_id: str) -> dict[str, object]:
            return {}

        def sleep(seconds: float) -> None:
            sleep_calls.append(seconds)
            clock["value"] += seconds

        backend._get_editable_conversation_detail = mock.Mock(side_effect=get_conversation)
        with (
            mock.patch.object(backend_module.time, "monotonic", side_effect=lambda: clock["value"]),
            mock.patch.object(backend_module.time, "sleep", side_effect=sleep),
            self.assertRaisesRegex(RuntimeError, "timed out waiting for ppt/zip outputs"),
        ):
            backend._wait_editable_output_artifacts(
                "conv-sleep",
                "ppt",
                (".pptx",),
                {"application/vnd.openxmlformats-officedocument.presentationml.presentation"},
                (),
                re.compile(r"\.pptx$"),
                timeout_secs=1,
                poll_interval_secs=100,
            )

        self.assertEqual(sleep_calls, [1])

    def test_build_fp_does_not_stringify_container_values(self) -> None:
        canary = "fingerprint-container-canary owner@example.com"
        backend = object.__new__(OpenAIBackendAPI)
        backend.account = {
            "fp": {
                "user-agent": {"secret": canary},
                "impersonate": [canary],
                "oai-device-id": {"secret": canary},
            },
            "user-agent": [canary],
            "oai-session-id": {"secret": canary},
        }

        fp = backend._build_fp()

        self.assertNotIn(canary, repr(fp))
        self.assertNotIsInstance(fp["user-agent"], (dict, list))
        self.assertNotIsInstance(fp["impersonate"], (dict, list))
        self.assertNotIsInstance(fp["oai-device-id"], (dict, list))
        self.assertNotIsInstance(fp["oai-session-id"], (dict, list))

    def test_api_message_text_parts_do_not_stringify_containers(self) -> None:
        canary = "api-message-text-container-canary owner@example.com"
        backend = object.__new__(OpenAIBackendAPI)

        result = backend._api_messages_to_conversation_messages([
            {
                "role": "user",
                "content": [{"type": "text", "text": {"secret": canary}}],
            },
        ])

        serialized = json.dumps(result, ensure_ascii=False, default=str)
        self.assertNotIn(canary, serialized)
        self.assertEqual(result[0]["content"]["parts"], [""])

    def test_api_message_image_mime_does_not_stringify_containers(self) -> None:
        canary = "api-message-mime-container-canary owner@example.com"
        backend = object.__new__(OpenAIBackendAPI)
        backend.access_token = "fixture-token"
        backend._upload_image = mock.Mock(return_value={
            "file_id": "file-1",
            "mime_type": "image/png",
            "file_name": "image_1.png",
            "width": 1,
            "height": 1,
            "file_size": 1,
        })

        backend._api_messages_to_conversation_messages([
            {
                "role": "user",
                "content": [{
                    "type": "image",
                    "data": b"x",
                    "mime": {"secret": canary},
                }],
            },
        ])

        upload_url = backend._upload_image.call_args.args[0]
        self.assertNotIn(canary, upload_url)
        self.assertTrue(upload_url.startswith("data:image/png;base64,"))

    def test_editable_artifact_download_streams_without_response_content(self) -> None:
        backend = object.__new__(OpenAIBackendAPI)
        backend.session = mock.Mock()
        backend._resolve_editable_download_url = mock.Mock(return_value="https://files.example.test/artifact")
        chunks = [b"first-", b"second"]

        with tempfile.TemporaryDirectory() as temp_dir:
            target_path = Path(temp_dir) / "artifact.bin"

            class StreamingResponse:
                status_code = 200
                url = "https://files.example.test/artifact"
                headers = {"Content-Type": "application/octet-stream", "Content-Length": "12"}

                @property
                def content(self):
                    raise AssertionError("download must not read response.content")

                def iter_content(self, chunk_size=None):
                    self.chunk_size = chunk_size
                    self.target_visible_during_stream = target_path.exists()
                    yield from chunks

                def close(self):
                    self.closed = True

            response = StreamingResponse()
            backend.session.get.return_value = response
            artifact = EditableFileArtifact(name="artifact.bin")

            result = backend._download_editable_artifact(
                "conversation-1",
                artifact,
                Path(temp_dir),
                set(),
                (),
                ".bin",
            )

            self.assertEqual(result.read_bytes(), b"first-second")
            self.assertFalse(response.target_visible_during_stream)
            self.assertTrue(response.closed)
            self.assertEqual(response.chunk_size, 1024 * 1024)
            backend.session.get.assert_called_once_with(
                "https://files.example.test/artifact",
                timeout=300,
                stream=True,
            )

    def test_editable_export_rejects_rebound_output_dir_before_writing(self) -> None:
        backend = object.__new__(OpenAIBackendAPI)
        backend.access_token = "fixture-token"
        backend.session = SimpleNamespace(headers={})
        backend._upload_editable_base64_image = mock.Mock(return_value={"mime_type": "image/png"})
        backend._prepare_editable_conversation = mock.Mock(return_value="conduit")
        backend._run_editable_conversation = mock.Mock(return_value="conversation")
        backend._wait_editable_output_artifacts = mock.Mock(return_value=[
            EditableFileArtifact(name="primary.pptx"),
            EditableFileArtifact(name="assets.zip"),
        ])

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "files"
            owner_dir = root / "owner" / "ppt"
            owner_dir.mkdir(parents=True)
            foreign_dir = Path(temp_dir) / "foreign"
            foreign_dir.mkdir()
            rebound = owner_dir / "task"
            if os.name == "nt":
                result = subprocess.run(
                    ["cmd", "/c", "mklink", "/J", str(rebound), str(foreign_dir)],
                    capture_output=True,
                    text=True,
                )
                if result.returncode:
                    self.skipTest(f"junction fixture unavailable: {result.stderr or result.stdout}")
            else:
                rebound.symlink_to(foreign_dir, target_is_directory=True)

            def download(_conversation_id, artifact, output_dir, *_args):
                target = output_dir / artifact.name
                secure_file.atomic_write_stream(target, output_dir, (b"foreign-write",))
                return target

            backend._download_editable_artifact = download

            with self.assertRaises(OSError):
                backend._export_editable_file_zip(
                    ["data:image/png;base64,AA=="],
                    "prompt",
                    rebound,
                    primary_label="ppt",
                    primary_suffixes=(".pptx",),
                    primary_mime_types={"application/vnd.openxmlformats-officedocument.presentationml.presentation"},
                    primary_mime_keywords=("presentationml.presentation",),
                    primary_default_extension=".pptx",
                    export_file_re=backend_module.EDITABLE_PPT_EXPORT_FILE_RE,
                    timeout_secs=1,
                    poll_interval_secs=0,
                )

            self.assertFalse((foreign_dir / "primary.pptx").exists())
            self.assertFalse((foreign_dir / "assets.zip").exists())

    def test_editable_artifact_download_rejects_bounded_size_overflow(self) -> None:
        backend = object.__new__(OpenAIBackendAPI)
        backend.session = mock.Mock()
        backend._resolve_editable_download_url = mock.Mock(return_value="https://files.example.test/artifact")

        class OversizedResponse:
            status_code = 200
            url = "https://files.example.test/artifact"
            headers = {"Content-Type": "application/octet-stream", "Content-Length": "5"}

            def iter_content(self, chunk_size=None):
                yield b"12345"

            def close(self):
                self.closed = True

        response = OversizedResponse()
        backend.session.get.return_value = response
        artifact = EditableFileArtifact(name="artifact.bin")

        with tempfile.TemporaryDirectory() as temp_dir, mock.patch.object(
            backend_module, "_MAX_EDITABLE_ARTIFACT_BYTES", 4,
        ):
            with self.assertRaisesRegex(ValueError, "artifact download is too large"):
                backend._download_editable_artifact(
                    "conversation-1",
                    artifact,
                    Path(temp_dir),
                    set(),
                    (),
                    ".bin",
                )

            self.assertEqual(list(Path(temp_dir).iterdir()), [])
            self.assertTrue(response.closed)

    @unittest.skipUnless(os.name == "posix", "requires POSIX dir-fd atomic replace")
    def test_editable_artifact_download_replace_failure_preserves_old_target(self) -> None:
        backend = object.__new__(OpenAIBackendAPI)
        backend.session = mock.Mock()
        backend._resolve_editable_download_url = mock.Mock(return_value="https://files.example.test/artifact")

        class StreamingResponse:
            status_code = 200
            url = "https://files.example.test/artifact"
            headers = {"Content-Type": "application/octet-stream", "Content-Length": "3"}

            def iter_content(self, chunk_size=None):
                yield b"new"

            def close(self):
                self.closed = True

        response = StreamingResponse()
        backend.session.get.return_value = response
        artifact = EditableFileArtifact(name="artifact.bin")

        with tempfile.TemporaryDirectory() as temp_dir:
            target = Path(temp_dir) / "artifact.bin"
            target.write_bytes(b"old")
            with mock.patch.object(secure_file.os, "replace", side_effect=OSError("replace failed")):
                with self.assertRaisesRegex(OSError, "replace failed"):
                    backend._download_editable_artifact(
                        "conversation-1",
                        artifact,
                        Path(temp_dir),
                        set(),
                        (),
                        ".bin",
                    )

            self.assertEqual(target.read_bytes(), b"old")
            self.assertEqual(list(Path(temp_dir).glob(".artifact.bin.*.tmp")), [])
            self.assertTrue(response.closed)

    def test_editable_artifact_download_rejects_actual_stream_overflow(self) -> None:
        backend = object.__new__(OpenAIBackendAPI)
        backend.session = mock.Mock()
        backend._resolve_editable_download_url = mock.Mock(return_value="https://files.example.test/artifact")

        class StreamingResponse:
            status_code = 200
            url = "https://files.example.test/artifact"
            headers = {"Content-Type": "application/octet-stream", "Content-Length": "4"}

            def iter_content(self, chunk_size=None):
                yield b"12345"

            def close(self):
                self.closed = True

        response = StreamingResponse()
        backend.session.get.return_value = response
        artifact = EditableFileArtifact(name="artifact.bin")

        with tempfile.TemporaryDirectory() as temp_dir, mock.patch.object(
            backend_module, "_MAX_EDITABLE_ARTIFACT_BYTES", 4,
        ):
            with self.assertRaisesRegex(ValueError, "artifact download is too large"):
                backend._download_editable_artifact(
                    "conversation-1",
                    artifact,
                    Path(temp_dir),
                    set(),
                    (),
                    ".bin",
                )

            self.assertEqual(list(Path(temp_dir).iterdir()), [])
            self.assertTrue(response.closed)

    def test_editable_download_url_parser_rejects_container_values(self) -> None:
        response = SimpleNamespace(
            json=lambda: {"download_url": {"secret": SECRET}, "url": [SECRET]},
        )

        result = OpenAIBackendAPI._download_url_from_response(response)

        self.assertEqual(result, "")
        self.assertNotIn(SECRET, result)

    def test_file_and_attachment_download_lookups_reject_untrusted_url_shapes(self) -> None:
        backend = object.__new__(OpenAIBackendAPI)
        backend.base_url = "https://chatgpt.example.test"
        backend.user_agent = "test-agent"
        backend.session = mock.Mock()
        backend.session.headers = {}

        cases = (
            {"download_url": {"secret": SECRET}, "url": [SECRET]},
            {"download_url": "file:///tmp/secret", "url": "https://files.example.test/fallback"},
            {"download_url": "https://user:password@files.example.test/image"},
        )
        for payload in cases:
            with self.subTest(payload=payload):
                response = _BoundedJSONResponse(payload)
                backend.session.get.return_value = response
                self.assertEqual(backend._get_file_download_url("file-1"), "")
                self.assertTrue(response.closed)

                response = _BoundedJSONResponse(payload)
                backend.session.get.return_value = response
                self.assertEqual(
                    backend._get_attachment_download_url("conversation-1", "attachment-1"),
                    "",
                )
                self.assertTrue(response.closed)

    def test_editable_artifact_parser_does_not_stringify_container_fields(self) -> None:
        backend = object.__new__(OpenAIBackendAPI)
        canary = "editable-artifact-container-canary owner@example.com"

        artifact = backend._editable_artifact_from_dict(
            {
                "id": "file-safe-123",
                "name": {"secret": canary},
                "mime_type": [canary],
                "asset_pointer": {"secret": canary},
            },
            "message-safe",
            "assistant",
            1.0,
            backend_module.EDITABLE_PPT_EXPORT_FILE_RE,
        )

        self.assertIsNotNone(artifact)
        self.assertEqual(artifact.name, "")
        self.assertEqual(artifact.mime_type, "")
        self.assertNotIn(canary, repr(artifact))

    def test_editable_message_text_does_not_stringify_container_parts(self) -> None:
        backend = object.__new__(OpenAIBackendAPI)
        canary = "editable-message-container-canary owner@example.com"

        text = backend._editable_message_text({
            "content": {
                "parts": [
                    {"text": {"secret": canary}},
                    {"asset_pointer": [canary]},
                    {"model_set_context": {"secret": canary}},
                ],
            },
        })

        self.assertEqual(text, "")
        self.assertNotIn(canary, text)

    def test_editable_download_url_probes_close_every_response(self) -> None:
        backend = object.__new__(OpenAIBackendAPI)

        responses = [
            _BoundedJSONResponse({}, status_code=404),
            _BoundedJSONResponse(
                {"download_url": "https://files.example.test/artifact"},
                url="https://files.example.test/artifact",
            ),
        ]
        backend.session = mock.Mock()
        backend.session.headers = {}
        backend.base_url = "https://chatgpt.example.test"
        backend.session.get.side_effect = responses
        artifact = EditableFileArtifact(attachment_id="attachment-1", file_id="file-1")

        result = backend._resolve_editable_download_url("conversation-1", artifact)

        self.assertEqual(result, "https://files.example.test/artifact")
        self.assertTrue(all(response.closed for response in responses))

    def test_image_download_streams_without_response_content_and_closes(self) -> None:
        backend = object.__new__(OpenAIBackendAPI)
        backend.session = mock.Mock()

        class StreamingResponse:
            status_code = 200
            headers = {"Content-Length": "11"}

            @property
            def content(self):
                raise AssertionError("image download must not read response.content")

            def iter_content(self, chunk_size=None):
                self.chunk_size = chunk_size
                yield b"hello"
                yield b" world"

            def close(self):
                self.closed = True

        response = StreamingResponse()
        backend.session.get.return_value = response

        self.assertEqual(backend.download_image_bytes(["https://files.example.test/image"]), [b"hello world"])
        self.assertEqual(response.chunk_size, 1024 * 1024)
        self.assertTrue(response.closed)
        backend.session.get.assert_called_once_with(
            "https://files.example.test/image",
            timeout=120,
            stream=True,
        )

    def test_image_download_url_lookups_close_json_responses(self) -> None:
        backend = object.__new__(OpenAIBackendAPI)
        backend.base_url = "https://chatgpt.example.test"
        backend.user_agent = "test-agent"
        backend.session = mock.Mock()
        backend.session.headers = {}

        for lookup in (
            lambda: backend._get_file_download_url("file-1"),
            lambda: backend._get_attachment_download_url("conversation-1", "attachment-1"),
        ):
            with self.subTest(lookup=lookup):
                response = _BoundedJSONResponse(
                    {"download_url": "https://files.example.test/image"},
                )
                backend.session.get.return_value = response
                self.assertEqual(lookup(), "https://files.example.test/image")
                self.assertTrue(response.closed)

    def test_image_download_rejects_actual_stream_overflow(self) -> None:
        backend = object.__new__(OpenAIBackendAPI)
        backend.session = mock.Mock()

        class OversizedResponse:
            status_code = 200
            headers = {"Content-Length": "4"}

            def iter_content(self, chunk_size=None):
                yield b"12345"

            def close(self):
                self.closed = True

        response = OversizedResponse()
        backend.session.get.return_value = response

        with mock.patch.object(backend_module, "_MAX_UPSTREAM_IMAGE_BYTES", 4):
            with self.assertRaisesRegex(ValueError, "image download is too large"):
                backend.download_image_bytes(["https://files.example.test/image"])
        self.assertTrue(response.closed)

    def test_editable_image_decoder_rejects_malformed_and_oversized_payloads(self) -> None:
        backend = object.__new__(OpenAIBackendAPI)

        with self.assertRaisesRegex(ValueError, "image payload"):
            backend._decode_editable_base64_image("data:image/png;base64,not-base64", 1)

        oversized = "data:image/png;base64," + ("A" * (14 * 1024 * 1024))
        with self.assertRaisesRegex(ValueError, "image payload"):
            backend._decode_editable_base64_image(oversized, 1)

    def test_editable_image_decoder_rejects_predicted_overflow_before_decode(self) -> None:
        backend = object.__new__(OpenAIBackendAPI)
        encoded = "A" * 8  # 6 decoded bytes; the patched contract limit is 4.

        with (
            mock.patch.object(backend_module, "_MAX_EDITABLE_IMAGE_BYTES", 4),
            mock.patch.object(
                backend_module.base64,
                "b64decode",
                wraps=backend_module.base64.b64decode,
            ) as decode,
        ):
            with self.assertRaisesRegex(ValueError, "image payload"):
                backend._decode_editable_base64_image(encoded, 1)

        decode.assert_not_called()

    def test_chat_image_decoder_rejects_predicted_overflow_before_decode(self) -> None:
        backend = object.__new__(OpenAIBackendAPI)
        encoded = "A" * 8  # 6 decoded bytes; the patched contract limit is 4.

        with (
            mock.patch.object(backend_module, "_MAX_CHAT_IMAGE_BYTES", 4),
            mock.patch.object(
                backend_module.base64,
                "b64decode",
                wraps=backend_module.base64.b64decode,
            ) as decode,
        ):
            with self.assertRaisesRegex(ValueError, "image payload"):
                backend._decode_image_base64(encoded)

        decode.assert_not_called()

    def test_chat_image_decoder_rejects_local_file_paths(self) -> None:
        backend = object.__new__(OpenAIBackendAPI)
        with tempfile.TemporaryDirectory() as tmp_dir:
            image_path = Path(tmp_dir) / "image.bin"
            image_path.write_bytes(b"12345")
            with self.assertRaisesRegex(ValueError, "image payload"):
                backend._decode_image_base64(str(image_path))

    def test_editable_upload_response_rejects_container_url_and_file_id(self) -> None:
        backend = object.__new__(OpenAIBackendAPI)
        backend.base_url = "https://chatgpt.example.test"
        backend.user_agent = "test-agent"
        backend._decode_editable_base64_image = mock.Mock(
            return_value=(b"image", "image.png", "image/png", 1, 1),
        )
        backend._headers = mock.Mock(return_value={})
        backend.session = mock.Mock()
        response = _BoundedJSONResponse({
            "upload_url": {"secret": SECRET},
            "file_id": [SECRET],
        })
        backend.session.post.return_value = response
        backend.session.put.return_value = SimpleNamespace(status_code=200, headers={}, json=lambda: {})

        with self.assertRaisesRegex(RuntimeError, "invalid upload response") as raised:
            backend._upload_editable_base64_image("data:image/png;base64,aW1hZ2U=", 1)

        self.assertNotIn(SECRET, str(raised.exception))
        backend.session.put.assert_not_called()

    def test_prepare_conduit_token_rejects_container_values(self) -> None:
        backend = object.__new__(OpenAIBackendAPI)
        backend.base_url = "https://chatgpt.example.test"
        backend._headers = mock.Mock(return_value={})
        backend.session = mock.Mock()
        backend.session.post.side_effect = [
            _BoundedJSONResponse({"conduit_token": {"secret": SECRET}}),
            _BoundedJSONResponse({"conduit_token": {"secret": SECRET}}),
        ]

        with self.assertRaisesRegex(RuntimeError, "missing conduit_token"):
            backend._prepare_editable_conversation("make a deck", [])

        with self.assertRaisesRegex(RuntimeError, "missing conduit_token"):
            backend._prepare_search_conversation("find an image", "gpt-5-5", timeout_secs=1)

        self.assertNotIn(SECRET, repr(backend.session.post.call_args_list))

    def test_chat_requirements_missing_token_does_not_include_upstream_payload(self) -> None:
        backend = object.__new__(OpenAIBackendAPI)
        backend.access_token = "access-token"
        backend.base_url = "https://chatgpt.example.test"
        backend.user_agent = "test-agent"
        backend.pow_script_sources = []
        backend.pow_data_build = ""
        backend._headers = mock.Mock(return_value={})
        backend.session = mock.Mock()

        class StreamingJSONResponse:
            status_code = 200
            ok = True

            def __init__(self, payload: dict) -> None:
                self.closed = False
                self.payload = json.dumps(payload).encode("utf-8")

            def iter_content(self, chunk_size=None):
                yield self.payload

            def close(self) -> None:
                self.closed = True

        prepare = StreamingJSONResponse({"prepare_token": "prepare-token"})
        finalize = StreamingJSONResponse({"token": {"secret": SECRET}})
        backend.session.post.side_effect = [prepare, finalize]

        with self.assertRaisesRegex(RuntimeError, "missing auth chat requirements token") as raised:
            backend._get_chat_requirements()

        self.assertNotIn(SECRET, str(raised.exception))
        self.assertTrue(prepare.closed)
        self.assertTrue(finalize.closed)

    def test_json_call_reads_iter_content_with_stream_and_closes_response(self) -> None:
        backend = object.__new__(OpenAIBackendAPI)
        backend.base_url = "https://chatgpt.example.test"
        backend._headers = mock.Mock(return_value={})
        backend.session = mock.Mock()

        class StreamingJSONResponse:
            status_code = 200

            def __init__(self) -> None:
                self.closed = False
                self.chunk_size = None

            def iter_content(self, chunk_size=None):
                self.chunk_size = chunk_size
                body = b'{"conduit_token":"streamed-token"}'
                yield body[:12]
                yield body[12:]

            def close(self) -> None:
                self.closed = True

        response = StreamingJSONResponse()
        backend.session.post.return_value = response

        self.assertEqual(backend._prepare_editable_conversation("make a deck", []), "streamed-token")
        self.assertTrue(response.closed)
        self.assertEqual(response.chunk_size, REMOTE_JSON_CHUNK_BYTES)
        self.assertTrue(backend.session.post.call_args.kwargs["stream"])

    def test_bootstrap_reads_bounded_stream_and_closes_response(self) -> None:
        backend = object.__new__(OpenAIBackendAPI)
        backend.base_url = "https://chatgpt.example.test"
        backend.user_agent = "test-agent"
        backend.session = mock.Mock()
        backend.session.headers = {
            "Sec-Ch-Ua": "ua",
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
        }

        class StreamingTextResponse:
            status_code = 200

            def __init__(self) -> None:
                self.closed = False
                self.chunk_size = None

            def iter_content(self, chunk_size=None):
                self.chunk_size = chunk_size
                yield b"<html></html>"

            def close(self) -> None:
                self.closed = True

        response = StreamingTextResponse()
        backend.session.get.return_value = response

        backend._bootstrap()

        self.assertTrue(response.closed)
        self.assertEqual(response.chunk_size, REMOTE_JSON_CHUNK_BYTES)
        self.assertTrue(backend.session.get.call_args.kwargs["stream"])

    def test_json_error_status_is_mapped_before_reading_body_and_closes(self) -> None:
        backend = object.__new__(OpenAIBackendAPI)
        backend.base_url = "https://chatgpt.example.test"
        backend._headers = mock.Mock(return_value={})
        backend.session = mock.Mock()

        class ErrorResponse:
            def __init__(self, status_code: int, chunks: list[bytes]) -> None:
                self.status_code = status_code
                self.chunks = chunks
                self.closed = False
                self.iterated = False

            def iter_content(self, chunk_size=None):
                self.iterated = True
                yield from self.chunks

            def close(self) -> None:
                self.closed = True

        for status_code, expected in ((401, InvalidAccessTokenError), (500, RuntimeError)):
            with self.subTest(status_code=status_code):
                response = ErrorResponse(status_code, [b"{" + SECRET.encode() + b"}"] * 2)
                backend.session.get.return_value = response
                with self.assertRaises(expected):
                    backend._get_me()
                self.assertTrue(response.closed)
                self.assertFalse(response.iterated)

    def test_blob_puts_are_streamed_and_closed(self) -> None:
        class JSONResponse:
            status_code = 200

            def __init__(self, payload: dict[str, str]) -> None:
                self.payload = payload
                self.closed = False

            def iter_content(self, chunk_size=None):
                yield json.dumps(self.payload).encode("utf-8")

            def close(self) -> None:
                self.closed = True

        class PutResponse:
            status_code = 201

            def __init__(self) -> None:
                self.closed = False

            def close(self) -> None:
                self.closed = True

        editable = object.__new__(OpenAIBackendAPI)
        editable.base_url = "https://chatgpt.example.test"
        editable.user_agent = "test-agent"
        editable._headers = mock.Mock(return_value={})
        editable._decode_editable_base64_image = mock.Mock(
            return_value=(b"image", "image.png", "image/png", 1, 1),
        )
        editable.session = mock.Mock()
        editable.session.post.side_effect = [
            JSONResponse({"upload_url": "https://blob.example.test/one", "file_id": "file-1"}),
            JSONResponse({}),
        ]
        editable_put = PutResponse()
        editable.session.put.return_value = editable_put
        editable._upload_editable_base64_image("payload", 1)
        self.assertTrue(editable.session.put.call_args.kwargs["stream"])
        self.assertTrue(editable_put.closed)

        normal = object.__new__(OpenAIBackendAPI)
        normal.base_url = "https://chatgpt.example.test"
        normal.user_agent = "test-agent"
        normal._headers = mock.Mock(return_value={})
        normal._decode_image_base64 = mock.Mock(return_value=b"image")
        normal.session = mock.Mock()
        normal.session.post.side_effect = [
            JSONResponse({"upload_url": "https://blob.example.test/two", "file_id": "file-2"}),
            JSONResponse({}),
        ]
        normal_put = PutResponse()
        normal.session.put.return_value = normal_put
        image = SimpleNamespace(size=(1, 1), format="PNG")
        with mock.patch.object(backend_module.Image, "open", return_value=image):
            normal._upload_image("payload")
        self.assertTrue(normal.session.put.call_args.kwargs["stream"])
        self.assertTrue(normal_put.closed)

    def test_sse_error_status_closes_response_but_success_keeps_stream_open(self) -> None:
        backend = object.__new__(OpenAIBackendAPI)
        backend.base_url = "https://chatgpt.example.test"
        backend._bootstrap = mock.Mock()
        backend._get_chat_requirements = mock.Mock(return_value={})
        backend._chat_target = mock.Mock(return_value=("/backend-api/conversation", "UTC"))
        backend._conversation_payload = mock.Mock(return_value={})
        backend._conversation_headers = mock.Mock(return_value={})
        backend.session = mock.Mock()

        class SSEResponse:
            def __init__(self, status_code: int) -> None:
                self.status_code = status_code
                self.closed = False
                self.iterated = False

            def iter_lines(self):
                self.iterated = True
                yield b"data: [DONE]"

            def close(self) -> None:
                self.closed = True

        error_response = SSEResponse(401)
        backend.session.post.return_value = error_response
        with self.assertRaises(InvalidAccessTokenError):
            list(backend.stream_conversation(prompt="hello"))
        self.assertTrue(error_response.closed)
        self.assertFalse(error_response.iterated)

        success_response = SSEResponse(200)
        backend.session.post.return_value = success_response
        stream = backend.stream_conversation(prompt="hello")
        self.assertEqual(next(stream), "[DONE]")
        self.assertFalse(success_response.closed)
        stream.close()
        self.assertTrue(success_response.closed)

    def test_codex_invalid_json_and_event_logs_do_not_include_body_or_text(self) -> None:
        with mock.patch.object(backend_module.logger, "info") as info:
            for body in (
                f"invalid {SECRET}",
                "[]",
                '{"type": 3}',
                '{"missing_type": true}',
            ):
                with self.subTest(body=body):
                    with self.assertRaisesRegex(RuntimeError, "malformed codex response event"):
                        list(OpenAIBackendAPI._iter_codex_response_events(_RawResponse(body)))
            event = json.dumps(
                {
                    "type": "response.output_text.delta",
                    "delta": SECRET,
                    "error": {"type": "upstream_error", "message": SECRET},
                },
                ensure_ascii=False,
            )
            list(OpenAIBackendAPI._iter_codex_response_events(_RawResponse(event)))

            nested_canary = "codex-nested-summary-canary owner@example.test"
            nested_event = json.dumps(
                {
                    "type": "response.completed",
                    "response": {"type": {"secret": nested_canary}, "status": [nested_canary]},
                },
                ensure_ascii=False,
            )
            list(OpenAIBackendAPI._iter_codex_response_events(_RawResponse(nested_event)))

            identifier_canary = "codex-event-identifier-canary owner@example.com opaque-token"
            identifier_event = json.dumps(
                {
                    "type": identifier_canary,
                    "id": identifier_canary,
                    "status": identifier_canary,
                    "item_id": identifier_canary,
                },
                ensure_ascii=False,
            )
            list(OpenAIBackendAPI._iter_codex_response_events(_RawResponse(identifier_event)))

        self.assertNotIn(SECRET, logged_text(info.call_args_list))
        self.assertNotIn(nested_canary, logged_text(info.call_args_list))
        self.assertNotIn(identifier_canary, logged_text(info.call_args_list))

        backend = object.__new__(OpenAIBackendAPI)
        backend._codex_responses_headers = mock.Mock(
            return_value={"Authorization": "Bearer token", "Content-Type": "application/json"},
        )
        payload = {
            "model": "gpt-5.5",
            "input": [{"content": [{"type": "input_text", "text": SECRET}]}],
            "tools": [{"model": "gpt-image-2", "action": "generate"}],
        }
        with mock.patch.object(backend_module.logger, "warning") as warning:
            backend._log_codex_response_failure(
                "/backend-api/codex/responses", 502, {"x-upstream": SECRET}, payload, {"message": SECRET},
            )
        self.assertNotIn(SECRET, logged_text(warning.call_args_list))

    def test_codex_logs_do_not_include_untrusted_model_text(self) -> None:
        canary = "codex-request-model-canary owner@example.com opaque-token"
        backend = object.__new__(OpenAIBackendAPI)
        backend._codex_responses_headers = mock.Mock(
            return_value={"Authorization": "Bearer token", "Content-Type": "application/json"},
        )
        payload = {
            "model": canary,
            "input": [],
            "tools": [],
            "stream": True,
        }

        with mock.patch.object(backend_module.logger, "warning") as warning:
            backend._log_codex_response_failure(
                "/backend-api/codex/responses", 502, {}, payload, {"error": "upstream_error"},
            )
        self.assertNotIn(canary, logged_text(warning.call_args_list))

        backend.access_token = "fixture-token"
        backend.account = {"source_type": "codex", "account_id": "acct-codex"}
        backend.base_url = "https://example.test"
        backend._ensure_codex_source_account = mock.Mock()
        with (
            mock.patch.object(backend_module.requests, "Session", side_effect=RuntimeError("transport")),
            mock.patch.object(backend_module.logger, "info") as info,
            self.assertRaisesRegex(RuntimeError, "transport"),
        ):
            list(backend.iter_codex_response_events(payload))
        self.assertNotIn(canary, logged_text(info.call_args_list))

    def test_codex_request_log_does_not_include_untrusted_stream_value(self) -> None:
        canary = "codex-stream-canary owner@example.com opaque-token"
        backend = object.__new__(OpenAIBackendAPI)
        backend.access_token = "fixture-token"
        backend.account = {"source_type": "codex", "account_id": "acct-codex"}
        backend.base_url = "https://example.test"
        backend._ensure_codex_source_account = mock.Mock()
        payload = {
            "model": "gpt-5.5",
            "input": [],
            "tools": [],
            "stream": {"secret": canary},
        }
        with (
            mock.patch.object(backend_module.requests, "Session", side_effect=RuntimeError("transport")),
            mock.patch.object(backend_module.logger, "info") as info,
            self.assertRaisesRegex(RuntimeError, "transport"),
        ):
            list(backend.iter_codex_response_events(payload))
        self.assertNotIn(canary, logged_text(info.call_args_list))

    def test_codex_request_log_does_not_include_untrusted_timeout_value(self) -> None:
        canary = "codex-timeout-canary owner@example.com opaque-token"
        backend = object.__new__(OpenAIBackendAPI)
        backend.access_token = "fixture-token"
        backend.account = {"source_type": "codex", "account_id": "acct-codex"}
        backend.base_url = "https://example.test"
        backend._ensure_codex_source_account = mock.Mock()
        payload = {"model": "gpt-5.5", "input": [], "tools": [], "stream": True}
        timeout = {"secret": canary}
        with (
            mock.patch.object(backend_module.requests, "Session", side_effect=RuntimeError("transport")),
            mock.patch.object(backend_module.logger, "info") as info,
            self.assertRaisesRegex(RuntimeError, "transport"),
        ):
            list(backend.iter_codex_response_events(payload, timeout=timeout))
        self.assertNotIn(canary, logged_text(info.call_args_list))

    def test_codex_response_log_does_not_include_untrusted_content_type(self) -> None:
        canary = "codex-content-type-canary owner@example.com opaque-token"
        response = _RawResponse('{"type":"response.completed"}')
        response.headers = {"content-type": f"text/event-stream; {canary}"}
        with mock.patch.object(backend_module.logger, "info") as info:
            list(OpenAIBackendAPI._iter_codex_response_events(response))
        self.assertNotIn(canary, logged_text(info.call_args_list))

    def test_codex_response_log_does_not_include_untrusted_status_value(self) -> None:
        canary = "codex-status-canary owner@example.com opaque-token"
        response = _RawResponse('{"type":"response.completed"}')
        response.status = {"secret": canary}
        with mock.patch.object(backend_module.logger, "info") as info:
            list(OpenAIBackendAPI._iter_codex_response_events(response))
        self.assertNotIn(canary, logged_text(info.call_args_list))

    def test_image_task_error_and_timeout_logs_do_not_include_error_text(self) -> None:
        backend = object.__new__(OpenAIBackendAPI)
        task = {
            "image_gen_message": {
                "metadata": {"is_error": True},
                "author": {"role": "assistant"},
                "content": {"content_type": "text", "parts": [SECRET]},
            }
        }
        backend._query_backend_tasks = mock.Mock(return_value=[task])
        backend._get_conversation = mock.Mock(return_value={})

        with (
            mock.patch.object(
                backend_module,
                "config",
                SimpleNamespace(
                    image_poll_initial_wait_secs=0,
                    image_poll_interval_secs=0,
                    image_settle_enabled=False,
                    image_check_before_hit_enabled=False,
                    image_settle_secs=0,
                ),
            ),
            mock.patch.object(backend_module.logger, "info") as info,
            self.assertRaises(ImagePollTimeoutError),
        ):
            backend._poll_image_results("conversation-1", timeout_secs=0.02)

        self.assertNotIn(SECRET, logged_text(info.call_args_list))

    def test_image_url_error_logs_do_not_include_upstream_body(self) -> None:
        backend = object.__new__(OpenAIBackendAPI)
        upstream_error = UpstreamHTTPError("image-url", 502, {"message": SECRET})
        backend._get_file_download_url = mock.Mock(side_effect=upstream_error)
        backend._get_attachment_download_url = mock.Mock(side_effect=upstream_error)
        backend._poll_image_results = mock.Mock(side_effect=upstream_error)
        backend._resolve_image_urls = mock.Mock(return_value=[])

        with mock.patch.object(backend_module.logger, "debug") as debug:
            backend._resolve_image_urls("conversation-1", ["file-1"], ["sediment-1"])
        with (
            mock.patch.object(
                backend_module,
                "config",
                SimpleNamespace(
                    image_check_before_hit_enabled=True,
                    image_settle_enabled=True,
                    image_settle_secs=0,
                ),
            ),
            mock.patch.object(backend_module.logger, "warning") as warning,
        ):
            backend.resolve_conversation_image_urls(
                "conversation-1", ["file-1"], [], poll=True, poll_timeout_secs=1,
            )

        self.assertNotIn(SECRET, logged_text(debug.call_args_list))
        self.assertNotIn(SECRET, logged_text(warning.call_args_list))


if __name__ == "__main__":
    unittest.main()
