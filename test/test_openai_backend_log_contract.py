from __future__ import annotations

import json
import unittest
from types import SimpleNamespace
from unittest import mock

import services.openai_backend_api as backend_module
from services.openai_backend_api import ImagePollTimeoutError, OpenAIBackendAPI
from utils.helper import UpstreamHTTPError


SECRET = "opaque-upstream-token owner@example.com response fragment"


class _RawResponse:
    status = 400

    def __init__(self, body: str, content_type: str = "application/json") -> None:
        self.headers = {"content-type": content_type}
        self._body = body.encode("utf-8")

    def read(self) -> bytes:
        return self._body


def logged_text(calls: object) -> str:
    return json.dumps(calls, ensure_ascii=False, default=str)


class OpenAIBackendLogContractTests(unittest.TestCase):
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

        self.assertNotIn(SECRET, logged_text(info.call_args_list))

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
