from __future__ import annotations

import json
import os
import unittest

import pytest
import requests

from test.utils import iter_sse_data, require_http_response, require_stream_response

AUTH_KEY = os.getenv("CHATGPT2API_LIVE_API_KEY", "")
BASE_URL = os.getenv("CHATGPT2API_LIVE_BASE_URL", "")
MODEL = "auto"


class AnthropicMessagesTests(unittest.TestCase):
    @staticmethod
    def _headers() -> dict[str, str]:
        return {
            "x-api-key": AUTH_KEY,
            "anthropic-version": "2023-06-01",
        }

    @pytest.mark.live_upstream
    def test_message_http(self):
        """测试 Anthropic Messages 的非流式 HTTP 调用。"""
        response = requests.post(
            f"{BASE_URL}/v1/messages",
            headers=self._headers(),
            json={
                "model": MODEL,
                "messages": [
                    {"role": "user", "content": "你好，请简单介绍一下你自己。"},
                ],
            },
            timeout=300,
        )
        require_http_response(response, content_type="application/json")
        payload = response.json()
        self.assertEqual(payload.get("type"), "message")
        content = payload.get("content")
        self.assertIsInstance(content, list)
        self.assertTrue(content)
        self.assertTrue(any(isinstance(block, dict) and str(block.get("text") or "").strip() for block in content))
        self.assertIn(payload.get("stop_reason"), {"end_turn", "tool_use", "max_tokens"})

    @pytest.mark.live_upstream
    def test_message_stream_http(self):
        """测试 Anthropic Messages 的流式 HTTP 调用。"""
        response = requests.post(
            f"{BASE_URL}/v1/messages",
            headers=self._headers(),
            json={
                "model": MODEL,
                "stream": True,
                "messages": [
                    {"role": "user", "content": "你好，请简单介绍一下你自己。"},
                ],
            },
            stream=True,
            timeout=300,
        )
        require_stream_response(response)
        event_types: list[str] = []
        text_deltas: list[str] = []
        stop_reasons: list[str] = []
        stream_errors: list[str] = []
        parse_errors: list[str] = []
        try:
            for payload_text in iter_sse_data(response):
                try:
                    payload = json.loads(payload_text)
                except Exception as exc:
                    parse_errors.append(type(exc).__name__)
                    continue
                event_type = payload.get("type")
                if isinstance(event_type, str):
                    event_types.append(event_type)
                if event_type == "error":
                    stream_errors.append("error")
                if event_type == "content_block_delta":
                    delta = payload.get("delta")
                    if isinstance(delta, dict):
                        text_deltas.append(str(delta.get("text") or ""))
                if event_type == "message_delta":
                    delta = payload.get("delta")
                    if isinstance(delta, dict) and isinstance(delta.get("stop_reason"), str):
                        stop_reasons.append(delta["stop_reason"])
                if payload.get("type") == "message_stop":
                    break
        finally:
            response.close()
        self.assertIn("message_start", event_types)
        self.assertIn("content_block_delta", event_types)
        self.assertIn("message_stop", event_types)
        self.assertTrue(any(text_deltas))
        self.assertTrue(stop_reasons)
        self.assertFalse(stream_errors)
        self.assertFalse(parse_errors, parse_errors)


if __name__ == "__main__":
    unittest.main()
