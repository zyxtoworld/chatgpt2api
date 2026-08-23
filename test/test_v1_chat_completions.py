from __future__ import annotations

import json
import os
import time
import unittest

import pytest
import requests

from test.utils import decode_image_data_urls, iter_sse_data, require_http_response, require_stream_response

AUTH_KEY = os.getenv("CHATGPT2API_LIVE_API_KEY", "")
BASE_URL = os.getenv("CHATGPT2API_LIVE_BASE_URL", "")


class ChatCompletionsTests(unittest.TestCase):
    @pytest.mark.live_upstream
    def test_text_completion_http(self):
        """测试文本对话的非流式 HTTP 调用。"""
        response = requests.post(
            f"{BASE_URL}/v1/chat/completions",
            headers={"Authorization": f"Bearer {AUTH_KEY}"},
            json={
                "model": "auto",
                "messages": [
                    {"role": "user", "content": "你好。"},
                    {"role": "assistant", "content": "你好，我可以帮助你处理文本和图片相关请求。"},
                    {"role": "user", "content": "那你再简单介绍一下你自己。"},
                ],
            },
            timeout=300,
        )
        require_http_response(response, content_type="application/json")
        payload = response.json()
        self.assertEqual(payload.get("object"), "chat.completion")
        choices = payload.get("choices")
        self.assertIsInstance(choices, list)
        self.assertTrue(choices)
        message = choices[0].get("message") if isinstance(choices[0], dict) else None
        self.assertIsInstance(message, dict)
        self.assertTrue(str(message.get("content") or "").strip() or message.get("tool_calls"))

    @pytest.mark.live_upstream
    def test_text_completion_stream_http(self):
        """测试文本对话的流式 HTTP 调用。"""
        response = requests.post(
            f"{BASE_URL}/v1/chat/completions",
            headers={"Authorization": f"Bearer {AUTH_KEY}"},
            json={
                "model": "auto",
                "stream": True,
                "messages": [
                    {"role": "user", "content": "你好。"},
                    {"role": "assistant", "content": "你好，我的名字是Claude。"},
                    {"role": "user", "content": "那你再简单介绍一下你自己，比如你的名字是什么。"},
                ],
            },
            stream=True,
            timeout=300,
        )
        require_stream_response(response)
        saw_done = False
        content_parts: list[str] = []
        finish_reasons: list[str] = []
        stream_errors: list[object] = []
        parse_errors: list[str] = []
        try:
            for payload_text in iter_sse_data(response):
                if payload_text == "[DONE]":
                    saw_done = True
                    break
                try:
                    payload = json.loads(payload_text)
                except Exception as exc:
                    parse_errors.append(type(exc).__name__)
                    continue
                if "error" in payload:
                    stream_errors.append(str(payload.get("type") or "error"))
                choices = payload.get("choices")
                if isinstance(choices, list) and choices and isinstance(choices[0], dict):
                    choice = choices[0]
                    delta = choice.get("delta")
                    if isinstance(delta, dict):
                        content_parts.append(str(delta.get("content") or ""))
                    finish_reason = choice.get("finish_reason")
                    if isinstance(finish_reason, str):
                        finish_reasons.append(finish_reason)
        finally:
            response.close()
        self.assertTrue(saw_done)
        self.assertIn("stop", finish_reasons)
        self.assertTrue("".join(content_parts).strip())
        self.assertFalse(stream_errors)
        self.assertFalse(parse_errors, parse_errors)

    @pytest.mark.live_upstream
    def test_image_completion_http(self):
        """测试图片对话的非流式 HTTP 调用。"""
        response = requests.post(
            f"{BASE_URL}/v1/chat/completions",
            headers={"Authorization": f"Bearer {AUTH_KEY}"},
            json={
                "model": "gpt-image-2",
                "messages": [
                    {"role": "user", "content": "我想做一张南京城市宣传海报图。"},
                ],
                "n": 1,
            },
            timeout=300,
        )
        require_http_response(response, content_type="application/json")
        payload = response.json()
        content = str((((payload.get("choices") or [{}])[0].get("message") or {}).get("content") or ""))
        self.assertEqual(payload.get("object"), "chat.completion")
        self.assertIn("data:image/", content)
        decoded_images = decode_image_data_urls(content)
        self.assertGreater(len(decoded_images), 0, "图片响应没有可解码的 data URL。")

    @pytest.mark.live_upstream
    def test_image_completion_stream_http(self):
        """测试图片对话的流式 HTTP 调用。"""
        response = requests.post(
            f"{BASE_URL}/v1/chat/completions",
            headers={"Authorization": f"Bearer {AUTH_KEY}"},
            json={
                "model": "gpt-image-2",
                "stream": True,
                "messages": [
                    {"role": "user", "content": "我想做一张南京城市宣传海报图。"},
                ],
                "n": 1,
            },
            stream=True,
            timeout=300,
        )
        require_stream_response(response)
        parts: list[str] = []
        saw_done = False
        finish_reasons: list[str] = []
        stream_errors: list[object] = []
        parse_errors: list[str] = []
        try:
            for payload in iter_sse_data(response):
                if payload == "[DONE]":
                    saw_done = True
                    break
                try:
                    chunk = json.loads(payload)
                except Exception as exc:
                    parse_errors.append(type(exc).__name__)
                    continue
                delta = ((chunk.get("choices") or [{}])[0].get("delta") or {})
                content = str(delta.get("content") or "")
                if content:
                    parts.append(content)
                if "error" in chunk:
                    stream_errors.append(str(chunk.get("type") or "error"))
                choices = chunk.get("choices")
                if isinstance(choices, list) and choices and isinstance(choices[0], dict):
                    finish_reason = choices[0].get("finish_reason")
                    if isinstance(finish_reason, str):
                        finish_reasons.append(finish_reason)
        finally:
            response.close()
        self.assertTrue(saw_done)
        self.assertIn("stop", finish_reasons)
        self.assertIn("data:image/", "".join(parts))
        self.assertFalse(stream_errors)
        self.assertFalse(parse_errors, parse_errors)
        decoded_images = decode_image_data_urls("".join(parts))
        self.assertGreater(len(decoded_images), 0, "图片流没有可解码的 data URL。")
