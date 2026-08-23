from __future__ import annotations

import json
import os
import unittest

import pytest
import requests

from test.utils import decode_image_payload, iter_sse_data, require_http_response, require_stream_response

AUTH_KEY = os.getenv("CHATGPT2API_LIVE_API_KEY", "")
BASE_URL = os.getenv("CHATGPT2API_LIVE_BASE_URL", "")
TEXT_MODEL = "auto"
IMAGE_MODEL = "gpt-image-2"
CODEX_IMAGE_MODEL = "codex-gpt-image-2"


class ResponsesTests(unittest.TestCase):
    @staticmethod
    def _iter_sse_payloads(response: requests.Response):
        yield from iter_sse_data(response)

    @pytest.mark.live_upstream
    def test_text_response_http(self):
        """测试 Responses 文本的非流式 HTTP 调用。"""
        response = requests.post(
            f"{BASE_URL}/v1/responses",
            headers={"Authorization": f"Bearer {AUTH_KEY}"},
            json={
                "model": TEXT_MODEL,
                "input": [
                    {
                        "role": "user",
                        "content": [
                            {"type": "input_text", "text": "你好，请简单介绍一下你自己。"},
                        ],
                    }
                ],
            },
            timeout=300,
        )
        require_http_response(response, content_type="application/json")
        payload = response.json()
        self.assertEqual(payload.get("object"), "response")
        self.assertEqual(payload.get("status"), "completed")
        self.assertIsNone(payload.get("error"))
        self.assertTrue(isinstance(payload.get("output"), list) and payload.get("output"))

    @pytest.mark.live_upstream
    def test_text_response_stream_http(self):
        """测试 Responses 文本的流式 HTTP 调用。"""
        response = requests.post(
            f"{BASE_URL}/v1/responses",
            headers={"Authorization": f"Bearer {AUTH_KEY}"},
            json={
                "model": TEXT_MODEL,
                "input": [
                    {
                        "role": "user",
                        "content": [
                            {"type": "input_text", "text": "你好，请简单介绍一下你自己。"},
                        ],
                    }
                ],
                "stream": True,
            },
            stream=True,
            timeout=300,
        )
        require_stream_response(response)
        event_types = []
        parse_errors: list[str] = []
        stream_errors: list[str] = []
        try:
            for payload_text in self._iter_sse_payloads(response):
                if payload_text == "[DONE]":
                    break
                try:
                    payload = json.loads(payload_text)
                except Exception as exc:
                    parse_errors.append(type(exc).__name__)
                    continue
                event_type = str(payload.get("type") or "")
                if event_type:
                    event_types.append(event_type)
                if event_type == "error":
                    stream_errors.append("error")
        finally:
            response.close()
        self.assertIn("response.created", event_types)
        self.assertIn("response.completed", event_types)
        self.assertFalse(stream_errors, stream_errors)
        self.assertFalse(parse_errors, parse_errors)

    @pytest.mark.live_upstream
    def test_image_response_http(self):
        """测试 Responses 画图的非流式 HTTP 调用。"""
        response = requests.post(
            f"{BASE_URL}/v1/responses",
            headers={"Authorization": f"Bearer {AUTH_KEY}"},
            json={
                "model": IMAGE_MODEL,
                "input": [
                    {
                        "role": "user",
                        "content": [
                            {"type": "input_text", "text": "我想做一张南京城市宣传海报图。"},
                        ],
                    }
                ],
                "tools": [{"type": "image_generation"}],
            },
            timeout=300,
        )
        require_http_response(response, content_type="application/json")
        payload = response.json()
        self.assertEqual(payload.get("object"), "response")
        self.assertEqual(payload.get("status"), "completed")
        self.assertIsNone(payload.get("error"))
        decoded_images: list[bytes] = []
        for index, item in enumerate(payload.get("output") or [], start=1):
            if not isinstance(item, dict):
                continue
            image_b64 = str(item.get("result") or "")
            if image_b64:
                decoded_images.append(decode_image_payload(image_b64))
        self.assertGreater(len(decoded_images), 0, "Responses 图片终态没有可解码 image_generation_call。")

    @pytest.mark.live_upstream
    def test_image_response_stream_http(self):
        """测试 Responses 画图的流式 HTTP 调用。"""
        response = requests.post(
            f"{BASE_URL}/v1/responses",
            headers={"Authorization": f"Bearer {AUTH_KEY}"},
            json={
                "model": IMAGE_MODEL,
                "input": [
                    {
                        "role": "user",
                        "content": [
                            {"type": "input_text", "text": "我想做一张南京城市宣传海报图。"},
                        ],
                    }
                ],
                "tools": [{"type": "image_generation"}],
                "stream": True,
            },
            stream=True,
            timeout=300,
        )
        require_stream_response(response)
        image_items: list[str] = []
        event_types: list[str] = []
        parse_errors: list[str] = []
        stream_errors: list[str] = []
        try:
            for payload_text in self._iter_sse_payloads(response):
                if payload_text == "[DONE]":
                    break
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
                if payload.get("type") != "response.output_item.done":
                    continue
                item = payload.get("item") or {}
                if str(item.get("type") or "") != "image_generation_call":
                    continue
                image_b64 = str(item.get("result") or "")
                if image_b64:
                    image_items.append(image_b64)
        finally:
            response.close()
        decoded_images = [decode_image_payload(value) for value in image_items]
        self.assertIn("response.completed", event_types)
        self.assertFalse(stream_errors, stream_errors)
        self.assertFalse(parse_errors, parse_errors)
        self.assertGreater(len(decoded_images), 0, "Responses 图片流没有可解码 image_generation_call。")

    @pytest.mark.live_upstream
    def test_codex_image_response_http(self):
        """测试 Responses 的 codex 画图非流式 HTTP 调用。"""
        response = requests.post(
            f"{BASE_URL}/v1/responses",
            headers={"Authorization": f"Bearer {AUTH_KEY}"},
            json={
                "model": CODEX_IMAGE_MODEL,
                "input": [
                    {
                        "role": "user",
                        "content": [
                            {"type": "input_text", "text": "我想做一张南京城市宣传海报图。"},
                        ],
                    }
                ],
                "tools": [{"type": "image_generation"}],
            },
            timeout=300,
        )
        require_http_response(response, content_type="application/json")
        payload = response.json()
        self.assertEqual(payload.get("object"), "response")
        self.assertEqual(payload.get("status"), "completed")
        self.assertIsNone(payload.get("error"))
        decoded_images: list[bytes] = []
        for index, item in enumerate(payload.get("output") or [], start=1):
            if not isinstance(item, dict):
                continue
            image_b64 = str(item.get("result") or "")
            if image_b64:
                decoded_images.append(decode_image_payload(image_b64))
        self.assertGreater(len(decoded_images), 0, "Codex Responses 图片终态没有可解码 image_generation_call。")

    @pytest.mark.live_upstream
    def test_codex_image_response_stream_http(self):
        """测试 Responses 的 codex 画图流式 HTTP 调用。"""
        response = requests.post(
            f"{BASE_URL}/v1/responses",
            headers={"Authorization": f"Bearer {AUTH_KEY}"},
            json={
                "model": CODEX_IMAGE_MODEL,
                "input": [
                    {
                        "role": "user",
                        "content": [
                            {"type": "input_text", "text": "我想做一张南京城市宣传海报图。"},
                        ],
                    }
                ],
                "tools": [{"type": "image_generation"}],
                "stream": True,
            },
            stream=True,
            timeout=300,
        )
        require_stream_response(response)
        image_items: list[str] = []
        event_types: list[str] = []
        parse_errors: list[str] = []
        stream_errors: list[str] = []
        try:
            for payload_text in self._iter_sse_payloads(response):
                if payload_text == "[DONE]":
                    break
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
                if payload.get("type") != "response.output_item.done":
                    continue
                item = payload.get("item") or {}
                if str(item.get("type") or "") != "image_generation_call":
                    continue
                image_b64 = str(item.get("result") or "")
                if image_b64:
                    image_items.append(image_b64)
        finally:
            response.close()
        decoded_images = [decode_image_payload(value) for value in image_items]
        self.assertIn("response.completed", event_types)
        self.assertFalse(stream_errors, stream_errors)
        self.assertFalse(parse_errors, parse_errors)
        self.assertGreater(len(decoded_images), 0, "Codex Responses 图片流没有可解码 image_generation_call。")


if __name__ == "__main__":
    unittest.main()
