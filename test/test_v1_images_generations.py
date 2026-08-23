from __future__ import annotations

import json
import os
import unittest

import pytest
import requests

from test.utils import decode_image_payload, iter_sse_data, require_http_response, require_stream_response

AUTH_KEY = os.getenv("CHATGPT2API_LIVE_API_KEY", "")
BASE_URL = os.getenv("CHATGPT2API_LIVE_BASE_URL", "")


class ImageGenerationsTests(unittest.TestCase):
    @pytest.mark.live_upstream
    def test_image_generation_http(self):
        """测试图片生成的非流式 HTTP 调用。"""
        response = requests.post(
            f"{BASE_URL}/v1/images/generations",
            headers={"Authorization": f"Bearer {AUTH_KEY}"},
            json={
                "model": "gpt-image-2",
                "prompt": "我想做一张南京城市宣传海报图。",
                "n": 1,
                "response_format": "b64_json",
            },
            timeout=300,
        )
        require_http_response(response, content_type="application/json")
        payload = response.json()
        decoded_images: list[bytes] = []
        for index, item in enumerate(payload.get("data") or [], start=1):
            b64_json = str((item or {}).get("b64_json") or "")
            if b64_json:
                decoded_images.append(decode_image_payload(b64_json))
        self.assertGreater(len(decoded_images), 0, "非流式接口未输出可解码图片。")

    @pytest.mark.live_upstream
    def test_image_generation_stream_http(self):
        """测试图片生成的流式 HTTP 调用。"""
        response = requests.post(
            f"{BASE_URL}/v1/images/generations",
            headers={"Authorization": f"Bearer {AUTH_KEY}"},
            json={
                "model": "gpt-image-2",
                "prompt": "我想做一张南京城市宣传海报图。",
                "n": 1,
                "response_format": "b64_json",
                "stream": True,
            },
            stream=True,
            timeout=300,
        )
        require_stream_response(response)
        image_items: list[dict[str, object]] = []
        stream_errors: list[str] = []
        parse_errors: list[str] = []
        completed_events = 0
        try:
            for payload in iter_sse_data(response):
                if payload == "[DONE]":
                    continue
                try:
                    chunk = json.loads(payload)
                except Exception as exc:
                    parse_errors.append(type(exc).__name__)
                    continue
                if chunk.get("type") == "error":
                    stream_errors.append("error")
                if chunk.get("type") == "image_generation.completed":
                    completed_events += 1
                    if chunk.get("b64_json"):
                        image_items.append({"b64_json": chunk["b64_json"]})
        finally:
            response.close()

        decoded_images = [
            decode_image_payload(str(item.get("b64_json") or ""))
            for item in image_items
        ]
        self.assertFalse(stream_errors, f"流式接口返回错误类型: {stream_errors}")
        self.assertFalse(parse_errors, parse_errors)
        self.assertGreater(completed_events, 0, "流式接口缺少 image_generation.completed 终态。")
        self.assertGreater(len(decoded_images), 0, "流式接口未输出可解码图片。")


if __name__ == "__main__":
    unittest.main()
