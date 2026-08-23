from __future__ import annotations

import json
import os
import time
import unittest

import pytest
import requests

from test.fixtures.image_inputs import image_fixture_bytes
from test.utils import decode_image_payload, iter_sse_data, require_http_response, require_stream_response
from utils.log import logger

AUTH_KEY = os.getenv("CHATGPT2API_LIVE_API_KEY", "")
BASE_URL = os.getenv("CHATGPT2API_LIVE_BASE_URL", "")


def load_asset_bytes(name: str) -> bytes:
    return image_fixture_bytes(name)


def summarize_chunk(chunk: dict[str, object]) -> dict[str, object]:
    return {
        "type": chunk.get("type"),
        "partial_image_index": chunk.get("partial_image_index"),
        "has_b64_json": bool(chunk.get("b64_json")),
        "has_usage": "usage" in chunk,
    }


class ImageEditsTests(unittest.TestCase):
    @pytest.mark.live_upstream
    def test_image_edit_http(self):
        """测试图片编辑的非流式 HTTP 调用。"""
        response = requests.post(
            f"{BASE_URL}/v1/images/edits",
            headers={"Authorization": f"Bearer {AUTH_KEY}"},
            data={
                "model": "gpt-image-2",
                "prompt": "参考输入图片，保持人物主体和二次元插画风格不变，让女孩怀里抱着一只可爱的小猫，画面自然协调。",
                "n": "1",
                "response_format": "b64_json",
            },
            files={"image": ("chery_studio.png", load_asset_bytes("chery_studio.png"), "image/png")},
            timeout=300,
        )
        require_http_response(response, content_type="application/json")
        payload = response.json()
        decoded_images: list[bytes] = []
        for index, item in enumerate(payload.get("data") or [], start=1):
            b64_json = str((item or {}).get("b64_json") or "")
            if b64_json:
                decoded_images.append(decode_image_payload(b64_json))
        self.assertGreater(len(decoded_images), 0, "非流式接口未输出图片。")
        logger.info({
            "event": "test_images_edits_non_stream_done",
            "status_code": response.status_code,
            "created": payload.get("created"),
            "image_count": len(decoded_images),
        })

    @pytest.mark.live_upstream
    def test_image_edit_stream_http(self):
        """测试图片编辑的流式 HTTP 调用。"""
        response = requests.post(
            f"{BASE_URL}/v1/images/edits",
            headers={"Authorization": f"Bearer {AUTH_KEY}"},
            data={
                "model": "gpt-image-2",
                "prompt": "请提取两张输入界面截图中的 6 个任务，并把这 6 个任务整合排版到同一张图里，做成一张清晰的中文任务总览海报，标题明确，六个任务分区展示，版面整洁。",
                "n": "1",
                "response_format": "b64_json",
                "stream": "true",
            },
            files=[
                ("image", ("image.png", load_asset_bytes("image.png"), "image/png")),
                ("image", ("image_edit.png", load_asset_bytes("image_edit.png"), "image/png")),
            ],
            stream=True,
            timeout=300,
        )
        image_items: list[dict[str, object]] = []
        stream_errors: list[str] = []
        parse_errors: list[str] = []
        completed_events = 0
        require_stream_response(response)
        logger.info({
            "event": "test_images_edits_stream_start",
            "status_code": response.status_code,
            "content_type": response.headers.get("content-type"),
        })
        try:
            for payload in iter_sse_data(response):
                if payload == "[DONE]":
                    break
                try:
                    chunk = json.loads(payload)
                except Exception as exc:
                    parse_errors.append(type(exc).__name__)
                    continue
                if chunk.get("type") == "error":
                    stream_errors.append("error")
                if chunk.get("type") == "image_edit.completed":
                    completed_events += 1
                logger.info({
                    "event": "test_images_edits_stream_chunk",
                    "chunk": summarize_chunk(chunk),
                })
                if chunk.get("type") == "image_edit.completed" and chunk.get("b64_json"):
                    image_items.append({"b64_json": chunk["b64_json"]})
        finally:
            response.close()

        decoded_images = [
            decode_image_payload(str(item.get("b64_json") or ""))
            for item in image_items
        ]
        self.assertFalse(stream_errors, f"流式接口返回错误类型: {stream_errors}")
        self.assertFalse(parse_errors, parse_errors)
        self.assertGreater(completed_events, 0, "流式接口缺少 image_edit.completed 终态。")
        self.assertGreater(len(decoded_images), 0, "流式接口未输出图片。")
        logger.info({
            "event": "test_images_edits_stream_done",
            "image_count": len(decoded_images),
        })


if __name__ == "__main__":
    unittest.main()
