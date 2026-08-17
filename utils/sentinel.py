"""OpenAI Sentinel Token (PoW) 生成与请求工具函数。

用于密码登录、注册等需要 sentinel token 的流程。
"""
from __future__ import annotations

import base64
import json
import random
import re
import time
import uuid
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from curl_cffi.requests import Session

from services.remote_response import close_response, parse_json_response


_MAX_SENTINEL_RESPONSE_BYTES = 1 * 1024 * 1024
_MAX_SENTINEL_POW_FIELD_CHARS = 256
_MAX_SENTINEL_TOKEN_CHARS = 4096


class SentinelTokenGenerator:
    """Sentinel Token 生成器（PoW - Proof of Work）。"""
    MAX_ATTEMPTS = 500_000
    ERROR_PREFIX = "wQ8Lk5FbGpA2NcR9dShT6gYjU7VxZ4D"

    def __init__(self, device_id: str, ua: str):
        self.device_id = device_id
        self.user_agent = ua
        self.sid = str(uuid.uuid4())

    @staticmethod
    def _fnv1a_32(text: str) -> str:
        h = 2166136261
        for ch in text:
            h ^= ord(ch)
            h = (h * 16777619) & 0xFFFFFFFF
        h ^= h >> 16
        h = (h * 2246822507) & 0xFFFFFFFF
        h ^= h >> 13
        h = (h * 3266489909) & 0xFFFFFFFF
        h ^= h >> 16
        return format(h & 0xFFFFFFFF, "08x")

    def _get_config(self) -> list:
        perf_now = random.uniform(1000, 50000)
        return [
            "1920x1080",
            time.strftime("%a %b %d %Y %H:%M:%S GMT+0000 (Coordinated Universal Time)", time.gmtime()),
            4294705152,
            random.random(),
            self.user_agent,
            "https://sentinel.openai.com/sentinel/20260124ceb8/sdk.js",
            None,
            None,
            "en-US",
            random.random(),
            random.choice(["vendorSub-undefined", "plugins-undefined", "mimeTypes-undefined", "hardwareConcurrency-undefined"]),
            random.choice(["location", "implementation", "URL", "documentURI", "compatMode"]),
            random.choice(["Object", "Function", "Array", "Number", "parseFloat", "undefined"]),
            perf_now,
            self.sid,
            "",
            random.choice([4, 8, 12, 16]),
            time.time() * 1000 - perf_now,
        ]

    @staticmethod
    def _b64(data) -> str:
        return base64.b64encode(json.dumps(data, separators=(",", ":"), ensure_ascii=False).encode("utf-8")).decode("ascii")

    def generate_requirements_token(self) -> str:
        data = self._get_config()
        data[3] = 1
        data[9] = round(random.uniform(5, 50))
        return "gAAAAAC" + self._b64(data)

    def generate_token(self, seed: str, difficulty: str) -> str:
        start = time.time()
        data = self._get_config()
        difficulty = str(difficulty or "0")
        for i in range(self.MAX_ATTEMPTS):
            data[3] = i
            data[9] = round((time.time() - start) * 1000)
            payload = self._b64(data)
            if self._fnv1a_32(seed + payload)[: len(difficulty)] <= difficulty:
                return "gAAAAAB" + payload + "~S"
        return "gAAAAAB" + self.ERROR_PREFIX + self._b64(str(None))


# ── 默认 User-Agent 和 sec-ch-ua ──────────────────────────────
DEFAULT_SENTINEL_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/145.0.0.0 Safari/537.36"
)
DEFAULT_SENTINEL_SEC_CH_UA = '"Chromium";v="145", "Google Chrome";v="145", "Not/A)Brand";v="99"'


def build_sentinel_token(
    session: "Session",
    device_id: str,
    flow: str,
    *,
    user_agent: str = "",
    sec_ch_ua: str = "",
) -> tuple[str, str]:
    """请求 sentinel token 并返回 (sentinel_header_value, oai_sc_cookie_value)。

    Args:
        session: curl_cffi Session 实例
        device_id: 设备 ID
        flow: 流程标识（如 "password_verify", "username_password_create" 等）
        user_agent: 可选的 User-Agent 覆盖
        sec_ch_ua: 可选的 sec-ch-ua 覆盖

    Returns:
        (openai-sentinel-token header value, oai-sc cookie value) 元组

    Raises:
        RuntimeError: sentinel 请求失败
    """
    ua = user_agent or DEFAULT_SENTINEL_USER_AGENT
    ch_ua = sec_ch_ua or DEFAULT_SENTINEL_SEC_CH_UA
    generator = SentinelTokenGenerator(device_id, ua)
    resp = session.post(
        "https://sentinel.openai.com/backend-api/sentinel/req",
        data=json.dumps({"p": generator.generate_requirements_token(), "id": device_id, "flow": flow}),
        headers={
            "Content-Type": "text/plain;charset=UTF-8",
            "Referer": "https://sentinel.openai.com/backend-api/sentinel/frame.html",
            "Origin": "https://sentinel.openai.com",
            "User-Agent": ua,
            "sec-ch-ua": ch_ua,
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
        },
        timeout=20,
        verify=True,
        stream=True,
    )

    try:
        if resp.status_code != 200:
            raise RuntimeError(f"sentinel_req_failed_{resp.status_code}")
        try:
            data = parse_json_response(
                resp,
                "sentinel response",
                max_bytes=_MAX_SENTINEL_RESPONSE_BYTES,
                require_ok=False,
                close=False,
            )
        except Exception:
            fallback = json.dumps(
                {"p": generator.generate_requirements_token(), "t": "", "c": "", "id": device_id, "flow": flow},
                separators=(",", ":"),
            )
            return fallback, ""

        if not isinstance(data, dict):
            raise RuntimeError(f"sentinel_req_failed_{resp.status_code}")
        token = data.get("token")
        token = token.strip() if isinstance(token, str) else ""
        if not token:
            raise RuntimeError(f"sentinel_req_failed_{resp.status_code}")
        if len(token) > _MAX_SENTINEL_TOKEN_CHARS:
            raise RuntimeError("sentinel response is invalid")
        raw_pow_data = data.get("proofofwork")
        if raw_pow_data is None:
            pow_data = {}
        elif isinstance(raw_pow_data, dict):
            pow_data = raw_pow_data
        else:
            raise RuntimeError("sentinel challenge is invalid")
        required = pow_data.get("required", False)
        if type(required) is not bool:
            raise RuntimeError("sentinel challenge is invalid")
        if required:
            seed = pow_data.get("seed")
            difficulty = pow_data.get("difficulty")
            if (
                not isinstance(seed, str)
                or not seed
                or len(seed) > _MAX_SENTINEL_POW_FIELD_CHARS
                or not isinstance(difficulty, str)
                or not difficulty
                or len(difficulty) > _MAX_SENTINEL_POW_FIELD_CHARS
                or re.fullmatch(r"[0-9a-fA-F]+", difficulty) is None
            ):
                raise RuntimeError("sentinel challenge is invalid")
            p_value = generator.generate_token(seed, difficulty)
        else:
            p_value = generator.generate_requirements_token()
        sentinel_value = json.dumps({"p": p_value, "t": "", "c": token, "id": device_id, "flow": flow}, separators=(",", ":"))
        # oai-sc cookie = "0" + sentinel token "c" value (the challenge token from the server)
        oai_sc_value = "0" + token
        return sentinel_value, oai_sc_value
    finally:
        close_response(resp)
