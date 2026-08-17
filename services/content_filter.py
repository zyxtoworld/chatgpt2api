from __future__ import annotations

import re
import threading

import anyio
from curl_cffi import requests
from fastapi import HTTPException

from services.config import config
from services.protocol.error_response import exception_log_message
from services.proxy_service import proxy_settings
from services.remote_response import parse_json_response
from utils.log import logger

DEFAULT_REVIEW_PROMPT = "判断用户请求是否允许。只回答 ALLOW 或 REJECT。"
CONTENT_FILTER_REJECTION_MESSAGE = "检测到敏感词，拒绝本次任务"

# Strip base64 image data URIs before review: a text-only review model can't
# analyze image bytes, and a single inlined image easily blows past the token
# budget of the upstream review service.
_BASE64_DATA_URI = re.compile(r"data:[\w/.+;-]+;base64,[A-Za-z0-9+/=]+")

# Cap aligned to the upstream review service's max context. If text still
# exceeds the cap after base64 stripping, keep equal head/tail halves so both
# the system prompt and the most recent user message survive.
_MAX_REVIEW_TEXT_LEN = 100_000
_MAX_REVIEW_RESPONSE_BYTES = 1 * 1024 * 1024
_TRUNCATION_MARKER = "\n…[truncated]…\n"
_STRUCTURED_INPUT_TRUNCATION_MARKER = "\n…[structured input truncated]…"
_MAX_STRUCTURED_INPUT_DEPTH = 64
_MAX_STRUCTURED_INPUT_NODES = 10_000
_CONTENT_REVIEW_THREAD_CAPACITY = 8
_CONTENT_REVIEW_THREAD_STATE = threading.local()


async def check_request_async(text: str) -> None:
    limiter = getattr(_CONTENT_REVIEW_THREAD_STATE, "limiter", None)
    if limiter is None:
        limiter = anyio.CapacityLimiter(_CONTENT_REVIEW_THREAD_CAPACITY)
        _CONTENT_REVIEW_THREAD_STATE.limiter = limiter
    await anyio.to_thread.run_sync(check_request, text, limiter=limiter)


_TEXT_FIELDS = ("text", "input_text", "content", "input", "instructions", "system", "prompt")


def _text(value: object) -> str:
    """Extract review text without recursively trusting client-controlled shape."""
    parts: list[str] = []
    stack: list[tuple[object, int]] = [(value, 0)]
    node_count = 0
    collected_chars = 0
    truncated = False

    while stack:
        current, depth = stack.pop()
        node_count += 1
        if depth > _MAX_STRUCTURED_INPUT_DEPTH or node_count > _MAX_STRUCTURED_INPUT_NODES:
            truncated = True
            continue
        if isinstance(current, str):
            if current:
                remaining = _MAX_REVIEW_TEXT_LEN - collected_chars
                if remaining <= 0:
                    truncated = True
                    stack.clear()
                    break
                if len(current) > remaining:
                    parts.append(current[:remaining])
                    collected_chars += remaining
                    truncated = True
                    stack.clear()
                    break
                parts.append(current)
                collected_chars += len(current)
            continue
        if isinstance(current, list):
            child_count = min(len(current), _MAX_STRUCTURED_INPUT_NODES - node_count)
            if child_count < len(current):
                truncated = True
            for index in range(child_count - 1, -1, -1):
                stack.append((current[index], depth + 1))
            continue
        if isinstance(current, dict):
            for key in reversed(_TEXT_FIELDS):
                stack.append((current.get(key), depth + 1))

    result = "\n".join(parts)
    if len(result) > _MAX_REVIEW_TEXT_LEN:
        truncated = True
        result = result[:_MAX_REVIEW_TEXT_LEN]
    if truncated:
        marker = _STRUCTURED_INPUT_TRUNCATION_MARKER
        result = result[: max(0, _MAX_REVIEW_TEXT_LEN - len(marker))] + marker
    return result


def request_text(*values: object) -> str:
    return "\n".join(part for value in values if (part := _text(value).strip()))


def request_shape(*values: object) -> dict[str, int]:
    """Return a safe structural summary without logging prompts or image bytes."""
    stats = {
        "response_message_items": 0,
        "input_image_parts": 0,
        "image_url_parts": 0,
        "image_parts": 0,
        "data_url_images": 0,
        "remote_image_urls": 0,
        "literal_image_placeholders": 0,
    }

    stack: list[tuple[object, str, int]] = [(value, "", 0) for value in reversed(values)]
    node_count = 0
    while stack and node_count < _MAX_STRUCTURED_INPUT_NODES:
        value, key, depth = stack.pop()
        node_count += 1
        if depth > _MAX_STRUCTURED_INPUT_DEPTH:
            continue
        if isinstance(value, str):
            text = value.strip()
            lower = text.lower()
            if "<image>" in lower:
                stats["literal_image_placeholders"] += 1
            if lower.startswith("data:image/"):
                stats["data_url_images"] += 1
            elif key in {"image_url", "url"} and lower.startswith(("http://", "https://")):
                stats["remote_image_urls"] += 1
            continue
        if isinstance(value, list):
            child_count = min(len(value), _MAX_STRUCTURED_INPUT_NODES - node_count)
            for index in range(child_count - 1, -1, -1):
                stack.append((value[index], key, depth + 1))
            continue
        if not isinstance(value, dict):
            continue
        item_type = value.get("type") if isinstance(value.get("type"), str) else ""
        item_type = item_type.strip()
        if item_type == "message":
            stats["response_message_items"] += 1
        elif item_type == "input_image":
            stats["input_image_parts"] += 1
        elif item_type == "image_url":
            stats["image_url_parts"] += 1
        elif item_type == "image":
            stats["image_parts"] += 1
        children = [(child, child_key) for child_key, child in value.items() if isinstance(child_key, str)]
        for child, child_key in reversed(children):
            stack.append((child, child_key, depth + 1))
    return {key: value for key, value in stats.items() if value}


def _sanitize_for_review(text: str) -> tuple[str, dict[str, int]]:
    """Strip base64 data URIs and truncate to the review-service context limit.

    Returns (sanitized_text, stats) where stats carries base64_blocks_stripped
    and truncated_chars so callers can emit structured logs.
    """
    sanitized, base64_blocks_stripped = _BASE64_DATA_URI.subn("[image]", text)
    truncated_chars = 0
    if len(sanitized) > _MAX_REVIEW_TEXT_LEN:
        # Reserve marker space so the result stays within the cap.
        half = (_MAX_REVIEW_TEXT_LEN - len(_TRUNCATION_MARKER)) // 2
        truncated_chars = len(sanitized) - 2 * half
        sanitized = sanitized[:half] + _TRUNCATION_MARKER + sanitized[-half:]
    stats = {
        "base64_blocks_stripped": base64_blocks_stripped,
        "truncated_chars": truncated_chars,
    }
    return sanitized, stats


def _extract_review_decision(data: object) -> str | None:
    """Defensively pull the decision text out of the review service response.

    Returns None when the response shape doesn't match the OpenAI chat-completion
    contract (e.g. {"error": ...} with no choices). The caller treats None as
    "undecided" and applies the configured fail-open policy.
    """
    if not isinstance(data, dict):
        return None
    choices = data.get("choices")
    if not isinstance(choices, list) or not choices:
        return None
    first = choices[0]
    if not isinstance(first, dict):
        return None
    message = first.get("message")
    if not isinstance(message, dict):
        return None
    content = message.get("content")
    if not isinstance(content, str):
        return None
    return content.strip().lower()


def _is_allow_decision(decision: str) -> bool:
    return decision.startswith(("allow", "pass", "true", "yes", "通过", "允许", "安全"))


def _is_reject_decision(decision: str) -> bool:
    return decision.startswith(("reject", "deny", "block", "false", "no", "拒绝", "不允许", "违规", "禁止"))


def _resolve_fail_open(review: dict) -> bool:
    """Resolve fail_open from review config. Defaults to True."""
    value = review.get("fail_open")
    if value is None:
        return True
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return True


def _resolve_enabled(review: dict) -> bool:
    value = review.get("enabled")
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return False


def _review_text_setting(review: dict, key: str) -> str:
    value = review.get(key)
    return value.strip() if isinstance(value, str) else ""


def check_request(text: str) -> None:
    text = str(text or "")
    if not text.strip():
        return
    # Local sensitive-word match runs on the raw text (cheap, no network).
    for word in config.sensitive_words:
        if word in text:
            raise HTTPException(status_code=400, detail={"error": CONTENT_FILTER_REJECTION_MESSAGE})
    review = config.ai_review
    if not isinstance(review, dict) or not _resolve_enabled(review):
        return
    base_url = _review_text_setting(review, "base_url").rstrip("/")
    api_key = _review_text_setting(review, "api_key")
    model = _review_text_setting(review, "model")
    if not base_url or not api_key or not model:
        raise HTTPException(status_code=400, detail={"error": "ai review config is incomplete"})

    fail_open = _resolve_fail_open(review)

    review_text, sanitize_stats = _sanitize_for_review(text)
    if sanitize_stats["base64_blocks_stripped"] or sanitize_stats["truncated_chars"]:
        logger.info({
            "event": "ai_review_text_sanitized",
            "original_text_len": len(text),
            "review_text_len": len(review_text),
            **sanitize_stats,
        })
    prompt = _review_text_setting(review, "prompt") or DEFAULT_REVIEW_PROMPT
    content = f"{prompt}\n\n用户请求:\n{review_text}\n\n只回答 ALLOW 或 REJECT。"

    # fail_open=True (default): on upstream failure or ambiguous reply, let the
    # request through. The review is a soft safety net; one missed review is
    # preferable to a 5xx storm when the review service is flaky. Set
    # config.ai_review.fail_open=false for strict-compliance deployments.
    def _on_failure(event_payload: dict) -> None:
        logger.warning(event_payload)
        if not fail_open:
            raise HTTPException(
                status_code=503,
                detail={"error": "AI 审核服务暂时不可用，请稍后重试"},
            )

    try:
        response = requests.post(
            f"{base_url}/v1/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={"model": model, "messages": [{"role": "user", "content": content}], "temperature": 0},
            timeout=60,
            stream=True,
            **proxy_settings.build_session_kwargs(require_tls_verification=True),
        )
    except Exception as exc:
        _on_failure({
            "event": "ai_review_request_failed",
            "error": exception_log_message(exc),
            "error_type": exc.__class__.__name__,
            "review_text_len": len(review_text),
            "original_text_len": len(text),
        })
        return

    try:
        if not 200 <= response.status_code < 300:
            _on_failure({
                "event": "ai_review_response_http_error",
                "status_code": response.status_code,
            })
            return
        try:
            data = parse_json_response(
                response,
                "ai review response",
                max_bytes=_MAX_REVIEW_RESPONSE_BYTES,
                require_ok=False,
                close=False,
            )
        except Exception as exc:
            _on_failure({
                "event": "ai_review_response_not_json",
                "status_code": response.status_code,
                "error": exception_log_message(exc),
            })
            return

        decision = _extract_review_decision(data)
        if decision is None:
            _on_failure({
                "event": "ai_review_malformed_response",
                "status_code": response.status_code,
                "review_text_len": len(review_text),
                "original_text_len": len(text),
            })
            return

        if _is_allow_decision(decision):
            return
        if _is_reject_decision(decision):
            raise HTTPException(status_code=400, detail={"error": "AI 审核未通过，拒绝本次任务"})
        # Ambiguous decisions (e.g. "MAYBE", empty content) fall back to fail-open policy.
        _on_failure({
            "event": "ai_review_ambiguous_decision",
            "decision_len": len(decision),
            "review_text_len": len(review_text),
        })
        return
    finally:
        close = getattr(response, "close", None)
        if callable(close):
            try:
                close()
            except Exception:
                pass
