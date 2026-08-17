from __future__ import annotations

import json
import re
import threading
from typing import Any, TypeGuard
from urllib.parse import unquote_to_bytes

import anyio
from fastapi import HTTPException, Request
from starlette.datastructures import UploadFile

from services.image_payload import ImagePayloadError, inspect_image_payload, validate_image_payload
from services.remote_image import download_remote_image
from services.protocol.error_response import PublicSafeValueError
from services.protocol.image_options import (
    normalize_image_output_compression,
    normalize_image_output_format,
    normalize_image_quality,
    normalize_image_size,
    normalize_supported_image_background,
    normalize_supported_image_moderation,
    normalize_supported_partial_images,
)
from utils.helper import is_supported_image_model
from utils.image_tokens import _decode_bounded_base64

ImageInput = tuple[bytes, str, str]
ImageSource = str | UploadFile | ImageInput

MAX_IMAGE_REFERENCE_BYTES = 50 * 1024 * 1024
MAX_IMAGE_EDIT_INPUTS = 16
_MAX_IMAGE_REFERENCE_DEPTH = 32
_UPLOAD_READ_CHUNK_BYTES = 1024 * 1024
_REMOTE_IMAGE_THREAD_CAPACITY = 4
_REMOTE_IMAGE_THREAD_STATE = threading.local()
_UPLOAD_CLOSED_MARKER = "_chatgpt2api_image_input_closed"
IMAGE_REFERENCE_FIELDS = {"image", "image[]", "images", "images[]", "image_url", "image_url[]"}
MASK_REFERENCE_FIELDS = {"mask", "mask[]"}
IMAGE_EDIT_OPTION_FIELDS = {
    "background",
    "client_task_id",
    "input_fidelity",
    "model",
    "moderation",
    "n",
    "output_compression",
    "output_format",
    "partial_images",
    "prompt",
    "quality",
    "response_format",
    "size",
    "stream",
    "style",
    "user",
}
IMAGE_EDIT_REQUEST_FIELDS = IMAGE_EDIT_OPTION_FIELDS | IMAGE_REFERENCE_FIELDS | MASK_REFERENCE_FIELDS
_IMAGE_EDIT_JSON_STRING_FIELDS = {
    "background",
    "client_task_id",
    "input_fidelity",
    "model",
    "moderation",
    "output_format",
    "prompt",
    "quality",
    "response_format",
    "size",
    "style",
    "user",
}
_IMAGE_EDIT_JSON_INTEGER_FIELDS = {"n", "output_compression", "partial_images"}
_MAX_OPTIONAL_INTEGER_TEXT_LENGTH = 10


def _clean(value: object, default: str = "") -> str:
    """清理字符串：转换为字符串并去掉首尾空白。"""
    text = str(value if value is not None else default).strip()
    return text or default


def _is_upload(value: object) -> TypeGuard[UploadFile]:
    """识别上传文件：兼容 Starlette 表单返回的 UploadFile。"""
    return isinstance(value, UploadFile)


def _parse_bool(value: object) -> bool | None:
    """解析布尔字段：兼容 JSON 布尔值和表单字符串。"""
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    text = _clean(value).lower()
    if text in {"true", "1", "yes", "y", "on"}:
        return True
    if text in {"false", "0", "no", "n", "off"}:
        return False
    raise HTTPException(status_code=400, detail={"error": "stream must be a boolean"})


def _parse_count(value: object) -> int:
    """解析生成数量：遵循官方图片接口的 1 到 10 限制。"""
    if value is None or value == "":
        return 1
    try:
        count = int(value)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail={"error": "n must be an integer"}) from exc
    if count < 1 or count > 10:
        raise HTTPException(status_code=400, detail={"error": "n must be between 1 and 10"})
    return count


def _parse_optional_int(value: object, field: str) -> int | None:
    if value is None or value == "":
        return None
    if type(value) is int:
        return value
    if isinstance(value, str):
        text = value.strip()
        if len(text) <= _MAX_OPTIONAL_INTEGER_TEXT_LENGTH and re.fullmatch(r"[+-]?\d+", text):
            try:
                return int(text)
            except (TypeError, ValueError, OverflowError):
                pass
    raise HTTPException(status_code=400, detail={"error": f"{field} must be an integer"})


def _validate_json_edit_option_types(fields: dict[str, Any]) -> None:
    for field in _IMAGE_EDIT_JSON_STRING_FIELDS:
        value = fields.get(field)
        if value is not None and not isinstance(value, str):
            raise HTTPException(status_code=400, detail={"error": f"{field} must be a string"})
    for field in _IMAGE_EDIT_JSON_INTEGER_FIELDS:
        value = fields.get(field)
        if value is not None and type(value) is not int:
            raise HTTPException(status_code=400, detail={"error": f"{field} must be an integer"})
    stream = fields.get("stream")
    if stream is not None and type(stream) is not bool:
        raise HTTPException(status_code=400, detail={"error": "stream must be a boolean"})


def validate_image_api_options(payload: dict[str, Any], *, editing: bool = False) -> dict[str, Any]:
    if not is_supported_image_model(payload.get("model")):
        raise HTTPException(status_code=400, detail={"error": "model must be a supported image model"})
    output_compression = _parse_optional_int(payload.get("output_compression"), "output_compression")
    partial_images = _parse_optional_int(payload.get("partial_images"), "partial_images")
    try:
        payload["quality"] = normalize_image_quality(payload.get("quality"))
        payload["size"] = normalize_image_size(payload.get("size"), editing=editing)
        payload["output_format"] = normalize_image_output_format(payload.get("output_format"))
        payload["output_compression"] = normalize_image_output_compression(
            output_compression,
            payload["output_format"],
        )
        payload["background"] = normalize_supported_image_background(payload.get("background"))
        payload["moderation"] = normalize_supported_image_moderation(payload.get("moderation"))
        payload["partial_images"] = normalize_supported_partial_images(partial_images)
    except PublicSafeValueError as exc:
        raise HTTPException(status_code=400, detail={"error": exc.public_safe_message()}) from exc

    if payload.get("style") is not None:
        raise HTTPException(status_code=400, detail={"error": "style is not supported by the configured image models"})
    if editing and payload.get("input_fidelity") is not None:
        raise HTTPException(status_code=400, detail={"error": "input_fidelity is not supported by the upstream image backend"})
    if payload.get("user") is not None:
        raise HTTPException(status_code=400, detail={"error": "user is not supported by the upstream image backend"})

    response_format = _clean(payload.get("response_format"), "b64_json")
    if response_format not in {"b64_json", "url"}:
        raise HTTPException(status_code=400, detail={"error": "response_format must be b64_json or url"})
    if payload.get("stream") and response_format != "b64_json":
        raise HTTPException(status_code=400, detail={"error": "streaming image responses require base64 output"})
    payload["response_format"] = response_format
    return payload


def _payload_from_fields(fields: dict[str, Any]) -> dict[str, Any]:
    """构造图片编辑载荷：从表单或 JSON 字段提取通用参数。"""
    prompt = _clean(fields.get("prompt"))
    if not prompt:
        raise HTTPException(status_code=400, detail={"error": "prompt is required"})
    payload = {
        "prompt": prompt,
        "model": _clean(fields.get("model"), "gpt-image-2"),
        "n": _parse_count(fields.get("n")),
        "size": _clean(fields.get("size")) or None,
        "quality": _clean(fields.get("quality"), "auto"),
        "response_format": _clean(fields.get("response_format"), "b64_json"),
        "stream": _parse_bool(fields.get("stream")),
        "background": fields.get("background"),
        "input_fidelity": fields.get("input_fidelity"),
        "moderation": fields.get("moderation"),
        "output_compression": fields.get("output_compression"),
        "output_format": fields.get("output_format"),
        "partial_images": fields.get("partial_images"),
        "style": fields.get("style"),
        "user": fields.get("user"),
    }
    if "client_task_id" in fields:
        payload["client_task_id"] = _clean(fields.get("client_task_id"))
    return validate_image_api_options(payload, editing=True)


def _json_reference_value(value: object) -> object:
    """解析表单图片引用：支持把 images 字段写成 JSON 字符串。"""
    if not isinstance(value, str):
        return value
    text = value.strip()
    if not text or text[0] not in "[{":
        return value
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return value


def _decode_base64_image(value: object, filename: str, mime_type: str) -> ImageInput:
    try:
        data = _decode_bounded_base64(value, max_bytes=MAX_IMAGE_REFERENCE_BYTES)
    except (TypeError, ValueError):
        data = None
    if data is None:
        raise HTTPException(status_code=400, detail={"error": "invalid base64 image data"})
    if not data:
        raise HTTPException(status_code=400, detail={"error": "image file is empty"})
    if len(data) > MAX_IMAGE_REFERENCE_BYTES:
        raise HTTPException(status_code=400, detail={"error": "image URL exceeds 50MB limit"})
    return _validated_image_input(data, filename, mime_type)


def _source_from_object(
    value: dict[str, Any],
    *,
    max_sources: int,
    depth: int,
    limit_error: str,
) -> list[ImageSource]:
    """提取图片引用对象：支持 image_url 或 url，明确拒绝 file_id。"""
    has_url = "image_url" in value or "url" in value
    if value.get("file_id"):
        raise HTTPException(
            status_code=400,
            detail={"error": "file_id image references are not supported; use image_url instead"},
        )
    inline = value.get("b64_json") or value.get("base64")
    if inline:
        filename = _clean(value.get("filename") or value.get("file_name"), "image.png")
        mime_type = _clean(value.get("mime_type") or value.get("mimeType"), "image/png")
        return [_decode_base64_image(inline, filename, mime_type)]
    if not has_url:
        raise HTTPException(status_code=400, detail={"error": "image reference must include image_url"})
    image_url = value.get("image_url", value.get("url"))
    if isinstance(image_url, dict):
        image_url = image_url.get("url")
    return _sources_from_value(
        image_url,
        max_sources=max_sources,
        depth=depth + 1,
        limit_error=limit_error,
    )


def _sources_from_value(
    value: object,
    *,
    max_sources: int = MAX_IMAGE_EDIT_INPUTS,
    depth: int = 0,
    limit_error: str = "images must contain at most 16 items",
) -> list[ImageSource]:
    """展开图片引用：把字符串、数组和对象统一成图片来源列表。"""
    if depth > _MAX_IMAGE_REFERENCE_DEPTH:
        raise HTTPException(status_code=400, detail={"error": "image references are too deeply nested"})
    value = _json_reference_value(value)
    if value is None:
        return []
    if _is_upload(value):
        if max_sources < 1:
            raise HTTPException(status_code=400, detail={"error": limit_error})
        return [value]
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        if max_sources < 1:
            raise HTTPException(status_code=400, detail={"error": limit_error})
        if text.lower().startswith(("data:", "http://", "https://")):
            return [text]
        return [_decode_base64_image(text, "image.png", "image/png")]
    if isinstance(value, list):
        if len(value) > max_sources:
            raise HTTPException(status_code=400, detail={"error": limit_error})
        sources: list[ImageSource] = []
        for item in value:
            child_sources = _sources_from_value(
                item,
                max_sources=max_sources - len(sources),
                depth=depth + 1,
                limit_error=limit_error,
            )
            sources.extend(child_sources)
        return sources
    if isinstance(value, dict):
        if max_sources < 1:
            raise HTTPException(status_code=400, detail={"error": limit_error})
        return _source_from_object(
            value,
            max_sources=max_sources,
            depth=depth,
            limit_error=limit_error,
        )
    raise HTTPException(status_code=400, detail={"error": "invalid image reference"})


def _json_image_sources(body: dict[str, Any]) -> list[ImageSource]:
    """读取 JSON 图片引用：优先支持官方 images 数组字段。"""
    sources: list[ImageSource] = []
    for key in ("images", "image", "image_url"):
        if key in body:
            sources.extend(_sources_from_value(
                body.get(key),
                max_sources=MAX_IMAGE_EDIT_INPUTS - len(sources),
            ))
    return sources


def _json_mask_sources(body: dict[str, Any]) -> list[ImageSource]:
    """读取 JSON mask 引用。"""
    mask = body.get("mask")
    if mask is not None:
        return _sources_from_value(
            mask,
            max_sources=1,
            limit_error="mask must contain at most one image",
        )
    return []


def _validate_edit_reference_counts(images: list[ImageSource], masks: list[ImageSource]) -> None:
    if len(images) > MAX_IMAGE_EDIT_INPUTS:
        raise HTTPException(status_code=400, detail={"error": "images must contain at most 16 items"})
    if len(masks) > 1:
        raise HTTPException(status_code=400, detail={"error": "mask must contain at most one image"})


async def _close_form_uploads(form: Any) -> None:
    seen: set[int] = set()
    for _key, value in form.multi_items():
        if not _is_upload(value) or id(value) in seen:
            continue
        seen.add(id(value))
        await _close_upload(value)


async def _close_upload(source: UploadFile) -> None:
    if getattr(source, _UPLOAD_CLOSED_MARKER, False):
        return
    try:
        await source.close()
    except Exception:
        pass
    else:
        setattr(source, _UPLOAD_CLOSED_MARKER, True)


async def close_image_sources(sources: list[ImageSource]) -> None:
    for source in sources:
        if _is_upload(source):
            await _close_upload(source)


async def parse_image_edit_request(request: Request) -> tuple[dict[str, Any], list[ImageSource], list[ImageSource]]:
    """解析图片编辑请求：同时支持 multipart 上传和 JSON 图片引用。
    
    返回 (payload, image_sources, mask_sources)
    """
    content_type = request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
    if content_type == "application/json":
        try:
            body = await request.json()
        except json.JSONDecodeError as exc:
            raise HTTPException(status_code=400, detail={"error": "invalid JSON body"}) from exc
        if not isinstance(body, dict):
            raise HTTPException(status_code=400, detail={"error": "JSON body must be an object"})
        if any(value is not None and key not in IMAGE_EDIT_REQUEST_FIELDS for key, value in body.items()):
            raise HTTPException(status_code=400, detail={"error": "parameter is not supported by the image edit endpoint"})
        _validate_json_edit_option_types(body)
        images = _json_image_sources(body)
        masks = _json_mask_sources(body)
        _validate_edit_reference_counts(images, masks)
        return _payload_from_fields(body), images, masks

    form = await request.form()
    keep_uploads_open = False
    try:
        if any(key not in IMAGE_EDIT_REQUEST_FIELDS for key, _value in form.multi_items()):
            raise HTTPException(status_code=400, detail={"error": "parameter is not supported by the image edit endpoint"})
        fields: dict[str, Any] = {}
        for key in IMAGE_EDIT_OPTION_FIELDS:
            value = form.get(key)
            if isinstance(value, str):
                fields[key] = value
        sources: list[ImageSource] = []
        mask_sources: list[ImageSource] = []
        for key, value in form.multi_items():
            if key in IMAGE_REFERENCE_FIELDS:
                sources.extend(_sources_from_value(
                    value,
                    max_sources=MAX_IMAGE_EDIT_INPUTS - len(sources),
                ))
            elif key in MASK_REFERENCE_FIELDS:
                mask_sources.extend(_sources_from_value(
                    value,
                    max_sources=1 - len(mask_sources),
                    limit_error="mask must contain at most one image",
                ))
        _validate_edit_reference_counts(sources, mask_sources)
        keep_uploads_open = True
        return _payload_from_fields(fields), sources, mask_sources
    finally:
        if not keep_uploads_open:
            await _close_form_uploads(form)


def _extension_from_mime(mime_type: str) -> str:
    """推导图片扩展名：把 MIME 类型转换为常见文件后缀。"""
    subtype = mime_type.split("/", 1)[1].split("+", 1)[0] if "/" in mime_type else "png"
    if subtype == "jpeg":
        return "jpg"
    return re.sub(r"[^a-z0-9]+", "", subtype.lower()) or "png"


_HEX_DIGITS = frozenset("0123456789abcdefABCDEF")


def _estimate_percent_decoded_size(payload: str, max_bytes: int) -> int:
    """在 unquote_to_bytes 前精确估算 data URL 的 UTF-8 字节数。"""
    size = 0
    index = 0
    while index < len(payload):
        char = payload[index]
        if char == "%":
            if (
                index + 2 >= len(payload)
                or payload[index + 1] not in _HEX_DIGITS
                or payload[index + 2] not in _HEX_DIGITS
            ):
                raise ValueError("invalid percent escape")
            size += 1
            index += 3
        else:
            try:
                size += len(char.encode("utf-8"))
            except UnicodeEncodeError as exc:
                raise ValueError("invalid data URL text") from exc
            index += 1
        if size > max_bytes:
            return size
    return size


def _decode_data_url(url: str) -> ImageInput:
    """解码 data URL：把内联图片转成标准图片输入元组。"""
    header, separator, payload = url.partition(",")
    if not separator:
        raise HTTPException(status_code=400, detail={"error": "invalid data image URL"})
    mime_type = header.split(";", 1)[0].removeprefix("data:") or "image/png"
    if not mime_type.startswith("image/"):
        raise HTTPException(status_code=400, detail={"error": "image_url must point to an image"})
    if ";base64" in header:
        try:
            data = _decode_bounded_base64(payload, max_bytes=MAX_IMAGE_REFERENCE_BYTES)
        except (TypeError, ValueError):
            data = None
    else:
        try:
            estimated_size = _estimate_percent_decoded_size(payload, MAX_IMAGE_REFERENCE_BYTES)
        except ValueError:
            data = None
        else:
            if estimated_size > MAX_IMAGE_REFERENCE_BYTES:
                raise HTTPException(status_code=400, detail={"error": "image URL exceeds 50MB limit"})
            try:
                data = unquote_to_bytes(payload)
            except (TypeError, ValueError, UnicodeError):
                data = None
    if data is None:
        raise HTTPException(status_code=400, detail={"error": "invalid data image URL"})
    if not data:
        raise HTTPException(status_code=400, detail={"error": "image URL is empty"})
    if len(data) > MAX_IMAGE_REFERENCE_BYTES:
        raise HTTPException(status_code=400, detail={"error": "image URL exceeds 50MB limit"})
    return _validated_image_input(data, f"image_url.{_extension_from_mime(mime_type)}", mime_type)


def _validated_image_input(data: bytes, filename: str, mime_type: str) -> ImageInput:
    try:
        info = validate_image_payload(data, mime_type)
    except ImagePayloadError as exc:
        raise HTTPException(status_code=400, detail={"error": "image data is invalid"}) from exc
    return data, filename, info.mime_type


def _download_image_url(url: str) -> ImageInput:
    """下载远程图片；data URL 保持本地解码，HTTP(S) 复用统一安全下载器。"""
    source = _clean(url)
    if source.lower().startswith("data:"):
        return _decode_data_url(source)
    return download_remote_image(source, max_bytes=MAX_IMAGE_REFERENCE_BYTES)


def _remote_image_thread_limiter() -> anyio.CapacityLimiter:
    limiter = getattr(_REMOTE_IMAGE_THREAD_STATE, "limiter", None)
    if limiter is None:
        limiter = anyio.CapacityLimiter(_REMOTE_IMAGE_THREAD_CAPACITY)
        _REMOTE_IMAGE_THREAD_STATE.limiter = limiter
    return limiter


async def _run_remote_image_io(url: str) -> ImageInput:
    return await anyio.to_thread.run_sync(
        _download_image_url,
        url,
        limiter=_remote_image_thread_limiter(),
    )


async def _read_upload_image_bounded(source: UploadFile) -> bytes:
    """Read an upload without materializing bytes beyond the image budget."""
    payload = bytearray()
    while len(payload) <= MAX_IMAGE_REFERENCE_BYTES:
        remaining = MAX_IMAGE_REFERENCE_BYTES + 1 - len(payload)
        chunk = await source.read(min(_UPLOAD_READ_CHUNK_BYTES, remaining))
        if not isinstance(chunk, (bytes, bytearray, memoryview)):
            raise HTTPException(status_code=400, detail={"error": "image data is invalid"})
        part = bytes(chunk)
        if len(part) > remaining:
            raise HTTPException(status_code=400, detail={"error": "image file exceeds 50MB limit"})
        if not part:
            break
        payload.extend(part)
        if len(payload) > MAX_IMAGE_REFERENCE_BYTES:
            raise HTTPException(status_code=400, detail={"error": "image file exceeds 50MB limit"})
    return bytes(payload)


async def read_image_sources(sources: list[ImageSource]) -> list[ImageInput]:
    """读取图片来源：上传文件或 data URL 解码后统一返回图片元组。"""
    images: list[ImageInput] = []
    for source in sources:
        if isinstance(source, tuple):
            images.append(source)
            continue
        if _is_upload(source):
            try:
                image_data = await _read_upload_image_bounded(source)
            finally:
                await _close_upload(source)
            if not image_data:
                raise HTTPException(status_code=400, detail={"error": "image file is empty"})
            if len(image_data) > MAX_IMAGE_REFERENCE_BYTES:
                raise HTTPException(status_code=400, detail={"error": "image file exceeds 50MB limit"})
            content_type = str(source.content_type or "").split(";", 1)[0].strip().lower()
            try:
                if content_type in {"", "application/octet-stream"}:
                    info = inspect_image_payload(image_data)
                else:
                    info = validate_image_payload(image_data, content_type)
            except ImagePayloadError as exc:
                raise HTTPException(status_code=400, detail={"error": "image data is invalid"}) from exc
            images.append((image_data, source.filename or "image.png", info.mime_type))
            continue
        images.append(await _run_remote_image_io(source))
    if not images:
        raise HTTPException(status_code=400, detail={"error": "image file or image_url is required"})
    return images
