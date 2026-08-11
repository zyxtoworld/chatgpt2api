from __future__ import annotations

import re

from services.protocol.error_response import PublicSafeValueError


IMAGE_QUALITY_VALUES = frozenset({"auto", "low", "medium", "high"})
IMAGE_EDIT_SIZE_VALUES = frozenset({"auto", "1024x1024", "1536x1024", "1024x1536"})
IMAGE_OUTPUT_FORMAT_VALUES = frozenset({"png", "jpeg", "webp"})
_IMAGE_SIZE_PATTERN = re.compile(r"([1-9]\d{0,3})x([1-9]\d{0,3})")
_MAX_IMAGE_LONG_EDGE = 3840
_MAX_IMAGE_SHORT_EDGE = 2160
_MAX_IMAGE_PIXELS = _MAX_IMAGE_LONG_EDGE * _MAX_IMAGE_SHORT_EDGE


def normalize_image_quality(value: object) -> str:
    if value is None or value == "":
        return "auto"
    if not isinstance(value, str):
        raise PublicSafeValueError("quality must be auto, low, medium, or high")
    quality = value.strip()
    if quality not in IMAGE_QUALITY_VALUES:
        raise PublicSafeValueError("quality must be auto, low, medium, or high")
    return quality


def normalize_image_size(value: object, *, editing: bool = False) -> str | None:
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        raise PublicSafeValueError("size must be a supported image size")
    size = value.strip()
    if editing:
        if size not in IMAGE_EDIT_SIZE_VALUES:
            raise PublicSafeValueError(
                "size must be auto, 1024x1024, 1536x1024, or 1024x1536 for image edits"
            )
        return size
    if size == "auto":
        return size

    match = _IMAGE_SIZE_PATTERN.fullmatch(size)
    if match is None:
        raise PublicSafeValueError("size must be auto or a supported WIDTHxHEIGHT value")
    width, height = (int(part) for part in match.groups())
    long_edge = max(width, height)
    short_edge = min(width, height)
    if (
        width % 16 != 0
        or height % 16 != 0
        or long_edge > short_edge * 3
        or long_edge > _MAX_IMAGE_LONG_EDGE
        or short_edge > _MAX_IMAGE_SHORT_EDGE
        or width * height > _MAX_IMAGE_PIXELS
    ):
        raise PublicSafeValueError("size is outside the supported gpt-image-2 limits")
    return f"{width}x{height}"


def normalize_image_output_format(value: object) -> str:
    if value is None or value == "":
        return "png"
    if not isinstance(value, str):
        raise PublicSafeValueError("output_format must be png, jpeg, or webp")
    output_format = value.strip()
    if output_format not in IMAGE_OUTPUT_FORMAT_VALUES:
        raise PublicSafeValueError("output_format must be png, jpeg, or webp")
    return output_format


def normalize_image_output_compression(value: object, output_format: str) -> int | None:
    if value is None or value == "":
        return None
    if type(value) is not int:
        raise PublicSafeValueError("output_compression must be an integer")
    if value < 0 or value > 100:
        raise PublicSafeValueError("output_compression must be between 0 and 100")
    if output_format == "png":
        raise PublicSafeValueError("output_compression requires jpeg or webp output")
    return value


def normalize_supported_image_background(value: object) -> str:
    if value is None or value == "":
        return "auto"
    if not isinstance(value, str) or value.strip() not in {"auto", "opaque", "transparent"}:
        raise PublicSafeValueError("background must be auto, opaque, or transparent")
    background = value.strip()
    if background != "auto":
        raise PublicSafeValueError("background control is not supported by the upstream image backend")
    return background


def normalize_supported_image_moderation(value: object) -> str:
    if value is None or value == "":
        return "auto"
    if not isinstance(value, str) or value.strip() not in {"auto", "low"}:
        raise PublicSafeValueError("moderation must be auto or low")
    moderation = value.strip()
    if moderation != "auto":
        raise PublicSafeValueError("moderation control is not supported by the upstream image backend")
    return moderation


def normalize_supported_partial_images(value: object) -> int | None:
    if value is None or value == "":
        return None
    if type(value) is not int:
        raise PublicSafeValueError("partial_images must be an integer")
    if value < 0 or value > 3:
        raise PublicSafeValueError("partial_images must be between 0 and 3")
    if value:
        raise PublicSafeValueError("partial image streaming is not supported by the upstream image backend")
    return value
