from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO

from PIL import Image


DEFAULT_MAX_IMAGE_PIXELS = 25_000_000
IMAGE_MIME_FORMATS: dict[str, tuple[frozenset[str], str]] = {
    "image/png": (frozenset({"PNG"}), "png"),
    "image/apng": (frozenset({"PNG"}), "png"),
    "image/jpeg": (frozenset({"JPEG"}), "jpg"),
    "image/jpg": (frozenset({"JPEG"}), "jpg"),
    "image/gif": (frozenset({"GIF"}), "gif"),
    "image/webp": (frozenset({"WEBP"}), "webp"),
    "image/bmp": (frozenset({"BMP"}), "bmp"),
    "image/tiff": (frozenset({"TIFF"}), "tiff"),
}


class ImagePayloadError(ValueError):
    pass


@dataclass(frozen=True)
class ImagePayloadInfo:
    format: str
    mime_type: str
    width: int
    height: int


def inspect_image_payload(
    data: bytes,
    *,
    max_pixels: int = DEFAULT_MAX_IMAGE_PIXELS,
) -> ImagePayloadInfo:
    if not isinstance(data, bytes) or not data:
        raise ImagePayloadError("image payload is empty")
    try:
        with Image.open(BytesIO(data)) as image:
            actual_format = str(image.format or "").upper()
            width, height = int(image.width), int(image.height)
            if width <= 0 or height <= 0 or width * height > max_pixels:
                raise ImagePayloadError("image dimensions exceed limit")
            image.load()
    except ImagePayloadError:
        raise
    except Exception as exc:
        raise ImagePayloadError("image payload is invalid") from exc

    mime_type = str(Image.MIME.get(actual_format) or "").lower()
    if not actual_format or not mime_type:
        raise ImagePayloadError("image format is unsupported")
    return ImagePayloadInfo(
        format=actual_format,
        mime_type=mime_type,
        width=width,
        height=height,
    )


def validate_image_payload(
    data: bytes,
    mime_type: str,
    *,
    max_pixels: int = DEFAULT_MAX_IMAGE_PIXELS,
) -> ImagePayloadInfo:
    normalized_mime = str(mime_type or "").split(";", 1)[0].strip().lower()
    expected = IMAGE_MIME_FORMATS.get(normalized_mime)
    if expected is None:
        raise ImagePayloadError("image mime type is unsupported")
    info = inspect_image_payload(data, max_pixels=max_pixels)
    if info.format not in expected[0]:
        raise ImagePayloadError("image type does not match content type")
    return info
