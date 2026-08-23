import base64
import binascii
import io
import json
import re
import sys
import urllib.request
from pathlib import Path

from PIL import Image


ROOT_DIR = Path(__file__).resolve().parents[1]
BASE_URL = "http://127.0.0.1:8000"

if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


def load_auth_key() -> str:
    return json.loads((ROOT_DIR / "config.json").read_text(encoding="utf-8"))["auth-key"]


def post_json(path: str, payload: dict) -> dict:
    request = urllib.request.Request(
        BASE_URL + path,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {load_auth_key()}"},
        method="POST",
    )
    with urllib.request.urlopen(request) as response:
        return json.loads(response.read().decode())


def _bounded_response_error(response, *, max_bytes: int = 2048) -> str:
    """Read only a bounded error prefix without using response.text/content/json."""
    collected = bytearray()
    try:
        for chunk in response.iter_content(chunk_size=min(1024, max_bytes)):
            if not chunk:
                continue
            if isinstance(chunk, str):
                chunk = chunk.encode("utf-8", "replace")
            remaining = max_bytes - len(collected)
            collected.extend(bytes(chunk)[:remaining])
            if len(collected) >= max_bytes:
                break
    finally:
        response.close()
    return bytes(collected[:max_bytes]).decode("utf-8", "replace")


def require_http_response(response, *, content_type: str | None = None) -> None:
    """Validate an HTTP response without embedding an unbounded body in failures."""
    if response.status_code != 200:
        summary = _bounded_response_error(response)
        raise AssertionError(f"HTTP status={response.status_code}, bounded_error={summary!r}")
    if content_type:
        actual = response.headers.get("content-type", "")
        if not actual.startswith(content_type):
            response.close()
            raise AssertionError(f"unexpected content-type={actual!r}")


def decode_image_payload(value: str, *, max_bytes: int = 8 * 1024 * 1024) -> bytes:
    """Decode one live image result without writing it to project or user data."""
    if not isinstance(value, str) or not value.strip():
        raise AssertionError("image result is empty")
    encoded = value.strip()
    if encoded.startswith("data:"):
        header, separator, encoded = encoded.partition(",")
        if not separator or not re.fullmatch(r"data:image/(?:png|jpeg|webp);base64", header, re.IGNORECASE):
            raise AssertionError("image result data URL has an unsupported type")
    try:
        image_bytes = base64.b64decode(encoded, validate=True)
    except (ValueError, binascii.Error) as exc:
        raise AssertionError("image result is not valid base64") from exc
    if not image_bytes or len(image_bytes) > max_bytes:
        raise AssertionError("image result exceeds the bounded non-empty image contract")
    try:
        with Image.open(io.BytesIO(image_bytes)) as image:
            if image.format not in {"PNG", "JPEG", "WEBP"}:
                raise AssertionError("image result format is not supported")
            width, height = image.size
            if width <= 0 or height <= 0 or width * height > 67_108_864:
                raise AssertionError("image result dimensions exceed the safety bound")
            image.load()
    except AssertionError:
        raise
    except Exception as exc:
        raise AssertionError("image result is not a decodable image") from exc
    return image_bytes


def decode_image_data_urls(text: str) -> list[bytes]:
    """Decode all bounded data URLs in a text response in memory."""
    if not isinstance(text, str):
        return []
    values = re.findall(r"data:image/(?:png|jpeg|webp);base64,[A-Za-z0-9+/=]+", text, re.IGNORECASE)
    return [decode_image_payload(value) for value in values]


def require_stream_response(response, *, content_type: str = "text/event-stream") -> None:
    """Validate a stream response without pre-consuming a successful body."""
    require_http_response(response, content_type=content_type)


def iter_sse_data(response):
    """Yield bounded SSE data fields; successful bodies are consumed only here."""
    for line in response.iter_lines():
        if not line:
            continue
        text = line.decode("utf-8", errors="replace") if isinstance(line, bytes) else str(line)
        if text.startswith("data:"):
            yield text[5:].strip()
