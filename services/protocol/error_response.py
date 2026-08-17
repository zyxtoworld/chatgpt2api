from __future__ import annotations

from typing import Any

from fastapi.responses import JSONResponse

from services.storage.base import StorageDataError


PUBLIC_SERVER_ERROR_MESSAGE = "The upstream request failed. Please try again later."

_SAFE_IMPORT_JOB_ERROR_MESSAGES = frozenset(
    {
        "invalid request",
        "invalid payload",
        "missing access_token",
        "unknown error",
    }
)
_IMPORT_JOB_ERROR_FALLBACK = "import failed"
IMPORT_JOB_ERROR_MAX_ITEMS = 100
_IMPORT_JOB_ERROR_FIELDS = frozenset({"name", "error"})


def _safe_import_job_error_message(value: object) -> str:
    message = value.strip() if isinstance(value, str) else ""
    if message in _SAFE_IMPORT_JOB_ERROR_MESSAGES or message == _IMPORT_JOB_ERROR_FALLBACK:
        return message
    prefix, separator, suffix = message.partition(" ")
    if prefix == "HTTP" and separator and suffix.isdecimal():
        status = int(suffix)
        if 100 <= status <= 599:
            return f"HTTP {status}"
    return _IMPORT_JOB_ERROR_FALLBACK


def _is_canonical_import_job_error_message(value: object) -> bool:
    if not isinstance(value, str):
        return False
    return value == value.strip() and _safe_import_job_error_message(value) == value


def canonicalize_import_job_errors(raw: object) -> list[dict[str, str]]:
    """Convert import failures into the bounded format stored on disk."""
    if not isinstance(raw, list):
        raise StorageDataError()
    return [
        {
            "name": f"item-{index + 1}",
            "error": _safe_import_job_error_message(item.get("error") if isinstance(item, dict) else ""),
        }
        for index, item in enumerate(raw[:IMPORT_JOB_ERROR_MAX_ITEMS])
    ]


def validate_import_job_errors(raw: object) -> list[dict[str, str]]:
    """Accept only the exact canonical import-error snapshot format."""
    if not isinstance(raw, list) or len(raw) > IMPORT_JOB_ERROR_MAX_ITEMS:
        raise StorageDataError()
    validated: list[dict[str, str]] = []
    for index, item in enumerate(raw):
        if not isinstance(item, dict) or set(item) != _IMPORT_JOB_ERROR_FIELDS:
            raise StorageDataError()
        name = item.get("name")
        error = item.get("error")
        if name != f"item-{index + 1}" or not _is_canonical_import_job_error_message(error):
            raise StorageDataError()
        validated.append({"name": name, "error": error})
    return validated


def sanitize_import_job_errors(raw: object) -> list[dict[str, str]]:
    """Project persisted import errors to bounded, provenance-free diagnostics."""
    if not isinstance(raw, list):
        return []

    safe_errors: list[dict[str, str]] = []
    for index, item in enumerate(raw[:IMPORT_JOB_ERROR_MAX_ITEMS]):
        if not isinstance(item, dict):
            continue
        safe_errors.append({"name": f"item-{index + 1}", "error": _safe_import_job_error_message(item.get("error"))})
    return safe_errors


class PublicSafeErrorMarker:
    """Marker contract for domain errors allowed to expose a controlled message."""

    def public_safe_message(self) -> str:
        raise NotImplementedError


class PublicSafeError(RuntimeError, PublicSafeErrorMarker):
    """领域错误 whose message is explicitly safe for task/API diagnostics."""

    def __init__(self, public_message: str) -> None:
        message = str(public_message or "").strip()
        if not message:
            raise ValueError("public_message is required")
        self._public_safe_message = message
        super().__init__(message)

    def public_safe_message(self) -> str:
        return self._public_safe_message


class PublicSafeValueError(ValueError, PublicSafeErrorMarker):
    """A validation error whose message was explicitly approved for clients."""

    def __init__(self, public_message: str) -> None:
        message = str(public_message or "").strip()
        if not message:
            raise ValueError("public_message is required")
        self._public_safe_message = message
        super().__init__(message)

    def public_safe_message(self) -> str:
        return self._public_safe_message


class ImportJobActiveError(PublicSafeValueError):
    """The connection cannot be mutated while its import job is active."""


def project_public_responses_event(event: object, *, model: str) -> object:
    """Project untrusted upstream Responses failures to a fixed public contract."""
    if not isinstance(event, dict):
        return event
    event_type = event.get("type")
    if event_type not in {"response.failed", "error"}:
        return event

    projected: dict[str, Any] = {"type": event_type}
    sequence_number = event.get("sequence_number")
    if type(sequence_number) is int and sequence_number >= 0:
        projected["sequence_number"] = sequence_number

    if event_type == "error":
        projected.update(
            {
                "code": "server_error",
                "message": PUBLIC_SERVER_ERROR_MESSAGE,
                "param": None,
            }
        )
        return projected

    response = event.get("response")
    response = response if isinstance(response, dict) else {}
    created_at = response.get("created_at")
    if type(created_at) is not int or created_at < 0:
        created_at = 0
    public_model = model if isinstance(model, str) and model.strip() else "auto"
    projected["response"] = {
        "id": "resp_failed",
        "object": "response",
        "created_at": created_at,
        "status": "failed",
        "error": {
            "code": "upstream_error",
            "message": PUBLIC_SERVER_ERROR_MESSAGE,
        },
        "incomplete_details": None,
        "model": public_model,
        "output": [],
        "parallel_tool_calls": False,
    }
    return projected


def _message_from_value(value: object) -> str:
    if isinstance(value, str):
        return value
    if not isinstance(value, dict):
        return ""
    message = value.get("message")
    if isinstance(message, str) and message:
        return message
    return _message_from_value(value.get("error"))


def error_message_from_detail(detail: object) -> str:
    if isinstance(detail, list):
        messages = []
        for item in detail:
            if not isinstance(item, dict):
                continue
            raw_location = item.get("loc", [])
            location = ".".join(
                part if isinstance(part, str) else str(part)
                for part in (raw_location if isinstance(raw_location, (list, tuple)) else ())
                if isinstance(part, (str, int)) and part != "body"
            )
            raw_message = item.get("msg")
            message = raw_message.strip() if isinstance(raw_message, str) else ""
            if location and message:
                messages.append(f"{location}: {message}")
            elif message:
                messages.append(message)
        if messages and all(message == "request validation failed" for message in messages):
            return "request validation failed"
        return ""
    if isinstance(detail, dict):
        message = _message_from_value(detail.get("error"))
        if message:
            return message
    return detail.strip() if isinstance(detail, str) else ""


def _safe_error_type(value: object, fallback: str) -> str:
    return value.strip() if isinstance(value, str) and value.strip() else fallback


def _safe_error_code(value: object, fallback: str) -> str:
    return value.strip() if isinstance(value, str) and value.strip() else fallback


def _safe_error_param(value: object, fallback: object = None) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else fallback if value is None else None


def public_exception_message(exc: BaseException, fallback: str) -> str:
    """Return only an explicitly marked domain message; fail closed otherwise."""
    safe_message = ""
    if isinstance(exc, PublicSafeErrorMarker):
        try:
            safe_message = exc.public_safe_message()
        except Exception:
            safe_message = ""
    if isinstance(safe_message, str) and safe_message.strip():
        return safe_message.strip()
    return str(fallback or "").strip() or PUBLIC_SERVER_ERROR_MESSAGE


def exception_log_message(exc: BaseException) -> str:
    """Return a diagnostic exception summary without its message/body."""
    status_code = getattr(exc, "status_code", None)
    if isinstance(status_code, int):
        return f"{exc.__class__.__name__} (status={status_code})"
    return exc.__class__.__name__


def _default_error_type(status_code: int) -> str:
    if status_code == 401:
        return "authentication_error"
    if status_code == 403:
        return "permission_error"
    if status_code == 429:
        return "rate_limit_error"
    if 400 <= status_code < 500:
        return "invalid_request_error"
    return "server_error"


def _default_error_code(status_code: int) -> str:
    if status_code == 401:
        return "invalid_api_key"
    if status_code == 403:
        return "permission_denied"
    if status_code == 429:
        return "rate_limit_exceeded"
    if 400 <= status_code < 500:
        return "bad_request"
    return "upstream_error"


def openai_error_payload(
    detail: object,
    status_code: int,
    *,
    error_type: str | None = None,
    code: object | None = None,
    param: object | None = None,
) -> dict[str, Any]:
    if status_code >= 500:
        return {
            "error": {
                "message": PUBLIC_SERVER_ERROR_MESSAGE,
                "type": _safe_error_type(error_type, _default_error_type(status_code)),
                "param": _safe_error_param(param),
                "code": _safe_error_code(code, _default_error_code(status_code)),
            }
        }
    error_detail = detail.get("error") if isinstance(detail, dict) else None
    if isinstance(error_detail, dict):
        return {
            "error": {
                "message": error_message_from_detail(error_detail) or "request failed",
                "type": _safe_error_type(
                    error_detail.get("type") or error_type,
                    _default_error_type(status_code),
                ),
                "param": _safe_error_param(error_detail.get("param", param)),
                "code": _safe_error_code(
                    error_detail.get("code") or code,
                    _default_error_code(status_code),
                ),
            }
        }
    return {
        "error": {
            "message": error_message_from_detail(detail) or "request failed",
            "type": _safe_error_type(error_type, _default_error_type(status_code)),
            "param": _safe_error_param(param),
            "code": _safe_error_code(code, _default_error_code(status_code)),
        }
    }


def openai_error_response(
    detail: object,
    status_code: int,
    *,
    headers: dict[str, str] | None = None,
    error_type: str | None = None,
    code: object | None = None,
    param: object | None = None,
) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content=openai_error_payload(detail, status_code, error_type=error_type, code=code, param=param),
        headers=headers,
    )


def anthropic_error_response(
    detail: object,
    status_code: int,
    *,
    headers: dict[str, str] | None = None,
) -> JSONResponse:
    error_type = "api_error" if status_code >= 500 else _default_error_type(status_code)
    return JSONResponse(
        status_code=status_code,
        content={
            "type": "error",
            "error": {
                "type": error_type,
                "message": (
                    PUBLIC_SERVER_ERROR_MESSAGE
                    if status_code >= 500
                    else error_message_from_detail(detail) or "request failed"
                ),
            },
        },
        headers=headers,
    )
