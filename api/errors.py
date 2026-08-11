from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from services.protocol.error_response import anthropic_error_response, openai_error_response
from services.task_executor import BackgroundTaskQueueFullError


def _is_openai_compatible_path(path: str) -> bool:
    return path == "/v1" or path.startswith("/v1/")


def _is_anthropic_messages_path(path: str) -> bool:
    return path == "/v1/messages"


def _public_validation_errors(exc: RequestValidationError) -> list[dict[str, object]]:
    """Project validation failures without reflecting submitted values or contexts."""
    projected: list[dict[str, object]] = []
    for error in exc.errors():
        if not isinstance(error, dict):
            continue
        location = error.get("loc")
        projected.append({
            "type": str(error.get("type") or "validation_error"),
            "loc": list(location) if isinstance(location, (list, tuple)) else [],
            "msg": str(error.get("msg") or "request validation failed"),
        })
    return projected


def _compatible_error_response(
    request: Request,
    detail: object,
    status_code: int,
    headers: dict[str, str] | None = None,
) -> JSONResponse:
    # Nested error objects commonly come from upstream HTTP payloads.  They
    # have no local provenance marker, so do not reflect their message/code.
    if status_code < 500 and isinstance(detail, dict) and isinstance(detail.get("error"), dict):
        detail = {"error": "request failed"}
    if _is_anthropic_messages_path(request.url.path):
        return anthropic_error_response(detail, status_code, headers=headers)
    return openai_error_response(detail, status_code, headers=headers)


def install_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(BackgroundTaskQueueFullError)
    async def background_task_queue_full_handler(
        request: Request,
        exc: BackgroundTaskQueueFullError,
    ) -> JSONResponse:
        message = exc.public_safe_message()
        headers = {"Retry-After": "1"}
        if _is_openai_compatible_path(request.url.path):
            return openai_error_response(message, 429, headers=headers)
        return JSONResponse(
            status_code=429,
            content={"detail": {"error": message}},
            headers=headers,
        )

    @app.exception_handler(StarletteHTTPException)
    async def http_exception_handler(request: Request, exc: StarletteHTTPException) -> JSONResponse:
        if _is_openai_compatible_path(request.url.path):
            return _compatible_error_response(request, exc.detail, exc.status_code, exc.headers)
        return JSONResponse(
            status_code=exc.status_code,
            content={"detail": jsonable_encoder(exc.detail)},
            headers=exc.headers,
        )

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
        errors = _public_validation_errors(exc)
        if _is_openai_compatible_path(request.url.path):
            return _compatible_error_response(request, errors, 422)
        return JSONResponse(status_code=422, content={"detail": jsonable_encoder(errors)})
