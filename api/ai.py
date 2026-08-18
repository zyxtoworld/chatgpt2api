from __future__ import annotations

import asyncio
import json
import threading
from functools import partial

import anyio
from fastapi import APIRouter, Header, HTTPException, Request, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, ConfigDict, Field

from api.image_inputs import (
    MAX_IMAGE_EDIT_INPUTS,
    close_image_sources,
    parse_image_edit_request,
    read_image_sources,
    validate_image_api_options,
)
from api.support import require_identity_async, resolve_image_base_url
from services.content_filter import (
    CONTENT_FILTER_REJECTION_MESSAGE,
    check_request_async,
    request_shape,
    request_text,
)
from services.editable_file_task_service import editable_file_task_service
from services.log_service import (
    LoggedCall,
    iterate_ws_ai_chunks,
    run_ws_ai_in_threadpool,
)
from services.protocol.error_response import (
    PUBLIC_SERVER_ERROR_MESSAGE,
    exception_log_message,
    openai_error_response,
    public_exception_message,
)
from services.protocol.responses_websocket import (
    CodexResponsesWebSocketTransport,
    CodexResponsesWebSocketUnavailable,
    PreparedResponsesWebSocketTurn,
    ResponsesWebSocketRequestError,
    ResponsesWebSocketSession,
)
from services.protocol import (
    anthropic_v1_messages,
    openai_v1_chat_complete,
    openai_v1_image_edit,
    openai_v1_image_generations,
    openai_v1_models,
    openai_v1_response,
    openai_search,
)
from services.opened_file_response import OpenedFileResponse
from services.model_service import ModelCatalogPendingError, run_model_catalog_in_threadpool


_RESPONSES_WEBSOCKET_CAPACITY = 64
_RESPONSES_WEBSOCKET_SLOTS = threading.BoundedSemaphore(_RESPONSES_WEBSOCKET_CAPACITY)
_EDITABLE_TASK_IO_CAPACITY = 8
_EDITABLE_TASK_IO_STATE = threading.local()


def _editable_task_io_limiter() -> anyio.CapacityLimiter:
    limiter = getattr(_EDITABLE_TASK_IO_STATE, "limiter", None)
    if limiter is None:
        limiter = anyio.CapacityLimiter(_EDITABLE_TASK_IO_CAPACITY)
        _EDITABLE_TASK_IO_STATE.limiter = limiter
    return limiter


async def run_editable_task_io(func, *args, **kwargs):
    return await anyio.to_thread.run_sync(
        partial(func, *args, **kwargs),
        limiter=_editable_task_io_limiter(),
    )


def _close_response_iterators(*iterators: object) -> None:
    seen: set[int] = set()
    for iterator in iterators:
        marker = id(iterator)
        if marker in seen:
            continue
        seen.add(marker)
        close = getattr(iterator, "close", None)
        if not callable(close):
            continue
        try:
            close()
        except Exception:
            pass


def _responses_websocket_turn_events(
        native_transport: CodexResponsesWebSocketTransport,
        turn: PreparedResponsesWebSocketTurn,
        cache_scope: str = "",
):
    warmup = turn.incremental_body.get("generate") is False
    try:
        yield from native_transport.events(turn)
        return
    except CodexResponsesWebSocketUnavailable:
        if warmup:
            raise
        # No request reached the upstream socket, so the established HTTP
        # implementation is a safe fallback for this connection.
        pass
    native_transport.close()
    if cache_scope:
        yield from openai_v1_response.response_events(
            turn.replay_body,
            cache_scope=cache_scope,
        )
    else:
        yield from openai_v1_response.response_events(turn.replay_body)


async def _close_expired_responses_websocket(websocket: WebSocket) -> None:
    await websocket.send_json({
        "type": "error",
        "error": {
            "type": "invalid_request_error",
            "code": "websocket_connection_limit_reached",
            "message": (
                "Responses websocket connection limit reached (60 minutes). "
                "Create a new websocket connection to continue."
            ),
        },
        "status": 400,
    })
    await websocket.close(code=1000, reason="websocket connection lifetime reached")


class ImageGenerationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    prompt: str = Field(..., min_length=1)
    model: str = "gpt-image-2"
    n: int = Field(default=1, ge=1, le=10)
    size: str | None = None
    quality: str = "auto"
    response_format: str = "b64_json"
    background: str | None = None
    moderation: str | None = None
    output_compression: int | None = None
    output_format: str | None = None
    partial_images: int | None = None
    style: str | None = None
    user: str | None = None
    stream: bool | None = None


class ChatCompletionRequest(BaseModel):
    model_config = ConfigDict(extra="allow", strict=True)
    model: str | None = None
    prompt: str | None = None
    n: int | None = None
    stream: bool | None = None
    modalities: list[str] | None = None
    messages: list[dict[str, object]] | None = None


class ResponseCreateRequest(BaseModel):
    model_config = ConfigDict(extra="allow", strict=True)
    model: str | None = None
    input: object | None = None
    tools: list[dict[str, object]] | None = None
    tool_choice: object | None = None
    stream: bool | None = None


class AnthropicMessageRequest(BaseModel):
    model_config = ConfigDict(extra="allow", strict=True)
    model: str | None = None
    messages: object | None = None
    system: object | None = None
    stream: bool | None = None


class SearchRequest(BaseModel):
    prompt: str = Field(..., min_length=1)


class EditableFileTaskRequest(BaseModel):
    prompt: str = ""
    base64_images: list[str] = Field(default_factory=list, max_length=MAX_IMAGE_EDIT_INPUTS)
    client_task_id: str | None = Field(default=None, max_length=256, pattern=r"^[^,]+$")


async def filter_or_log(call: LoggedCall, text: str) -> None:
    try:
        await check_request_async(text)
    except HTTPException as exc:
        await call.log_async("调用失败", status="failed", error=exception_log_message(exc))
        raise


def create_router() -> APIRouter:
    router = APIRouter()

    @router.get("/v1/models")
    async def list_models(authorization: str | None = Header(default=None)):
        await require_identity_async(authorization)
        try:
            return await run_model_catalog_in_threadpool(openai_v1_models.list_models)
        except ModelCatalogPendingError as exc:
            return openai_error_response(
                exc,
                exc.status_code,
                headers={"Retry-After": str(exc.retry_after_seconds)},
                safe_message=public_exception_message(exc, "The model catalog is still warming up."),
            )
        except Exception as exc:
            raise HTTPException(status_code=502, detail={"error": PUBLIC_SERVER_ERROR_MESSAGE}) from exc

    @router.post("/v1/images/generations")
    async def generate_images(
            body: ImageGenerationRequest,
            request: Request,
            authorization: str | None = Header(default=None),
    ):
        identity = await require_identity_async(authorization)
        payload = validate_image_api_options(body.model_dump(mode="python"))
        payload["base_url"] = resolve_image_base_url(request)
        call = LoggedCall(identity, "/v1/images/generations", body.model, "文生图", request_text=body.prompt)
        await filter_or_log(call, body.prompt)
        return await call.run(openai_v1_image_generations.handle, payload, sse="images")

    @router.post("/v1/images/edits")
    async def edit_images(
        request: Request,
        authorization: str | None = Header(default=None),
    ):
        identity = await require_identity_async(authorization)
        payload, image_sources, mask_sources = await parse_image_edit_request(request)
        try:
            if "client_task_id" in payload:
                raise HTTPException(
                    status_code=400,
                    detail={"error": "client_task_id is only supported by /api/image-tasks/edits"},
                )
            prompt = str(payload["prompt"])
            model = str(payload["model"])
            call = LoggedCall(identity, "/v1/images/edits", model, "图生图", request_text=prompt)
            await filter_or_log(call, prompt)
            payload["images"] = await read_image_sources(image_sources)
            if mask_sources:
                payload["mask"] = await read_image_sources(mask_sources)
            payload["base_url"] = resolve_image_base_url(request)
            return await call.run(openai_v1_image_edit.handle, payload, sse="images")
        finally:
            await close_image_sources([*image_sources, *mask_sources])

    @router.post("/v1/chat/completions")
    async def create_chat_completion(body: ChatCompletionRequest, authorization: str | None = Header(default=None)):
        identity = await require_identity_async(authorization)
        payload = body.model_dump(mode="python")
        model = str(payload.get("model") or "auto")
        request_preview = request_text(payload.get("prompt"), payload.get("messages"))
        call = LoggedCall(
            identity,
            "/v1/chat/completions",
            model,
            "文本生成",
            request_text=request_preview,
            request_shape=request_shape(payload.get("messages")),
        )
        await filter_or_log(call, request_preview)
        return await call.run(
            openai_v1_chat_complete.handle,
            payload,
            cache_scope=str(identity.get("id") or ""),
            authenticated=True,
        )

    @router.post("/v1/responses")
    async def create_response(body: ResponseCreateRequest, authorization: str | None = Header(default=None)):
        identity = await require_identity_async(authorization)
        payload = body.model_dump(mode="python")
        model = str(payload.get("model") or "auto")
        request_preview = request_text(payload.get("input"), payload.get("instructions"))
        call = LoggedCall(
            identity,
            "/v1/responses",
            model,
            "Responses",
            request_text=request_preview,
            request_shape=request_shape(payload.get("input")),
        )
        await filter_or_log(call, request_preview)
        return await call.run(
            openai_v1_response.handle,
            payload,
            sse="responses",
            cache_scope=str(identity.get("id") or ""),
        )

    @router.websocket("/v1/responses")
    async def create_response_websocket(websocket: WebSocket):
        authorization = websocket.headers.get("authorization")
        try:
            identity = await require_identity_async(authorization)
        except HTTPException:
            await websocket.close(code=1008, reason="invalid authorization")
            return

        session = ResponsesWebSocketSession()
        native_transport = CodexResponsesWebSocketTransport()
        await websocket.accept()
        if not _RESPONSES_WEBSOCKET_SLOTS.acquire(blocking=False):
            try:
                await websocket.send_json({
                    "type": "error",
                    "error": {
                        "type": "server_error",
                        "code": "websocket_connection_capacity_reached",
                        "message": "Responses websocket connection capacity reached; try again later.",
                    },
                    "status": 429,
                })
                await websocket.close(code=1013, reason="websocket connection capacity reached")
            finally:
                session.close()
            return
        try:
            while True:
                remaining_lifetime = session.remaining_lifetime_seconds()
                if remaining_lifetime <= 0:
                    await _close_expired_responses_websocket(websocket)
                    return
                try:
                    inbound = await asyncio.wait_for(
                        websocket.receive_json(),
                        timeout=remaining_lifetime,
                    )
                except TimeoutError:
                    await _close_expired_responses_websocket(websocket)
                    return
                except (json.JSONDecodeError, UnicodeDecodeError, TypeError, KeyError):
                    await websocket.send_json({
                        "type": "error",
                        "error": {
                            "type": "invalid_request_error",
                            "code": "invalid_json",
                            "message": "Request body is not valid JSON.",
                        },
                    })
                    continue
                try:
                    identity = await require_identity_async(authorization)
                except HTTPException:
                    await websocket.close(code=1008, reason="authorization expired or revoked")
                    return
                try:
                    turn = session.prepare_turn(inbound)
                except ResponsesWebSocketRequestError as exc:
                    await websocket.send_json({
                        "type": "error",
                        "error": {
                            "type": "invalid_request_error",
                            "code": exc.code,
                            "message": exc.message,
                        },
                    })
                    continue
                payload = turn.replay_body
                model = str(payload.get("model") or "auto")
                request_preview = request_text(payload.get("input"), payload.get("instructions"))
                call = LoggedCall(
                    identity,
                    "/v1/responses",
                    model,
                    "Responses WebSocket",
                    request_text=request_preview,
                    request_shape=request_shape(payload.get("input")),
                )
                try:
                    await filter_or_log(call, request_preview)
                except HTTPException as exc:
                    status_code = exc.status_code if isinstance(exc.status_code, int) else 500
                    if 400 <= status_code < 500:
                        await websocket.send_json({
                            "type": "error",
                            "error": {
                                "type": "invalid_request_error",
                                "code": "content_filter",
                                "message": CONTENT_FILTER_REJECTION_MESSAGE,
                            },
                        })
                        continue
                    raise
                raw_events = _responses_websocket_turn_events(
                    native_transport,
                    turn,
                    cache_scope=str(identity.get("id") or ""),
                )
                events = call.stream(raw_events)
                turn_terminal = False
                try:
                    async for event in iterate_ws_ai_chunks(events):
                        await websocket.send_json(event)
                        if event.get("type") in {"response.completed", "response.incomplete"}:
                            session.commit(turn.replay_body, event.get("response"))
                        elif event.get("type") in {"response.failed", "error"}:
                            session.fail(turn)
                        if event.get("type") in {
                                "response.completed",
                                "response.incomplete",
                                "response.failed",
                                "error",
                        }:
                            turn_terminal = True
                            break
                    if not turn_terminal:
                        session.fail(turn)
                        await websocket.send_json({
                            "type": "error",
                            "error": {
                                "type": "server_error",
                                "code": "upstream_error",
                                "message": PUBLIC_SERVER_ERROR_MESSAGE,
                            },
                        })
                finally:
                    await run_ws_ai_in_threadpool(_close_response_iterators, events, raw_events)
        except WebSocketDisconnect:
            return
        except Exception as exc:
            await websocket.send_json({
                "type": "error",
                "error": {
                    "type": "server_error",
                    "code": "upstream_error",
                    "message": public_exception_message(exc, PUBLIC_SERVER_ERROR_MESSAGE),
                },
            })
        finally:
            try:
                await run_ws_ai_in_threadpool(native_transport.close)
            finally:
                try:
                    session.close()
                finally:
                    _RESPONSES_WEBSOCKET_SLOTS.release()

    @router.post("/v1/messages")
    async def create_message(
            body: AnthropicMessageRequest,
            authorization: str | None = Header(default=None),
            x_api_key: str | None = Header(default=None, alias="x-api-key"),
            anthropic_version: str | None = Header(default=None, alias="anthropic-version"),
    ):
        identity = await require_identity_async(authorization or (f"Bearer {x_api_key}" if x_api_key else None))
        payload = body.model_dump(mode="python")
        model = str(payload.get("model") or "auto")
        request_preview = request_text(payload.get("system"), payload.get("messages"), payload.get("tools"))
        call = LoggedCall(identity, "/v1/messages", model, "Messages", request_text=request_preview)
        await filter_or_log(call, request_preview)
        return await call.run(anthropic_v1_messages.handle, payload, sse="anthropic")

    @router.post("/v1/search")
    async def search(body: SearchRequest, authorization: str | None = Header(default=None)):
        identity = await require_identity_async(authorization)
        call = LoggedCall(identity, "/v1/search", openai_search.MODEL, "搜索", request_text=body.prompt)
        await filter_or_log(call, body.prompt)
        return await call.run(openai_search.handle, body.model_dump(mode="python"))

    @router.get("/v1/editable-file-tasks")
    async def list_editable_file_tasks(ids: str = "", authorization: str | None = Header(default=None)):
        identity = await require_identity_async(authorization)
        task_ids = [item.strip() for item in ids.split(",") if item.strip()]
        return await run_editable_task_io(editable_file_task_service.list_tasks, identity, task_ids)

    @router.api_route("/files/{file_path:path}", methods=["GET", "HEAD"])
    async def download_editable_file(file_path: str):
        try:
            opened_file = await run_editable_task_io(editable_file_task_service.open_public_file, file_path)
        except Exception as exc:
            raise HTTPException(status_code=404, detail={"error": "file not found"}) from exc
        try:
            return OpenedFileResponse(opened_file)
        except Exception:
            opened_file.file.close()
            raise

    @router.post("/v1/ppt/generations")
    async def create_ppt_task(body: EditableFileTaskRequest, request: Request, authorization: str | None = Header(default=None)):
        identity = await require_identity_async(authorization)
        await filter_or_log(LoggedCall(identity, "/v1/ppt/generations", "gpt-5-5-thinking", "PPT生成任务", request_text=body.prompt), body.prompt)
        try:
            return await run_editable_task_io(
                editable_file_task_service.submit_ppt,
                identity,
                client_task_id=body.client_task_id or "",
                prompt=body.prompt,
                base64_images=body.base64_images,
                base_url=resolve_image_base_url(request),
            )
        except ValueError as exc:
            raise HTTPException(
                status_code=400,
                detail={"error": public_exception_message(exc, "editable file task request failed")},
            ) from exc

    @router.post("/v1/psd/generations")
    async def create_psd_task(body: EditableFileTaskRequest, request: Request, authorization: str | None = Header(default=None)):
        identity = await require_identity_async(authorization)
        await filter_or_log(LoggedCall(identity, "/v1/psd/generations", "gpt-5-5-thinking", "PSD生成任务", request_text=body.prompt), body.prompt)
        try:
            return await run_editable_task_io(
                editable_file_task_service.submit_psd,
                identity,
                client_task_id=body.client_task_id or "",
                prompt=body.prompt,
                base64_images=body.base64_images,
                base_url=resolve_image_base_url(request),
            )
        except ValueError as exc:
            raise HTTPException(
                status_code=400,
                detail={"error": public_exception_message(exc, "editable file task request failed")},
            ) from exc

    return router
