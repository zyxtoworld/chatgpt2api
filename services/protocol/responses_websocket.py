from __future__ import annotations

import copy
import json
import time
import uuid
import weakref
from dataclasses import dataclass
from threading import Lock
from typing import Any, Callable, Iterator

from fastapi import HTTPException
from websockets.exceptions import InvalidStatus
from websockets.sync.client import connect as websocket_connect

from services.account_service import account_service
from services.model_service import ModelUnavailableError
from services.openai_backend_api import CODEX_RESPONSE_MAX_EVENT_BYTES
from services.protocol.openai_v1_response import (
    codex_response_payload,
    created_response_from_event,
    in_progress_response_from_event,
    messages_from_input,
    project_public_codex_response_event,
    resolve_codex_reasoning_effort,
    terminal_response_from_event,
    validate_response_core_parameters,
)
from services.proxy_service import proxy_settings
from utils.log import logger

MAX_RESPONSES_WEBSOCKET_TRANSCRIPT_BYTES = 16 * 1024 * 1024
MAX_RESPONSES_WEBSOCKET_TOTAL_TRANSCRIPT_BYTES = 128 * 1024 * 1024
MAX_RESPONSES_WEBSOCKET_LIFETIME_SECONDS = 60 * 60
CODEX_RESPONSES_WEBSOCKET_BETA = "responses_websockets=2026-02-06"
CODEX_RESPONSES_WEBSOCKET_URL = "wss://chatgpt.com/backend-api/codex/responses"
CODEX_RESPONSES_WEBSOCKET_USER_AGENT = (
    "codex-tui/0.146.0 (Mac OS 26.5.0; arm64) iTerm.app/3.6.10 (codex-tui; 0.146.0)"
)
CODEX_RESPONSES_WEBSOCKET_CONNECT_RETRY_DELAYS = (0.2, 0.4)
_CODEX_WEBSOCKET_SUCCESS_TERMINALS = {
    "response.completed",
    "response.incomplete",
}
_CODEX_WEBSOCKET_FAILURE_TERMINALS = {"response.failed", "error"}
_CODEX_WEBSOCKET_REUSE_PROPERTY_FIELDS = (
    "model",
    "instructions",
    "tools",
    "tool_choice",
    "parallel_tool_calls",
    "reasoning",
    "store",
    "stream",
    "include",
    "service_tier",
    "prompt_cache_key",
    "text",
    "context_management",
)


class CodexResponsesWebSocketUnavailable(RuntimeError):
    """The native upstream handshake failed before a request was sent."""


class CodexResponsesWebSocketProtocolError(RuntimeError):
    """The native upstream socket returned an invalid or incomplete turn."""


@dataclass(frozen=True)
class PreparedResponsesWebSocketTurn:
    incremental_body: dict[str, Any]
    replay_body: dict[str, Any]
    previous_response_id: str


@dataclass(frozen=True)
class ResponsesWebSocketRequestError(ValueError):
    code: str
    message: str

    def __str__(self) -> str:
        return self.message


def _validation_error_message(exc: HTTPException) -> str:
    detail = exc.detail if isinstance(exc.detail, dict) else {}
    message = detail.get("error")
    return message.strip() if isinstance(message, str) and message.strip() else "invalid Responses WebSocket request"


def _transcript_items(input_value: object) -> list[dict[str, Any]]:
    if isinstance(input_value, str):
        return messages_from_input(input_value)
    if isinstance(input_value, dict):
        return [copy.deepcopy(input_value)]
    if isinstance(input_value, list):
        return [copy.deepcopy(item) for item in input_value if isinstance(item, dict)]
    return []


def _websocket_reuse_properties(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        field: copy.deepcopy(payload.get(field))
        for field in _CODEX_WEBSOCKET_REUSE_PROPERTY_FIELDS
    }


def _reconcile_completed_output(
        event: dict[str, Any],
        completed_items: dict[int, dict[str, Any]],
) -> dict[str, Any]:
    response = event.get("response")
    if not isinstance(response, dict) or not isinstance(response.get("output"), list):
        return event
    reconciled = copy.deepcopy(event)
    output = reconciled["response"]["output"]
    for index, item in enumerate(output):
        completed = completed_items.get(index)
        if not isinstance(item, dict) or completed is None:
            continue
        merged = copy.deepcopy(completed)
        merged.update(item)
        output[index] = merged
    return reconciled


@dataclass
class _TranscriptReservation:
    size: int = 0


class _ResponsesWebSocketTranscriptBudget:
    def __init__(self, max_bytes: int) -> None:
        self._max_bytes = max(1, int(max_bytes))
        self._used_bytes = 0
        self._lock = Lock()

    @property
    def max_bytes(self) -> int:
        return self._max_bytes

    def replace(self, reservation: _TranscriptReservation, size: int) -> bool:
        next_size = max(0, int(size))
        with self._lock:
            next_used = self._used_bytes - reservation.size + next_size
            if next_used > self._max_bytes:
                return False
            self._used_bytes = next_used
            reservation.size = next_size
            return True

    def release(self, reservation: _TranscriptReservation) -> None:
        with self._lock:
            self._used_bytes = max(0, self._used_bytes - reservation.size)
            reservation.size = 0


_RESPONSES_WEBSOCKET_TRANSCRIPT_BUDGET = _ResponsesWebSocketTranscriptBudget(
    MAX_RESPONSES_WEBSOCKET_TOTAL_TRANSCRIPT_BYTES
)


class ResponsesWebSocketSession:
    """Connection-local state for official Responses WebSocket turns."""

    def __init__(
            self,
            max_transcript_bytes: int = MAX_RESPONSES_WEBSOCKET_TRANSCRIPT_BYTES,
            *,
            max_lifetime_seconds: float = MAX_RESPONSES_WEBSOCKET_LIFETIME_SECONDS,
            clock: Callable[[], float] = time.monotonic,
            transcript_budget: _ResponsesWebSocketTranscriptBudget = _RESPONSES_WEBSOCKET_TRANSCRIPT_BUDGET,
    ):
        self._max_transcript_bytes = max(1, int(max_transcript_bytes))
        self._clock = clock
        self._expires_at = clock() + max(0.0, float(max_lifetime_seconds))
        self._last_response_id = ""
        self._last_request_properties: dict[str, Any] | None = None
        self._transcript: list[dict[str, Any]] = []
        self._unavailable_response_id = ""
        self._unavailable_error_code = ""
        self._unavailable_error_message = ""
        self._transcript_budget = transcript_budget
        self._transcript_reservation = _TranscriptReservation()
        self._transcript_finalizer = weakref.finalize(
            self,
            transcript_budget.release,
            self._transcript_reservation,
        )

    def remaining_lifetime_seconds(self) -> float:
        return max(0.0, self._expires_at - self._clock())

    def prepare(self, inbound: object) -> dict[str, Any]:
        return self.prepare_turn(inbound).replay_body

    def prepare_turn(self, inbound: object) -> PreparedResponsesWebSocketTurn:
        if not isinstance(inbound, dict) or inbound.get("type") != "response.create":
            raise ResponsesWebSocketRequestError("unsupported_event", "unsupported websocket event")

        body = copy.deepcopy(inbound)
        body.pop("type", None)
        try:
            validate_response_core_parameters(body)
        except HTTPException as exc:
            raise ResponsesWebSocketRequestError(
                "invalid_request_error",
                _validation_error_message(exc),
            ) from exc
        if body.get("background") is not None:
            raise ResponsesWebSocketRequestError(
                "invalid_request_error",
                "background is not supported in Responses WebSocket mode",
            )
        body.pop("background", None)
        body.pop("stream", None)
        if "generate" in body and body.get("generate") is not False:
            raise ResponsesWebSocketRequestError(
                "invalid_request_error",
                "generate must be false in Responses WebSocket mode",
            )
        raw_previous_response_id = body.get("previous_response_id")
        if raw_previous_response_id is not None and not isinstance(raw_previous_response_id, str):
            raise ResponsesWebSocketRequestError(
                "invalid_request_error",
                "previous_response_id must be a string",
            )
        previous_response_id = (raw_previous_response_id or "").strip()
        if previous_response_id:
            if previous_response_id == self._unavailable_response_id:
                raise ResponsesWebSocketRequestError(
                    self._unavailable_error_code,
                    self._unavailable_error_message,
                )
            if not self._last_response_id or previous_response_id != self._last_response_id:
                raise ResponsesWebSocketRequestError(
                    "previous_response_not_found",
                    "previous response is not available on this websocket connection",
                )

        incremental_body = copy.deepcopy(body)
        if previous_response_id:
            incremental_body["previous_response_id"] = previous_response_id
        else:
            incremental_body.pop("previous_response_id", None)
        incremental_body["stream"] = True

        validation_body = copy.deepcopy(incremental_body)
        validation_body.pop("previous_response_id", None)
        try:
            validation_payload = codex_response_payload(validation_body, websocket=True)
        except HTTPException as exc:
            raise ResponsesWebSocketRequestError(
                "invalid_request_error",
                _validation_error_message(exc),
            ) from exc

        replay_body = copy.deepcopy(incremental_body)
        replay_body.pop("previous_response_id", None)
        if previous_response_id:
            current_items = _transcript_items(replay_body.get("input"))
            replay_body["input"] = [*copy.deepcopy(self._transcript), *current_items]

        request_properties = _websocket_reuse_properties(validation_payload)
        if previous_response_id and request_properties != self._last_request_properties:
            incremental_body = copy.deepcopy(replay_body)

        self._check_size(incremental_body)
        self._check_size(replay_body)
        return PreparedResponsesWebSocketTurn(
            incremental_body=incremental_body,
            replay_body=replay_body,
            previous_response_id=previous_response_id,
        )

    def commit(self, body: dict[str, Any], completed_response: object) -> bool:
        if not isinstance(completed_response, dict):
            return False
        response_id = str(completed_response.get("id") or "").strip()
        if not response_id:
            return False

        request_properties = _websocket_reuse_properties(
            codex_response_payload(body, websocket=True)
        )

        transcript = _transcript_items(body.get("input"))
        output = completed_response.get("output")
        if isinstance(output, list):
            for item in output:
                if not isinstance(item, dict) or not str(item.get("type") or "").strip():
                    continue
                transcript.append(copy.deepcopy(item))

        transcript_size = self._encoded_size(transcript)
        if transcript_size > self._max_transcript_bytes:
            self._invalidate_transcript(
                response_id,
                "websocket_session_too_large",
                "websocket session state is too large",
            )
            logger.warning({
                "event": "responses_websocket_session_state_too_large",
                "requested_bytes": transcript_size,
                "capacity_bytes": self._max_transcript_bytes,
            })
            return False
        if not self._transcript_budget.replace(self._transcript_reservation, transcript_size):
            self._invalidate_transcript(
                response_id,
                "websocket_server_capacity_reached",
                "websocket session capacity reached; start a new response without previous_response_id",
            )
            logger.warning({
                "event": "responses_websocket_transcript_capacity_reached",
                "requested_bytes": transcript_size,
                "capacity_bytes": self._transcript_budget.max_bytes,
            })
            return False
        self._transcript = transcript
        self._last_response_id = response_id
        self._last_request_properties = request_properties
        self._clear_unavailable_response()
        return True

    def fail(self, turn: PreparedResponsesWebSocketTurn) -> None:
        """Evict the connection-local state referenced by a failed continuation."""
        if not turn.previous_response_id or turn.previous_response_id != self._last_response_id:
            return
        self._clear_transcript()

    def close(self) -> None:
        self._clear_transcript()
        self._clear_unavailable_response()

    def _invalidate_transcript(self, response_id: str, code: str, message: str) -> None:
        self._clear_transcript()
        self._unavailable_response_id = response_id
        self._unavailable_error_code = code
        self._unavailable_error_message = message

    def _clear_unavailable_response(self) -> None:
        self._unavailable_response_id = ""
        self._unavailable_error_code = ""
        self._unavailable_error_message = ""

    def _clear_transcript(self) -> None:
        self._transcript_budget.release(self._transcript_reservation)
        self._last_response_id = ""
        self._last_request_properties = None
        self._transcript = []

    def _check_size(self, value: object) -> int:
        encoded_size = self._encoded_size(value)
        if encoded_size > self._max_transcript_bytes:
            raise ResponsesWebSocketRequestError(
                "websocket_session_too_large",
                "websocket session state is too large",
            )
        return encoded_size

    @staticmethod
    def _encoded_size(value: object) -> int:
        return len(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))


class CodexResponsesWebSocketTransport:
    """One downstream Responses session backed by one reusable Codex socket."""

    def __init__(self, connector: Callable[..., Any] = websocket_connect) -> None:
        self._connector = connector
        self._connection: Any | None = None
        self._credential_key: tuple[str, str, str] | None = None
        self._disabled = False

    @property
    def is_connected(self) -> bool:
        return self._connection is not None and not self._disabled

    def events(self, turn: PreparedResponsesWebSocketTurn) -> Iterator[dict[str, Any]]:
        if self._disabled:
            raise CodexResponsesWebSocketUnavailable("native codex websocket is unavailable")
        model = str(turn.replay_body.get("model") or "auto").strip() or "auto"
        try:
            access_token = account_service.get_text_access_token(model=model, source_type="codex")
        except ModelUnavailableError as exc:
            raise CodexResponsesWebSocketUnavailable("native codex websocket is unavailable") from exc
        expected_account = None
        get_account_lease = getattr(account_service, "_get_account_lease", None)
        if callable(get_account_lease):
            _, expected_account = get_account_lease(access_token)
        account = account_service.get_account(access_token)
        account = account if isinstance(account, dict) else {}
        account_id = str(account.get("account_id") or account.get("chatgpt_account_id") or "").strip()
        proxy_url = proxy_settings.get_profile(account=account).proxy_url
        credential_key = (access_token, account_id, proxy_url)

        reused = self._connection is not None and self._credential_key == credential_key
        if not reused:
            self.close()
            self._connect(credential_key)

        body = turn.incremental_body if reused else turn.replay_body
        request_body = copy.deepcopy(body)
        previous_response_id = str(request_body.pop("previous_response_id", "") or "").strip()
        request_body.pop("type", None)
        request_body["stream"] = True
        wire_payload = codex_response_payload(request_body, websocket=True)
        resolve_codex_reasoning_effort(wire_payload, access_token=access_token)
        wire_payload["type"] = "response.create"
        if reused and previous_response_id:
            wire_payload["previous_response_id"] = previous_response_id

        connection = self._connection
        if connection is None:
            raise CodexResponsesWebSocketUnavailable("native codex websocket is unavailable")

        terminal = False
        completed_items: dict[int, dict[str, Any]] = {}
        try:
            connection.send(json.dumps(wire_payload, ensure_ascii=False, separators=(",", ":")))
            while True:
                raw_event = connection.recv(timeout=5 * 60)
                if not isinstance(raw_event, str):
                    raise CodexResponsesWebSocketProtocolError("invalid codex websocket event")
                try:
                    event = json.loads(raw_event)
                except (TypeError, ValueError) as exc:
                    raise CodexResponsesWebSocketProtocolError("invalid codex websocket event") from exc
                if not isinstance(event, dict) or not isinstance(event.get("type"), str):
                    raise CodexResponsesWebSocketProtocolError("invalid codex websocket event")
                try:
                    public_event = project_public_codex_response_event(event)
                except RuntimeError as exc:
                    raise CodexResponsesWebSocketProtocolError("invalid codex websocket event") from exc
                if public_event is None:
                    continue
                event = public_event
                event_type = event["type"]
                if event_type in {"response.created", "response.in_progress"}:
                    try:
                        active_response = (
                            created_response_from_event(event)
                            if event_type == "response.created"
                            else in_progress_response_from_event(event)
                        )
                    except RuntimeError as exc:
                        raise CodexResponsesWebSocketProtocolError("invalid codex websocket event") from exc
                    event = {**event, "response": active_response}
                if event_type == "response.output_item.done":
                    output_index = event.get("output_index")
                    item = event.get("item")
                    if (
                            isinstance(output_index, int)
                            and not isinstance(output_index, bool)
                            and output_index >= 0
                            and isinstance(item, dict)
                    ):
                        completed_items[output_index] = copy.deepcopy(item)
                if event_type in _CODEX_WEBSOCKET_SUCCESS_TERMINALS:
                    try:
                        terminal_response = terminal_response_from_event(event)
                    except RuntimeError as exc:
                        raise CodexResponsesWebSocketProtocolError("invalid codex websocket terminal event") from exc
                    event = {**event, "response": terminal_response}
                    event = _reconcile_completed_output(event, completed_items)
                    terminal = True
                    try:
                        if expected_account is None:
                            account_service.mark_text_used(access_token)
                        else:
                            account_service.mark_text_used(
                                access_token,
                                expected_account=expected_account,
                            )
                    except Exception as exc:
                        logger.warning({
                            "event": "codex_responses_websocket_usage_mark_failed",
                            "error_type": type(exc).__name__,
                        })
                    yield event
                    return
                yield event
                if event_type in _CODEX_WEBSOCKET_FAILURE_TERMINALS:
                    terminal = True
                    self.close()
                    return
        finally:
            if not terminal:
                self.close()

    def _connect(self, credential_key: tuple[str, str, str]) -> None:
        access_token, account_id, proxy_url = credential_key
        session_id = str(uuid.uuid4())
        thread_id = str(uuid.uuid4())
        headers = {
            "Authorization": f"Bearer {access_token}",
            "User-Agent": CODEX_RESPONSES_WEBSOCKET_USER_AGENT,
            "Originator": "codex-tui",
            "OpenAI-Beta": CODEX_RESPONSES_WEBSOCKET_BETA,
            "session-id": session_id,
            "thread-id": thread_id,
        }
        if account_id:
            headers["ChatGPT-Account-ID"] = account_id
        max_attempts = len(CODEX_RESPONSES_WEBSOCKET_CONNECT_RETRY_DELAYS) + 1
        for attempt in range(max_attempts):
            try:
                connection = self._connector(
                    CODEX_RESPONSES_WEBSOCKET_URL,
                    additional_headers=headers,
                    user_agent_header=None,
                    proxy=proxy_url or None,
                    open_timeout=20,
                    ping_interval=45,
                    ping_timeout=20,
                    close_timeout=10,
                    max_size=CODEX_RESPONSE_MAX_EVENT_BYTES,
                    max_queue=128,
                )
                break
            except Exception as exc:
                status = exc.response.status_code if isinstance(exc, InvalidStatus) else None
                exhausted = attempt + 1 >= max_attempts
                upgrade_unsupported = status == 426
                if exhausted or upgrade_unsupported:
                    self._connection = None
                    self._credential_key = None
                    self._disabled = True
                    logger.warning({
                        "event": "codex_responses_websocket_connect_fallback",
                        "attempt": attempt + 1,
                        "max_attempts": max_attempts,
                        "status": status,
                        "error_type": type(exc).__name__,
                    })
                    raise CodexResponsesWebSocketUnavailable(
                        "native codex websocket is unavailable"
                    ) from exc
                wait_secs = CODEX_RESPONSES_WEBSOCKET_CONNECT_RETRY_DELAYS[attempt]
                logger.warning({
                    "event": "codex_responses_websocket_connect_retry",
                    "attempt": attempt + 1,
                    "max_attempts": max_attempts,
                    "wait_secs": wait_secs,
                    "status": status,
                    "error_type": type(exc).__name__,
                })
                time.sleep(wait_secs)
        self._connection = connection
        self._credential_key = credential_key

    def close(self) -> None:
        connection = self._connection
        self._connection = None
        self._credential_key = None
        if connection is None:
            return
        try:
            connection.close()
        except Exception:
            pass
