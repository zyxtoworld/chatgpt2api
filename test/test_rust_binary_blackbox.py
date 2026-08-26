"""Black-box startup and native model-catalog contract for the Rust binary."""

from __future__ import annotations

import asyncio
import http.client
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit
from uuid import uuid4

import pytest


ROOT = Path(__file__).resolve().parents[1]
CODEX_LOCAL = ROOT / ".local" / "codex"


class _ModelUpstreamState:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.calls: list[tuple[str, str]] = []

    def record(self, path: str, authorization: str) -> None:
        with self.lock:
            self.calls.append((path, authorization))

    def snapshot(self) -> list[tuple[str, str]]:
        with self.lock:
            return list(self.calls)


class _ModelUpstreamHandler(BaseHTTPRequestHandler):
    state: _ModelUpstreamState

    def log_message(self, format: str, *args: object) -> None:
        del format, args

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        authorization = self.headers.get("Authorization", "")
        path = self.path
        self.state.record(path, authorization)
        if path == "/":
            self._send(200, b"<html></html>", "text/html")
            return
        if path == "/backend-anon/models?iim=false&is_gizmo=false":
            if authorization:
                self._send(401, b"unauthorized", "text/plain")
            else:
                self._send(
                    200,
                    json.dumps(
                        {"models": [{"slug": "anon-model", "title": "Anonymous"}]}
                    ).encode(),
                    "application/json",
                )
            return
        if path == "/backend-api/models?history_and_training_disabled=false":
            token = authorization.removeprefix("Bearer ").strip()
            if token == "free-a":
                self._send(
                    200,
                    json.dumps({"models": []}).encode(),
                    "application/json",
                )
            elif token == "free-b":
                self._send(
                    200,
                    json.dumps(
                        {"models": [{"slug": "free-model", "title": "Free"}]}
                    ).encode(),
                    "application/json",
                )
            elif token == "plus-a":
                self._send(
                    200,
                    json.dumps(
                        {"models": [{"slug": "plus-model", "title": "Plus"}]}
                    ).encode(),
                    "application/json",
                )
            else:
                self._send(401, b"unauthorized", "text/plain")
            return
        self._send(404, b"not found", "text/plain")

    def _send(self, status: int, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def _wait_for_port(address: tuple[str, int], timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(address, timeout=0.2):
                return
        except OSError:
            time.sleep(0.02)
    raise AssertionError(f"Rust binary did not bind {address}")


def _free_local_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def _request(address: tuple[str, int], path: str, authorization: str | None = None) -> tuple[int, bytes]:
    connection = http.client.HTTPConnection(*address, timeout=5)
    try:
        headers = {"Authorization": authorization} if authorization else {}
        connection.request("GET", path, headers=headers)
        response = connection.getresponse()
        return response.status, response.read()
    finally:
        connection.close()


def test_clean_rust_binary_native_models_black_box() -> None:
    run_id = f"{os.getpid()}-{uuid4().hex}"
    root = CODEX_LOCAL / "tmp" / "python" / f"rust-binary-blackbox-{run_id}"
    data_dir = root / "data"
    build_dir = CODEX_LOCAL / "target" / "rust-binary-blackbox"
    log_path = root / "rust.log"
    root.mkdir(parents=True, exist_ok=False)
    data_dir.mkdir()
    (data_dir / "accounts.json").write_text(
        json.dumps(
            {
                "items": [
                    {
                        "access_token": "free-a",
                        "status": "正常",
                        "type": "free",
                        "source_type": "web",
                    },
                    {
                        "access_token": "free-b",
                        "status": "正常",
                        "type": "free",
                        "source_type": "web",
                    },
                    {
                        "access_token": "plus-a",
                        "status": "正常",
                        "type": "plus",
                        "source_type": "web",
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    upstream_state = _ModelUpstreamState()
    _ModelUpstreamHandler.state = upstream_state
    upstream = ThreadingHTTPServer(("127.0.0.1", 0), _ModelUpstreamHandler)
    upstream_thread = threading.Thread(target=upstream.serve_forever, daemon=True)
    upstream_thread.start()
    rust_address = ("127.0.0.1", _free_local_port())

    command_env = os.environ.copy()
    command_tmp = CODEX_LOCAL / "tmp" / "python" / "rust-binary-blackbox-command"
    command_tmp.mkdir(parents=True, exist_ok=True)
    command_env.update(
        {
            "TEMP": str(command_tmp),
            "TMP": str(command_tmp),
            "TMPDIR": str(command_tmp),
            "CARGO_TARGET_DIR": str(build_dir),
            "CARGO_HOME": str(CODEX_LOCAL / "cargo-home"),
        }
    )
    binary_name = "chatgpt2api-rust.exe" if os.name == "nt" else "chatgpt2api-rust"
    binary = build_dir / "release" / binary_name
    process: subprocess.Popen[bytes] | None = None
    try:
        subprocess.run(
            [
                "cargo",
                "build",
                "--release",
                "--bin",
                "chatgpt2api-rust",
                "--manifest-path",
                str(ROOT / "rust" / "Cargo.toml"),
            ],
            cwd=ROOT,
            env=command_env,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        assert binary.is_file(), binary
        upstream_address = upstream.server_address
        process_env = command_env | {
            "RUST_BIND": f"{rust_address[0]}:{rust_address[1]}",
            "RUST_AUTH_KEY": "client-key",
            "RUST_DATA_DIR": str(data_dir),
            "RUST_ACCOUNTS_PATH": str(data_dir / "accounts.json"),
            "RUST_MODELS": "auto",
            "RUST_UPSTREAM_BASE_URL": f"http://{upstream_address[0]}:{upstream_address[1]}",
            "RUST_UPSTREAM_PROTOCOL": "chatgpt",
            "CODEX_CLIENT_VERSION": "0.147.0",
            "RUST_VERSION": "blackbox-test",
        }
        with log_path.open("wb") as log:
            process = subprocess.Popen(
                [str(binary)],
                cwd=ROOT,
                env=process_env,
                stdout=log,
                stderr=subprocess.STDOUT,
            )
        _wait_for_port(rust_address)

        accounts_path = data_dir / "accounts.json"
        healthy_status, healthy_body = _request(rust_address, "/health?format=json")
        assert healthy_status == 200
        healthy_payload = json.loads(healthy_body)
        assert healthy_payload == {
            "status": "ok",
            "healthy": True,
            "version": "blackbox-test",
            "storage": {"backend": "json", "health": {"status": "healthy"}},
            "proxy_runtime": {"enabled": False, "clearance_enabled": False},
            "accounts": {
                "total": 3,
                "cumulative_total": 3,
                "active": 3,
                "limited": 0,
                "abnormal": 0,
                "disabled": 0,
                "total_quota": 0,
                "total_success": 0,
                "total_fail": 0,
                "by_type": {"free": 2, "other": 1},
            },
        }
        for authorization in (None, "Bearer invalid-storage-info-key"):
            status, body = _request(
                rust_address,
                "/api/storage/info",
                authorization=authorization,
            )
            assert status == 401
            assert json.loads(body) == {
                "detail": {"error": "密钥无效或已失效，请重新登录"}
            }
        storage_info_status, storage_info_body = _request(
            rust_address,
            "/api/storage/info",
            authorization="Bearer client-key",
        )
        assert storage_info_status == 200
        assert json.loads(storage_info_body) == {
            "backend": {"type": "json"},
            "health": {"status": "healthy"},
        }
        assert _request(rust_address, "/v1/models")[0] == 401
        expected_ids = {
            "anon-model",
            "free-model",
            "plus-model",
        }
        deadline = time.monotonic() + 10
        payload: dict[str, object] = {}
        while time.monotonic() < deadline:
            status, body = _request(
                rust_address,
                "/v1/models",
                authorization="Bearer client-key",
            )
            assert status == 200
            payload = json.loads(body)
            model_ids = {item["id"] for item in payload["data"]}
            if expected_ids <= model_ids:
                break
            time.sleep(0.05)
        assert expected_ids <= {item["id"] for item in payload["data"]}

        calls = upstream_state.snapshot()
        authenticated_model_calls = [
            (path, authorization)
            for path, authorization in calls
            if path == "/backend-api/models?history_and_training_disabled=false"
        ]
        assert ("/backend-api/models?history_and_training_disabled=false", "Bearer free-a") in authenticated_model_calls
        assert ("/backend-api/models?history_and_training_disabled=false", "Bearer free-b") in authenticated_model_calls
        assert ("/backend-api/models?history_and_training_disabled=false", "Bearer plus-a") in authenticated_model_calls
        assert authenticated_model_calls.count(
            ("/backend-api/models?history_and_training_disabled=false", "Bearer free-a")
        ) == 1
        assert authenticated_model_calls.count(
            ("/backend-api/models?history_and_training_disabled=false", "Bearer plus-a")
        ) == 1
        assert authenticated_model_calls.count(
            ("/backend-api/models?history_and_training_disabled=false", "Bearer free-b")
        ) == 1
        assert sum(
            path == "/backend-anon/models?iim=false&is_gizmo=false" and not authorization
            for path, authorization in calls
        ) == 1

        accounts_path.write_text("{broken", encoding="utf-8")
        corrupt_bytes = accounts_path.read_bytes()
        degraded_status, degraded_body = _request(rust_address, "/health?format=json")
        assert degraded_status == 200
        degraded_payload = json.loads(degraded_body)
        assert degraded_payload == {
            "status": "degraded",
            "healthy": False,
            "version": "blackbox-test",
            "storage": {
                "backend": "json",
                "health": {
                    "status": "unhealthy",
                    "error": "存储后端健康检查失败",
                },
            },
            "proxy_runtime": {"enabled": False, "clearance_enabled": False},
            "accounts": {
                "total": 0,
                "cumulative_total": 0,
                "active": 0,
                "limited": 0,
                "abnormal": 0,
                "disabled": 0,
                "total_quota": 0,
                "total_success": 0,
                "total_fail": 0,
                "by_type": {},
            },
        }
        assert accounts_path.read_bytes() == corrupt_bytes
    finally:
        upstream.shutdown()
        upstream.server_close()
        upstream_thread.join(timeout=2)
        if process is not None:
            process.send_signal(signal.SIGTERM)
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=3)
        shutil.rmtree(root, ignore_errors=True)


class _NativeConversationState:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.calls: list[dict[str, object]] = []
        self.fail_next_codex = False

    def record(self, path: str, authorization: str, body: bytes) -> bool:
        with self.lock:
            self.calls.append(
                {
                    "path": path,
                    "authorization": authorization,
                    "body": json.loads(body) if body else None,
                }
            )
            if path == "/backend-api/codex/responses" and self.fail_next_codex:
                self.fail_next_codex = False
                return True
            return False

    def snapshot(self) -> list[dict[str, object]]:
        with self.lock:
            return list(self.calls)

    def fail_next_response(self) -> None:
        with self.lock:
            self.fail_next_codex = True


class _NativeConversationHandler(BaseHTTPRequestHandler):
    state: _NativeConversationState

    def log_message(self, format: str, *args: object) -> None:
        del format, args

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        authorization = self.headers.get("Authorization", "")
        if self.path == "/":
            self._send(200, b"<html></html>", "text/html")
            return
        if self.path == "/backend-api/models?history_and_training_disabled=false":
            token = authorization.removeprefix("Bearer ").strip()
            model = {
                "web-token": "web-model",
                "codex-token": "codex-model",
            }.get(token)
            if model is None:
                self._send(401, b"unauthorized", "text/plain")
            else:
                self._send(
                    200,
                    json.dumps({"models": [{"slug": model, "title": model}]}).encode(),
                    "application/json",
                )
            return
        if self.path == "/backend-anon/models?iim=false&is_gizmo=false":
            self._send(200, b'{"models":[]}', "application/json")
            return
        self._send(404, b"not found", "text/plain")

    def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        authorization = self.headers.get("Authorization", "")
        failed = self.state.record(self.path, authorization, body)
        if self.path.endswith("/sentinel/chat-requirements/prepare"):
            self._send(200, b'{"prepare_token":"prepare-token"}', "application/json")
            return
        if self.path.endswith("/sentinel/chat-requirements/finalize"):
            self._send(200, b'{"token":"requirements-token"}', "application/json")
            return
        if self.path == "/backend-api/codex/responses":
            if failed:
                self._send(502, b"upstream failed", "text/plain")
            else:
                self._send(
                    200,
                    (
                        "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp-blackbox\",\"model\":\"codex-model\",\"status\":\"in_progress\"},\"sequence_number\":0}\n\n"
                        "data: {\"type\":\"response.output_text.delta\",\"content_index\":0,\"item_id\":\"msg-blackbox\",\"delta\":\"native response\",\"logprobs\":[],\"output_index\":0,\"sequence_number\":1}\n\n"
                        "data: [DONE]\n\n"
                        "data: {\"type\":\"response.completed\",\"sequence_number\":2,\"response\":{\"id\":\"resp-blackbox\",\"model\":\"codex-model\",\"status\":\"completed\",\"output\":[{\"type\":\"message\",\"id\":\"msg-blackbox\",\"role\":\"assistant\",\"status\":\"completed\",\"content\":[{\"type\":\"output_text\",\"text\":\"native response\",\"annotations\":[]}]}]}}\n\n"
                    ).encode(),
                    "text/event-stream",
                )
            return
        if self.path == "/backend-api/conversation":
            self._send(
                200,
                (
                    "data: {\"message\":{\"author\":{\"role\":\"assistant\"},\"content\":{\"parts\":[\"native chat\"]}}}\n\n"
                    "data: [DONE]\n\n"
                ).encode(),
                "text/event-stream",
            )
            return
        self._send(404, b"not found", "text/plain")

    def _send(self, status: int, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def _post_json(
    address: tuple[str, int],
    path: str,
    payload: dict[str, object],
    authorization: str,
) -> tuple[int, bytes]:
    connection = http.client.HTTPConnection(*address, timeout=10)
    try:
        body = json.dumps(payload).encode()
        connection.request(
            "POST",
            path,
            body=body,
            headers={
                "Authorization": authorization,
                "Content-Type": "application/json",
                "Content-Length": str(len(body)),
            },
        )
        response = connection.getresponse()
        return response.status, response.read()
    finally:
        connection.close()


def test_clean_rust_binary_native_chat_and_responses_black_box() -> None:
    run_id = f"{os.getpid()}-{uuid4().hex}"
    root = CODEX_LOCAL / "tmp" / "python" / f"rust-binary-native-routes-{run_id}"
    data_dir = root / "data"
    build_dir = CODEX_LOCAL / "target" / "rust-binary-blackbox"
    log_path = root / "rust.log"
    root.mkdir(parents=True, exist_ok=False)
    data_dir.mkdir()
    (data_dir / "accounts.json").write_text(
        json.dumps(
            {
                "items": [
                    {
                        "access_token": "web-token",
                        "status": "正常",
                        "type": "free",
                        "source_type": "web",
                        "models": ["web-model"],
                    },
                    {
                        "access_token": "codex-token",
                        "status": "正常",
                        "type": "plus",
                        "source_type": "codex",
                        "models": ["codex-model"],
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    upstream_state = _NativeConversationState()
    _NativeConversationHandler.state = upstream_state
    upstream = ThreadingHTTPServer(("127.0.0.1", 0), _NativeConversationHandler)
    upstream_thread = threading.Thread(target=upstream.serve_forever, daemon=True)
    upstream_thread.start()
    rust_address = ("127.0.0.1", _free_local_port())
    command_tmp = root / "command-tmp"
    command_tmp.mkdir()
    command_env = os.environ.copy()
    command_env.update(
        {
            "TEMP": str(command_tmp),
            "TMP": str(command_tmp),
            "TMPDIR": str(command_tmp),
            "CARGO_TARGET_DIR": str(build_dir),
            "CARGO_HOME": str(CODEX_LOCAL / "cargo-home"),
        }
    )
    binary_name = "chatgpt2api-rust.exe" if os.name == "nt" else "chatgpt2api-rust"
    binary = build_dir / "release" / binary_name
    process: subprocess.Popen[bytes] | None = None
    try:
        subprocess.run(
            [
                "cargo",
                "build",
                "--release",
                "--bin",
                "chatgpt2api-rust",
                "--manifest-path",
                str(ROOT / "rust" / "Cargo.toml"),
            ],
            cwd=ROOT,
            env=command_env,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        assert binary.is_file(), binary
        upstream_address = upstream.server_address
        process_env = command_env | {
            "RUST_BIND": f"{rust_address[0]}:{rust_address[1]}",
            "RUST_AUTH_KEY": "client-key",
            "RUST_DATA_DIR": str(data_dir),
            "RUST_ACCOUNTS_PATH": str(data_dir / "accounts.json"),
            "RUST_MODELS": "auto",
            "RUST_UPSTREAM_BASE_URL": f"http://{upstream_address[0]}:{upstream_address[1]}",
            "RUST_UPSTREAM_PROTOCOL": "chatgpt",
            "CODEX_CLIENT_VERSION": "0.147.0",
            "RUST_VERSION": "blackbox-test",
        }
        with log_path.open("wb") as log:
            process = subprocess.Popen(
                [str(binary)],
                cwd=ROOT,
                env=process_env,
                stdout=log,
                stderr=subprocess.STDOUT,
            )
        _wait_for_port(rust_address)

        expected_ids = {"web-model", "codex-model"}
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            status, body = _request(
                rust_address,
                "/v1/models",
                authorization="Bearer client-key",
            )
            assert status == 200
            model_ids = {item["id"] for item in json.loads(body)["data"]}
            if expected_ids <= model_ids:
                break
            time.sleep(0.05)
        assert expected_ids <= model_ids

        status, body = _post_json(
            rust_address,
            "/v1/chat/completions",
            {
                "model": "web-model",
                "messages": [{"role": "user", "content": "hello"}],
            },
            "Bearer client-key",
        )
        assert status == 200
        assert json.loads(body)["choices"][0]["message"]["content"] == "native chat"

        status, body = _post_json(
            rust_address,
            "/v1/chat/completions",
            {
                "model": "web-model",
                "messages": [{"role": "user", "content": "stream"}],
                "stream": True,
            },
            "Bearer client-key",
        )
        assert status == 200
        assert b"native chat" in body and b"[DONE]" in body

        status, body = _post_json(
            rust_address,
            "/v1/responses",
            {"model": "codex-model", "input": "hello"},
            "Bearer client-key",
        )
        assert status == 200, (
            status,
            body.decode(errors="replace"),
            [
                (call["path"], call["authorization"])
                for call in upstream_state.snapshot()
            ],
        )
        assert json.loads(body)["output"][0]["content"][0]["text"] == "native response"

        status, body = _post_json(
            rust_address,
            "/v1/responses",
            {
                "model": "codex-model",
                "stream": True,
                "input": [
                    {
                        "role": "user",
                        "content": [
                            {"type": "input_text", "text": "search this"},
                            {
                                "type": "input_image",
                                "image_url": "data:image/png;base64,AQI=",
                            },
                        ],
                    }
                ],
                "tools": [
                    {
                        "type": "function",
                        "name": "lookup",
                        "description": "lookup",
                        "parameters": {"type": "object", "properties": {}},
                    },
                    {"type": "web_search_preview", "search_context_size": "high"},
                ],
                "tool_choice": "auto",
            },
            "Bearer client-key",
        )
        assert status == 200
        assert b"response.completed" in body

        upstream_state.fail_next_response()
        status, _ = _post_json(
            rust_address,
            "/v1/responses",
            {"model": "codex-model", "input": "upstream failure"},
            "Bearer client-key",
        )
        assert status in {502, 503}

        status, body = _post_json(
            rust_address,
            "/v1/responses",
            {"model": "codex-model", "input": "after failure"},
            "Bearer client-key",
        )
        assert status == 200
        assert json.loads(body)["id"] == "resp-blackbox"

        calls = upstream_state.snapshot()
        conversation = [call for call in calls if call["path"] == "/backend-api/conversation"]
        codex = [call for call in calls if call["path"] == "/backend-api/codex/responses"]
        assert len(conversation) == 2
        assert all(call["authorization"] == "Bearer web-token" for call in conversation)
        assert len(codex) == 4
        assert all(call["authorization"] == "Bearer codex-token" for call in codex)
        rich_payload = codex[1]["body"]
        assert isinstance(rich_payload, dict)
        assert rich_payload["stream"] is True
        assert rich_payload["input"][0]["content"][1]["type"] == "input_image"
        assert {tool["type"] for tool in rich_payload["tools"]} == {"function", "web_search"}
    finally:
        upstream.shutdown()
        upstream.server_close()
        upstream_thread.join(timeout=2)
        if process is not None:
            process.send_signal(signal.SIGTERM)
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=3)
        shutil.rmtree(root, ignore_errors=True)


class _NativeWebSocketState:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.connection_count = 0
        self.payloads: list[dict[str, object]] = []
        self.authorizations: list[str] = []
        self.http_paths: list[str] = []
        self.port: int | None = None
        self.closed_after_first_response = threading.Event()

    def opened(self, authorization: str) -> int:
        with self.lock:
            self.connection_count += 1
            self.authorizations.append(authorization)
            return self.connection_count

    def recorded(self, payload: dict[str, object]) -> None:
        with self.lock:
            self.payloads.append(payload)

    def http_recorded(self, path: str) -> None:
        with self.lock:
            self.http_paths.append(path)

    def snapshot(self) -> tuple[int, list[dict[str, object]], list[str], list[str]]:
        with self.lock:
            return (
                self.connection_count,
                list(self.payloads),
                list(self.authorizations),
                list(self.http_paths),
            )

async def _serve_native_websocket_upstream(
    state: _NativeWebSocketState,
    ready: threading.Event,
    stop: asyncio.Event,
) -> None:
    from websockets.asyncio.server import serve
    from websockets.datastructures import Headers
    from websockets.http11 import Response

    def http_response(status: int, body: bytes, content_type: str) -> Response:
        return Response(
            status,
            "OK" if status == 200 else "Not Found",
            Headers(
                [
                    ("Content-Type", content_type),
                    ("Content-Length", str(len(body))),
                    # websockets' HTTP rejection path doesn't serve a second
                    # request on this connection; advertise that boundary so
                    # reqwest doesn't reuse a dead keep-alive socket.
                    ("Connection", "close"),
                ]
            ),
            body,
        )

    async def process_request(connection, request):
        state.http_recorded(request.path)
        parsed = urlsplit(request.path)
        if parsed.path == "/":
            return http_response(200, b"<html></html>", "text/html")
        if (
            parsed.path == "/backend-api/models"
            and parsed.query == "history_and_training_disabled=false"
        ):
            return http_response(
                200,
                b'{"models":[{"slug":"codex-model","title":"Codex"}]}',
                "application/json",
            )
        if (
            parsed.path == "/backend-anon/models"
            and parsed.query == "iim=false&is_gizmo=false"
        ):
            return http_response(200, b'{"models":[]}', "application/json")
        if parsed.path == "/backend-api/codex/responses":
            return None
        return http_response(404, b"not found", "text/plain")

    async def send_response(connection, response_id: str, *, warmup: bool) -> None:
        await connection.send(
            json.dumps(
                {
                    "type": "response.created",
                    "response": {
                        "id": response_id,
                        "model": "codex-model",
                        "status": "in_progress",
                    },
                    "sequence_number": 0,
                }
            )
        )
        output = []
        if not warmup:
            await connection.send(
                json.dumps(
                    {
                        "type": "response.output_text.delta",
                        "content_index": 0,
                        "item_id": f"msg-{response_id}",
                        "delta": response_id,
                        "logprobs": [],
                        "output_index": 0,
                        "sequence_number": 1,
                    }
                )
            )
            output = [
                {
                    "type": "message",
                    "id": f"msg-{response_id}",
                    "role": "assistant",
                    "status": "completed",
                    "content": [
                        {
                            "type": "output_text",
                            "text": response_id,
                            "annotations": [],
                        }
                    ],
                }
            ]
        await connection.send(
            json.dumps(
                {
                    "type": "response.completed",
                    "sequence_number": 2,
                    "response": {
                        "id": response_id,
                        "model": "codex-model",
                        "status": "completed",
                        "output": output,
                    },
                }
            )
        )

    async def handler(connection) -> None:
        authorization = connection.request.headers.get("Authorization", "")
        connection_index = state.opened(authorization)
        try:
            async for raw in connection:
                payload = json.loads(raw)
                state.recorded(payload)
                if payload.get("generate") is False:
                    await send_response(connection, "warmup", warmup=True)
                    continue
                response_id = "resp-1" if connection_index == 1 else "resp-2"
                await send_response(connection, response_id, warmup=False)
                if connection_index == 1:
                    state.closed_after_first_response.set()
                    await connection.close()
        except Exception:
            return

    server = await serve(
        handler,
        "127.0.0.1",
        0,
        process_request=process_request,
        ping_interval=None,
    )
    state.port = server.sockets[0].getsockname()[1]
    ready.set()
    await stop.wait()
    server.close()
    await server.wait_closed()


def test_clean_rust_binary_native_websocket_warmup_and_reconnect_black_box() -> None:
    run_id = f"{os.getpid()}-{uuid4().hex}"
    root = CODEX_LOCAL / "tmp" / "python" / f"rust-binary-native-ws-{run_id}"
    data_dir = root / "data"
    build_dir = CODEX_LOCAL / "target" / "rust-binary-blackbox"
    log_path = root / "rust.log"
    root.mkdir(parents=True, exist_ok=False)
    data_dir.mkdir()
    (data_dir / "accounts.json").write_text(
        json.dumps(
            {
                "items": [
                    {
                        "access_token": "codex-token",
                        "status": "正常",
                        "type": "free",
                        "source_type": "codex",
                        "models": ["codex-model"],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    upstream_state = _NativeWebSocketState()
    ready = threading.Event()
    loop = asyncio.new_event_loop()
    stop_holder: dict[str, asyncio.Event] = {}

    def run_upstream() -> None:
        asyncio.set_event_loop(loop)
        stop = asyncio.Event()
        stop_holder["event"] = stop
        loop.run_until_complete(_serve_native_websocket_upstream(upstream_state, ready, stop))
        loop.close()

    upstream_thread = threading.Thread(target=run_upstream, daemon=True)
    upstream_thread.start()
    assert ready.wait(5), "native WebSocket upstream did not start"
    assert upstream_state.port is not None

    rust_address = ("127.0.0.1", _free_local_port())
    command_tmp = root / "command-tmp"
    command_tmp.mkdir()
    command_env = os.environ.copy()
    command_env.update(
        {
            "TEMP": str(command_tmp),
            "TMP": str(command_tmp),
            "TMPDIR": str(command_tmp),
            "CARGO_TARGET_DIR": str(build_dir),
            "CARGO_HOME": str(CODEX_LOCAL / "cargo-home"),
        }
    )
    binary_name = "chatgpt2api-rust.exe" if os.name == "nt" else "chatgpt2api-rust"
    binary = build_dir / "release" / binary_name
    process: subprocess.Popen[bytes] | None = None
    try:
        subprocess.run(
            [
                "cargo",
                "build",
                "--release",
                "--bin",
                "chatgpt2api-rust",
                "--manifest-path",
                str(ROOT / "rust" / "Cargo.toml"),
            ],
            cwd=ROOT,
            env=command_env,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        process_env = command_env | {
            "RUST_BIND": f"{rust_address[0]}:{rust_address[1]}",
            "RUST_AUTH_KEY": "client-key",
            "RUST_DATA_DIR": str(data_dir),
            "RUST_ACCOUNTS_PATH": str(data_dir / "accounts.json"),
            "RUST_MODELS": "auto",
            "RUST_UPSTREAM_BASE_URL": f"http://127.0.0.1:{upstream_state.port}",
            "RUST_UPSTREAM_PROTOCOL": "chatgpt",
            "CODEX_CLIENT_VERSION": "0.147.0",
            "RUST_VERSION": "blackbox-test",
        }
        with log_path.open("wb") as log:
            process = subprocess.Popen(
                [str(binary)],
                cwd=ROOT,
                env=process_env,
                stdout=log,
                stderr=subprocess.STDOUT,
            )
        _wait_for_port(rust_address)

        deadline = time.monotonic() + 10
        model_ids: set[str] = set()
        while time.monotonic() < deadline:
            status, body = _request(
                rust_address,
                "/v1/models",
                authorization="Bearer client-key",
            )
            assert status == 200
            model_ids = {item["id"] for item in json.loads(body)["data"]}
            if "codex-model" in model_ids:
                break
            time.sleep(0.05)
        if "codex-model" not in model_ids:
            _connections, _payloads, _authorizations, http_paths = upstream_state.snapshot()
            rust_log = log_path.read_text(encoding="utf-8", errors="replace")[-8192:]
            rust_log = rust_log.replace("codex-token", "<redacted>").replace(
                "client-key",
                "<redacted>",
            )
            pytest.fail(
                "native model catalog did not publish codex-model: "
                f"upstream_paths={http_paths!r}, "
                f"rust_exit={process.poll()}, rust_log_tail={rust_log!r}"
            )

        from websockets.exceptions import InvalidStatus
        from websockets.sync.client import connect

        try:
            with connect(
                f"ws://{rust_address[0]}:{rust_address[1]}/v1/responses",
                proxy=None,
                open_timeout=5,
            ):
                raise AssertionError("unauthenticated websocket unexpectedly connected")
        except InvalidStatus as error:
            assert error.response.status_code == 401

        with connect(
            f"ws://{rust_address[0]}:{rust_address[1]}/v1/responses",
            additional_headers={"Authorization": "Bearer client-key"},
            proxy=None,
            open_timeout=10,
        ) as websocket:
            websocket.send(
                json.dumps(
                    {
                        "type": "response.create",
                        "model": "codex-model",
                        "input": [],
                        "tools": [],
                        "generate": False,
                    }
                )
            )
            warmup = json.loads(websocket.recv(timeout=10))
            assert warmup["type"] == "response.created"
            assert json.loads(websocket.recv(timeout=10))["type"] == "response.completed"

            websocket.send(
                json.dumps(
                    {
                        "type": "response.create",
                        "model": "codex-model",
                        "previous_response_id": "warmup",
                        "input": "first",
                        "tools": [],
                    }
                )
            )
            first_created = json.loads(websocket.recv(timeout=10))
            assert first_created["type"] == "response.created"
            assert json.loads(websocket.recv(timeout=10))["type"] == "response.output_text.delta"
            first_completed = json.loads(websocket.recv(timeout=10))
            assert first_completed["type"] == "response.completed"
            assert first_completed["response"]["id"] == "resp-1"
            assert upstream_state.closed_after_first_response.wait(5)

            websocket.send(
                json.dumps(
                    {
                        "type": "response.create",
                        "model": "codex-model",
                        "previous_response_id": "resp-1",
                        "input": "second",
                        "tools": [],
                    }
                )
            )
            second_created = json.loads(websocket.recv(timeout=10))
            assert second_created["type"] == "response.created"
            assert json.loads(websocket.recv(timeout=10))["type"] == "response.output_text.delta"
            second_completed = json.loads(websocket.recv(timeout=10))
            assert second_completed["type"] == "response.completed"
            assert second_completed["response"]["id"] == "resp-2"

        connection_count, payloads, authorizations, _ = upstream_state.snapshot()
        assert connection_count >= 2
        assert authorizations and all(value == "Bearer codex-token" for value in authorizations)
        assert payloads[0]["generate"] is False
        assert payloads[1]["previous_response_id"] == "warmup"
        assert len(payloads) >= 3
        replay = payloads[2]
        assert "previous_response_id" not in replay
        assert len(replay["input"]) >= 3
    finally:
        stop = stop_holder.get("event")
        if stop is not None:
            try:
                loop.call_soon_threadsafe(stop.set)
            except RuntimeError:
                pass
        upstream_thread.join(timeout=3)
        if process is not None:
            process.send_signal(signal.SIGTERM)
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=3)
        shutil.rmtree(root, ignore_errors=True)
