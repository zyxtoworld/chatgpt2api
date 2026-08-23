from __future__ import annotations

import ast
import base64
import copy
import json
import os
import shutil
from urllib.parse import urlsplit
from pathlib import Path
from uuid import uuid4

from test.fixtures.image_inputs import image_fixture_bytes
from test.utils import (
    ROOT_DIR,
    decode_image_data_urls,
    decode_image_payload,
    iter_sse_data,
    require_stream_response,
)


LIVE_MODULES = (
    "test_codex_4k.py",
    "test_generations.py",
    "test_image.py",
    "test_gpt_ppt.py",
    "test_gpt_psd.py",
    "test_image_output_tokens.py",
)
FORBIDDEN_LIVE_CALLS = {
    "open",
    "mkdir",
    "save_image",
    "save_images_from_text",
    "write_bytes",
    "write_text",
}


class _UnbufferedSseResponse:
    status_code = 200
    headers = {"content-type": "text/event-stream; charset=utf-8"}

    def __init__(self) -> None:
        self.iterated = False
        self.closed = False

    @property
    def text(self):  # pragma: no cover - access is the regression
        raise AssertionError("successful SSE response was pre-consumed via .text")

    @property
    def content(self):  # pragma: no cover - access is the regression
        raise AssertionError("successful SSE response was pre-consumed via .content")

    def json(self):  # pragma: no cover - access is the regression
        raise AssertionError("successful SSE response was pre-consumed via .json()")

    def iter_lines(self):
        self.iterated = True
        yield b'data: {"type":"response.created"}'
        yield b'data: {"type":"response.completed"}'

    def close(self) -> None:
        self.closed = True


def _filesystem_snapshot(path: Path) -> tuple[object, ...]:
    if not path.exists():
        return ("missing",)
    if path.is_file():
        stat = path.stat()
        return ("file", stat.st_size, stat.st_mtime_ns)
    entries: list[tuple[str, str, int, int]] = []
    for child in sorted(path.rglob("*")):
        relative = child.relative_to(path).as_posix()
        if child.is_symlink():
            entries.append((relative, "link", 0, 0))
        elif child.is_file():
            stat = child.stat()
            entries.append((relative, "file", stat.st_size, stat.st_mtime_ns))
        else:
            entries.append((relative, "directory", 0, 0))
    return ("directory", *entries)


def test_stream_contract_exposes_first_event_before_terminal_without_preconsuming():
    response = _UnbufferedSseResponse()
    require_stream_response(response)
    assert not response.closed
    assert not response.iterated

    events = iter(iter_sse_data(response))
    first = next(events)
    assert first == '{"type":"response.created"}'
    assert response.iterated
    assert next(events) == '{"type":"response.completed"}'
    response.close()
    assert response.closed


def test_live_image_assertions_are_memory_only():
    one_pixel_png = (
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk"
        "YAAAAAYAAjCB0C8AAAAASUVORK5CYII="
    )
    data_url = f"data:image/png;base64,{one_pixel_png}"
    output_paths = (ROOT_DIR / "data" / "output", ROOT_DIR / "utils" / "output")
    before = {path: _filesystem_snapshot(path) for path in output_paths}

    assert decode_image_payload(one_pixel_png)
    assert decode_image_data_urls(data_url) == [base64.b64decode(one_pixel_png)]

    after = {path: _filesystem_snapshot(path) for path in output_paths}
    assert after == before


def _run_no_write_guard(workload, *, label: str) -> None:
    """Run a live workload in an isolated tree and always remove that tree."""
    run_root = ROOT_DIR / ".local" / "codex" / "tmp" / f"{label}-{uuid4().hex}"
    roots = [run_root / name for name in ("cwd", "userprofile", "temp", "tmp", "tmpdir")]
    for path in roots:
        path.mkdir(parents=True, exist_ok=False)
    watched = [
        *roots,
        ROOT_DIR / "data" / "output",
        ROOT_DIR / "utils" / "output",
        ROOT_DIR / "data" / "accounts.json",
        ROOT_DIR / "data" / "logs.jsonl",
    ]
    before = {path: _filesystem_snapshot(path) for path in watched}
    original_cwd = Path.cwd()
    workload_error = None
    snapshot_error = None
    cleanup_error = None
    try:
        workload(roots)
    except BaseException as exc:
        workload_error = exc
    finally:
        try:
            after = {path: _filesystem_snapshot(path) for path in watched}
            if after != before:
                snapshot_error = AssertionError(f"{label} created or modified an ambient output")
        except BaseException as exc:
            snapshot_error = exc
        try:
            os.chdir(original_cwd)
            shutil.rmtree(run_root, ignore_errors=False)
        except BaseException as exc:
            cleanup_error = exc
    if cleanup_error is not None:
        if workload_error is not None:
            cleanup_error.add_note(f"original live workload error: {workload_error!r}")
        if snapshot_error is not None:
            cleanup_error.add_note(f"ambient output error: {snapshot_error!r}")
        raise cleanup_error
    if workload_error is not None:
        if snapshot_error is not None:
            workload_error.add_note(f"ambient output error: {snapshot_error!r}")
        raise workload_error.with_traceback(workload_error.__traceback__)
    if snapshot_error is not None:
        raise snapshot_error


def test_no_write_guard_cleans_tree_when_workload_writes_file():
    error = None
    try:
        def bad_workload(roots):
            (roots[0] / "unexpected-output.bin").write_bytes(b"forbidden")

        _run_no_write_guard(bad_workload, label="intentional-write-negative")
    except AssertionError as exc:
        error = exc
    assert error is not None
    leftovers = list((ROOT_DIR / ".local" / "codex" / "tmp").glob("intentional-write-negative-*"))
    assert not leftovers


def test_legacy_live_modules_are_collectable_and_have_no_ambient_output_writes():
    for name in LIVE_MODULES:
        path = Path(__file__).with_name(name)
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(path))
        functions = [
            node
            for node in ast.walk(tree)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name.startswith("test_")
        ]
        assert functions, f"{name} has no pytest-collectable test function"
        assert "pytest.mark.live_upstream" in source, f"{name} is not explicitly live_upstream"
        assert "config.json" not in source
        assert "localhost:8000" not in source
        assert "127.0.0.1:8000" not in source
        assert "USERPROFILE" not in source
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if isinstance(node.func, ast.Name):
                call_name = node.func.id
            elif isinstance(node.func, ast.Attribute):
                call_name = node.func.attr
            else:
                call_name = ""
            assert call_name not in FORBIDDEN_LIVE_CALLS, f"{name} uses ambient output call {call_name}"
        assert not any(isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "print" for node in ast.walk(tree)), name


class _FakeResponse:
    def __init__(self, transport, *, payload=None, content_type="application/json", lines=(), stream=False):
        self._transport = transport
        self._payload = copy.deepcopy(payload)
        self._lines = tuple(lines)
        self._stream = stream
        self.status_code = 200
        self.headers = {"content-type": content_type}
        self.iterated = False
        self.closed = False

    @property
    def text(self):
        raise AssertionError("fake successful response was consumed through .text")

    @property
    def content(self):
        raise AssertionError("fake successful response was consumed through .content")

    def json(self):
        if self._stream:
            raise AssertionError("fake successful stream was pre-consumed through .json()")
        return copy.deepcopy(self._payload)

    def iter_lines(self, **_kwargs):
        self.iterated = True
        for line in self._lines:
            yield line if isinstance(line, bytes) else str(line).encode("utf-8")

    def iter_content(self, chunk_size=2048):
        del chunk_size
        body = json.dumps(self._payload or {}, ensure_ascii=False).encode("utf-8")
        yield body

    def close(self):
        self.closed = True


class _FakeLiveTransport:
    _PNG_B64 = base64.b64encode(image_fixture_bytes("image.png")).decode("ascii")

    def __init__(self):
        self.responses: list[_FakeResponse] = []

    @staticmethod
    def _lines(events, *, done=True):
        values = [f"data: {json.dumps(event, ensure_ascii=False)}" for event in events]
        if done:
            values.append("data: [DONE]")
        return values

    def _response(self, *, payload=None, content_type="application/json", events=(), stream=False):
        response = _FakeResponse(
            self,
            payload=payload,
            content_type=content_type,
            lines=self._lines(events),
            stream=stream,
        )
        self.responses.append(response)
        return response

    def _image_json(self, *, response_format="b64_json"):
        item = {"url": "/files/fake-image.png", "revised_prompt": "fake"}
        if response_format == "b64_json":
            item["b64_json"] = self._PNG_B64
        return {"object": "list", "created": 1, "data": [item], "usage": {"input_tokens": 1, "output_tokens": 1, "total_tokens": 2}}

    def _response_image_json(self):
        return {
            "object": "response",
            "status": "completed",
            "error": None,
            "output": [{"type": "image_generation_call", "result": self._PNG_B64}],
        }

    def post(self, url, **kwargs):
        path = urlsplit(url).path
        body = kwargs.get("json") or {}
        stream = bool(kwargs.get("stream"))
        if path == "/backend-api/codex/responses":
            events = [
                {"type": "response.output_item.done", "item": {"type": "image_generation_call", "result": self._PNG_B64}},
                {"type": "response.completed", "response": {"status": "completed", "error": None}},
            ]
            return self._response(content_type="text/event-stream", events=events, stream=stream)
        if path == "/v1/images/generations":
            response_format = str(body.get("response_format") or "b64_json")
            if stream:
                return self._response(
                    content_type="text/event-stream",
                    events=[{"type": "image_generation.completed", "b64_json": self._PNG_B64}],
                    stream=True,
                )
            return self._response(payload=self._image_json(response_format=response_format), stream=False)
        if path == "/v1/images/edits":
            if stream:
                return self._response(
                    content_type="text/event-stream",
                    events=[{"type": "image_edit.completed", "b64_json": self._PNG_B64}],
                    stream=True,
                )
            return self._response(payload=self._image_json(), stream=False)
        if path == "/v1/chat/completions":
            model = str(body.get("model") or "")
            if model.startswith("gpt-image"):
                if stream:
                    return self._response(
                        content_type="text/event-stream",
                        events=[
                            {"choices": [{"delta": {"content": f"data:image/png;base64,{self._PNG_B64}"}, "finish_reason": None}]},
                            {"choices": [{"delta": {}, "finish_reason": "stop"}]},
                        ],
                        stream=True,
                    )
                return self._response(payload={"object": "chat.completion", "choices": [{"message": {"content": f"data:image/png;base64,{self._PNG_B64}"}}]})
            if stream:
                return self._response(
                    content_type="text/event-stream",
                    events=[
                        {"choices": [{"delta": {"content": "fake text"}, "finish_reason": None}]},
                        {"choices": [{"delta": {}, "finish_reason": "stop"}]},
                    ],
                    stream=True,
                )
            return self._response(payload={"object": "chat.completion", "choices": [{"message": {"content": "fake text"}, "finish_reason": "stop"}]})
        if path == "/v1/responses":
            image = any(isinstance(item, dict) and item.get("type") == "image_generation" for item in body.get("tools") or [])
            if image:
                if stream:
                    return self._response(
                        content_type="text/event-stream",
                        events=[
                            {"type": "response.created"},
                            {"type": "response.output_item.done", "item": {"type": "image_generation_call", "result": self._PNG_B64}},
                            {"type": "response.completed"},
                        ],
                        stream=True,
                    )
                return self._response(payload=self._response_image_json())
            if stream:
                return self._response(
                    content_type="text/event-stream",
                    events=[{"type": "response.created"}, {"type": "response.output_text.delta"}, {"type": "response.completed"}],
                    stream=True,
                )
            return self._response(payload={"object": "response", "status": "completed", "error": None, "output": [{"type": "message", "content": [{"type": "output_text", "text": "fake response"}]}]})
        if path in {"/v1/ppt/generations", "/v1/psd/generations"}:
            return self._response(payload={"id": "fake-task", "status": "queued"})
        if path == "/v1/messages":
            if stream:
                return self._response(
                    content_type="text/event-stream",
                    events=[
                        {"type": "message_start", "message": {"type": "message"}},
                        {"type": "content_block_delta", "delta": {"text": "fake message"}},
                        {"type": "message_delta", "delta": {"stop_reason": "end_turn"}},
                        {"type": "message_stop"},
                    ],
                    stream=True,
                )
            return self._response(payload={"type": "message", "content": [{"type": "text", "text": "fake message"}], "stop_reason": "end_turn"})
        raise AssertionError(f"unhandled fake POST path: {path}")

    def get(self, url, **_kwargs):
        path = urlsplit(url).path
        if path == "/v1/models":
            return self._response(payload={"object": "list", "data": [{"id": "fake-model", "object": "model"}]})
        if path == "/v1/editable-file-tasks":
            return self._response(payload={"items": [{"id": "fake-task", "status": "success", "result": {"primary_url": "/files/primary", "zip_url": "/files/archive"}}], "missing_ids": []})
        raise AssertionError(f"unhandled fake GET path: {path}")

    def request(self, method, url, **kwargs):
        if str(method).upper() == "POST":
            return self.post(url, **kwargs)
        if str(method).upper() == "GET":
            return self.get(url, **kwargs)
        raise AssertionError(f"unhandled fake method: {method}")


def test_all_live_image_and_stream_nodes_run_without_ambient_files(monkeypatch):
    import requests

    from test import test_codex_4k, test_generations, test_gpt_ppt, test_gpt_psd, test_image, test_image_output_tokens
    from test import test_v1_chat_completions, test_v1_images_edits, test_v1_images_generations, test_v1_messages, test_v1_models, test_v1_responses

    transport = _FakeLiveTransport()
    monkeypatch.setattr(requests, "post", transport.post)
    monkeypatch.setattr(requests, "get", transport.get)
    monkeypatch.setattr(requests, "request", transport.request)
    monkeypatch.setenv("CHATGPT2API_LIVE_BASE_URL", "http://fake.local")
    monkeypatch.setenv("CHATGPT2API_LIVE_API_KEY", "synthetic-live-key")
    monkeypatch.setenv("CHATGPT2API_LIVE_CODEX_BASE_URL", "http://fake.local")

    for module in (
        test_v1_chat_completions,
        test_v1_images_edits,
        test_v1_images_generations,
        test_v1_messages,
        test_v1_models,
        test_v1_responses,
    ):
        monkeypatch.setattr(module, "BASE_URL", "http://fake.local")
        monkeypatch.setattr(module, "AUTH_KEY", "synthetic-live-key")

    def workload(roots):
        monkeypatch.chdir(roots[0])
        for env_name, path in zip(("USERPROFILE", "TEMP", "TMP", "TMPDIR"), roots[1:]):
            monkeypatch.setenv(env_name, str(path))
        test_codex_4k.test_codex_4k_image_stream_has_completed_terminal_and_decodable_result()
        test_generations.test_image_generation_non_stream_returns_decodable_images_in_memory()
        test_generations.test_image_generation_stream_has_terminal_and_decodable_images_in_memory()
        test_image.test_image_generation_legacy_model_returns_decodable_image_in_memory()
        test_image_output_tokens.test_image_generation_url_response_is_successful_and_nonempty()
        test_gpt_ppt.test_ppt_generation_has_success_terminal_and_download_urls()
        test_gpt_psd.test_psd_generation_has_success_terminal_and_download_urls()
        test_v1_chat_completions.ChatCompletionsTests().test_image_completion_http()
        test_v1_chat_completions.ChatCompletionsTests().test_image_completion_stream_http()
        test_v1_images_generations.ImageGenerationsTests().test_image_generation_http()
        test_v1_images_generations.ImageGenerationsTests().test_image_generation_stream_http()
        test_v1_images_edits.ImageEditsTests().test_image_edit_http()
        test_v1_images_edits.ImageEditsTests().test_image_edit_stream_http()
        test_v1_responses.ResponsesTests().test_image_response_http()
        test_v1_responses.ResponsesTests().test_image_response_stream_http()
        test_v1_responses.ResponsesTests().test_codex_image_response_http()
        test_v1_responses.ResponsesTests().test_codex_image_response_stream_http()
        test_v1_messages.AnthropicMessagesTests().test_message_stream_http()
    _run_no_write_guard(workload, label="live-no-write")
    assert all(response.iterated for response in transport.responses if response._stream)
