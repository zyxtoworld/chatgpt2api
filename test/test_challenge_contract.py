from __future__ import annotations

import base64
import json
from unittest import mock

import pytest

import services.openai_backend_api as backend_module
import utils.turnstile as turnstile_module
from utils.pow import parse_pow_resources
from services.openai_backend_api import OpenAIBackendAPI


def _backend_without_network() -> OpenAIBackendAPI:
    backend = OpenAIBackendAPI.__new__(OpenAIBackendAPI)
    backend.user_agent = "test-user-agent"
    backend.pow_script_sources = []
    backend.pow_data_build = ""
    return backend


def test_turnstile_solver_decodes_and_executes_a_valid_challenge() -> None:
    source_p = "source-p"
    # The tiny program emits its string operand as the solved token.  It
    # exercises the real decode/dispatch path without any network dependency.
    program = [[3, source_p]]
    encoded = base64.b64encode(
        turnstile_module._xor_string(json.dumps(program), source_p).encode()
    ).decode()

    assert turnstile_module.solve_turnstile_token(encoded, source_p) == base64.b64encode(
        source_p.encode()
    ).decode()


def test_turnstile_solver_rejects_an_oversized_instruction_program() -> None:
    source_p = "source-p"
    program = [[3, "x"]] * 100_001
    encoded = base64.b64encode(
        turnstile_module._xor_string(json.dumps(program), source_p).encode()
    ).decode()

    assert turnstile_module.solve_turnstile_token(encoded, source_p) is None


def test_python_pow_resource_parser_accepts_single_quoted_data_build() -> None:
    scripts, data_build = parse_pow_resources("<html data-build='build-single-quote'></html>")
    assert scripts == ["https://chatgpt.com/backend-api/sentinel/sdk.js"]
    assert data_build == "build-single-quote"

def test_build_requirements_rejects_oversized_turnstile_dx_before_solving() -> None:
    backend = _backend_without_network()
    data = {
        "token": "requirements-token",
        "turnstile": {
            "required": True,
            "dx": "x" * (2 * 1024 * 1024 + 1),
        },
    }

    with mock.patch.object(backend_module, "solve_turnstile_token") as solve:
        with pytest.raises(RuntimeError, match="turnstile challenge is invalid"):
            backend._build_requirements(data)

    solve.assert_not_called()


def test_build_requirements_rejects_required_turnstile_without_solution() -> None:
    backend = _backend_without_network()
    data = {
        "token": "requirements-token",
        "turnstile": {"required": True, "dx": "valid-dx"},
    }

    with mock.patch.object(backend_module, "solve_turnstile_token", return_value=None) as solve:
        with pytest.raises(RuntimeError, match="turnstile challenge is invalid"):
            backend._build_requirements(data, source_p="source-token")

    solve.assert_called_once_with("valid-dx", "source-token")


def test_chat_requirements_does_not_finalize_after_turnstile_failure() -> None:
    class Response:
        status_code = 200
        ok = True

        def __init__(self, payload: dict) -> None:
            self._payload = payload
            self.closed = False

        def iter_content(self, *, chunk_size: int):
            assert chunk_size > 0
            import json

            yield json.dumps(self._payload).encode("utf-8")

        def close(self) -> None:
            self.closed = True

    prepare = Response({
        "prepare_token": "prepare-token",
        "turnstile": {"required": True, "dx": "valid-dx"},
    })
    session = mock.Mock()
    session.post.return_value = prepare

    backend = _backend_without_network()
    backend.access_token = ""
    backend.base_url = "https://chatgpt.example.test"
    backend.session = session
    backend._headers = mock.Mock(return_value={})
    backend._search_remaining = mock.Mock(return_value=30.0)

    with mock.patch.object(backend_module, "solve_turnstile_token", return_value=None):
        with pytest.raises(RuntimeError, match="turnstile challenge is invalid"):
            backend._get_chat_requirements()

    session.post.assert_called_once()
    prepare_close = prepare.closed
    assert prepare_close


def test_chat_requirements_rejects_malformed_proof_of_work_container() -> None:
    class Response:
        status_code = 200
        ok = True

        def __init__(self) -> None:
            self.closed = False

        def iter_content(self, *, chunk_size: int):
            assert chunk_size > 0
            yield b'{"prepare_token":"prepare-token","proofofwork":["proof-canary"]}'

        def close(self) -> None:
            self.closed = True

    response = Response()
    session = mock.Mock()
    session.post.return_value = response
    backend = _backend_without_network()
    backend.access_token = ""
    backend.base_url = "https://chatgpt.example.test"
    backend.session = session
    backend._headers = mock.Mock(return_value={})
    backend._search_remaining = mock.Mock(return_value=30.0)

    with pytest.raises(RuntimeError, match="chat requirements proof-of-work is invalid"):
        backend._get_chat_requirements()

    session.post.assert_called_once()
    assert response.closed


@pytest.mark.parametrize(
    "seed,difficulty",
    [("seed", ""), ("seed", "abc"), ("seed", "gg"), ("", "00"), ("s" * 257, "00")],
)
def test_chat_requirements_rejects_malformed_proof_of_work_fields(seed: str, difficulty: str) -> None:
    backend = _backend_without_network()
    with mock.patch.object(backend_module, "build_proof_token") as build:
        with pytest.raises(RuntimeError, match="chat requirements proof-of-work is invalid"):
            backend_module._build_proof_of_work_token(
                {"required": True, "seed": seed, "difficulty": difficulty},
                backend.user_agent,
            )

    build.assert_not_called()


def test_chat_requirements_rejects_non_object_prepare_response() -> None:
    class Response:
        status_code = 200
        ok = True

        def __init__(self) -> None:
            self.closed = False

        def iter_content(self, *, chunk_size: int):
            assert chunk_size > 0
            yield b'["prepare-canary"]'

        def close(self) -> None:
            self.closed = True

    response = Response()
    session = mock.Mock()
    session.post.return_value = response
    backend = _backend_without_network()
    backend.access_token = ""
    backend.base_url = "https://chatgpt.example.test"
    backend.session = session
    backend._headers = mock.Mock(return_value={})
    backend._search_remaining = mock.Mock(return_value=30.0)

    with pytest.raises(RuntimeError, match="chat requirements response is invalid"):
        backend._get_chat_requirements()

    session.post.assert_called_once()
    assert response.closed


def test_chat_requirements_rejects_malformed_arkose_container() -> None:
    class Response:
        status_code = 200
        ok = True

        def __init__(self) -> None:
            self.closed = False

        def iter_content(self, *, chunk_size: int):
            assert chunk_size > 0
            yield b'{"prepare_token":"prepare-token","arkose":["arkose-canary"]}'

        def close(self) -> None:
            self.closed = True

    response = Response()
    session = mock.Mock()
    session.post.return_value = response
    backend = _backend_without_network()
    backend.access_token = ""
    backend.base_url = "https://chatgpt.example.test"
    backend.session = session
    backend._headers = mock.Mock(return_value={})
    backend._search_remaining = mock.Mock(return_value=30.0)

    with pytest.raises(RuntimeError, match="chat requirements arkose is invalid"):
        backend._get_chat_requirements()

    session.post.assert_called_once()
    assert response.closed


def test_chat_requirements_rejects_non_object_finalize_response() -> None:
    class Response:
        status_code = 200
        ok = True

        def __init__(self, payload: bytes) -> None:
            self.payload = payload
            self.closed = False

        def iter_content(self, *, chunk_size: int):
            assert chunk_size > 0
            yield self.payload

        def close(self) -> None:
            self.closed = True

    prepare = Response(b'{"prepare_token":"prepare-token"}')
    finalize = Response(b'["finalize-canary"]')
    session = mock.Mock()
    session.post.side_effect = [prepare, finalize]
    backend = _backend_without_network()
    backend.access_token = ""
    backend.base_url = "https://chatgpt.example.test"
    backend.session = session
    backend._headers = mock.Mock(return_value={})
    backend._search_remaining = mock.Mock(return_value=30.0)

    with pytest.raises(RuntimeError, match="chat requirements response is invalid"):
        backend._get_chat_requirements()

    assert session.post.call_count == 2
    assert prepare.closed
    assert finalize.closed


def test_chat_requirements_rejects_arkose_required_in_finalize_response() -> None:
    class Response:
        status_code = 200
        ok = True

        def __init__(self, payload: bytes) -> None:
            self.payload = payload
            self.closed = False

        def iter_content(self, *, chunk_size: int):
            assert chunk_size > 0
            yield self.payload

        def close(self) -> None:
            self.closed = True

    prepare = Response(b'{"prepare_token":"prepare-token"}')
    finalize = Response(b'{"token":"chat-token","arkose":{"required":true}}')
    session = mock.Mock()
    session.post.side_effect = [prepare, finalize]
    backend = _backend_without_network()
    backend.access_token = ""
    backend.base_url = "https://chatgpt.example.test"
    backend.session = session
    backend._headers = mock.Mock(return_value={})
    backend._search_remaining = mock.Mock(return_value=30.0)

    with pytest.raises(RuntimeError, match="chat requirements requires arkose token"):
        backend._get_chat_requirements()

    assert session.post.call_count == 2
    assert prepare.closed
    assert finalize.closed


def test_chat_requirements_rejects_non_string_prepare_token_before_finalize() -> None:
    class Response:
        status_code = 200
        ok = True

        def __init__(self, payload: bytes) -> None:
            self.payload = payload
            self.closed = False

        def iter_content(self, *, chunk_size: int):
            assert chunk_size > 0
            yield self.payload

        def close(self) -> None:
            self.closed = True

    prepare = Response(b'{"prepare_token":{"secret":"prepare-canary"}}')
    session = mock.Mock()
    session.post.return_value = prepare
    backend = _backend_without_network()
    backend.access_token = ""
    backend.base_url = "https://chatgpt.example.test"
    backend.session = session
    backend._headers = mock.Mock(return_value={})
    backend._search_remaining = mock.Mock(return_value=30.0)

    with pytest.raises(RuntimeError):
        backend._get_chat_requirements()

    session.post.assert_called_once()
    assert prepare.closed
