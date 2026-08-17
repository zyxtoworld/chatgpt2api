from __future__ import annotations

import pytest

from services.remote_response import parse_json_response


class _StreamResponse:
    ok = True
    status_code = 200

    def __init__(self, chunks):
        self.chunks = chunks
        self.closed = False
        self.json_called = False

    def iter_content(self, *, chunk_size):
        assert chunk_size > 0
        yield from self.chunks

    def json(self):
        self.json_called = True
        raise AssertionError("bounded parser must consume the stream")

    def close(self):
        self.closed = True


class _JsonOnlyResponse:
    ok = True
    status_code = 200

    def __init__(self, *, headers=None):
        self.headers = headers or {}
        self.closed = False
        self.json_called = False

    def json(self):
        self.json_called = True
        raise RuntimeError("json body token canary")

    def close(self):
        self.closed = True


class _CountingResponse(_StreamResponse):
    def __init__(self, chunks, *, headers=None):
        super().__init__(chunks)
        self.headers = headers or {}
        self.iterated = False

    def iter_content(self, *, chunk_size):
        self.iterated = True
        yield from super().iter_content(chunk_size=chunk_size)


class _CloseRaisesResponse(_StreamResponse):
    def __init__(self, chunks):
        super().__init__(chunks)
        self.close_calls = 0

    def close(self):
        self.close_calls += 1
        raise OSError("close body canary")


def test_remote_json_parser_reads_stream_with_budget_and_closes_response():
    response = _StreamResponse([b'{"data":', b"[]}"])

    assert parse_json_response(response, "fixture", max_bytes=32) == {"data": []}
    assert response.closed
    assert not response.json_called


def test_remote_json_parser_rejects_stream_overflow_and_closes_response():
    response = _StreamResponse([b"1234", b"5"])

    with pytest.raises(RuntimeError, match="response body too large"):
        parse_json_response(response, "fixture", max_bytes=4)

    assert response.closed


def test_remote_json_parser_rejects_malformed_stream_and_closes_response():
    response = _StreamResponse([b"not-json"])

    with pytest.raises(RuntimeError, match="invalid JSON"):
        parse_json_response(response, "fixture", max_bytes=32)

    assert response.closed


def test_remote_json_parser_does_not_bypass_budget_through_json_fallback():
    response = _JsonOnlyResponse()

    with pytest.raises(RuntimeError, match="invalid response body") as error:
        parse_json_response(response, "fixture", max_bytes=4)

    assert "canary" not in str(error.value)
    assert response.closed
    assert not response.json_called


def test_remote_json_parser_rejects_content_length_before_reading_body():
    response = _CountingResponse([b"{}"], headers={"content-length": "5"})

    with pytest.raises(RuntimeError, match="response body too large"):
        parse_json_response(response, "fixture", max_bytes=4)

    assert not response.iterated
    assert response.closed


def test_remote_json_parser_rejects_nonpositive_budget_before_reading_body():
    response = _CountingResponse([b"{}"])

    with pytest.raises(RuntimeError, match="response body too large"):
        parse_json_response(response, "fixture", max_bytes=0)

    assert not response.iterated
    assert response.closed

    negative_response = _CountingResponse([b"{}"])
    with pytest.raises(RuntimeError, match="response body too large"):
        parse_json_response(negative_response, "fixture", max_bytes=-1)
    assert not negative_response.iterated
    assert negative_response.closed


def test_remote_json_parser_maps_iterator_failure_to_fixed_error_and_closes():
    class _IteratorFailureResponse(_StreamResponse):
        def iter_content(self, *, chunk_size):
            raise RuntimeError("upstream body token canary")

    response = _IteratorFailureResponse([])

    with pytest.raises(RuntimeError, match="invalid response body") as error:
        parse_json_response(response, "fixture", max_bytes=32)

    assert "canary" not in str(error.value)
    assert response.closed


def test_remote_json_parser_maps_none_iterator_and_none_chunk_to_fixed_error():
    class _NoneIteratorResponse(_StreamResponse):
        def iter_content(self, *, chunk_size):
            return None

    class _NoneChunkResponse(_StreamResponse):
        def iter_content(self, *, chunk_size):
            yield None

    for response in (_NoneIteratorResponse([]), _NoneChunkResponse([])):
        with pytest.raises(RuntimeError, match="invalid response body"):
            parse_json_response(response, "fixture", max_bytes=32)
        assert response.closed


def test_remote_json_parser_preserves_explicit_error_payload_opt_in():
    response = _StreamResponse([b'{"error":{"code":"expired"}}'])
    response.ok = False
    response.status_code = 401

    assert parse_json_response(response, "fixture", require_ok=False) == {
        "error": {"code": "expired"}
    }
    assert response.closed


def test_remote_json_parser_does_not_replace_primary_error_when_close_fails():
    response = _CloseRaisesResponse([b"not-json"])

    with pytest.raises(RuntimeError, match="invalid JSON"):
        parse_json_response(response, "fixture", max_bytes=32)

    assert response.close_calls == 1
