from __future__ import annotations

import asyncio
import os
import tempfile
from pathlib import Path

import pytest

from services.opened_file_response import OpenedFileResponse
from services.secure_file import OpenedFile


async def _call(response: OpenedFileResponse, *, method: str = "GET", headers=()):
    messages: list[dict[str, object]] = []

    async def send(message):
        messages.append(message)

    await response(
        {
            "type": "http",
            "method": method,
            "headers": list(headers),
        },
        None,
        send,
    )
    return messages


def _body(messages: list[dict[str, object]]) -> bytes:
    return b"".join(message.get("body", b"") for message in messages[1:])


class _EarlyEOFFile:
    def __init__(self, file, first_chunk: bytes = b"abc") -> None:
        self._file = file
        self._first_chunk = first_chunk
        self._read_once = False

    @property
    def closed(self) -> bool:
        return self._file.closed

    def fileno(self) -> int:
        return self._file.fileno()

    def read(self, size: int) -> bytes:
        if self._read_once:
            return b""
        self._read_once = True
        return self._first_chunk[:size]

    def seek(self, offset: int) -> int:
        return self._file.seek(offset)

    def close(self) -> None:
        self._file.close()


def _response_for_file(file, stat_result) -> OpenedFileResponse:
    return OpenedFileResponse(
        OpenedFile(file=file, filename="asset.bin", stat_result=stat_result),
        include_filename=False,
    )


def test_truncate_after_open_refreshes_content_length_from_same_handle() -> None:
    async def scenario() -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "asset.bin"
            path.write_bytes(b"0123456789")
            file = path.open("rb")
            opened = OpenedFile(file=file, filename=path.name, stat_result=os.fstat(file.fileno()))
            with path.open("r+b") as mutator:
                mutator.truncate(3)

            messages = await _call(OpenedFileResponse(opened, include_filename=False))
            response_headers = dict(messages[0]["headers"])
            assert response_headers[b"content-length"] == b"3"
            assert _body(messages) == b"012"
            assert file.closed

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("range_header", "expected_prefix"),
    [
        (None, b"abc"),
        ("bytes=0-9", b"abc"),
        ("bytes=0-4,6-9", b"abc"),
    ],
)
def test_early_eof_fails_before_a_successful_terminal_body(
    range_header: str | None,
    expected_prefix: bytes,
) -> None:
    async def scenario() -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "asset.bin"
            path.write_bytes(b"0123456789")
            raw_file = path.open("rb")
            file = _EarlyEOFFile(raw_file, expected_prefix)
            response = _response_for_file(file, os.fstat(raw_file.fileno()))
            headers = [] if range_header is None else [(b"range", range_header.encode("ascii"))]

            with pytest.raises(RuntimeError, match="file changed while streaming"):
                await _call(response, headers=headers)

            assert file.closed

    asyncio.run(scenario())


def test_head_and_range_errors_close_the_open_handle() -> None:
    async def scenario() -> None:
        for headers, method, status in (
            ([], "HEAD", 200),
            ([(b"range", b"bytes=broken")], "GET", 400),
            ([(b"range", b"bytes=10-11")], "GET", 416),
        ):
            with tempfile.TemporaryDirectory() as temp_dir:
                path = Path(temp_dir) / "asset.bin"
                path.write_bytes(b"0123456789")
                file = path.open("rb")
                response = _response_for_file(file, os.fstat(file.fileno()))
                messages = await _call(response, method=method, headers=headers)
                assert messages[0]["status"] == status
                assert file.closed

    asyncio.run(scenario())


def test_send_failure_closes_the_open_handle() -> None:
    async def scenario() -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "asset.bin"
            path.write_bytes(b"0123456789")
            file = path.open("rb")
            response = _response_for_file(file, os.fstat(file.fileno()))

            async def failing_send(message) -> None:
                if message["type"] == "http.response.body":
                    raise RuntimeError("client disconnected")

            with pytest.raises(RuntimeError, match="client disconnected"):
                await response(
                    {"type": "http", "method": "GET", "headers": []},
                    None,
                    failing_send,
                )
            assert file.closed

    asyncio.run(scenario())
