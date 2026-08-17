from __future__ import annotations

import io
import os
import threading
from secrets import token_hex
from typing import Mapping

import anyio
from fastapi.responses import FileResponse
from starlette.datastructures import MutableHeaders


_OPENED_FILE_IO_CAPACITY = 8
_OPENED_FILE_IO_STATE = threading.local()


def _opened_file_io_limiter() -> anyio.CapacityLimiter:
    limiter = getattr(_OPENED_FILE_IO_STATE, "limiter", None)
    if limiter is None:
        limiter = anyio.CapacityLimiter(_OPENED_FILE_IO_CAPACITY)
        _OPENED_FILE_IO_STATE.limiter = limiter
    return limiter


class OpenedFileResponse(FileResponse):
    def __init__(
        self,
        opened_file,
        *,
        headers: Mapping[str, str] | None = None,
        include_filename: bool = True,
    ) -> None:
        self._opened_file = opened_file.file
        super().__init__(
            opened_file.filename,
            filename=opened_file.filename if include_filename else None,
            headers=headers,
            stat_result=opened_file.stat_result,
        )

    def _close_opened_file(self) -> None:
        if not self._opened_file.closed:
            self._opened_file.close()

    def _refresh_opened_file_size(self) -> None:
        """Use the already-open handle as the response's size authority."""
        try:
            file_descriptor = self._opened_file.fileno()
        except (AttributeError, io.UnsupportedOperation):
            return
        current_stat = os.fstat(file_descriptor)
        if self.stat_result is None or current_stat.st_size == self.stat_result.st_size:
            return
        self.stat_result = current_stat
        for header_name in ("content-length", "last-modified", "etag"):
            try:
                del self.headers[header_name]
            except KeyError:
                pass
        self.set_stat_headers(current_stat)

    async def __call__(self, scope, receive, send):
        try:
            self._refresh_opened_file_size()
            return await super().__call__(scope, receive, send)
        finally:
            self._close_opened_file()

    async def _read_opened_file(self, size: int) -> bytes:
        return await anyio.to_thread.run_sync(
            self._opened_file.read,
            size,
            limiter=_opened_file_io_limiter(),
        )

    async def _seek_opened_file(self, offset: int) -> None:
        await anyio.to_thread.run_sync(
            self._opened_file.seek,
            offset,
            limiter=_opened_file_io_limiter(),
        )

    async def _handle_simple(self, send, send_header_only: bool, _send_pathsend: bool) -> None:
        await send({"type": "http.response.start", "status": self.status_code, "headers": self.raw_headers})
        if send_header_only:
            await send({"type": "http.response.body", "body": b"", "more_body": False})
            return
        remaining = self.stat_result.st_size
        if remaining == 0:
            await send({"type": "http.response.body", "body": b"", "more_body": False})
            return
        while remaining:
            chunk = await self._read_opened_file(min(self.chunk_size, remaining))
            if not chunk:
                raise RuntimeError("file changed while streaming")
            remaining -= len(chunk)
            await send({"type": "http.response.body", "body": chunk, "more_body": bool(remaining)})

    async def _handle_single_range(
        self,
        send,
        start: int,
        end: int,
        file_size: int,
        send_header_only: bool,
    ) -> None:
        headers = MutableHeaders(raw=list(self.raw_headers))
        headers["content-range"] = f"bytes {start}-{end - 1}/{file_size}"
        headers["content-length"] = str(end - start)
        await send({"type": "http.response.start", "status": 206, "headers": headers.raw})
        if send_header_only:
            await send({"type": "http.response.body", "body": b"", "more_body": False})
            return
        await self._seek_opened_file(start)
        remaining = end - start
        while remaining:
            chunk = await self._read_opened_file(min(self.chunk_size, remaining))
            if not chunk:
                raise RuntimeError("file changed while streaming")
            remaining -= len(chunk)
            await send({"type": "http.response.body", "body": chunk, "more_body": bool(remaining)})

    async def _handle_multiple_ranges(
        self,
        send,
        ranges: list[tuple[int, int]],
        file_size: int,
        send_header_only: bool,
    ) -> None:
        boundary = token_hex(13)
        content_length, header_generator = self.generate_multipart(
            ranges,
            boundary,
            file_size,
            self.headers["content-type"],
        )
        headers = MutableHeaders(raw=list(self.raw_headers))
        headers["content-type"] = f"multipart/byteranges; boundary={boundary}"
        headers["content-length"] = str(content_length)
        await send({"type": "http.response.start", "status": 206, "headers": headers.raw})
        if send_header_only:
            await send({"type": "http.response.body", "body": b"", "more_body": False})
            return
        for start, end in ranges:
            await send({"type": "http.response.body", "body": header_generator(start, end), "more_body": True})
            await self._seek_opened_file(start)
            remaining = end - start
            while remaining:
                chunk = await self._read_opened_file(min(self.chunk_size, remaining))
                if not chunk:
                    raise RuntimeError("file changed while streaming")
                remaining -= len(chunk)
                await send({"type": "http.response.body", "body": chunk, "more_body": True})
            await send({"type": "http.response.body", "body": b"\r\n", "more_body": True})
        await send({"type": "http.response.body", "body": f"--{boundary}--".encode("latin-1"), "more_body": False})
