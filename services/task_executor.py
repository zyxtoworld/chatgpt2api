from __future__ import annotations

import threading
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Callable

from services.protocol.error_response import PublicSafeError


BACKGROUND_TASK_WORKERS = 16
BACKGROUND_TASK_CAPACITY = 32


class BackgroundTaskQueueFullError(PublicSafeError):
    pass


_EXECUTOR = ThreadPoolExecutor(
    max_workers=BACKGROUND_TASK_WORKERS,
    thread_name_prefix="background-task",
)
_CAPACITY = threading.BoundedSemaphore(BACKGROUND_TASK_CAPACITY)


def _release_capacity(_future: Future[Any]) -> None:
    _CAPACITY.release()


class BackgroundTaskReservation:
    def __init__(self) -> None:
        self._active = True

    def submit(
        self,
        function: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> Future[Any]:
        if not self._active:
            raise RuntimeError("background task reservation is no longer active")
        try:
            future = _EXECUTOR.submit(function, *args, **kwargs)
        except BaseException:
            self.cancel()
            raise
        self._active = False
        future.add_done_callback(_release_capacity)
        return future

    def cancel(self) -> None:
        if not self._active:
            return
        self._active = False
        _CAPACITY.release()

    def release(self) -> None:
        self.cancel()


def reserve_background_task() -> BackgroundTaskReservation:
    if not _CAPACITY.acquire(blocking=False):
        raise BackgroundTaskQueueFullError("background task queue is full; try again later")
    return BackgroundTaskReservation()
