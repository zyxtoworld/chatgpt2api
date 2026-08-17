from __future__ import annotations

import math
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
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
# Timed-out calls may continue until their own I/O deadline because Python
# cannot forcibly stop a running thread. Reuse a bounded pool so repeated
# caller deadlines cannot create one permanently live worker per timeout.
_TIMEOUT_EXECUTOR = ThreadPoolExecutor(
    max_workers=BACKGROUND_TASK_WORKERS,
    thread_name_prefix="bounded-call",
)
_TIMEOUT_CAPACITY = threading.BoundedSemaphore(BACKGROUND_TASK_WORKERS)
_CAPACITY = threading.BoundedSemaphore(BACKGROUND_TASK_CAPACITY)
_ACTIVE_FUTURES: set[Future[Any]] = set()
_ACTIVE_FUTURES_LOCK = threading.Lock()
_ACTIVE_TIMEOUT_FUTURES: set[Future[Any]] = set()
_ACTIVE_TIMEOUT_FUTURES_LOCK = threading.Lock()


def _release_capacity(_future: Future[Any]) -> None:
    with _ACTIVE_FUTURES_LOCK:
        _ACTIVE_FUTURES.discard(_future)
    _CAPACITY.release()


def _discard_timeout_future(future: Future[Any]) -> None:
    with _ACTIVE_TIMEOUT_FUTURES_LOCK:
        _ACTIVE_TIMEOUT_FUTURES.discard(future)


class BackgroundTaskReservation:
    def __init__(self) -> None:
        self._state_lock = threading.RLock()
        self._active = True

    def submit(
        self,
        function: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> Future[Any]:
        with self._state_lock:
            if not self._active:
                raise RuntimeError("background task reservation is no longer active")
            try:
                # Shutdown draining snapshots this set. Keep executor
                # acceptance and registration atomic so an accepted task
                # cannot fall between the snapshot and the registry. The
                # reservation state lock also makes cancel wait for this
                # linearization point instead of releasing capacity early.
                with _ACTIVE_FUTURES_LOCK:
                    future = _EXECUTOR.submit(function, *args, **kwargs)
                    self._active = False
                    _ACTIVE_FUTURES.add(future)
            except BaseException:
                self.cancel()
                raise
        future.add_done_callback(_release_capacity)
        return future

    def cancel(self) -> None:
        with self._state_lock:
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


def wait_for_background_tasks() -> None:
    """Drain accepted tasks without cancelling server-side writes."""
    while True:
        with _ACTIVE_FUTURES_LOCK:
            futures = tuple(_ACTIVE_FUTURES)
        with _ACTIVE_TIMEOUT_FUTURES_LOCK:
            timeout_futures = tuple(_ACTIVE_TIMEOUT_FUTURES)
        if not futures and not timeout_futures:
            return
        for future in (*futures, *timeout_futures):
            try:
                future.result()
            except BaseException:
                # The task's caller owns its error reporting; shutdown still
                # must wait for its side effects to finish.
                pass


def run_with_timeout(
    function: Callable[..., Any],
    *args: Any,
    timeout: float,
    timeout_message: str,
    **kwargs: Any,
) -> Any:
    """Wait for one external call without extending the caller's deadline.

    The callable itself may be in a non-cancellable network operation. The
    caller returns at its deadline and must ignore the late result; the
    bounded timeout pool retains its admission slot until that worker exits.
    Interpreter shutdown retains the normal ThreadPoolExecutor atexit
    behavior rather than promising a non-waiting process exit.
    """
    if timeout <= 0:
        raise TimeoutError(timeout_message)
    deadline = None if math.isinf(timeout) else time.monotonic() + timeout
    if deadline is None:
        acquired = _TIMEOUT_CAPACITY.acquire()
    else:
        acquired = _TIMEOUT_CAPACITY.acquire(timeout=max(0.0, deadline - time.monotonic()))
    if not acquired:
        raise TimeoutError(timeout_message)

    future: Future[Any] | None = None
    callback_registered = False
    release_lock = threading.Lock()
    released = False

    def release_slot(_future: Future[Any] | None = None) -> None:
        nonlocal released
        with release_lock:
            if released:
                return
            released = True
            _TIMEOUT_CAPACITY.release()

    try:
        if deadline is not None and deadline <= time.monotonic():
            raise TimeoutError(timeout_message)
        future = _TIMEOUT_EXECUTOR.submit(function, *args, **kwargs)
        with _ACTIVE_TIMEOUT_FUTURES_LOCK:
            _ACTIVE_TIMEOUT_FUTURES.add(future)

        def timeout_future_done(done_future: Future[Any]) -> None:
            _discard_timeout_future(done_future)
            release_slot(done_future)

        try:
            future.add_done_callback(timeout_future_done)
        except BaseException:
            _discard_timeout_future(future)
            raise
        callback_registered = True
        remaining = None if deadline is None else deadline - time.monotonic()
        if remaining is not None and remaining <= 0:
            future.cancel()
            raise TimeoutError(timeout_message)
        try:
            if remaining is None:
                return future.result()
            return future.result(timeout=remaining)
        except FuturesTimeoutError as exc:
            future.cancel()
            raise TimeoutError(timeout_message) from exc
    finally:
        if future is None or not callback_registered:
            release_slot()
