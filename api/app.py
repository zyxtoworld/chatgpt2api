from __future__ import annotations

import asyncio
import logging
import sys
from contextlib import asynccontextmanager
from threading import Event

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from api import accounts, ai, image_tasks, system
from api.errors import install_exception_handlers
from api.support import open_web_asset, start_limited_account_watcher, stop_limited_account_watcher
from services.backup_service import backup_service
from services.config import config
from services.image_service import start_image_cleanup_scheduler, stop_image_cleanup_scheduler
from services.opened_file_response import OpenedFileResponse
from services.protocol.conversation import wait_for_image_cleanup_tasks
from services.task_executor import wait_for_background_tasks


logger = logging.getLogger(__name__)


_NO_SPA_FALLBACK_PREFIXES = frozenset({
    "api",
    "auth",
    "files",
    "health",
    "image-thumbnails",
    "images",
    "v1",
})


def create_app() -> FastAPI:
    app_version = config.app_version

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        stop_event = Event()
        thread = None
        cleanup_thread = None
        backup_start_attempted = False
        try:
            thread = start_limited_account_watcher(stop_event)
            cleanup_thread = start_image_cleanup_scheduler(stop_event)
            backup_start_attempted = True
            backup_service.start()
            await asyncio.to_thread(config.cleanup_old_images)
            yield
        finally:
            startup_error = sys.exc_info()[1]
            cleanup_errors: list[Exception] = []

            def run_cleanup(phase: str, callback) -> bool:
                try:
                    callback()
                    return True
                except Exception as exc:
                    cleanup_errors.append(exc)
                    try:
                        logger.error(
                            "application lifecycle cleanup failed",
                            extra={"phase": phase, "error_type": type(exc).__name__},
                        )
                    except Exception:
                        pass
                    return False

            async def run_async_cleanup(phase: str, callback) -> None:
                try:
                    await callback()
                except Exception as exc:
                    cleanup_errors.append(exc)
                    try:
                        logger.error(
                            "application lifecycle cleanup failed",
                            extra={"phase": phase, "error_type": type(exc).__name__},
                        )
                    except Exception:
                        pass

            watcher_cleanup_succeeded = run_cleanup(
                "account_watcher",
                lambda: stop_limited_account_watcher(stop_event, thread),
            )
            if not watcher_cleanup_succeeded:
                stop_event.set()
                if thread is not None:
                    run_cleanup("account_watcher_join", lambda: thread.join(timeout=1))
            if cleanup_thread is not None:
                run_cleanup(
                    "image_cleanup_thread",
                    lambda: stop_image_cleanup_scheduler(stop_event, cleanup_thread),
                )
            if backup_start_attempted:
                run_cleanup("backup_scheduler", backup_service.stop)
            await run_async_cleanup("management_tasks", accounts.wait_for_management_tasks)
            await run_async_cleanup("health_probe_tasks", system.wait_for_health_probe_tasks)
            await run_async_cleanup(
                "background_tasks",
                lambda: asyncio.to_thread(wait_for_background_tasks),
            )
            await run_async_cleanup(
                "image_cleanup_tasks",
                lambda: asyncio.to_thread(wait_for_image_cleanup_tasks),
            )
            run_cleanup(
                "storage_backend",
                lambda: config.get_storage_backend().close(),
            )
            if startup_error is None and cleanup_errors:
                raise cleanup_errors[0]

    app = FastAPI(title="chatgpt2api", version=app_version, lifespan=lifespan)
    install_exception_handlers(app)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.include_router(ai.create_router())
    app.include_router(accounts.create_router())
    app.include_router(image_tasks.create_router())
    app.include_router(system.create_router(app_version))

    @app.api_route("/{full_path:path}", methods=["GET", "HEAD"], include_in_schema=False)
    async def serve_web(full_path: str):
        asset = open_web_asset(full_path)
        if asset is not None:
            try:
                return OpenedFileResponse(asset, include_filename=False)
            except Exception:
                asset.file.close()
                raise
        clean_path = full_path.strip("/")
        first_segment = clean_path.split("/", 1)[0]
        if clean_path.startswith("_next/") or first_segment in _NO_SPA_FALLBACK_PREFIXES:
            raise HTTPException(status_code=404, detail="Not Found")
        fallback = open_web_asset("")
        if fallback is None:
            raise HTTPException(status_code=404, detail="Not Found")
        try:
            return OpenedFileResponse(fallback, include_filename=False)
        except Exception:
            fallback.file.close()
            raise

    return app
