from __future__ import annotations

import errno
import json
import os
from pathlib import Path
import subprocess
import sys
from unittest import mock

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest

import api.system as system_module
import services.config as config_module
from api.system import create_router
from services.config import ConfigStore


ROOT = Path(__file__).resolve().parents[1]
COMPOSE_FILES = (
    ROOT / "docker-compose.yml",
    ROOT / "docker-compose.local.yml",
    ROOT / "docker-compose.warp.yml",
)


def test_config_file_path_can_be_selected_for_a_directory_mount(tmp_path: Path) -> None:
    config_path = tmp_path / "config_mount" / "config.json"
    config_path.parent.mkdir()
    config_path.write_text('{"auth-key":"fixture-auth"}\n', encoding="utf-8")

    env = os.environ.copy()
    env["CHATGPT2API_CONFIG_FILE"] = str(config_path)
    env["CHATGPT2API_AUTH_KEY"] = "fixture-auth"
    result = subprocess.run(
        [sys.executable, "-c", "from services.config import CONFIG_FILE; print(CONFIG_FILE)"],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )

    assert Path(result.stdout.strip()) == config_path


def test_compose_uses_the_original_fixed_config_file_mount() -> None:
    for compose_file in COMPOSE_FILES:
        text = compose_file.read_text(encoding="utf-8")
        assert "./config.json:/app/config.json" in text
        assert "config_mount" not in text
        assert "CHATGPT2API_CONFIG_FILE" not in text


def test_bind_mounted_config_hot_saves_when_atomic_replace_is_busy(tmp_path: Path) -> None:
    config_path = tmp_path / "config.json"
    config_path.write_text('{"auth-key":"fixture-auth","proxy":""}\n', encoding="utf-8")
    before = config_path.stat()
    store = ConfigStore(config_path)

    with mock.patch.object(
        config_module,
        "atomic_write_bytes",
        side_effect=OSError(errno.EBUSY, "Device or resource busy"),
    ):
        updated = store.update({"proxy": "http://proxy.example.test:8080"})

    assert updated["proxy"] == "http://proxy.example.test:8080"
    assert config_module.json.loads(config_path.read_text(encoding="utf-8"))["proxy"] == updated["proxy"]
    after = config_path.stat()
    assert after.st_ino == before.st_ino
    assert after.st_mode == before.st_mode


def test_bind_mounted_config_partial_write_failure_preserves_old_snapshot(tmp_path: Path) -> None:
    config_path = tmp_path / "config.json"
    original_payload = json.dumps({"auth-key": "fixture-auth", "proxy": ""}, separators=(",", ":")) + "\n"
    config_path.write_text(original_payload, encoding="utf-8")
    store = ConfigStore(config_path)
    original_write = config_module.os.write
    calls = 0

    def fail_after_partial_write(fd: int, payload: bytes | memoryview) -> int:
        nonlocal calls
        calls += 1
        if calls == 1:
            current_payload = bytes(payload)
            old_bytes = original_payload.encode("utf-8")
            first_difference = next(
                index for index, (old, new) in enumerate(zip(old_bytes, current_payload)) if old != new
            )
            return original_write(fd, current_payload[: first_difference + 1])
        if calls == 2:
            raise OSError("bind mount write failed")
        return original_write(fd, payload)

    with (
        mock.patch.object(
            config_module,
            "atomic_write_bytes",
            side_effect=OSError(errno.EBUSY, "Device or resource busy"),
        ),
        mock.patch.object(config_module.os, "write", side_effect=fail_after_partial_write),
        pytest.raises(OSError, match="bind mount write failed"),
    ):
        store.update({"proxy": "http://proxy.example.test:8080"})

    assert config_path.read_text(encoding="utf-8") == original_payload


def test_bind_mounted_config_rejects_hardlinked_target(tmp_path: Path) -> None:
    config_path = tmp_path / "config.json"
    alias_path = tmp_path / "secret-alias.json"
    original_payload = json.dumps({"auth-key": "fixture-auth", "proxy": ""}, separators=(",", ":")) + "\n"
    config_path.write_text(original_payload, encoding="utf-8")
    os.link(config_path, alias_path)
    store = ConfigStore(config_path)

    with (
        mock.patch.object(
            config_module,
            "atomic_write_bytes",
            side_effect=OSError(errno.EBUSY, "Device or resource busy"),
        ),
        pytest.raises(OSError),
    ):
        store.update({"proxy": "http://proxy.example.test:8080"})

    assert config_path.read_text(encoding="utf-8") == original_payload
    assert alias_path.read_text(encoding="utf-8") == original_payload


def test_container_runtime_uses_the_frozen_environment_without_syncing_dependencies() -> None:
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")

    assert "RUN uv sync --frozen --no-dev --no-install-project" in dockerfile
    assert 'CMD ["uv", "run", "--no-sync", "uvicorn"' in dockerfile


def test_container_frontend_build_uses_the_bun_lock() -> None:
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")

    assert "FROM --platform=$BUILDPLATFORM oven/bun:1-alpine AS web-build" in dockerfile
    assert "COPY web/package.json web/bun.lock ./" in dockerfile
    assert "RUN bun install --frozen-lockfile" in dockerfile
    assert 'RUN NEXT_PUBLIC_APP_VERSION="$(cat /app/VERSION)" bun run build' in dockerfile
    assert "npm install" not in dockerfile
    assert "npm run build" not in dockerfile


def test_settings_endpoint_persists_global_proxy_through_real_config_store(tmp_path: Path) -> None:
    config_path = tmp_path / "config_mount" / "config.json"
    config_path.parent.mkdir()
    config_path.write_text('{"auth-key":"fixture-auth"}\n', encoding="utf-8")
    store = ConfigStore(config_path)
    app = FastAPI()
    app.include_router(create_router("test"))

    with (
        mock.patch.object(system_module, "config", store),
        mock.patch.object(
            system_module,
            "require_admin_async",
            mock.AsyncMock(return_value={"role": "admin"}),
        ),
    ):
        response = TestClient(app).post(
            "/api/settings",
            json={"proxy": "http://proxy.example.test:8080"},
        )

    assert response.status_code == 200, response.text
    assert response.json()["config"]["proxy"] == "http://proxy.example.test:8080"
    assert '"proxy": "http://proxy.example.test:8080"' in config_path.read_text(encoding="utf-8")


def test_settings_endpoint_rejects_unknown_fields_without_persisting(tmp_path: Path) -> None:
    config_path = tmp_path / "config.json"
    config_path.write_text('{"auth-key":"original-auth","proxy":""}\n', encoding="utf-8")
    store = ConfigStore(config_path)
    app = FastAPI()
    app.include_router(create_router("test"))

    with (
        mock.patch.object(system_module, "config", store),
        mock.patch.object(
            system_module,
            "require_admin_async",
            mock.AsyncMock(return_value={"role": "admin"}),
        ),
    ):
        response = TestClient(app).post(
            "/api/settings",
            json={"auth-key": "new-auth-canary"},
        )

    assert response.status_code == 400, response.text
    assert "new-auth-canary" not in response.text
    assert config_path.read_text(encoding="utf-8") == '{"auth-key":"original-auth","proxy":""}\n'


def test_settings_endpoint_does_not_persist_unknown_ai_review_fields(tmp_path: Path) -> None:
    config_path = tmp_path / "config.json"
    config_path.write_text('{"auth-key":"original-auth"}\n', encoding="utf-8")
    store = ConfigStore(config_path)
    app = FastAPI()
    app.include_router(create_router("test"))
    canary = "ai-review-internal-canary owner@example.com"

    with (
        mock.patch.object(system_module, "config", store),
        mock.patch.object(
            system_module,
            "require_admin_async",
            mock.AsyncMock(return_value={"role": "admin"}),
        ),
    ):
        response = TestClient(app).post(
            "/api/settings",
            json={
                "ai_review": {
                    "enabled": True,
                    "model": "review-model",
                    "internal_metadata": canary,
                }
            },
        )

    assert response.status_code == 200, response.text
    assert canary not in config_path.read_text(encoding="utf-8")


def test_settings_endpoint_normalizes_sensitive_words_before_persisting(tmp_path: Path) -> None:
    config_path = tmp_path / "config.json"
    config_path.write_text('{"auth-key":"original-auth"}\n', encoding="utf-8")
    store = ConfigStore(config_path)
    app = FastAPI()
    app.include_router(create_router("test"))
    canary = "sensitive-word-internal-canary owner@example.com"

    with (
        mock.patch.object(system_module, "config", store),
        mock.patch.object(
            system_module,
            "require_admin_async",
            mock.AsyncMock(return_value={"role": "admin"}),
        ),
    ):
        response = TestClient(app).post(
            "/api/settings",
            json={"sensitive_words": [{"value": canary}, "  blocked  ", ""]},
        )

    assert response.status_code == 200, response.text
    saved = config_module.json.loads(config_path.read_text(encoding="utf-8"))
    assert saved["sensitive_words"] == ["blocked"]
    assert canary not in config_path.read_text(encoding="utf-8")


def test_settings_endpoint_normalizes_scalar_and_log_settings_before_persisting(tmp_path: Path) -> None:
    config_path = tmp_path / "config.json"
    config_path.write_text('{"auth-key":"original-auth"}\n', encoding="utf-8")
    store = ConfigStore(config_path)
    app = FastAPI()
    app.include_router(create_router("test"))
    canary = "settings-value-internal-canary owner@example.com"

    with (
        mock.patch.object(system_module, "config", store),
        mock.patch.object(
            system_module,
            "require_admin_async",
            mock.AsyncMock(return_value={"role": "admin"}),
        ),
    ):
        response = TestClient(app).post(
            "/api/settings",
            json={
                "global_system_prompt": {"value": canary},
                "default_upstream_model_name": {"value": canary},
                "default_thinking_effort": {"value": canary},
                "log_levels": [{"value": canary}, " DEBUG "],
                "image_poll_timeout_secs": {"value": canary},
            },
        )

    assert response.status_code == 200, response.text
    saved = config_module.json.loads(config_path.read_text(encoding="utf-8"))
    assert saved["global_system_prompt"] == ""
    assert saved["default_upstream_model_name"] == "gpt-5-5"
    assert saved["default_thinking_effort"] == "auto"
    assert saved["log_levels"] == ["debug"]
    assert saved["image_poll_timeout_secs"] == 120
    assert canary not in config_path.read_text(encoding="utf-8")


def test_settings_endpoint_returns_image_tuning_fields(tmp_path: Path) -> None:
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "auth-key": "fixture-auth",
                "image_settle_enabled": False,
                "image_check_before_hit_enabled": False,
                "image_settle_secs": 0.5,
            }
        ),
        encoding="utf-8",
    )
    store = ConfigStore(config_path)
    app = FastAPI()
    app.include_router(create_router("test"))

    with (
        mock.patch.object(system_module, "config", store),
        mock.patch.object(
            system_module,
            "require_admin_async",
            mock.AsyncMock(return_value={"role": "admin"}),
        ),
    ):
        response = TestClient(app).get("/api/settings")

    assert response.status_code == 200, response.text
    settings = response.json()["config"]
    assert settings["image_settle_enabled"] is False
    assert settings["image_check_before_hit_enabled"] is False
    assert settings["image_settle_secs"] == 0.5
