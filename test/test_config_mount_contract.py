from __future__ import annotations

import errno
import os
from pathlib import Path
import subprocess
import sys
from unittest import mock

from fastapi import FastAPI
from fastapi.testclient import TestClient

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


def test_container_runtime_uses_the_frozen_environment_without_syncing_dependencies() -> None:
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")

    assert "RUN uv sync --frozen --no-dev --no-install-project" in dockerfile
    assert 'CMD ["uv", "run", "--no-sync", "uvicorn"' in dockerfile


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
