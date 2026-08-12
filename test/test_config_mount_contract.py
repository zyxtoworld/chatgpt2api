from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys
from unittest import mock

from fastapi import FastAPI
from fastapi.testclient import TestClient

import api.system as system_module
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


def test_compose_mounts_config_parent_directory_for_atomic_replacement() -> None:
    gitignore = (ROOT / ".gitignore").read_text(encoding="utf-8")
    assert "/chatgpt2api-config/" in gitignore
    dockerignore = (ROOT / ".dockerignore").read_text(encoding="utf-8")
    assert "chatgpt2api-config/" in dockerignore

    for compose_file in COMPOSE_FILES:
        text = compose_file.read_text(encoding="utf-8")
        assert "./config.json:/app/config.json" not in text
        assert "/app/config_mount/config.json" in text
        assert "./chatgpt2api-config:/app/config_mount" in text
        assert "./:/app/config_mount" not in text
        assert "/opt/services:/app/config_mount" not in text


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
