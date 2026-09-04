from __future__ import annotations

import errno
import json
import os
from pathlib import Path
import re
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


def test_container_uses_rust_app_and_external_config() -> None:
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    workflow = (ROOT / ".github" / "workflows" / "docker-publish.yml").read_text(encoding="utf-8")

    app_marker = re.search(r"^FROM [^\n]+ AS app$", dockerfile, re.MULTILINE)
    assert app_marker is not None
    app_stage = dockerfile[app_marker.start() :]
    assert " AS rust-build" in dockerfile
    assert "COPY rust/Cargo.toml rust/Cargo.lock ./" in dockerfile
    assert "COPY account_snapshot_contract.json /app/account_snapshot_contract.json" in dockerfile
    assert (
        "COPY services/protocol/codex_public_item_manifest.json "
        "/app/services/protocol/codex_public_item_manifest.json"
    ) in dockerfile
    assert "COPY rust/file_identity ./file_identity" in dockerfile
    assert "COPY rust/src ./src" in dockerfile
    assert "cargo build --release --locked --bin chatgpt2api-rust" in dockerfile
    assert (
        "COPY --from=rust-build /app/rust/target/release/chatgpt2api-rust "
        "/usr/local/bin/chatgpt2api-rust"
    ) in app_stage
    assert "COPY --from=web-build /app/web/out ./web_dist" in app_stage
    assert 'CMD ["/usr/local/bin/chatgpt2api-rust"]' in app_stage
    for forbidden_runtime in (
        "python:",
        "pip install",
        "pyproject.toml",
        "uv.lock",
        "uv sync",
        "COPY main.py",
        "COPY api",
        "COPY services",
        "COPY utils",
        "COPY scripts",
        "uvicorn",
        "main:app",
    ):
        assert forbidden_runtime not in app_stage

    assert "FROM --platform=$TARGETPLATFORM python:3.13-slim AS app" not in dockerfile
    assert "COPY config.json" not in dockerfile
    docker_job = workflow.split("\n  docker:\n", 1)[1]
    assert "target: app" in docker_job


def test_container_carries_the_pinned_codex_client_version() -> None:
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    workflow = (ROOT / ".github" / "workflows" / "docker-publish.yml").read_text(encoding="utf-8")

    assert "ARG CODEX_CLIENT_VERSION" in dockerfile
    assert "CODEX_CLIENT_VERSION=${CODEX_CLIENT_VERSION}" in dockerfile
    assert "CODEX_CLIENT_VERSION=0.147.0" in workflow
    assert "CODEX_MODELS_CLIENT_VERSION" not in dockerfile
    assert "CODEX_MODELS_CLIENT_VERSION" not in workflow


def test_publish_workflow_blocks_docker_on_full_program_and_dependency_gates() -> None:
    workflow = (ROOT / ".github" / "workflows" / "docker-publish.yml").read_text(encoding="utf-8")

    assert "\n  verify:\n" in workflow
    docker_job = workflow.split("\n  docker:\n", 1)[1]
    assert docker_job.startswith("    needs: verify\n")
    for required_command in (
        "uv run pytest -q",
        "cargo fmt --all -- --check",
        "cargo check --workspace --locked --all-targets",
        "cargo clippy --workspace --locked --all-targets -- -D warnings",
        "cargo test --workspace --locked --all-targets",
        "cargo audit --deny warnings --ignore RUSTSEC-2023-0071 --file rust/Cargo.lock",
        "cargo deny --manifest-path rust/Cargo.toml --locked check all",
        "bun install --frozen-lockfile",
        "node --test test/*.test.mjs",
        "bun x tsc --noEmit",
        "bun run build",
    ):
        assert required_command in workflow
    assert "continue-on-error: true" not in workflow


def test_rust_ci_runs_locked_path_dependency_tests_as_workspace_members() -> None:
    manifest = (ROOT / "rust" / "Cargo.toml").read_text(encoding="utf-8")
    assert "[workspace]" in manifest
    assert 'members = ["file_identity"]' in manifest

    for workflow_path in (
        ROOT / ".github" / "workflows" / "rust-canary.yml",
        ROOT / ".github" / "workflows" / "docker-publish.yml",
    ):
        workflow = workflow_path.read_text(encoding="utf-8")
        for command in (
            "cargo check --workspace --locked --all-targets",
            "cargo clippy --workspace --locked --all-targets -- -D warnings",
            "cargo test --workspace --locked --all-targets",
        ):
            assert command in workflow, f"{workflow_path.name} omits locked workspace gate: {command}"


def test_rust_canary_provisions_python_for_cross_runtime_lock_tests() -> None:
    workflow = (ROOT / ".github" / "workflows" / "rust-canary.yml").read_text(encoding="utf-8")

    assert "astral-sh/setup-uv@c771a70e6277c0a99b617c7a806ffedaca235ff9" in workflow
    assert 'version: "0.12.1"' in workflow
    assert "uv python install 3.13" in workflow
    assert "uv sync --frozen --all-groups --no-install-project" in workflow
    assert "CHATGPT2API_TEST_PYTHON: ${{ github.workspace }}/.venv/bin/python" in workflow


def test_docker_and_rust_ci_use_the_verified_toolchain_versions() -> None:
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    workflow = (ROOT / ".github" / "workflows" / "docker-publish.yml").read_text(encoding="utf-8")
    rust_workflow = (ROOT / ".github" / "workflows" / "rust-canary.yml").read_text(encoding="utf-8")

    assert (
        "FROM --platform=$BUILDPLATFORM "
        "oven/bun:1.4.0-alpine@sha256:07235578f79ef8c6f97d94aee7938e76f5cdba5f21ae5dbfdd3d3d38058437eb "
        "AS web-build"
    ) in dockerfile
    assert (
        "FROM --platform=$TARGETPLATFORM "
        "rust:1.98-bookworm@sha256:e70e2eec3d495fd5c8e0be74adda86507dfac7f51a724fbf9813ff59b2b247c7 "
        "AS rust-build"
    ) in dockerfile
    assert "toolchain: 1.98.0" in workflow
    assert "toolchain: 1.98.0" in rust_workflow
    assert "oven/bun:1-alpine" not in dockerfile
    assert "rust:1.89-bookworm" not in dockerfile


def test_workflows_pin_external_actions_to_immutable_commits() -> None:
    floating: list[str] = []
    for workflow_path in sorted((ROOT / ".github" / "workflows").glob("*.yml")):
        workflow = workflow_path.read_text(encoding="utf-8")
        for reference in re.findall(r"^\s*uses:\s*([^\s#]+)", workflow, re.MULTILINE):
            if reference.startswith("./"):
                continue
            if re.fullmatch(r"[^@]+@[0-9a-f]{40}", reference) is None:
                floating.append(f"{workflow_path.name}:{reference}")

    assert floating == [], f"floating GitHub Actions references: {floating}"


def test_rust_release_matrix_keeps_unimplemented_public_contracts_as_hard_blocks() -> None:
    matrix = (ROOT / "docs" / "rust-production-capability-matrix.md").read_text(encoding="utf-8")
    assert "WebSocket /v1/responses" in matrix
    assert "GET/HEAD /files/{file_path}" in matrix
    assert "POST /api/images/download" in matrix
    assert "CPA" in matrix and "Sub2API" in matrix and "CCLoad" in matrix
    assert "当前结论：Rust 目录模型端点修复是有效的局部修复" in matrix
    assert matrix.count("| 阻断 |") >= 10


def test_container_frontend_build_uses_the_bun_lock() -> None:
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")

    assert (
        "FROM --platform=$BUILDPLATFORM "
        "oven/bun:1.4.0-alpine@sha256:07235578f79ef8c6f97d94aee7938e76f5cdba5f21ae5dbfdd3d3d38058437eb "
        "AS web-build"
    ) in dockerfile
    assert "COPY web/package.json web/bun.lock ./" in dockerfile
    assert "RUN bun install --frozen-lockfile" in dockerfile
    assert 'RUN NEXT_PUBLIC_APP_VERSION="$(cat /app/VERSION)" bun run build' in dockerfile
    assert "npm install" not in dockerfile
    assert "npm run build" not in dockerfile


def test_container_healthcheck_requires_public_healthy_boolean() -> None:
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")

    assert "/health?format=json" in dockerfile
    assert '"healthy":true' in dockerfile
    assert "output-document=-" in dockerfile


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
