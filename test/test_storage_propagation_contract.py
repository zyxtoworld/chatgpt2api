from __future__ import annotations

import asyncio
import hashlib
import json
import html
from unittest import mock
from typing import Any

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest

import api.accounts as accounts_module
import api.system as system_module
import services.account_service as account_module
from services.auth_service import AuthService
from api.system import create_router
from services.account_service import AccountService
from services.storage.base import StorageBackend, StorageConflictError, StorageDataError
from services.storage.json_storage import JSONStorageBackend


class LeakyStorageBackend(StorageBackend):
    sentinel = "storage-public-leak-sentinel-9b2f"

    def __init__(self, account_type: str = "free") -> None:
        self.health_calls = 0
        self.account_type = account_type

    def load_accounts(self) -> list[dict[str, Any]]:
        return [{
            "access_token": "health-token",
            "status": "正常",
            "quota": 1,
            "type": self.account_type,
        }]

    def save_accounts(self, accounts: list[dict[str, Any]]) -> None:
        raise AssertionError("health endpoint must not write accounts")

    def load_auth_keys(self) -> list[dict[str, Any]]:
        return []

    def save_auth_keys(self, auth_keys: list[dict[str, Any]]) -> None:
        raise AssertionError("health endpoint must not write auth keys")

    def health_check(self) -> dict[str, Any]:
        self.health_calls += 1
        return {
            "status": "healthy",
            "backend": "database",
            "file_path": "C:/private/accounts.json",
            "auth_keys_file_path": "C:/private/auth_keys.json",
            "database_url": f"postgresql://user:{self.sentinel}@db.internal/app",
            "repo_url": f"https://token:{self.sentinel}@git.internal/repo",
            "local_cache_dir": "C:/private/git-cache",
            "branch": "private-branch",
            "file": "private/accounts.json",
            "secret": self.sentinel,
            "future_storage_field": "storage-future-sentinel",
        }

    def get_backend_info(self) -> dict[str, Any]:
        return {
            "type": "database",
            "description": self.sentinel,
            "file_path": "C:/private/accounts.json",
            "auth_keys_file_path": "C:/private/auth_keys.json",
            "database_url": f"postgresql://user:{self.sentinel}@db.internal/app",
            "repo_url": f"https://token:{self.sentinel}@git.internal/repo",
            "local_cache_dir": "C:/private/git-cache",
            "branch": "private-branch",
            "file": "private/accounts.json",
            "secret": self.sentinel,
            "future_storage_field": "storage-future-sentinel",
        }


def test_health_does_not_report_cached_accounts_as_healthy_after_snapshot_corruption(tmp_path) -> None:
    accounts_path = tmp_path / "accounts.json"
    auth_keys_path = tmp_path / "auth_keys.json"
    accounts_path.write_text(
        json.dumps([{"access_token": "cached-token", "status": "正常", "quota": 1}]),
        encoding="utf-8",
    )
    auth_keys_path.write_text("[]", encoding="utf-8")
    storage = JSONStorageBackend(accounts_path, auth_keys_path)
    service = AccountService(storage)

    assert service.get_stats()["active"] == 1
    accounts_path.write_text("{broken", encoding="utf-8")

    app = FastAPI()
    app.include_router(create_router("test"))
    with (
        mock.patch.object(system_module.config, "get_storage_backend", return_value=storage),
        mock.patch.object(account_module, "account_service", service),
    ):
        response = TestClient(app).get("/health?format=json")

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["storage"] == {
        "backend": "json",
        "health": {"status": "unhealthy", "error": "存储后端健康检查失败"},
    }
    assert payload["healthy"] is False
    assert payload["status"] == "degraded"


def test_public_health_projects_storage_details_without_paths_or_urls() -> None:
    storage = LeakyStorageBackend()
    service = AccountService(storage)
    app = FastAPI()
    app.include_router(create_router("test"))

    with (
        mock.patch.object(system_module.config, "get_storage_backend", return_value=storage),
        mock.patch.object(account_module, "account_service", service),
    ):
        json_response = TestClient(app).get("/health?format=json")
        assert storage.health_calls == 1
        html_response = TestClient(app).get("/health")

    assert json_response.status_code == 200, json_response.text
    payload = json_response.json()
    assert payload["storage"] == {
        "backend": "database",
        "health": {"status": "healthy"},
    }
    assert storage.health_calls == 2
    for response_text in (json_response.text, html_response.text):
        for sensitive_key in (
            "file_path",
            "auth_keys_file_path",
            "database_url",
            "repo_url",
            "local_cache_dir",
            "branch",
            "file",
            "secret",
            "future_storage_field",
        ):
            assert sensitive_key not in response_text
        assert LeakyStorageBackend.sentinel not in response_text
        assert "storage-future-sentinel" not in response_text


def test_public_health_does_not_run_storage_io_on_event_loop() -> None:
    class LoopCheckingStorage(LeakyStorageBackend):
        def health_check(self) -> dict[str, Any]:
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                pass
            else:
                raise AssertionError("storage health I/O ran on the ASGI event loop")
            return super().health_check()

    storage = LoopCheckingStorage()
    service = AccountService(storage)
    app = FastAPI()
    app.include_router(create_router("test"))

    with (
        mock.patch.object(system_module.config, "get_storage_backend", return_value=storage),
        mock.patch.object(account_module, "account_service", service),
    ):
        response = TestClient(app).get("/health?format=json")

    assert response.status_code == 200, response.text
    assert response.json()["storage"]["health"] == {"status": "healthy"}


def test_health_html_escapes_persisted_account_type() -> None:
    malicious_type = '<img src=x onerror="storage-html-sentinel">'
    storage = LeakyStorageBackend(account_type=malicious_type)
    service = AccountService(storage)
    app = FastAPI()
    app.include_router(create_router("test"))

    with (
        mock.patch.object(system_module.config, "get_storage_backend", return_value=storage),
        mock.patch.object(account_module, "account_service", service),
    ):
        response = TestClient(app).get("/health")

    assert response.status_code == 200, response.text
    assert "<img" not in response.text
    assert 'onerror="storage-html-sentinel"' not in response.text
    assert html.escape(malicious_type, quote=True) in response.text


def test_accounts_delete_does_not_overwrite_corrupt_snapshot(tmp_path) -> None:
    accounts_path = tmp_path / "accounts.json"
    auth_keys_path = tmp_path / "auth_keys.json"
    accounts_path.write_text(
        json.dumps([{"access_token": "delete-token", "status": "正常", "quota": 1}]),
        encoding="utf-8",
    )
    auth_keys_path.write_text("[]", encoding="utf-8")
    storage = JSONStorageBackend(accounts_path, auth_keys_path)
    service = AccountService(storage)
    accounts_path.write_text("{broken", encoding="utf-8")

    app = FastAPI()
    app.include_router(accounts_module.create_router())
    with (
        mock.patch.object(accounts_module, "account_service", service),
        mock.patch.object(accounts_module, "require_admin_async", return_value={"role": "admin"}),
    ):
        response = TestClient(app, raise_server_exceptions=False).request(
            "DELETE",
            "/api/accounts",
            json={"tokens": ["delete-token"]},
        )

    assert response.status_code == 500, response.text
    assert "{broken" not in response.text
    assert accounts_path.read_text(encoding="utf-8") == "{broken"


def _new_account_service_with_corrupt_snapshot(tmp_path):
    accounts_path = tmp_path / "accounts.json"
    auth_keys_path = tmp_path / "auth_keys.json"
    accounts_path.write_text(
        json.dumps([{"access_token": "base-token", "status": "正常", "quota": 1}]),
        encoding="utf-8",
    )
    auth_keys_path.write_text("[]", encoding="utf-8")
    storage = JSONStorageBackend(accounts_path, auth_keys_path)
    service = AccountService(storage)
    accounts_path.write_text("{broken", encoding="utf-8")
    return service, accounts_path


@pytest.mark.parametrize("operation", ("add", "update", "disable"))
def test_account_mutation_paths_do_not_overwrite_corrupt_snapshot(tmp_path, operation: str) -> None:
    service, accounts_path = _new_account_service_with_corrupt_snapshot(tmp_path)

    with pytest.raises(StorageDataError):
        if operation == "add":
            service.add_accounts(["new-token"])
        elif operation == "update":
            service.update_account("base-token", {"quota": 2})
        else:
            service.update_account("base-token", {"status": "禁用"})

    assert accounts_path.read_text(encoding="utf-8") == "{broken"


def _new_auth_service_with_corrupt_snapshot(tmp_path):
    accounts_path = tmp_path / "accounts.json"
    auth_keys_path = tmp_path / "auth_keys.json"
    raw_key = "auth-base-raw-key"
    auth_keys_path.write_text(
        json.dumps({
            "items": [{
                "id": "base-key",
                "role": "user",
                "key_hash": hashlib.sha256(raw_key.encode("utf-8")).hexdigest(),
                "name": "base",
                "enabled": True,
            }],
        }),
        encoding="utf-8",
    )
    accounts_path.write_text("[]", encoding="utf-8")
    storage = JSONStorageBackend(accounts_path, auth_keys_path)
    service = AuthService(storage)
    auth_keys_path.write_text("{broken", encoding="utf-8")
    return service, auth_keys_path, raw_key


@pytest.mark.parametrize("operation", ("create", "update", "delete"))
def test_auth_key_mutation_paths_do_not_overwrite_corrupt_snapshot(tmp_path, operation: str) -> None:
    service, auth_keys_path, _ = _new_auth_service_with_corrupt_snapshot(tmp_path)

    with pytest.raises(StorageDataError):
        if operation == "create":
            service.create_key(role="user", name="new")
        elif operation == "update":
            service.update_key("base-key", {"enabled": False}, role="user")
        else:
            service.delete_key("base-key", role="user")

    assert auth_keys_path.read_text(encoding="utf-8") == "{broken"


def test_authenticate_fails_closed_on_corrupt_auth_snapshot(tmp_path) -> None:
    service, auth_keys_path, raw_key = _new_auth_service_with_corrupt_snapshot(tmp_path)

    with pytest.raises(StorageDataError):
        service.authenticate(raw_key)

    assert auth_keys_path.read_text(encoding="utf-8") == "{broken"


@pytest.mark.parametrize("enabled", ("false", 0, 1, None, [], {}))
def test_auth_service_rejects_non_boolean_enabled_snapshot(tmp_path, enabled) -> None:
    accounts_path = tmp_path / "accounts.json"
    auth_keys_path = tmp_path / "auth_keys.json"
    raw_key = "auth-enabled-raw-key"
    payload = {
        "items": [{
            "id": "enabled-key",
            "role": "user",
            "key_hash": hashlib.sha256(raw_key.encode("utf-8")).hexdigest(),
            "name": "enabled",
            "enabled": enabled,
        }],
    }
    auth_keys_path.write_text(json.dumps(payload), encoding="utf-8")
    accounts_path.write_text("[]", encoding="utf-8")
    before = auth_keys_path.read_text(encoding="utf-8")

    with pytest.raises(StorageDataError):
        AuthService(JSONStorageBackend(accounts_path, auth_keys_path))

    assert auth_keys_path.read_text(encoding="utf-8") == before


@pytest.mark.parametrize("enabled, expected_authenticated", ((False, False), (True, True)))
def test_auth_service_honors_explicit_boolean_enabled(tmp_path, enabled: bool, expected_authenticated: bool) -> None:
    accounts_path = tmp_path / "accounts.json"
    auth_keys_path = tmp_path / "auth_keys.json"
    raw_key = "auth-explicit-enabled-key"
    auth_keys_path.write_text(
        json.dumps({
            "items": [{
                "id": "explicit-enabled-key",
                "role": "user",
                "key_hash": hashlib.sha256(raw_key.encode("utf-8")).hexdigest(),
                "name": "explicit-enabled",
                "enabled": enabled,
            }],
        }),
        encoding="utf-8",
    )
    accounts_path.write_text("[]", encoding="utf-8")
    before = auth_keys_path.read_text(encoding="utf-8")
    service = AuthService(JSONStorageBackend(accounts_path, auth_keys_path))

    identity = service.authenticate(raw_key)

    assert (identity is not None) is expected_authenticated
    if not expected_authenticated:
        assert auth_keys_path.read_text(encoding="utf-8") == before


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("role", {"name": "user"}),
        ("key_hash", {"hash": "opaque"}),
        ("id", 123),
        ("created_at", 123),
        ("last_used_at", {"timestamp": "opaque"}),
        ("created_at", "not-a-timestamp"),
        ("last_used_at", "not-a-timestamp"),
    ),
)
def test_auth_service_rejects_wrong_identity_field_types_and_formats(tmp_path, field: str, value) -> None:
    accounts_path = tmp_path / "accounts.json"
    auth_keys_path = tmp_path / "auth_keys.json"
    raw_key = "auth-field-contract-key"
    item = {
        "id": "field-contract-key",
        "role": "user",
        "key_hash": hashlib.sha256(raw_key.encode("utf-8")).hexdigest(),
        "name": "field-contract",
        "enabled": True,
    }
    item[field] = value
    auth_keys_path.write_text(json.dumps({"items": [item]}), encoding="utf-8")
    accounts_path.write_text("[]", encoding="utf-8")
    before = auth_keys_path.read_text(encoding="utf-8")

    with pytest.raises(StorageDataError):
        AuthService(JSONStorageBackend(accounts_path, auth_keys_path))

    assert auth_keys_path.read_text(encoding="utf-8") == before


@pytest.mark.parametrize("operation", ("disable", "delete"))
def test_stale_auth_service_rejects_key_revoked_by_other_service(tmp_path, operation: str) -> None:
    accounts_path = tmp_path / "accounts.json"
    auth_keys_path = tmp_path / "auth_keys.json"
    raw_key = "auth-base-raw-key"
    auth_keys_path.write_text(
        json.dumps({
            "items": [{
                "id": "base-key",
                "role": "user",
                "key_hash": hashlib.sha256(raw_key.encode("utf-8")).hexdigest(),
                "name": "base",
                "enabled": True,
            }],
        }),
        encoding="utf-8",
    )
    accounts_path.write_text("[]", encoding="utf-8")
    service_one = AuthService(JSONStorageBackend(accounts_path, auth_keys_path))
    service_two = AuthService(JSONStorageBackend(accounts_path, auth_keys_path))

    assert service_two.authenticate(raw_key) is not None
    if operation == "disable":
        assert service_one.update_key("base-key", {"enabled": False}, role="user") is not None
    else:
        assert service_one.delete_key("base-key", role="user") is True

    assert service_two.authenticate(raw_key) is None


def test_two_account_services_do_not_last_writer_win(tmp_path) -> None:
    accounts_path = tmp_path / "accounts.json"
    auth_keys_path = tmp_path / "auth_keys.json"
    accounts_path.write_text(
        json.dumps([{"access_token": "base-token", "status": "正常", "quota": 1}]),
        encoding="utf-8",
    )
    auth_keys_path.write_text("[]", encoding="utf-8")
    service_one = AccountService(JSONStorageBackend(accounts_path, auth_keys_path))
    service_two = AccountService(JSONStorageBackend(accounts_path, auth_keys_path))

    with mock.patch.object(AccountService, "_save_cumulative_total"):
        service_one.add_accounts(["new-token"])
        with pytest.raises(StorageConflictError):
            service_two.update_account("base-token", {"status": "禁用"})

    persisted = JSONStorageBackend(accounts_path, auth_keys_path).load_accounts()
    persisted_by_token = {item["access_token"]: item for item in persisted}
    assert set(persisted_by_token) == {"base-token", "new-token"}
    assert persisted_by_token["base-token"]["status"] == "正常"


def test_two_auth_services_do_not_last_writer_win(tmp_path) -> None:
    accounts_path = tmp_path / "accounts.json"
    auth_keys_path = tmp_path / "auth_keys.json"
    raw_key = "auth-base-raw-key"
    auth_keys_path.write_text(
        json.dumps({
            "items": [{
                "id": "base-key",
                "role": "user",
                "key_hash": hashlib.sha256(raw_key.encode("utf-8")).hexdigest(),
                "name": "base",
                "enabled": True,
            }],
        }),
        encoding="utf-8",
    )
    accounts_path.write_text("[]", encoding="utf-8")
    service_one = AuthService(JSONStorageBackend(accounts_path, auth_keys_path))
    service_two = AuthService(JSONStorageBackend(accounts_path, auth_keys_path))

    new_item, _ = service_one.create_key(role="user", name="new")
    assert service_two.authenticate(raw_key) is not None

    persisted = JSONStorageBackend(accounts_path, auth_keys_path).load_auth_keys()
    assert {item["id"] for item in persisted} == {"base-key", new_item["id"]}


def test_authenticate_skips_only_audit_cas_conflict_without_overwrite(tmp_path) -> None:
    accounts_path = tmp_path / "accounts.json"
    auth_keys_path = tmp_path / "auth_keys.json"
    raw_key = "auth-base-raw-key"
    auth_keys_path.write_text(
        json.dumps({
            "items": [{
                "id": "base-key",
                "role": "user",
                "key_hash": hashlib.sha256(raw_key.encode("utf-8")).hexdigest(),
                "name": "base",
                "enabled": True,
            }],
        }),
        encoding="utf-8",
    )
    accounts_path.write_text("[]", encoding="utf-8")
    backend = JSONStorageBackend(accounts_path, auth_keys_path)
    service = AuthService(backend)
    original_save = backend.save_auth_keys_if_revision
    unrelated_item = {
        "id": "unrelated-key",
        "role": "user",
        "key_hash": hashlib.sha256(b"unrelated-raw-key").hexdigest(),
        "name": "unrelated",
        "enabled": True,
    }

    def race_with_unrelated_write(expected, auth_keys):
        current = backend.load_auth_keys()
        backend.save_auth_keys([*current, unrelated_item])
        return original_save(expected, auth_keys)

    with mock.patch.object(backend, "save_auth_keys_if_revision", side_effect=race_with_unrelated_write):
        identity = service.authenticate(raw_key)

    assert identity is not None
    persisted = backend.load_auth_keys()
    assert {item["id"] for item in persisted} == {"base-key", "unrelated-key"}
    assert all("last_used_at" not in item for item in persisted if item["id"] == "base-key")


def test_authenticate_does_not_swallow_corruption_during_audit_write(tmp_path) -> None:
    service, auth_keys_path, raw_key = _new_auth_service_with_corrupt_snapshot(tmp_path)
    auth_keys_path.write_text(
        json.dumps({
            "items": [{
                "id": "base-key",
                "role": "user",
                "key_hash": hashlib.sha256(raw_key.encode("utf-8")).hexdigest(),
                "name": "base",
                "enabled": True,
            }],
        }),
        encoding="utf-8",
    )
    original_save = service.storage.save_auth_keys_if_revision

    def corrupt_before_audit(expected, auth_keys):
        auth_keys_path.write_text("{broken", encoding="utf-8")
        return original_save(expected, auth_keys)

    with mock.patch.object(
        service.storage,
        "save_auth_keys_if_revision",
        side_effect=corrupt_before_audit,
    ):
        with pytest.raises(StorageDataError):
            service.authenticate(raw_key)

    assert auth_keys_path.read_text(encoding="utf-8") == "{broken"
