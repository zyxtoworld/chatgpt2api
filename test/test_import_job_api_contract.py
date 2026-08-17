from __future__ import annotations

from unittest import mock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import api.accounts as accounts_module
from services.protocol.error_response import ImportJobActiveError


AUTH_HEADERS = {"Authorization": "Bearer chatgpt2api"}


DELETE_CASES = (
    ("cpa", "/api/cpa/pools/pool-1", "cpa_config", "delete_pool"),
    ("sub2api", "/api/sub2api/servers/server-1", "sub2api_config", "delete_server"),
    ("ccload", "/api/ccload/servers/server-1", "ccload_config", "delete_server"),
)


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(accounts_module.create_router())
    return TestClient(app, raise_server_exceptions=False)


@pytest.mark.parametrize("_name, path, config_name, delete_name", DELETE_CASES)
def test_delete_routes_project_only_active_import_conflict_to_409(
    _name: str,
    path: str,
    config_name: str,
    delete_name: str,
) -> None:
    config = getattr(accounts_module, config_name)
    with (
        mock.patch.object(accounts_module, "require_admin_async", new=mock.AsyncMock(return_value={"role": "admin"})),
        mock.patch.object(config, delete_name, side_effect=ImportJobActiveError("import is already running")),
    ):
        response = _client().delete(path, headers=AUTH_HEADERS)

    assert response.status_code == 409, response.text
    assert response.json()["detail"]["error"] == "import is already running"


@pytest.mark.parametrize("_name, path, config_name, delete_name", DELETE_CASES)
def test_delete_routes_keep_missing_connection_as_404(
    _name: str,
    path: str,
    config_name: str,
    delete_name: str,
) -> None:
    config = getattr(accounts_module, config_name)
    with (
        mock.patch.object(accounts_module, "require_admin_async", new=mock.AsyncMock(return_value={"role": "admin"})),
        mock.patch.object(config, delete_name, return_value=False),
    ):
        response = _client().delete(path, headers=AUTH_HEADERS)

    assert response.status_code == 404, response.text
    assert response.json()["detail"]["error"] in {"pool not found", "server not found"}
