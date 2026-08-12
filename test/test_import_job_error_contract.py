from __future__ import annotations

import json
from unittest import mock

from fastapi import FastAPI
from fastapi.testclient import TestClient

import api.accounts as accounts_module
from api.accounts import create_router


SECRET = "import-job-opaque-secret owner@example.com"


class _RawProgressConfig:
    def __init__(self, record: dict) -> None:
        self.record = record

    def get_pool(self, pool_id: str):
        return self.record if self.record.get("id") == pool_id else None

    def get_server(self, server_id: str):
        return self.record if self.record.get("id") == server_id else None


def test_persisted_import_errors_are_not_reflected_by_management_api(tmp_path) -> None:
    app = FastAPI()
    app.include_router(create_router())

    cpa_record = {
        "id": "pool-1",
        "base_url": "https://cpa.example.test",
        "secret_key": "cpa-secret",
        "import_job": {
            "job_id": "job-cpa",
            "status": "failed",
            "created_at": "2026-08-11T00:00:00+00:00",
            "updated_at": "2026-08-11T00:00:01+00:00",
            "total": 1,
            "completed": 1,
            "added": 0,
            "skipped": 0,
            "refreshed": 0,
            "failed": 1,
            "errors": [{"name": "owner@example.com", "error": SECRET}],
        },
    }
    sub2api_record = {
        "id": "server-1",
        "base_url": "https://sub2api.example.test",
        "password": "password-secret",
        "api_key": "api-key-secret",
        "import_job": {
            "job_id": "job-sub2api",
            "status": "failed",
            "created_at": "2026-08-11T00:00:00+00:00",
            "updated_at": "2026-08-11T00:00:01+00:00",
            "total": 1,
            "completed": 1,
            "added": 0,
            "skipped": 0,
            "refreshed": 0,
            "failed": 1,
            "errors": [{"name": "owner@example.com", "error": SECRET}],
        },
    }

    with (
        mock.patch.object(accounts_module, "require_admin_async", return_value={"role": "admin"}),
        mock.patch.object(accounts_module, "cpa_config", _RawProgressConfig(cpa_record)),
        mock.patch.object(accounts_module, "sub2api_config", _RawProgressConfig(sub2api_record)),
    ):
        client = TestClient(app)
        responses = [
            client.get("/api/cpa/pools/pool-1/import"),
            client.get("/api/sub2api/servers/server-1/import"),
        ]

    for response in responses:
        assert response.status_code == 200, response.text
        assert SECRET not in response.text
        assert "owner@example.com" not in response.text
        assert "import-job-opaque-secret" not in json.dumps(response.json(), ensure_ascii=False)
