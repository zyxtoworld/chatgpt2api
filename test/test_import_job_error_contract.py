from __future__ import annotations

import json
from unittest import mock

from fastapi import FastAPI
from fastapi.testclient import TestClient

import api.accounts as accounts_module
from api.accounts import create_router
from services.cpa_service import CPAConfig
from services.sub2api_service import Sub2APIConfig


SECRET = "import-job-opaque-secret owner@example.com"


def test_persisted_import_errors_are_not_reflected_by_management_api(tmp_path) -> None:
    app = FastAPI()
    app.include_router(create_router())

    cpa_path = tmp_path / "cpa_config.json"
    cpa_path.write_text(
        json.dumps(
            [{
                "id": "pool-1",
                "base_url": "https://cpa.example.test",
                "import_job": {
                    "job_id": "job-cpa",
                    "status": "failed",
                    "errors": [{"name": "owner@example.com", "error": SECRET}],
                },
            }],
        ),
        encoding="utf-8",
    )
    sub2api_path = tmp_path / "sub2api_config.json"
    sub2api_path.write_text(
        json.dumps(
            [{
                "id": "server-1",
                "base_url": "https://sub2api.example.test",
                "import_job": {
                    "job_id": "job-sub2api",
                    "status": "failed",
                    "errors": [{"name": "owner@example.com", "error": SECRET}],
                },
            }],
        ),
        encoding="utf-8",
    )

    with (
        mock.patch.object(accounts_module, "require_admin_async", return_value={"role": "admin"}),
        mock.patch.object(accounts_module, "cpa_config", CPAConfig(cpa_path)),
        mock.patch.object(accounts_module, "sub2api_config", Sub2APIConfig(sub2api_path)),
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
