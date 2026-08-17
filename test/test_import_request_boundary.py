import unittest
from unittest import mock

from fastapi import FastAPI
from fastapi.testclient import TestClient

import api.accounts as accounts_module


AUTH_HEADERS = {"Authorization": "Bearer chatgpt2api"}


class ImportRequestBoundaryTests(unittest.TestCase):
    def setUp(self) -> None:
        app = FastAPI()
        app.include_router(accounts_module.create_router())
        self.client = TestClient(app)

    def test_import_routes_reject_more_than_service_batch_limit_before_start(self) -> None:
        with (
            mock.patch.object(accounts_module, "require_admin_async", return_value={"role": "admin"}),
            mock.patch.object(accounts_module.cpa_config, "get_pool", return_value={"id": "pool-1"}),
            mock.patch.object(accounts_module.sub2api_config, "get_server", return_value={"id": "server-1"}),
            mock.patch.object(accounts_module.ccload_config, "get_server", return_value={"id": "server-1"}),
            mock.patch.object(accounts_module.cpa_import_service, "start_import", return_value={}) as cpa_start,
            mock.patch.object(accounts_module.sub2api_import_service, "start_import", return_value={}) as sub2api_start,
            mock.patch.object(accounts_module.ccload_import_service, "start_import", return_value={}) as ccload_start,
        ):
            responses = [
                self.client.post(
                    "/api/cpa/pools/pool-1/import",
                    headers=AUTH_HEADERS,
                    json={
                        "names": [
                            f"file-{index}"
                            for index in range(accounts_module.CPA_MAX_REMOTE_FILES + 1)
                        ],
                    },
                ),
                self.client.post(
                    "/api/sub2api/servers/server-1/import",
                    headers=AUTH_HEADERS,
                    json={
                        "account_ids": [
                            f"account-{index}"
                            for index in range(accounts_module.SUB2API_MAX_REMOTE_ITEMS + 1)
                        ],
                    },
                ),
                self.client.post(
                    "/api/ccload/servers/server-1/import",
                    headers=AUTH_HEADERS,
                    json={
                        "channel_ids": [
                            str(index + 1)
                            for index in range(accounts_module.CCLOAD_MAX_CHANNELS + 1)
                        ],
                    },
                ),
            ]

        for response in responses:
            with self.subTest(path=str(response.request.url)):
                self.assertEqual(response.status_code, 422, response.text)
        cpa_start.assert_not_called()
        sub2api_start.assert_not_called()
        ccload_start.assert_not_called()


if __name__ == "__main__":
    unittest.main()
