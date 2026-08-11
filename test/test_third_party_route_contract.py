from __future__ import annotations

import json
import unittest
from unittest import mock

from fastapi import FastAPI
from fastapi.testclient import TestClient

import api.system as system_module
from api.system import create_router


AUTH_HEADERS = {"Authorization": "Bearer chatgpt2api"}


class ThirdPartyRouteContractTests(unittest.TestCase):
    def test_third_party_settings_do_not_return_script_url(self) -> None:
        script_url = "javascript:alert('third-party-xss')"
        app = FastAPI()
        app.include_router(create_router("test"))
        with mock.patch.dict(
            system_module.config.data,
            {
                "third_party_apps": {
                    "infinite_canvas": {
                        "enabled": True,
                        "url": script_url,
                    },
                },
            },
        ):
            response = TestClient(app).get("/api/third-party-apps", headers=AUTH_HEADERS)

        self.assertEqual(response.status_code, 200, response.text)
        self.assertNotIn(script_url, json.dumps(response.json(), ensure_ascii=False))


if __name__ == "__main__":
    unittest.main()
