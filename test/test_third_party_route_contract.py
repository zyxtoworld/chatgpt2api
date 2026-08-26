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
    def test_third_party_settings_distinguish_missing_url_from_invalid_present_url(self) -> None:
        app = FastAPI()
        app.include_router(create_router("test"))
        cases = [
            ({"enabled": True}, {"enabled": True, "url": "https://canvas.best"}),
            ({"enabled": True, "url": ""}, {"enabled": False, "url": ""}),
            ({"enabled": True, "url": 123}, {"enabled": False, "url": ""}),
            ({"enabled": True, "url": "javascript:alert(1)"}, {"enabled": False, "url": ""}),
        ]
        for canvas_input, expected in cases:
            with self.subTest(canvas_input=canvas_input):
                with mock.patch.dict(
                    system_module.config.data,
                    {
                        "third_party_apps": {
                            "infinite_canvas": canvas_input,
                            "private_canary": "must-not-project",
                        },
                    },
                ):
                    response = TestClient(app).get("/api/third-party-apps", headers=AUTH_HEADERS)

                self.assertEqual(response.status_code, 200, response.text)
                self.assertEqual(
                    response.json(),
                    {"third_party_apps": {"infinite_canvas": expected}},
                )
                self.assertNotIn("private_canary", response.text)

    def test_third_party_settings_reject_url_containers_without_stringifying(self) -> None:
        canary = "third-party-url-container-canary"
        coercions = 0

        class HostileURL:
            def __str__(self) -> str:
                nonlocal coercions
                coercions += 1
                return f"https://example.test/{canary}"

        app = FastAPI()
        app.include_router(create_router("test"))
        with mock.patch.dict(
            system_module.config.data,
            {
                "third_party_apps": {
                    "infinite_canvas": {
                        "enabled": True,
                        "url": HostileURL(),
                    },
                },
            },
        ):
            response = TestClient(app).get("/api/third-party-apps", headers=AUTH_HEADERS)

        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(coercions, 0)
        self.assertNotIn(canary, response.text)
        self.assertEqual(response.json()["third_party_apps"]["infinite_canvas"]["url"], "")

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
