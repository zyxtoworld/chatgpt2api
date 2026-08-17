from __future__ import annotations

import json
import unittest
from unittest import mock

import requests

from services.protocol import openai_v1_models


AUTH_KEY = "chatgpt2api"
BASE_URL = "http://localhost:8000"


class ModelListTests(unittest.TestCase):
    def test_list_models_only_returns_image_models_backed_by_account_types(self):
        with (
            mock.patch.object(
                openai_v1_models.model_catalog_service,
                "list_models",
                return_value={"object": "list", "data": []},
            ),
            mock.patch.object(
                openai_v1_models.account_service,
                "list_accounts",
                return_value=[
                    {"access_token": "token-free", "type": "free", "status": "正常", "quota": 1},
                    {
                        "access_token": "token-web-team",
                        "type": "Team",
                        "source_type": "web",
                        "status": "正常",
                        "quota": 1,
                    },
                    {
                        "access_token": "token-codex-team",
                        "type": "Team",
                        "source_type": "codex",
                        "status": "正常",
                        "quota": 1,
                    },
                ],
            ),
        ):
            result = openai_v1_models.list_models()

        ids = {item["id"] for item in result["data"]}
        self.assertIn("gpt-image-2", ids)
        self.assertIn("codex-gpt-image-2", ids)
        self.assertIn("team-codex-gpt-image-2", ids)
        self.assertNotIn("plus-codex-gpt-image-2", ids)
        self.assertNotIn("pro-codex-gpt-image-2", ids)
        by_id = {item["id"]: item for item in result["data"]}
        self.assertEqual(by_id["gpt-image-2"]["supported_account_types"], ["free", "team"])
        self.assertEqual(by_id["codex-gpt-image-2"]["supported_account_types"], ["team"])
        self.assertEqual(by_id["team-codex-gpt-image-2"]["supported_account_types"], ["team"])
        self.assertFalse(by_id["gpt-image-2"]["allow_anonymous"])

    def test_list_models_does_not_return_codex_models_for_web_plus_accounts(self):
        with (
            mock.patch.object(
                openai_v1_models.model_catalog_service,
                "list_models",
                return_value={"object": "list", "data": []},
            ),
            mock.patch.object(
                openai_v1_models.account_service,
                "list_accounts",
                return_value=[
                    {
                        "access_token": "token-web-plus",
                        "type": "Plus",
                        "source_type": "web",
                        "status": "正常",
                        "quota": 1,
                    },
                ],
            ),
        ):
            result = openai_v1_models.list_models()

        ids = {item["id"] for item in result["data"]}
        self.assertIn("gpt-image-2", ids)
        self.assertNotIn("codex-gpt-image-2", ids)
        self.assertNotIn("plus-codex-gpt-image-2", ids)

    def test_list_models_does_not_advertise_dynamic_images_for_disabled_accounts(self):
        with (
            mock.patch.object(
                openai_v1_models.model_catalog_service,
                "list_models",
                return_value={"object": "list", "data": []},
            ),
            mock.patch.object(
                openai_v1_models.account_service,
                "list_accounts",
                return_value=[
                    {"access_token": "disabled-web", "type": "free", "status": "禁用"},
                    {
                        "access_token": "disabled-codex",
                        "type": "Pro",
                        "source_type": "codex",
                        "status": "异常",
                    },
                ],
            ),
        ):
            result = openai_v1_models.list_models()

        ids = {item["id"] for item in result["data"]}
        self.assertNotIn("gpt-image-2", ids)
        self.assertNotIn("codex-gpt-image-2", ids)
        self.assertNotIn("pro-codex-gpt-image-2", ids)

    def test_list_models_removes_catalog_image_models_without_available_accounts(self):
        catalog_models = [
            {"id": model_id, "object": "model"}
            for model_id in (
                "gpt-image-2",
                "codex-gpt-image-2",
                "plus-codex-gpt-image-2",
                "team-codex-gpt-image-2",
                "pro-codex-gpt-image-2",
            )
        ]
        with (
            mock.patch.object(
                openai_v1_models.model_catalog_service,
                "list_models",
                return_value={"object": "list", "data": catalog_models},
            ),
            mock.patch.object(
                openai_v1_models.account_service,
                "list_accounts",
                return_value=[],
            ),
        ):
            result = openai_v1_models.list_models()

        self.assertEqual(result["data"], [])

    def test_list_models_does_not_expose_unknown_image_catalog_models(self):
        catalog_models = [
            {"id": "gpt-image-1", "object": "model"},
            {"id": "gpt-5", "object": "model"},
        ]
        with (
            mock.patch.object(
                openai_v1_models.model_catalog_service,
                "list_models",
                return_value={"object": "list", "data": catalog_models},
            ),
            mock.patch.object(
                openai_v1_models.account_service,
                "list_accounts",
                return_value=[
                    {"access_token": "healthy-web", "type": "free", "status": "正常", "quota": 1},
                ],
            ),
        ):
            result = openai_v1_models.list_models()

        ids = {item["id"] for item in result["data"]}
        self.assertIn("gpt-5", ids)
        self.assertIn("gpt-image-2", ids)
        self.assertNotIn("gpt-image-1", ids)

    def test_list_models_rewrites_existing_image_metadata_from_local_capabilities(self):
        with (
            mock.patch.object(
                openai_v1_models.model_catalog_service,
                "list_models",
                return_value={
                    "object": "list",
                    "data": [{
                        "id": "gpt-image-2",
                        "object": "model",
                        "allow_anonymous": True,
                        "supported_account_types": ["pro"],
                    }],
                },
            ),
            mock.patch.object(
                openai_v1_models.account_service,
                "list_accounts",
                return_value=[
                    {"access_token": "healthy-free", "type": "free", "status": "正常", "quota": 1},
                ],
            ),
        ):
            result = openai_v1_models.list_models()

        image_model = next(item for item in result["data"] if item["id"] == "gpt-image-2")
        self.assertEqual(image_model["supported_account_types"], ["free"])
        self.assertFalse(image_model["allow_anonymous"])

    def test_list_models_matches_image_account_availability_contract(self):
        cases = [
            (
                "limited-web",
                [{"access_token": "limited-web", "type": "free", "status": "限流", "quota": 5}],
                False,
                False,
            ),
            (
                "zero-quota",
                [{"access_token": "zero-quota", "type": "free", "status": "正常", "quota": 0}],
                False,
                False,
            ),
            (
                "missing-quota",
                [{"access_token": "missing-quota", "type": "free", "status": "正常"}],
                False,
                False,
            ),
            (
                "string-quota",
                [{"access_token": "string-quota", "type": "free", "status": "正常", "quota": "5"}],
                False,
                False,
            ),
            (
                "healthy-web",
                [{"access_token": "healthy-web", "type": "free", "status": "正常", "quota": 5}],
                True,
                False,
            ),
            (
                "limited-codex",
                [{"access_token": "limited-codex", "type": "Pro", "source_type": "codex", "status": "限流", "quota": 5}],
                False,
                False,
            ),
            (
                "healthy-codex",
                [{"access_token": "healthy-codex", "type": "Pro", "source_type": "codex", "status": "正常", "quota": 5}],
                True,
                True,
            ),
        ]

        for name, accounts, has_web_image, has_codex_image in cases:
            with self.subTest(name=name), mock.patch.object(
                openai_v1_models.model_catalog_service,
                "list_models",
                return_value={"object": "list", "data": []},
            ), mock.patch.object(
                openai_v1_models.account_service,
                "list_accounts",
                return_value=accounts,
            ):
                result = openai_v1_models.list_models()

            ids = {item["id"] for item in result["data"]}
            self.assertEqual("gpt-image-2" in ids, has_web_image)
            self.assertEqual("pro-codex-gpt-image-2" in ids, has_codex_image)

    def test_list_models_function(self):
        """测试直接调用服务层获取模型列表。"""
        with (
            mock.patch.object(
                openai_v1_models.model_catalog_service,
                "list_models",
                return_value={"object": "list", "data": []},
            ),
            mock.patch.object(openai_v1_models.account_service, "list_accounts", return_value=[]),
        ):
            result = openai_v1_models.list_models()
        self.assertEqual(result, {"object": "list", "data": []})

    def test_list_models_http(self):
        """测试通过 HTTP 接口获取模型列表。"""
        response = requests.get(
            f"{BASE_URL}/v1/models",
            headers={"Authorization": f"Bearer {AUTH_KEY}"},
            timeout=30,
        )
        print("http status:")
        print(response.status_code)
        print("http result:")
        print(json.dumps(response.json(), ensure_ascii=False, indent=2))
