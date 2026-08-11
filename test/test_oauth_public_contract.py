from __future__ import annotations

import io
import unittest
from contextlib import redirect_stdout
from unittest import mock

from fastapi import FastAPI
from fastapi.testclient import TestClient

import api.accounts as accounts_module
from services.oauth_login_service import OAuthLoginError


AUTH_HEADERS = {"Authorization": "Bearer chatgpt2api"}
CALLBACK_CODE = "oauth-code-secret"
CALLBACK_STATE = "oauth-state-secret"


class OAuthPublicContractTests(unittest.TestCase):
    def test_finish_does_not_log_callback_code_or_state(self) -> None:
        app = FastAPI()
        app.include_router(accounts_module.create_router())
        client = TestClient(app)
        callback = f"https://platform.openai.com/callback?code={CALLBACK_CODE}&state={CALLBACK_STATE}"
        output = io.StringIO()

        with (
            mock.patch.object(
                accounts_module.oauth_login_service,
                "finish",
                side_effect=OAuthLoginError("oauth rejected"),
            ),
            redirect_stdout(output),
        ):
            response = client.post(
                "/api/accounts/oauth/finish",
                headers=AUTH_HEADERS,
                json={"session_id": "session-secret", "callback": callback},
            )

        self.assertEqual(response.status_code, 400, response.text)
        self.assertNotIn(CALLBACK_CODE, output.getvalue())
        self.assertNotIn(CALLBACK_STATE, output.getvalue())
        self.assertNotIn(CALLBACK_CODE, response.text)
        self.assertNotIn(CALLBACK_STATE, response.text)


if __name__ == "__main__":
    unittest.main()
