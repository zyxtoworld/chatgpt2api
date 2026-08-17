from __future__ import annotations

import contextlib
import importlib.util
import io
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "verify_oauth_refresh.py"


def load_script_module():
    spec = importlib.util.spec_from_file_location("verify_oauth_refresh_under_test", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)
    return module


class FakeAccountService:
    old_token = "access-token-canary"
    new_token = "new-access-token-canary"

    def list_tokens(self):
        return [self.old_token]

    def get_account(self, token):
        if token == self.old_token:
            return {
                "email": "account@example.test",
                "refresh_token": "refresh-token-canary",
                "last_token_refresh_at": "timestamp-canary",
                "last_token_refresh_error": "error-canary",
            }
        return {
            "email": "account@example.test",
            "last_token_refresh_at": "timestamp-canary",
            "last_token_refresh_error": "error-canary",
        }

    def _token_expires_in(self, _token):
        return 3600

    def refresh_access_token(self, _token, *, force, event):
        assert force is True
        assert event == "manual_verify"
        return self.new_token


def test_diagnostics_never_print_raw_tokens_or_refresh_errors():
    module = load_script_module()
    module.account_service = FakeAccountService()
    output = io.StringIO()

    with contextlib.redirect_stdout(output):
        tokens = module.diagnose()
        module.force_refresh(tokens)

    rendered = output.getvalue()
    for canary in (
        FakeAccountService.old_token,
        FakeAccountService.new_token,
        "refresh-token-canary",
        "error-canary",
        "timestamp-canary",
    ):
        assert canary not in rendered
