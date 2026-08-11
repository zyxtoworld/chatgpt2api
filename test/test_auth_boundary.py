from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest import mock

from fastapi import HTTPException

import api.support as support


class AuthBoundaryTests(unittest.TestCase):
    def test_authorization_parser_requires_bearer_scheme_and_value(self) -> None:
        self.assertEqual(support.extract_bearer_token(None), "")
        self.assertEqual(support.extract_bearer_token(""), "")
        self.assertEqual(support.extract_bearer_token("Basic secret"), "")
        self.assertEqual(support.extract_bearer_token("Bearer    user-key  "), "user-key")
        self.assertEqual(support.extract_bearer_token("bearer user-key"), "user-key")

    def test_legacy_admin_key_uses_constant_time_comparison(self) -> None:
        with (
            mock.patch.object(support, "config", SimpleNamespace(auth_key="admin-secret")),
            mock.patch.object(support.hmac, "compare_digest", return_value=True) as compare_digest,
        ):
            identity = support._legacy_admin_identity("candidate")

        self.assertEqual(identity["role"], "admin")
        compare_digest.assert_called_once_with("candidate", "admin-secret")

    def test_invalid_authorization_is_401(self) -> None:
        with self.assertRaises(HTTPException) as raised:
            support.require_identity("Basic admin-secret")

        self.assertEqual(raised.exception.status_code, 401)


if __name__ == "__main__":
    unittest.main()
