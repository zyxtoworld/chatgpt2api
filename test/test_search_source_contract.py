from __future__ import annotations

from unittest import mock

import services.protocol.openai_search as openai_search
from services.protocol.web_search_tool import normalized_sources


def test_search_sources_only_publish_canonical_http_links() -> None:
    result = {
        "sources": [
            {"title": "valid", "url": " HTTPS://Example.Test/path?q=1#section ", "snippet": "ok"},
            {"title": "duplicate", "url": "https://example.test/path?q=1#other"},
            {"title": "data", "url": "data:text/html,<script>alert(1)</script>"},
            {"title": "javascript", "url": "javascript:alert(1)"},
            {"title": "relative", "url": "/settings"},
            {"title": "protocol relative", "url": "//example.test/path"},
            {"title": "userinfo", "url": "https://user:secret@example.test/path"},
            {"title": "backslash", "url": "https://example.test\\@attacker.test/path"},
            {"title": "bad port", "url": "https://example.test:bad/path"},
            {"title": "control", "url": "https://example.test/\nsecret"},
        ],
    }

    assert normalized_sources(result) == [
        {
            "title": "valid",
            "url": "https://example.test/path?q=1",
            "snippet": "ok",
        },
    ]


def test_search_sources_preserve_valid_ipv6_and_nondefault_ports() -> None:
    assert normalized_sources(
        {
            "sources": [
                {"title": "ipv6", "url": "http://[2001:db8::1]:8080/a?b=c#fragment"},
            ],
        },
    ) == [
        {
            "title": "ipv6",
            "url": "http://[2001:db8::1]:8080/a?b=c",
            "snippet": "",
        },
    ]


def test_direct_search_handler_projects_backend_sources_before_public_response() -> None:
    backend = mock.Mock()
    backend.search.return_value = {
        "answer": "answer",
        "sources": [
            {"title": "unsafe", "url": "data:text/html,secret"},
            {"title": "safe", "url": "https://example.test/result#section"},
        ],
    }
    with (
        mock.patch.object(openai_search.account_service, "get_text_access_token", return_value="fixture-token"),
        mock.patch.object(openai_search.account_service, "get_account", return_value={}),
        mock.patch.object(openai_search.account_service, "mark_text_used") as mark_used,
        mock.patch.object(openai_search, "OpenAIBackendAPI", return_value=backend),
    ):
        result = openai_search.handle({"prompt": "fixture query"})

    assert result["sources"] == [
        {"title": "safe", "url": "https://example.test/result", "snippet": ""},
    ]
    backend.close.assert_called_once_with()
    mark_used.assert_called_once_with("fixture-token")
