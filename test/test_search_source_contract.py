from __future__ import annotations

import tempfile
import threading
from pathlib import Path
from unittest import mock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import api.ai as ai_module
import services.protocol.openai_search as openai_search
import services.protocol.web_search_tool as web_search_tool
from services.account_service import AccountService
from services.openai_backend_api import OpenAIBackendAPI
from services.protocol.web_search_tool import (
    message_text,
    normalized_sources,
    search_query_from_messages,
    text_with_url_citations,
)
from services.storage.json_storage import JSONStorageBackend


def test_search_result_containers_are_not_stringified_into_public_text() -> None:
    canary = "search-result-container-secret"
    result = {
        "answer": {"secret": canary},
        "sources": [{
            "title": [canary],
            "url": "https://example.test/result",
            "snippet": {"secret": canary},
        }],
    }

    text, annotations = text_with_url_citations(result)
    projected = normalized_sources(result)
    assert canary not in text
    assert canary not in repr(annotations)
    assert projected == [{
        "title": "",
        "url": "https://example.test/result",
        "snippet": "",
    }]


def test_search_message_text_does_not_stringify_container_parts() -> None:
    canary = "search-message-container-canary owner@example.com"
    messages = [{
        "role": "user",
        "content": [{"text": {"secret": canary}}, {"input_text": [canary]}],
    }]

    assert message_text(messages[0]["content"]) == ""
    assert search_query_from_messages(messages) == ""
    assert canary not in repr(message_text(messages[0]["content"]))


def test_backend_search_source_parser_does_not_stringify_container_fields() -> None:
    canary = "backend-search-source-container-canary"
    backend = object.__new__(OpenAIBackendAPI)
    sources = backend._extract_search_sources({
        "sources": [{
            "title": {"secret": canary},
            "url": "https://example.test/result",
            "snippet": [canary],
            "type": {"secret": canary},
        }],
    })

    projected = normalized_sources({"sources": sources})

    assert canary not in repr(sources)
    assert canary not in repr(projected)
    assert projected == [{
        "title": "",
        "url": "https://example.test/result",
        "snippet": "",
    }]


def test_backend_search_result_does_not_stringify_malformed_public_scalars() -> None:
    canary = "backend-search-result-scalar-canary"
    backend = object.__new__(OpenAIBackendAPI)

    result = backend._extract_search_result("conversation-1", {
        "mapping": {
            "node-1": {
                "message": {
                    "author": {"role": "assistant"},
                    "id": {"secret": canary},
                    "create_time": [canary],
                    "metadata": {"status": {"secret": canary}},
                    "content": {"text": "answer"},
                },
            },
        },
    })

    assert canary not in repr(result)
    assert result["status"] == ""
    assert result["assistant_message_id"] == ""
    assert result["create_time"] == 0.0


def test_backend_search_message_text_does_not_stringify_container_parts() -> None:
    canary = "backend-search-message-container-canary"
    backend = object.__new__(OpenAIBackendAPI)

    text = backend._search_message_text({
        "content": {
            "parts": [
                {"text": {"secret": canary}},
                {"summary": [canary]},
                {"content": "safe text"},
            ],
        },
    })

    assert text == "safe text"
    assert canary not in text


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


def test_search_sources_reject_malformed_percent_escapes() -> None:
    result = {
        "sources": [
            {"title": "bad", "url": "https://example.test/%ZZ"},
            {"title": "bare", "url": "https://example.test/%"},
            {"title": "valid", "url": "https://example.test/%E4%B8%AD"},
        ],
    }

    assert normalized_sources(result) == [{
        "title": "valid",
        "url": "https://example.test/%E4%B8%AD",
        "snippet": "",
    }]


def test_search_public_sources_are_bounded() -> None:
    sources = [
        {
            "title": f"title-{index}-" + ("x" * 5000),
            "url": f"https://example.test/source-{index}",
            "snippet": "s" * 5000,
        }
        for index in range(101)
    ]

    projected = normalized_sources({"sources": sources})

    assert len(projected) == 100
    assert all(len(item["title"]) <= 4096 for item in projected)
    assert all(len(item["snippet"]) <= 4096 for item in projected)


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


def test_search_usage_does_not_mutate_replaced_same_token_account() -> None:
    entered = threading.Event()
    release = threading.Event()

    class BlockingBackend:
        def search(self, _prompt: str) -> dict[str, object]:
            entered.set()
            if not release.wait(5):
                raise AssertionError("search did not receive release")
            return {"answer": "answer", "sources": []}

        def close(self) -> None:
            pass

    with tempfile.TemporaryDirectory() as temp_dir:
        service = AccountService(JSONStorageBackend(Path(temp_dir) / "accounts.json"))
        service.add_account_items([{"access_token": "search-token", "type": "free", "status": "正常"}])
        service.get_text_access_token = lambda **_kwargs: "search-token"
        result: list[dict[str, object]] = []
        errors: list[BaseException] = []

        def run_search() -> None:
            try:
                result.append(openai_search.handle({"prompt": "fixture query"}))
            except BaseException as exc:
                errors.append(exc)

        with (
            mock.patch.object(openai_search, "account_service", service),
            mock.patch.object(openai_search, "OpenAIBackendAPI", return_value=BlockingBackend()),
        ):
            worker = threading.Thread(target=run_search)
            worker.start()
            assert entered.wait(5)
            service.update_account(
                "search-token",
                {"last_used_at": "2000-01-01 00:00:00", "success": 99},
            )
            release.set()
            worker.join(5)

        assert not worker.is_alive()
        assert errors == []
        assert result == [{"answer": "answer", "sources": []}]
        current = service.get_account("search-token")
        assert current is not None
        assert current["last_used_at"] == "2000-01-01 00:00:00"
        assert current["success"] == 99


def test_search_lease_capture_failure_does_not_create_backend() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        service = AccountService(JSONStorageBackend(Path(temp_dir) / "accounts.json"))
        service.add_account_items([{"access_token": "search-token", "type": "free", "status": "正常"}])
        service.get_text_access_token = lambda **_kwargs: "search-token"
        with (
            mock.patch.object(
                service,
                "_get_account_lease",
                side_effect=RuntimeError("lease capture failed"),
            ),
            mock.patch.object(openai_search, "account_service", service),
            mock.patch.object(openai_search, "OpenAIBackendAPI") as backend,
            pytest.raises(RuntimeError, match="lease capture failed"),
        ):
            openai_search.handle({"prompt": "fixture query"})

        backend.assert_not_called()


def test_web_search_usage_does_not_mutate_replaced_same_token_account() -> None:
    entered = threading.Event()
    release = threading.Event()

    class BlockingBackend:
        def search(self, _query: str) -> dict[str, object]:
            entered.set()
            if not release.wait(5):
                raise AssertionError("web search did not receive release")
            return {"answer": "answer", "sources": []}

        def close(self) -> None:
            pass

    with tempfile.TemporaryDirectory() as temp_dir:
        service = AccountService(JSONStorageBackend(Path(temp_dir) / "accounts.json"))
        service.add_account_items([{"access_token": "web-search-token", "type": "free", "status": "正常"}])
        service.get_text_access_token = lambda **_kwargs: "web-search-token"
        result: list[dict[str, object]] = []
        errors: list[BaseException] = []

        def run_search() -> None:
            try:
                result.append(web_search_tool.run_web_search("fixture query"))
            except BaseException as exc:
                errors.append(exc)

        with (
            mock.patch.object(web_search_tool, "account_service", service),
            mock.patch.object(web_search_tool, "OpenAIBackendAPI", return_value=BlockingBackend()),
        ):
            worker = threading.Thread(target=run_search)
            worker.start()
            assert entered.wait(5)
            service.update_account(
                "web-search-token",
                {"last_used_at": "2000-01-01 00:00:00", "success": 99},
            )
            release.set()
            worker.join(5)

        assert not worker.is_alive()
        assert errors == []
        assert result == [{"answer": "answer", "sources": []}]
        current = service.get_account("web-search-token")
        assert current is not None
        assert current["last_used_at"] == "2000-01-01 00:00:00"
        assert current["success"] == 99


def test_web_search_lease_capture_failure_does_not_create_backend() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        service = AccountService(JSONStorageBackend(Path(temp_dir) / "accounts.json"))
        service.add_account_items([{"access_token": "web-search-token", "type": "free", "status": "正常"}])
        service.get_text_access_token = lambda **_kwargs: "web-search-token"
        with (
            mock.patch.object(
                service,
                "_get_account_lease",
                side_effect=RuntimeError("lease capture failed"),
            ),
            mock.patch.object(web_search_tool, "account_service", service),
            mock.patch.object(web_search_tool, "OpenAIBackendAPI") as backend,
            pytest.raises(RuntimeError, match="lease capture failed"),
        ):
            web_search_tool.run_web_search("fixture query")

        backend.assert_not_called()


def test_direct_search_handler_does_not_publish_account_email() -> None:
    account_email = "owner@example.test"
    backend = mock.Mock()
    backend.search.return_value = {"answer": "answer", "sources": []}
    with (
        mock.patch.object(openai_search.account_service, "get_text_access_token", return_value="fixture-token"),
        mock.patch.object(openai_search.account_service, "get_account", return_value={"email": account_email}),
        mock.patch.object(openai_search.account_service, "mark_text_used"),
        mock.patch.object(openai_search, "OpenAIBackendAPI", return_value=backend),
    ):
        result = openai_search.handle({"prompt": "fixture query"})

    assert "_account_email" not in result
    assert account_email not in repr(result)


def test_direct_search_handler_drops_unknown_backend_fields() -> None:
    canary = "search-upstream-field-canary owner@example.com"
    backend = mock.Mock()
    backend.search.return_value = {
        "conversation_id": "conversation-1",
        "status": "finished_successfully",
        "answer": "answer",
        "sources": [],
        "access_token": canary,
        "internal_metadata": {"nested": canary},
    }
    with (
        mock.patch.object(openai_search.account_service, "get_text_access_token", return_value="fixture-token"),
        mock.patch.object(openai_search.account_service, "mark_text_used"),
        mock.patch.object(openai_search, "OpenAIBackendAPI", return_value=backend),
    ):
        result = openai_search.handle({"prompt": "fixture query"})

    assert result == {
        "conversation_id": "conversation-1",
        "status": "finished_successfully",
        "answer": "answer",
        "sources": [],
    }
    assert canary not in repr(result)


def test_public_search_route_does_not_publish_account_email() -> None:
    account_email = "owner@example.test"
    app = FastAPI()
    app.include_router(ai_module.create_router())
    backend = mock.Mock()
    backend.search.return_value = {"answer": "answer", "sources": []}
    with (
        mock.patch.object(ai_module, "require_identity_async", new=mock.AsyncMock(return_value={"id": "user-1"})),
        mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
        mock.patch.object(openai_search.account_service, "get_text_access_token", return_value="fixture-token"),
        mock.patch.object(openai_search.account_service, "get_account", return_value={"email": account_email}),
        mock.patch.object(openai_search.account_service, "mark_text_used"),
        mock.patch.object(openai_search, "OpenAIBackendAPI", return_value=backend),
    ):
        response = TestClient(app).post(
            "/v1/search",
            headers={"Authorization": "Bearer fixture"},
            json={"prompt": "fixture query"},
        )

    assert response.status_code == 200, response.text
    assert account_email not in response.text
    assert "_account_email" not in response.json()
