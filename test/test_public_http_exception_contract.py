from __future__ import annotations

from fastapi import FastAPI, HTTPException
from fastapi.exceptions import RequestValidationError
from fastapi.testclient import TestClient

from api.errors import install_exception_handlers


SECRET = "http-exception-upstream-secret owner@example.com"


def test_openai_http_exception_does_not_reflect_unknown_message_detail() -> None:
    app = FastAPI()
    install_exception_handlers(app)

    @app.get("/v1/opaque-message")
    async def opaque_message_error():
        raise HTTPException(status_code=400, detail={"message": SECRET})

    response = TestClient(app).get("/v1/opaque-message")

    assert response.status_code == 400
    assert SECRET not in response.text
    assert response.json()["error"]["message"] == "request failed"


def test_openai_http_exception_does_not_reflect_untrusted_detail() -> None:
    app = FastAPI()
    install_exception_handlers(app)

    @app.get("/v1/opaque")
    async def opaque_error():
        raise HTTPException(
            status_code=400,
            detail={
                "error": {
                    "message": SECRET,
                    "type": "upstream_error",
                    "param": SECRET,
                    "code": SECRET,
                }
            },
        )

    response = TestClient(app).get("/v1/opaque")

    assert response.status_code == 400
    assert SECRET not in response.text


def test_openai_validation_error_does_not_stringify_container_metadata() -> None:
    app = FastAPI()
    install_exception_handlers(app)
    canary = "validation-container-canary owner@example.com"

    @app.get("/v1/validation-container")
    async def validation_container_error():
        raise RequestValidationError([{
            "loc": ("body", "prompt"),
            "type": {"secret": canary},
            "msg": {"secret": canary},
        }])

    response = TestClient(app).get("/v1/validation-container")

    assert response.status_code == 422
    assert canary not in response.text
    error = response.json()["error"]
    assert error["type"] == "invalid_request_error"
    assert error["message"] == "request validation failed"


def test_management_validation_error_does_not_reflect_request_input() -> None:
    app = FastAPI()
    install_exception_handlers(app)

    @app.get("/api/validation")
    async def validation_error():
        raise RequestValidationError([{
            "loc": ("body", "access_token"),
            "type": "string_type",
            "msg": "Input should be a valid string",
            "input": SECRET,
            "ctx": {"canary": SECRET},
        }])

    response = TestClient(app).get("/api/validation")

    assert response.status_code == 422
    assert SECRET not in response.text
    assert response.json() == {
        "detail": [{
            "type": "string_type",
            "loc": [],
            "msg": "request validation failed",
        }],
    }


def test_openai_http_exception_list_detail_does_not_reflect_arbitrary_message() -> None:
    app = FastAPI()
    install_exception_handlers(app)

    @app.get("/v1/list-detail")
    async def list_detail_error():
        raise HTTPException(
            status_code=400,
            detail=[{"loc": ["body"], "msg": "opaque-list-secret owner@example.com"}],
        )

    response = TestClient(app).get("/v1/list-detail")

    assert response.status_code == 400
    assert "opaque-list-secret" not in response.text
    assert response.json()["error"]["message"] == "request failed"
