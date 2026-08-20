use axum::{
    Json,
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde_json::json;

use super::{INVALID_AUTH, PUBLIC_SERVER_ERROR};

#[derive(Debug)]
pub(super) struct ApiError {
    pub(super) status: StatusCode,
    kind: &'static str,
    code: &'static str,
    message: &'static str,
    retry_after: Option<&'static str>,
}

impl ApiError {
    #[cfg(test)]
    pub(super) fn code(&self) -> &'static str {
        self.code
    }

    pub(super) fn unauthorized() -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            kind: "authentication_error",
            code: "invalid_api_key",
            message: INVALID_AUTH,
            retry_after: None,
        }
    }
    pub(super) fn invalid_request() -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            kind: "invalid_request_error",
            code: "bad_request",
            message: "request validation failed",
            retry_after: None,
        }
    }
    pub(super) fn unsupported_capability() -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            kind: "invalid_request_error",
            code: "unsupported_capability",
            message: "The requested capability is not supported.",
            retry_after: None,
        }
    }
    pub(super) fn validation() -> Self {
        Self {
            status: StatusCode::UNPROCESSABLE_ENTITY,
            kind: "invalid_request_error",
            code: "bad_request",
            message: "request validation failed",
            retry_after: None,
        }
    }
    pub(super) fn upstream() -> Self {
        Self {
            status: StatusCode::BAD_GATEWAY,
            kind: "server_error",
            code: "upstream_error",
            message: PUBLIC_SERVER_ERROR,
            retry_after: None,
        }
    }
    pub(super) fn unavailable() -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            kind: "server_error",
            code: "upstream_unavailable",
            message: PUBLIC_SERVER_ERROR,
            retry_after: None,
        }
    }
    pub(super) fn model_unavailable() -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            kind: "invalid_request_error",
            code: "model_not_found",
            message: "The requested model is not available.",
            retry_after: None,
        }
    }
    pub(super) fn catalog_pending() -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            kind: "server_error",
            code: "model_catalog_pending",
            message: "The model catalog is still warming up. Please try again shortly.",
            retry_after: Some("5"),
        }
    }

    pub(super) fn into_anthropic_response(self) -> Response {
        let mut response = (
            self.status,
            Json(json!({
                "type": "error",
                "error": {"type": self.kind, "message": self.message, "code": self.code}
            })),
        )
            .into_response();
        if let Some(retry_after) = self.retry_after {
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from_static(retry_after));
        }
        response
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut response = (
            self.status,
            Json(json!({ "error": { "message": self.message, "type": self.kind, "param": null, "code": self.code } })),
        ).into_response();
        if let Some(retry_after) = self.retry_after {
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from_static(retry_after));
        }
        response
    }
}
