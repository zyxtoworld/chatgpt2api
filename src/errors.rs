use axum::{
    Json,
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde_json::json;
use std::borrow::Cow;

use super::{INVALID_AUTH, PUBLIC_SERVER_ERROR};

#[derive(Debug)]
pub(super) struct ApiError {
    pub(super) status: StatusCode,
    kind: &'static str,
    code: &'static str,
    message: Cow<'static, str>,
    retry_after: Option<&'static str>,
    python_detail: bool,
    detail_status: Option<u16>,
}

impl ApiError {
    pub(super) fn code(&self) -> &'static str {
        self.code
    }

    pub(super) fn detail_status(&self) -> Option<u16> {
        self.detail_status
    }

    pub(super) fn unauthorized() -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            kind: "authentication_error",
            code: "invalid_api_key",
            message: Cow::Borrowed(INVALID_AUTH),
            retry_after: None,
            python_detail: false,
            detail_status: None,
        }
    }

    pub(super) fn management_unauthorized() -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            kind: "authentication_error",
            code: "invalid_api_key",
            message: Cow::Borrowed("密钥无效或已失效，请重新登录"),
            retry_after: None,
            python_detail: true,
            detail_status: None,
        }
    }

    pub(super) fn management_forbidden() -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            kind: "authorization_error",
            code: "admin_required",
            message: Cow::Borrowed("需要管理员权限才能执行这个操作"),
            retry_after: None,
            python_detail: true,
            detail_status: None,
        }
    }

    pub(super) fn settings_invalid_field() -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            kind: "invalid_request_error",
            code: "unsupported_field",
            message: Cow::Borrowed("配置更新包含不支持的字段"),
            retry_after: None,
            python_detail: true,
            detail_status: None,
        }
    }
    pub(super) fn invalid_request() -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            kind: "invalid_request_error",
            code: "bad_request",
            message: Cow::Borrowed("request validation failed"),
            retry_after: None,
            python_detail: false,
            detail_status: None,
        }
    }
    pub(super) fn not_found() -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            kind: "invalid_request_error",
            code: "not_found",
            message: Cow::Borrowed("The requested resource was not found."),
            retry_after: None,
            python_detail: false,
            detail_status: None,
        }
    }
    pub(super) fn unsupported_capability() -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            kind: "invalid_request_error",
            code: "unsupported_capability",
            message: Cow::Borrowed("The requested capability is not supported."),
            retry_after: None,
            python_detail: false,
            detail_status: None,
        }
    }

    pub(super) fn invalid_image_mask() -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            kind: "invalid_request_error",
            code: "invalid_image_mask",
            message: Cow::Borrowed("invalid image edit mask"),
            retry_after: None,
            python_detail: false,
            detail_status: None,
        }
    }

    pub(super) fn backup_busy() -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            kind: "invalid_request_error",
            code: "backup_busy",
            message: Cow::Borrowed("当前已有备份任务正在执行"),
            retry_after: None,
            python_detail: true,
            detail_status: None,
        }
    }

    pub(super) fn backup_delete_busy() -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            kind: "invalid_request_error",
            code: "backup_busy",
            message: Cow::Borrowed("当前备份正在写入该对象，请稍后再删除"),
            retry_after: None,
            python_detail: true,
            detail_status: None,
        }
    }

    pub(super) fn backup_state_invalid() -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            kind: "invalid_request_error",
            code: "backup_state_invalid",
            message: Cow::Borrowed("上一次备份状态无效，已停止重试"),
            retry_after: None,
            python_detail: true,
            detail_status: None,
        }
    }
    pub(super) fn validation() -> Self {
        Self {
            status: StatusCode::UNPROCESSABLE_ENTITY,
            kind: "invalid_request_error",
            code: "bad_request",
            message: Cow::Borrowed("request validation failed"),
            retry_after: None,
            python_detail: false,
            detail_status: None,
        }
    }
    pub(super) fn upstream() -> Self {
        Self {
            status: StatusCode::BAD_GATEWAY,
            kind: "server_error",
            code: "upstream_error",
            message: Cow::Borrowed(PUBLIC_SERVER_ERROR),
            retry_after: None,
            python_detail: false,
            detail_status: None,
        }
    }
    pub(super) fn unavailable() -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            kind: "server_error",
            code: "upstream_unavailable",
            message: Cow::Borrowed(PUBLIC_SERVER_ERROR),
            retry_after: None,
            python_detail: false,
            detail_status: None,
        }
    }
    pub(super) fn shutting_down() -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            kind: "server_error",
            code: "server_shutting_down",
            message: Cow::Borrowed("The server is shutting down. Please retry shortly."),
            retry_after: Some("1"),
            python_detail: false,
            detail_status: None,
        }
    }
    pub(super) fn model_unavailable() -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            kind: "invalid_request_error",
            code: "model_not_found",
            message: Cow::Borrowed("The requested model is not available."),
            retry_after: None,
            python_detail: false,
            detail_status: None,
        }
    }
    pub(super) fn catalog_pending() -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            kind: "server_error",
            code: "model_catalog_pending",
            message: Cow::Borrowed(
                "The model catalog is still warming up. Please try again shortly.",
            ),
            retry_after: Some("5"),
            python_detail: false,
            detail_status: None,
        }
    }

    pub(super) fn background_queue_full() -> Self {
        Self {
            status: StatusCode::TOO_MANY_REQUESTS,
            kind: "rate_limit_error",
            code: "rate_limit_exceeded",
            message: Cow::Borrowed("background task queue is full; try again later"),
            retry_after: Some("1"),
            python_detail: false,
            detail_status: None,
        }
    }

    pub(super) fn websocket_parts(&self) -> (&'static str, &'static str, &str) {
        (self.kind, self.code, self.message.as_ref())
    }

    pub(super) fn websocket_custom(
        kind: &'static str,
        code: &'static str,
        message: &'static str,
    ) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            kind,
            code,
            message: Cow::Borrowed(message),
            retry_after: None,
            python_detail: false,
            detail_status: None,
        }
    }

    pub(super) fn backup_r2_message(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            kind: "invalid_request_error",
            code,
            message: Cow::Owned(message.into()),
            retry_after: None,
            python_detail: true,
            detail_status: None,
        }
    }

    pub(super) fn backup_r2_status(code: &'static str, prefix: &'static str, status: u16) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            kind: "invalid_request_error",
            code,
            message: Cow::Owned(format!("{prefix}：HTTP {status}")),
            retry_after: None,
            python_detail: true,
            detail_status: Some(status),
        }
    }

    pub(super) fn backup_r2_config_incomplete(missing: String) -> Self {
        Self::backup_r2_message(
            "r2_config_incomplete",
            format!("R2 配置不完整：缺少 {missing}"),
        )
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
        if self.python_detail {
            let mut response = (
                self.status,
                Json(json!({"detail": {"error": self.message}})),
            )
                .into_response();
            if let Some(retry_after) = self.retry_after {
                response
                    .headers_mut()
                    .insert(header::RETRY_AFTER, HeaderValue::from_static(retry_after));
            }
            return response;
        }
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
