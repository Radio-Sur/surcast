use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

/// Machine-readable error categories. One per icon/color on the frontend.
/// Kept generic on purpose: the UI maps category -> presentation, `code`
/// pinpoints the case, `message` stays human-readable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCategory {
    Auth,
    Validation,
    NotFound,
    Conflict,
    Stream,
    Icecast,
    Server,
}

impl ErrorCategory {
    pub fn as_str(self) -> &'static str {
        match self {
            ErrorCategory::Auth => "auth",
            ErrorCategory::Validation => "validation",
            ErrorCategory::NotFound => "not_found",
            ErrorCategory::Conflict => "conflict",
            ErrorCategory::Stream => "stream",
            ErrorCategory::Icecast => "icecast",
            ErrorCategory::Server => "server",
        }
    }
}

#[derive(Debug)]
pub enum AppError {
    Unauthorized {
        message: String,
        code: &'static str,
    },
    Forbidden {
        message: String,
        code: &'static str,
    },
    BadRequest {
        message: String,
        code: &'static str,
    },
    NotFound {
        message: String,
        code: &'static str,
    },
    Conflict {
        message: String,
        code: &'static str,
    },
    Stream {
        message: String,
        code: &'static str,
        /// Sanitized cause detail for the client (pipeline reason, blocking
        /// elements). Never secrets: no tokens, passwords, or DB internals.
        details: Option<String>,
    },
    Icecast {
        message: String,
        code: &'static str,
    },
    Internal {
        message: String,
        code: &'static str,
    },
}

impl AppError {
    pub fn unauthorized(message: String) -> Self {
        Self::Unauthorized {
            message,
            code: "UNAUTHORIZED",
        }
    }

    pub fn forbidden(message: String) -> Self {
        Self::Forbidden {
            message,
            code: "FORBIDDEN",
        }
    }

    pub fn bad_request(message: String) -> Self {
        Self::BadRequest {
            message,
            code: "BAD_REQUEST",
        }
    }

    pub fn not_found(message: String) -> Self {
        Self::NotFound {
            message,
            code: "NOT_FOUND",
        }
    }

    pub fn conflict(message: String) -> Self {
        Self::Conflict { message, code: "CONFLICT" }
    }

    pub fn internal(message: String) -> Self {
        Self::Internal { message, code: "INTERNAL" }
    }

    pub fn stream(message: String) -> Self {
        Self::Stream {
            message,
            code: "STREAM_ERROR",
            details: None,
        }
    }

    /// Attaches a sanitized cause detail (shown to the client). Callers must
    /// only pass data free of secrets (pipeline reasons and element states
    /// qualify; tokens, passwords and DB internals do not).
    pub fn with_details(mut self, details: String) -> Self {
        if let Self::Stream { details: slot, .. } = &mut self {
            *slot = Some(details);
        }
        self
    }

    pub fn icecast(message: String) -> Self {
        Self::Icecast {
            message,
            code: "ICECAST_ERROR",
        }
    }

    /// Maps a pipeline failure to a categorized stream error, keeping the
    /// caller-provided human message but choosing the code from the cause.
    pub(crate) fn from_pipeline(error: &crate::streamer::pipeline::PipelineError, message: String) -> Self {
        use crate::streamer::pipeline::PipelineError as PE;
        let code = match error {
            PE::MissingElement(_) | PE::Initialization(_) => "STREAM_INIT_FAILED",
            PE::InvalidTarget(_) | PE::InvalidTransitionMode(_) => "STREAM_CONFIG_FAILED",
            PE::StalePlan => "STREAM_STALE_PLAN",
            PE::Pipeline(_) => "STREAM_PLAYBACK_FAILED",
        };
        Self::Stream {
            message,
            code,
            details: Some(error.to_string()),
        }
    }

    /// Override the generic code, e.g. stream/icecast specifics.
    pub fn with_code(self, code: &'static str) -> Self {
        match self {
            Self::Unauthorized { message, .. } => Self::Unauthorized { message, code },
            Self::Forbidden { message, .. } => Self::Forbidden { message, code },
            Self::BadRequest { message, .. } => Self::BadRequest { message, code },
            Self::NotFound { message, .. } => Self::NotFound { message, code },
            Self::Conflict { message, .. } => Self::Conflict { message, code },
            Self::Stream { message, details, .. } => Self::Stream { message, code, details },
            Self::Icecast { message, .. } => Self::Icecast { message, code },
            Self::Internal { message, .. } => Self::Internal { message, code },
        }
    }

    fn status(&self) -> StatusCode {
        match self {
            Self::Unauthorized { .. } => StatusCode::UNAUTHORIZED,
            Self::Forbidden { .. } => StatusCode::FORBIDDEN,
            Self::BadRequest { .. } => StatusCode::BAD_REQUEST,
            Self::NotFound { .. } => StatusCode::NOT_FOUND,
            Self::Conflict { .. } => StatusCode::CONFLICT,
            Self::Stream { .. } | Self::Icecast { .. } | Self::Internal { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn category(&self) -> ErrorCategory {
        match self {
            Self::Unauthorized { .. } | Self::Forbidden { .. } => ErrorCategory::Auth,
            Self::BadRequest { .. } => ErrorCategory::Validation,
            Self::NotFound { .. } => ErrorCategory::NotFound,
            Self::Conflict { .. } => ErrorCategory::Conflict,
            Self::Stream { .. } => ErrorCategory::Stream,
            Self::Icecast { .. } => ErrorCategory::Icecast,
            Self::Internal { .. } => ErrorCategory::Server,
        }
    }

    fn message(&self) -> &str {
        match self {
            Self::Unauthorized { message, .. }
            | Self::Forbidden { message, .. }
            | Self::BadRequest { message, .. }
            | Self::NotFound { message, .. }
            | Self::Conflict { message, .. }
            | Self::Stream { message, .. }
            | Self::Icecast { message, .. }
            | Self::Internal { message, .. } => message,
        }
    }

    fn code(&self) -> &'static str {
        match self {
            Self::Unauthorized { code, .. }
            | Self::Forbidden { code, .. }
            | Self::BadRequest { code, .. }
            | Self::NotFound { code, .. }
            | Self::Conflict { code, .. }
            | Self::Stream { code, .. }
            | Self::Icecast { code, .. }
            | Self::Internal { code, .. } => code,
        }
    }
}

impl std::fmt::Display for AppError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}", self.code(), self.message())
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let status = self.status();
        // Never leak server internals: Stream carries only the friendly
        // message plus a code; the cause stays in server logs.
        let message = match &self {
            Self::Internal { .. } => "Internal server error",
            _ => self.message(),
        };
        let mut body = json!({
            "error": message,
            "code": self.code(),
            "category": self.category().as_str(),
        });
        if let Self::Stream {
            details: Some(details), ..
        } = &self
        {
            body["details"] = json!(details);
        }
        (status, Json(body)).into_response()
    }
}

pub trait DbResult<T> {
    fn db_error(self, ctx: &str) -> Result<T, AppError>;
}

impl<T, E: std::fmt::Display> DbResult<T> for Result<T, E> {
    fn db_error(self, ctx: &str) -> Result<T, AppError> {
        self.map_err(|e| {
            tracing::error!("{ctx}: {e}");
            AppError::internal("Database operation failed".to_string()).with_code("DB_ERROR")
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn response_body(error: AppError) -> (StatusCode, serde_json::Value) {
        let response = error.into_response();
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), 1024 * 64).await.unwrap();
        (status, serde_json::from_slice(&body).unwrap())
    }

    #[test]
    fn test_db_result_ok() {
        let result: Result<i32, &str> = Ok(42);
        let res = result.db_error("my context");
        assert_eq!(res.unwrap(), 42);
    }

    #[test]
    fn test_db_result_err_type() {
        let result: Result<i32, &str> = Err("db failure");
        let res = result.db_error("my context");
        match res {
            Err(AppError::Internal { .. }) => {}
            _ => panic!("Expected AppError::Internal"),
        }
    }

    #[tokio::test]
    async fn test_envelope_shape_per_variant() {
        for (error, status, code, category) in [
            (
                AppError::unauthorized("nope".to_string()),
                StatusCode::UNAUTHORIZED,
                "UNAUTHORIZED",
                "auth",
            ),
            (AppError::forbidden("nope".to_string()), StatusCode::FORBIDDEN, "FORBIDDEN", "auth"),
            (
                AppError::bad_request("nope".to_string()),
                StatusCode::BAD_REQUEST,
                "BAD_REQUEST",
                "validation",
            ),
            (
                AppError::not_found("nope".to_string()),
                StatusCode::NOT_FOUND,
                "NOT_FOUND",
                "not_found",
            ),
            (AppError::conflict("nope".to_string()), StatusCode::CONFLICT, "CONFLICT", "conflict"),
            (
                AppError::internal("boom".to_string()),
                StatusCode::INTERNAL_SERVER_ERROR,
                "INTERNAL",
                "server",
            ),
            (
                AppError::stream("stall".to_string()),
                StatusCode::INTERNAL_SERVER_ERROR,
                "STREAM_ERROR",
                "stream",
            ),
        ] {
            let (actual_status, body) = response_body(error).await;
            assert_eq!(actual_status, status);
            assert_eq!(body["code"], code);
            assert_eq!(body["category"], category);
            assert!(body["error"].is_string());
        }
    }

    #[tokio::test]
    async fn test_internal_message_is_redacted_but_code_survives() {
        let (_, body) = response_body(AppError::internal("secret db password".to_string()).with_code("DB_ERROR")).await;
        assert_eq!(body["error"], "Internal server error");
        assert_eq!(body["code"], "DB_ERROR");
        assert_eq!(body["category"], "server");
    }

    #[tokio::test]
    async fn test_with_code_override() {
        let (_, body) = response_body(AppError::internal("x".to_string()).with_code("STREAM_PLAYBACK_FAILED")).await;
        assert_eq!(body["code"], "STREAM_PLAYBACK_FAILED");
    }

    #[tokio::test]
    async fn test_stream_message_is_not_redacted() {
        let (_, body) = response_body(AppError::stream("Stream playback failed".to_string())).await;
        assert_eq!(body["error"], "Stream playback failed");
        assert_eq!(body["category"], "stream");
        assert!(body.get("details").is_none());
    }

    #[tokio::test]
    async fn test_stream_details_reach_the_client() {
        let (_, body) =
            response_body(AppError::stream("Stream playback failed".to_string()).with_details("stalled at Paused".to_string())).await;
        assert_eq!(body["details"], "stalled at Paused");
    }

    #[test]
    fn test_from_pipeline_picks_code_from_cause() {
        use crate::streamer::pipeline::PipelineError;
        let cases = [
            (PipelineError::MissingElement("x"), "STREAM_INIT_FAILED"),
            (PipelineError::Initialization("x".into()), "STREAM_INIT_FAILED"),
            (PipelineError::InvalidTarget("x".into()), "STREAM_CONFIG_FAILED"),
            (PipelineError::InvalidTransitionMode("x".into()), "STREAM_CONFIG_FAILED"),
            (PipelineError::StalePlan, "STREAM_STALE_PLAN"),
            (PipelineError::Pipeline("x".into()), "STREAM_PLAYBACK_FAILED"),
        ];
        for (cause, code) in cases {
            match AppError::from_pipeline(&cause, "friendly".to_string()) {
                AppError::Stream {
                    message,
                    code: actual,
                    details,
                } => {
                    assert_eq!(message, "friendly");
                    assert_eq!(actual, code);
                    assert!(details.is_some(), "pipeline cause must reach the client");
                }
                other => panic!("expected Stream, got {other:?}"),
            }
        }
    }
}
