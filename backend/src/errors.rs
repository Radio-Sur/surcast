use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

#[derive(Debug)]
pub enum AppError {
    Unauthorized(String),
    BadRequest(String),
    NotFound(String),
    Conflict(String),
    #[allow(dead_code)]
    Internal(String),
}

impl std::fmt::Display for AppError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AppError::Unauthorized(msg)
            | AppError::BadRequest(msg)
            | AppError::NotFound(msg)
            | AppError::Conflict(msg)
            | AppError::Internal(msg) => write!(f, "{msg}"),
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            AppError::Unauthorized(msg) => (StatusCode::UNAUTHORIZED, msg),
            AppError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
            AppError::NotFound(msg) => (StatusCode::NOT_FOUND, msg),
            AppError::Conflict(msg) => (StatusCode::CONFLICT, msg),
            AppError::Internal(_) => (StatusCode::INTERNAL_SERVER_ERROR, "Internal server error".into()),
        };

        (status, Json(json!({ "error": message }))).into_response()
    }
}

pub trait DbResult<T> {
    fn db_error(self, ctx: &str) -> Result<T, AppError>;
}

impl<T, E: std::fmt::Display> DbResult<T> for Result<T, E> {
    fn db_error(self, ctx: &str) -> Result<T, AppError> {
        self.map_err(|e| {
            tracing::error!("{ctx}: {e}");
            AppError::Internal("Database operation failed".into())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
            Err(AppError::Internal(msg)) => assert_eq!(msg, "Database operation failed"),
            _ => panic!("Expected AppError::Internal"),
        }
    }
}
