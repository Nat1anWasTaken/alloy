use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Yrs decode error: {0}")]
    YrsDecode(#[from] yrs::encoding::read::Error),

    #[error("Environment variable error: {0}")]
    Env(#[from] std::env::VarError),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, msg) = match self {
            AppError::Database(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Database error: {}", e),
            ),
            AppError::YrsDecode(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Yrs error: {}", e),
            ),
            AppError::Env(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Env error: {}", e),
            ),
        };
        (status, msg).into_response()
    }
}
