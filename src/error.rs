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

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Address parse error: {0}")]
    AddrParse(#[from] std::net::AddrParseError),

    // Yrs observation failure indicates a critical logic error in document setup
    #[error("Failed to observe document updates: {0}")]
    YrsObserve(String),
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
            AppError::Io(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("IO error: {}", e),
            ),
            AppError::AddrParse(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Address parse error: {}", e),
            ),
            AppError::YrsObserve(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Document observation error: {}", e),
            ),
        };
        (status, msg).into_response()
    }
}
