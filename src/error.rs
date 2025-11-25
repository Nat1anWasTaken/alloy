use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("Yrs decode error: {0}")]
    YrsDecode(#[from] yrs::encoding::read::Error),

    #[error("Environment variable error: {0}")]
    Env(#[from] std::env::VarError),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Address parse error: {0}")]
    AddrParse(#[from] std::net::AddrParseError),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, msg) = match self {
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
        };
        (status, msg).into_response()
    }
}
