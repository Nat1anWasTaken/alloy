use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use thiserror::Error;

use crate::persistence::IdError;

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

    #[error("Store error: {0}")]
    Store(String),

    #[error("Id error: {0}")]
    Id(#[from] IdError),

    #[error("authentication error: {0}")]
    Auth(String),

    #[error("invalid ticket: {0}")]
    InvalidTicket(String),

    #[error("invalid input: {0}")]
    InvalidInput(String),
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
            AppError::Store(e) => (StatusCode::INTERNAL_SERVER_ERROR, e),
            AppError::Id(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Id generation error: {}", e),
            ),
            AppError::Auth(e) | AppError::InvalidTicket(e) => {
                (StatusCode::UNAUTHORIZED, format!("auth error: {}", e))
            }
            AppError::InvalidInput(e) => (StatusCode::BAD_REQUEST, e),
        };
        (status, msg).into_response()
    }
}

impl From<jsonwebtoken::errors::Error> for AppError {
    fn from(value: jsonwebtoken::errors::Error) -> Self {
        AppError::Auth(value.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    #[test]
    fn test_io_error_into_response() {
        let io_err = io::Error::new(io::ErrorKind::NotFound, "file not found");
        let app_err = AppError::Io(io_err);

        let response = app_err.into_response();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn test_addr_parse_error_into_response() {
        let parse_err = "not_an_ip:123".parse::<std::net::SocketAddr>();
        assert!(parse_err.is_err());
        let err = match parse_err {
            Ok(_) => return,
            Err(err) => err,
        };
        let app_err = AppError::AddrParse(err);

        let response = app_err.into_response();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn test_env_error_into_response() {
        let env_err = std::env::var("NONEXISTENT_VAR_12345");
        assert!(env_err.is_err());
        let err = match env_err {
            Ok(_) => return,
            Err(err) => err,
        };
        let app_err = AppError::Env(err);

        let response = app_err.into_response();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn test_error_display_formatting() {
        let io_err = io::Error::new(io::ErrorKind::NotFound, "test file");
        let app_err = AppError::Io(io_err);

        let display = format!("{}", app_err);
        assert!(display.contains("IO error"));
        assert!(display.contains("test file"));
    }

    #[test]
    fn test_addr_parse_error_display() {
        let parse_err = "invalid".parse::<std::net::SocketAddr>();
        assert!(parse_err.is_err());
        let app_err = match parse_err {
            Ok(_) => return,
            Err(err) => AppError::AddrParse(err),
        };

        let display = format!("{}", app_err);
        assert!(display.contains("Address parse error"));
    }

    #[test]
    fn test_error_debug_output() {
        let io_err = io::Error::new(io::ErrorKind::PermissionDenied, "access denied");
        let app_err = AppError::Io(io_err);

        let debug_output = format!("{:?}", app_err);
        assert!(debug_output.contains("Io"));
    }
}
