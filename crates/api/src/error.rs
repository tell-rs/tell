//! API error types
//!
//! Provides structured error responses for the HTTP API.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;
use thiserror::Error;

/// API errors
#[derive(Debug, Error)]
pub enum ApiError {
    /// Invalid request parameters
    #[error("bad request: {0}")]
    BadRequest(String),

    /// Authentication required
    #[error("authentication required")]
    Unauthorized,

    /// Permission denied
    #[error("permission denied: {0}")]
    Forbidden(String),

    /// Resource not found
    #[error("not found: {0}")]
    NotFound(String),

    /// Resource already exists
    #[error("conflict: {0} already exists")]
    Conflict(String),

    /// Validation error
    #[error("validation error: {field} - {message}")]
    Validation { field: String, message: String },

    /// Invalid time range
    #[error("invalid time range: {0}")]
    InvalidTimeRange(String),

    /// Invalid filter
    #[error("invalid filter: {0}")]
    InvalidFilter(String),

    /// Query execution failed
    #[error("query failed: {0}")]
    QueryFailed(String),

    /// Internal server error
    #[error("internal error: {0}")]
    Internal(String),

    /// Analytics error
    #[error(transparent)]
    Analytics(#[from] tell_analytics::AnalyticsError),

    /// Query error
    #[error(transparent)]
    Query(#[from] tell_query::QueryError),
}

impl ApiError {
    /// Get the HTTP status code for this error
    pub fn status_code(&self) -> StatusCode {
        match self {
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::Forbidden(_) => StatusCode::FORBIDDEN,
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::Conflict(_) => StatusCode::CONFLICT,
            Self::Validation { .. } => StatusCode::UNPROCESSABLE_ENTITY,
            Self::InvalidTimeRange(_) => StatusCode::BAD_REQUEST,
            Self::InvalidFilter(_) => StatusCode::BAD_REQUEST,
            Self::QueryFailed(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Analytics(_) => StatusCode::BAD_REQUEST,
            Self::Query(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// Get the error code for this error
    pub fn code(&self) -> &'static str {
        match self {
            Self::BadRequest(_) => "BAD_REQUEST",
            Self::Unauthorized => "UNAUTHORIZED",
            Self::Forbidden(_) => "FORBIDDEN",
            Self::NotFound(_) => "NOT_FOUND",
            Self::Conflict(_) => "CONFLICT",
            Self::Validation { .. } => "VALIDATION_ERROR",
            Self::InvalidTimeRange(_) => "INVALID_TIME_RANGE",
            Self::InvalidFilter(_) => "INVALID_FILTER",
            Self::QueryFailed(_) => "QUERY_FAILED",
            Self::Internal(_) => "INTERNAL_ERROR",
            Self::Analytics(_) => "ANALYTICS_ERROR",
            Self::Query(_) => "QUERY_ERROR",
        }
    }

    // Helper constructors

    /// Create an internal error
    pub fn internal(msg: impl Into<String>) -> Self {
        Self::Internal(msg.into())
    }

    /// Create a not found error
    pub fn not_found(entity: &str, id: &str) -> Self {
        Self::NotFound(format!("{} '{}' not found", entity, id))
    }

    /// Create a forbidden error
    pub fn forbidden(msg: impl Into<String>) -> Self {
        Self::Forbidden(msg.into())
    }

    /// Create a conflict error
    pub fn conflict(entity: &str, id: &str) -> Self {
        Self::Conflict(format!("{} '{}'", entity, id))
    }

    /// Create a validation error
    pub fn validation(field: &str, message: impl Into<String>) -> Self {
        Self::Validation {
            field: field.to_string(),
            message: message.into(),
        }
    }
}

/// Error response body
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    /// Error code (machine-readable)
    pub error: &'static str,
    /// Error message (human-readable)
    pub message: String,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = self.status_code();
        let body = ErrorResponse {
            error: self.code(),
            message: self.to_string(),
        };

        tracing::warn!(
            error_code = body.error,
            error_message = %body.message,
            status = %status,
            "API error"
        );

        (status, Json(body)).into_response()
    }
}

/// Result type for API operations
pub type Result<T> = std::result::Result<T, ApiError>;
