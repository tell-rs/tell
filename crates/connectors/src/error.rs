//! Error types for connectors

use thiserror::Error;

/// Errors that can occur during connector operations
#[derive(Error, Debug)]
pub enum ConnectorError {
    /// Failed to initialize connector (e.g., HTTP client creation failed)
    #[error("failed to initialize connector: {0}")]
    Init(String),

    /// HTTP request failed
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    /// JSON parsing failed
    #[error("JSON parse error: {0}")]
    Json(#[from] serde_json::Error),

    /// Invalid entity format
    #[error("Invalid entity format: {0}")]
    InvalidEntity(String),

    /// API rate limited
    #[error("Rate limited, retry after {retry_after_secs} seconds")]
    RateLimited { retry_after_secs: u64 },

    /// Authentication failed
    #[error("Authentication failed: {0}")]
    AuthFailed(String),

    /// Resource not found
    #[error("Resource not found: {0}")]
    NotFound(String),

    /// Configuration error
    #[error("Configuration error: {0}")]
    ConfigError(String),

    /// Unknown connector type
    #[error("Unknown connector type: {0}")]
    UnknownConnector(String),

    /// Schedule parsing error
    #[error("Invalid cron schedule: {0}")]
    InvalidSchedule(String),
}
