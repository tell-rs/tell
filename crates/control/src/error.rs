//! Control plane error types

use thiserror::Error;

/// Control plane errors
#[derive(Debug, Error)]
pub enum ControlError {
    /// Database connection error
    #[error("database error: {0}")]
    Database(#[from] turso::Error),

    /// Entity not found
    #[error("{entity} not found: {id}")]
    NotFound { entity: &'static str, id: String },

    /// Entity already exists
    #[error("{entity} already exists: {id}")]
    AlreadyExists { entity: &'static str, id: String },

    /// Invalid data
    #[error("invalid {field}: {message}")]
    Invalid {
        field: &'static str,
        message: String,
    },

    /// JSON serialization error
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    /// Permission denied
    #[error("permission denied: {0}")]
    PermissionDenied(String),
}

impl ControlError {
    /// Create a not found error
    pub fn not_found(entity: &'static str, id: impl Into<String>) -> Self {
        Self::NotFound {
            entity,
            id: id.into(),
        }
    }

    /// Create an already exists error
    pub fn already_exists(entity: &'static str, id: impl Into<String>) -> Self {
        Self::AlreadyExists {
            entity,
            id: id.into(),
        }
    }

    /// Create an invalid data error
    pub fn invalid(field: &'static str, message: impl Into<String>) -> Self {
        Self::Invalid {
            field,
            message: message.into(),
        }
    }
}

/// Result type for control plane operations
pub type Result<T> = std::result::Result<T, ControlError>;
