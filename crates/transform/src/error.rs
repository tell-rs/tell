//! Transform error types
//!
//! Errors that can occur during batch transformation.

use thiserror::Error;

#[cfg(test)]
#[path = "error_test.rs"]
mod tests;

/// Errors that can occur during transformation
#[derive(Debug, Error)]
pub enum TransformError {
    /// Failed to decode FlatBuffer message
    #[error("failed to decode message: {0}")]
    DecodeError(String),

    /// Transformation logic failed
    #[error("transform failed: {0}")]
    TransformFailed(String),

    /// Transformer not initialized
    #[error("transformer not initialized: {0}")]
    NotInitialized(String),

    /// Invalid configuration
    #[error("invalid configuration: {0}")]
    Config(String),

    /// I/O error (e.g., loading pattern files)
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Context cancelled
    #[error("operation cancelled")]
    Cancelled,
}

impl TransformError {
    /// Create a decode error
    pub fn decode(msg: impl Into<String>) -> Self {
        Self::DecodeError(msg.into())
    }

    /// Create a transform failed error
    pub fn failed(msg: impl Into<String>) -> Self {
        Self::TransformFailed(msg.into())
    }

    /// Create a config error
    pub fn config(msg: impl Into<String>) -> Self {
        Self::Config(msg.into())
    }

    /// Create a not initialized error
    pub fn not_initialized(name: impl Into<String>) -> Self {
        Self::NotInitialized(name.into())
    }
}
