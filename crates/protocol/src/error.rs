//! Protocol error types
//!
//! Errors that can occur when parsing or handling protocol messages.

use thiserror::Error;

/// Errors that can occur during protocol operations
#[derive(Debug, Error)]
pub enum ProtocolError {
    /// Message is too short to contain required fields
    #[error("message too short: expected at least {expected} bytes, got {actual}")]
    MessageTooShort { expected: usize, actual: usize },

    /// Invalid FlatBuffer format
    #[error("invalid flatbuffer: {0}")]
    InvalidFlatBuffer(String),

    /// Missing required field
    #[error("missing required field: {0}")]
    MissingField(&'static str),

    /// Invalid API key length
    #[error("invalid API key length: expected {expected} bytes, got {actual}")]
    InvalidApiKeyLength { expected: usize, actual: usize },

    /// Invalid schema type value
    #[error("invalid schema type: {0}")]
    InvalidSchemaType(u8),

    /// Invalid source IP length
    #[error("invalid source IP length: expected {expected} bytes, got {actual}")]
    InvalidSourceIpLength { expected: usize, actual: usize },

    /// Buffer overflow - message exceeds maximum size
    #[error("buffer overflow: message size {size} exceeds maximum {max}")]
    BufferOverflow { size: usize, max: usize },

    /// Batch is full
    #[error("batch is full: contains {count} items (max {max})")]
    BatchFull { count: usize, max: usize },

    /// Empty data payload
    #[error("empty data payload")]
    EmptyData,
}

impl ProtocolError {
    /// Create a message too short error
    #[inline]
    pub fn too_short(expected: usize, actual: usize) -> Self {
        Self::MessageTooShort { expected, actual }
    }

    /// Create an invalid flatbuffer error
    #[inline]
    pub fn invalid_flatbuffer(msg: impl Into<String>) -> Self {
        Self::InvalidFlatBuffer(msg.into())
    }

    /// Create a missing field error
    #[inline]
    pub fn missing_field(field: &'static str) -> Self {
        Self::MissingField(field)
    }

    /// Create an invalid API key length error
    #[inline]
    pub fn invalid_api_key_length(actual: usize) -> Self {
        Self::InvalidApiKeyLength {
            expected: crate::API_KEY_LENGTH,
            actual,
        }
    }

    /// Create an invalid source IP length error
    #[inline]
    pub fn invalid_source_ip_length(actual: usize) -> Self {
        Self::InvalidSourceIpLength {
            expected: crate::IPV6_LENGTH,
            actual,
        }
    }

    /// Create a message too large error
    #[inline]
    pub fn message_too_large(size: usize, max: usize) -> Self {
        Self::BufferOverflow { size, max }
    }

    /// Check if this is a recoverable error (can continue processing)
    pub fn is_recoverable(&self) -> bool {
        matches!(
            self,
            Self::InvalidSchemaType(_) | Self::EmptyData | Self::BatchFull { .. }
        )
    }
}
