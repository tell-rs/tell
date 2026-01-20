//! Error types for the client builders
//!
//! Provides structured errors for batch, event, and log building failures.

use thiserror::Error;

/// Result type for builder operations
pub type Result<T> = std::result::Result<T, BuilderError>;

/// Errors that can occur when building batches, events, or logs
#[derive(Debug, Error)]
pub enum BuilderError {
    // =========================================================================
    // Batch errors
    // =========================================================================
    /// API key is missing (required field)
    #[error("api_key is required")]
    MissingApiKey,

    /// API key has wrong length (must be 16 bytes)
    #[error("api_key must be exactly 16 bytes, got {0}")]
    InvalidApiKeyLength(usize),

    /// Data payload is missing (required field)
    #[error("data payload is required")]
    MissingData,

    /// Source IP has wrong length (must be 16 bytes for IPv6)
    #[error("source_ip must be exactly 16 bytes (IPv6 format), got {0}")]
    InvalidSourceIpLength(usize),

    /// Data payload exceeds maximum size
    #[error("data payload too large: {size} bytes exceeds maximum {max} bytes")]
    DataTooLarge {
        /// Actual size provided
        size: usize,
        /// Maximum allowed size
        max: usize,
    },

    /// Buffer allocation failed
    #[error("failed to allocate buffer of size {0}")]
    AllocationFailed(usize),

    // =========================================================================
    // Event errors
    // =========================================================================
    /// UUID field has wrong length (must be 16 bytes)
    #[error("{field} must be exactly 16 bytes, got {len}")]
    InvalidUuidLength {
        /// Field name (device_id, session_id)
        field: &'static str,
        /// Actual length provided
        len: usize,
    },

    /// Event name exceeds maximum length
    #[error("event_name too long: {len} bytes exceeds maximum {max} bytes")]
    EventNameTooLong {
        /// Actual length
        len: usize,
        /// Maximum allowed
        max: usize,
    },

    /// Payload exceeds maximum size
    #[error("payload too large: {len} bytes exceeds maximum {max} bytes")]
    PayloadTooLarge {
        /// Actual length
        len: usize,
        /// Maximum allowed
        max: usize,
    },

    /// EventData must contain at least one event
    #[error("EventData must contain at least one event")]
    EmptyEventData,

    // =========================================================================
    // Log errors
    // =========================================================================
    /// Source string exceeds maximum length
    #[error("source too long: {len} bytes exceeds maximum {max} bytes")]
    SourceTooLong {
        /// Actual length
        len: usize,
        /// Maximum allowed
        max: usize,
    },

    /// Service string exceeds maximum length
    #[error("service too long: {len} bytes exceeds maximum {max} bytes")]
    ServiceTooLong {
        /// Actual length
        len: usize,
        /// Maximum allowed
        max: usize,
    },

    /// LogData must contain at least one log entry
    #[error("LogData must contain at least one log entry")]
    EmptyLogData,
}

impl BuilderError {
    /// Create a DataTooLarge error
    pub fn data_too_large(size: usize, max: usize) -> Self {
        Self::DataTooLarge { size, max }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display_missing_api_key() {
        let err = BuilderError::MissingApiKey;
        assert_eq!(err.to_string(), "api_key is required");
    }

    #[test]
    fn test_error_display_invalid_api_key_length() {
        let err = BuilderError::InvalidApiKeyLength(10);
        assert_eq!(err.to_string(), "api_key must be exactly 16 bytes, got 10");
    }

    #[test]
    fn test_error_display_missing_data() {
        let err = BuilderError::MissingData;
        assert_eq!(err.to_string(), "data payload is required");
    }

    #[test]
    fn test_error_display_invalid_source_ip_length() {
        let err = BuilderError::InvalidSourceIpLength(4);
        assert_eq!(
            err.to_string(),
            "source_ip must be exactly 16 bytes (IPv6 format), got 4"
        );
    }

    #[test]
    fn test_error_display_data_too_large() {
        let err = BuilderError::data_too_large(1_000_000, 500_000);
        assert_eq!(
            err.to_string(),
            "data payload too large: 1000000 bytes exceeds maximum 500000 bytes"
        );
    }

    #[test]
    fn test_error_display_allocation_failed() {
        let err = BuilderError::AllocationFailed(usize::MAX);
        assert!(err.to_string().contains("failed to allocate buffer"));
    }

    #[test]
    fn test_error_display_invalid_uuid_length() {
        let err = BuilderError::InvalidUuidLength {
            field: "device_id",
            len: 10,
        };
        assert_eq!(
            err.to_string(),
            "device_id must be exactly 16 bytes, got 10"
        );
    }

    #[test]
    fn test_error_display_event_name_too_long() {
        let err = BuilderError::EventNameTooLong { len: 300, max: 256 };
        assert_eq!(
            err.to_string(),
            "event_name too long: 300 bytes exceeds maximum 256 bytes"
        );
    }

    #[test]
    fn test_error_display_payload_too_large() {
        let err = BuilderError::PayloadTooLarge {
            len: 2_000_000,
            max: 1_000_000,
        };
        assert_eq!(
            err.to_string(),
            "payload too large: 2000000 bytes exceeds maximum 1000000 bytes"
        );
    }

    #[test]
    fn test_error_display_empty_event_data() {
        let err = BuilderError::EmptyEventData;
        assert_eq!(err.to_string(), "EventData must contain at least one event");
    }

    #[test]
    fn test_error_display_source_too_long() {
        let err = BuilderError::SourceTooLong { len: 300, max: 256 };
        assert_eq!(
            err.to_string(),
            "source too long: 300 bytes exceeds maximum 256 bytes"
        );
    }

    #[test]
    fn test_error_display_service_too_long() {
        let err = BuilderError::ServiceTooLong { len: 300, max: 256 };
        assert_eq!(
            err.to_string(),
            "service too long: 300 bytes exceeds maximum 256 bytes"
        );
    }

    #[test]
    fn test_error_display_empty_log_data() {
        let err = BuilderError::EmptyLogData;
        assert_eq!(
            err.to_string(),
            "LogData must contain at least one log entry"
        );
    }
}
