//! Telemetry error types.

use thiserror::Error;

/// Errors that can occur during telemetry operations.
#[derive(Debug, Error)]
pub enum TelemetryError {
    /// Telemetry channel is full (non-blocking send failed)
    #[error("telemetry channel full, payload dropped")]
    ChannelFull,

    /// Network error during telemetry submission
    #[error("network error: {0}")]
    Network(String),

    /// Server returned an error status
    #[error("server error: HTTP {0}")]
    Server(u16),

    /// Protocol error (FBS building failed)
    #[error("protocol error: {0}")]
    Protocol(String),

    /// Serialization error
    #[error("serialization error: {0}")]
    Serialization(String),

    /// IO error (e.g., reading/writing install ID)
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}
