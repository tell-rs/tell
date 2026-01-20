//! HTTP source error types
//!
//! Structured errors for the HTTP ingestion source.

use std::fmt;

/// HTTP source errors
#[derive(Debug, thiserror::Error)]
pub enum HttpSourceError {
    /// Failed to bind to address
    #[error("failed to bind to {address}: {source}")]
    Bind {
        address: String,
        #[source]
        source: std::io::Error,
    },

    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// JSON parsing error
    #[error("JSON parse error at line {line}: {message}")]
    JsonParse { line: usize, message: String },

    /// Invalid request format
    #[error("invalid request: {0}")]
    InvalidRequest(String),

    /// Authentication failed
    #[error("authentication failed: {0}")]
    AuthFailed(String),

    /// Missing required field
    #[error("missing required field: {0}")]
    MissingField(&'static str),

    /// Invalid field value
    #[error("invalid value for field '{field}': {message}")]
    InvalidField { field: &'static str, message: String },

    /// Payload too large
    #[error("payload size {size} exceeds limit {limit}")]
    PayloadTooLarge { size: usize, limit: usize },

    /// Channel closed (pipeline shutdown)
    #[error("batch channel closed")]
    ChannelClosed,

    /// Service unavailable (backpressure)
    #[error("service unavailable: {0}")]
    ServiceUnavailable(String),

    /// Hyper/HTTP error
    #[error("HTTP error: {0}")]
    Http(String),
}

/// Result of processing a single line in JSONL
#[derive(Debug)]
pub struct LineError {
    /// 1-indexed line number
    pub line: usize,
    /// Error message
    pub error: String,
}

impl fmt::Display for LineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "line {}: {}", self.line, self.error)
    }
}

/// Result of processing a batch request
#[derive(Debug)]
pub struct BatchResult {
    /// Number of successfully accepted items
    pub accepted: usize,
    /// Number of rejected items
    pub rejected: usize,
    /// Per-line errors (if any)
    pub errors: Vec<LineError>,
}

impl BatchResult {
    /// Create a fully successful result
    #[allow(dead_code)]
    pub fn success(count: usize) -> Self {
        Self {
            accepted: count,
            rejected: 0,
            errors: Vec::new(),
        }
    }

    /// Check if all items were accepted
    pub fn is_complete_success(&self) -> bool {
        self.rejected == 0
    }

    /// Check if any items were accepted
    pub fn has_accepted(&self) -> bool {
        self.accepted > 0
    }
}
