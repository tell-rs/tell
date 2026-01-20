//! JSON request and response types for HTTP API
//!
//! Defines the JSON schema for events and logs ingestion.

use serde::{Deserialize, Serialize};

// =============================================================================
// Event Types
// =============================================================================

/// JSON event request (single event from JSONL line)
#[derive(Debug, Clone, Deserialize)]
pub struct JsonEvent {
    /// Event type: "track", "identify", "group", "alias", "enrich", "context"
    #[serde(rename = "type")]
    pub event_type: String,

    /// Event name (required for track events)
    #[serde(default)]
    pub event: Option<String>,

    /// Device UUID (required)
    pub device_id: String,

    /// Session UUID (optional)
    #[serde(default)]
    pub session_id: Option<String>,

    /// User ID (required for identify/alias)
    #[serde(default)]
    pub user_id: Option<String>,

    /// Group ID (required for group events)
    #[serde(default)]
    pub group_id: Option<String>,

    /// Timestamp in milliseconds (optional, server sets if missing)
    #[serde(default)]
    pub timestamp: Option<u64>,

    /// Event properties (track events)
    #[serde(default)]
    pub properties: Option<serde_json::Value>,

    /// User traits (identify events)
    #[serde(default)]
    pub traits: Option<serde_json::Value>,

    /// Context data
    #[serde(default)]
    pub context: Option<serde_json::Value>,
}

// =============================================================================
// Log Types
// =============================================================================

/// JSON log entry request
#[derive(Debug, Clone, Deserialize)]
pub struct JsonLogEntry {
    /// Log level: "trace", "debug", "info", "warning", "error", "critical", etc.
    #[serde(default = "default_log_level")]
    pub level: String,

    /// Log message
    pub message: String,

    /// Timestamp in milliseconds (optional)
    #[serde(default)]
    pub timestamp: Option<u64>,

    /// Source hostname/instance
    #[serde(default)]
    pub source: Option<String>,

    /// Service/application name
    #[serde(default)]
    pub service: Option<String>,

    /// Session UUID for correlation
    #[serde(default)]
    pub session_id: Option<String>,

    /// Additional structured data
    #[serde(default)]
    pub data: Option<serde_json::Value>,

    /// Log event type: "log" or "enrich" (defaults to "log")
    #[serde(rename = "type", default = "default_log_type")]
    pub log_type: String,
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_log_type() -> String {
    "log".to_string()
}

// =============================================================================
// Response Types
// =============================================================================

/// Successful ingestion response
#[derive(Debug, Clone, Serialize)]
pub struct IngestResponse {
    /// Number of items accepted
    pub accepted: usize,

    /// Request ID for tracking
    pub request_id: String,
}

/// Partial success response (some items rejected)
#[derive(Debug, Clone, Serialize)]
pub struct PartialResponse {
    /// Number of items accepted
    pub accepted: usize,

    /// Number of items rejected
    pub rejected: usize,

    /// Per-line errors
    pub errors: Vec<LineErrorResponse>,

    /// Request ID for tracking
    pub request_id: String,
}

/// Per-line error in response
#[derive(Debug, Clone, Serialize)]
pub struct LineErrorResponse {
    /// 1-indexed line number
    pub line: usize,

    /// Error message
    pub error: String,
}

/// Error response
#[derive(Debug, Clone, Serialize)]
pub struct ErrorResponse {
    /// Error code
    pub error: String,

    /// Human-readable message
    pub message: String,

    /// Request ID for tracking
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
}

impl ErrorResponse {
    /// Create an error response
    pub fn new(error: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            error: error.into(),
            message: message.into(),
            request_id: None,
        }
    }

    /// Add request ID
    #[allow(dead_code)]
    pub fn with_request_id(mut self, id: impl Into<String>) -> Self {
        self.request_id = Some(id.into());
        self
    }
}
