//! HTTP source configuration
//!
//! Configuration options for the HTTP/REST ingestion endpoint.

use std::time::Duration;

use tell_protocol::SourceId;

/// Default HTTP port
const DEFAULT_PORT: u16 = 8080;

/// Default maximum payload size (16MB - matches TCP)
const DEFAULT_MAX_PAYLOAD_SIZE: usize = 16 * 1024 * 1024;

/// Default batch size (number of items before flush)
const DEFAULT_BATCH_SIZE: usize = 500;

/// Default flush interval
const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_millis(100);

/// Default request timeout
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// HTTP source configuration
#[derive(Debug, Clone)]
pub struct HttpSourceConfig {
    /// Source identifier for routing
    pub id: String,

    /// Bind address (e.g., "0.0.0.0")
    pub address: String,

    /// Listen port
    pub port: u16,

    /// Maximum request payload size in bytes
    pub max_payload_size: usize,

    /// Maximum items per batch before flush
    pub batch_size: usize,

    /// Flush interval for partial batches
    pub flush_interval: Duration,

    /// Request timeout
    pub request_timeout: Duration,

    /// Enable CORS (for browser clients)
    pub cors_enabled: bool,

    /// CORS allowed origins (empty = allow all)
    pub cors_origins: Vec<String>,

    /// Enable request logging
    pub request_logging: bool,

    /// TLS certificate path (None = HTTP only)
    pub tls_cert_path: Option<String>,

    /// TLS key path
    pub tls_key_path: Option<String>,
}

impl Default for HttpSourceConfig {
    fn default() -> Self {
        Self {
            id: "http".into(),
            address: "0.0.0.0".into(),
            port: DEFAULT_PORT,
            max_payload_size: DEFAULT_MAX_PAYLOAD_SIZE,
            batch_size: DEFAULT_BATCH_SIZE,
            flush_interval: DEFAULT_FLUSH_INTERVAL,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            cors_enabled: true,
            cors_origins: Vec::new(),
            request_logging: false,
            tls_cert_path: None,
            tls_key_path: None,
        }
    }
}

impl HttpSourceConfig {
    /// Create config with custom port
    pub fn with_port(port: u16) -> Self {
        Self {
            port,
            ..Default::default()
        }
    }

    /// Get the socket address to bind to
    pub fn bind_address(&self) -> String {
        format!("{}:{}", self.address, self.port)
    }

    /// Get the source ID
    pub fn source_id(&self) -> SourceId {
        SourceId::new(&self.id)
    }

    /// Check if TLS is configured
    pub fn is_tls_enabled(&self) -> bool {
        self.tls_cert_path.is_some() && self.tls_key_path.is_some()
    }
}
