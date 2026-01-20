//! HTTP source metrics
//!
//! Atomic counters for tracking HTTP ingestion performance.

use std::sync::atomic::{AtomicU64, Ordering};

use tell_metrics::{SourceMetricsProvider, SourceMetricsSnapshot};

use crate::common::SourceMetrics;

/// HTTP source metrics
#[derive(Debug, Default)]
pub struct HttpSourceMetrics {
    /// Base source metrics
    pub base: SourceMetrics,

    /// Total HTTP requests received
    pub requests_total: AtomicU64,

    /// Successful requests (2xx)
    pub requests_success: AtomicU64,

    /// Client errors (4xx)
    pub requests_client_error: AtomicU64,

    /// Server errors (5xx)
    pub requests_server_error: AtomicU64,

    /// Authentication failures
    pub auth_failures: AtomicU64,

    /// Parse errors (invalid JSON)
    pub parse_errors: AtomicU64,

    /// Items accepted
    pub items_accepted: AtomicU64,

    /// Items rejected
    pub items_rejected: AtomicU64,
}

impl HttpSourceMetrics {
    /// Create new metrics instance
    pub const fn new() -> Self {
        Self {
            base: SourceMetrics::new(),
            requests_total: AtomicU64::new(0),
            requests_success: AtomicU64::new(0),
            requests_client_error: AtomicU64::new(0),
            requests_server_error: AtomicU64::new(0),
            auth_failures: AtomicU64::new(0),
            parse_errors: AtomicU64::new(0),
            items_accepted: AtomicU64::new(0),
            items_rejected: AtomicU64::new(0),
        }
    }

    /// Record a request received
    #[inline]
    pub fn request_received(&self) {
        self.requests_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a successful request
    #[inline]
    pub fn request_success(&self) {
        self.requests_success.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a client error (4xx)
    #[inline]
    pub fn request_client_error(&self) {
        self.requests_client_error.fetch_add(1, Ordering::Relaxed);
        self.base.errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a server error (5xx)
    #[inline]
    pub fn request_server_error(&self) {
        self.requests_server_error.fetch_add(1, Ordering::Relaxed);
        self.base.errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an authentication failure
    #[inline]
    pub fn auth_failure(&self) {
        self.auth_failures.fetch_add(1, Ordering::Relaxed);
        self.request_client_error();
    }

    /// Record a parse error
    #[inline]
    pub fn parse_error(&self) {
        self.parse_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Record items processed
    #[inline]
    pub fn items_processed(&self, accepted: usize, rejected: usize) {
        self.items_accepted.fetch_add(accepted as u64, Ordering::Relaxed);
        self.items_rejected.fetch_add(rejected as u64, Ordering::Relaxed);
        self.base.messages_received.fetch_add(accepted as u64, Ordering::Relaxed);
    }

    /// Record bytes received
    #[inline]
    pub fn bytes_received(&self, bytes: u64) {
        self.base.bytes_received.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record batch sent
    #[inline]
    pub fn batch_sent(&self) {
        self.base.batch_sent();
    }

    /// Get extended snapshot
    pub fn snapshot(&self) -> HttpMetricsSnapshot {
        HttpMetricsSnapshot {
            requests_total: self.requests_total.load(Ordering::Relaxed),
            requests_success: self.requests_success.load(Ordering::Relaxed),
            requests_client_error: self.requests_client_error.load(Ordering::Relaxed),
            requests_server_error: self.requests_server_error.load(Ordering::Relaxed),
            auth_failures: self.auth_failures.load(Ordering::Relaxed),
            parse_errors: self.parse_errors.load(Ordering::Relaxed),
            items_accepted: self.items_accepted.load(Ordering::Relaxed),
            items_rejected: self.items_rejected.load(Ordering::Relaxed),
            messages_received: self.base.messages_received.load(Ordering::Relaxed),
            bytes_received: self.base.bytes_received.load(Ordering::Relaxed),
            batches_sent: self.base.batches_sent.load(Ordering::Relaxed),
            errors: self.base.errors.load(Ordering::Relaxed),
        }
    }
}

/// Extended metrics snapshot
#[derive(Debug, Clone, Copy)]
pub struct HttpMetricsSnapshot {
    pub requests_total: u64,
    pub requests_success: u64,
    pub requests_client_error: u64,
    pub requests_server_error: u64,
    pub auth_failures: u64,
    pub parse_errors: u64,
    pub items_accepted: u64,
    pub items_rejected: u64,
    pub messages_received: u64,
    pub bytes_received: u64,
    pub batches_sent: u64,
    pub errors: u64,
}

/// Handle for accessing HTTP source metrics
#[derive(Clone)]
pub struct HttpSourceMetricsHandle {
    id: String,
    metrics: std::sync::Arc<HttpSourceMetrics>,
}

impl HttpSourceMetricsHandle {
    /// Create a new metrics handle
    pub fn new(id: String, metrics: std::sync::Arc<HttpSourceMetrics>) -> Self {
        Self { id, metrics }
    }

    /// Get extended snapshot
    pub fn extended_snapshot(&self) -> HttpMetricsSnapshot {
        self.metrics.snapshot()
    }
}

impl SourceMetricsProvider for HttpSourceMetricsHandle {
    fn source_id(&self) -> &str {
        &self.id
    }

    fn source_type(&self) -> &str {
        "http"
    }

    fn snapshot(&self) -> SourceMetricsSnapshot {
        let s = self.metrics.base.snapshot();
        SourceMetricsSnapshot {
            connections_active: s.connections_active,
            connections_total: s.connections_total,
            messages_received: s.messages_received,
            bytes_received: s.bytes_received,
            batches_sent: s.batches_sent,
            errors: s.errors,
        }
    }
}
