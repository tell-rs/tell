//! ClickHouse sink metrics
//!
//! Atomic counters for tracking sink performance and health.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tell_metrics::{SinkMetricsConfig, SinkMetricsProvider, SinkMetricsSnapshot};

// =============================================================================
// Metrics
// =============================================================================

/// Metrics for ClickHouse sink
#[derive(Debug, Default)]
pub struct ClickHouseMetrics {
    /// Total batches received
    pub batches_received: AtomicU64,

    /// Events written (TRACK)
    pub events_written: AtomicU64,

    /// Users written (IDENTIFY)
    pub users_written: AtomicU64,

    /// User devices written (IDENTIFY)
    pub user_devices_written: AtomicU64,

    /// User traits written (IDENTIFY)
    pub user_traits_written: AtomicU64,

    /// Context events written (CONTEXT)
    pub context_written: AtomicU64,

    /// Logs written
    pub logs_written: AtomicU64,

    /// Snapshots written (connectors)
    pub snapshots_written: AtomicU64,

    /// Total batches written to ClickHouse
    pub batches_written: AtomicU64,

    /// Write errors
    pub write_errors: AtomicU64,

    /// Retry attempts
    pub retry_count: AtomicU64,

    /// Decode errors
    pub decode_errors: AtomicU64,
}

impl ClickHouseMetrics {
    /// Create new metrics instance
    pub const fn new() -> Self {
        Self {
            batches_received: AtomicU64::new(0),
            events_written: AtomicU64::new(0),
            users_written: AtomicU64::new(0),
            user_devices_written: AtomicU64::new(0),
            user_traits_written: AtomicU64::new(0),
            context_written: AtomicU64::new(0),
            logs_written: AtomicU64::new(0),
            snapshots_written: AtomicU64::new(0),
            batches_written: AtomicU64::new(0),
            write_errors: AtomicU64::new(0),
            retry_count: AtomicU64::new(0),
            decode_errors: AtomicU64::new(0),
        }
    }

    /// Record a batch received
    #[inline]
    pub fn record_batch_received(&self) {
        self.batches_received.fetch_add(1, Ordering::Relaxed);
    }

    /// Record events written
    #[inline]
    pub fn record_events_written(&self, count: u64) {
        self.events_written.fetch_add(count, Ordering::Relaxed);
    }

    /// Record users written
    #[inline]
    pub fn record_users_written(&self, count: u64) {
        self.users_written.fetch_add(count, Ordering::Relaxed);
    }

    /// Record user devices written
    #[inline]
    pub fn record_user_devices_written(&self, count: u64) {
        self.user_devices_written
            .fetch_add(count, Ordering::Relaxed);
    }

    /// Record user traits written
    #[inline]
    pub fn record_user_traits_written(&self, count: u64) {
        self.user_traits_written.fetch_add(count, Ordering::Relaxed);
    }

    /// Record context written
    #[inline]
    pub fn record_context_written(&self, count: u64) {
        self.context_written.fetch_add(count, Ordering::Relaxed);
    }

    /// Record logs written
    #[inline]
    pub fn record_logs_written(&self, count: u64) {
        self.logs_written.fetch_add(count, Ordering::Relaxed);
    }

    /// Record snapshots written
    #[inline]
    pub fn record_snapshots_written(&self, count: u64) {
        self.snapshots_written.fetch_add(count, Ordering::Relaxed);
    }

    /// Record a batch written
    #[inline]
    pub fn record_batch_written(&self) {
        self.batches_written.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a write error
    #[inline]
    pub fn record_error(&self) {
        self.write_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a retry attempt
    #[inline]
    pub fn record_retry(&self) {
        self.retry_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a decode error
    #[inline]
    pub fn record_decode_error(&self) {
        self.decode_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Get snapshot of metrics
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            batches_received: self.batches_received.load(Ordering::Relaxed),
            events_written: self.events_written.load(Ordering::Relaxed),
            users_written: self.users_written.load(Ordering::Relaxed),
            user_devices_written: self.user_devices_written.load(Ordering::Relaxed),
            user_traits_written: self.user_traits_written.load(Ordering::Relaxed),
            context_written: self.context_written.load(Ordering::Relaxed),
            logs_written: self.logs_written.load(Ordering::Relaxed),
            snapshots_written: self.snapshots_written.load(Ordering::Relaxed),
            batches_written: self.batches_written.load(Ordering::Relaxed),
            write_errors: self.write_errors.load(Ordering::Relaxed),
            retry_count: self.retry_count.load(Ordering::Relaxed),
            decode_errors: self.decode_errors.load(Ordering::Relaxed),
        }
    }
}

/// Point-in-time snapshot of metrics
#[derive(Debug, Clone, Copy, Default)]
pub struct MetricsSnapshot {
    pub batches_received: u64,
    pub events_written: u64,
    pub users_written: u64,
    pub user_devices_written: u64,
    pub user_traits_written: u64,
    pub context_written: u64,
    pub logs_written: u64,
    pub snapshots_written: u64,
    pub batches_written: u64,
    pub write_errors: u64,
    pub retry_count: u64,
    pub decode_errors: u64,
}

// =============================================================================
// Metrics Handle
// =============================================================================

/// Handle for accessing ClickHouse sink metrics
///
/// This can be obtained before running the sink and used for metrics reporting.
/// It holds an Arc to the metrics, so it remains valid even after the sink
/// is consumed by `run()`.
#[derive(Clone)]
pub struct ClickHouseSinkMetricsHandle {
    id: String,
    metrics: Arc<ClickHouseMetrics>,
    config: SinkMetricsConfig,
}

impl ClickHouseSinkMetricsHandle {
    /// Create a new metrics handle
    pub fn new(id: String, metrics: Arc<ClickHouseMetrics>, flush_interval: Duration) -> Self {
        Self {
            id,
            metrics,
            config: SinkMetricsConfig {
                enabled: true,
                interval: flush_interval,
            },
        }
    }
}

impl SinkMetricsProvider for ClickHouseSinkMetricsHandle {
    fn sink_id(&self) -> &str {
        &self.id
    }

    fn sink_type(&self) -> &str {
        "clickhouse"
    }

    fn metrics_config(&self) -> SinkMetricsConfig {
        self.config
    }

    fn snapshot(&self) -> SinkMetricsSnapshot {
        let s = self.metrics.snapshot();
        // Sum all row types for total messages written
        let total_messages = s.events_written
            + s.users_written
            + s.user_devices_written
            + s.user_traits_written
            + s.context_written
            + s.logs_written
            + s.snapshots_written;
        SinkMetricsSnapshot {
            batches_received: s.batches_received,
            batches_written: s.batches_written,
            messages_written: total_messages,
            bytes_written: 0, // ClickHouse sink doesn't track bytes
            write_errors: s.write_errors,
            flush_count: s.batches_written,
        }
    }
}
