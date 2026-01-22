//! Metrics provider traits
//!
//! Traits for components to expose their metrics to the unified reporter.
//! Sources, sinks, and transformers implement these traits so the reporter
//! can collect their metrics without knowing the concrete types.
//!
//! # Design
//!
//! - Traits use `&self` for zero-copy metric access
//! - All providers are `Send + Sync` for thread-safe collection
//! - Metric structs use atomics internally, so no locks needed

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Metrics for a source component
///
/// Sources track connection and message statistics.
/// All fields use atomics for lock-free updates.
#[derive(Debug, Default)]
pub struct SourceMetrics {
    /// Currently active connections
    pub connections_active: AtomicU64,
    /// Total connections accepted since start
    pub connections_total: AtomicU64,
    /// Total messages received
    pub messages_received: AtomicU64,
    /// Total bytes received
    pub bytes_received: AtomicU64,
    /// Total batches sent to pipeline
    pub batches_sent: AtomicU64,
    /// Total errors encountered
    pub errors: AtomicU64,
}

impl SourceMetrics {
    /// Create new metrics with all counters at zero
    pub const fn new() -> Self {
        Self {
            connections_active: AtomicU64::new(0),
            connections_total: AtomicU64::new(0),
            messages_received: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            batches_sent: AtomicU64::new(0),
            errors: AtomicU64::new(0),
        }
    }

    /// Take a snapshot of current values
    #[inline]
    pub fn snapshot(&self) -> SourceMetricsSnapshot {
        SourceMetricsSnapshot {
            connections_active: self.connections_active.load(Ordering::Relaxed),
            connections_total: self.connections_total.load(Ordering::Relaxed),
            messages_received: self.messages_received.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
            batches_sent: self.batches_sent.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
        }
    }
}

/// Point-in-time snapshot of source metrics
#[derive(Debug, Clone, Copy, Default, serde::Serialize)]
pub struct SourceMetricsSnapshot {
    pub connections_active: u64,
    pub connections_total: u64,
    pub messages_received: u64,
    pub bytes_received: u64,
    pub batches_sent: u64,
    pub errors: u64,
}

/// Trait for sources to provide metrics to the reporter
///
/// Sources can implement this directly or use an adapter.
/// The `snapshot()` method is the main requirement - it returns
/// a copy of the current metrics that can be safely used for reporting.
pub trait SourceMetricsProvider: Send + Sync {
    /// Unique identifier for this source instance
    fn source_id(&self) -> &str;

    /// Source type (e.g., "tcp", "syslog_tcp", "syslog_udp")
    fn source_type(&self) -> &str;

    /// Get a snapshot of current metrics
    ///
    /// This is the primary method - implementations convert their
    /// internal metrics format to the common snapshot struct.
    fn snapshot(&self) -> SourceMetricsSnapshot;
}

/// Metrics for a sink component
///
/// Sinks track write statistics and errors.
/// All fields use atomics for lock-free updates.
#[derive(Debug, Default)]
pub struct SinkMetrics {
    /// Total batches received from router
    pub batches_received: AtomicU64,
    /// Total batches successfully written
    pub batches_written: AtomicU64,
    /// Total messages written
    pub messages_written: AtomicU64,
    /// Total bytes written
    pub bytes_written: AtomicU64,
    /// Total write errors
    pub write_errors: AtomicU64,
    /// Total flush operations
    pub flush_count: AtomicU64,
}

impl SinkMetrics {
    /// Create new metrics with all counters at zero
    pub const fn new() -> Self {
        Self {
            batches_received: AtomicU64::new(0),
            batches_written: AtomicU64::new(0),
            messages_written: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            write_errors: AtomicU64::new(0),
            flush_count: AtomicU64::new(0),
        }
    }

    /// Record a received batch
    #[inline]
    pub fn record_received(&self) {
        self.batches_received.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a successfully written batch
    #[inline]
    pub fn record_written(&self, messages: u64, bytes: u64) {
        self.batches_written.fetch_add(1, Ordering::Relaxed);
        self.messages_written.fetch_add(messages, Ordering::Relaxed);
        self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record a write error
    #[inline]
    pub fn record_error(&self) {
        self.write_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a flush operation
    #[inline]
    pub fn record_flush(&self) {
        self.flush_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Take a snapshot of current values
    #[inline]
    pub fn snapshot(&self) -> SinkMetricsSnapshot {
        SinkMetricsSnapshot {
            batches_received: self.batches_received.load(Ordering::Relaxed),
            batches_written: self.batches_written.load(Ordering::Relaxed),
            messages_written: self.messages_written.load(Ordering::Relaxed),
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
            write_errors: self.write_errors.load(Ordering::Relaxed),
            flush_count: self.flush_count.load(Ordering::Relaxed),
        }
    }
}

/// Point-in-time snapshot of sink metrics
#[derive(Debug, Clone, Copy, Default, serde::Serialize)]
pub struct SinkMetricsSnapshot {
    pub batches_received: u64,
    pub batches_written: u64,
    pub messages_written: u64,
    pub bytes_written: u64,
    pub write_errors: u64,
    pub flush_count: u64,
}

/// Per-sink metrics configuration
#[derive(Debug, Clone, Copy)]
pub struct SinkMetricsConfig {
    /// Whether per-sink metrics reporting is enabled
    pub enabled: bool,
    /// Reporting interval for this sink
    pub interval: Duration,
}

impl Default for SinkMetricsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval: Duration::from_secs(10),
        }
    }
}

/// Trait for sinks to provide metrics to the reporter
///
/// Sinks can implement this directly or use an adapter.
/// The `snapshot()` method is the main requirement.
pub trait SinkMetricsProvider: Send + Sync {
    /// Unique identifier for this sink instance
    fn sink_id(&self) -> &str;

    /// Sink type (e.g., "clickhouse", "disk_binary", "stdout")
    fn sink_type(&self) -> &str;

    /// Get per-sink metrics configuration
    fn metrics_config(&self) -> SinkMetricsConfig;

    /// Get a snapshot of current metrics
    fn snapshot(&self) -> SinkMetricsSnapshot;
}

/// Metrics for a transformer component
///
/// Transformers track processing statistics.
/// All fields use atomics for lock-free updates.
#[derive(Debug, Default)]
pub struct TransformerMetrics {
    /// Total batches processed
    pub batches_processed: AtomicU64,
    /// Batches where the transform modified data
    pub batches_modified: AtomicU64,
    /// Batches dropped/filtered out entirely
    pub batches_dropped: AtomicU64,
    /// Total processing time in nanoseconds
    pub processing_time_ns: AtomicU64,
    /// Total errors during transformation
    pub errors: AtomicU64,
}

impl TransformerMetrics {
    /// Create new metrics with all counters at zero
    pub const fn new() -> Self {
        Self {
            batches_processed: AtomicU64::new(0),
            batches_modified: AtomicU64::new(0),
            batches_dropped: AtomicU64::new(0),
            processing_time_ns: AtomicU64::new(0),
            errors: AtomicU64::new(0),
        }
    }

    /// Record a processed batch
    #[inline]
    pub fn record_processed(&self, duration: Duration, modified: bool) {
        self.batches_processed.fetch_add(1, Ordering::Relaxed);
        self.processing_time_ns
            .fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
        if modified {
            self.batches_modified.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record a dropped batch
    #[inline]
    pub fn record_dropped(&self) {
        self.batches_dropped.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an error
    #[inline]
    pub fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Take a snapshot of current values
    #[inline]
    pub fn snapshot(&self) -> TransformerMetricsSnapshot {
        TransformerMetricsSnapshot {
            batches_processed: self.batches_processed.load(Ordering::Relaxed),
            batches_modified: self.batches_modified.load(Ordering::Relaxed),
            batches_dropped: self.batches_dropped.load(Ordering::Relaxed),
            processing_time_ns: self.processing_time_ns.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
        }
    }
}

/// Point-in-time snapshot of transformer metrics
#[derive(Debug, Clone, Copy, Default, serde::Serialize)]
pub struct TransformerMetricsSnapshot {
    pub batches_processed: u64,
    pub batches_modified: u64,
    pub batches_dropped: u64,
    pub processing_time_ns: u64,
    pub errors: u64,
}

impl TransformerMetricsSnapshot {
    /// Calculate average processing time per batch
    #[inline]
    pub fn avg_processing_time(&self) -> Duration {
        if self.batches_processed == 0 {
            Duration::ZERO
        } else {
            Duration::from_nanos(self.processing_time_ns / self.batches_processed)
        }
    }
}

/// Trait for transformers to provide metrics to the reporter
///
/// Transformers can implement this directly or use an adapter.
/// The `snapshot()` method is the main requirement.
pub trait TransformerMetricsProvider: Send + Sync {
    /// Unique identifier for this transformer instance
    fn transformer_id(&self) -> &str;

    /// Transformer type (e.g., "pattern", "redact", "filter")
    fn transformer_type(&self) -> &str;

    /// Get a snapshot of current metrics
    fn snapshot(&self) -> TransformerMetricsSnapshot;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_source_metrics_snapshot() {
        let metrics = SourceMetrics::new();
        metrics.connections_active.fetch_add(5, Ordering::Relaxed);
        metrics.messages_received.fetch_add(1000, Ordering::Relaxed);
        metrics.bytes_received.fetch_add(50000, Ordering::Relaxed);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.connections_active, 5);
        assert_eq!(snapshot.messages_received, 1000);
        assert_eq!(snapshot.bytes_received, 50000);
    }

    #[test]
    fn test_sink_metrics_operations() {
        let metrics = SinkMetrics::new();

        metrics.record_received();
        metrics.record_received();
        metrics.record_written(100, 5000);
        metrics.record_error();
        metrics.record_flush();

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.batches_received, 2);
        assert_eq!(snapshot.batches_written, 1);
        assert_eq!(snapshot.messages_written, 100);
        assert_eq!(snapshot.bytes_written, 5000);
        assert_eq!(snapshot.write_errors, 1);
        assert_eq!(snapshot.flush_count, 1);
    }

    #[test]
    fn test_transformer_metrics_operations() {
        let metrics = TransformerMetrics::new();

        metrics.record_processed(Duration::from_micros(100), true);
        metrics.record_processed(Duration::from_micros(200), false);
        metrics.record_dropped();
        metrics.record_error();

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.batches_processed, 2);
        assert_eq!(snapshot.batches_modified, 1);
        assert_eq!(snapshot.batches_dropped, 1);
        assert_eq!(snapshot.processing_time_ns, 300_000);
        assert_eq!(snapshot.errors, 1);
    }

    #[test]
    fn test_transformer_avg_processing_time() {
        let snapshot = TransformerMetricsSnapshot {
            batches_processed: 100,
            processing_time_ns: 1_000_000, // 1ms total
            ..Default::default()
        };

        assert_eq!(snapshot.avg_processing_time(), Duration::from_micros(10));
    }

    #[test]
    fn test_transformer_avg_processing_time_zero() {
        let snapshot = TransformerMetricsSnapshot::default();
        assert_eq!(snapshot.avg_processing_time(), Duration::ZERO);
    }

    #[test]
    fn test_sink_metrics_config_default() {
        let config = SinkMetricsConfig::default();
        assert!(config.enabled);
        assert_eq!(config.interval, Duration::from_secs(10));
    }
}
