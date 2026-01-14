//! Pipeline router metrics
//!
//! Atomic counters for tracking router performance.
//! All operations use relaxed ordering for maximum performance.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Metrics for the pipeline router
///
/// All counters use relaxed ordering for maximum performance.
/// These metrics are eventually consistent, not real-time.
///
/// # Thread Safety
///
/// All methods are safe to call from multiple threads concurrently.
/// The atomic operations ensure no data races, though values may be
/// slightly stale when read.
#[derive(Debug, Default)]
pub struct RouterMetrics {
    /// Total batches received from sources
    batches_received: AtomicU64,

    /// Batches successfully routed to at least one sink
    batches_routed: AtomicU64,

    /// Batches dropped (no matching route)
    batches_dropped: AtomicU64,

    /// Individual sink sends that succeeded
    sink_sends_success: AtomicU64,

    /// Individual sink sends that failed (backpressure or closed)
    sink_sends_failed: AtomicU64,

    /// Times a sink channel was full (backpressure)
    backpressure_events: AtomicU64,

    /// Total messages processed (sum of batch.count)
    messages_processed: AtomicU64,

    /// Total bytes processed (sum of batch.total_bytes)
    bytes_processed: AtomicU64,

    /// Successful transform operations
    transforms_success: AtomicU64,

    /// Failed transform operations
    transforms_failed: AtomicU64,

    /// Total transform duration in nanoseconds
    transform_duration_ns: AtomicU64,
}

impl RouterMetrics {
    /// Create new metrics instance with all counters at zero
    #[inline]
    pub const fn new() -> Self {
        Self {
            batches_received: AtomicU64::new(0),
            batches_routed: AtomicU64::new(0),
            batches_dropped: AtomicU64::new(0),
            sink_sends_success: AtomicU64::new(0),
            sink_sends_failed: AtomicU64::new(0),
            backpressure_events: AtomicU64::new(0),
            messages_processed: AtomicU64::new(0),
            bytes_processed: AtomicU64::new(0),
            transforms_success: AtomicU64::new(0),
            transforms_failed: AtomicU64::new(0),
            transform_duration_ns: AtomicU64::new(0),
        }
    }

    /// Record a batch received from a source
    ///
    /// Call this when a batch enters the router, before routing decisions.
    #[inline]
    pub fn record_received(&self, message_count: u64, byte_count: u64) {
        self.batches_received.fetch_add(1, Ordering::Relaxed);
        self.messages_processed
            .fetch_add(message_count, Ordering::Relaxed);
        self.bytes_processed
            .fetch_add(byte_count, Ordering::Relaxed);
    }

    /// Record a batch successfully routed to at least one sink
    #[inline]
    pub fn record_routed(&self) {
        self.batches_routed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a batch dropped (no matching sinks or all sends failed)
    #[inline]
    pub fn record_dropped(&self) {
        self.batches_dropped.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a successful send to a sink
    #[inline]
    pub fn record_sink_send_success(&self) {
        self.sink_sends_success.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a failed send to a sink
    #[inline]
    pub fn record_sink_send_failed(&self) {
        self.sink_sends_failed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a backpressure event (sink channel full)
    #[inline]
    pub fn record_backpressure(&self) {
        self.backpressure_events.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a successful transform operation
    #[inline]
    pub fn record_transform_success(&self, duration: Duration) {
        self.transforms_success.fetch_add(1, Ordering::Relaxed);
        self.transform_duration_ns
            .fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
    }

    /// Record a failed transform operation
    #[inline]
    pub fn record_transform_error(&self) {
        self.transforms_failed.fetch_add(1, Ordering::Relaxed);
    }

    /// Get a snapshot of all metrics
    ///
    /// Returns a point-in-time copy of all counters.
    #[inline]
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            batches_received: self.batches_received.load(Ordering::Relaxed),
            batches_routed: self.batches_routed.load(Ordering::Relaxed),
            batches_dropped: self.batches_dropped.load(Ordering::Relaxed),
            sink_sends_success: self.sink_sends_success.load(Ordering::Relaxed),
            sink_sends_failed: self.sink_sends_failed.load(Ordering::Relaxed),
            backpressure_events: self.backpressure_events.load(Ordering::Relaxed),
            messages_processed: self.messages_processed.load(Ordering::Relaxed),
            bytes_processed: self.bytes_processed.load(Ordering::Relaxed),
            transforms_success: self.transforms_success.load(Ordering::Relaxed),
            transforms_failed: self.transforms_failed.load(Ordering::Relaxed),
            transform_duration_ns: self.transform_duration_ns.load(Ordering::Relaxed),
        }
    }

    /// Reset all metrics to zero
    ///
    /// Useful for testing or periodic metric collection.
    pub fn reset(&self) {
        self.batches_received.store(0, Ordering::Relaxed);
        self.batches_routed.store(0, Ordering::Relaxed);
        self.batches_dropped.store(0, Ordering::Relaxed);
        self.sink_sends_success.store(0, Ordering::Relaxed);
        self.sink_sends_failed.store(0, Ordering::Relaxed);
        self.backpressure_events.store(0, Ordering::Relaxed);
        self.messages_processed.store(0, Ordering::Relaxed);
        self.bytes_processed.store(0, Ordering::Relaxed);
        self.transforms_success.store(0, Ordering::Relaxed);
        self.transforms_failed.store(0, Ordering::Relaxed);
        self.transform_duration_ns.store(0, Ordering::Relaxed);
    }

    // Direct accessors for individual metrics (for logging)

    /// Get batches received count
    #[inline]
    pub fn batches_received(&self) -> u64 {
        self.batches_received.load(Ordering::Relaxed)
    }

    /// Get batches routed count
    #[inline]
    pub fn batches_routed(&self) -> u64 {
        self.batches_routed.load(Ordering::Relaxed)
    }

    /// Get batches dropped count
    #[inline]
    pub fn batches_dropped(&self) -> u64 {
        self.batches_dropped.load(Ordering::Relaxed)
    }

    /// Get backpressure events count
    #[inline]
    pub fn backpressure_events(&self) -> u64 {
        self.backpressure_events.load(Ordering::Relaxed)
    }

    /// Get messages processed count
    #[inline]
    pub fn messages_processed(&self) -> u64 {
        self.messages_processed.load(Ordering::Relaxed)
    }

    /// Get bytes processed count
    #[inline]
    pub fn bytes_processed(&self) -> u64 {
        self.bytes_processed.load(Ordering::Relaxed)
    }
}

/// Point-in-time snapshot of router metrics
///
/// This is a simple struct that can be copied, compared, and serialized.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MetricsSnapshot {
    /// Total batches received from sources
    pub batches_received: u64,
    /// Batches successfully routed
    pub batches_routed: u64,
    /// Batches dropped (no route or all sinks failed)
    pub batches_dropped: u64,
    /// Successful sink sends
    pub sink_sends_success: u64,
    /// Failed sink sends
    pub sink_sends_failed: u64,
    /// Backpressure events
    pub backpressure_events: u64,
    /// Total messages processed
    pub messages_processed: u64,
    /// Total bytes processed
    pub bytes_processed: u64,
    /// Successful transform operations
    pub transforms_success: u64,
    /// Failed transform operations
    pub transforms_failed: u64,
    /// Total transform duration in nanoseconds
    pub transform_duration_ns: u64,
}

impl MetricsSnapshot {
    /// Calculate routing success rate (0.0 - 1.0)
    ///
    /// Returns None if no batches have been received.
    #[inline]
    pub fn routing_success_rate(&self) -> Option<f64> {
        if self.batches_received == 0 {
            None
        } else {
            Some(self.batches_routed as f64 / self.batches_received as f64)
        }
    }

    /// Calculate sink send success rate (0.0 - 1.0)
    ///
    /// Returns None if no sink sends have been attempted.
    #[inline]
    pub fn sink_success_rate(&self) -> Option<f64> {
        let total = self.sink_sends_success + self.sink_sends_failed;
        if total == 0 {
            None
        } else {
            Some(self.sink_sends_success as f64 / total as f64)
        }
    }

    /// Calculate the difference from another snapshot
    ///
    /// Useful for calculating rates over time intervals.
    #[inline]
    pub fn diff(&self, previous: &MetricsSnapshot) -> MetricsSnapshot {
        MetricsSnapshot {
            batches_received: self
                .batches_received
                .saturating_sub(previous.batches_received),
            batches_routed: self.batches_routed.saturating_sub(previous.batches_routed),
            batches_dropped: self
                .batches_dropped
                .saturating_sub(previous.batches_dropped),
            sink_sends_success: self
                .sink_sends_success
                .saturating_sub(previous.sink_sends_success),
            sink_sends_failed: self
                .sink_sends_failed
                .saturating_sub(previous.sink_sends_failed),
            backpressure_events: self
                .backpressure_events
                .saturating_sub(previous.backpressure_events),
            messages_processed: self
                .messages_processed
                .saturating_sub(previous.messages_processed),
            bytes_processed: self
                .bytes_processed
                .saturating_sub(previous.bytes_processed),
            transforms_success: self
                .transforms_success
                .saturating_sub(previous.transforms_success),
            transforms_failed: self
                .transforms_failed
                .saturating_sub(previous.transforms_failed),
            transform_duration_ns: self
                .transform_duration_ns
                .saturating_sub(previous.transform_duration_ns),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_new() {
        let metrics = RouterMetrics::new();
        let snapshot = metrics.snapshot();

        assert_eq!(snapshot.batches_received, 0);
        assert_eq!(snapshot.batches_routed, 0);
        assert_eq!(snapshot.batches_dropped, 0);
        assert_eq!(snapshot.messages_processed, 0);
    }

    #[test]
    fn test_record_received() {
        let metrics = RouterMetrics::new();

        metrics.record_received(100, 5000);
        metrics.record_received(50, 2500);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.batches_received, 2);
        assert_eq!(snapshot.messages_processed, 150);
        assert_eq!(snapshot.bytes_processed, 7500);
    }

    #[test]
    fn test_record_routed() {
        let metrics = RouterMetrics::new();

        metrics.record_routed();
        metrics.record_routed();
        metrics.record_routed();

        assert_eq!(metrics.batches_routed(), 3);
    }

    #[test]
    fn test_record_dropped() {
        let metrics = RouterMetrics::new();

        metrics.record_dropped();
        metrics.record_dropped();

        assert_eq!(metrics.batches_dropped(), 2);
    }

    #[test]
    fn test_record_sink_sends() {
        let metrics = RouterMetrics::new();

        metrics.record_sink_send_success();
        metrics.record_sink_send_success();
        metrics.record_sink_send_failed();

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.sink_sends_success, 2);
        assert_eq!(snapshot.sink_sends_failed, 1);
    }

    #[test]
    fn test_record_backpressure() {
        let metrics = RouterMetrics::new();

        metrics.record_backpressure();
        metrics.record_backpressure();

        assert_eq!(metrics.backpressure_events(), 2);
    }

    #[test]
    fn test_metrics_reset() {
        let metrics = RouterMetrics::new();

        metrics.record_received(50, 1000);
        metrics.record_dropped();
        metrics.record_backpressure();
        metrics.reset();

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.batches_received, 0);
        assert_eq!(snapshot.batches_dropped, 0);
        assert_eq!(snapshot.backpressure_events, 0);
        assert_eq!(snapshot.messages_processed, 0);
        assert_eq!(snapshot.bytes_processed, 0);
    }

    #[test]
    fn test_snapshot_routing_success_rate() {
        let snapshot = MetricsSnapshot {
            batches_received: 100,
            batches_routed: 95,
            batches_dropped: 5,
            ..Default::default()
        };

        assert_eq!(snapshot.routing_success_rate(), Some(0.95));
    }

    #[test]
    fn test_snapshot_routing_success_rate_empty() {
        let snapshot = MetricsSnapshot::default();
        assert_eq!(snapshot.routing_success_rate(), None);
    }

    #[test]
    fn test_snapshot_sink_success_rate() {
        let snapshot = MetricsSnapshot {
            sink_sends_success: 90,
            sink_sends_failed: 10,
            ..Default::default()
        };

        assert_eq!(snapshot.sink_success_rate(), Some(0.9));
    }

    #[test]
    fn test_snapshot_sink_success_rate_empty() {
        let snapshot = MetricsSnapshot::default();
        assert_eq!(snapshot.sink_success_rate(), None);
    }

    #[test]
    fn test_snapshot_diff() {
        let prev = MetricsSnapshot {
            batches_received: 100,
            batches_routed: 95,
            batches_dropped: 5,
            messages_processed: 10000,
            bytes_processed: 500000,
            ..Default::default()
        };

        let current = MetricsSnapshot {
            batches_received: 200,
            batches_routed: 190,
            batches_dropped: 10,
            messages_processed: 20000,
            bytes_processed: 1000000,
            ..Default::default()
        };

        let diff = current.diff(&prev);
        assert_eq!(diff.batches_received, 100);
        assert_eq!(diff.batches_routed, 95);
        assert_eq!(diff.batches_dropped, 5);
        assert_eq!(diff.messages_processed, 10000);
        assert_eq!(diff.bytes_processed, 500000);
    }

    #[test]
    fn test_snapshot_diff_saturating() {
        let prev = MetricsSnapshot {
            batches_received: 100,
            ..Default::default()
        };

        let current = MetricsSnapshot {
            batches_received: 50, // Less than previous (shouldn't happen, but handle gracefully)
            ..Default::default()
        };

        let diff = current.diff(&prev);
        assert_eq!(diff.batches_received, 0); // Saturating sub prevents underflow
    }

    #[test]
    fn test_metrics_default() {
        let metrics = RouterMetrics::default();
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot, MetricsSnapshot::default());
    }

    #[test]
    fn test_concurrent_access() {
        use std::sync::Arc;
        use std::thread;

        let metrics = Arc::new(RouterMetrics::new());
        let mut handles = vec![];

        // Spawn multiple threads incrementing metrics
        for _ in 0..4 {
            let m = Arc::clone(&metrics);
            handles.push(thread::spawn(move || {
                for _ in 0..1000 {
                    m.record_received(1, 100);
                    m.record_routed();
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.batches_received, 4000);
        assert_eq!(snapshot.batches_routed, 4000);
        assert_eq!(snapshot.messages_processed, 4000);
        assert_eq!(snapshot.bytes_processed, 400000);
    }
}
