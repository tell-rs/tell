//! Null sink - discards all data
//!
//! This sink is used for benchmarking the pipeline without any I/O overhead.
//! It receives batches, updates metrics, and immediately discards the data.
//!
//! # Use Cases
//!
//! - **Benchmarking**: Measure pure pipeline throughput without sink bottlenecks
//! - **Testing**: Validate routing and source configuration
//! - **Development**: Quick iteration without setting up external services
//!
//! # Example
//!
//! ```ignore
//! use sinks::null::NullSink;
//! use tokio::sync::mpsc;
//!
//! let (tx, rx) = mpsc::channel(1000);
//! let sink = NullSink::new(rx);
//!
//! // Register tx with router, then run sink
//! sink.run().await;
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use metrics::{SinkMetricsConfig, SinkMetricsProvider, SinkMetricsSnapshot};
use cdp_protocol::Batch;
use tokio::sync::mpsc;

/// Configuration for null sink
#[derive(Debug, Clone)]
pub struct NullSinkConfig {
    /// Sink identifier
    pub id: String,
    /// Whether metrics reporting is enabled
    pub metrics_enabled: bool,
    /// Metrics reporting interval
    pub metrics_interval: Duration,
}

impl Default for NullSinkConfig {
    fn default() -> Self {
        Self {
            id: "null".into(),
            metrics_enabled: true,
            metrics_interval: Duration::from_secs(10),
        }
    }
}

/// Null sink that discards all received batches
///
/// This sink simply receives batches, records metrics, and drops them.
/// It's useful for benchmarking the pipeline without I/O bottlenecks.
pub struct NullSink {
    /// Configuration
    config: NullSinkConfig,

    /// Channel receiver for incoming batches
    receiver: mpsc::Receiver<Arc<Batch>>,

    /// Metrics for this sink (Arc for sharing with metrics handle)
    metrics: Arc<NullSinkMetrics>,
}

/// Metrics for the null sink
#[derive(Debug, Default)]
pub struct NullSinkMetrics {
    /// Total batches received
    batches_received: AtomicU64,

    /// Total messages received (sum of batch.count())
    messages_received: AtomicU64,

    /// Total bytes received (sum of batch.total_bytes())
    bytes_received: AtomicU64,
}

impl NullSinkMetrics {
    /// Create new metrics instance
    #[inline]
    pub const fn new() -> Self {
        Self {
            batches_received: AtomicU64::new(0),
            messages_received: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
        }
    }

    /// Record a received batch
    #[inline]
    pub fn record_batch(&self, message_count: u64, byte_count: u64) {
        self.batches_received.fetch_add(1, Ordering::Relaxed);
        self.messages_received
            .fetch_add(message_count, Ordering::Relaxed);
        self.bytes_received.fetch_add(byte_count, Ordering::Relaxed);
    }

    /// Get batches received count
    #[inline]
    pub fn batches_received(&self) -> u64 {
        self.batches_received.load(Ordering::Relaxed)
    }

    /// Get messages received count
    #[inline]
    pub fn messages_received(&self) -> u64 {
        self.messages_received.load(Ordering::Relaxed)
    }

    /// Get bytes received count
    #[inline]
    pub fn bytes_received(&self) -> u64 {
        self.bytes_received.load(Ordering::Relaxed)
    }

    /// Get snapshot of metrics
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            batches_received: self.batches_received.load(Ordering::Relaxed),
            messages_received: self.messages_received.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
        }
    }

    /// Reset all metrics to zero
    pub fn reset(&self) {
        self.batches_received.store(0, Ordering::Relaxed);
        self.messages_received.store(0, Ordering::Relaxed);
        self.bytes_received.store(0, Ordering::Relaxed);
    }
}

/// Point-in-time snapshot of null sink metrics
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub batches_received: u64,
    pub messages_received: u64,
    pub bytes_received: u64,
}

/// Handle for accessing null sink metrics
///
/// This can be obtained before running the sink and used for metrics reporting.
/// It holds an Arc to the metrics, so it remains valid even after the sink
/// is consumed by `run()`.
#[derive(Clone)]
pub struct NullSinkMetricsHandle {
    id: String,
    metrics: Arc<NullSinkMetrics>,
    config: SinkMetricsConfig,
}

impl SinkMetricsProvider for NullSinkMetricsHandle {
    fn sink_id(&self) -> &str {
        &self.id
    }

    fn sink_type(&self) -> &str {
        "null"
    }

    fn metrics_config(&self) -> SinkMetricsConfig {
        self.config
    }

    fn snapshot(&self) -> SinkMetricsSnapshot {
        let s = self.metrics.snapshot();
        // For null sink, "received" is effectively "written" since we discard immediately
        SinkMetricsSnapshot {
            batches_received: s.batches_received,
            batches_written: s.batches_received,
            messages_written: s.messages_received,
            bytes_written: s.bytes_received,
            write_errors: 0,
            flush_count: 0,
        }
    }
}

impl NullSink {
    /// Create a new null sink with the given receiver
    pub fn new(receiver: mpsc::Receiver<Arc<Batch>>) -> Self {
        Self::with_config(NullSinkConfig::default(), receiver)
    }

    /// Create a new null sink with a custom name
    pub fn with_name(receiver: mpsc::Receiver<Arc<Batch>>, name: impl Into<String>) -> Self {
        let config = NullSinkConfig {
            id: name.into(),
            ..Default::default()
        };
        Self::with_config(config, receiver)
    }

    /// Create a new null sink with full configuration
    pub fn with_config(config: NullSinkConfig, receiver: mpsc::Receiver<Arc<Batch>>) -> Self {
        Self {
            config,
            receiver,
            metrics: Arc::new(NullSinkMetrics::new()),
        }
    }

    /// Get reference to metrics
    #[inline]
    pub fn metrics(&self) -> &NullSinkMetrics {
        &self.metrics
    }

    /// Get a metrics handle for reporting
    ///
    /// The handle implements `SinkMetricsProvider` and can be registered
    /// with the metrics reporter. It remains valid even after `run()` consumes
    /// the sink.
    pub fn metrics_handle(&self) -> NullSinkMetricsHandle {
        NullSinkMetricsHandle {
            id: self.config.id.clone(),
            metrics: Arc::clone(&self.metrics),
            config: SinkMetricsConfig {
                enabled: self.config.metrics_enabled,
                interval: self.config.metrics_interval,
            },
        }
    }

    /// Get the sink name
    #[inline]
    pub fn name(&self) -> &str {
        &self.config.id
    }

    /// Run the sink, consuming batches until the channel closes
    ///
    /// This is the main loop - it receives batches, updates metrics,
    /// and immediately discards the data.
    ///
    /// Returns the final metrics snapshot when the channel closes.
    pub async fn run(mut self) -> MetricsSnapshot {
        let sink_name = self.config.id.clone();
        tracing::info!(sink = %sink_name, "null sink starting");

        while let Some(batch) = self.receiver.recv().await {
            // Record metrics using accessor methods
            self.metrics
                .record_batch(batch.count() as u64, batch.total_bytes() as u64);

            // Batch is dropped here - Arc refcount decrements
            // If this is the last reference, memory is freed
        }

        // Log final metrics
        let snapshot = self.metrics.snapshot();
        tracing::info!(
            sink = %sink_name,
            batches = snapshot.batches_received,
            messages = snapshot.messages_received,
            bytes = snapshot.bytes_received,
            "null sink shutting down"
        );

        snapshot
    }

    /// Run the sink with a callback for each batch
    ///
    /// Useful for testing or custom processing before discard.
    pub async fn run_with_callback<F>(mut self, mut callback: F) -> MetricsSnapshot
    where
        F: FnMut(&Batch),
    {
        while let Some(batch) = self.receiver.recv().await {
            callback(&batch);
            self.metrics
                .record_batch(batch.count() as u64, batch.total_bytes() as u64);
        }

        self.metrics.snapshot()
    }
}

#[cfg(test)]
#[path = "null_test.rs"]
mod null_test;
