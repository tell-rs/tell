//! Stdout Sink - Human-readable debug output
//!
//! Outputs batches to stdout in a human-readable format for debugging.
//! Not intended for production use at high throughput.
//!
//! # Features
//!
//! - Prints batch summaries to stdout
//! - Configurable verbosity levels
//! - Optional message-level details
//!
//! # Example Output
//!
//! ```text
//! [BATCH] type=Event workspace=42 source=tcp_main count=500 bytes=25000
//! [BATCH] type=Log workspace=42 source=syslog count=100 bytes=8500
//! ```
//!
//! With verbose mode:
//! ```text
//! [BATCH] type=Event workspace=42 source=tcp_main count=500 bytes=25000
//!   → message 0 (156 bytes)
//!   → message 1 (142 bytes)
//!   ...
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use cdp_protocol::{Batch, BatchType};
use metrics::{SinkMetricsConfig, SinkMetricsProvider, SinkMetricsSnapshot};
use tokio::sync::mpsc;

// Note: serde_json dep can be removed from Cargo.toml if not used elsewhere in sinks

/// Stdout sink for debug output
pub struct StdoutSink {
    /// Channel receiver for batches
    receiver: mpsc::Receiver<Arc<Batch>>,

    /// Configuration
    config: StdoutConfig,

    /// Sink name for logging
    name: String,

    /// Metrics (Arc for sharing with metrics handle)
    metrics: Arc<StdoutSinkMetrics>,
}

/// Configuration for stdout sink
#[derive(Debug, Clone)]
pub struct StdoutConfig {
    /// Whether to show detailed message contents
    pub verbose: bool,

    /// Maximum messages to show in verbose mode (0 = all)
    pub max_messages: usize,

    /// Whether to show batch type
    pub show_batch_type: bool,

    /// Whether to show workspace ID
    pub show_workspace: bool,

    /// Whether to show source ID
    pub show_source: bool,

    /// Whether to show byte counts
    pub show_bytes: bool,
}

impl Default for StdoutConfig {
    fn default() -> Self {
        Self {
            verbose: false,
            max_messages: 10,
            show_batch_type: true,
            show_workspace: true,
            show_source: true,
            show_bytes: true,
        }
    }
}

impl StdoutConfig {
    /// Create a minimal config (just batch counts)
    pub fn minimal() -> Self {
        Self {
            verbose: false,
            max_messages: 0,
            show_batch_type: true,
            show_workspace: false,
            show_source: false,
            show_bytes: false,
        }
    }

    /// Create a verbose config (show message details)
    pub fn verbose() -> Self {
        Self {
            verbose: true,
            max_messages: 10,
            show_batch_type: true,
            show_workspace: true,
            show_source: true,
            show_bytes: true,
        }
    }
}

/// Metrics for stdout sink
#[derive(Debug, Default)]
pub struct StdoutSinkMetrics {
    /// Total batches received
    batches_received: AtomicU64,

    /// Total messages received
    messages_received: AtomicU64,

    /// Total bytes received
    bytes_received: AtomicU64,
}

impl StdoutSinkMetrics {
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
}

/// Point-in-time snapshot of stdout sink metrics
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub batches_received: u64,
    pub messages_received: u64,
    pub bytes_received: u64,
}

/// Handle for accessing stdout sink metrics
///
/// This can be obtained before running the sink and used for metrics reporting.
/// It holds an Arc to the metrics, so it remains valid even after the sink
/// is consumed by `run()`.
#[derive(Clone)]
pub struct StdoutSinkMetricsHandle {
    id: String,
    metrics: Arc<StdoutSinkMetrics>,
}

impl SinkMetricsProvider for StdoutSinkMetricsHandle {
    fn sink_id(&self) -> &str {
        &self.id
    }

    fn sink_type(&self) -> &str {
        "stdout"
    }

    fn metrics_config(&self) -> SinkMetricsConfig {
        SinkMetricsConfig {
            enabled: true,
            interval: Duration::from_secs(10),
        }
    }

    fn snapshot(&self) -> SinkMetricsSnapshot {
        let s = self.metrics.snapshot();
        // For stdout sink, "received" is effectively "written" since we output immediately
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

impl StdoutSink {
    /// Create a new stdout sink with default config
    pub fn new(receiver: mpsc::Receiver<Arc<Batch>>) -> Self {
        Self::with_config(receiver, StdoutConfig::default())
    }

    /// Create a new stdout sink with custom config
    pub fn with_config(receiver: mpsc::Receiver<Arc<Batch>>, config: StdoutConfig) -> Self {
        Self::with_name_and_config(receiver, "stdout", config)
    }

    /// Create a new stdout sink with custom name and config
    pub fn with_name_and_config(
        receiver: mpsc::Receiver<Arc<Batch>>,
        name: impl Into<String>,
        config: StdoutConfig,
    ) -> Self {
        Self {
            receiver,
            config,
            name: name.into(),
            metrics: Arc::new(StdoutSinkMetrics::new()),
        }
    }

    /// Get reference to metrics
    #[inline]
    pub fn metrics(&self) -> &StdoutSinkMetrics {
        &self.metrics
    }

    /// Get a metrics handle for reporting
    ///
    /// The handle implements `SinkMetricsProvider` and can be registered
    /// with the metrics reporter. It remains valid even after `run()` consumes
    /// the sink.
    pub fn metrics_handle(&self) -> StdoutSinkMetricsHandle {
        StdoutSinkMetricsHandle {
            id: self.name.clone(),
            metrics: Arc::clone(&self.metrics),
        }
    }

    /// Get the sink name
    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Run the sink, processing batches until the channel closes
    ///
    /// Returns the final metrics snapshot when the channel closes.
    pub async fn run(mut self) -> MetricsSnapshot {
        tracing::info!(sink = %self.name, "stdout sink starting");

        while let Some(batch) = self.receiver.recv().await {
            self.process_batch(&batch);
        }

        // Log final metrics
        let snapshot = self.metrics.snapshot();
        tracing::info!(
            sink = %self.name,
            batches = snapshot.batches_received,
            messages = snapshot.messages_received,
            bytes = snapshot.bytes_received,
            "stdout sink shutting down"
        );

        snapshot
    }

    /// Process a single batch - print to stdout and record metrics
    fn process_batch(&self, batch: &Batch) {
        // Record metrics
        self.metrics
            .record_batch(batch.count() as u64, batch.total_bytes() as u64);

        // Build output line
        let mut output = String::with_capacity(128);
        output.push_str("[BATCH]");

        if self.config.show_batch_type {
            output.push_str(&format!(" type={:?}", batch.batch_type()));
        }

        if self.config.show_workspace {
            output.push_str(&format!(" workspace={}", batch.workspace_id()));
        }

        if self.config.show_source {
            output.push_str(&format!(" source={}", batch.source_id()));
        }

        output.push_str(&format!(" count={}", batch.count()));

        if self.config.show_bytes {
            output.push_str(&format!(" bytes={}", batch.total_bytes()));
        }

        println!("{}", output);

        // For Snapshot batches, print JSON content (JSONL format - one per line)
        if batch.batch_type() == BatchType::Snapshot {
            for i in 0..batch.message_count() {
                if let Some(msg) = batch.get_message(i) {
                    // Print as compact JSON (JSONL)
                    println!("{}", String::from_utf8_lossy(msg));
                }
            }
        } else if self.config.verbose {
            // Show message details in verbose mode for other batch types
            let max = if self.config.max_messages == 0 {
                batch.message_count()
            } else {
                self.config.max_messages.min(batch.message_count())
            };

            let lengths = batch.lengths();
            for (i, &len) in lengths.iter().enumerate().take(max) {
                println!("  → message {} ({} bytes)", i, len);
            }

            if batch.message_count() > max {
                println!("  ... and {} more messages", batch.message_count() - max);
            }
        }
    }
}

#[cfg(test)]
#[path = "stdout_test.rs"]
mod stdout_test;
