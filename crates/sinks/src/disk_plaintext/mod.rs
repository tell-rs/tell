//! Disk Plaintext Sink - Human-readable log storage
//!
//! Writes batches as human-readable text files with enrichment metadata.
//! Uses atomic rotation for zero data loss at high throughput.
//!
//! # Output Format
//!
//! Each line contains enrichment metadata followed by the raw message:
//! ```text
//! [2025-01-15T10:30:45.123Z] [EVENT] workspace=42 source_ip=192.168.1.100 msg_len=256 payload=...
//! ```
//!
//! # Features
//!
//! - **Human-readable**: Easy to grep, tail, and debug
//! - **Enriched**: Includes timestamp, batch type, workspace, source IP
//! - **Optional LZ4**: 70-85% smaller files with compression enabled
//! - **Atomic rotation**: Zero data loss during file rotation
//!
//! # Directory Structure
//!
//! ```text
//! logs/
//! └── workspace_42/
//!     └── 2025-01-15/
//!         └── 10/
//!             └── data.log
//! ```

use std::fmt::Write as FmtWrite;
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::BytesMut;
use cdp_protocol::Batch;
use chrono::{DateTime, Utc};
use metrics::{SinkMetricsConfig, SinkMetricsProvider, SinkMetricsSnapshot};
use tokio::sync::mpsc;

use crate::util::{
    AtomicRotationSink, ChainWriter, Lz4Writer, PlainTextWriter, RotationConfig, RotationInterval,
};

/// Configuration for disk plaintext sink
#[derive(Debug, Clone)]
pub struct DiskPlaintextConfig {
    /// Output directory path
    pub path: PathBuf,

    /// Buffer size for writes (default: 8MB)
    pub buffer_size: usize,

    /// Write queue size per workspace
    pub queue_size: usize,

    /// File rotation interval
    pub rotation_interval: RotationInterval,

    /// Enable LZ4 compression
    pub compression: bool,

    /// Flush interval for periodic flushing
    pub flush_interval: Duration,

    /// Enable metrics reporting
    pub metrics_enabled: bool,
}

impl Default for DiskPlaintextConfig {
    fn default() -> Self {
        Self {
            path: PathBuf::from("logs"),
            buffer_size: 8 * 1024 * 1024, // 8MB
            queue_size: 10000,
            rotation_interval: RotationInterval::Daily,
            compression: false,
            flush_interval: Duration::from_millis(100),
            metrics_enabled: true,
        }
    }
}

impl DiskPlaintextConfig {
    /// Create config with LZ4 compression enabled
    #[must_use]
    pub fn with_compression(mut self) -> Self {
        self.compression = true;
        self
    }

    /// Create config with custom path
    #[must_use]
    pub fn with_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.path = path.into();
        self
    }

    /// Create config with hourly rotation
    #[must_use]
    pub fn with_hourly_rotation(mut self) -> Self {
        self.rotation_interval = RotationInterval::Hourly;
        self
    }

    /// Create config with daily rotation
    #[must_use]
    pub fn with_daily_rotation(mut self) -> Self {
        self.rotation_interval = RotationInterval::Daily;
        self
    }
}

/// Metrics for disk plaintext sink
#[derive(Debug, Default)]
pub struct DiskPlaintextMetrics {
    /// Total batches received
    pub batches_received: AtomicU64,

    /// Total batches written
    pub batches_written: AtomicU64,

    /// Total lines written
    pub lines_written: AtomicU64,

    /// Total bytes written (before compression)
    pub bytes_written: AtomicU64,

    /// Write errors
    pub write_errors: AtomicU64,

    /// Queue full rejections
    pub queue_full: AtomicU64,
}

impl DiskPlaintextMetrics {
    /// Create new metrics instance
    pub const fn new() -> Self {
        Self {
            batches_received: AtomicU64::new(0),
            batches_written: AtomicU64::new(0),
            lines_written: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            write_errors: AtomicU64::new(0),
            queue_full: AtomicU64::new(0),
        }
    }

    /// Record a batch processed
    #[inline]
    pub fn record_batch(&self, lines: u64, bytes: u64) {
        self.batches_written.fetch_add(1, Ordering::Relaxed);
        self.lines_written.fetch_add(lines, Ordering::Relaxed);
        self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record a batch received
    #[inline]
    pub fn record_received(&self) {
        self.batches_received.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an error
    #[inline]
    pub fn record_error(&self) {
        self.write_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Record queue full rejection
    #[inline]
    pub fn record_queue_full(&self) {
        self.queue_full.fetch_add(1, Ordering::Relaxed);
    }

    /// Get snapshot of metrics
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            batches_received: self.batches_received.load(Ordering::Relaxed),
            batches_written: self.batches_written.load(Ordering::Relaxed),
            lines_written: self.lines_written.load(Ordering::Relaxed),
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
            write_errors: self.write_errors.load(Ordering::Relaxed),
            queue_full: self.queue_full.load(Ordering::Relaxed),
        }
    }
}

/// Point-in-time snapshot of metrics
#[derive(Debug, Clone, Copy, Default)]
pub struct MetricsSnapshot {
    pub batches_received: u64,
    pub batches_written: u64,
    pub lines_written: u64,
    pub bytes_written: u64,
    pub write_errors: u64,
    pub queue_full: u64,
}

/// Handle for accessing disk plaintext sink metrics
///
/// This can be obtained before running the sink and used for metrics reporting.
/// It holds an Arc to the metrics, so it remains valid even after the sink
/// is consumed by `run()`.
#[derive(Clone)]
pub struct DiskPlaintextSinkMetricsHandle {
    id: String,
    metrics: Arc<DiskPlaintextMetrics>,
    config: SinkMetricsConfig,
}

impl SinkMetricsProvider for DiskPlaintextSinkMetricsHandle {
    fn sink_id(&self) -> &str {
        &self.id
    }

    fn sink_type(&self) -> &str {
        "disk_plaintext"
    }

    fn metrics_config(&self) -> SinkMetricsConfig {
        self.config
    }

    fn snapshot(&self) -> SinkMetricsSnapshot {
        let s = self.metrics.snapshot();
        SinkMetricsSnapshot {
            batches_received: s.batches_received,
            batches_written: s.batches_written,
            messages_written: s.lines_written,
            bytes_written: s.bytes_written,
            write_errors: s.write_errors + s.queue_full,
            flush_count: s.batches_written,
        }
    }
}

/// Disk plaintext sink for human-readable log storage
///
/// Uses atomic rotation for zero data loss during file rotation.
/// The hot path (process_batch) is lock-free and non-blocking.
pub struct DiskPlaintextSink {
    /// Channel receiver for batches
    receiver: mpsc::Receiver<Arc<Batch>>,

    /// Atomic rotation system
    rotation: Arc<AtomicRotationSink>,

    /// Sink name for logging
    name: String,

    /// Metrics (Arc for sharing with metrics handle)
    metrics: Arc<DiskPlaintextMetrics>,

    /// Config for metrics reporting
    flush_interval: Duration,
}

impl DiskPlaintextSink {
    /// Create a new disk plaintext sink
    pub fn new(config: DiskPlaintextConfig, receiver: mpsc::Receiver<Arc<Batch>>) -> Self {
        Self::with_name(config, receiver, "disk_plaintext")
    }

    /// Create a new disk plaintext sink with a custom name
    pub fn with_name(
        config: DiskPlaintextConfig,
        receiver: mpsc::Receiver<Arc<Batch>>,
        name: impl Into<String>,
    ) -> Self {
        let flush_interval = config.flush_interval;
        let rotation_config = RotationConfig {
            base_path: config.path,
            rotation_interval: config.rotation_interval,
            file_prefix: "data".into(),
            buffer_size: config.buffer_size,
            queue_size: config.queue_size,
            flush_interval: config.flush_interval,
        };

        let rotation = if config.compression {
            let chain_writer = Lz4Writer::new(config.buffer_size);
            Arc::new(AtomicRotationSink::new(rotation_config, chain_writer))
        } else {
            let chain_writer = PlainTextWriterWithLogExt::new(config.buffer_size);
            Arc::new(AtomicRotationSink::new(rotation_config, chain_writer))
        };

        Self {
            receiver,
            rotation,
            name: name.into(),
            metrics: Arc::new(DiskPlaintextMetrics::new()),
            flush_interval,
        }
    }

    /// Get reference to metrics
    pub fn metrics(&self) -> &DiskPlaintextMetrics {
        &self.metrics
    }

    /// Get a metrics handle for reporting
    ///
    /// The handle implements `SinkMetricsProvider` and can be registered
    /// with the metrics reporter. It remains valid even after `run()` consumes
    /// the sink.
    pub fn metrics_handle(&self) -> DiskPlaintextSinkMetricsHandle {
        DiskPlaintextSinkMetricsHandle {
            id: self.name.clone(),
            metrics: Arc::clone(&self.metrics),
            config: SinkMetricsConfig {
                enabled: true,
                interval: self.flush_interval,
            },
        }
    }

    /// Get the sink name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Run the sink, processing batches until the channel closes
    pub async fn run(mut self) -> MetricsSnapshot {
        tracing::info!(sink = %self.name, "disk plaintext sink starting");

        // Spawn rotation checker task
        let rotation_for_task = Arc::clone(&self.rotation);
        let rotation_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                rotation_for_task.check_rotation_all().await;
            }
        });

        // Process batches
        while let Some(batch) = self.receiver.recv().await {
            self.metrics.record_received();
            self.process_batch(&batch);
        }

        // Shutdown
        rotation_handle.abort();
        self.rotation.stop().await;

        let snapshot = self.metrics.snapshot();
        tracing::info!(
            sink = %self.name,
            batches_received = snapshot.batches_received,
            batches_written = snapshot.batches_written,
            lines = snapshot.lines_written,
            bytes = snapshot.bytes_written,
            errors = snapshot.write_errors,
            "disk plaintext sink shutting down"
        );

        snapshot
    }

    /// Process a single batch - format and submit to rotation system
    fn process_batch(&self, batch: &Batch) {
        let workspace_id = batch.workspace_id();
        if workspace_id == 0 {
            tracing::warn!(sink = %self.name, "batch missing workspace_id, skipping");
            self.metrics.record_error();
            return;
        }

        // Convert workspace_id to string for file path
        let workspace_str = workspace_id.to_string();

        // Get buffer from pool
        let mut buffer = self.rotation.get_buffer();

        // Format batch to buffer
        let (line_count, bytes_written) = self.format_batch(batch, &mut buffer);

        if line_count == 0 {
            self.rotation.put_buffer(buffer);
            return;
        }

        // Submit to rotation system
        match self.rotation.submit(&workspace_str, buffer, line_count) {
            Ok(()) => {
                self.metrics
                    .record_batch(line_count as u64, bytes_written as u64);
            }
            Err(returned_buffer) => {
                self.metrics.record_queue_full();
                tracing::warn!(
                    sink = %self.name,
                    workspace = %workspace_str,
                    lines = line_count,
                    "queue full, dropping batch"
                );
                self.rotation.put_buffer(returned_buffer);
            }
        }
    }

    /// Format a batch to the buffer as human-readable lines
    fn format_batch(&self, batch: &Batch, buffer: &mut BytesMut) -> (usize, usize) {
        let timestamp: DateTime<Utc> = Utc::now();
        let timestamp_str = timestamp.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();

        let batch_type = batch.batch_type().as_str().to_uppercase();
        let workspace_id = batch.workspace_id();
        let source_ip = format_source_ip(batch.source_ip());

        let mut line_count = 0;
        let initial_len = buffer.len();

        // Pre-allocate a string buffer for formatting
        let mut line_buf = String::with_capacity(512);

        for i in 0..batch.message_count() {
            let Some(msg) = batch.get_message(i) else {
                continue;
            };

            line_buf.clear();

            // Format the line header
            let _ = write!(
                &mut line_buf,
                "[{}] [{}] workspace={} source_ip={} msg_len={} payload=",
                timestamp_str,
                batch_type,
                workspace_id,
                source_ip,
                msg.len()
            );

            // Format payload - try UTF-8 first, fall back to hex for binary
            if let Ok(text) = std::str::from_utf8(msg) {
                // Escape newlines and control chars for single-line output
                for ch in text.chars() {
                    match ch {
                        '\n' => line_buf.push_str("\\n"),
                        '\r' => line_buf.push_str("\\r"),
                        '\t' => line_buf.push_str("\\t"),
                        '\\' => line_buf.push_str("\\\\"),
                        c if c.is_control() => {
                            let _ = write!(&mut line_buf, "\\x{:02x}", c as u32);
                        }
                        c => line_buf.push(c),
                    }
                }
            } else {
                // Binary data - use hex encoding (truncate if too long)
                let max_hex_bytes = 256;
                let truncated = msg.len() > max_hex_bytes;
                let display_bytes = if truncated { max_hex_bytes } else { msg.len() };

                line_buf.push_str("0x");
                for byte in &msg[..display_bytes] {
                    let _ = write!(&mut line_buf, "{:02x}", byte);
                }
                if truncated {
                    let _ = write!(&mut line_buf, "...(+{} bytes)", msg.len() - max_hex_bytes);
                }
            }

            line_buf.push('\n');

            // Write to buffer
            buffer.extend_from_slice(line_buf.as_bytes());
            line_count += 1;
        }

        let bytes_written = buffer.len() - initial_len;
        (line_count, bytes_written)
    }
}

/// Format source IP for display
fn format_source_ip(ip: IpAddr) -> String {
    match ip {
        IpAddr::V4(v4) => v4.to_string(),
        IpAddr::V6(v6) => {
            // Check for IPv4-mapped IPv6 address (::ffff:x.x.x.x)
            if let Some(v4) = v6.to_ipv4_mapped() {
                v4.to_string()
            } else {
                v6.to_string()
            }
        }
    }
}

/// PlainText writer with .log extension instead of .txt
#[derive(Debug, Clone)]
struct PlainTextWriterWithLogExt {
    inner: PlainTextWriter,
}

impl PlainTextWriterWithLogExt {
    fn new(buffer_size: usize) -> Self {
        Self {
            inner: PlainTextWriter::new(buffer_size),
        }
    }
}

impl ChainWriter for PlainTextWriterWithLogExt {
    fn wrap(&self, file: std::fs::File) -> std::io::Result<Box<dyn crate::util::ChainWrite>> {
        self.inner.wrap(file)
    }

    fn file_extension(&self) -> &'static str {
        ".log"
    }
}

/// Errors from disk plaintext sink
#[derive(Debug, thiserror::Error)]
pub enum DiskPlaintextSinkError {
    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Failed to create directory
    #[error("failed to create directory: {path}")]
    CreateDir {
        path: String,
        #[source]
        source: std::io::Error,
    },

    /// Queue full
    #[error("write queue full for workspace {workspace_id}")]
    QueueFull { workspace_id: String },
}

#[cfg(test)]
#[path = "disk_plaintext_test.rs"]
mod disk_plaintext_test;
