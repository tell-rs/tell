//! Disk Binary Sink - High-performance binary storage
//!
//! Stores batches as binary files with 24-byte metadata headers and optional
//! LZ4 compression. Uses atomic rotation for zero data loss at 40M+ events/sec.
//!
//! # Features
//!
//! - **Atomic rotation**: Zero data loss during file rotation
//! - **Time bucketing**: Hour-based directories for efficient queries
//! - **Optional LZ4**: 60-80% smaller files with minimal CPU overhead
//! - **Buffer pooling**: Reusable buffers to reduce allocations
//!
//! # Example
//!
//! ```ignore
//! let config = DiskBinaryConfig::default()
//!     .with_path("/data/archive")
//!     .with_compression();
//!
//! let (tx, rx) = mpsc::channel(1000);
//! let sink = DiskBinarySink::new(config, rx);
//! tokio::spawn(sink.run());
//! ```
//!
//! # File Format
//!
//! Each message is stored as:
//! ```text
//! [24-byte Metadata][4-byte FB size][FlatBuffer data]
//! ```
//!
//! Metadata layout (24 bytes total):
//! - `batch_timestamp`: u64 (8 bytes) - Unix milliseconds when sink processed
//! - `source_ip`: [u8; 16] (16 bytes) - IPv6 format source IP
//!
//! # Directory Structure
//!
//! Files are organized by workspace and time:
//! ```text
//! {base_path}/{workspace_id}/{date}/{hour}/data.bin
//! {base_path}/{workspace_id}/{date}/{hour}/data.bin.lz4  # if compression enabled
//! ```

mod writer;

use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::BytesMut;
use metrics::{SinkMetricsConfig, SinkMetricsProvider, SinkMetricsSnapshot};
use cdp_protocol::Batch;
use tokio::sync::mpsc;

use crate::util::{AtomicRotationSink, BinaryWriter, RotationConfig, RotationInterval};

pub use writer::{BinaryReader, METADATA_SIZE, MessageMetadata};

/// Configuration for disk binary sink
#[derive(Debug, Clone)]
pub struct DiskBinaryConfig {
    /// Sink identifier (used in routing and metrics)
    pub id: String,

    /// Base path for output files
    pub path: PathBuf,

    /// Buffer size for writes (default: 32MB)
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

    /// Metrics reporting interval
    pub metrics_interval: Duration,
}

impl Default for DiskBinaryConfig {
    fn default() -> Self {
        Self {
            id: "disk_binary".into(),
            path: PathBuf::from("logs"),
            buffer_size: 32 * 1024 * 1024, // 32MB
            queue_size: 1000,
            rotation_interval: RotationInterval::Hourly,
            compression: false,
            flush_interval: Duration::from_millis(100),
            metrics_enabled: true,
            metrics_interval: Duration::from_secs(10),
        }
    }
}

impl DiskBinaryConfig {
    /// Create config with LZ4 compression enabled
    pub fn with_compression(mut self) -> Self {
        self.compression = true;
        self
    }

    /// Create config with custom path
    pub fn with_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.path = path.into();
        self
    }

    /// Create config with daily rotation
    pub fn with_daily_rotation(mut self) -> Self {
        self.rotation_interval = RotationInterval::Daily;
        self
    }
}

/// Metrics for disk binary sink
#[derive(Debug, Default)]
pub struct DiskBinarySinkMetrics {
    /// Total batches received
    pub batches_received: AtomicU64,

    /// Total batches written
    pub batches_written: AtomicU64,

    /// Total messages written
    pub messages_written: AtomicU64,

    /// Total bytes written (before compression)
    pub bytes_written: AtomicU64,

    /// Write errors
    pub write_errors: AtomicU64,

    /// Queue full rejections
    pub queue_full: AtomicU64,
}

impl DiskBinarySinkMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_batch(&self, messages: u64, bytes: u64) {
        self.batches_written.fetch_add(1, Ordering::Relaxed);
        self.messages_written.fetch_add(messages, Ordering::Relaxed);
        self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_received(&self) {
        self.batches_received.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_error(&self) {
        self.write_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_queue_full(&self) {
        self.queue_full.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            batches_received: self.batches_received.load(Ordering::Relaxed),
            batches_written: self.batches_written.load(Ordering::Relaxed),
            messages_written: self.messages_written.load(Ordering::Relaxed),
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
    pub messages_written: u64,
    pub bytes_written: u64,
    pub write_errors: u64,
    pub queue_full: u64,
}

/// Handle for accessing disk binary sink metrics
///
/// This can be obtained before running the sink and used for metrics reporting.
/// It holds an Arc to the metrics, so it remains valid even after the sink
/// is consumed by `run()`.
#[derive(Clone)]
pub struct DiskBinarySinkMetricsHandle {
    id: String,
    metrics: Arc<DiskBinarySinkMetrics>,
    config: SinkMetricsConfig,
}

impl DiskBinarySinkMetricsHandle {
    /// Get an extended snapshot including sink-specific metrics
    pub fn extended_snapshot(&self) -> MetricsSnapshot {
        self.metrics.snapshot()
    }
}

impl SinkMetricsProvider for DiskBinarySinkMetricsHandle {
    fn sink_id(&self) -> &str {
        &self.id
    }

    fn sink_type(&self) -> &str {
        "disk_binary"
    }

    fn metrics_config(&self) -> SinkMetricsConfig {
        self.config
    }

    fn snapshot(&self) -> SinkMetricsSnapshot {
        let s = self.metrics.snapshot();
        SinkMetricsSnapshot {
            batches_received: s.batches_received,
            batches_written: s.batches_written,
            messages_written: s.messages_written,
            bytes_written: s.bytes_written,
            write_errors: s.write_errors,
            flush_count: 0, // Not tracked separately
        }
    }
}

/// Disk binary sink for high-performance storage
///
/// Uses atomic rotation for zero data loss during file rotation.
/// The hot path (process_batch) is lock-free and non-blocking.
pub struct DiskBinarySink {
    /// Configuration
    config: DiskBinaryConfig,

    /// Channel receiver for batches
    receiver: mpsc::Receiver<Arc<Batch>>,

    /// Atomic rotation system (wrapped in Arc for sharing with rotation task)
    rotation: Arc<AtomicRotationSink>,

    /// Metrics (Arc for sharing with metrics handle)
    metrics: Arc<DiskBinarySinkMetrics>,
}

impl DiskBinarySink {
    /// Create a new disk binary sink
    pub fn new(config: DiskBinaryConfig, receiver: mpsc::Receiver<Arc<Batch>>) -> Self {
        let rotation_config = RotationConfig {
            base_path: config.path.clone(),
            rotation_interval: config.rotation_interval,
            file_prefix: "data".into(),
            buffer_size: config.buffer_size,
            queue_size: config.queue_size,
            flush_interval: config.flush_interval,
        };

        let chain_writer = BinaryWriter::new(config.buffer_size, config.compression);
        let rotation = Arc::new(AtomicRotationSink::new(rotation_config, chain_writer));

        Self {
            config,
            receiver,
            rotation,
            metrics: Arc::new(DiskBinarySinkMetrics::new()),
        }
    }

    /// Get reference to metrics
    pub fn metrics(&self) -> &DiskBinarySinkMetrics {
        &self.metrics
    }

    /// Get a metrics handle for reporting
    ///
    /// The handle implements `SinkMetricsProvider` and can be registered
    /// with the metrics reporter. It remains valid even after `run()` consumes
    /// the sink.
    pub fn metrics_handle(&self) -> DiskBinarySinkMetricsHandle {
        DiskBinarySinkMetricsHandle {
            id: self.config.id.clone(),
            metrics: Arc::clone(&self.metrics),
            config: SinkMetricsConfig {
                enabled: self.config.metrics_enabled,
                interval: self.config.metrics_interval,
            },
        }
    }

    /// Get the sink name/id
    pub fn name(&self) -> &str {
        &self.config.id
    }

    /// Run the sink, processing batches until the channel closes
    ///
    /// Returns the final metrics snapshot.
    pub async fn run(mut self) -> MetricsSnapshot {
        let sink_name = self.config.id.clone();
        tracing::info!(sink = %sink_name, "disk binary sink starting");

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
            sink = %sink_name,
            batches_received = snapshot.batches_received,
            batches_written = snapshot.batches_written,
            messages = snapshot.messages_written,
            bytes = snapshot.bytes_written,
            errors = snapshot.write_errors,
            "disk binary sink shutting down"
        );

        snapshot
    }

    /// Process a single batch - serialize and submit to rotation system
    fn process_batch(&self, batch: &Batch) {
        let workspace_id = batch.workspace_id();
        if workspace_id == 0 {
            tracing::warn!(sink = %self.config.id, "batch missing workspace_id, skipping");
            self.metrics.record_error();
            return;
        }

        // Convert workspace_id to string for file path
        let workspace_str = workspace_id.to_string();

        // Get buffer from pool
        let mut buffer = self.rotation.get_buffer();

        // Serialize batch to buffer
        let (message_count, bytes_written) = self.serialize_batch(batch, &mut buffer);

        if message_count == 0 {
            self.rotation.put_buffer(buffer);
            return;
        }

        // Submit to rotation system
        match self.rotation.submit(&workspace_str, buffer, message_count) {
            Ok(()) => {
                self.metrics
                    .record_batch(message_count as u64, bytes_written as u64);
            }
            Err(returned_buffer) => {
                self.metrics.record_queue_full();
                tracing::warn!(
                    sink = %self.config.id,
                    workspace = %workspace_str,
                    messages = message_count,
                    "queue full, dropping batch"
                );
                self.rotation.put_buffer(returned_buffer);
            }
        }
    }

    /// Serialize a batch to the buffer with metadata headers
    ///
    /// Format per message: [24-byte metadata][4-byte size][FlatBuffer data]
    fn serialize_batch(&self, batch: &Batch, buffer: &mut BytesMut) -> (usize, usize) {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        // Extract source IP (convert to IPv6 format)
        let source_ip = self.extract_source_ip(batch);

        let mut message_count = 0;
        let mut bytes_written = 0;

        for i in 0..batch.message_count() {
            let Some(msg) = batch.get_message(i) else {
                continue;
            };

            // Write metadata (24 bytes)
            buffer.extend_from_slice(&timestamp.to_be_bytes());
            buffer.extend_from_slice(&source_ip);

            // Write message size (4 bytes, big-endian)
            let msg_len = msg.len() as u32;
            buffer.extend_from_slice(&msg_len.to_be_bytes());

            // Write message data
            buffer.extend_from_slice(msg);

            message_count += 1;
            bytes_written += METADATA_SIZE + 4 + msg.len();
        }

        (message_count, bytes_written)
    }

    /// Extract source IP from batch as IPv6 bytes
    fn extract_source_ip(&self, batch: &Batch) -> [u8; 16] {
        let mut ip_bytes = [0u8; 16];

        match batch.source_ip() {
            IpAddr::V4(v4) => {
                // IPv4-mapped IPv6: ::ffff:x.x.x.x
                ip_bytes[10] = 0xff;
                ip_bytes[11] = 0xff;
                ip_bytes[12..16].copy_from_slice(&v4.octets());
            }
            IpAddr::V6(v6) => {
                ip_bytes = v6.octets();
            }
        }

        ip_bytes
    }
}

/// Errors from disk binary sink
#[derive(Debug, thiserror::Error)]
pub enum DiskBinarySinkError {
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
#[path = "disk_binary_test.rs"]
mod disk_binary_test;
