//! Disk Plaintext Sink - Human-readable log storage
//!
//! Writes batches as human-readable text files with decoded FlatBuffer fields.
//! Uses atomic rotation for zero data loss at high throughput.
//!
//! # Output Format
//!
//! Events are decoded and written with structured fields:
//! ```text
//! [2025-01-15T10:30:45.123Z] [TRACK] device_id=abc123 session_id=xyz789 event_name=button_click source_ip=192.168.1.100 payload={...}
//! ```
//!
//! Logs include level and service information:
//! ```text
//! [2025-01-15T10:30:45.123Z] [INFO] session_id=xyz789 source=api service=auth source_ip=192.168.1.100 payload=request completed
//! ```
//!
//! # Features
//!
//! - **Human-readable**: Easy to grep, tail, and debug
//! - **Decoded**: FlatBuffer messages decoded to structured fields
//! - **Split by type**: Separate files for events, logs, and snapshots
//! - **Optional LZ4**: 70-85% smaller files with compression enabled
//! - **Atomic rotation**: Zero data loss during file rotation
//! - **Write retry**: 3 retries with 10ms delay on transient failures
//!
//! # Directory Structure
//!
//! ```text
//! logs/
//! └── workspace_42/
//!     └── 2025-01-15/
//!         ├── events.log    # Product analytics events
//!         ├── logs.log      # Structured logs
//!         ├── snapshots.log # Connector snapshots
//!         ├── metrics.log   # Metrics (if used)
//!         ├── traces.log    # Traces (if used)
//!         └── raw.log       # Failed decodes only
//! ```

use std::fmt::Write as FmtWrite;
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tell_protocol::{
    Batch, FlatBatch, SchemaType,
    decode_event_data, decode_log_data, DecodedEvent, DecodedLogEntry,
};
use chrono::Utc;
use tell_metrics::{SinkMetricsConfig, SinkMetricsProvider, SinkMetricsSnapshot};
use tokio::sync::mpsc;

use crate::util::{
    AtomicRotationSink, ChainWriter, Lz4Writer, PlainTextWriter, RotationConfig, RotationInterval,
    RateLimitedLogger,
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
/// Messages are decoded from FlatBuffer format and written to separate files
/// based on schema type.
pub struct DiskPlaintextSink {
    /// Channel receiver for batches
    receiver: mpsc::Receiver<Arc<Batch>>,

    /// Rotation sink for events (SchemaType::Event)
    events_rotation: Arc<AtomicRotationSink>,

    /// Rotation sink for logs (SchemaType::Log)
    logs_rotation: Arc<AtomicRotationSink>,

    /// Rotation sink for snapshots (SchemaType::Snapshot)
    snapshots_rotation: Arc<AtomicRotationSink>,

    /// Rotation sink for metrics (SchemaType::Metric)
    metrics_rotation: Arc<AtomicRotationSink>,

    /// Rotation sink for traces (SchemaType::Trace)
    traces_rotation: Arc<AtomicRotationSink>,

    /// Rotation sink for raw/failed decodes
    raw_rotation: Arc<AtomicRotationSink>,

    /// Sink name for logging
    name: String,

    /// Metrics (Arc for sharing with metrics handle)
    metrics: Arc<DiskPlaintextMetrics>,

    /// Config for metrics reporting
    flush_interval: Duration,

    /// Rate-limited error logger for decode failures
    error_logger: RateLimitedLogger,
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

        // Helper to create rotation config with specific file prefix
        let make_rotation_config = |prefix: &str| RotationConfig {
            base_path: config.path.clone(),
            rotation_interval: config.rotation_interval,
            file_prefix: prefix.into(),
            buffer_size: config.buffer_size,
            queue_size: config.queue_size,
            flush_interval: config.flush_interval,
            // Use default retry settings (3 retries, 10ms delay - matches Go)
            ..Default::default()
        };

        // Helper to create rotation sink with appropriate writer
        let make_rotation_sink = |prefix: &str| -> Arc<AtomicRotationSink> {
            if config.compression {
                Arc::new(AtomicRotationSink::new(
                    make_rotation_config(prefix),
                    Lz4WriterWithLogExt::new(config.buffer_size),
                ))
            } else {
                Arc::new(AtomicRotationSink::new(
                    make_rotation_config(prefix),
                    PlainTextWriterWithLogExt::new(config.buffer_size),
                ))
            }
        };

        Self {
            receiver,
            events_rotation: make_rotation_sink("events"),
            logs_rotation: make_rotation_sink("logs"),
            snapshots_rotation: make_rotation_sink("snapshots"),
            metrics_rotation: make_rotation_sink("metrics"),
            traces_rotation: make_rotation_sink("traces"),
            raw_rotation: make_rotation_sink("raw"),
            name: name.into(),
            metrics: Arc::new(DiskPlaintextMetrics::new()),
            flush_interval,
            error_logger: RateLimitedLogger::default(),
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

        // Spawn rotation checker tasks for all file types
        let events_rotation = Arc::clone(&self.events_rotation);
        let logs_rotation = Arc::clone(&self.logs_rotation);
        let snapshots_rotation = Arc::clone(&self.snapshots_rotation);
        let metrics_rotation = Arc::clone(&self.metrics_rotation);
        let traces_rotation = Arc::clone(&self.traces_rotation);
        let raw_rotation = Arc::clone(&self.raw_rotation);

        let rotation_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                // Check all rotations in parallel
                tokio::join!(
                    events_rotation.check_rotation_all(),
                    logs_rotation.check_rotation_all(),
                    snapshots_rotation.check_rotation_all(),
                    metrics_rotation.check_rotation_all(),
                    traces_rotation.check_rotation_all(),
                    raw_rotation.check_rotation_all(),
                );
            }
        });

        // Process batches
        while let Some(batch) = self.receiver.recv().await {
            self.metrics.record_received();
            self.process_batch(&batch);
        }

        // Shutdown all rotation sinks
        rotation_handle.abort();
        tokio::join!(
            self.events_rotation.stop(),
            self.logs_rotation.stop(),
            self.snapshots_rotation.stop(),
            self.metrics_rotation.stop(),
            self.traces_rotation.stop(),
            self.raw_rotation.stop(),
        );

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

    /// Process a single batch - decode, format, and submit to rotation system
    fn process_batch(&self, batch: &Batch) {
        let workspace_id = batch.workspace_id();
        if workspace_id == 0 {
            tracing::warn!(sink = %self.name, "batch missing workspace_id, skipping");
            self.metrics.record_error();
            return;
        }

        let workspace_str = workspace_id.to_string();
        let source_ip = format_source_ip(batch.source_ip());
        let timestamp_str = Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();

        // Buffers for each file type
        let mut events_buffer = self.events_rotation.get_buffer();
        let mut logs_buffer = self.logs_rotation.get_buffer();
        let mut snapshots_buffer = self.snapshots_rotation.get_buffer();
        let mut metrics_buffer = self.metrics_rotation.get_buffer();
        let mut traces_buffer = self.traces_rotation.get_buffer();
        let mut raw_buffer = self.raw_rotation.get_buffer();

        let mut events_count = 0usize;
        let mut logs_count = 0usize;
        let mut snapshots_count = 0usize;
        let mut metrics_count = 0usize;
        let mut traces_count = 0usize;
        let mut raw_count = 0usize;

        // Pre-allocate line buffer
        let mut line_buf = String::with_capacity(1024);

        // Process each message
        for i in 0..batch.message_count() {
            let Some(msg) = batch.get_message(i) else {
                continue;
            };

            // Try to parse as FlatBuffer
            let flat_batch = match FlatBatch::parse(msg) {
                Ok(fb) => fb,
                Err(e) => {
                    // Not a valid FlatBuffer - write to raw.log
                    self.error_logger.error("FlatBuffer parse failed", &e);
                    self.format_raw_message(&timestamp_str, &source_ip, msg, &mut line_buf);
                    raw_buffer.extend_from_slice(line_buf.as_bytes());
                    raw_count += 1;
                    continue;
                }
            };

            let schema_type = flat_batch.schema_type();
            let data = match flat_batch.data() {
                Ok(d) => d,
                Err(e) => {
                    self.error_logger.error("FlatBuffer data missing", &e);
                    self.format_raw_message(&timestamp_str, &source_ip, msg, &mut line_buf);
                    raw_buffer.extend_from_slice(line_buf.as_bytes());
                    raw_count += 1;
                    continue;
                }
            };

            match schema_type {
                SchemaType::Event => {
                    match decode_event_data(data) {
                        Ok(events) => {
                            for event in events {
                                self.format_event(&timestamp_str, &source_ip, &event, &mut line_buf);
                                events_buffer.extend_from_slice(line_buf.as_bytes());
                                events_count += 1;
                            }
                        }
                        Err(e) => {
                            self.error_logger.error("Event decode failed", &e);
                            self.format_raw_message(&timestamp_str, &source_ip, msg, &mut line_buf);
                            raw_buffer.extend_from_slice(line_buf.as_bytes());
                            raw_count += 1;
                        }
                    }
                }
                SchemaType::Log => {
                    match decode_log_data(data) {
                        Ok(logs) => {
                            for log_entry in logs {
                                self.format_log(&timestamp_str, &source_ip, &log_entry, &mut line_buf);
                                logs_buffer.extend_from_slice(line_buf.as_bytes());
                                logs_count += 1;
                            }
                        }
                        Err(e) => {
                            self.error_logger.error("Log decode failed", &e);
                            self.format_raw_message(&timestamp_str, &source_ip, msg, &mut line_buf);
                            raw_buffer.extend_from_slice(line_buf.as_bytes());
                            raw_count += 1;
                        }
                    }
                }
                SchemaType::Snapshot => {
                    self.format_schema_message(&timestamp_str, &source_ip, "SNAPSHOT", data, &mut line_buf);
                    snapshots_buffer.extend_from_slice(line_buf.as_bytes());
                    snapshots_count += 1;
                }
                SchemaType::Metric => {
                    self.format_schema_message(&timestamp_str, &source_ip, "METRIC", data, &mut line_buf);
                    metrics_buffer.extend_from_slice(line_buf.as_bytes());
                    metrics_count += 1;
                }
                SchemaType::Trace => {
                    self.format_schema_message(&timestamp_str, &source_ip, "TRACE", data, &mut line_buf);
                    traces_buffer.extend_from_slice(line_buf.as_bytes());
                    traces_count += 1;
                }
                SchemaType::Unknown => {
                    // Unknown schema types go to raw.log
                    self.format_raw_message(&timestamp_str, &source_ip, msg, &mut line_buf);
                    raw_buffer.extend_from_slice(line_buf.as_bytes());
                    raw_count += 1;
                }
            }
        }

        // Submit buffers to rotation systems
        let mut total_lines = 0u64;
        let mut total_bytes = 0u64;

        // Helper to submit buffer and track metrics
        let submit_buffer = |rotation: &AtomicRotationSink,
                             buffer: bytes::BytesMut,
                             count: usize,
                             total_lines: &mut u64,
                             total_bytes: &mut u64,
                             metrics: &DiskPlaintextMetrics| {
            if count > 0 {
                let bytes = buffer.len();
                match rotation.submit(&workspace_str, buffer, count) {
                    Ok(()) => {
                        *total_lines += count as u64;
                        *total_bytes += bytes as u64;
                    }
                    Err(buf) => {
                        metrics.record_queue_full();
                        rotation.put_buffer(buf);
                    }
                }
            } else {
                rotation.put_buffer(buffer);
            }
        };

        submit_buffer(&self.events_rotation, events_buffer, events_count, &mut total_lines, &mut total_bytes, &self.metrics);
        submit_buffer(&self.logs_rotation, logs_buffer, logs_count, &mut total_lines, &mut total_bytes, &self.metrics);
        submit_buffer(&self.snapshots_rotation, snapshots_buffer, snapshots_count, &mut total_lines, &mut total_bytes, &self.metrics);
        submit_buffer(&self.metrics_rotation, metrics_buffer, metrics_count, &mut total_lines, &mut total_bytes, &self.metrics);
        submit_buffer(&self.traces_rotation, traces_buffer, traces_count, &mut total_lines, &mut total_bytes, &self.metrics);
        submit_buffer(&self.raw_rotation, raw_buffer, raw_count, &mut total_lines, &mut total_bytes, &self.metrics);

        if total_lines > 0 {
            self.metrics.record_batch(total_lines, total_bytes);
        }
    }

    /// Format a decoded event to the line buffer
    fn format_event(&self, timestamp: &str, source_ip: &str, event: &DecodedEvent<'_>, buf: &mut String) {
        buf.clear();

        // Format: [timestamp] [EVENT_TYPE] device_id=X session_id=Y event_name=Z source_ip=W payload=...
        let _ = write!(
            buf,
            "[{}] [{}] device_id={} session_id={} event_name={} timestamp={} source_ip={} payload=",
            timestamp,
            event.event_type.as_str().to_uppercase(),
            format_uuid(event.device_id),
            format_uuid(event.session_id),
            event.event_name.unwrap_or("-"),
            event.timestamp,
            source_ip,
        );

        // Append escaped payload
        append_escaped_payload(buf, event.payload);
        buf.push('\n');
    }

    /// Format a decoded log entry to the line buffer
    fn format_log(&self, timestamp: &str, source_ip: &str, log: &DecodedLogEntry<'_>, buf: &mut String) {
        buf.clear();

        // Format: [timestamp] [LEVEL] session_id=X source=Y service=Z timestamp=T source_ip=W payload=...
        let _ = write!(
            buf,
            "[{}] [{}] session_id={} source={} service={} timestamp={} source_ip={} payload=",
            timestamp,
            log.level.as_str().to_uppercase(),
            format_uuid(log.session_id),
            log.source.unwrap_or("-"),
            log.service.unwrap_or("-"),
            log.timestamp,
            source_ip,
        );

        // Append escaped payload
        append_escaped_payload(buf, log.payload);
        buf.push('\n');
    }

    /// Format a schema message (snapshots, metrics, traces)
    fn format_schema_message(&self, timestamp: &str, source_ip: &str, type_name: &str, data: &[u8], buf: &mut String) {
        buf.clear();

        let _ = write!(
            buf,
            "[{}] [{}] source_ip={} data_len={} payload=",
            timestamp,
            type_name,
            source_ip,
            data.len(),
        );

        append_escaped_payload(buf, data);
        buf.push('\n');
    }

    /// Format raw message when decode fails
    fn format_raw_message(&self, timestamp: &str, source_ip: &str, msg: &[u8], buf: &mut String) {
        buf.clear();

        let _ = write!(
            buf,
            "[{}] [RAW] source_ip={} msg_len={} payload=",
            timestamp,
            source_ip,
            msg.len(),
        );

        append_escaped_payload(buf, msg);
        buf.push('\n');
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

/// Format a 16-byte UUID as hex string, or "-" if None
fn format_uuid(uuid: Option<&[u8; 16]>) -> String {
    match uuid {
        Some(bytes) => {
            // Format as standard UUID: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
            format!(
                "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                bytes[0], bytes[1], bytes[2], bytes[3],
                bytes[4], bytes[5],
                bytes[6], bytes[7],
                bytes[8], bytes[9],
                bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]
            )
        }
        None => "-".to_string(),
    }
}

/// Append payload bytes to buffer, escaping special characters
/// For UTF-8 text, escapes newlines and control chars
/// For binary data, uses hex encoding with truncation
fn append_escaped_payload(buf: &mut String, payload: &[u8]) {
    if let Ok(text) = std::str::from_utf8(payload) {
        // Escape newlines and control chars for single-line output
        for ch in text.chars() {
            match ch {
                '\n' => buf.push_str("\\n"),
                '\r' => buf.push_str("\\r"),
                '\t' => buf.push_str("\\t"),
                '\\' => buf.push_str("\\\\"),
                c if c.is_control() => {
                    let _ = write!(buf, "\\x{:02x}", c as u32);
                }
                c => buf.push(c),
            }
        }
    } else {
        // Binary data - use hex encoding (truncate if too long)
        const MAX_HEX_BYTES: usize = 256;
        let truncated = payload.len() > MAX_HEX_BYTES;
        let display_bytes = if truncated { MAX_HEX_BYTES } else { payload.len() };

        buf.push_str("0x");
        for byte in &payload[..display_bytes] {
            let _ = write!(buf, "{:02x}", byte);
        }
        if truncated {
            let _ = write!(buf, "...(+{} bytes)", payload.len() - MAX_HEX_BYTES);
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

/// LZ4 writer with .log.lz4 extension
#[derive(Debug, Clone)]
struct Lz4WriterWithLogExt {
    inner: Lz4Writer,
}

impl Lz4WriterWithLogExt {
    fn new(buffer_size: usize) -> Self {
        Self {
            inner: Lz4Writer::new(buffer_size),
        }
    }
}

impl ChainWriter for Lz4WriterWithLogExt {
    fn wrap(&self, file: std::fs::File) -> std::io::Result<Box<dyn crate::util::ChainWrite>> {
        self.inner.wrap(file)
    }

    fn file_extension(&self) -> &'static str {
        ".log.lz4"
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
