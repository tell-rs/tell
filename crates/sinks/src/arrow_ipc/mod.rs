//! Arrow IPC Sink - Fast columnar storage for hot data
//!
//! Writes batches to Arrow IPC files (also known as Feather) for fast I/O
//! and zero-copy reads. Arrow IPC is optimized for hot data that needs
//! frequent access and inter-process communication.
//!
//! # Features
//!
//! - **Fast I/O**: ~10x faster read/write than Parquet
//! - **Zero-copy reads**: Memory-mapped files avoid deserialization
//! - **Inter-process communication**: Ideal for streaming between processes
//! - **Schema separation**: Events, Logs, and Snapshots get different schemas
//! - **Time-based partitioning**: Files organized by workspace/date/hour
//!
//! # When to use Arrow IPC vs Parquet
//!
//! | Use Case | Format |
//! |----------|--------|
//! | Hot data (recent, frequent access) | Arrow IPC |
//! | Cold data (archival, analytics) | Parquet |
//! | Real-time dashboards | Arrow IPC |
//! | Data warehousing | Parquet |
//! | Inter-service communication | Arrow IPC |
//! | Long-term storage | Parquet |
//!
//! # Schema
//!
//! Uses the same schemas as the Parquet sink (shared via util::arrow_rows).
//! Field order is optimized for predicate pushdown.
//!
//! # File Organization
//!
//! ```text
//! {base_path}/
//! └── {workspace_id}/
//!     └── {date}/
//!         └── {hour}/
//!             ├── events.arrow
//!             ├── logs.arrow
//!             └── snapshots.arrow
//! ```
//!
//! # Compatibility
//!
//! Arrow IPC files can be read by:
//! - PyArrow: `pa.ipc.open_file("path.arrow")`
//! - DuckDB: `SELECT * FROM 'path.arrow'`
//! - Polars: `pl.read_ipc("path.arrow")`
//! - DataFusion: Native support

mod writer;

use std::collections::HashMap;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tell_protocol::{Batch, BatchType, FlatBatch, SchemaType, decode_event_data, decode_log_data};
use chrono::{DateTime, Datelike, Timelike, Utc};
use tell_metrics::{SinkMetricsConfig, SinkMetricsProvider, SinkMetricsSnapshot};
use parking_lot::Mutex;
use tokio::sync::mpsc;

pub use writer::ArrowIpcWriter;

// Re-export shared types
pub use crate::util::{EventRow, LogRow, SnapshotRow};

// =============================================================================
// Configuration
// =============================================================================

/// Configuration for Arrow IPC sink
#[derive(Debug, Clone)]
pub struct ArrowIpcConfig {
    /// Base output directory path
    pub path: PathBuf,

    /// File rotation interval
    pub rotation_interval: RotationInterval,

    /// Maximum rows to buffer before flushing (default: 10,000)
    pub buffer_size: usize,

    /// Flush interval for periodic flushing
    pub flush_interval: Duration,
}

impl Default for ArrowIpcConfig {
    fn default() -> Self {
        Self {
            path: PathBuf::from("arrow_ipc"),
            rotation_interval: RotationInterval::Hourly,
            buffer_size: 10_000,
            flush_interval: Duration::from_secs(60),
        }
    }
}

impl ArrowIpcConfig {
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

    /// Create config with custom buffer size
    pub fn with_buffer_size(mut self, size: usize) -> Self {
        self.buffer_size = size;
        self
    }

    /// Create config with custom flush interval
    pub fn with_flush_interval(mut self, interval: Duration) -> Self {
        self.flush_interval = interval;
        self
    }
}

/// File rotation interval
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RotationInterval {
    /// Rotate files every hour
    Hourly,
    /// Rotate files every day
    Daily,
}

// =============================================================================
// Metrics
// =============================================================================

/// Metrics for Arrow IPC sink
#[derive(Debug, Default)]
pub struct ArrowIpcSinkMetrics {
    /// Total batches received
    pub batches_received: AtomicU64,

    /// Total event rows written
    pub event_rows_written: AtomicU64,

    /// Total log rows written
    pub log_rows_written: AtomicU64,

    /// Total snapshot rows written
    pub snapshot_rows_written: AtomicU64,

    /// Total bytes written
    pub bytes_written: AtomicU64,

    /// Total files created
    pub files_created: AtomicU64,

    /// Write errors
    pub write_errors: AtomicU64,

    /// Decode errors (invalid FlatBuffer data)
    pub decode_errors: AtomicU64,
}

impl ArrowIpcSinkMetrics {
    /// Create new metrics
    pub const fn new() -> Self {
        Self {
            batches_received: AtomicU64::new(0),
            event_rows_written: AtomicU64::new(0),
            log_rows_written: AtomicU64::new(0),
            snapshot_rows_written: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            files_created: AtomicU64::new(0),
            write_errors: AtomicU64::new(0),
            decode_errors: AtomicU64::new(0),
        }
    }

    #[inline]
    pub fn record_batch(&self) {
        self.batches_received.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_event_rows(&self, count: u64) {
        self.event_rows_written.fetch_add(count, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_log_rows(&self, count: u64) {
        self.log_rows_written.fetch_add(count, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_snapshot_rows(&self, count: u64) {
        self.snapshot_rows_written
            .fetch_add(count, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_bytes(&self, bytes: u64) {
        self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_file_created(&self) {
        self.files_created.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_error(&self) {
        self.write_errors.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_decode_error(&self) {
        self.decode_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Get a snapshot of current metrics
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            batches_received: self.batches_received.load(Ordering::Relaxed),
            event_rows_written: self.event_rows_written.load(Ordering::Relaxed),
            log_rows_written: self.log_rows_written.load(Ordering::Relaxed),
            snapshot_rows_written: self.snapshot_rows_written.load(Ordering::Relaxed),
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
            files_created: self.files_created.load(Ordering::Relaxed),
            write_errors: self.write_errors.load(Ordering::Relaxed),
            decode_errors: self.decode_errors.load(Ordering::Relaxed),
        }
    }
}

/// Point-in-time snapshot of metrics
#[derive(Debug, Clone, Copy, Default)]
pub struct MetricsSnapshot {
    pub batches_received: u64,
    pub event_rows_written: u64,
    pub log_rows_written: u64,
    pub snapshot_rows_written: u64,
    pub bytes_written: u64,
    pub files_created: u64,
    pub write_errors: u64,
    pub decode_errors: u64,
}

/// Handle for accessing arrow_ipc sink metrics
#[derive(Clone)]
pub struct ArrowIpcSinkMetricsHandle {
    id: String,
    metrics: Arc<ArrowIpcSinkMetrics>,
    config: SinkMetricsConfig,
}

impl SinkMetricsProvider for ArrowIpcSinkMetricsHandle {
    fn sink_id(&self) -> &str {
        &self.id
    }

    fn sink_type(&self) -> &str {
        "arrow_ipc"
    }

    fn metrics_config(&self) -> SinkMetricsConfig {
        self.config
    }

    fn snapshot(&self) -> SinkMetricsSnapshot {
        let s = self.metrics.snapshot();
        SinkMetricsSnapshot {
            batches_received: s.batches_received,
            batches_written: s.files_created,
            messages_written: s.event_rows_written + s.log_rows_written + s.snapshot_rows_written,
            bytes_written: s.bytes_written,
            write_errors: s.write_errors + s.decode_errors,
            flush_count: s.files_created,
        }
    }
}

// =============================================================================
// Buffered Writers (per workspace/time bucket)
// =============================================================================

/// Key for workspace + time bucket
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct WriterKey {
    workspace_id: u32,
    date: String,
    hour: Option<u8>,
}

impl WriterKey {
    fn new(workspace_id: u32, timestamp: DateTime<Utc>, interval: RotationInterval) -> Self {
        let date = format!(
            "{:04}-{:02}-{:02}",
            timestamp.year(),
            timestamp.month(),
            timestamp.day()
        );
        let hour = match interval {
            RotationInterval::Hourly => Some(timestamp.hour() as u8),
            RotationInterval::Daily => None,
        };
        Self {
            workspace_id,
            date,
            hour,
        }
    }

    fn path(&self, base: &Path) -> PathBuf {
        let mut path = base.join(self.workspace_id.to_string()).join(&self.date);
        if let Some(hour) = self.hour {
            path = path.join(format!("{:02}", hour));
        }
        path
    }
}

/// Buffers rows before writing to Arrow IPC files
struct RowBuffer {
    events: Vec<EventRow>,
    logs: Vec<LogRow>,
    snapshots: Vec<SnapshotRow>,
    buffer_size: usize,
}

impl RowBuffer {
    fn new(buffer_size: usize) -> Self {
        Self {
            events: Vec::with_capacity(buffer_size),
            logs: Vec::with_capacity(buffer_size),
            snapshots: Vec::with_capacity(buffer_size),
            buffer_size,
        }
    }

    fn add_event(&mut self, row: EventRow) {
        self.events.push(row);
    }

    fn add_log(&mut self, row: LogRow) {
        self.logs.push(row);
    }

    #[allow(dead_code)]
    fn add_snapshot(&mut self, row: SnapshotRow) {
        self.snapshots.push(row);
    }

    fn should_flush(&self) -> bool {
        self.events.len() >= self.buffer_size
            || self.logs.len() >= self.buffer_size
            || self.snapshots.len() >= self.buffer_size
    }

    fn take_events(&mut self) -> Vec<EventRow> {
        std::mem::take(&mut self.events)
    }

    fn take_logs(&mut self) -> Vec<LogRow> {
        std::mem::take(&mut self.logs)
    }

    fn take_snapshots(&mut self) -> Vec<SnapshotRow> {
        std::mem::take(&mut self.snapshots)
    }

    fn is_empty(&self) -> bool {
        self.events.is_empty() && self.logs.is_empty() && self.snapshots.is_empty()
    }
}

// =============================================================================
// Arrow IPC Sink
// =============================================================================

/// Arrow IPC sink for fast columnar storage
///
/// Writes event and log data to Arrow IPC files organized by workspace and time.
/// Optimized for hot data that needs frequent access.
pub struct ArrowIpcSink {
    /// Channel receiver for batches
    receiver: mpsc::Receiver<Arc<Batch>>,

    /// Configuration
    config: ArrowIpcConfig,

    /// Row buffers per workspace/time bucket
    buffers: Mutex<HashMap<WriterKey, RowBuffer>>,

    /// Metrics
    metrics: Arc<ArrowIpcSinkMetrics>,

    /// Sink name
    name: String,
}

impl ArrowIpcSink {
    /// Create a new Arrow IPC sink
    pub fn new(config: ArrowIpcConfig, receiver: mpsc::Receiver<Arc<Batch>>) -> Self {
        Self::with_name(config, receiver, "arrow_ipc")
    }

    /// Create a new Arrow IPC sink with a custom name
    pub fn with_name(
        config: ArrowIpcConfig,
        receiver: mpsc::Receiver<Arc<Batch>>,
        name: impl Into<String>,
    ) -> Self {
        Self {
            receiver,
            config,
            buffers: Mutex::new(HashMap::new()),
            metrics: Arc::new(ArrowIpcSinkMetrics::new()),
            name: name.into(),
        }
    }

    /// Get reference to metrics
    pub fn metrics(&self) -> &ArrowIpcSinkMetrics {
        &self.metrics
    }

    /// Get a metrics handle for reporting
    pub fn metrics_handle(&self) -> ArrowIpcSinkMetricsHandle {
        ArrowIpcSinkMetricsHandle {
            id: self.name.clone(),
            metrics: Arc::clone(&self.metrics),
            config: SinkMetricsConfig {
                enabled: true,
                interval: self.config.flush_interval,
            },
        }
    }

    /// Get the sink name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get reference to config
    pub fn config(&self) -> &ArrowIpcConfig {
        &self.config
    }

    /// Run the sink
    pub async fn run(mut self) -> Result<MetricsSnapshot, ArrowIpcSinkError> {
        tracing::info!(path = %self.config.path.display(), "arrow_ipc sink starting");

        // Spawn periodic flush task
        let flush_interval = self.config.flush_interval;
        let (flush_tx, mut flush_rx) = mpsc::channel::<()>(1);

        let flush_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(flush_interval);
            loop {
                interval.tick().await;
                if flush_tx.send(()).await.is_err() {
                    break;
                }
            }
        });

        loop {
            tokio::select! {
                batch = self.receiver.recv() => {
                    match batch {
                        Some(batch) => {
                            if let Err(e) = self.process_batch(&batch) {
                                tracing::error!(error = %e, "failed to process batch");
                                self.metrics.record_error();
                            }
                        }
                        None => break,
                    }
                }
                _ = flush_rx.recv() => {
                    if let Err(e) = self.flush_all() {
                        tracing::error!(error = %e, "failed to flush buffers");
                    }
                }
            }
        }

        // Shutdown
        flush_handle.abort();

        // Final flush
        if let Err(e) = self.flush_all() {
            tracing::error!(error = %e, "failed final flush");
        }

        let snapshot = self.metrics.snapshot();
        tracing::info!(
            batches = snapshot.batches_received,
            event_rows = snapshot.event_rows_written,
            log_rows = snapshot.log_rows_written,
            snapshot_rows = snapshot.snapshot_rows_written,
            files = snapshot.files_created,
            bytes = snapshot.bytes_written,
            errors = snapshot.write_errors,
            "arrow_ipc sink shutting down"
        );

        Ok(snapshot)
    }

    fn process_batch(&self, batch: &Batch) -> Result<(), ArrowIpcSinkError> {
        self.metrics.record_batch();

        let workspace_id = batch.workspace_id();
        if workspace_id == 0 {
            tracing::warn!("batch missing workspace_id, skipping");
            return Ok(());
        }

        let now = Utc::now();
        let batch_timestamp = now.timestamp_millis();
        let key = WriterKey::new(workspace_id, now, self.config.rotation_interval);
        let source_ip = extract_source_ip(batch);

        match batch.batch_type() {
            BatchType::Event => {
                self.process_events(batch, &key, workspace_id, source_ip, batch_timestamp)?;
            }
            BatchType::Log => {
                self.process_logs(batch, &key, workspace_id, source_ip, batch_timestamp)?;
            }
            BatchType::Syslog => {
                self.process_raw_logs(batch, &key, workspace_id, source_ip, batch_timestamp)?;
            }
            BatchType::Metric | BatchType::Trace | BatchType::Snapshot => {
                tracing::debug!(batch_type = %batch.batch_type(), "skipping unsupported batch type");
            }
        }

        let should_flush = {
            let buffers = self.buffers.lock();
            buffers.get(&key).map(|b| b.should_flush()).unwrap_or(false)
        };

        if should_flush {
            self.flush_key(&key)?;
        }

        Ok(())
    }

    fn process_events(
        &self,
        batch: &Batch,
        key: &WriterKey,
        workspace_id: u32,
        source_ip: [u8; 16],
        batch_timestamp: i64,
    ) -> Result<(), ArrowIpcSinkError> {
        for i in 0..batch.message_count() {
            let Some(msg) = batch.get_message(i) else {
                continue;
            };

            let flat_batch = match FlatBatch::parse(msg) {
                Ok(fb) => fb,
                Err(e) => {
                    tracing::debug!(error = %e, "failed to parse FlatBuffer wrapper");
                    self.metrics.record_decode_error();
                    continue;
                }
            };

            if flat_batch.schema_type() != SchemaType::Event {
                continue;
            }

            let data = match flat_batch.data() {
                Ok(d) => d,
                Err(e) => {
                    tracing::debug!(error = %e, "failed to get data from FlatBuffer");
                    self.metrics.record_decode_error();
                    continue;
                }
            };
            let events = match decode_event_data(data) {
                Ok(e) => e,
                Err(e) => {
                    tracing::debug!(error = %e, "failed to decode event data");
                    self.metrics.record_decode_error();
                    continue;
                }
            };

            let mut buffers = self.buffers.lock();
            let buffer = buffers
                .entry(key.clone())
                .or_insert_with(|| RowBuffer::new(self.config.buffer_size));

            for event in events {
                let row = EventRow {
                    batch_timestamp,
                    timestamp: event.timestamp as i64,
                    event_type: event.event_type.as_str().to_string(),
                    device_id: event.device_id.map(|b| b.to_vec()),
                    session_id: event.session_id.map(|b| b.to_vec()),
                    event_name: event.event_name.map(|s| s.to_string()),
                    payload: event.payload.to_vec(),
                    source_ip: source_ip.to_vec(),
                    workspace_id: workspace_id as u64,
                };
                buffer.add_event(row);
            }
        }

        Ok(())
    }

    fn process_logs(
        &self,
        batch: &Batch,
        key: &WriterKey,
        workspace_id: u32,
        source_ip: [u8; 16],
        batch_timestamp: i64,
    ) -> Result<(), ArrowIpcSinkError> {
        for i in 0..batch.message_count() {
            let Some(msg) = batch.get_message(i) else {
                continue;
            };

            let flat_batch = match FlatBatch::parse(msg) {
                Ok(fb) => fb,
                Err(e) => {
                    tracing::debug!(error = %e, "failed to parse FlatBuffer wrapper");
                    self.metrics.record_decode_error();
                    continue;
                }
            };

            if flat_batch.schema_type() != SchemaType::Log {
                continue;
            }

            let data = match flat_batch.data() {
                Ok(d) => d,
                Err(e) => {
                    tracing::debug!(error = %e, "failed to get data from FlatBuffer");
                    self.metrics.record_decode_error();
                    continue;
                }
            };
            let logs = match decode_log_data(data) {
                Ok(l) => l,
                Err(e) => {
                    tracing::debug!(error = %e, "failed to decode log data");
                    self.metrics.record_decode_error();
                    continue;
                }
            };

            let mut buffers = self.buffers.lock();
            let buffer = buffers
                .entry(key.clone())
                .or_insert_with(|| RowBuffer::new(self.config.buffer_size));

            for log in logs {
                let row = LogRow {
                    batch_timestamp,
                    timestamp: log.timestamp as i64,
                    event_type: log.event_type.as_str().to_string(),
                    level: log.level.as_str().to_string(),
                    session_id: log.session_id.map(|b| b.to_vec()),
                    source: log.source.map(|s| s.to_string()),
                    service: log.service.map(|s| s.to_string()),
                    payload: log.payload.to_vec(),
                    source_ip: source_ip.to_vec(),
                    workspace_id: workspace_id as u64,
                };
                buffer.add_log(row);
            }
        }

        Ok(())
    }

    fn process_raw_logs(
        &self,
        batch: &Batch,
        key: &WriterKey,
        workspace_id: u32,
        source_ip: [u8; 16],
        batch_timestamp: i64,
    ) -> Result<(), ArrowIpcSinkError> {
        let mut buffers = self.buffers.lock();
        let buffer = buffers
            .entry(key.clone())
            .or_insert_with(|| RowBuffer::new(self.config.buffer_size));

        for i in 0..batch.message_count() {
            let Some(msg) = batch.get_message(i) else {
                continue;
            };

            let row = LogRow {
                batch_timestamp,
                timestamp: batch_timestamp,
                event_type: "log".to_string(),
                level: "info".to_string(),
                session_id: None,
                source: None,
                service: None,
                payload: msg.to_vec(),
                source_ip: source_ip.to_vec(),
                workspace_id: workspace_id as u64,
            };
            buffer.add_log(row);
        }

        Ok(())
    }

    fn flush_all(&self) -> Result<(), ArrowIpcSinkError> {
        let keys: Vec<WriterKey> = {
            let buffers = self.buffers.lock();
            buffers.keys().cloned().collect()
        };

        for key in keys {
            self.flush_key(&key)?;
        }

        Ok(())
    }

    fn flush_key(&self, key: &WriterKey) -> Result<(), ArrowIpcSinkError> {
        let (events, logs, snapshots) = {
            let mut buffers = self.buffers.lock();
            match buffers.get_mut(key) {
                Some(buffer) if !buffer.is_empty() => (
                    buffer.take_events(),
                    buffer.take_logs(),
                    buffer.take_snapshots(),
                ),
                _ => return Ok(()),
            }
        };

        let dir = key.path(&self.config.path);

        // Write events if any
        if !events.is_empty() {
            let path = dir.join("events.arrow");
            let count = events.len();

            std::fs::create_dir_all(&dir).map_err(|e| ArrowIpcSinkError::CreateDir {
                path: dir.display().to_string(),
                source: e,
            })?;

            let bytes = ArrowIpcWriter::write_events(&path, events)?;

            self.metrics.record_event_rows(count as u64);
            self.metrics.record_bytes(bytes);
            self.metrics.record_file_created();

            tracing::debug!(
                path = %path.display(),
                rows = count,
                bytes = bytes,
                "wrote events arrow file"
            );
        }

        // Write logs if any
        if !logs.is_empty() {
            let path = dir.join("logs.arrow");
            let count = logs.len();

            std::fs::create_dir_all(&dir).map_err(|e| ArrowIpcSinkError::CreateDir {
                path: dir.display().to_string(),
                source: e,
            })?;

            let bytes = ArrowIpcWriter::write_logs(&path, logs)?;

            self.metrics.record_log_rows(count as u64);
            self.metrics.record_bytes(bytes);
            self.metrics.record_file_created();

            tracing::debug!(
                path = %path.display(),
                rows = count,
                bytes = bytes,
                "wrote logs arrow file"
            );
        }

        // Write snapshots if any
        if !snapshots.is_empty() {
            let path = dir.join("snapshots.arrow");
            let count = snapshots.len();

            std::fs::create_dir_all(&dir).map_err(|e| ArrowIpcSinkError::CreateDir {
                path: dir.display().to_string(),
                source: e,
            })?;

            let bytes = ArrowIpcWriter::write_snapshots(&path, snapshots)?;

            self.metrics.record_snapshot_rows(count as u64);
            self.metrics.record_bytes(bytes);
            self.metrics.record_file_created();

            tracing::debug!(
                path = %path.display(),
                rows = count,
                bytes = bytes,
                "wrote snapshots arrow file"
            );
        }

        Ok(())
    }
}

// =============================================================================
// Helpers
// =============================================================================

/// Extract source IP from batch as IPv6 bytes
fn extract_source_ip(batch: &Batch) -> [u8; 16] {
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

// =============================================================================
// Errors
// =============================================================================

/// Errors from Arrow IPC sink
#[derive(Debug, thiserror::Error)]
pub enum ArrowIpcSinkError {
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

    /// Arrow error
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    /// Invalid configuration
    #[error("invalid configuration: {0}")]
    Config(String),
}

#[cfg(test)]
#[path = "arrow_ipc_test.rs"]
mod arrow_ipc_test;
