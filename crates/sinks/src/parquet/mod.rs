//! Parquet Sink - Columnar storage for data warehousing
//!
//! Writes batches to Parquet files for analytics and data warehousing use cases.
//! Parquet is a columnar format optimized for analytical queries with excellent
//! compression ratios.
//!
//! # Features
//!
//! - **Columnar format**: Optimized for analytical queries (SELECT specific columns)
//! - **Compression options**: Snappy, LZ4, Zstd, or uncompressed
//! - **Schema separation**: Events, Logs, and Snapshots get different schemas
//! - **Time-based partitioning**: Files organized by workspace/date/hour
//! - **Row group batching**: Accumulates rows before writing for efficiency
//! - **Predicate pushdown**: Field order optimized for analytical queries
//!
//! # Schema
//!
//! Field order is optimized for predicate pushdown - frequently filtered fields
//! come first, large payloads come last.
//!
//! ## Event Schema (9 fields)
//! - `timestamp` (INT64): Event timestamp in milliseconds (when event occurred)
//! - `batch_timestamp` (INT64): Processing timestamp (when sink received data)
//! - `workspace_id` (UINT64): Workspace identifier for tenant isolation
//! - `event_type` (UTF8): Event type (track, identify, group, alias, enrich)
//! - `event_name` (UTF8): Optional event name (page_view, button_click)
//! - `device_id` (BINARY): 16-byte device UUID
//! - `session_id` (BINARY): 16-byte session UUID
//! - `source_ip` (BINARY): 16-byte IPv6 source address
//! - `payload` (BINARY): Event payload as JSON bytes
//!
//! ## Log Schema (10 fields)
//! - `timestamp` (INT64): Log timestamp in milliseconds (when log was generated)
//! - `batch_timestamp` (INT64): Processing timestamp (when sink received data)
//! - `workspace_id` (UINT64): Workspace identifier for tenant isolation
//! - `level` (UTF8): Log level (emergency, alert, critical, error, warning, etc.)
//! - `event_type` (UTF8): Log event type (log, enrich)
//! - `source` (UTF8): Source hostname/instance
//! - `service` (UTF8): Service/application name
//! - `session_id` (BINARY): 16-byte session UUID
//! - `source_ip` (BINARY): 16-byte IPv6 source address
//! - `payload` (BINARY): Log payload as bytes
//!
//! ## Snapshot Schema (7 fields)
//! - `timestamp` (INT64): Snapshot timestamp in milliseconds
//! - `batch_timestamp` (INT64): Processing timestamp (when sink received data)
//! - `workspace_id` (UINT64): Workspace identifier for tenant isolation
//! - `source` (UTF8): Connector name (github, stripe, linear)
//! - `entity` (UTF8): Resource identifier (user/repo, acct_123)
//! - `source_ip` (BINARY): 16-byte IPv6 source address
//! - `payload` (BINARY): Snapshot payload as JSON bytes
//!
//! # File Organization
//!
//! ```text
//! {base_path}/
//! └── {workspace_id}/
//!     └── {date}/
//!         └── {hour}/
//!             ├── events.parquet
//!             ├── logs.parquet
//!             └── snapshots.parquet
//! ```
//!
//! # Compatibility
//!
//! Parquet files can be read by:
//! - Apache Spark: `spark.read.parquet("path/")`
//! - DuckDB: `SELECT * FROM 'path/*.parquet'`
//! - ClickHouse: `SELECT * FROM file('path/*.parquet', Parquet)`
//! - Pandas: `pd.read_parquet("path/")`
//! - Polars: `pl.read_parquet("path/")`

mod schema;
mod writer;

use std::collections::HashMap;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use chrono::{DateTime, Datelike, Timelike, Utc};
use parking_lot::Mutex;
use tell_metrics::{SinkMetricsConfig, SinkMetricsProvider, SinkMetricsSnapshot};
use tell_protocol::{Batch, BatchType, FlatBatch, SchemaType, decode_event_data, decode_log_data};
use tokio::sync::mpsc;

// Re-export types (schema re-exports from util::arrow_rows)
pub use schema::{Compression, EventRow, LogRow, SnapshotRow};
pub use writer::ParquetWriter;

// =============================================================================
// Configuration
// =============================================================================

/// Configuration for Parquet sink
#[derive(Debug, Clone)]
pub struct ParquetConfig {
    /// Base output directory path
    pub path: PathBuf,

    /// File rotation interval
    pub rotation_interval: RotationInterval,

    /// Compression codec
    pub compression: Compression,

    /// Maximum rows per row group (default: 100,000)
    pub row_group_size: usize,

    /// Maximum rows to buffer before flushing (default: 10,000)
    pub buffer_size: usize,

    /// Flush interval for periodic flushing
    pub flush_interval: Duration,
}

impl Default for ParquetConfig {
    fn default() -> Self {
        Self {
            path: PathBuf::from("parquet"),
            rotation_interval: RotationInterval::Hourly,
            compression: Compression::Snappy,
            row_group_size: 100_000,
            buffer_size: 10_000,
            flush_interval: Duration::from_secs(60),
        }
    }
}

impl ParquetConfig {
    /// Create config with custom path
    pub fn with_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.path = path.into();
        self
    }

    /// Create config with Zstd compression
    pub fn with_zstd(mut self) -> Self {
        self.compression = Compression::Zstd;
        self
    }

    /// Create config with LZ4 compression
    pub fn with_lz4(mut self) -> Self {
        self.compression = Compression::Lz4;
        self
    }

    /// Create config with no compression
    pub fn uncompressed(mut self) -> Self {
        self.compression = Compression::None;
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

/// Metrics for Parquet sink
#[derive(Debug, Default)]
pub struct ParquetSinkMetrics {
    /// Total batches received
    pub batches_received: AtomicU64,

    /// Total event rows written
    pub event_rows_written: AtomicU64,

    /// Total log rows written
    pub log_rows_written: AtomicU64,

    /// Total snapshot rows written
    pub snapshot_rows_written: AtomicU64,

    /// Total bytes written (compressed)
    pub bytes_written: AtomicU64,

    /// Total files created
    pub files_created: AtomicU64,

    /// Write errors
    pub write_errors: AtomicU64,

    /// Decode errors (invalid FlatBuffer data)
    pub decode_errors: AtomicU64,
}

impl ParquetSinkMetrics {
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

    /// Record a batch processed
    #[inline]
    pub fn record_batch(&self) {
        self.batches_received.fetch_add(1, Ordering::Relaxed);
    }

    /// Record event rows written
    #[inline]
    pub fn record_event_rows(&self, count: u64) {
        self.event_rows_written.fetch_add(count, Ordering::Relaxed);
    }

    /// Record log rows written
    #[inline]
    pub fn record_log_rows(&self, count: u64) {
        self.log_rows_written.fetch_add(count, Ordering::Relaxed);
    }

    /// Record snapshot rows written
    #[inline]
    pub fn record_snapshot_rows(&self, count: u64) {
        self.snapshot_rows_written
            .fetch_add(count, Ordering::Relaxed);
    }

    /// Record bytes written
    #[inline]
    pub fn record_bytes(&self, bytes: u64) {
        self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record a file created
    #[inline]
    pub fn record_file_created(&self) {
        self.files_created.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a write error
    #[inline]
    pub fn record_error(&self) {
        self.write_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a decode error
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

/// Handle for accessing parquet sink metrics
///
/// This can be obtained before running the sink and used for metrics reporting.
/// It holds an Arc to the metrics, so it remains valid even after the sink
/// is consumed by `run()`.
#[derive(Clone)]
pub struct ParquetSinkMetricsHandle {
    id: String,
    metrics: Arc<ParquetSinkMetrics>,
    config: SinkMetricsConfig,
}

impl SinkMetricsProvider for ParquetSinkMetricsHandle {
    fn sink_id(&self) -> &str {
        &self.id
    }

    fn sink_type(&self) -> &str {
        "parquet"
    }

    fn metrics_config(&self) -> SinkMetricsConfig {
        self.config
    }

    fn snapshot(&self) -> SinkMetricsSnapshot {
        let s = self.metrics.snapshot();
        SinkMetricsSnapshot {
            batches_received: s.batches_received,
            batches_written: s.files_created, // Files created = batches written to disk
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

/// Buffers rows before writing to Parquet files
struct RowBuffer {
    events: Vec<EventRow>,
    logs: Vec<LogRow>,
    snapshots: Vec<SnapshotRow>,
    config: ParquetConfig,
}

impl RowBuffer {
    fn new(config: ParquetConfig) -> Self {
        Self {
            events: Vec::with_capacity(config.buffer_size),
            logs: Vec::with_capacity(config.buffer_size),
            snapshots: Vec::with_capacity(config.buffer_size),
            config,
        }
    }

    fn add_event(&mut self, row: EventRow) {
        self.events.push(row);
    }

    fn add_log(&mut self, row: LogRow) {
        self.logs.push(row);
    }

    #[allow(dead_code)] // Ready for when snapshot decoding is implemented
    fn add_snapshot(&mut self, row: SnapshotRow) {
        self.snapshots.push(row);
    }

    fn should_flush(&self) -> bool {
        self.events.len() >= self.config.buffer_size
            || self.logs.len() >= self.config.buffer_size
            || self.snapshots.len() >= self.config.buffer_size
    }

    #[allow(dead_code)] // Used in tests
    fn event_count(&self) -> usize {
        self.events.len()
    }

    #[allow(dead_code)] // Used in tests
    fn log_count(&self) -> usize {
        self.logs.len()
    }

    #[allow(dead_code)] // Used in tests
    fn snapshot_count(&self) -> usize {
        self.snapshots.len()
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
// Parquet Sink
// =============================================================================

/// Parquet sink for columnar storage
///
/// Writes event and log data to Parquet files organized by workspace and time.
/// Buffers rows in memory before writing to achieve optimal row group sizes.
pub struct ParquetSink {
    /// Channel receiver for batches
    receiver: mpsc::Receiver<Arc<Batch>>,

    /// Configuration
    config: ParquetConfig,

    /// Row buffers per workspace/time bucket
    buffers: Mutex<HashMap<WriterKey, RowBuffer>>,

    /// Metrics (Arc for sharing with metrics handle)
    metrics: Arc<ParquetSinkMetrics>,

    /// Sink name for identification
    name: String,
}

impl ParquetSink {
    /// Create a new Parquet sink
    pub fn new(config: ParquetConfig, receiver: mpsc::Receiver<Arc<Batch>>) -> Self {
        Self::with_name(config, receiver, "parquet")
    }

    /// Create a new Parquet sink with a custom name
    pub fn with_name(
        config: ParquetConfig,
        receiver: mpsc::Receiver<Arc<Batch>>,
        name: impl Into<String>,
    ) -> Self {
        Self {
            receiver,
            config,
            buffers: Mutex::new(HashMap::new()),
            metrics: Arc::new(ParquetSinkMetrics::new()),
            name: name.into(),
        }
    }

    /// Get reference to metrics
    pub fn metrics(&self) -> &ParquetSinkMetrics {
        &self.metrics
    }

    /// Get a metrics handle for reporting
    ///
    /// The handle implements `SinkMetricsProvider` and can be registered
    /// with the metrics reporter. It remains valid even after `run()` consumes
    /// the sink.
    pub fn metrics_handle(&self) -> ParquetSinkMetricsHandle {
        ParquetSinkMetricsHandle {
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
    pub fn config(&self) -> &ParquetConfig {
        &self.config
    }

    /// Run the sink
    pub async fn run(mut self) -> Result<MetricsSnapshot, ParquetSinkError> {
        tracing::info!(path = %self.config.path.display(), "parquet sink starting");

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
                        None => break, // Channel closed
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
            "parquet sink shutting down"
        );

        Ok(snapshot)
    }

    /// Process a single batch
    fn process_batch(&self, batch: &Batch) -> Result<(), ParquetSinkError> {
        self.metrics.record_batch();

        let workspace_id = batch.workspace_id();
        if workspace_id == 0 {
            tracing::warn!("batch missing workspace_id, skipping");
            return Ok(());
        }

        // Get timestamp for bucket key and batch_timestamp
        let now = Utc::now();
        let batch_timestamp = now.timestamp_millis();
        let key = WriterKey::new(workspace_id, now, self.config.rotation_interval);

        // Extract source IP
        let source_ip = extract_source_ip(batch);

        // Decode and buffer based on batch type
        match batch.batch_type() {
            BatchType::Event => {
                self.process_events(batch, &key, workspace_id, source_ip, batch_timestamp)?;
            }
            BatchType::Log => {
                self.process_logs(batch, &key, workspace_id, source_ip, batch_timestamp)?;
            }
            BatchType::Syslog => {
                // Syslog batches contain raw messages, treat as logs
                self.process_raw_logs(batch, &key, workspace_id, source_ip, batch_timestamp)?;
            }
            BatchType::Metric | BatchType::Trace | BatchType::Snapshot => {
                // Snapshot decoding not yet implemented in protocol crate
                tracing::debug!(batch_type = %batch.batch_type(), "skipping unsupported batch type");
            }
        }

        // Check if we should flush
        let should_flush = {
            let buffers = self.buffers.lock();
            buffers.get(&key).map(|b| b.should_flush()).unwrap_or(false)
        };

        if should_flush {
            self.flush_key(&key)?;
        }

        Ok(())
    }

    /// Process event batch
    fn process_events(
        &self,
        batch: &Batch,
        key: &WriterKey,
        workspace_id: u32,
        source_ip: [u8; 16],
        batch_timestamp: i64,
    ) -> Result<(), ParquetSinkError> {
        // Try to decode FlatBuffer data
        for i in 0..batch.message_count() {
            let Some(msg) = batch.get_message(i) else {
                continue;
            };

            // Parse the FlatBuffer wrapper to get the data field
            let flat_batch = match FlatBatch::parse(msg) {
                Ok(fb) => fb,
                Err(e) => {
                    tracing::debug!(error = %e, "failed to parse FlatBuffer wrapper");
                    self.metrics.record_decode_error();
                    continue;
                }
            };

            // Only process event schema
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
                .or_insert_with(|| RowBuffer::new(self.config.clone()));

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

    /// Process log batch
    fn process_logs(
        &self,
        batch: &Batch,
        key: &WriterKey,
        workspace_id: u32,
        source_ip: [u8; 16],
        batch_timestamp: i64,
    ) -> Result<(), ParquetSinkError> {
        for i in 0..batch.message_count() {
            let Some(msg) = batch.get_message(i) else {
                continue;
            };

            // Parse the FlatBuffer wrapper
            let flat_batch = match FlatBatch::parse(msg) {
                Ok(fb) => fb,
                Err(e) => {
                    tracing::debug!(error = %e, "failed to parse FlatBuffer wrapper");
                    self.metrics.record_decode_error();
                    continue;
                }
            };

            // Only process log schema
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
                .or_insert_with(|| RowBuffer::new(self.config.clone()));

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

    /// Process raw syslog messages as logs
    fn process_raw_logs(
        &self,
        batch: &Batch,
        key: &WriterKey,
        workspace_id: u32,
        source_ip: [u8; 16],
        batch_timestamp: i64,
    ) -> Result<(), ParquetSinkError> {
        let mut buffers = self.buffers.lock();
        let buffer = buffers
            .entry(key.clone())
            .or_insert_with(|| RowBuffer::new(self.config.clone()));

        for i in 0..batch.message_count() {
            let Some(msg) = batch.get_message(i) else {
                continue;
            };

            let row = LogRow {
                batch_timestamp,
                timestamp: batch_timestamp, // Raw syslog doesn't have its own timestamp
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

    // NOTE: process_snapshots will be added when snapshot decoding is
    // implemented in the protocol crate. The schema, writer, and buffer
    // support are already in place.

    /// Flush all buffers to disk
    fn flush_all(&self) -> Result<(), ParquetSinkError> {
        let keys: Vec<WriterKey> = {
            let buffers = self.buffers.lock();
            buffers.keys().cloned().collect()
        };

        for key in keys {
            self.flush_key(&key)?;
        }

        Ok(())
    }

    /// Flush a specific buffer to disk
    fn flush_key(&self, key: &WriterKey) -> Result<(), ParquetSinkError> {
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
            let path = dir.join("events.parquet");
            let count = events.len();

            std::fs::create_dir_all(&dir).map_err(|e| ParquetSinkError::CreateDir {
                path: dir.display().to_string(),
                source: e,
            })?;

            let bytes = ParquetWriter::write_events(&path, events, self.config.compression)?;

            self.metrics.record_event_rows(count as u64);
            self.metrics.record_bytes(bytes);
            self.metrics.record_file_created();

            tracing::debug!(
                path = %path.display(),
                rows = count,
                bytes = bytes,
                "wrote events parquet file"
            );
        }

        // Write logs if any
        if !logs.is_empty() {
            let path = dir.join("logs.parquet");
            let count = logs.len();

            std::fs::create_dir_all(&dir).map_err(|e| ParquetSinkError::CreateDir {
                path: dir.display().to_string(),
                source: e,
            })?;

            let bytes = ParquetWriter::write_logs(&path, logs, self.config.compression)?;

            self.metrics.record_log_rows(count as u64);
            self.metrics.record_bytes(bytes);
            self.metrics.record_file_created();

            tracing::debug!(
                path = %path.display(),
                rows = count,
                bytes = bytes,
                "wrote logs parquet file"
            );
        }

        // Write snapshots if any
        if !snapshots.is_empty() {
            let path = dir.join("snapshots.parquet");
            let count = snapshots.len();

            std::fs::create_dir_all(&dir).map_err(|e| ParquetSinkError::CreateDir {
                path: dir.display().to_string(),
                source: e,
            })?;

            let bytes = ParquetWriter::write_snapshots(&path, snapshots, self.config.compression)?;

            self.metrics.record_snapshot_rows(count as u64);
            self.metrics.record_bytes(bytes);
            self.metrics.record_file_created();

            tracing::debug!(
                path = %path.display(),
                rows = count,
                bytes = bytes,
                "wrote snapshots parquet file"
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

/// Errors from Parquet sink
#[derive(Debug, thiserror::Error)]
pub enum ParquetSinkError {
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

    /// Parquet write error
    #[error("parquet write error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    /// Arrow error
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    /// Invalid configuration
    #[error("invalid configuration: {0}")]
    Config(String),
}

#[cfg(test)]
#[path = "parquet_test.rs"]
mod parquet_test;
