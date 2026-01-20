//! Arrow-based ClickHouse sink
//!
//! High-performance sink that builds Arrow RecordBatches directly from FlatBuffer
//! data and sends them to ClickHouse via HTTP Arrow format.
//!
//! # Performance
//!
//! - UUIDs: FlatBuffer bytes → Arrow FixedSizeBinary(16) (single 16-byte copy)
//! - Strings: Directly appended to Arrow StringBuilder
//! - Batching: Columnar Arrow buffers, single HTTP request per flush
//! - ClickHouse: Native Arrow ingestion, no row-by-row parsing

use std::sync::Arc;
use std::time::Duration;

use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use tell_protocol::{
    Batch, BatchType, EventType, FlatBatch, LogLevel, SchemaType, decode_event_data,
    decode_log_data,
};
use tokio::sync::mpsc;
use tokio::time::interval;

use super::builders::{
    ContextBuilder, EventsBuilder, LogsBuilder, SnapshotsBuilder, UserDevicesBuilder,
    UsersBuilder, UserTraitsBuilder,
};
use crate::clickhouse::config::ClickHouseConfig;
use crate::clickhouse::error::ClickHouseSinkError;
use crate::clickhouse::helpers::{extract_source_ip, generate_user_id_from_email, normalize_locale};
use crate::clickhouse::metrics::{ClickHouseMetrics, ClickHouseSinkMetricsHandle, MetricsSnapshot};
use crate::util::json::{extract_json_object, extract_json_string};

const ZERO_UUID: [u8; 16] = [0u8; 16];

/// Arrow-based ClickHouse sink
pub struct ArrowClickHouseSink {
    config: ClickHouseConfig,
    receiver: mpsc::Receiver<Arc<Batch>>,
    client: reqwest::Client,
    metrics: Arc<ClickHouseMetrics>,
    name: String,

    // Arrow builders for each table
    events: EventsBuilder,
    context: ContextBuilder,
    logs: LogsBuilder,
    users: UsersBuilder,
    user_devices: UserDevicesBuilder,
    user_traits: UserTraitsBuilder,
    snapshots: SnapshotsBuilder,
}

impl ArrowClickHouseSink {
    /// Create a new Arrow ClickHouse sink
    pub fn new(config: ClickHouseConfig, receiver: mpsc::Receiver<Arc<Batch>>) -> Self {
        Self::with_name(config, receiver, "clickhouse_arrow")
    }

    /// Create with custom name
    pub fn with_name(
        config: ClickHouseConfig,
        receiver: mpsc::Receiver<Arc<Batch>>,
        name: impl Into<String>,
    ) -> Self {
        let client = reqwest::Client::builder()
            .timeout(config.connection_timeout)
            .build()
            .expect("failed to create HTTP client");

        let batch_size = config.batch_size;

        Self {
            config,
            receiver,
            client,
            metrics: Arc::new(ClickHouseMetrics::new()),
            name: name.into(),
            events: EventsBuilder::new(batch_size),
            context: ContextBuilder::new(batch_size),
            logs: LogsBuilder::new(batch_size),
            users: UsersBuilder::new(batch_size),
            user_devices: UserDevicesBuilder::new(batch_size),
            user_traits: UserTraitsBuilder::new(batch_size),
            snapshots: SnapshotsBuilder::new(batch_size),
        }
    }

    /// Get metrics handle for external reporting
    pub fn metrics_handle(&self) -> ClickHouseSinkMetricsHandle {
        ClickHouseSinkMetricsHandle::new(
            self.name.clone(),
            Arc::clone(&self.metrics),
            self.config.flush_interval,
        )
    }

    /// Run the sink
    pub async fn run(mut self) -> Result<MetricsSnapshot, ClickHouseSinkError> {
        tracing::info!(sink = %self.name, "arrow clickhouse sink starting");

        let mut flush_interval = interval(self.config.flush_interval);
        flush_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                // Receive batch from pipeline
                batch = self.receiver.recv() => {
                    match batch {
                        Some(batch) => {
                            self.process_batch(&batch)?;
                            self.metrics.record_batch_received();

                            // Check if we should flush based on batch size
                            if self.should_flush() {
                                self.flush_all().await;
                            }
                        }
                        None => {
                            // Channel closed, final flush and exit
                            tracing::info!(sink = %self.name, "channel closed, performing final flush");
                            self.flush_all().await;
                            break;
                        }
                    }
                }

                // Periodic flush
                _ = flush_interval.tick() => {
                    self.flush_all().await;
                }
            }
        }

        let snapshot = self.metrics.snapshot();
        tracing::info!(
            sink = %self.name,
            batches = snapshot.batches_received,
            events = snapshot.events_written,
            logs = snapshot.logs_written,
            errors = snapshot.write_errors,
            "arrow clickhouse sink shutting down"
        );

        Ok(snapshot)
    }

    fn should_flush(&self) -> bool {
        let batch_size = self.config.batch_size;
        self.events.len() >= batch_size
            || self.logs.len() >= batch_size
            || self.context.len() >= batch_size
            || self.users.len() >= batch_size
    }

    /// Process a pipeline batch
    fn process_batch(&mut self, batch: &Batch) -> Result<(), ClickHouseSinkError> {
        let source_ip = extract_source_ip(batch);

        match batch.batch_type() {
            BatchType::Event => self.process_events(batch, source_ip),
            BatchType::Log => self.process_logs(batch, source_ip, None),
            BatchType::Snapshot => self.process_snapshots(batch),
            _ => {
                tracing::debug!(batch_type = ?batch.batch_type(), "skipping unknown batch type");
                Ok(())
            }
        }
    }

    /// Process event batch
    fn process_events(
        &mut self,
        batch: &Batch,
        source_ip: [u8; 16],
    ) -> Result<(), ClickHouseSinkError> {
        for i in 0..batch.message_count() {
            let Some(msg) = batch.get_message(i) else {
                continue;
            };

            let flat_batch = match FlatBatch::parse(msg) {
                Ok(fb) => fb,
                Err(e) => {
                    tracing::debug!(error = %e, "failed to parse FlatBatch");
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
                    tracing::debug!(error = %e, "failed to get FlatBatch data");
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

            for event in &events {
                let device_id = event.device_id.unwrap_or(&ZERO_UUID);
                let session_id = event.session_id.unwrap_or(&ZERO_UUID);

                match event.event_type {
                    EventType::Track => {
                        self.events.append(
                            event.timestamp as i64,
                            event.event_name.unwrap_or(""),
                            device_id,
                            session_id,
                            &source_ip,
                            event.payload,
                        );
                    }
                    EventType::Identify => {
                        self.add_identify_event(event, device_id);
                    }
                    EventType::Context => {
                        self.add_context_event(event, device_id, session_id, &source_ip);
                    }
                    _ => {
                        tracing::debug!(event_type = ?event.event_type, "skipping unsupported event type");
                    }
                }
            }
        }

        Ok(())
    }

    /// Add IDENTIFY event to users, user_devices, user_traits builders
    fn add_identify_event(
        &mut self,
        event: &tell_protocol::DecodedEvent<'_>,
        device_id: &[u8; 16],
    ) {
        let payload = event.payload;
        let timestamp = event.timestamp as i64;

        let email = extract_json_string(payload, "email");
        let user_id = generate_user_id_from_email(&email);

        if user_id.is_empty() {
            tracing::debug!("IDENTIFY event missing email, skipping");
            return;
        }

        let name = extract_json_string(payload, "name");

        // 1. Users table
        self.users.append(&user_id, &email, &name, timestamp);

        // 2. User devices table
        if device_id != &ZERO_UUID {
            self.user_devices.append(&user_id, device_id, timestamp);
        }

        // 3. User traits table
        let traits = extract_json_object(payload, "traits");
        for (key, value) in traits {
            self.user_traits.append(&user_id, &key, &value, timestamp);
        }
    }

    /// Add CONTEXT event to context builder
    fn add_context_event(
        &mut self,
        event: &tell_protocol::DecodedEvent<'_>,
        device_id: &[u8; 16],
        session_id: &[u8; 16],
        source_ip: &[u8; 16],
    ) {
        let payload = event.payload;

        let device_type = extract_json_string(payload, "device_type");
        let device_model = extract_json_string(payload, "device_model");
        let operating_system = extract_json_string(payload, "operating_system");
        let os_version = extract_json_string(payload, "os_version");
        let app_version = extract_json_string(payload, "app_version");
        let app_build = extract_json_string(payload, "app_build");
        let timezone = extract_json_string(payload, "timezone");
        let locale = normalize_locale(&extract_json_string(payload, "locale"));
        let country = extract_json_string(payload, "country");
        let region = extract_json_string(payload, "region");
        let city = extract_json_string(payload, "city");

        self.context.append(
            event.timestamp as i64,
            device_id,
            session_id,
            source_ip,
            &device_type,
            &device_model,
            &operating_system,
            &os_version,
            &app_version,
            &app_build,
            &timezone,
            &locale,
            &country,
            &region,
            &city,
            payload,
        );
    }

    /// Process log batch
    fn process_logs(
        &mut self,
        batch: &Batch,
        source_ip: [u8; 16],
        pattern_ids: Option<&[u64]>,
    ) -> Result<(), ClickHouseSinkError> {
        let mut pattern_offset = 0;

        for i in 0..batch.message_count() {
            let Some(msg) = batch.get_message(i) else {
                continue;
            };

            let flat_batch = match FlatBatch::parse(msg) {
                Ok(fb) => fb,
                Err(e) => {
                    tracing::debug!(error = %e, "failed to parse FlatBatch");
                    self.metrics.record_decode_error();
                    continue;
                }
            };

            let data = match flat_batch.data() {
                Ok(d) => d,
                Err(e) => {
                    tracing::debug!(error = %e, "failed to get FlatBatch data");
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

            for (log_idx, log) in logs.iter().enumerate() {
                let pattern_id = pattern_ids
                    .and_then(|ids| ids.get(pattern_offset + log_idx).copied())
                    .filter(|&id| id != 0);

                let session_id = log.session_id.unwrap_or(&ZERO_UUID);
                let level = log_level_to_i8(log.level);

                self.logs.append(
                    log.timestamp as i64,
                    level,
                    log.source.unwrap_or(""),
                    log.service.unwrap_or(""),
                    session_id,
                    &source_ip,
                    pattern_id,
                    log.payload,
                );
            }

            pattern_offset += logs.len();
        }

        Ok(())
    }

    /// Process snapshot batch
    fn process_snapshots(&mut self, batch: &Batch) -> Result<(), ClickHouseSinkError> {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        for i in 0..batch.message_count() {
            let Some(msg) = batch.get_message(i) else {
                continue;
            };

            let json_str = String::from_utf8_lossy(msg);
            let connector = extract_json_string(msg, "connector");
            let entity = extract_json_string(msg, "entity");

            self.snapshots
                .append(timestamp, &connector, &entity, &json_str);
        }

        Ok(())
    }

    /// Flush all non-empty builders to ClickHouse
    async fn flush_all(&mut self) {
        // Extract all batches first (synchronous, takes ownership from builders)
        let events_batch = if !self.events.is_empty() {
            let count = self.events.len();
            let batch = self.events.finish();
            self.events.clear();
            Some((batch, count, self.config.tables.events.clone()))
        } else {
            None
        };

        let logs_batch = if !self.logs.is_empty() {
            let count = self.logs.len();
            let batch = self.logs.finish();
            self.logs.clear();
            Some((batch, count, self.config.tables.logs.clone()))
        } else {
            None
        };

        let context_batch = if !self.context.is_empty() {
            let batch = self.context.finish();
            self.context.clear();
            Some((batch, self.config.tables.context.clone()))
        } else {
            None
        };

        let users_batch = if !self.users.is_empty() {
            let batch = self.users.finish();
            self.users.clear();
            Some((batch, self.config.tables.users.clone()))
        } else {
            None
        };

        let user_devices_batch = if !self.user_devices.is_empty() {
            let batch = self.user_devices.finish();
            self.user_devices.clear();
            Some((batch, self.config.tables.user_devices.clone()))
        } else {
            None
        };

        let user_traits_batch = if !self.user_traits.is_empty() {
            let batch = self.user_traits.finish();
            self.user_traits.clear();
            Some((batch, self.config.tables.user_traits.clone()))
        } else {
            None
        };

        let snapshots_batch = if !self.snapshots.is_empty() {
            let batch = self.snapshots.finish();
            self.snapshots.clear();
            Some((batch, self.config.tables.snapshots.clone()))
        } else {
            None
        };

        // Send all batches in parallel (async)
        let (events_result, logs_result, context_result, users_result, devices_result, traits_result, snapshots_result) = tokio::join!(
            async {
                if let Some((batch, count, table)) = events_batch {
                    let result = self.send_arrow_batch(&table, batch).await;
                    if result.is_ok() {
                        self.metrics.record_events_written(count as u64);
                    }
                    result
                } else {
                    Ok(())
                }
            },
            async {
                if let Some((batch, count, table)) = logs_batch {
                    let result = self.send_arrow_batch(&table, batch).await;
                    if result.is_ok() {
                        self.metrics.record_logs_written(count as u64);
                    }
                    result
                } else {
                    Ok(())
                }
            },
            async {
                if let Some((batch, table)) = context_batch {
                    self.send_arrow_batch(&table, batch).await
                } else {
                    Ok(())
                }
            },
            async {
                if let Some((batch, table)) = users_batch {
                    self.send_arrow_batch(&table, batch).await
                } else {
                    Ok(())
                }
            },
            async {
                if let Some((batch, table)) = user_devices_batch {
                    self.send_arrow_batch(&table, batch).await
                } else {
                    Ok(())
                }
            },
            async {
                if let Some((batch, table)) = user_traits_batch {
                    self.send_arrow_batch(&table, batch).await
                } else {
                    Ok(())
                }
            },
            async {
                if let Some((batch, table)) = snapshots_batch {
                    self.send_arrow_batch(&table, batch).await
                } else {
                    Ok(())
                }
            },
        );

        // Log any errors
        if let Err(e) = events_result {
            tracing::error!(error = %e, "failed to flush events");
        }
        if let Err(e) = logs_result {
            tracing::error!(error = %e, "failed to flush logs");
        }
        if let Err(e) = context_result {
            tracing::error!(error = %e, "failed to flush context");
        }
        if let Err(e) = users_result {
            tracing::error!(error = %e, "failed to flush users");
        }
        if let Err(e) = devices_result {
            tracing::error!(error = %e, "failed to flush user_devices");
        }
        if let Err(e) = traits_result {
            tracing::error!(error = %e, "failed to flush user_traits");
        }
        if let Err(e) = snapshots_result {
            tracing::error!(error = %e, "failed to flush snapshots");
        }
    }

    /// Send Arrow RecordBatch to ClickHouse via HTTP
    async fn send_arrow_batch(
        &self,
        table: &str,
        batch: RecordBatch,
    ) -> Result<(), ClickHouseSinkError> {
        // Serialize to Arrow IPC format
        let mut buffer = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buffer, &batch.schema())
                .map_err(|e| ClickHouseSinkError::ArrowError(e.to_string()))?;
            writer
                .write(&batch)
                .map_err(|e| ClickHouseSinkError::ArrowError(e.to_string()))?;
            writer
                .finish()
                .map_err(|e| ClickHouseSinkError::ArrowError(e.to_string()))?;
        }

        // Build URL for ClickHouse HTTP interface
        let url = format!(
            "{}/?database={}&query=INSERT%20INTO%20{}%20FORMAT%20Arrow",
            self.config.url, self.config.database, table
        );

        // Send with retry
        let mut last_error = None;
        for attempt in 0..self.config.retry_attempts {
            let mut request = self.client.post(&url);

            // Add basic auth if username is provided
            if let Some(ref username) = self.config.username {
                request = request.basic_auth(username, self.config.password.as_ref());
            }

            let result = request
                .header("Content-Type", "application/octet-stream")
                .body(buffer.clone())
                .send()
                .await;

            match result {
                Ok(response) => {
                    if response.status().is_success() {
                        return Ok(());
                    }
                    let status = response.status();
                    let body = response.text().await.unwrap_or_default();
                    last_error = Some(format!("HTTP {}: {}", status, body));
                    tracing::warn!(
                        table = %table,
                        attempt = attempt,
                        error = %last_error.as_ref().unwrap(),
                        "ClickHouse insert failed"
                    );
                }
                Err(e) => {
                    last_error = Some(e.to_string());
                    tracing::warn!(
                        table = %table,
                        attempt = attempt,
                        error = %e,
                        "ClickHouse request failed"
                    );
                }
            }

            // Exponential backoff
            if attempt < self.config.retry_attempts - 1 {
                tokio::time::sleep(Duration::from_millis(100 * (1 << attempt))).await;
            }
        }

        self.metrics.record_error();
        Err(ClickHouseSinkError::InsertFailed {
            table: table.to_string(),
            message: last_error.unwrap_or_else(|| "unknown error".to_string()),
        })
    }
}

/// Convert LogLevel to ClickHouse Enum8 value
#[inline]
fn log_level_to_i8(level: LogLevel) -> i8 {
    match level {
        LogLevel::Trace => 8,
        LogLevel::Debug => 7,
        LogLevel::Info => 6,
        LogLevel::Warning => 4,
        LogLevel::Error => 3,
        LogLevel::Fatal => 0, // EMERGENCY
    }
}
