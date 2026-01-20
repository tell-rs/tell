//! ClickHouse sink implementation
//!
//! Core sink that receives batches and writes to ClickHouse tables.

use std::sync::Arc;

use clickhouse::{Client, insert::Insert};
use tell_protocol::{
    Batch, BatchType, EventType, FlatBatch, SchemaType, decode_event_data, decode_log_data,
};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::util::json::{extract_json_object, extract_json_string};

use super::config::ClickHouseConfig;
use super::error::ClickHouseSinkError;
use super::helpers::{extract_source_ip, generate_user_id_from_email, normalize_locale};
use super::metrics::{ClickHouseMetrics, ClickHouseSinkMetricsHandle, MetricsSnapshot};
use super::tables::{
    ContextRow, EventRow, LogLevelEnum, LogRow, SnapshotRow, UserDeviceRow, UserRow, UserTraitRow,
};

// =============================================================================
// Per-Table Batches
// =============================================================================

/// Batches for each table type
pub(crate) struct TableBatches {
    /// TRACK events → events_v1
    pub(crate) track: Vec<EventRow>,
    /// IDENTIFY events → users_v1
    pub(crate) users: Vec<UserRow>,
    /// IDENTIFY events → user_devices
    pub(crate) user_devices: Vec<UserDeviceRow>,
    /// IDENTIFY events → user_traits
    pub(crate) user_traits: Vec<UserTraitRow>,
    /// CONTEXT events → context_v1
    pub(crate) context: Vec<ContextRow>,
    /// Logs → logs_v1
    pub(crate) logs: Vec<LogRow>,
    /// Connector snapshots → snapshots_v1
    pub(crate) snapshots: Vec<SnapshotRow>,
}

impl TableBatches {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            track: Vec::with_capacity(capacity),
            users: Vec::with_capacity(capacity),
            user_devices: Vec::with_capacity(capacity),
            user_traits: Vec::with_capacity(capacity),
            context: Vec::with_capacity(capacity),
            logs: Vec::with_capacity(capacity),
            snapshots: Vec::with_capacity(capacity),
        }
    }

    pub(crate) fn any_needs_flush(&self, threshold: usize) -> bool {
        self.track.len() >= threshold
            || self.users.len() >= threshold
            || self.user_devices.len() >= threshold
            || self.user_traits.len() >= threshold
            || self.context.len() >= threshold
            || self.logs.len() >= threshold
            || self.snapshots.len() >= threshold
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.track.is_empty()
            && self.users.is_empty()
            && self.user_devices.is_empty()
            && self.user_traits.is_empty()
            && self.context.is_empty()
            && self.logs.is_empty()
            && self.snapshots.is_empty()
    }
}

// =============================================================================
// ClickHouse Sink
// =============================================================================

/// ClickHouse sink for Tell analytics
///
/// Routes events to appropriate tables based on EventType:
/// - TRACK → events_v1
/// - IDENTIFY → users_v1 + user_devices + user_traits (parallel)
/// - CONTEXT → context_v1
/// - Logs → logs_v1
/// - Snapshots → snapshots_v1 (connectors)
pub struct ClickHouseSink {
    /// Channel receiver for batches
    receiver: mpsc::Receiver<Arc<Batch>>,

    /// Configuration
    config: ClickHouseConfig,

    /// ClickHouse client
    client: Client,

    /// Per-table batches
    pub(crate) batches: TableBatches,

    /// Metrics (Arc for sharing with metrics handle)
    metrics: Arc<ClickHouseMetrics>,

    /// Sink name for identification
    name: String,
}

impl ClickHouseSink {
    /// Create a new ClickHouse sink
    pub fn new(config: ClickHouseConfig, receiver: mpsc::Receiver<Arc<Batch>>) -> Self {
        Self::with_name(config, receiver, "clickhouse")
    }

    /// Create a new ClickHouse sink with a custom name
    pub fn with_name(
        config: ClickHouseConfig,
        receiver: mpsc::Receiver<Arc<Batch>>,
        name: impl Into<String>,
    ) -> Self {
        let client = config.build_client();
        let capacity = config.batch_size;

        Self {
            receiver,
            batches: TableBatches::new(capacity),
            client,
            config,
            metrics: Arc::new(ClickHouseMetrics::new()),
            name: name.into(),
        }
    }

    /// Get reference to metrics
    pub fn metrics(&self) -> &ClickHouseMetrics {
        &self.metrics
    }

    /// Get a metrics handle for reporting
    pub fn metrics_handle(&self) -> ClickHouseSinkMetricsHandle {
        ClickHouseSinkMetricsHandle::new(
            self.name.clone(),
            Arc::clone(&self.metrics),
            self.config.flush_interval,
        )
    }

    /// Get the sink name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get reference to config
    pub fn config(&self) -> &ClickHouseConfig {
        &self.config
    }

    /// Run the sink
    pub async fn run(mut self) -> Result<MetricsSnapshot, ClickHouseSinkError> {
        tracing::info!(
            url = %self.config.url,
            database = %self.config.database,
            "clickhouse sink starting (Tell v1.1 schema)"
        );

        let mut flush_interval = tokio::time::interval(self.config.flush_interval);

        loop {
            tokio::select! {
                batch_opt = self.receiver.recv() => {
                    match batch_opt {
                        Some(batch) => {
                            self.metrics.record_batch_received();
                            if let Err(e) = self.process_batch(&batch) {
                                tracing::error!(error = %e, "failed to process batch");
                            }

                            // Check if any table needs flush
                            if self.batches.any_needs_flush(self.config.batch_size)
                                && let Err(e) = self.flush_all().await
                            {
                                tracing::error!(error = %e, "failed to flush batches");
                            }
                        }
                        None => break, // Channel closed
                    }
                }
                _ = flush_interval.tick() => {
                    if !self.batches.is_empty()
                        && let Err(e) = self.flush_all().await
                    {
                        tracing::error!(error = %e, "failed to flush batches on interval");
                    }
                }
            }
        }

        // Final flush
        if !self.batches.is_empty()
            && let Err(e) = self.flush_all().await
        {
            tracing::error!(error = %e, "failed final flush");
        }

        let snapshot = self.metrics.snapshot();
        tracing::info!(
            batches_received = snapshot.batches_received,
            events = snapshot.events_written,
            users = snapshot.users_written,
            user_devices = snapshot.user_devices_written,
            user_traits = snapshot.user_traits_written,
            context = snapshot.context_written,
            logs = snapshot.logs_written,
            snapshots = snapshot.snapshots_written,
            errors = snapshot.write_errors,
            "clickhouse sink shutting down"
        );

        Ok(snapshot)
    }

    /// Process a single batch
    pub(crate) fn process_batch(&mut self, batch: &Batch) -> Result<(), ClickHouseSinkError> {
        let source_ip = extract_source_ip(batch);

        // Get pattern IDs if available (from transformer)
        let pattern_ids = batch.pattern_ids();

        match batch.batch_type() {
            BatchType::Event => {
                self.process_events(batch, source_ip)?;
            }
            BatchType::Log => {
                self.process_logs(batch, source_ip, pattern_ids)?;
            }
            BatchType::Syslog => {
                self.process_raw_logs(batch, source_ip)?;
            }
            BatchType::Snapshot => {
                self.process_snapshots(batch)?;
            }
            BatchType::Metric | BatchType::Trace => {
                tracing::debug!(batch_type = %batch.batch_type(), "skipping unsupported batch type");
            }
        }

        Ok(())
    }

    /// Process event batch - route by EventType
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

            for event in events {
                self.route_event(&event, source_ip);
            }
        }

        Ok(())
    }

    /// Route a single event to the appropriate batch based on EventType
    fn route_event(&mut self, event: &tell_protocol::DecodedEvent<'_>, source_ip: [u8; 16]) {
        match event.event_type {
            EventType::Track => {
                self.add_track_event(event, source_ip);
            }
            EventType::Identify => {
                self.add_identify_event(event, source_ip);
            }
            EventType::Context => {
                self.add_context_event(event, source_ip);
            }
            EventType::Group | EventType::Alias | EventType::Enrich | EventType::Unknown => {
                tracing::warn!(
                    event_type = %event.event_type,
                    "unknown event type, skipping"
                );
            }
        }
    }

    /// Add TRACK event to events batch
    fn add_track_event(&mut self, event: &tell_protocol::DecodedEvent<'_>, source_ip: [u8; 16]) {
        let device_id = event
            .device_id
            .map(|b| Uuid::from_bytes(*b))
            .unwrap_or(Uuid::nil());
        let session_id = event
            .session_id
            .map(|b| Uuid::from_bytes(*b))
            .unwrap_or(Uuid::nil());

        let row = EventRow {
            timestamp: event.timestamp as i64,
            event_name: event.event_name.map(|s| s.to_string()).unwrap_or_default(),
            device_id,
            session_id,
            source_ip,
            properties: String::from_utf8_lossy(event.payload).into_owned(),
            raw: String::from_utf8_lossy(event.payload).into_owned(),
        };
        self.batches.track.push(row);
    }

    /// Add IDENTIFY event to users, user_devices, and user_traits batches
    fn add_identify_event(
        &mut self,
        event: &tell_protocol::DecodedEvent<'_>,
        _source_ip: [u8; 16],
    ) {
        let payload = event.payload;
        let timestamp = event.timestamp as i64;

        // Extract email and generate user_id
        let email = extract_json_string(payload, "email");
        let user_id = generate_user_id_from_email(&email);

        if user_id.is_empty() {
            tracing::debug!("IDENTIFY event missing email, skipping");
            return;
        }

        // 1. Add to users_v1
        let name = extract_json_string(payload, "name");
        self.batches.users.push(UserRow {
            user_id: user_id.clone(),
            email: email.clone(),
            name,
            updated_at: timestamp,
        });

        // 2. Add to user_devices
        if let Some(device_id) = event.device_id {
            self.batches.user_devices.push(UserDeviceRow {
                user_id: user_id.clone(),
                device_id: Uuid::from_bytes(*device_id),
                linked_at: timestamp,
            });
        }

        // 3. Add traits to user_traits
        let traits = extract_json_object(payload, "traits");
        for (key, value) in traits {
            self.batches.user_traits.push(UserTraitRow {
                user_id: user_id.clone(),
                trait_key: key,
                trait_value: value,
                updated_at: timestamp,
            });
        }
    }

    /// Add CONTEXT event to context batch
    fn add_context_event(&mut self, event: &tell_protocol::DecodedEvent<'_>, source_ip: [u8; 16]) {
        let payload = event.payload;

        let device_id = event
            .device_id
            .map(|b| Uuid::from_bytes(*b))
            .unwrap_or(Uuid::nil());
        let session_id = event
            .session_id
            .map(|b| Uuid::from_bytes(*b))
            .unwrap_or(Uuid::nil());

        // Extract device/location fields from payload
        let device_type = extract_json_string(payload, "device_type");
        let device_model = extract_json_string(payload, "device_model");
        let operating_system = extract_json_string(payload, "operating_system");
        let os_version = extract_json_string(payload, "os_version");
        let app_version = extract_json_string(payload, "app_version");
        let app_build = extract_json_string(payload, "app_build");
        let timezone = extract_json_string(payload, "timezone");
        let locale = extract_json_string(payload, "locale");
        let country = extract_json_string(payload, "country");
        let region = extract_json_string(payload, "region");
        let city = extract_json_string(payload, "city");

        // Ensure locale is exactly 5 characters for FixedString(5)
        let locale = normalize_locale(&locale);

        let row = ContextRow {
            timestamp: event.timestamp as i64,
            device_id,
            session_id,
            source_ip,
            device_type,
            device_model,
            operating_system,
            os_version,
            app_version,
            app_build,
            timezone,
            locale,
            country,
            region,
            city,
            properties: String::from_utf8_lossy(payload).into_owned(),
        };
        self.batches.context.push(row);
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

            for (log_idx, log) in logs.iter().enumerate() {
                // Get pattern_id if available
                let pattern_id = pattern_ids
                    .and_then(|ids| ids.get(pattern_offset + log_idx).copied())
                    .filter(|&id| id != 0);

                let session_id = log
                    .session_id
                    .map(|b| Uuid::from_bytes(*b))
                    .unwrap_or(Uuid::nil());

                let row = LogRow {
                    timestamp: log.timestamp as i64,
                    level: LogLevelEnum::from_protocol_level(log.level),
                    source: log.source.map(|s| s.to_string()).unwrap_or_default(),
                    service: log.service.map(|s| s.to_string()).unwrap_or_default(),
                    session_id,
                    source_ip,
                    pattern_id,
                    message: String::from_utf8_lossy(log.payload).into_owned(),
                    raw: String::from_utf8_lossy(log.payload).into_owned(),
                };
                self.batches.logs.push(row);
            }

            pattern_offset += logs.len();
        }

        Ok(())
    }

    /// Process raw syslog messages as logs
    fn process_raw_logs(
        &mut self,
        batch: &Batch,
        source_ip: [u8; 16],
    ) -> Result<(), ClickHouseSinkError> {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        for i in 0..batch.message_count() {
            let Some(msg) = batch.get_message(i) else {
                continue;
            };

            let payload = String::from_utf8_lossy(msg).into_owned();

            let row = LogRow {
                timestamp,
                level: LogLevelEnum::Info,
                source: String::new(),
                service: String::new(),
                session_id: Uuid::nil(),
                source_ip,
                pattern_id: None,
                message: payload.clone(),
                raw: payload,
            };
            self.batches.logs.push(row);
        }

        Ok(())
    }

    /// Process connector snapshot batch
    fn process_snapshots(&mut self, batch: &Batch) -> Result<(), ClickHouseSinkError> {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        for i in 0..batch.message_count() {
            let Some(msg) = batch.get_message(i) else {
                continue;
            };

            // Snapshot messages are JSON with connector, entity, and metrics fields
            let connector = extract_json_string(msg, "connector");
            let entity = extract_json_string(msg, "entity");

            // Extract metrics as JSON string - the entire metrics object
            let metrics = self.extract_metrics_json(msg);

            if connector.is_empty() || entity.is_empty() {
                tracing::debug!("snapshot missing connector or entity, skipping");
                continue;
            }

            let row = SnapshotRow {
                timestamp,
                connector,
                entity,
                metrics,
            };
            self.batches.snapshots.push(row);
        }

        Ok(())
    }

    /// Extract the metrics JSON object from a snapshot message
    fn extract_metrics_json(&self, payload: &[u8]) -> String {
        let json_str = match std::str::from_utf8(payload) {
            Ok(s) => s,
            Err(_) => return "{}".to_string(),
        };

        // Find "metrics" key and extract the JSON object
        let search = "\"metrics\"";
        if let Some(start) = json_str.find(search) {
            let after_key = &json_str[start + search.len()..];
            let after_colon = after_key
                .trim_start()
                .strip_prefix(':')
                .unwrap_or(after_key);
            let after_colon = after_colon.trim_start();

            if after_colon.starts_with('{') {
                // Find matching closing brace
                let mut depth = 0;
                let mut obj_end = 0;
                for (i, ch) in after_colon.chars().enumerate() {
                    match ch {
                        '{' => depth += 1,
                        '}' => {
                            depth -= 1;
                            if depth == 0 {
                                obj_end = i + 1;
                                break;
                            }
                        }
                        _ => {}
                    }
                }

                if obj_end > 0 {
                    return after_colon[..obj_end].to_string();
                }
            }
        }

        "{}".to_string()
    }

    /// Flush all batches to ClickHouse concurrently
    async fn flush_all(&mut self) -> Result<(), ClickHouseSinkError> {
        // Take all batches
        let track = std::mem::take(&mut self.batches.track);
        let users = std::mem::take(&mut self.batches.users);
        let user_devices = std::mem::take(&mut self.batches.user_devices);
        let user_traits = std::mem::take(&mut self.batches.user_traits);
        let context = std::mem::take(&mut self.batches.context);
        let logs = std::mem::take(&mut self.batches.logs);
        let snapshots = std::mem::take(&mut self.batches.snapshots);

        // Re-initialize with capacity
        let capacity = self.config.batch_size;
        self.batches = TableBatches::new(capacity);

        // Flush all tables concurrently
        let (
            track_result,
            users_result,
            devices_result,
            traits_result,
            context_result,
            logs_result,
            snapshots_result,
        ) = tokio::join!(
            self.flush_track_events(track),
            self.flush_users(users),
            self.flush_user_devices(user_devices),
            self.flush_user_traits(user_traits),
            self.flush_context_events(context),
            self.flush_logs(logs),
            self.flush_snapshots(snapshots),
        );

        // Check for errors
        if let Err(e) = track_result {
            tracing::error!(error = %e, "failed to flush track events");
            self.metrics.record_error();
        }
        if let Err(e) = users_result {
            tracing::error!(error = %e, "failed to flush users");
            self.metrics.record_error();
        }
        if let Err(e) = devices_result {
            tracing::error!(error = %e, "failed to flush user devices");
            self.metrics.record_error();
        }
        if let Err(e) = traits_result {
            tracing::error!(error = %e, "failed to flush user traits");
            self.metrics.record_error();
        }
        if let Err(e) = context_result {
            tracing::error!(error = %e, "failed to flush context events");
            self.metrics.record_error();
        }
        if let Err(e) = logs_result {
            tracing::error!(error = %e, "failed to flush logs");
            self.metrics.record_error();
        }
        if let Err(e) = snapshots_result {
            tracing::error!(error = %e, "failed to flush snapshots");
            self.metrics.record_error();
        }

        Ok(())
    }

    /// Flush TRACK events to events_v1
    async fn flush_track_events(&self, events: Vec<EventRow>) -> Result<(), ClickHouseSinkError> {
        if events.is_empty() {
            return Ok(());
        }

        let count = events.len();
        let table = &self.config.tables.events;

        self.insert_with_retry(table, &events).await?;

        self.metrics.record_events_written(count as u64);
        self.metrics.record_batch_written();
        tracing::debug!(table = %table, count = count, "flushed track events");

        Ok(())
    }

    /// Flush users to users_v1
    async fn flush_users(&self, users: Vec<UserRow>) -> Result<(), ClickHouseSinkError> {
        if users.is_empty() {
            return Ok(());
        }

        let count = users.len();
        let table = &self.config.tables.users;

        self.insert_with_retry(table, &users).await?;

        self.metrics.record_users_written(count as u64);
        self.metrics.record_batch_written();
        tracing::debug!(table = %table, count = count, "flushed users");

        Ok(())
    }

    /// Flush user devices to user_devices
    async fn flush_user_devices(
        &self,
        devices: Vec<UserDeviceRow>,
    ) -> Result<(), ClickHouseSinkError> {
        if devices.is_empty() {
            return Ok(());
        }

        let count = devices.len();
        let table = &self.config.tables.user_devices;

        self.insert_with_retry(table, &devices).await?;

        self.metrics.record_user_devices_written(count as u64);
        self.metrics.record_batch_written();
        tracing::debug!(table = %table, count = count, "flushed user devices");

        Ok(())
    }

    /// Flush user traits to user_traits
    async fn flush_user_traits(
        &self,
        traits: Vec<UserTraitRow>,
    ) -> Result<(), ClickHouseSinkError> {
        if traits.is_empty() {
            return Ok(());
        }

        let count = traits.len();
        let table = &self.config.tables.user_traits;

        self.insert_with_retry(table, &traits).await?;

        self.metrics.record_user_traits_written(count as u64);
        self.metrics.record_batch_written();
        tracing::debug!(table = %table, count = count, "flushed user traits");

        Ok(())
    }

    /// Flush CONTEXT events to context_v1
    async fn flush_context_events(
        &self,
        context: Vec<ContextRow>,
    ) -> Result<(), ClickHouseSinkError> {
        if context.is_empty() {
            return Ok(());
        }

        let count = context.len();
        let table = &self.config.tables.context;

        self.insert_with_retry(table, &context).await?;

        self.metrics.record_context_written(count as u64);
        self.metrics.record_batch_written();
        tracing::debug!(table = %table, count = count, "flushed context events");

        Ok(())
    }

    /// Flush logs to logs_v1
    async fn flush_logs(&self, logs: Vec<LogRow>) -> Result<(), ClickHouseSinkError> {
        if logs.is_empty() {
            return Ok(());
        }

        let count = logs.len();
        let table = &self.config.tables.logs;

        self.insert_with_retry(table, &logs).await?;

        self.metrics.record_logs_written(count as u64);
        self.metrics.record_batch_written();
        tracing::debug!(table = %table, count = count, "flushed logs");

        Ok(())
    }

    /// Flush snapshots to snapshots_v1
    async fn flush_snapshots(
        &self,
        snapshots: Vec<SnapshotRow>,
    ) -> Result<(), ClickHouseSinkError> {
        if snapshots.is_empty() {
            return Ok(());
        }

        let count = snapshots.len();
        let table = &self.config.tables.snapshots;

        self.insert_with_retry(table, &snapshots).await?;

        self.metrics.record_snapshots_written(count as u64);
        self.metrics.record_batch_written();
        tracing::debug!(table = %table, count = count, "flushed snapshots");

        Ok(())
    }

    /// Insert rows with retry logic
    async fn insert_with_retry<T>(&self, table: &str, rows: &[T]) -> Result<(), ClickHouseSinkError>
    where
        T: clickhouse::Row + serde::Serialize + Send + Sync + 'static,
        for<'a> T: clickhouse::Row<Value<'a> = T>,
    {
        let mut delay = self.config.retry_base_delay;

        for attempt in 0..=self.config.retry_attempts {
            if attempt > 0 {
                self.metrics.record_retry();
                tracing::warn!(
                    table = %table,
                    attempt = attempt,
                    max_attempts = self.config.retry_attempts,
                    delay_ms = delay.as_millis(),
                    "retrying insert"
                );
                tokio::time::sleep(delay).await;
                delay = std::cmp::min(delay * 2, self.config.retry_max_delay);
            }

            match self.do_insert(table, rows).await {
                Ok(()) => return Ok(()),
                Err(e) if attempt < self.config.retry_attempts => {
                    tracing::warn!(error = %e, table = %table, attempt = attempt, "insert failed, will retry");
                }
                Err(e) => return Err(e),
            }
        }

        Err(ClickHouseSinkError::InsertError(format!(
            "max retries exceeded for table {}",
            table
        )))
    }

    /// Perform the actual insert
    async fn do_insert<T>(&self, table: &str, rows: &[T]) -> Result<(), ClickHouseSinkError>
    where
        T: clickhouse::Row + serde::Serialize + Send + Sync + 'static,
        for<'a> T: clickhouse::Row<Value<'a> = T>,
    {
        let mut insert: Insert<T> = self.client.insert(table).await?;

        for row in rows {
            insert.write(row).await?;
        }

        insert.end().await?;
        Ok(())
    }
}
