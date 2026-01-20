//! Arrow array builders for ClickHouse tables
//!
//! Provides typed builders that construct Arrow RecordBatches directly from
//! FlatBuffer data with minimal copying. UUIDs go directly from FB bytes
//! to FixedSizeBinary arrays.

use std::sync::Arc;

use arrow::array::{
    ArrayBuilder, ArrayRef, FixedSizeBinaryBuilder, Int8Builder, RecordBatch, StringBuilder,
    TimestampMillisecondBuilder, UInt64Builder,
};
use arrow::datatypes::Schema;

use super::schema;

/// Builder for events_v1 table
pub struct EventsBuilder {
    schema: Arc<Schema>,
    timestamp: TimestampMillisecondBuilder,
    event_name: StringBuilder,
    device_id: FixedSizeBinaryBuilder,
    session_id: FixedSizeBinaryBuilder,
    source_ip: FixedSizeBinaryBuilder,
    properties: StringBuilder,
    raw: StringBuilder,
}

impl EventsBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            schema: schema::events_schema(),
            timestamp: TimestampMillisecondBuilder::with_capacity(capacity),
            event_name: StringBuilder::with_capacity(capacity, capacity * 32),
            device_id: FixedSizeBinaryBuilder::with_capacity(capacity, 16),
            session_id: FixedSizeBinaryBuilder::with_capacity(capacity, 16),
            source_ip: FixedSizeBinaryBuilder::with_capacity(capacity, 16),
            properties: StringBuilder::with_capacity(capacity, capacity * 256),
            raw: StringBuilder::with_capacity(capacity, capacity * 256),
        }
    }

    /// Append an event row directly from FlatBuffer data
    #[inline]
    pub fn append(
        &mut self,
        timestamp_ms: i64,
        event_name: &str,
        device_id: &[u8; 16],
        session_id: &[u8; 16],
        source_ip: &[u8; 16],
        payload: &[u8],
    ) {
        self.timestamp.append_value(timestamp_ms);
        self.event_name.append_value(event_name);
        self.device_id.append_value(device_id).unwrap();
        self.session_id.append_value(session_id).unwrap();
        self.source_ip.append_value(source_ip).unwrap();
        // Payload as UTF-8 string for both properties and _raw
        let payload_str = String::from_utf8_lossy(payload);
        self.properties.append_value(&payload_str);
        self.raw.append_value(&payload_str);
    }

    pub fn len(&self) -> usize {
        self.timestamp.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Finish building and return RecordBatch
    pub fn finish(&mut self) -> RecordBatch {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.timestamp.finish()),
            Arc::new(self.event_name.finish()),
            Arc::new(self.device_id.finish()),
            Arc::new(self.session_id.finish()),
            Arc::new(self.source_ip.finish()),
            Arc::new(self.properties.finish()),
            Arc::new(self.raw.finish()),
        ];
        RecordBatch::try_new(self.schema.clone(), columns).unwrap()
    }

    /// Clear and reuse the builder
    pub fn clear(&mut self) {
        // Builders are cleared after finish(), just recreate with same capacity
        let cap = 1000;
        self.timestamp = TimestampMillisecondBuilder::with_capacity(cap);
        self.event_name = StringBuilder::with_capacity(cap, cap * 32);
        self.device_id = FixedSizeBinaryBuilder::with_capacity(cap, 16);
        self.session_id = FixedSizeBinaryBuilder::with_capacity(cap, 16);
        self.source_ip = FixedSizeBinaryBuilder::with_capacity(cap, 16);
        self.properties = StringBuilder::with_capacity(cap, cap * 256);
        self.raw = StringBuilder::with_capacity(cap, cap * 256);
    }
}

/// Builder for logs_v1 table
pub struct LogsBuilder {
    schema: Arc<Schema>,
    timestamp: TimestampMillisecondBuilder,
    level: Int8Builder,
    source: StringBuilder,
    service: StringBuilder,
    session_id: FixedSizeBinaryBuilder,
    source_ip: FixedSizeBinaryBuilder,
    pattern_id: UInt64Builder,
    message: StringBuilder,
    raw: StringBuilder,
}

impl LogsBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            schema: schema::logs_schema(),
            timestamp: TimestampMillisecondBuilder::with_capacity(capacity),
            level: Int8Builder::with_capacity(capacity),
            source: StringBuilder::with_capacity(capacity, capacity * 64),
            service: StringBuilder::with_capacity(capacity, capacity * 32),
            session_id: FixedSizeBinaryBuilder::with_capacity(capacity, 16),
            source_ip: FixedSizeBinaryBuilder::with_capacity(capacity, 16),
            pattern_id: UInt64Builder::with_capacity(capacity),
            message: StringBuilder::with_capacity(capacity, capacity * 256),
            raw: StringBuilder::with_capacity(capacity, capacity * 256),
        }
    }

    /// Append a log row directly from FlatBuffer data
    #[inline]
    #[allow(clippy::too_many_arguments)]
    pub fn append(
        &mut self,
        timestamp_ms: i64,
        level: i8,
        source: &str,
        service: &str,
        session_id: &[u8; 16],
        source_ip: &[u8; 16],
        pattern_id: Option<u64>,
        payload: &[u8],
    ) {
        self.timestamp.append_value(timestamp_ms);
        self.level.append_value(level);
        self.source.append_value(source);
        self.service.append_value(service);
        self.session_id.append_value(session_id).unwrap();
        self.source_ip.append_value(source_ip).unwrap();
        self.pattern_id.append_option(pattern_id);
        let payload_str = String::from_utf8_lossy(payload);
        self.message.append_value(&payload_str);
        self.raw.append_value(&payload_str);
    }

    pub fn len(&self) -> usize {
        self.timestamp.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn finish(&mut self) -> RecordBatch {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.timestamp.finish()),
            Arc::new(self.level.finish()),
            Arc::new(self.source.finish()),
            Arc::new(self.service.finish()),
            Arc::new(self.session_id.finish()),
            Arc::new(self.source_ip.finish()),
            Arc::new(self.pattern_id.finish()),
            Arc::new(self.message.finish()),
            Arc::new(self.raw.finish()),
        ];
        RecordBatch::try_new(self.schema.clone(), columns).unwrap()
    }

    pub fn clear(&mut self) {
        let cap = 1000;
        self.timestamp = TimestampMillisecondBuilder::with_capacity(cap);
        self.level = Int8Builder::with_capacity(cap);
        self.source = StringBuilder::with_capacity(cap, cap * 64);
        self.service = StringBuilder::with_capacity(cap, cap * 32);
        self.session_id = FixedSizeBinaryBuilder::with_capacity(cap, 16);
        self.source_ip = FixedSizeBinaryBuilder::with_capacity(cap, 16);
        self.pattern_id = UInt64Builder::with_capacity(cap);
        self.message = StringBuilder::with_capacity(cap, cap * 256);
        self.raw = StringBuilder::with_capacity(cap, cap * 256);
    }
}

/// Builder for context_v1 table
pub struct ContextBuilder {
    schema: Arc<Schema>,
    timestamp: TimestampMillisecondBuilder,
    device_id: FixedSizeBinaryBuilder,
    session_id: FixedSizeBinaryBuilder,
    source_ip: FixedSizeBinaryBuilder,
    device_type: StringBuilder,
    device_model: StringBuilder,
    operating_system: StringBuilder,
    os_version: StringBuilder,
    app_version: StringBuilder,
    app_build: StringBuilder,
    timezone: StringBuilder,
    locale: StringBuilder,
    country: StringBuilder,
    region: StringBuilder,
    city: StringBuilder,
    properties: StringBuilder,
}

impl ContextBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            schema: schema::context_schema(),
            timestamp: TimestampMillisecondBuilder::with_capacity(capacity),
            device_id: FixedSizeBinaryBuilder::with_capacity(capacity, 16),
            session_id: FixedSizeBinaryBuilder::with_capacity(capacity, 16),
            source_ip: FixedSizeBinaryBuilder::with_capacity(capacity, 16),
            device_type: StringBuilder::with_capacity(capacity, capacity * 16),
            device_model: StringBuilder::with_capacity(capacity, capacity * 32),
            operating_system: StringBuilder::with_capacity(capacity, capacity * 16),
            os_version: StringBuilder::with_capacity(capacity, capacity * 16),
            app_version: StringBuilder::with_capacity(capacity, capacity * 16),
            app_build: StringBuilder::with_capacity(capacity, capacity * 8),
            timezone: StringBuilder::with_capacity(capacity, capacity * 32),
            locale: StringBuilder::with_capacity(capacity, capacity * 8),
            country: StringBuilder::with_capacity(capacity, capacity * 32),
            region: StringBuilder::with_capacity(capacity, capacity * 32),
            city: StringBuilder::with_capacity(capacity, capacity * 32),
            properties: StringBuilder::with_capacity(capacity, capacity * 256),
        }
    }

    #[inline]
    #[allow(clippy::too_many_arguments)]
    pub fn append(
        &mut self,
        timestamp_ms: i64,
        device_id: &[u8; 16],
        session_id: &[u8; 16],
        source_ip: &[u8; 16],
        device_type: &str,
        device_model: &str,
        operating_system: &str,
        os_version: &str,
        app_version: &str,
        app_build: &str,
        timezone: &str,
        locale: &str,
        country: &str,
        region: &str,
        city: &str,
        payload: &[u8],
    ) {
        self.timestamp.append_value(timestamp_ms);
        self.device_id.append_value(device_id).unwrap();
        self.session_id.append_value(session_id).unwrap();
        self.source_ip.append_value(source_ip).unwrap();
        self.device_type.append_value(device_type);
        self.device_model.append_value(device_model);
        self.operating_system.append_value(operating_system);
        self.os_version.append_value(os_version);
        self.app_version.append_value(app_version);
        self.app_build.append_value(app_build);
        self.timezone.append_value(timezone);
        self.locale.append_value(locale);
        self.country.append_value(country);
        self.region.append_value(region);
        self.city.append_value(city);
        self.properties
            .append_value(String::from_utf8_lossy(payload));
    }

    pub fn len(&self) -> usize {
        self.timestamp.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn finish(&mut self) -> RecordBatch {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.timestamp.finish()),
            Arc::new(self.device_id.finish()),
            Arc::new(self.session_id.finish()),
            Arc::new(self.source_ip.finish()),
            Arc::new(self.device_type.finish()),
            Arc::new(self.device_model.finish()),
            Arc::new(self.operating_system.finish()),
            Arc::new(self.os_version.finish()),
            Arc::new(self.app_version.finish()),
            Arc::new(self.app_build.finish()),
            Arc::new(self.timezone.finish()),
            Arc::new(self.locale.finish()),
            Arc::new(self.country.finish()),
            Arc::new(self.region.finish()),
            Arc::new(self.city.finish()),
            Arc::new(self.properties.finish()),
        ];
        RecordBatch::try_new(self.schema.clone(), columns).unwrap()
    }

    pub fn clear(&mut self) {
        *self = Self::new(1000);
    }
}

/// Builder for users_v1 table
pub struct UsersBuilder {
    schema: Arc<Schema>,
    user_id: StringBuilder,
    email: StringBuilder,
    name: StringBuilder,
    updated_at: TimestampMillisecondBuilder,
}

impl UsersBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            schema: schema::users_schema(),
            user_id: StringBuilder::with_capacity(capacity, capacity * 36),
            email: StringBuilder::with_capacity(capacity, capacity * 64),
            name: StringBuilder::with_capacity(capacity, capacity * 64),
            updated_at: TimestampMillisecondBuilder::with_capacity(capacity),
        }
    }

    #[inline]
    pub fn append(&mut self, user_id: &str, email: &str, name: &str, timestamp_ms: i64) {
        self.user_id.append_value(user_id);
        self.email.append_value(email);
        self.name.append_value(name);
        self.updated_at.append_value(timestamp_ms);
    }

    pub fn len(&self) -> usize {
        self.user_id.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn finish(&mut self) -> RecordBatch {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.user_id.finish()),
            Arc::new(self.email.finish()),
            Arc::new(self.name.finish()),
            Arc::new(self.updated_at.finish()),
        ];
        RecordBatch::try_new(self.schema.clone(), columns).unwrap()
    }

    pub fn clear(&mut self) {
        *self = Self::new(1000);
    }
}

/// Builder for user_devices table
pub struct UserDevicesBuilder {
    schema: Arc<Schema>,
    user_id: StringBuilder,
    device_id: FixedSizeBinaryBuilder,
    linked_at: TimestampMillisecondBuilder,
}

impl UserDevicesBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            schema: schema::user_devices_schema(),
            user_id: StringBuilder::with_capacity(capacity, capacity * 36),
            device_id: FixedSizeBinaryBuilder::with_capacity(capacity, 16),
            linked_at: TimestampMillisecondBuilder::with_capacity(capacity),
        }
    }

    #[inline]
    pub fn append(&mut self, user_id: &str, device_id: &[u8; 16], timestamp_ms: i64) {
        self.user_id.append_value(user_id);
        self.device_id.append_value(device_id).unwrap();
        self.linked_at.append_value(timestamp_ms);
    }

    pub fn len(&self) -> usize {
        self.user_id.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn finish(&mut self) -> RecordBatch {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.user_id.finish()),
            Arc::new(self.device_id.finish()),
            Arc::new(self.linked_at.finish()),
        ];
        RecordBatch::try_new(self.schema.clone(), columns).unwrap()
    }

    pub fn clear(&mut self) {
        *self = Self::new(1000);
    }
}

/// Builder for user_traits table
pub struct UserTraitsBuilder {
    schema: Arc<Schema>,
    user_id: StringBuilder,
    trait_key: StringBuilder,
    trait_value: StringBuilder,
    updated_at: TimestampMillisecondBuilder,
}

impl UserTraitsBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            schema: schema::user_traits_schema(),
            user_id: StringBuilder::with_capacity(capacity, capacity * 36),
            trait_key: StringBuilder::with_capacity(capacity, capacity * 32),
            trait_value: StringBuilder::with_capacity(capacity, capacity * 64),
            updated_at: TimestampMillisecondBuilder::with_capacity(capacity),
        }
    }

    #[inline]
    pub fn append(&mut self, user_id: &str, key: &str, value: &str, timestamp_ms: i64) {
        self.user_id.append_value(user_id);
        self.trait_key.append_value(key);
        self.trait_value.append_value(value);
        self.updated_at.append_value(timestamp_ms);
    }

    pub fn len(&self) -> usize {
        self.user_id.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn finish(&mut self) -> RecordBatch {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.user_id.finish()),
            Arc::new(self.trait_key.finish()),
            Arc::new(self.trait_value.finish()),
            Arc::new(self.updated_at.finish()),
        ];
        RecordBatch::try_new(self.schema.clone(), columns).unwrap()
    }

    pub fn clear(&mut self) {
        *self = Self::new(1000);
    }
}

/// Builder for snapshots_v1 table
pub struct SnapshotsBuilder {
    schema: Arc<Schema>,
    timestamp: TimestampMillisecondBuilder,
    connector: StringBuilder,
    entity: StringBuilder,
    metrics: StringBuilder,
}

impl SnapshotsBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            schema: schema::snapshots_schema(),
            timestamp: TimestampMillisecondBuilder::with_capacity(capacity),
            connector: StringBuilder::with_capacity(capacity, capacity * 16),
            entity: StringBuilder::with_capacity(capacity, capacity * 64),
            metrics: StringBuilder::with_capacity(capacity, capacity * 256),
        }
    }

    #[inline]
    pub fn append(&mut self, timestamp_ms: i64, connector: &str, entity: &str, metrics: &str) {
        self.timestamp.append_value(timestamp_ms);
        self.connector.append_value(connector);
        self.entity.append_value(entity);
        self.metrics.append_value(metrics);
    }

    pub fn len(&self) -> usize {
        self.timestamp.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn finish(&mut self) -> RecordBatch {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.timestamp.finish()),
            Arc::new(self.connector.finish()),
            Arc::new(self.entity.finish()),
            Arc::new(self.metrics.finish()),
        ];
        RecordBatch::try_new(self.schema.clone(), columns).unwrap()
    }

    pub fn clear(&mut self) {
        *self = Self::new(1000);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_events_builder() {
        let mut builder = EventsBuilder::new(10);
        let device_id = [1u8; 16];
        let session_id = [2u8; 16];
        let source_ip = [0u8; 16];

        builder.append(
            1234567890000,
            "page_view",
            &device_id,
            &session_id,
            &source_ip,
            b"{\"url\":\"/home\"}",
        );

        assert_eq!(builder.len(), 1);

        let batch = builder.finish();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 7);
    }

    #[test]
    fn test_logs_builder() {
        let mut builder = LogsBuilder::new(10);
        let session_id = [3u8; 16];
        let source_ip = [0u8; 16];

        builder.append(
            1234567890000,
            6, // INFO level
            "web-01",
            "api-gateway",
            &session_id,
            &source_ip,
            Some(12345),
            b"{\"msg\":\"request processed\"}",
        );

        assert_eq!(builder.len(), 1);

        let batch = builder.finish();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 9);
    }
}
