//! Arrow schema definitions for ClickHouse tables
//!
//! Defines Arrow schemas that match the Tell v1.1 ClickHouse tables.
//! UUIDs are represented as FixedSizeBinary(16) for zero-copy from FlatBuffers.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

/// Create schema for events_v1 table
///
/// ```sql
/// CREATE TABLE events_v1 (
///     timestamp DateTime64(3),
///     event_name LowCardinality(String),
///     device_id UUID,
///     session_id UUID,
///     source_ip FixedString(16),
///     properties JSON,
///     _raw String
/// )
/// ```
pub fn events_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("event_name", DataType::Utf8, false),
        Field::new("device_id", DataType::FixedSizeBinary(16), false),
        Field::new("session_id", DataType::FixedSizeBinary(16), false),
        Field::new("source_ip", DataType::FixedSizeBinary(16), false),
        Field::new("properties", DataType::Utf8, false),
        Field::new("_raw", DataType::Utf8, false),
    ]))
}

/// Create schema for context_v1 table
///
/// ```sql
/// CREATE TABLE context_v1 (
///     timestamp DateTime64(3),
///     device_id UUID,
///     session_id UUID,
///     source_ip FixedString(16),
///     device_type LowCardinality(String),
///     device_model LowCardinality(String),
///     operating_system LowCardinality(String),
///     os_version String,
///     app_version String,
///     app_build String,
///     timezone String,
///     locale FixedString(5),
///     country LowCardinality(String),
///     region String,
///     city String,
///     properties JSON
/// )
/// ```
pub fn context_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("device_id", DataType::FixedSizeBinary(16), false),
        Field::new("session_id", DataType::FixedSizeBinary(16), false),
        Field::new("source_ip", DataType::FixedSizeBinary(16), false),
        Field::new("device_type", DataType::Utf8, false),
        Field::new("device_model", DataType::Utf8, false),
        Field::new("operating_system", DataType::Utf8, false),
        Field::new("os_version", DataType::Utf8, false),
        Field::new("app_version", DataType::Utf8, false),
        Field::new("app_build", DataType::Utf8, false),
        Field::new("timezone", DataType::Utf8, false),
        Field::new("locale", DataType::Utf8, false),
        Field::new("country", DataType::Utf8, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("city", DataType::Utf8, false),
        Field::new("properties", DataType::Utf8, false),
    ]))
}

/// Create schema for logs_v1 table
///
/// ```sql
/// CREATE TABLE logs_v1 (
///     timestamp DateTime64(3),
///     level Enum8(...),
///     source LowCardinality(String),
///     service LowCardinality(String),
///     session_id UUID,
///     source_ip FixedString(16),
///     pattern_id Nullable(UInt64),
///     message JSON,
///     _raw String
/// )
/// ```
pub fn logs_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("level", DataType::Int8, false), // Enum8 as Int8
        Field::new("source", DataType::Utf8, false),
        Field::new("service", DataType::Utf8, false),
        Field::new("session_id", DataType::FixedSizeBinary(16), false),
        Field::new("source_ip", DataType::FixedSizeBinary(16), false),
        Field::new("pattern_id", DataType::UInt64, true), // Nullable
        Field::new("message", DataType::Utf8, false),
        Field::new("_raw", DataType::Utf8, false),
    ]))
}

/// Create schema for users_v1 table
///
/// ```sql
/// CREATE TABLE users_v1 (
///     user_id String,
///     email String,
///     name String,
///     updated_at DateTime64(3)
/// )
/// ```
pub fn users_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("email", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new(
            "updated_at",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
    ]))
}

/// Create schema for user_devices table
///
/// ```sql
/// CREATE TABLE user_devices (
///     user_id String,
///     device_id UUID,
///     linked_at DateTime64(3)
/// )
/// ```
pub fn user_devices_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("device_id", DataType::FixedSizeBinary(16), false),
        Field::new(
            "linked_at",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
    ]))
}

/// Create schema for user_traits table
///
/// ```sql
/// CREATE TABLE user_traits (
///     user_id String,
///     trait_key String,
///     trait_value String,
///     updated_at DateTime64(3)
/// )
/// ```
pub fn user_traits_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("trait_key", DataType::Utf8, false),
        Field::new("trait_value", DataType::Utf8, false),
        Field::new(
            "updated_at",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
    ]))
}

/// Create schema for snapshots_v1 table
///
/// ```sql
/// CREATE TABLE snapshots_v1 (
///     timestamp DateTime64(3),
///     connector LowCardinality(String),
///     entity String,
///     metrics JSON
/// )
/// ```
pub fn snapshots_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("connector", DataType::Utf8, false),
        Field::new("entity", DataType::Utf8, false),
        Field::new("metrics", DataType::Utf8, false),
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_events_schema() {
        let schema = events_schema();
        assert_eq!(schema.fields().len(), 7);
        assert_eq!(schema.field(0).name(), "timestamp");
        assert_eq!(schema.field(2).name(), "device_id");
        assert!(matches!(
            schema.field(2).data_type(),
            DataType::FixedSizeBinary(16)
        ));
    }

    #[test]
    fn test_logs_schema() {
        let schema = logs_schema();
        assert_eq!(schema.fields().len(), 9);
        // level is Int8 for Enum8
        assert!(matches!(schema.field(1).data_type(), DataType::Int8));
        // pattern_id is nullable
        assert!(schema.field(6).is_nullable());
    }
}
