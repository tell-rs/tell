//! Event table row types (events_v1, context_v1)

use clickhouse::Row;
use serde::Serialize;
use uuid::Uuid;

use super::fixed_bytes_16;

/// Event row for TRACK events (events_v1 table)
///
/// ```sql
/// CREATE TABLE events_v1 (
///     timestamp DateTime64(3),
///     event_name LowCardinality(String),
///     device_id UUID,
///     session_id UUID,
///     source_ip FixedString(16),
///     properties JSON,
///     _raw String CODEC(ZSTD(3))
/// ) ENGINE = MergeTree()
/// PARTITION BY toYYYYMM(timestamp)
/// ORDER BY (timestamp, event_name, device_id);
/// ```
#[derive(Debug, Clone, Row, Serialize)]
pub struct EventRow {
    /// Event timestamp in milliseconds since epoch
    pub timestamp: i64,

    /// Event name (e.g., "page_view", "button_click")
    pub event_name: String,

    /// Device UUID
    #[serde(with = "clickhouse::serde::uuid")]
    pub device_id: Uuid,

    /// Session UUID
    #[serde(with = "clickhouse::serde::uuid")]
    pub session_id: Uuid,

    /// Source IP address (16 bytes, IPv6 format)
    #[serde(with = "fixed_bytes_16")]
    pub source_ip: [u8; 16],

    /// Event properties as JSON string
    pub properties: String,

    /// Raw payload (same as properties for zero-copy)
    #[serde(rename = "_raw")]
    pub raw: String,
}

/// Context row for CONTEXT events (context_v1 table)
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
/// ) ENGINE = MergeTree()
/// PARTITION BY toYYYYMM(timestamp)
/// ORDER BY (timestamp, device_type, device_model, operating_system, app_version);
/// ```
#[derive(Debug, Clone, Row, Serialize)]
pub struct ContextRow {
    /// Context timestamp in milliseconds
    pub timestamp: i64,

    /// Device UUID
    #[serde(with = "clickhouse::serde::uuid")]
    pub device_id: Uuid,

    /// Session UUID
    #[serde(with = "clickhouse::serde::uuid")]
    pub session_id: Uuid,

    /// Source IP address (16 bytes, IPv6 format)
    #[serde(with = "fixed_bytes_16")]
    pub source_ip: [u8; 16],

    /// Device type (e.g., "mobile", "desktop", "tablet")
    pub device_type: String,

    /// Device model (e.g., "iPhone 14 Pro")
    pub device_model: String,

    /// Operating system (e.g., "iOS", "Android", "Windows")
    pub operating_system: String,

    /// OS version (e.g., "16.4")
    pub os_version: String,

    /// Application version (e.g., "2.1.0")
    pub app_version: String,

    /// Application build number (e.g., "1234")
    pub app_build: String,

    /// User timezone (e.g., "America/New_York")
    pub timezone: String,

    /// User locale, exactly 5 chars (e.g., "en_US")
    pub locale: String,

    /// Country code or name
    pub country: String,

    /// Region/state
    pub region: String,

    /// City name
    pub city: String,

    /// Raw properties as JSON string
    pub properties: String,
}
