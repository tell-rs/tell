//! Log table row types (logs_v1)

use clickhouse::Row;
use serde::Serialize;

use super::fixed_bytes_16;

/// Log row for log entries (logs_v1 table)
///
/// ```sql
/// CREATE TABLE logs_v1 (
///     timestamp DateTime64(3),
///     level LowCardinality(String),
///     source String,
///     service String,
///     session_id UUID,
///     source_ip FixedString(16),
///     pattern_id Nullable(UInt64),
///     message String,
///     _raw String
/// ) ENGINE = MergeTree()
/// PARTITION BY toYYYYMM(timestamp)
/// ORDER BY (timestamp, level);
/// ```
#[derive(Debug, Clone, Row, Serialize)]
pub struct LogRow {
    /// Log timestamp in milliseconds
    pub timestamp: i64,

    /// Log level (emergency, alert, critical, error, warning, notice, info, debug, trace)
    pub level: String,

    /// Source hostname/instance
    pub source: String,

    /// Service/application name
    pub service: String,

    /// Session UUID (16 bytes)
    #[serde(with = "fixed_bytes_16")]
    pub session_id: [u8; 16],

    /// Source IP address (16 bytes, IPv6 format)
    #[serde(with = "fixed_bytes_16")]
    pub source_ip: [u8; 16],

    /// Pattern ID from transformer (optional)
    pub pattern_id: Option<u64>,

    /// Log message (structured data as string)
    pub message: String,

    /// Raw payload
    #[serde(rename = "_raw")]
    pub raw: String,
}
