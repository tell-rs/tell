//! Log table row types (logs_v1)

use clickhouse::Row;
use serde::Serialize;
use serde_repr::Serialize_repr;
use uuid::Uuid;

use super::fixed_bytes_16;

/// Log level enum matching ClickHouse Enum8 (RFC 5424 syslog levels + TRACE)
///
/// Maps to: Enum8('EMERGENCY'=0, 'ALERT'=1, 'CRITICAL'=2, 'ERROR'=3, 'WARNING'=4, 'NOTICE'=5, 'INFO'=6, 'DEBUG'=7, 'TRACE'=8)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr)]
#[repr(i8)]
pub enum LogLevelEnum {
    Emergency = 0,
    Alert = 1,
    Critical = 2,
    Error = 3,
    Warning = 4,
    Notice = 5,
    Info = 6,
    Debug = 7,
    Trace = 8,
}

impl LogLevelEnum {
    /// Convert from tell_protocol::LogLevel
    pub fn from_protocol_level(level: tell_protocol::LogLevel) -> Self {
        match level {
            tell_protocol::LogLevel::Trace => Self::Trace,
            tell_protocol::LogLevel::Debug => Self::Debug,
            tell_protocol::LogLevel::Info => Self::Info,
            tell_protocol::LogLevel::Warning => Self::Warning,
            tell_protocol::LogLevel::Error => Self::Error,
            tell_protocol::LogLevel::Fatal => Self::Emergency, // Map Fatal to Emergency (RFC 5424)
        }
    }

    /// Convert from string level name (case-insensitive)
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Self {
        match s.to_uppercase().as_str() {
            "EMERGENCY" | "FATAL" => Self::Emergency,
            "ALERT" => Self::Alert,
            "CRITICAL" | "CRIT" => Self::Critical,
            "ERROR" | "ERR" => Self::Error,
            "WARNING" | "WARN" => Self::Warning,
            "NOTICE" => Self::Notice,
            "INFO" => Self::Info,
            "DEBUG" => Self::Debug,
            "TRACE" => Self::Trace,
            _ => Self::Info, // Default to Info for unknown levels
        }
    }
}

/// Log row for log entries (logs_v1 table)
///
/// ```sql
/// CREATE TABLE logs_v1 (
///     timestamp DateTime64(3),
///     level Enum8('EMERGENCY'=0, 'ALERT'=1, 'CRITICAL'=2, 'ERROR'=3, 'WARNING'=4, 'NOTICE'=5, 'INFO'=6, 'DEBUG'=7, 'TRACE'=8),
///     source LowCardinality(String),
///     service LowCardinality(String),
///     session_id UUID,
///     source_ip FixedString(16),
///     pattern_id Nullable(UInt64),
///     message JSON,
///     _raw String CODEC(ZSTD(3))
/// ) ENGINE = MergeTree()
/// PARTITION BY toYYYYMM(timestamp)
/// ORDER BY (timestamp, service, level);
/// ```
#[derive(Debug, Clone, Row, Serialize)]
pub struct LogRow {
    /// Log timestamp in milliseconds
    pub timestamp: i64,

    /// Log level (Enum8 matching RFC 5424 syslog levels)
    pub level: LogLevelEnum,

    /// Source hostname/instance
    pub source: String,

    /// Service/application name
    pub service: String,

    /// Session UUID
    #[serde(with = "clickhouse::serde::uuid")]
    pub session_id: Uuid,

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
