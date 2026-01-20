//! Log decoding from FlatBuffer payloads
//!
//! Parses LogData FlatBuffer containing structured log entries.
//!
//! # Schema Reference (log.fbs)
//!
//! ```text
//! table LogEntry {
//!     event_type:LogEventType (id: 0);
//!     session_id:[ubyte] (id: 1);
//!     level:LogLevel (id: 2);
//!     timestamp:uint64 (id: 3);
//!     source:string (id: 4);
//!     service:string (id: 5);
//!     payload:[ubyte] (id: 6);
//! }
//! table LogData { logs:[LogEntry] (required); }
//! ```

use crate::{ProtocolError, Result};
use super::table::{FlatTable, read_u32};

// =============================================================================
// Log Event Types
// =============================================================================

/// Log event types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum LogEventType {
    Unknown = 0,
    Log = 1,
    Enrich = 2,
}

impl LogEventType {
    /// Parse from raw byte value
    #[inline]
    pub const fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Log,
            2 => Self::Enrich,
            _ => Self::Unknown,
        }
    }

    /// Get string representation
    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Log => "log",
            Self::Enrich => "enrich",
        }
    }
}

impl std::fmt::Display for LogEventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

// =============================================================================
// Log Levels
// =============================================================================

/// Log severity levels (matching syslog/standard conventions)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(u8)]
pub enum LogLevel {
    Trace = 0,
    Debug = 1,
    Info = 6,
    Warning = 4,
    Error = 3,
    Fatal = 2,
}

impl LogLevel {
    /// Parse from raw byte value
    #[inline]
    pub const fn from_u8(value: u8) -> Self {
        match value {
            0 => Self::Trace,
            1 => Self::Debug,
            2 => Self::Fatal,
            3 => Self::Error,
            4 => Self::Warning,
            6 => Self::Info,
            _ => Self::Info, // Default to Info for unknown values
        }
    }

    /// Get string representation
    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Trace => "trace",
            Self::Debug => "debug",
            Self::Info => "info",
            Self::Warning => "warning",
            Self::Error => "error",
            Self::Fatal => "fatal",
        }
    }

    /// Check if this is an error-level log (Error or Fatal)
    #[inline]
    pub const fn is_error(self) -> bool {
        matches!(self, Self::Error | Self::Fatal)
    }
}

impl std::fmt::Display for LogLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

// =============================================================================
// Decoded Log Entry
// =============================================================================

/// A decoded log entry from LogData
#[derive(Debug, Clone)]
pub struct DecodedLogEntry<'a> {
    /// Log event type
    pub event_type: LogEventType,
    /// Session ID (16 bytes UUID)
    pub session_id: Option<&'a [u8; 16]>,
    /// Log severity level
    pub level: LogLevel,
    /// Timestamp in milliseconds since epoch
    pub timestamp: u64,
    /// Source hostname/instance
    pub source: Option<&'a str>,
    /// Service/application name
    pub service: Option<&'a str>,
    /// Payload bytes (usually JSON)
    pub payload: &'a [u8],
}

// =============================================================================
// LogData Parser
// =============================================================================

/// Parse LogData FlatBuffer from bytes
///
/// # Layout
///
/// LogData table:
/// - Field 0: logs (vector of LogEntry tables)
pub fn decode_log_data(buf: &[u8]) -> Result<Vec<DecodedLogEntry<'_>>> {
    if buf.len() < 8 {
        return Err(ProtocolError::too_short(8, buf.len()));
    }

    // Read root offset
    let root_offset = read_u32(buf, 0)? as usize;
    if root_offset >= buf.len() {
        return Err(ProtocolError::invalid_flatbuffer(
            "root offset out of bounds",
        ));
    }

    // Parse LogData table
    let table = FlatTable::parse(buf, root_offset)?;

    // Field 0: logs vector
    let logs_vec = table
        .read_vector_of_tables(0)?
        .ok_or_else(|| ProtocolError::missing_field("logs"))?;

    let mut logs = Vec::with_capacity(logs_vec.len());

    for log_table in logs_vec {
        let log = parse_log_entry(&log_table)?;
        logs.push(log);
    }

    Ok(logs)
}

/// Parse a single LogEntry table
fn parse_log_entry<'a>(table: &FlatTable<'a>) -> Result<DecodedLogEntry<'a>> {
    // Field 0: event_type (u8)
    let event_type = LogEventType::from_u8(table.read_u8(0, 0));

    // Field 1: session_id (vector of ubyte, 16 bytes)
    let session_id = table.read_fixed_bytes::<16>(1)?;

    // Field 2: level (u8)
    let level = LogLevel::from_u8(table.read_u8(2, 6)); // Default to Info

    // Field 3: timestamp (u64)
    let timestamp = table.read_u64(3, 0);

    // Field 4: source (string)
    let source = table.read_string(4)?;

    // Field 5: service (string)
    let service = table.read_string(5)?;

    // Field 6: payload (vector of ubyte)
    let payload = table.read_bytes(6)?.unwrap_or(&[]);

    Ok(DecodedLogEntry {
        event_type,
        session_id,
        level,
        timestamp,
        source,
        service,
        payload,
    })
}
