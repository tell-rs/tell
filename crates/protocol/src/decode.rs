//! Decoder for nested FlatBuffer payloads
//!
//! This module provides zero-copy parsing of the `data` field from `FlatBatch`,
//! which contains either `EventData` (array of events) or `LogData` (array of logs).
//!
//! # Schema Reference
//!
//! From `event.fbs`:
//! ```text
//! table Event {
//!     event_type:EventType (id: 0);
//!     timestamp:uint64 (id: 1);
//!     device_id:[ubyte] (id: 2);
//!     session_id:[ubyte] (id: 3);
//!     event_name:string (id: 4);
//!     payload:[ubyte] (id: 5);
//! }
//! table EventData { events:[Event] (required); }
//! ```
//!
//! From `log.fbs`:
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

// =============================================================================
// Event Types (from event.fbs)
// =============================================================================

/// Event types for different processing paths
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum EventType {
    Unknown = 0,
    Track = 1,
    Identify = 2,
    Group = 3,
    Alias = 4,
    Enrich = 5,
    Context = 6,
}

impl EventType {
    /// Parse from raw byte value
    #[inline]
    pub const fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Track,
            2 => Self::Identify,
            3 => Self::Group,
            4 => Self::Alias,
            5 => Self::Enrich,
            6 => Self::Context,
            _ => Self::Unknown,
        }
    }

    /// Get string representation
    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Track => "track",
            Self::Identify => "identify",
            Self::Group => "group",
            Self::Alias => "alias",
            Self::Enrich => "enrich",
            Self::Context => "context",
        }
    }
}

impl std::fmt::Display for EventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

// =============================================================================
// Log Types (from log.fbs)
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

/// Log severity levels (RFC 5424 + TRACE)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(u8)]
pub enum LogLevel {
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

impl LogLevel {
    /// Parse from raw byte value
    #[inline]
    pub const fn from_u8(value: u8) -> Self {
        match value {
            0 => Self::Emergency,
            1 => Self::Alert,
            2 => Self::Critical,
            3 => Self::Error,
            4 => Self::Warning,
            5 => Self::Notice,
            6 => Self::Info,
            7 => Self::Debug,
            8 => Self::Trace,
            _ => Self::Info, // Default to Info for unknown
        }
    }

    /// Get string representation
    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Emergency => "emergency",
            Self::Alert => "alert",
            Self::Critical => "critical",
            Self::Error => "error",
            Self::Warning => "warning",
            Self::Notice => "notice",
            Self::Info => "info",
            Self::Debug => "debug",
            Self::Trace => "trace",
        }
    }
}

impl std::fmt::Display for LogLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

// =============================================================================
// Decoded Event
// =============================================================================

/// A decoded event from EventData
#[derive(Debug, Clone)]
pub struct DecodedEvent<'a> {
    /// Event type for routing
    pub event_type: EventType,
    /// Timestamp in milliseconds since epoch
    pub timestamp: u64,
    /// Device ID (16 bytes UUID)
    pub device_id: Option<&'a [u8; 16]>,
    /// Session ID (16 bytes UUID)
    pub session_id: Option<&'a [u8; 16]>,
    /// Event name (e.g., "page_view", "button_click")
    pub event_name: Option<&'a str>,
    /// JSON payload bytes
    pub payload: &'a [u8],
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
// Decoded Batch Result
// =============================================================================

/// Result of decoding a FlatBuffer data payload
#[derive(Debug)]
pub enum DecodedData<'a> {
    /// Contains decoded events
    Events(Vec<DecodedEvent<'a>>),
    /// Contains decoded log entries
    Logs(Vec<DecodedLogEntry<'a>>),
}

impl<'a> DecodedData<'a> {
    /// Get events if this is event data
    pub fn events(&self) -> Option<&[DecodedEvent<'a>]> {
        match self {
            Self::Events(events) => Some(events),
            Self::Logs(_) => None,
        }
    }

    /// Get logs if this is log data
    pub fn logs(&self) -> Option<&[DecodedLogEntry<'a>]> {
        match self {
            Self::Events(_) => None,
            Self::Logs(logs) => Some(logs),
        }
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Events(events) => events.is_empty(),
            Self::Logs(logs) => logs.is_empty(),
        }
    }

    /// Get count of items
    pub fn len(&self) -> usize {
        match self {
            Self::Events(events) => events.len(),
            Self::Logs(logs) => logs.len(),
        }
    }
}

// =============================================================================
// EventData Parser
// =============================================================================

/// Parse EventData FlatBuffer from bytes
///
/// # Layout
///
/// EventData table:
/// - Field 0: events (vector of Event tables)
pub fn decode_event_data(buf: &[u8]) -> Result<Vec<DecodedEvent<'_>>> {
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

    // Parse EventData table
    let table = FlatTable::parse(buf, root_offset)?;

    // Field 0: events vector
    let events_vec = table
        .read_vector_of_tables(0)?
        .ok_or_else(|| ProtocolError::missing_field("events"))?;

    let mut events = Vec::with_capacity(events_vec.len());

    for event_table in events_vec {
        let event = parse_event(buf, &event_table)?;
        events.push(event);
    }

    Ok(events)
}

/// Parse a single Event table
fn parse_event<'a>(_buf: &'a [u8], table: &FlatTable<'a>) -> Result<DecodedEvent<'a>> {
    // Field 0: event_type (u8)
    let event_type = EventType::from_u8(table.read_u8(0, 0));

    // Field 1: timestamp (u64)
    let timestamp = table.read_u64(1, 0);

    // Field 2: device_id (vector of ubyte, 16 bytes)
    let device_id = table.read_fixed_bytes::<16>(2)?;

    // Field 3: session_id (vector of ubyte, 16 bytes)
    let session_id = table.read_fixed_bytes::<16>(3)?;

    // Field 4: event_name (string)
    let event_name = table.read_string(4)?;

    // Field 5: payload (vector of ubyte)
    let payload = table.read_bytes(5)?.unwrap_or(&[]);

    Ok(DecodedEvent {
        event_type,
        timestamp,
        device_id,
        session_id,
        event_name,
        payload,
    })
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
        let log = parse_log_entry(buf, &log_table)?;
        logs.push(log);
    }

    Ok(logs)
}

/// Parse a single LogEntry table
fn parse_log_entry<'a>(_buf: &'a [u8], table: &FlatTable<'a>) -> Result<DecodedLogEntry<'a>> {
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

// =============================================================================
// FlatBuffer Table Helper
// =============================================================================

/// Helper for parsing FlatBuffer tables
#[derive(Debug, Clone, Copy)]
struct FlatTable<'a> {
    buf: &'a [u8],
    table_offset: usize,
    vtable_offset: usize,
    vtable_fields: usize,
}

impl<'a> FlatTable<'a> {
    /// Parse a table at the given offset
    fn parse(buf: &'a [u8], table_offset: usize) -> Result<Self> {
        if table_offset + 4 > buf.len() {
            return Err(ProtocolError::invalid_flatbuffer(
                "table offset out of bounds",
            ));
        }

        // Read vtable soffset (signed offset from table to vtable)
        let vtable_soffset = read_i32(buf, table_offset)?;
        let vtable_offset = if vtable_soffset < 0 {
            table_offset
                .checked_sub((-vtable_soffset) as usize)
                .ok_or_else(|| ProtocolError::invalid_flatbuffer("vtable offset underflow"))?
        } else {
            table_offset + vtable_soffset as usize
        };

        if vtable_offset + 4 > buf.len() {
            return Err(ProtocolError::invalid_flatbuffer("vtable out of bounds"));
        }

        let vtable_size = read_u16(buf, vtable_offset)? as usize;
        if vtable_size < 4 || vtable_offset + vtable_size > buf.len() {
            return Err(ProtocolError::invalid_flatbuffer("invalid vtable size"));
        }

        let vtable_fields = (vtable_size - 4) / 2;

        Ok(Self {
            buf,
            table_offset,
            vtable_offset,
            vtable_fields,
        })
    }

    /// Get field offset from vtable
    fn field_offset(&self, field_index: usize) -> Option<usize> {
        if field_index >= self.vtable_fields {
            return None;
        }

        let slot_offset = self.vtable_offset + 4 + (field_index * 2);
        if slot_offset + 2 > self.buf.len() {
            return None;
        }

        let field_offset = read_u16(self.buf, slot_offset).ok()? as usize;
        if field_offset == 0 {
            None
        } else {
            Some(self.table_offset + field_offset)
        }
    }

    /// Read u8 field with default
    fn read_u8(&self, field_index: usize, default: u8) -> u8 {
        self.field_offset(field_index)
            .and_then(|off| self.buf.get(off).copied())
            .unwrap_or(default)
    }

    /// Read u64 field with default
    fn read_u64(&self, field_index: usize, default: u64) -> u64 {
        self.field_offset(field_index)
            .and_then(|off| read_u64(self.buf, off).ok())
            .unwrap_or(default)
    }

    /// Read bytes vector
    fn read_bytes(&self, field_index: usize) -> Result<Option<&'a [u8]>> {
        let Some(field_offset) = self.field_offset(field_index) else {
            return Ok(None);
        };

        read_vector(self.buf, field_offset)
    }

    /// Read fixed-size bytes (e.g., UUID)
    fn read_fixed_bytes<const N: usize>(&self, field_index: usize) -> Result<Option<&'a [u8; N]>> {
        let Some(bytes) = self.read_bytes(field_index)? else {
            return Ok(None);
        };

        if bytes.len() != N {
            return Ok(None); // Wrong length, treat as not present
        }

        // Safe: we just checked the length
        Ok(Some(bytes.try_into().unwrap()))
    }

    /// Read string field
    fn read_string(&self, field_index: usize) -> Result<Option<&'a str>> {
        let Some(bytes) = self.read_bytes(field_index)? else {
            return Ok(None);
        };

        std::str::from_utf8(bytes)
            .map(Some)
            .map_err(|_| ProtocolError::invalid_flatbuffer("invalid UTF-8 string"))
    }

    /// Read vector of tables
    fn read_vector_of_tables(&self, field_index: usize) -> Result<Option<Vec<FlatTable<'a>>>> {
        let Some(field_offset) = self.field_offset(field_index) else {
            return Ok(None);
        };

        if field_offset + 4 > self.buf.len() {
            return Err(ProtocolError::invalid_flatbuffer(
                "vector offset out of bounds",
            ));
        }

        // Read relative offset to vector
        let vector_rel = read_u32(self.buf, field_offset)? as usize;
        let vector_offset = field_offset + vector_rel;

        if vector_offset + 4 > self.buf.len() {
            return Err(ProtocolError::invalid_flatbuffer(
                "vector data out of bounds",
            ));
        }

        // Read vector length
        let length = read_u32(self.buf, vector_offset)? as usize;
        let data_start = vector_offset + 4;

        // Each element is a 4-byte offset to a table
        if data_start + length * 4 > self.buf.len() {
            return Err(ProtocolError::invalid_flatbuffer(
                "vector elements out of bounds",
            ));
        }

        let mut tables = Vec::with_capacity(length);

        for i in 0..length {
            let elem_offset_pos = data_start + i * 4;
            let elem_rel = read_u32(self.buf, elem_offset_pos)? as usize;
            let elem_offset = elem_offset_pos + elem_rel;

            let table = FlatTable::parse(self.buf, elem_offset)?;
            tables.push(table);
        }

        Ok(Some(tables))
    }
}

// =============================================================================
// Read Helpers
// =============================================================================

/// Read vector from buffer
fn read_vector(buf: &[u8], field_offset: usize) -> Result<Option<&[u8]>> {
    if field_offset + 4 > buf.len() {
        return Err(ProtocolError::invalid_flatbuffer(
            "vector offset out of bounds",
        ));
    }

    let vector_rel = read_u32(buf, field_offset)? as usize;
    let vector_offset = field_offset + vector_rel;

    if vector_offset + 4 > buf.len() {
        return Err(ProtocolError::invalid_flatbuffer(
            "vector data out of bounds",
        ));
    }

    let length = read_u32(buf, vector_offset)? as usize;
    let data_start = vector_offset + 4;

    if data_start + length > buf.len() {
        return Err(ProtocolError::invalid_flatbuffer(
            "vector data extends past buffer",
        ));
    }

    Ok(Some(&buf[data_start..data_start + length]))
}

#[inline]
fn read_u16(buf: &[u8], offset: usize) -> Result<u16> {
    if offset + 2 > buf.len() {
        return Err(ProtocolError::too_short(offset + 2, buf.len()));
    }
    Ok(u16::from_le_bytes([buf[offset], buf[offset + 1]]))
}

#[inline]
fn read_u32(buf: &[u8], offset: usize) -> Result<u32> {
    if offset + 4 > buf.len() {
        return Err(ProtocolError::too_short(offset + 4, buf.len()));
    }
    Ok(u32::from_le_bytes([
        buf[offset],
        buf[offset + 1],
        buf[offset + 2],
        buf[offset + 3],
    ]))
}

#[inline]
fn read_i32(buf: &[u8], offset: usize) -> Result<i32> {
    if offset + 4 > buf.len() {
        return Err(ProtocolError::too_short(offset + 4, buf.len()));
    }
    Ok(i32::from_le_bytes([
        buf[offset],
        buf[offset + 1],
        buf[offset + 2],
        buf[offset + 3],
    ]))
}

#[inline]
fn read_u64(buf: &[u8], offset: usize) -> Result<u64> {
    if offset + 8 > buf.len() {
        return Err(ProtocolError::too_short(offset + 8, buf.len()));
    }
    Ok(u64::from_le_bytes([
        buf[offset],
        buf[offset + 1],
        buf[offset + 2],
        buf[offset + 3],
        buf[offset + 4],
        buf[offset + 5],
        buf[offset + 6],
        buf[offset + 7],
    ]))
}

#[cfg(test)]
#[path = "decode_test.rs"]
mod decode_test;
