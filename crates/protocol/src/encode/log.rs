//! Log FlatBuffer encoding
//!
//! Encodes log entries into LogData FlatBuffer format as defined in log.fbs.
//!
//! # Schema (log.fbs)
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
//!
//! # Wire Format Layout
//!
//! Forward layout for FlatBuffer compatibility:
//! ```text
//! [root_offset:u32]
//! [LogData vtable][LogData table][logs vector offset]
//! [LogEntry 0 vtable][LogEntry 0 table][LogEntry 0 vectors...]
//! [LogEntry 1 vtable][LogEntry 1 table][LogEntry 1 vectors...]
//! ...
//! ```

use super::{write_i32, write_u16, write_u32, write_u64};

/// Log event type (matches LogEventType in log.fbs)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum LogEventTypeValue {
    Unknown = 0,
    Log = 1,
    Enrich = 2,
}

impl LogEventTypeValue {
    /// Parse from string (case-insensitive)
    pub fn parse(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "log" => Self::Log,
            "enrich" => Self::Enrich,
            _ => Self::Unknown,
        }
    }
}

/// Log level (matches LogLevel in log.fbs)
///
/// Values match the decoder's LogLevel enum for correct round-trip.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum LogLevelValue {
    Trace = 0,
    Debug = 1,
    Fatal = 2,
    Error = 3,
    Warning = 4,
    Info = 6,
}

impl LogLevelValue {
    /// Parse from string (case-insensitive)
    pub fn parse(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "trace" => Self::Trace,
            "debug" => Self::Debug,
            "fatal" | "critical" | "crit" | "emergency" | "emerg" | "alert" => Self::Fatal,
            "error" | "err" => Self::Error,
            "warning" | "warn" | "notice" => Self::Warning,
            "info" | "information" => Self::Info,
            _ => Self::Info, // Default to Info
        }
    }
}

/// Log entry data for encoding
#[derive(Debug, Clone)]
pub struct EncodedLogEntry {
    pub event_type: LogEventTypeValue,
    pub session_id: Option<[u8; 16]>,
    pub level: LogLevelValue,
    pub timestamp: u64,
    pub source: Option<String>,
    pub service: Option<String>,
    pub payload: Vec<u8>,
}

impl Default for EncodedLogEntry {
    fn default() -> Self {
        Self {
            event_type: LogEventTypeValue::Log,
            session_id: None,
            level: LogLevelValue::Info,
            timestamp: 0,
            source: None,
            service: None,
            payload: Vec::new(),
        }
    }
}

/// Encoder for LogData FlatBuffer
pub struct LogEncoder;

impl LogEncoder {
    /// Encode multiple log entries into LogData FlatBuffer
    ///
    /// Layout:
    /// - Root offset (4 bytes)
    /// - LogData vtable + table
    /// - Logs vector (offsets to LogEntry tables)
    /// - LogEntry tables with their vectors
    pub fn encode_logs(logs: &[EncodedLogEntry]) -> Vec<u8> {
        // Estimate capacity
        let capacity = 64 + logs.len() * 128;
        let mut buf = Vec::with_capacity(capacity);

        // Reserve root offset
        buf.extend_from_slice(&[0u8; 4]);

        // === LogData vtable ===
        let log_data_vtable_start = buf.len();
        write_u16(&mut buf, 6); // vtable_size: 4 header + 1 field * 2
        write_u16(&mut buf, 8); // table_size: soffset(4) + logs_offset(4)
        write_u16(&mut buf, 4); // field 0 (logs) at offset 4

        // Align to 4
        while !buf.len().is_multiple_of(4) {
            buf.push(0);
        }

        // === LogData table ===
        let log_data_table_start = buf.len();
        let soffset = (log_data_table_start - log_data_vtable_start) as i32;
        write_i32(&mut buf, soffset);

        // Placeholder for logs vector offset
        let logs_vec_offset_pos = buf.len();
        buf.extend_from_slice(&[0u8; 4]);

        // Align
        while !buf.len().is_multiple_of(4) {
            buf.push(0);
        }

        // === Logs vector (offsets to LogEntry tables) ===
        let logs_vec_start = buf.len();
        write_u32(&mut buf, logs.len() as u32);

        // Placeholder slots for log table offsets
        let log_offset_slots_start = buf.len();
        for _ in 0..logs.len() {
            buf.extend_from_slice(&[0u8; 4]);
        }

        // Align
        while !buf.len().is_multiple_of(4) {
            buf.push(0);
        }

        // === Encode each LogEntry ===
        let mut log_table_offsets = Vec::with_capacity(logs.len());

        for log in logs {
            let table_offset = encode_single_log_entry(&mut buf, log);
            log_table_offsets.push(table_offset);
        }

        // === Fill in offsets ===

        // LogData -> logs vector offset
        let logs_vec_rel = (logs_vec_start - logs_vec_offset_pos) as u32;
        buf[logs_vec_offset_pos..logs_vec_offset_pos + 4]
            .copy_from_slice(&logs_vec_rel.to_le_bytes());

        // Logs vector -> each LogEntry table offset
        for (i, &table_offset) in log_table_offsets.iter().enumerate() {
            let slot_pos = log_offset_slots_start + i * 4;
            let rel = (table_offset - slot_pos) as u32;
            buf[slot_pos..slot_pos + 4].copy_from_slice(&rel.to_le_bytes());
        }

        // Root offset
        let root_offset = log_data_table_start as u32;
        buf[0..4].copy_from_slice(&root_offset.to_le_bytes());

        buf
    }
}

impl Default for LogEncoder {
    fn default() -> Self {
        Self
    }
}

/// Encode a single LogEntry table with forward layout
/// Returns the table offset in the buffer
fn encode_single_log_entry(buf: &mut Vec<u8>, log: &EncodedLogEntry) -> usize {
    // === LogEntry vtable ===
    let vtable_start = buf.len();
    let num_fields = 7;
    let vtable_size = 4 + num_fields * 2; // 18 bytes

    // Table inline layout (after soffset):
    // +4: event_type (u8)
    // +5: level (u8)
    // +6: padding (2 bytes)
    // +8: timestamp (u64)
    // +16: session_id offset (u32)
    // +20: source offset (u32)
    // +24: service offset (u32)
    // +28: payload offset (u32)
    let table_inline_size = 4 + 1 + 1 + 2 + 8 + 4 + 4 + 4 + 4; // 32 bytes

    write_u16(buf, vtable_size as u16);
    write_u16(buf, table_inline_size as u16);

    // Field offsets
    write_u16(buf, 4); // field 0: event_type at +4
    write_u16(buf, if log.session_id.is_some() { 16 } else { 0 }); // field 1
    write_u16(buf, 5); // field 2: level at +5
    write_u16(buf, 8); // field 3: timestamp at +8
    write_u16(buf, if log.source.is_some() { 20 } else { 0 }); // field 4
    write_u16(buf, if log.service.is_some() { 24 } else { 0 }); // field 5
    write_u16(buf, if !log.payload.is_empty() { 28 } else { 0 }); // field 6

    // Align
    while !buf.len().is_multiple_of(4) {
        buf.push(0);
    }

    // === LogEntry table ===
    let table_start = buf.len();
    let soffset = (table_start - vtable_start) as i32;
    write_i32(buf, soffset);

    // event_type (u8)
    buf.push(log.event_type as u8);
    // level (u8)
    buf.push(log.level as u8);
    // padding (2 bytes)
    buf.extend_from_slice(&[0, 0]);

    // timestamp (u64)
    write_u64(buf, log.timestamp);

    // Placeholder offsets for vectors
    let session_id_offset_pos = buf.len();
    buf.extend_from_slice(&[0u8; 4]);

    let source_offset_pos = buf.len();
    buf.extend_from_slice(&[0u8; 4]);

    let service_offset_pos = buf.len();
    buf.extend_from_slice(&[0u8; 4]);

    let payload_offset_pos = buf.len();
    buf.extend_from_slice(&[0u8; 4]);

    // Align
    while !buf.len().is_multiple_of(4) {
        buf.push(0);
    }

    // === Vectors (written AFTER table) ===

    // session_id
    if let Some(ref session_id) = log.session_id {
        let vec_start = buf.len();
        write_u32(buf, 16);
        buf.extend_from_slice(session_id);
        while !buf.len().is_multiple_of(4) {
            buf.push(0);
        }
        let rel = (vec_start - session_id_offset_pos) as u32;
        buf[session_id_offset_pos..session_id_offset_pos + 4].copy_from_slice(&rel.to_le_bytes());
    }

    // source
    if let Some(ref source) = log.source {
        let vec_start = buf.len();
        write_u32(buf, source.len() as u32);
        buf.extend_from_slice(source.as_bytes());
        buf.push(0); // null terminator
        while !buf.len().is_multiple_of(4) {
            buf.push(0);
        }
        let rel = (vec_start - source_offset_pos) as u32;
        buf[source_offset_pos..source_offset_pos + 4].copy_from_slice(&rel.to_le_bytes());
    }

    // service
    if let Some(ref service) = log.service {
        let vec_start = buf.len();
        write_u32(buf, service.len() as u32);
        buf.extend_from_slice(service.as_bytes());
        buf.push(0); // null terminator
        while !buf.len().is_multiple_of(4) {
            buf.push(0);
        }
        let rel = (vec_start - service_offset_pos) as u32;
        buf[service_offset_pos..service_offset_pos + 4].copy_from_slice(&rel.to_le_bytes());
    }

    // payload
    if !log.payload.is_empty() {
        let vec_start = buf.len();
        write_u32(buf, log.payload.len() as u32);
        buf.extend_from_slice(&log.payload);
        while !buf.len().is_multiple_of(4) {
            buf.push(0);
        }
        let rel = (vec_start - payload_offset_pos) as u32;
        buf[payload_offset_pos..payload_offset_pos + 4].copy_from_slice(&rel.to_le_bytes());
    }

    table_start
}
