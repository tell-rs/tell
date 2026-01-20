//! LogEntry and LogData FlatBuffer builders
//!
//! Constructs valid FlatBuffer wire format for structured log entries.

use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::{BuilderError, Result};
use crate::log::{LogEventType, LogLevel};

/// UUID length in bytes
const UUID_LENGTH: usize = 16;

/// Maximum payload size (1 MB)
const MAX_PAYLOAD_SIZE: usize = 1024 * 1024;

/// Maximum source/service string length
const MAX_STRING_LENGTH: usize = 256;

/// Builder for constructing a single LogEntry
///
/// # Example
///
/// ```
/// use tell_client::log::{LogEntryBuilder, LogLevel};
///
/// let log = LogEntryBuilder::new()
///     .level(LogLevel::Error)
///     .source("web-01.prod")
///     .service("api-gateway")
///     .timestamp_now()
///     .payload_json(r#"{"error": "timeout"}"#)
///     .build()
///     .unwrap();
/// ```
#[derive(Debug, Clone, Default)]
pub struct LogEntryBuilder {
    event_type: LogEventType,
    session_id: Option<[u8; UUID_LENGTH]>,
    level: LogLevel,
    timestamp: u64,
    source: Option<String>,
    service: Option<String>,
    payload: Option<Vec<u8>>,
}

impl LogEntryBuilder {
    /// Create a new log entry builder
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the log event type
    #[inline]
    #[must_use]
    pub fn event_type(mut self, event_type: LogEventType) -> Self {
        self.event_type = event_type;
        self
    }

    /// Set the session ID (16-byte UUID)
    #[inline]
    #[must_use]
    pub fn session_id(mut self, id: [u8; UUID_LENGTH]) -> Self {
        self.session_id = Some(id);
        self
    }

    /// Set the session ID from a slice
    ///
    /// # Errors
    ///
    /// Returns error if slice is not exactly 16 bytes.
    pub fn session_id_slice(mut self, id: &[u8]) -> Result<Self> {
        if id.len() != UUID_LENGTH {
            return Err(BuilderError::InvalidUuidLength {
                field: "session_id",
                len: id.len(),
            });
        }
        let mut arr = [0u8; UUID_LENGTH];
        arr.copy_from_slice(id);
        self.session_id = Some(arr);
        Ok(self)
    }

    /// Set the log level
    #[inline]
    #[must_use]
    pub fn level(mut self, level: LogLevel) -> Self {
        self.level = level;
        self
    }

    /// Convenience: set level to Emergency
    #[inline]
    #[must_use]
    pub fn emergency(self) -> Self {
        self.level(LogLevel::Emergency)
    }

    /// Convenience: set level to Alert
    #[inline]
    #[must_use]
    pub fn alert(self) -> Self {
        self.level(LogLevel::Alert)
    }

    /// Convenience: set level to Critical
    #[inline]
    #[must_use]
    pub fn critical(self) -> Self {
        self.level(LogLevel::Critical)
    }

    /// Convenience: set level to Error
    #[inline]
    #[must_use]
    pub fn error(self) -> Self {
        self.level(LogLevel::Error)
    }

    /// Convenience: set level to Warning
    #[inline]
    #[must_use]
    pub fn warning(self) -> Self {
        self.level(LogLevel::Warning)
    }

    /// Convenience: set level to Notice
    #[inline]
    #[must_use]
    pub fn notice(self) -> Self {
        self.level(LogLevel::Notice)
    }

    /// Convenience: set level to Info
    #[inline]
    #[must_use]
    pub fn info(self) -> Self {
        self.level(LogLevel::Info)
    }

    /// Convenience: set level to Debug
    #[inline]
    #[must_use]
    pub fn debug(self) -> Self {
        self.level(LogLevel::Debug)
    }

    /// Convenience: set level to Trace
    #[inline]
    #[must_use]
    pub fn trace(self) -> Self {
        self.level(LogLevel::Trace)
    }

    /// Set the timestamp (milliseconds since Unix epoch)
    #[inline]
    #[must_use]
    pub fn timestamp(mut self, timestamp_ms: u64) -> Self {
        self.timestamp = timestamp_ms;
        self
    }

    /// Set the timestamp to now
    #[inline]
    #[must_use]
    pub fn timestamp_now(mut self) -> Self {
        self.timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        self
    }

    /// Set the source hostname/instance
    #[inline]
    #[must_use]
    pub fn source(mut self, source: &str) -> Self {
        self.source = Some(source.to_string());
        self
    }

    /// Set the service/application name
    #[inline]
    #[must_use]
    pub fn service(mut self, service: &str) -> Self {
        self.service = Some(service.to_string());
        self
    }

    /// Set the payload as raw bytes
    #[inline]
    #[must_use]
    pub fn payload(mut self, data: &[u8]) -> Self {
        self.payload = Some(data.to_vec());
        self
    }

    /// Set the payload from a JSON string
    #[inline]
    #[must_use]
    pub fn payload_json(self, json: &str) -> Self {
        self.payload(json.as_bytes())
    }

    /// Set the payload from owned bytes
    #[inline]
    #[must_use]
    pub fn payload_owned(mut self, data: Vec<u8>) -> Self {
        self.payload = Some(data);
        self
    }

    /// Build the LogEntry FlatBuffer
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - `source` exceeds maximum length
    /// - `service` exceeds maximum length
    /// - `payload` exceeds maximum size
    pub fn build(self) -> Result<BuiltLogEntry> {
        // Validate source length
        if let Some(ref source) = self.source
            && source.len() > MAX_STRING_LENGTH
        {
            return Err(BuilderError::SourceTooLong {
                len: source.len(),
                max: MAX_STRING_LENGTH,
            });
        }

        // Validate service length
        if let Some(ref service) = self.service
            && service.len() > MAX_STRING_LENGTH
        {
            return Err(BuilderError::ServiceTooLong {
                len: service.len(),
                max: MAX_STRING_LENGTH,
            });
        }

        // Validate payload size
        if let Some(ref payload) = self.payload
            && payload.len() > MAX_PAYLOAD_SIZE
        {
            return Err(BuilderError::PayloadTooLarge {
                len: payload.len(),
                max: MAX_PAYLOAD_SIZE,
            });
        }

        let bytes = build_log_entry_flatbuffer(
            self.event_type,
            self.session_id.as_ref(),
            self.level,
            self.timestamp,
            self.source.as_deref(),
            self.service.as_deref(),
            self.payload.as_deref(),
        );

        Ok(BuiltLogEntry { bytes })
    }
}

/// A built LogEntry ready to add to LogData
#[derive(Debug, Clone)]
pub struct BuiltLogEntry {
    bytes: Vec<u8>,
}

impl BuiltLogEntry {
    /// Get the raw bytes
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Convert to owned Vec
    #[inline]
    pub fn into_vec(self) -> Vec<u8> {
        self.bytes
    }

    /// Get the length in bytes
    #[inline]
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Check if empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

/// Builder for constructing LogData (batch of log entries)
///
/// # Example
///
/// ```
/// use tell_client::log::{LogEntryBuilder, LogDataBuilder, LogLevel};
///
/// let log1 = LogEntryBuilder::new()
///     .error()
///     .source("server-1")
///     .build()
///     .unwrap();
///
/// let log2 = LogEntryBuilder::new()
///     .info()
///     .source("server-1")
///     .build()
///     .unwrap();
///
/// let log_data = LogDataBuilder::new()
///     .add(log1)
///     .add(log2)
///     .build()
///     .unwrap();
/// ```
#[derive(Debug, Clone, Default)]
pub struct LogDataBuilder {
    logs: Vec<BuiltLogEntry>,
}

impl LogDataBuilder {
    /// Create a new LogData builder
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a log entry to the batch
    #[inline]
    #[must_use]
    #[allow(clippy::should_implement_trait)]
    pub fn add(mut self, log: BuiltLogEntry) -> Self {
        self.logs.push(log);
        self
    }

    /// Add multiple log entries
    #[inline]
    #[must_use]
    pub fn extend(mut self, logs: impl IntoIterator<Item = BuiltLogEntry>) -> Self {
        self.logs.extend(logs);
        self
    }

    /// Get the number of log entries
    #[inline]
    pub fn len(&self) -> usize {
        self.logs.len()
    }

    /// Check if empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.logs.is_empty()
    }

    /// Build the LogData FlatBuffer
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - No log entries were added
    pub fn build(self) -> Result<BuiltLogData> {
        if self.logs.is_empty() {
            return Err(BuilderError::EmptyLogData);
        }

        let bytes = build_log_data_flatbuffer(&self.logs);
        Ok(BuiltLogData { bytes })
    }
}

/// A built LogData ready to use as Batch.data
#[derive(Debug, Clone)]
pub struct BuiltLogData {
    bytes: Vec<u8>,
}

impl BuiltLogData {
    /// Get the raw bytes
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Convert to owned Vec
    #[inline]
    pub fn into_vec(self) -> Vec<u8> {
        self.bytes
    }

    /// Get the length in bytes
    #[inline]
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Check if empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

// =============================================================================
// FlatBuffer encoding
// =============================================================================

/// Build a single LogEntry FlatBuffer
///
/// LogEntry table layout:
/// - event_type: u8 (field 0)
/// - session_id: [u8] vector (field 1)
/// - level: u8 (field 2)
/// - timestamp: u64 (field 3)
/// - source: string (field 4)
/// - service: string (field 5)
/// - payload: [u8] vector (field 6)
fn build_log_entry_flatbuffer(
    event_type: LogEventType,
    session_id: Option<&[u8; UUID_LENGTH]>,
    level: LogLevel,
    timestamp: u64,
    source: Option<&str>,
    service: Option<&str>,
    payload: Option<&[u8]>,
) -> Vec<u8> {
    // VTable: size(u16) + table_size(u16) + 7 field slots (u16 each) = 18 bytes
    let vtable_size: u16 = 4 + 7 * 2;

    // Fixed table layout (after soffset):
    // +4: session_id offset (u32)
    // +8: source offset (u32)
    // +12: service offset (u32)
    // +16: payload offset (u32)
    // +20: timestamp (u64)
    // +28: event_type (u8)
    // +29: level (u8)
    // +30-31: padding
    let table_size: u16 = 4 + 28;

    // Estimate buffer size
    let session_id_size = if session_id.is_some() {
        4 + UUID_LENGTH
    } else {
        0
    };
    let source_size = source.map(|s| 4 + s.len() + 1).unwrap_or(0);
    let service_size = service.map(|s| 4 + s.len() + 1).unwrap_or(0);
    let payload_size = payload.map(|p| 4 + p.len()).unwrap_or(0);

    let estimated_size = 4
        + vtable_size as usize
        + table_size as usize
        + session_id_size
        + source_size
        + service_size
        + payload_size
        + 16;

    let mut buf = Vec::with_capacity(estimated_size);

    // === Root offset placeholder ===
    buf.extend_from_slice(&[0u8; 4]);

    // === VTable ===
    let vtable_start = buf.len();

    buf.extend_from_slice(&vtable_size.to_le_bytes());
    buf.extend_from_slice(&table_size.to_le_bytes());

    // Field offsets
    buf.extend_from_slice(&28u16.to_le_bytes()); // field 0: event_type at +28
    buf.extend_from_slice(&(if session_id.is_some() { 4u16 } else { 0u16 }).to_le_bytes()); // field 1: session_id
    buf.extend_from_slice(&29u16.to_le_bytes()); // field 2: level at +29
    buf.extend_from_slice(&20u16.to_le_bytes()); // field 3: timestamp at +20
    buf.extend_from_slice(&(if source.is_some() { 8u16 } else { 0u16 }).to_le_bytes()); // field 4: source
    buf.extend_from_slice(&(if service.is_some() { 12u16 } else { 0u16 }).to_le_bytes()); // field 5: service
    buf.extend_from_slice(&(if payload.is_some() { 16u16 } else { 0u16 }).to_le_bytes()); // field 6: payload

    // Align vtable to 4 bytes (vtable_size is 18, need 2 bytes padding)
    buf.extend_from_slice(&[0u8; 2]);

    // === Table ===
    let table_start = buf.len();

    // soffset: distance from table to vtable (standard FlatBuffers: vtable = table - soffset)
    let soffset: i32 = (table_start - vtable_start) as i32;
    buf.extend_from_slice(&soffset.to_le_bytes());

    // Placeholders for vector/string offsets
    let session_id_offset_pos = buf.len();
    buf.extend_from_slice(&[0u8; 4]);

    let source_offset_pos = buf.len();
    buf.extend_from_slice(&[0u8; 4]);

    let service_offset_pos = buf.len();
    buf.extend_from_slice(&[0u8; 4]);

    let payload_offset_pos = buf.len();
    buf.extend_from_slice(&[0u8; 4]);

    // timestamp (u64)
    buf.extend_from_slice(&timestamp.to_le_bytes());

    // event_type (u8)
    buf.push(event_type.as_u8());

    // level (u8)
    buf.push(level.as_u8());

    // padding (2 bytes)
    buf.extend_from_slice(&[0u8; 2]);

    // === Vectors and strings ===

    // Align to 4 bytes
    while buf.len() % 4 != 0 {
        buf.push(0);
    }

    // session_id vector
    let session_id_vec_start = if let Some(id) = session_id {
        let start = buf.len();
        buf.extend_from_slice(&(UUID_LENGTH as u32).to_le_bytes());
        buf.extend_from_slice(id);
        Some(start)
    } else {
        None
    };

    // Align
    while buf.len() % 4 != 0 {
        buf.push(0);
    }

    // source string
    let source_start = if let Some(s) = source {
        let start = buf.len();
        buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
        buf.extend_from_slice(s.as_bytes());
        buf.push(0); // null terminator
        Some(start)
    } else {
        None
    };

    // Align
    while buf.len() % 4 != 0 {
        buf.push(0);
    }

    // service string
    let service_start = if let Some(s) = service {
        let start = buf.len();
        buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
        buf.extend_from_slice(s.as_bytes());
        buf.push(0); // null terminator
        Some(start)
    } else {
        None
    };

    // Align
    while buf.len() % 4 != 0 {
        buf.push(0);
    }

    // payload vector
    let payload_vec_start = if let Some(data) = payload {
        let start = buf.len();
        buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
        buf.extend_from_slice(data);
        Some(start)
    } else {
        None
    };

    // === Fill in offsets ===

    // Root offset
    buf[0..4].copy_from_slice(&(table_start as u32).to_le_bytes());

    // session_id offset
    if let Some(start) = session_id_vec_start {
        let rel = (start - session_id_offset_pos) as u32;
        buf[session_id_offset_pos..session_id_offset_pos + 4].copy_from_slice(&rel.to_le_bytes());
    }

    // source offset
    if let Some(start) = source_start {
        let rel = (start - source_offset_pos) as u32;
        buf[source_offset_pos..source_offset_pos + 4].copy_from_slice(&rel.to_le_bytes());
    }

    // service offset
    if let Some(start) = service_start {
        let rel = (start - service_offset_pos) as u32;
        buf[service_offset_pos..service_offset_pos + 4].copy_from_slice(&rel.to_le_bytes());
    }

    // payload offset
    if let Some(start) = payload_vec_start {
        let rel = (start - payload_offset_pos) as u32;
        buf[payload_offset_pos..payload_offset_pos + 4].copy_from_slice(&rel.to_le_bytes());
    }

    buf
}

/// Build LogData FlatBuffer (vector of LogEntry tables)
///
/// IMPORTANT: Each BuiltLogEntry is a standalone FlatBuffer with its own root offset.
/// When embedding logs into the vector, the vector offsets must point to the
/// actual table data (soffset position), not to the root offset. We achieve this
/// by reading each log's root offset and adding it to get the table position.
fn build_log_data_flatbuffer(logs: &[BuiltLogEntry]) -> Vec<u8> {
    // VTable: size(u16) + table_size(u16) + 1 field slot = 8 bytes
    let vtable_size: u16 = 4 + 2;

    // Table: soffset(i32) + logs_offset(u32) = 8 bytes
    let table_size: u16 = 8;

    // Estimate size
    let logs_total_size: usize = logs.iter().map(|l| l.len() + 4).sum();
    let estimated_size = 4 + vtable_size as usize + table_size as usize + 4 + logs_total_size + 64;

    let mut buf = Vec::with_capacity(estimated_size);

    // === Root offset placeholder ===
    buf.extend_from_slice(&[0u8; 4]);

    // === VTable ===
    let vtable_start = buf.len();

    buf.extend_from_slice(&vtable_size.to_le_bytes());
    buf.extend_from_slice(&table_size.to_le_bytes());
    buf.extend_from_slice(&4u16.to_le_bytes()); // field 0: logs offset at +4

    // Align vtable to 4 bytes
    buf.extend_from_slice(&[0u8; 2]);

    // === Table ===
    let table_start = buf.len();

    // soffset: distance from table to vtable (standard FlatBuffers: vtable = table - soffset)
    let soffset: i32 = (table_start - vtable_start) as i32;
    buf.extend_from_slice(&soffset.to_le_bytes());

    // logs vector offset placeholder
    let logs_offset_pos = buf.len();
    buf.extend_from_slice(&[0u8; 4]);

    // Align
    while buf.len() % 4 != 0 {
        buf.push(0);
    }

    // === Logs vector ===
    let logs_vec_start = buf.len();
    let log_count = logs.len();

    // Vector length
    buf.extend_from_slice(&(log_count as u32).to_le_bytes());

    // Reserve space for offsets
    let offsets_start = buf.len();
    for _ in 0..log_count {
        buf.extend_from_slice(&[0u8; 4]);
    }

    // Align
    while buf.len() % 4 != 0 {
        buf.push(0);
    }

    // Write log data and calculate table positions
    // Each log is a standalone FlatBuffer: [root_offset:u32][data...]
    // The table position = log_start + root_offset_value
    let mut table_positions = Vec::with_capacity(log_count);
    for log in logs {
        // Align before each log
        while buf.len() % 4 != 0 {
            buf.push(0);
        }

        let log_start = buf.len();
        let log_bytes = log.as_bytes();

        // Read the root offset from the log (first 4 bytes, little-endian u32)
        let root_offset = if log_bytes.len() >= 4 {
            u32::from_le_bytes([log_bytes[0], log_bytes[1], log_bytes[2], log_bytes[3]]) as usize
        } else {
            0
        };

        // The actual table position within the LogData buffer
        let table_pos = log_start + root_offset;
        table_positions.push(table_pos);

        buf.extend_from_slice(log_bytes);
    }

    // Fill in log offsets (relative from offset position to table position)
    for (i, &table_pos) in table_positions.iter().enumerate() {
        let offset_pos = offsets_start + i * 4;
        let rel = (table_pos - offset_pos) as u32;
        buf[offset_pos..offset_pos + 4].copy_from_slice(&rel.to_le_bytes());
    }

    // Fill in logs vector offset
    let logs_rel = (logs_vec_start - logs_offset_pos) as u32;
    buf[logs_offset_pos..logs_offset_pos + 4].copy_from_slice(&logs_rel.to_le_bytes());

    // Fill in root offset
    buf[0..4].copy_from_slice(&(table_start as u32).to_le_bytes());

    buf
}
