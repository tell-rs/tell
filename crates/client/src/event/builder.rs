//! Event and EventData FlatBuffer builders
//!
//! Constructs valid FlatBuffer wire format for product analytics events.

use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::{BuilderError, Result};
use crate::event::EventType;

/// UUID length in bytes
const UUID_LENGTH: usize = 16;

/// Maximum payload size (1 MB)
const MAX_PAYLOAD_SIZE: usize = 1024 * 1024;

/// Maximum event name length
const MAX_EVENT_NAME_LENGTH: usize = 256;

/// Builder for constructing a single Event
///
/// # Example
///
/// ```
/// use tell_client::event::{EventBuilder, EventType};
///
/// let event = EventBuilder::new()
///     .event_type(EventType::Track)
///     .event_name("page_view")
///     .device_id([0x01; 16])
///     .timestamp_now()
///     .payload(b"{\"page\": \"/home\"}")
///     .build()
///     .unwrap();
/// ```
#[derive(Debug, Clone, Default)]
pub struct EventBuilder {
    event_type: EventType,
    timestamp: u64,
    device_id: Option<[u8; UUID_LENGTH]>,
    session_id: Option<[u8; UUID_LENGTH]>,
    event_name: Option<String>,
    payload: Option<Vec<u8>>,
}

impl EventBuilder {
    /// Create a new event builder
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the event type
    #[inline]
    #[must_use]
    pub fn event_type(mut self, event_type: EventType) -> Self {
        self.event_type = event_type;
        self
    }

    /// Convenience: set event type to Track with a name
    #[inline]
    #[must_use]
    pub fn track(self, name: &str) -> Self {
        self.event_type(EventType::Track).event_name(name)
    }

    /// Convenience: set event type to Identify
    #[inline]
    #[must_use]
    pub fn identify(self) -> Self {
        self.event_type(EventType::Identify)
    }

    /// Convenience: set event type to Group
    #[inline]
    #[must_use]
    pub fn group(self) -> Self {
        self.event_type(EventType::Group)
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

    /// Set the device ID (16-byte UUID)
    #[inline]
    #[must_use]
    pub fn device_id(mut self, id: [u8; UUID_LENGTH]) -> Self {
        self.device_id = Some(id);
        self
    }

    /// Set the device ID from a slice
    ///
    /// # Errors
    ///
    /// Returns error if slice is not exactly 16 bytes.
    pub fn device_id_slice(mut self, id: &[u8]) -> Result<Self> {
        if id.len() != UUID_LENGTH {
            return Err(BuilderError::InvalidUuidLength {
                field: "device_id",
                len: id.len(),
            });
        }
        let mut arr = [0u8; UUID_LENGTH];
        arr.copy_from_slice(id);
        self.device_id = Some(arr);
        Ok(self)
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

    /// Set the event name (e.g., "page_view", "checkout_completed")
    #[inline]
    #[must_use]
    pub fn event_name(mut self, name: &str) -> Self {
        self.event_name = Some(name.to_string());
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

    /// Build the Event FlatBuffer
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - `event_name` exceeds maximum length
    /// - `payload` exceeds maximum size
    pub fn build(self) -> Result<BuiltEvent> {
        // Validate event name length
        if let Some(ref name) = self.event_name
            && name.len() > MAX_EVENT_NAME_LENGTH
        {
            return Err(BuilderError::EventNameTooLong {
                len: name.len(),
                max: MAX_EVENT_NAME_LENGTH,
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

        let bytes = build_event_flatbuffer(
            self.event_type,
            self.timestamp,
            self.device_id.as_ref(),
            self.session_id.as_ref(),
            self.event_name.as_deref(),
            self.payload.as_deref(),
        );

        Ok(BuiltEvent { bytes })
    }
}

/// A built Event ready to add to EventData
#[derive(Debug, Clone)]
pub struct BuiltEvent {
    bytes: Vec<u8>,
}

impl BuiltEvent {
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

/// Builder for constructing EventData (batch of events)
///
/// # Example
///
/// ```
/// use tell_client::event::{EventBuilder, EventDataBuilder, EventType};
///
/// let event1 = EventBuilder::new()
///     .track("page_view")
///     .device_id([0x01; 16])
///     .build()
///     .unwrap();
///
/// let event2 = EventBuilder::new()
///     .track("button_click")
///     .device_id([0x01; 16])
///     .build()
///     .unwrap();
///
/// let event_data = EventDataBuilder::new()
///     .add(event1)
///     .add(event2)
///     .build()
///     .unwrap();
/// ```
#[derive(Debug, Clone, Default)]
pub struct EventDataBuilder {
    events: Vec<BuiltEvent>,
}

impl EventDataBuilder {
    /// Create a new EventData builder
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add an event to the batch
    #[inline]
    #[must_use]
    #[allow(clippy::should_implement_trait)]
    pub fn add(mut self, event: BuiltEvent) -> Self {
        self.events.push(event);
        self
    }

    /// Add multiple events
    #[inline]
    #[must_use]
    pub fn extend(mut self, events: impl IntoIterator<Item = BuiltEvent>) -> Self {
        self.events.extend(events);
        self
    }

    /// Get the number of events
    #[inline]
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// Check if empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Build the EventData FlatBuffer
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - No events were added
    pub fn build(self) -> Result<BuiltEventData> {
        if self.events.is_empty() {
            return Err(BuilderError::EmptyEventData);
        }

        let bytes = build_event_data_flatbuffer(&self.events);
        Ok(BuiltEventData { bytes })
    }
}

/// A built EventData ready to use as Batch.data
#[derive(Debug, Clone)]
pub struct BuiltEventData {
    bytes: Vec<u8>,
}

impl BuiltEventData {
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

/// Build a single Event FlatBuffer
///
/// Event table layout:
/// - event_type: u8 (field 0)
/// - timestamp: u64 (field 1)
/// - device_id: [u8] vector (field 2)
/// - session_id: [u8] vector (field 3)
/// - event_name: string (field 4)
/// - payload: [u8] vector (field 5)
fn build_event_flatbuffer(
    event_type: EventType,
    timestamp: u64,
    device_id: Option<&[u8; UUID_LENGTH]>,
    session_id: Option<&[u8; UUID_LENGTH]>,
    event_name: Option<&str>,
    payload: Option<&[u8]>,
) -> Vec<u8> {
    // VTable: size(u16) + table_size(u16) + 6 field slots (u16 each) = 16 bytes
    let vtable_size: u16 = 4 + 6 * 2;

    // Calculate table inline size
    // Fixed layout (after soffset):
    // +4: device_id offset (u32)
    // +8: session_id offset (u32)
    // +12: event_name offset (u32)
    // +16: payload offset (u32)
    // +20: timestamp (u64)
    // +28: event_type (u8)
    // +29-31: padding
    let table_size: u16 = 4 + 28; // soffset + inline data

    // Estimate buffer size
    let device_id_size = if device_id.is_some() {
        4 + UUID_LENGTH
    } else {
        0
    };
    let session_id_size = if session_id.is_some() {
        4 + UUID_LENGTH
    } else {
        0
    };
    let event_name_size = event_name.map(|s| 4 + s.len() + 1).unwrap_or(0); // +1 for null terminator
    let payload_size = payload.map(|p| 4 + p.len()).unwrap_or(0);

    let estimated_size = 4
        + vtable_size as usize
        + table_size as usize
        + device_id_size
        + session_id_size
        + event_name_size
        + payload_size
        + 16;

    let mut buf = Vec::with_capacity(estimated_size);

    // === Root offset placeholder ===
    buf.extend_from_slice(&[0u8; 4]);

    // === VTable ===
    let vtable_start = buf.len();

    buf.extend_from_slice(&vtable_size.to_le_bytes());
    buf.extend_from_slice(&table_size.to_le_bytes());

    // Field offsets (0 if not present)
    buf.extend_from_slice(&28u16.to_le_bytes()); // field 0: event_type at +28
    buf.extend_from_slice(&20u16.to_le_bytes()); // field 1: timestamp at +20
    buf.extend_from_slice(&(if device_id.is_some() { 4u16 } else { 0u16 }).to_le_bytes()); // field 2: device_id
    buf.extend_from_slice(&(if session_id.is_some() { 8u16 } else { 0u16 }).to_le_bytes()); // field 3: session_id
    buf.extend_from_slice(&(if event_name.is_some() { 12u16 } else { 0u16 }).to_le_bytes()); // field 4: event_name
    buf.extend_from_slice(&(if payload.is_some() { 16u16 } else { 0u16 }).to_le_bytes()); // field 5: payload

    // === Table ===
    let table_start = buf.len();

    // soffset: distance from table to vtable (standard FlatBuffers: vtable = table - soffset)
    let soffset: i32 = (table_start - vtable_start) as i32;
    buf.extend_from_slice(&soffset.to_le_bytes());

    // Placeholders for vector/string offsets
    let device_id_offset_pos = buf.len();
    buf.extend_from_slice(&[0u8; 4]);

    let session_id_offset_pos = buf.len();
    buf.extend_from_slice(&[0u8; 4]);

    let event_name_offset_pos = buf.len();
    buf.extend_from_slice(&[0u8; 4]);

    let payload_offset_pos = buf.len();
    buf.extend_from_slice(&[0u8; 4]);

    // timestamp (u64)
    buf.extend_from_slice(&timestamp.to_le_bytes());

    // event_type (u8)
    buf.push(event_type.as_u8());

    // padding (3 bytes to align to 4)
    buf.extend_from_slice(&[0u8; 3]);

    // === Vectors and strings ===

    // Align to 4 bytes
    while buf.len() % 4 != 0 {
        buf.push(0);
    }

    // device_id vector
    let device_id_vec_start = if let Some(id) = device_id {
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

    // event_name string (FlatBuffers strings are null-terminated)
    let event_name_start = if let Some(name) = event_name {
        let start = buf.len();
        buf.extend_from_slice(&(name.len() as u32).to_le_bytes());
        buf.extend_from_slice(name.as_bytes());
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

    // device_id offset
    if let Some(start) = device_id_vec_start {
        let rel = (start - device_id_offset_pos) as u32;
        buf[device_id_offset_pos..device_id_offset_pos + 4].copy_from_slice(&rel.to_le_bytes());
    }

    // session_id offset
    if let Some(start) = session_id_vec_start {
        let rel = (start - session_id_offset_pos) as u32;
        buf[session_id_offset_pos..session_id_offset_pos + 4].copy_from_slice(&rel.to_le_bytes());
    }

    // event_name offset
    if let Some(start) = event_name_start {
        let rel = (start - event_name_offset_pos) as u32;
        buf[event_name_offset_pos..event_name_offset_pos + 4].copy_from_slice(&rel.to_le_bytes());
    }

    // payload offset
    if let Some(start) = payload_vec_start {
        let rel = (start - payload_offset_pos) as u32;
        buf[payload_offset_pos..payload_offset_pos + 4].copy_from_slice(&rel.to_le_bytes());
    }

    buf
}

/// Build EventData FlatBuffer (vector of Event tables)
///
/// EventData table layout:
/// - events: [Event] vector (field 0, required)
///
/// Vector of tables format:
/// - length (u32)
/// - offsets to each table (u32 each, relative to offset position)
///
/// IMPORTANT: Each BuiltEvent is a standalone FlatBuffer with its own root offset.
/// When embedding events into the vector, the vector offsets must point to the
/// actual table data (soffset position), not to the root offset. We achieve this
/// by reading each event's root offset and adding it to get the table position.
fn build_event_data_flatbuffer(events: &[BuiltEvent]) -> Vec<u8> {
    // VTable: size(u16) + table_size(u16) + 1 field slot = 8 bytes
    let vtable_size: u16 = 4 + 2;

    // Table: soffset(i32) + events_offset(u32) = 8 bytes
    let table_size: u16 = 8;

    // Estimate size
    let events_total_size: usize = events.iter().map(|e| e.len() + 4).sum();
    let estimated_size =
        4 + vtable_size as usize + table_size as usize + 4 + events_total_size + 64;

    let mut buf = Vec::with_capacity(estimated_size);

    // === Root offset placeholder ===
    buf.extend_from_slice(&[0u8; 4]);

    // === VTable ===
    let vtable_start = buf.len();

    buf.extend_from_slice(&vtable_size.to_le_bytes());
    buf.extend_from_slice(&table_size.to_le_bytes());
    buf.extend_from_slice(&4u16.to_le_bytes()); // field 0: events offset at +4

    // Align vtable to 4 bytes (vtable_size is 6, need 2 bytes padding)
    buf.extend_from_slice(&[0u8; 2]);

    // === Table ===
    let table_start = buf.len();

    // soffset: distance from table to vtable (standard FlatBuffers: vtable = table - soffset)
    let soffset: i32 = (table_start - vtable_start) as i32;
    buf.extend_from_slice(&soffset.to_le_bytes());

    // events vector offset placeholder
    let events_offset_pos = buf.len();
    buf.extend_from_slice(&[0u8; 4]);

    // Align
    while buf.len() % 4 != 0 {
        buf.push(0);
    }

    // === Events vector ===
    // For vector of tables, we write:
    // 1. Vector length
    // 2. Offsets to each table (relative to offset position)
    // 3. Table data (embedded FlatBuffers)

    let events_vec_start = buf.len();
    let event_count = events.len();

    // Vector length
    buf.extend_from_slice(&(event_count as u32).to_le_bytes());

    // Reserve space for offsets
    let offsets_start = buf.len();
    for _ in 0..event_count {
        buf.extend_from_slice(&[0u8; 4]);
    }

    // Align
    while buf.len() % 4 != 0 {
        buf.push(0);
    }

    // Write event data and calculate table positions
    // Each event is a standalone FlatBuffer: [root_offset:u32][data...]
    // The table position = event_start + root_offset_value
    let mut table_positions = Vec::with_capacity(event_count);
    for event in events {
        // Align before each event
        while buf.len() % 4 != 0 {
            buf.push(0);
        }

        let event_start = buf.len();
        let event_bytes = event.as_bytes();

        // Read the root offset from the event (first 4 bytes, little-endian u32)
        let root_offset = if event_bytes.len() >= 4 {
            u32::from_le_bytes([
                event_bytes[0],
                event_bytes[1],
                event_bytes[2],
                event_bytes[3],
            ]) as usize
        } else {
            0
        };

        // The actual table position within the EventData buffer
        let table_pos = event_start + root_offset;
        table_positions.push(table_pos);

        buf.extend_from_slice(event_bytes);
    }

    // Fill in event offsets (relative from offset position to table position)
    for (i, &table_pos) in table_positions.iter().enumerate() {
        let offset_pos = offsets_start + i * 4;
        let rel = (table_pos - offset_pos) as u32;
        buf[offset_pos..offset_pos + 4].copy_from_slice(&rel.to_le_bytes());
    }

    // Fill in events vector offset
    let events_rel = (events_vec_start - events_offset_pos) as u32;
    buf[events_offset_pos..events_offset_pos + 4].copy_from_slice(&events_rel.to_le_bytes());

    // Fill in root offset
    buf[0..4].copy_from_slice(&(table_start as u32).to_le_bytes());

    buf
}
