//! Event decoding from FlatBuffer payloads
//!
//! Parses EventData FlatBuffer containing product analytics events.
//!
//! # Schema Reference (event.fbs)
//!
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

use crate::{ProtocolError, Result};
use super::table::{FlatTable, read_u32};

// =============================================================================
// Event Types
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
        let event = parse_event(&event_table)?;
        events.push(event);
    }

    Ok(events)
}

/// Parse a single Event table
fn parse_event<'a>(table: &FlatTable<'a>) -> Result<DecodedEvent<'a>> {
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
