//! Event FlatBuffer encoding
//!
//! Encodes events into EventData FlatBuffer format as defined in event.fbs.
//!
//! # Schema (event.fbs)
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
//!
//! # Wire Format Layout
//!
//! Forward layout for FlatBuffer compatibility:
//! ```text
//! [root_offset:u32]
//! [EventData vtable][EventData table][events vector offset]
//! [Event 0 vtable][Event 0 table][Event 0 vectors...]
//! [Event 1 vtable][Event 1 table][Event 1 vectors...]
//! ...
//! ```

use super::{write_i32, write_u16, write_u32, write_u64};

/// Event type enum (matches EventType in event.fbs)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum EventTypeValue {
    Unknown = 0,
    Track = 1,
    Identify = 2,
    Group = 3,
    Alias = 4,
    Enrich = 5,
    Context = 6,
}

impl EventTypeValue {
    /// Parse from string (case-insensitive)
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "track" => Self::Track,
            "identify" => Self::Identify,
            "group" => Self::Group,
            "alias" => Self::Alias,
            "enrich" => Self::Enrich,
            "context" => Self::Context,
            _ => Self::Unknown,
        }
    }
}

/// Event data for encoding
#[derive(Debug, Clone)]
pub struct EncodedEvent {
    pub event_type: EventTypeValue,
    pub timestamp: u64,
    pub device_id: Option<[u8; 16]>,
    pub session_id: Option<[u8; 16]>,
    pub event_name: Option<String>,
    pub payload: Vec<u8>,
}

impl Default for EncodedEvent {
    fn default() -> Self {
        Self {
            event_type: EventTypeValue::Unknown,
            timestamp: 0,
            device_id: None,
            session_id: None,
            event_name: None,
            payload: Vec::new(),
        }
    }
}

/// Encoder for EventData FlatBuffer
pub struct EventEncoder;

impl EventEncoder {
    /// Encode multiple events into EventData FlatBuffer
    ///
    /// Layout:
    /// - Root offset (4 bytes)
    /// - EventData vtable + table
    /// - Events vector (offsets to Event tables)
    /// - Event tables with their vectors
    pub fn encode_events(events: &[EncodedEvent]) -> Vec<u8> {
        // Estimate capacity
        let capacity = 64 + events.len() * 128;
        let mut buf = Vec::with_capacity(capacity);

        // Reserve root offset
        buf.extend_from_slice(&[0u8; 4]);

        // === EventData vtable ===
        let event_data_vtable_start = buf.len();
        write_u16(&mut buf, 6); // vtable_size: 4 header + 1 field * 2
        write_u16(&mut buf, 8); // table_size: soffset(4) + events_offset(4)
        write_u16(&mut buf, 4); // field 0 (events) at offset 4

        // Align to 4
        while buf.len() % 4 != 0 {
            buf.push(0);
        }

        // === EventData table ===
        let event_data_table_start = buf.len();
        let soffset = (event_data_table_start - event_data_vtable_start) as i32;
        write_i32(&mut buf, soffset);

        // Placeholder for events vector offset
        let events_vec_offset_pos = buf.len();
        buf.extend_from_slice(&[0u8; 4]);

        // Align
        while buf.len() % 4 != 0 {
            buf.push(0);
        }

        // === Events vector (offsets to Event tables) ===
        let events_vec_start = buf.len();
        write_u32(&mut buf, events.len() as u32);

        // Placeholder slots for event table offsets
        let event_offset_slots_start = buf.len();
        for _ in 0..events.len() {
            buf.extend_from_slice(&[0u8; 4]);
        }

        // Align
        while buf.len() % 4 != 0 {
            buf.push(0);
        }

        // === Encode each Event ===
        let mut event_table_offsets = Vec::with_capacity(events.len());

        for event in events {
            let table_offset = encode_single_event(&mut buf, event);
            event_table_offsets.push(table_offset);
        }

        // === Fill in offsets ===

        // EventData -> events vector offset
        let events_vec_rel = (events_vec_start - events_vec_offset_pos) as u32;
        buf[events_vec_offset_pos..events_vec_offset_pos + 4]
            .copy_from_slice(&events_vec_rel.to_le_bytes());

        // Events vector -> each Event table offset
        for (i, &table_offset) in event_table_offsets.iter().enumerate() {
            let slot_pos = event_offset_slots_start + i * 4;
            let rel = (table_offset as usize - slot_pos) as u32;
            buf[slot_pos..slot_pos + 4].copy_from_slice(&rel.to_le_bytes());
        }

        // Root offset
        let root_offset = event_data_table_start as u32;
        buf[0..4].copy_from_slice(&root_offset.to_le_bytes());

        buf
    }
}

impl Default for EventEncoder {
    fn default() -> Self {
        Self
    }
}

/// Encode a single Event table with forward layout
/// Returns the table offset in the buffer
fn encode_single_event(buf: &mut Vec<u8>, event: &EncodedEvent) -> usize {
    // === Event vtable ===
    let vtable_start = buf.len();
    let num_fields = 6;
    let vtable_size = 4 + num_fields * 2; // 16 bytes

    // Table inline layout (after soffset):
    // +4: event_type (u8) + 3 padding
    // +8: timestamp (u64)
    // +16: device_id offset (u32)
    // +20: session_id offset (u32)
    // +24: event_name offset (u32)
    // +28: payload offset (u32)
    let table_inline_size = 4 + 4 + 8 + 4 + 4 + 4 + 4; // 32 bytes

    write_u16(buf, vtable_size as u16);
    write_u16(buf, table_inline_size as u16);

    // Field offsets
    write_u16(buf, 4); // field 0: event_type at +4
    write_u16(buf, 8); // field 1: timestamp at +8
    write_u16(buf, if event.device_id.is_some() { 16 } else { 0 }); // field 2
    write_u16(buf, if event.session_id.is_some() { 20 } else { 0 }); // field 3
    write_u16(buf, if event.event_name.is_some() { 24 } else { 0 }); // field 4
    write_u16(buf, if !event.payload.is_empty() { 28 } else { 0 }); // field 5

    // Align
    while buf.len() % 4 != 0 {
        buf.push(0);
    }

    // === Event table ===
    let table_start = buf.len();
    let soffset = (table_start - vtable_start) as i32;
    write_i32(buf, soffset);

    // event_type (u8) + 3 padding
    buf.push(event.event_type as u8);
    buf.extend_from_slice(&[0, 0, 0]);

    // timestamp (u64)
    write_u64(buf, event.timestamp);

    // Placeholder offsets for vectors
    let device_id_offset_pos = buf.len();
    buf.extend_from_slice(&[0u8; 4]);

    let session_id_offset_pos = buf.len();
    buf.extend_from_slice(&[0u8; 4]);

    let event_name_offset_pos = buf.len();
    buf.extend_from_slice(&[0u8; 4]);

    let payload_offset_pos = buf.len();
    buf.extend_from_slice(&[0u8; 4]);

    // Align
    while buf.len() % 4 != 0 {
        buf.push(0);
    }

    // === Vectors (written AFTER table) ===

    // device_id
    if let Some(ref device_id) = event.device_id {
        let vec_start = buf.len();
        write_u32(buf, 16);
        buf.extend_from_slice(device_id);
        while buf.len() % 4 != 0 {
            buf.push(0);
        }
        let rel = (vec_start - device_id_offset_pos) as u32;
        buf[device_id_offset_pos..device_id_offset_pos + 4].copy_from_slice(&rel.to_le_bytes());
    }

    // session_id
    if let Some(ref session_id) = event.session_id {
        let vec_start = buf.len();
        write_u32(buf, 16);
        buf.extend_from_slice(session_id);
        while buf.len() % 4 != 0 {
            buf.push(0);
        }
        let rel = (vec_start - session_id_offset_pos) as u32;
        buf[session_id_offset_pos..session_id_offset_pos + 4].copy_from_slice(&rel.to_le_bytes());
    }

    // event_name
    if let Some(ref name) = event.event_name {
        let vec_start = buf.len();
        write_u32(buf, name.len() as u32);
        buf.extend_from_slice(name.as_bytes());
        buf.push(0); // null terminator
        while buf.len() % 4 != 0 {
            buf.push(0);
        }
        let rel = (vec_start - event_name_offset_pos) as u32;
        buf[event_name_offset_pos..event_name_offset_pos + 4].copy_from_slice(&rel.to_le_bytes());
    }

    // payload
    if !event.payload.is_empty() {
        let vec_start = buf.len();
        write_u32(buf, event.payload.len() as u32);
        buf.extend_from_slice(&event.payload);
        while buf.len() % 4 != 0 {
            buf.push(0);
        }
        let rel = (vec_start - payload_offset_pos) as u32;
        buf[payload_offset_pos..payload_offset_pos + 4].copy_from_slice(&rel.to_le_bytes());
    }

    table_start
}
