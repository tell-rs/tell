//! Tests for the decoder module

#![allow(dead_code)] // Test helpers for future FlatBuffer decoding tests

use super::*;

// =============================================================================
// Test Helpers - Build valid FlatBuffer messages
// =============================================================================

/// Build a valid EventData FlatBuffer with the given events
fn build_event_data(events: &[TestEvent]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(512);

    // We build from a known working layout
    // Root offset placeholder
    buf.extend_from_slice(&[0u8; 4]);

    // VTable for EventData: vtable_size(2) + table_size(2) + 1 field(2) = 6 bytes
    let vtable_start = buf.len();
    buf.extend_from_slice(&6u16.to_le_bytes()); // vtable_size
    buf.extend_from_slice(&8u16.to_le_bytes()); // table_size (soffset + events offset)
    buf.extend_from_slice(&4u16.to_le_bytes()); // field 0: events at offset 4

    // Table for EventData
    let table_start = buf.len();
    // FlatBuffers spec: soffset = table_start - vtable_start (positive)
    let soffset: i32 = (table_start - vtable_start) as i32;
    buf.extend_from_slice(&soffset.to_le_bytes());

    // Placeholder for events vector offset
    let events_offset_pos = buf.len();
    buf.extend_from_slice(&[0u8; 4]);

    // Align to 4 bytes
    while !buf.len().is_multiple_of(4) {
        buf.push(0);
    }

    // Build event tables and collect their offsets
    let mut event_table_offsets = Vec::with_capacity(events.len());

    for event in events {
        // Align
        while !buf.len().is_multiple_of(4) {
            buf.push(0);
        }

        let event_offset = build_event_table(&mut buf, event);
        event_table_offsets.push(event_offset);
    }

    // Align
    while !buf.len().is_multiple_of(4) {
        buf.push(0);
    }

    // Build events vector (vector of offsets to tables)
    let events_vector_start = buf.len();
    buf.extend_from_slice(&(events.len() as u32).to_le_bytes());

    // Write offsets - each is relative to its position
    for &table_offset in event_table_offsets.iter() {
        let offset_pos = buf.len();
        let rel_offset = (table_offset as i32 - offset_pos as i32) as u32;
        buf.extend_from_slice(&rel_offset.to_le_bytes());
    }

    // Fill in events vector offset
    let events_rel = (events_vector_start - events_offset_pos) as u32;
    buf[events_offset_pos..events_offset_pos + 4].copy_from_slice(&events_rel.to_le_bytes());

    // Fill in root offset
    buf[0..4].copy_from_slice(&(table_start as u32).to_le_bytes());

    buf
}

/// Test event data structure
struct TestEvent {
    event_type: u8,
    timestamp: u64,
    device_id: Option<[u8; 16]>,
    session_id: Option<[u8; 16]>,
    event_name: Option<String>,
    payload: Vec<u8>,
}

impl Default for TestEvent {
    fn default() -> Self {
        Self {
            event_type: 1, // Track
            timestamp: 1234567890,
            device_id: None,
            session_id: None,
            event_name: None,
            payload: Vec::new(),
        }
    }
}

/// Build a single Event table, returns offset to table start
fn build_event_table(buf: &mut Vec<u8>, event: &TestEvent) -> usize {
    // First, build all vectors we need
    let device_id_offset = event.device_id.map(|id| {
        while !buf.len().is_multiple_of(4) {
            buf.push(0);
        }
        let start = buf.len();
        buf.extend_from_slice(&16u32.to_le_bytes());
        buf.extend_from_slice(&id);
        start
    });

    let session_id_offset = event.session_id.map(|id| {
        while !buf.len().is_multiple_of(4) {
            buf.push(0);
        }
        let start = buf.len();
        buf.extend_from_slice(&16u32.to_le_bytes());
        buf.extend_from_slice(&id);
        start
    });

    let event_name_offset = event.event_name.as_ref().map(|name| {
        while !buf.len().is_multiple_of(4) {
            buf.push(0);
        }
        let start = buf.len();
        buf.extend_from_slice(&(name.len() as u32).to_le_bytes());
        buf.extend_from_slice(name.as_bytes());
        buf.push(0); // null terminator
        start
    });

    let payload_offset = if !event.payload.is_empty() {
        while !buf.len().is_multiple_of(4) {
            buf.push(0);
        }
        let start = buf.len();
        buf.extend_from_slice(&(event.payload.len() as u32).to_le_bytes());
        buf.extend_from_slice(&event.payload);
        Some(start)
    } else {
        None
    };

    // Align before vtable
    while !buf.len().is_multiple_of(4) {
        buf.push(0);
    }

    // VTable for Event: 6 fields
    // vtable_size(2) + table_size(2) + 6 fields(12) = 16 bytes
    let vtable_start = buf.len();
    buf.extend_from_slice(&16u16.to_le_bytes()); // vtable_size
    buf.extend_from_slice(&32u16.to_le_bytes()); // table_size

    // Field offsets (from table start after soffset)
    buf.extend_from_slice(&4u16.to_le_bytes()); // field 0: event_type at +4
    buf.extend_from_slice(&8u16.to_le_bytes()); // field 1: timestamp at +8
    buf.extend_from_slice(
        &(if device_id_offset.is_some() {
            16u16
        } else {
            0u16
        })
        .to_le_bytes(),
    ); // field 2: device_id
    buf.extend_from_slice(
        &(if session_id_offset.is_some() {
            20u16
        } else {
            0u16
        })
        .to_le_bytes(),
    ); // field 3: session_id
    buf.extend_from_slice(
        &(if event_name_offset.is_some() {
            24u16
        } else {
            0u16
        })
        .to_le_bytes(),
    ); // field 4: event_name
    buf.extend_from_slice(
        &(if payload_offset.is_some() {
            28u16
        } else {
            0u16
        })
        .to_le_bytes(),
    ); // field 5: payload

    // Table
    let table_start = buf.len();
    // FlatBuffers spec: soffset = table_start - vtable_start (positive)
    let soffset: i32 = (table_start - vtable_start) as i32;
    buf.extend_from_slice(&soffset.to_le_bytes()); // soffset

    // Field 0: event_type (u8) + padding
    buf.push(event.event_type);
    buf.extend_from_slice(&[0u8; 3]); // padding to align timestamp

    // Field 1: timestamp (u64)
    buf.extend_from_slice(&event.timestamp.to_le_bytes());

    // Field 2: device_id offset
    let device_id_offset_pos = buf.len();
    buf.extend_from_slice(&[0u8; 4]);

    // Field 3: session_id offset
    let session_id_offset_pos = buf.len();
    buf.extend_from_slice(&[0u8; 4]);

    // Field 4: event_name offset
    let event_name_offset_pos = buf.len();
    buf.extend_from_slice(&[0u8; 4]);

    // Field 5: payload offset
    let payload_offset_pos = buf.len();
    buf.extend_from_slice(&[0u8; 4]);

    // Fill in vector offsets
    if let Some(offset) = device_id_offset {
        let rel = (offset as i32 - device_id_offset_pos as i32) as u32;
        buf[device_id_offset_pos..device_id_offset_pos + 4].copy_from_slice(&rel.to_le_bytes());
    }

    if let Some(offset) = session_id_offset {
        let rel = (offset as i32 - session_id_offset_pos as i32) as u32;
        buf[session_id_offset_pos..session_id_offset_pos + 4].copy_from_slice(&rel.to_le_bytes());
    }

    if let Some(offset) = event_name_offset {
        let rel = (offset as i32 - event_name_offset_pos as i32) as u32;
        buf[event_name_offset_pos..event_name_offset_pos + 4].copy_from_slice(&rel.to_le_bytes());
    }

    if let Some(offset) = payload_offset {
        let rel = (offset as i32 - payload_offset_pos as i32) as u32;
        buf[payload_offset_pos..payload_offset_pos + 4].copy_from_slice(&rel.to_le_bytes());
    }

    table_start
}

// =============================================================================
// EventType Tests
// =============================================================================

#[test]
fn test_event_type_from_u8() {
    assert_eq!(EventType::from_u8(0), EventType::Unknown);
    assert_eq!(EventType::from_u8(1), EventType::Track);
    assert_eq!(EventType::from_u8(2), EventType::Identify);
    assert_eq!(EventType::from_u8(3), EventType::Group);
    assert_eq!(EventType::from_u8(4), EventType::Alias);
    assert_eq!(EventType::from_u8(5), EventType::Enrich);
    assert_eq!(EventType::from_u8(6), EventType::Context);
    assert_eq!(EventType::from_u8(255), EventType::Unknown);
}

#[test]
fn test_event_type_as_str() {
    assert_eq!(EventType::Unknown.as_str(), "unknown");
    assert_eq!(EventType::Track.as_str(), "track");
    assert_eq!(EventType::Identify.as_str(), "identify");
    assert_eq!(EventType::Group.as_str(), "group");
    assert_eq!(EventType::Alias.as_str(), "alias");
    assert_eq!(EventType::Enrich.as_str(), "enrich");
    assert_eq!(EventType::Context.as_str(), "context");
}

#[test]
fn test_event_type_display() {
    assert_eq!(format!("{}", EventType::Track), "track");
    assert_eq!(format!("{}", EventType::Identify), "identify");
}

// =============================================================================
// LogEventType Tests
// =============================================================================

#[test]
fn test_log_event_type_from_u8() {
    assert_eq!(LogEventType::from_u8(0), LogEventType::Unknown);
    assert_eq!(LogEventType::from_u8(1), LogEventType::Log);
    assert_eq!(LogEventType::from_u8(2), LogEventType::Enrich);
    assert_eq!(LogEventType::from_u8(255), LogEventType::Unknown);
}

#[test]
fn test_log_event_type_as_str() {
    assert_eq!(LogEventType::Unknown.as_str(), "unknown");
    assert_eq!(LogEventType::Log.as_str(), "log");
    assert_eq!(LogEventType::Enrich.as_str(), "enrich");
}

// =============================================================================
// LogLevel Tests
// =============================================================================

#[test]
fn test_log_level_from_u8() {
    // LogLevel values based on actual wire format
    assert_eq!(LogLevel::from_u8(0), LogLevel::Trace);
    assert_eq!(LogLevel::from_u8(1), LogLevel::Debug);
    assert_eq!(LogLevel::from_u8(2), LogLevel::Fatal);
    assert_eq!(LogLevel::from_u8(3), LogLevel::Error);
    assert_eq!(LogLevel::from_u8(4), LogLevel::Warning);
    assert_eq!(LogLevel::from_u8(6), LogLevel::Info);
    assert_eq!(LogLevel::from_u8(255), LogLevel::Info); // Default to Info
}

#[test]
fn test_log_level_as_str() {
    assert_eq!(LogLevel::Trace.as_str(), "trace");
    assert_eq!(LogLevel::Debug.as_str(), "debug");
    assert_eq!(LogLevel::Info.as_str(), "info");
    assert_eq!(LogLevel::Warning.as_str(), "warning");
    assert_eq!(LogLevel::Error.as_str(), "error");
    assert_eq!(LogLevel::Fatal.as_str(), "fatal");
}

#[test]
fn test_log_level_is_error() {
    assert!(!LogLevel::Trace.is_error());
    assert!(!LogLevel::Debug.is_error());
    assert!(!LogLevel::Info.is_error());
    assert!(!LogLevel::Warning.is_error());
    assert!(LogLevel::Error.is_error());
    assert!(LogLevel::Fatal.is_error());
}

#[test]
fn test_log_level_display() {
    assert_eq!(format!("{}", LogLevel::Error), "error");
    assert_eq!(format!("{}", LogLevel::Info), "info");
}

// =============================================================================
// DecodedData Tests
// =============================================================================

#[test]
fn test_decoded_data_events() {
    let data = DecodedData::Events(vec![]);
    assert!(data.as_events().is_some());
    assert!(data.as_logs().is_none());
    assert!(data.is_empty());
    assert_eq!(data.len(), 0);
}

#[test]
fn test_decoded_data_logs() {
    let data = DecodedData::Logs(vec![]);
    assert!(data.as_events().is_none());
    assert!(data.as_logs().is_some());
    assert!(data.is_empty());
    assert_eq!(data.len(), 0);
}

// =============================================================================
// decode_event_data Tests
// =============================================================================

#[test]
fn test_decode_event_data_empty_buffer() {
    let result = decode_event_data(&[]);
    assert!(result.is_err());
}

#[test]
fn test_decode_event_data_too_short() {
    let result = decode_event_data(&[0, 0, 0]);
    assert!(result.is_err());
}

// =============================================================================
// decode_log_data Tests
// =============================================================================

#[test]
fn test_decode_log_data_empty_buffer() {
    let result = decode_log_data(&[]);
    assert!(result.is_err());
}

#[test]
fn test_decode_log_data_too_short() {
    let result = decode_log_data(&[0, 0, 0]);
    assert!(result.is_err());
}
