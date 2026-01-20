//! Tests for FlatBuffer parsing module
//!
//! The test helper `create_test_batch` builds valid FlatBuffer wire format
//! messages that match the schema in common.fbs.

use crate::error::ProtocolError;
use crate::flatbuf::{FlatBatch, read_u16, read_u32, read_u64};
use crate::schema::SchemaType;

/// Helper to create a valid FlatBuffer Batch message.
///
/// FlatBuffer wire format (simplified for our Batch table):
/// - Root: 4-byte offset pointing to the table
/// - Table: starts with signed offset to vtable, then inline data and offsets to vectors
/// - VTable: [vtable_size:u16][table_size:u16][field_offsets:u16...]
/// - Vectors: [length:u32][data...]
///
/// Our Batch table fields (by ID):
/// 0: api_key (vector of ubyte, required)
/// 1: schema_type (uint8)
/// 2: version (uint8)
/// 3: batch_id (uint64)
/// 4: data (vector of ubyte, required)
/// 5: source_ip (vector of ubyte, optional)
fn create_test_batch(api_key: &[u8; 16], schema_type: u8, data: &[u8]) -> Vec<u8> {
    // We'll build the buffer from back to front (as FlatBuffers does),
    // but for simplicity we'll calculate offsets and build forward.
    //
    // Layout we'll create:
    // [0..4]: root offset (points to table)
    // [4..vtable_end]: vtable
    // [vtable_end..table_end]: table
    // [table_end..]: vectors (api_key, data)

    let mut buf = Vec::with_capacity(128 + api_key.len() + data.len());

    // === Build VTable ===
    // VTable has: vtable_size (u16), table_size (u16), then field offsets (u16 each)
    // We have 6 fields (0-5), so 6 field slots
    let vtable_size: u16 = 4 + 6 * 2; // 16 bytes
    let table_inline_size: u16 = 4 + 4 + 1 + 1 + 2 + 8 + 4 + 4; // soffset + fields with padding

    // Reserve space for root offset
    buf.extend_from_slice(&[0u8; 4]);

    // VTable starts at offset 4
    let vtable_start = buf.len();
    buf.extend_from_slice(&vtable_size.to_le_bytes());
    buf.extend_from_slice(&table_inline_size.to_le_bytes());

    // Field offsets (from table start):
    // Field 0 (api_key): offset to the u32 that points to vector
    // Field 1 (schema_type): offset to inline u8
    // Field 2 (version): offset to inline u8
    // Field 3 (batch_id): offset to inline u64
    // Field 4 (data): offset to the u32 that points to vector
    // Field 5 (source_ip): 0 (not present)

    // Table layout (after soffset):
    // +4: api_key offset (u32) -> 4 bytes
    // +8: schema_type (u8) -> 1 byte
    // +9: version (u8) -> 1 byte
    // +10: padding -> 2 bytes (align to 4)
    // +12: batch_id (u64) -> 8 bytes (but we need align to 8, so at +16)
    // Actually let's simplify and use a working layout:
    //
    // +4: api_key vector offset (u32)
    // +8: data vector offset (u32)
    // +12: schema_type (u8)
    // +13: version (u8)
    // +14: padding (2 bytes)
    // +16: batch_id (u64) - optional, we'll set to 0

    let field_api_key_offset: u16 = 4;
    let field_schema_type_offset: u16 = 12;
    let field_version_offset: u16 = 13;
    let field_batch_id_offset: u16 = 0; // not present (or 16 if we want it)
    let field_data_offset: u16 = 8;
    let field_source_ip_offset: u16 = 0; // not present

    buf.extend_from_slice(&field_api_key_offset.to_le_bytes());
    buf.extend_from_slice(&field_schema_type_offset.to_le_bytes());
    buf.extend_from_slice(&field_version_offset.to_le_bytes());
    buf.extend_from_slice(&field_batch_id_offset.to_le_bytes());
    buf.extend_from_slice(&field_data_offset.to_le_bytes());
    buf.extend_from_slice(&field_source_ip_offset.to_le_bytes());

    let vtable_end = buf.len();

    // === Build Table ===
    let table_start = vtable_end;

    // soffset: distance from table to vtable (standard FlatBuffers: vtable = table - soffset)
    let soffset: i32 = (table_start - vtable_start) as i32;
    buf.extend_from_slice(&soffset.to_le_bytes());

    // Placeholder for api_key vector offset (will fill in later)
    let api_key_offset_pos = buf.len();
    buf.extend_from_slice(&[0u8; 4]);

    // Placeholder for data vector offset (will fill in later)
    let data_offset_pos = buf.len();
    buf.extend_from_slice(&[0u8; 4]);

    // schema_type (u8)
    buf.push(schema_type);

    // version (u8)
    buf.push(100); // v1.0

    // Padding to align (2 bytes)
    buf.extend_from_slice(&[0u8; 2]);

    // Align to 4 bytes for vectors
    while buf.len() % 4 != 0 {
        buf.push(0);
    }

    // === Build Vectors ===

    // API key vector
    let api_key_vec_start = buf.len();
    buf.extend_from_slice(&(api_key.len() as u32).to_le_bytes());
    buf.extend_from_slice(api_key);

    // Align to 4 bytes
    while buf.len() % 4 != 0 {
        buf.push(0);
    }

    // Data vector
    let data_vec_start = buf.len();
    buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
    buf.extend_from_slice(data);

    // === Fill in offsets ===

    // api_key offset: relative offset from the field position to the vector
    let api_key_rel = (api_key_vec_start - api_key_offset_pos) as u32;
    buf[api_key_offset_pos..api_key_offset_pos + 4].copy_from_slice(&api_key_rel.to_le_bytes());

    // data offset: relative offset from the field position to the vector
    let data_rel = (data_vec_start - data_offset_pos) as u32;
    buf[data_offset_pos..data_offset_pos + 4].copy_from_slice(&data_rel.to_le_bytes());

    // Root offset: points to table start
    let root_offset = table_start as u32;
    buf[0..4].copy_from_slice(&root_offset.to_le_bytes());

    buf
}

// =============================================================================
// FlatBatch::parse tests
// =============================================================================

#[test]
fn test_parse_valid_batch() {
    let api_key = [1u8; 16];
    let data = b"test payload data";
    let buf = create_test_batch(&api_key, 1, data);

    let batch = FlatBatch::parse(&buf).expect("should parse valid batch");

    assert_eq!(batch.api_key().unwrap(), &api_key);
    assert_eq!(batch.schema_type(), SchemaType::Event);
    assert_eq!(batch.version(), 100);
    assert_eq!(batch.data().unwrap(), data);
    assert!(!batch.has_source_ip());
}

#[test]
fn test_parse_too_short_minimal() {
    let buf = [0u8; 4];
    let result = FlatBatch::parse(&buf);
    assert!(matches!(result, Err(ProtocolError::MessageTooShort { .. })));
}

#[test]
fn test_parse_too_short_empty() {
    let buf: [u8; 0] = [];
    let result = FlatBatch::parse(&buf);
    assert!(matches!(result, Err(ProtocolError::MessageTooShort { .. })));
}

#[test]
fn test_parse_invalid_root_offset_past_buffer() {
    let mut buf = vec![0u8; 100];
    buf[0..4].copy_from_slice(&1000u32.to_le_bytes());

    let result = FlatBatch::parse(&buf);
    assert!(matches!(result, Err(ProtocolError::InvalidFlatBuffer(_))));
}

#[test]
fn test_parse_invalid_root_offset_at_end() {
    let mut buf = vec![0u8; 100];
    buf[0..4].copy_from_slice(&99u32.to_le_bytes());

    let result = FlatBatch::parse(&buf);
    assert!(matches!(result, Err(ProtocolError::InvalidFlatBuffer(_))));
}

// =============================================================================
// Schema type tests
// =============================================================================

#[test]
fn test_schema_type_event() {
    let api_key = [0u8; 16];
    let buf = create_test_batch(&api_key, 1, b"x");
    let batch = FlatBatch::parse(&buf).unwrap();
    assert_eq!(batch.schema_type(), SchemaType::Event);
}

#[test]
fn test_schema_type_log() {
    let api_key = [0u8; 16];
    let buf = create_test_batch(&api_key, 2, b"x");
    let batch = FlatBatch::parse(&buf).unwrap();
    assert_eq!(batch.schema_type(), SchemaType::Log);
}

#[test]
fn test_schema_type_metric() {
    let api_key = [0u8; 16];
    let buf = create_test_batch(&api_key, 3, b"x");
    let batch = FlatBatch::parse(&buf).unwrap();
    assert_eq!(batch.schema_type(), SchemaType::Metric);
}

#[test]
fn test_schema_type_trace() {
    let api_key = [0u8; 16];
    let buf = create_test_batch(&api_key, 4, b"x");
    let batch = FlatBatch::parse(&buf).unwrap();
    assert_eq!(batch.schema_type(), SchemaType::Trace);
}

#[test]
fn test_schema_type_snapshot() {
    let api_key = [0u8; 16];
    let buf = create_test_batch(&api_key, 5, b"x");
    let batch = FlatBatch::parse(&buf).unwrap();
    assert_eq!(batch.schema_type(), SchemaType::Snapshot);
}

#[test]
fn test_schema_type_unknown_zero() {
    let api_key = [0u8; 16];
    let buf = create_test_batch(&api_key, 0, b"x");
    let batch = FlatBatch::parse(&buf).unwrap();
    assert_eq!(batch.schema_type(), SchemaType::Unknown);
}

#[test]
fn test_schema_type_unknown_invalid() {
    let api_key = [0u8; 16];
    let buf = create_test_batch(&api_key, 255, b"x");
    let batch = FlatBatch::parse(&buf).unwrap();
    assert_eq!(batch.schema_type(), SchemaType::Unknown);
}

// =============================================================================
// API key tests
// =============================================================================

#[test]
fn test_api_key_all_zeros() {
    let api_key = [0u8; 16];
    let buf = create_test_batch(&api_key, 1, b"data");
    let batch = FlatBatch::parse(&buf).unwrap();
    assert_eq!(batch.api_key().unwrap(), &api_key);
}

#[test]
fn test_api_key_all_ones() {
    let api_key = [0xFFu8; 16];
    let buf = create_test_batch(&api_key, 1, b"data");
    let batch = FlatBatch::parse(&buf).unwrap();
    assert_eq!(batch.api_key().unwrap(), &api_key);
}

#[test]
fn test_api_key_sequential() {
    let api_key: [u8; 16] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
    let buf = create_test_batch(&api_key, 1, b"data");
    let batch = FlatBatch::parse(&buf).unwrap();
    assert_eq!(batch.api_key().unwrap(), &api_key);
}

#[test]
fn test_api_key_bytes() {
    let api_key = [42u8; 16];
    let buf = create_test_batch(&api_key, 1, b"data");
    let batch = FlatBatch::parse(&buf).unwrap();
    let bytes = batch.api_key_bytes().unwrap();
    assert_eq!(bytes.len(), 16);
    assert_eq!(bytes, &api_key[..]);
}

// =============================================================================
// Data payload tests
// =============================================================================

#[test]
fn test_data_empty() {
    let api_key = [0u8; 16];
    let buf = create_test_batch(&api_key, 1, b"");
    let batch = FlatBatch::parse(&buf).unwrap();
    assert_eq!(batch.data().unwrap(), b"");
}

#[test]
fn test_data_small() {
    let api_key = [0u8; 16];
    let data = b"small";
    let buf = create_test_batch(&api_key, 1, data);
    let batch = FlatBatch::parse(&buf).unwrap();
    assert_eq!(batch.data().unwrap(), data);
}

#[test]
fn test_data_large() {
    let api_key = [0u8; 16];
    let data = vec![0xABu8; 10000];
    let buf = create_test_batch(&api_key, 1, &data);
    let batch = FlatBatch::parse(&buf).unwrap();
    assert_eq!(batch.data().unwrap(), &data[..]);
}

#[test]
fn test_data_binary() {
    let api_key = [0u8; 16];
    let data: Vec<u8> = (0u8..=255).collect();
    let buf = create_test_batch(&api_key, 1, &data);
    let batch = FlatBatch::parse(&buf).unwrap();
    assert_eq!(batch.data().unwrap(), &data[..]);
}

// =============================================================================
// Version tests
// =============================================================================

#[test]
fn test_version_value() {
    let api_key = [0u8; 16];
    let buf = create_test_batch(&api_key, 1, b"data");
    let batch = FlatBatch::parse(&buf).unwrap();
    assert_eq!(batch.version(), 100); // v1.0
}

// =============================================================================
// Batch ID tests
// =============================================================================

#[test]
fn test_batch_id_not_present() {
    let api_key = [0u8; 16];
    let buf = create_test_batch(&api_key, 1, b"data");
    let batch = FlatBatch::parse(&buf).unwrap();
    assert_eq!(batch.batch_id(), 0); // default when not present
}

// =============================================================================
// Source IP tests
// =============================================================================

#[test]
fn test_source_ip_not_present() {
    let api_key = [0u8; 16];
    let buf = create_test_batch(&api_key, 1, b"data");
    let batch = FlatBatch::parse(&buf).unwrap();
    assert!(!batch.has_source_ip());
    assert!(batch.source_ip().unwrap().is_none());
    assert!(batch.source_ip_bytes().unwrap().is_none());
}

// =============================================================================
// raw_bytes tests
// =============================================================================

#[test]
fn test_raw_bytes_returns_original_buffer() {
    let api_key = [0u8; 16];
    let buf = create_test_batch(&api_key, 1, b"data");
    let batch = FlatBatch::parse(&buf).unwrap();
    assert_eq!(batch.raw_bytes(), &buf[..]);
}

// =============================================================================
// Read helper tests
// =============================================================================

#[test]
fn test_read_u16_valid() {
    let buf = [0x01, 0x02, 0x03, 0x04];
    assert_eq!(read_u16(&buf, 0).unwrap(), 0x0201);
    assert_eq!(read_u16(&buf, 1).unwrap(), 0x0302);
    assert_eq!(read_u16(&buf, 2).unwrap(), 0x0403);
}

#[test]
fn test_read_u16_out_of_bounds() {
    let buf = [0x01, 0x02, 0x03, 0x04];
    assert!(read_u16(&buf, 3).is_err());
    assert!(read_u16(&buf, 4).is_err());
    assert!(read_u16(&buf, 100).is_err());
}

#[test]
fn test_read_u32_valid() {
    let buf = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08];
    assert_eq!(read_u32(&buf, 0).unwrap(), 0x04030201);
    assert_eq!(read_u32(&buf, 4).unwrap(), 0x08070605);
}

#[test]
fn test_read_u32_out_of_bounds() {
    let buf = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08];
    assert!(read_u32(&buf, 5).is_err());
    assert!(read_u32(&buf, 8).is_err());
}

#[test]
fn test_read_u64_valid() {
    let buf = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08];
    assert_eq!(read_u64(&buf, 0).unwrap(), 0x0807060504030201);
}

#[test]
fn test_read_u64_out_of_bounds() {
    let buf = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08];
    assert!(read_u64(&buf, 1).is_err());
    assert!(read_u64(&buf, 8).is_err());
}

#[test]
fn test_read_u64_empty_buffer() {
    let buf: [u8; 0] = [];
    assert!(read_u64(&buf, 0).is_err());
}

// =============================================================================
// Crash/malicious data tests (ported from Go crash_test_client.go)
// These validate the 5-stage SafeGetRootAsBatch equivalent validation
// =============================================================================

use crate::MAX_REASONABLE_SIZE;
use crate::flatbuf::is_likely_flatbuffer;

// Stage 1: Size validation tests
#[test]
fn test_crash_empty_buffer() {
    let buf: [u8; 0] = [];
    assert!(!is_likely_flatbuffer(&buf));
    assert!(FlatBatch::parse(&buf).is_err());
}

#[test]
fn test_crash_tiny_buffers() {
    for size in 1..16 {
        let buf = vec![0xAA; size];
        assert!(!is_likely_flatbuffer(&buf));
        assert!(FlatBatch::parse(&buf).is_err());
    }
}

#[test]
fn test_crash_max_reasonable_size_exceeded() {
    // We can't actually allocate 100MB+ in tests, so we verify the constant
    // and that the check exists in parse()
    assert_eq!(MAX_REASONABLE_SIZE, 100 * 1024 * 1024);

    // Test with a buffer that claims to be larger than reasonable
    // (the TCP layer would reject this, but parse() also checks)
    // This is really just testing that the constant is correct
}

// Stage 2: Root offset validation tests
#[test]
fn test_crash_massive_root_offset() {
    // Root offset 0xFFFFFFFF would cause segfault in unsafe code
    let mut buf = vec![0u8; 32];
    buf[0..4].copy_from_slice(&0xFFFFFFFFu32.to_le_bytes());
    assert!(!is_likely_flatbuffer(&buf));
    assert!(FlatBatch::parse(&buf).is_err());
}

#[test]
fn test_crash_root_offset_beyond_buffer() {
    // offset=100, buffer=32
    let mut buf = vec![0x41u8; 32];
    buf[0..4].copy_from_slice(&100u32.to_le_bytes());
    assert!(!is_likely_flatbuffer(&buf));
    assert!(FlatBatch::parse(&buf).is_err());
}

#[test]
fn test_crash_zero_root_offset() {
    // Zero offset is invalid (points before actual table data)
    let mut buf = vec![0x42u8; 32];
    buf[0..4].copy_from_slice(&0u32.to_le_bytes());
    assert!(!is_likely_flatbuffer(&buf));
    assert!(FlatBatch::parse(&buf).is_err());
}

#[test]
fn test_crash_root_offset_one() {
    let mut buf = vec![0u8; 32];
    buf[0..4].copy_from_slice(&1u32.to_le_bytes());
    assert!(!is_likely_flatbuffer(&buf));
}

#[test]
fn test_crash_root_offset_two() {
    let mut buf = vec![0u8; 32];
    buf[0..4].copy_from_slice(&2u32.to_le_bytes());
    assert!(!is_likely_flatbuffer(&buf));
}

#[test]
fn test_crash_root_offset_three() {
    let mut buf = vec![0u8; 32];
    buf[0..4].copy_from_slice(&3u32.to_le_bytes());
    assert!(!is_likely_flatbuffer(&buf));
}

// Stage 3: VTable corruption tests
#[test]
fn test_crash_vtable_beyond_buffer() {
    // Valid root offset but vtable position would be out of bounds
    let mut buf = vec![0u8; 30];
    buf[0..4].copy_from_slice(&20u32.to_le_bytes()); // Root at 20
    // VTable soffset at position 20 - would point beyond buffer
    buf[20..24].copy_from_slice(&100i32.to_le_bytes());
    assert!(FlatBatch::parse(&buf).is_err());
}

#[test]
fn test_crash_invalid_vtable_structure() {
    let mut buf = vec![0u8; 100];
    buf[0..4].copy_from_slice(&50u32.to_le_bytes()); // Root at 50
    // Set a reasonable soffset
    buf[50..54].copy_from_slice(&10i32.to_le_bytes()); // vtable at 40
    // Corrupt vtable data
    buf[40] = 0xFF;
    buf[41] = 0xFF;
    // This should either error or handle gracefully
    let _ = FlatBatch::parse(&buf);
}

// JSON/XML garbage rejection tests (is_likely_flatbuffer heuristic)
#[test]
fn test_crash_json_object() {
    let json = br#"{"message": "hello world"}"#;
    assert!(!is_likely_flatbuffer(json));
}

#[test]
fn test_crash_json_array() {
    let json = br#"[1, 2, 3, 4, 5]"#;
    assert!(!is_likely_flatbuffer(json));
}

#[test]
fn test_crash_json_api_key() {
    let json = br#"{"api_key": "test", "data": [1,2,3,4,5]}"#;
    assert!(!is_likely_flatbuffer(json));
}

#[test]
fn test_crash_json_events() {
    let json = br#"{"events": [{"user_id": "123", "event": "click"}]}"#;
    assert!(!is_likely_flatbuffer(json));
}

#[test]
fn test_crash_xml_data() {
    let xml = br#"<root><message>hello</message></root>"#;
    assert!(!is_likely_flatbuffer(xml));
}

#[test]
fn test_crash_xml_events() {
    let xml = br#"<events><event id="123"/></events>"#;
    assert!(!is_likely_flatbuffer(xml));
}

// Random garbage tests
#[test]
fn test_crash_random_garbage_patterns() {
    let deadbeef: [u8; 48] = [
        0xDE, 0xAD, 0xBE, 0xEF, 0xDE, 0xAD, 0xBE, 0xEF, 0xDE, 0xAD, 0xBE, 0xEF, 0xDE, 0xAD, 0xBE,
        0xEF, 0xDE, 0xAD, 0xBE, 0xEF, 0xDE, 0xAD, 0xBE, 0xEF, 0xDE, 0xAD, 0xBE, 0xEF, 0xDE, 0xAD,
        0xBE, 0xEF, 0xDE, 0xAD, 0xBE, 0xEF, 0xDE, 0xAD, 0xBE, 0xEF, 0xDE, 0xAD, 0xBE, 0xEF, 0xDE,
        0xAD, 0xBE, 0xEF,
    ];
    let patterns: &[&[u8]] = &[
        &[0xFF; 50],
        &[0x00; 50],
        &deadbeef,
        b"GET / HTTP/1.1\r\nHost: example.com\r\n\r\n",
        b"CONNECT localhost:443 HTTP/1.1\r\n",
        b"\x00\x00\x00\x00AAAA",
    ];

    for pattern in patterns {
        // Should not panic, should return error or false
        let likely = is_likely_flatbuffer(pattern);
        if likely {
            // If heuristic passes, parse should handle gracefully
            let _ = FlatBatch::parse(pattern);
        }
    }
}

#[test]
fn test_crash_binary_garbage() {
    // Various binary patterns that might cause issues
    for i in 0..50 {
        let mut garbage = vec![0u8; 50 + i * 10];
        for (j, byte) in garbage.iter_mut().enumerate() {
            *byte = ((i + j) % 256) as u8;
        }
        // Should handle gracefully - no panics
        let _ = is_likely_flatbuffer(&garbage);
        let _ = FlatBatch::parse(&garbage);
    }
}

// Edge case tests
#[test]
fn test_crash_minimum_valid_size() {
    // Exactly 16 bytes - minimum valid size
    let buf = vec![0u8; 16];
    // This is still invalid due to content, but shouldn't crash
    let _ = FlatBatch::parse(&buf);
}

#[test]
fn test_crash_valid_root_invalid_vtable_size() {
    let mut buf = vec![0u8; 50];
    buf[0..4].copy_from_slice(&20u32.to_le_bytes()); // Root at 20
    buf[20..24].copy_from_slice(&10i32.to_le_bytes()); // vtable at 10
    // Invalid vtable size (0)
    buf[10..12].copy_from_slice(&0u16.to_le_bytes());
    assert!(FlatBatch::parse(&buf).is_err());
}

#[test]
fn test_crash_vtable_size_too_large() {
    let mut buf = vec![0u8; 50];
    buf[0..4].copy_from_slice(&20u32.to_le_bytes()); // Root at 20
    buf[20..24].copy_from_slice(&10i32.to_le_bytes()); // vtable at 10
    // Huge vtable size
    buf[10..12].copy_from_slice(&0xFFFFu16.to_le_bytes());
    assert!(FlatBatch::parse(&buf).is_err());
}
