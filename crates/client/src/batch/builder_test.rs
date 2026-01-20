//! Tests for BatchBuilder
//!
//! These tests verify that BatchBuilder produces valid FlatBuffer wire format
//! that can be parsed by the FlatBatch parser in tell-protocol.

use tell_protocol::{FlatBatch, SchemaType};

use crate::{BatchBuilder, BuilderError};

// =============================================================================
// Basic building tests
// =============================================================================

#[test]
fn test_build_minimal_batch() {
    let api_key = [0x01u8; 16];
    let data = b"test data";

    let batch = BatchBuilder::new()
        .api_key(api_key)
        .data(data)
        .build()
        .expect("should build minimal batch");

    assert!(!batch.is_empty());
    assert!(batch.len() > 0);
}

#[test]
fn test_build_full_batch() {
    let api_key = [0x42u8; 16];
    let data = b"full batch data";
    let source_ip = [0xABu8; 16];

    let batch = BatchBuilder::new()
        .api_key(api_key)
        .schema_type(SchemaType::Event)
        .version(101)
        .batch_id(12345)
        .data(data)
        .source_ip(source_ip)
        .build()
        .expect("should build full batch");

    assert!(!batch.is_empty());
}

#[test]
fn test_build_and_parse_roundtrip() {
    let api_key = [
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F,
        0x10,
    ];
    let data = b"roundtrip test payload";

    let built = BatchBuilder::new()
        .api_key(api_key)
        .schema_type(SchemaType::Log)
        .version(100)
        .data(data)
        .build()
        .expect("should build batch");

    // Parse the built batch
    let parsed = FlatBatch::parse(built.as_bytes()).expect("should parse built batch");

    assert_eq!(parsed.api_key().unwrap(), &api_key);
    assert_eq!(parsed.schema_type(), SchemaType::Log);
    assert_eq!(parsed.version(), 100);
    assert_eq!(parsed.data().unwrap(), data);
}

#[test]
fn test_build_and_parse_with_all_fields() {
    let api_key = [0xFFu8; 16];
    let data = b"complete payload with all fields";
    let source_ip = [
        0x20, 0x01, 0x0d, 0xb8, 0x85, 0xa3, 0x00, 0x00, 0x00, 0x00, 0x8a, 0x2e, 0x03, 0x70, 0x73,
        0x34,
    ];

    let built = BatchBuilder::new()
        .api_key(api_key)
        .schema_type(SchemaType::Trace)
        .version(200)
        .batch_id(9999999)
        .data(data)
        .source_ip(source_ip)
        .build()
        .expect("should build batch with all fields");

    let parsed = FlatBatch::parse(built.as_bytes()).expect("should parse batch with all fields");

    assert_eq!(parsed.api_key().unwrap(), &api_key);
    assert_eq!(parsed.schema_type(), SchemaType::Trace);
    assert_eq!(parsed.version(), 200);
    assert_eq!(parsed.batch_id(), 9999999);
    assert_eq!(parsed.data().unwrap(), data);
    assert!(parsed.has_source_ip());
    assert_eq!(parsed.source_ip().unwrap(), Some(&source_ip));
}

// =============================================================================
// Schema type tests
// =============================================================================

#[test]
fn test_schema_type_unknown() {
    let batch = BatchBuilder::new()
        .api_key([0u8; 16])
        .data(b"x")
        .build()
        .unwrap();

    let parsed = FlatBatch::parse(batch.as_bytes()).unwrap();
    assert_eq!(parsed.schema_type(), SchemaType::Unknown);
}

#[test]
fn test_schema_type_event() {
    let batch = BatchBuilder::new()
        .api_key([0u8; 16])
        .schema_type(SchemaType::Event)
        .data(b"x")
        .build()
        .unwrap();

    let parsed = FlatBatch::parse(batch.as_bytes()).unwrap();
    assert_eq!(parsed.schema_type(), SchemaType::Event);
}

#[test]
fn test_schema_type_log() {
    let batch = BatchBuilder::new()
        .api_key([0u8; 16])
        .schema_type(SchemaType::Log)
        .data(b"x")
        .build()
        .unwrap();

    let parsed = FlatBatch::parse(batch.as_bytes()).unwrap();
    assert_eq!(parsed.schema_type(), SchemaType::Log);
}

#[test]
fn test_schema_type_metric() {
    let batch = BatchBuilder::new()
        .api_key([0u8; 16])
        .schema_type(SchemaType::Metric)
        .data(b"x")
        .build()
        .unwrap();

    let parsed = FlatBatch::parse(batch.as_bytes()).unwrap();
    assert_eq!(parsed.schema_type(), SchemaType::Metric);
}

#[test]
fn test_schema_type_trace() {
    let batch = BatchBuilder::new()
        .api_key([0u8; 16])
        .schema_type(SchemaType::Trace)
        .data(b"x")
        .build()
        .unwrap();

    let parsed = FlatBatch::parse(batch.as_bytes()).unwrap();
    assert_eq!(parsed.schema_type(), SchemaType::Trace);
}

#[test]
fn test_schema_type_snapshot() {
    let batch = BatchBuilder::new()
        .api_key([0u8; 16])
        .schema_type(SchemaType::Snapshot)
        .data(b"x")
        .build()
        .unwrap();

    let parsed = FlatBatch::parse(batch.as_bytes()).unwrap();
    assert_eq!(parsed.schema_type(), SchemaType::Snapshot);
}

// =============================================================================
// API key tests
// =============================================================================

#[test]
fn test_api_key_all_zeros() {
    let api_key = [0u8; 16];
    let batch = BatchBuilder::new()
        .api_key(api_key)
        .data(b"data")
        .build()
        .unwrap();

    let parsed = FlatBatch::parse(batch.as_bytes()).unwrap();
    assert_eq!(parsed.api_key().unwrap(), &api_key);
}

#[test]
fn test_api_key_all_ones() {
    let api_key = [0xFFu8; 16];
    let batch = BatchBuilder::new()
        .api_key(api_key)
        .data(b"data")
        .build()
        .unwrap();

    let parsed = FlatBatch::parse(batch.as_bytes()).unwrap();
    assert_eq!(parsed.api_key().unwrap(), &api_key);
}

#[test]
fn test_api_key_sequential() {
    let api_key: [u8; 16] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
    let batch = BatchBuilder::new()
        .api_key(api_key)
        .data(b"data")
        .build()
        .unwrap();

    let parsed = FlatBatch::parse(batch.as_bytes()).unwrap();
    assert_eq!(parsed.api_key().unwrap(), &api_key);
}

#[test]
fn test_api_key_slice_valid() {
    let key_slice: &[u8] = &[0x42u8; 16];
    let batch = BatchBuilder::new()
        .api_key_slice(key_slice)
        .unwrap()
        .data(b"data")
        .build()
        .unwrap();

    let parsed = FlatBatch::parse(batch.as_bytes()).unwrap();
    assert_eq!(parsed.api_key_bytes().unwrap(), key_slice);
}

#[test]
fn test_api_key_slice_too_short() {
    let result = BatchBuilder::new().api_key_slice(&[0u8; 10]);
    assert!(matches!(result, Err(BuilderError::InvalidApiKeyLength(10))));
}

#[test]
fn test_api_key_slice_too_long() {
    let result = BatchBuilder::new().api_key_slice(&[0u8; 20]);
    assert!(matches!(result, Err(BuilderError::InvalidApiKeyLength(20))));
}

// =============================================================================
// Data payload tests
// =============================================================================

#[test]
fn test_data_empty() {
    let batch = BatchBuilder::new()
        .api_key([0u8; 16])
        .data(b"")
        .build()
        .unwrap();

    let parsed = FlatBatch::parse(batch.as_bytes()).unwrap();
    assert_eq!(parsed.data().unwrap(), b"");
}

#[test]
fn test_data_small() {
    let data = b"small payload";
    let batch = BatchBuilder::new()
        .api_key([0u8; 16])
        .data(data)
        .build()
        .unwrap();

    let parsed = FlatBatch::parse(batch.as_bytes()).unwrap();
    assert_eq!(parsed.data().unwrap(), data);
}

#[test]
fn test_data_medium() {
    let data = vec![0xABu8; 10000];
    let batch = BatchBuilder::new()
        .api_key([0u8; 16])
        .data(&data)
        .build()
        .unwrap();

    let parsed = FlatBatch::parse(batch.as_bytes()).unwrap();
    assert_eq!(parsed.data().unwrap(), &data[..]);
}

#[test]
fn test_data_large() {
    let data = vec![0xCDu8; 1_000_000]; // 1 MB
    let batch = BatchBuilder::new()
        .api_key([0u8; 16])
        .data(&data)
        .build()
        .unwrap();

    let parsed = FlatBatch::parse(batch.as_bytes()).unwrap();
    assert_eq!(parsed.data().unwrap(), &data[..]);
}

#[test]
fn test_data_binary_all_bytes() {
    let data: Vec<u8> = (0u8..=255).collect();
    let batch = BatchBuilder::new()
        .api_key([0u8; 16])
        .data(&data)
        .build()
        .unwrap();

    let parsed = FlatBatch::parse(batch.as_bytes()).unwrap();
    assert_eq!(parsed.data().unwrap(), &data[..]);
}

#[test]
fn test_data_owned() {
    let data = vec![1, 2, 3, 4, 5];
    let batch = BatchBuilder::new()
        .api_key([0u8; 16])
        .data_owned(data.clone())
        .build()
        .unwrap();

    let parsed = FlatBatch::parse(batch.as_bytes()).unwrap();
    assert_eq!(parsed.data().unwrap(), &data[..]);
}

// =============================================================================
// Version tests
// =============================================================================

#[test]
fn test_version_default() {
    let batch = BatchBuilder::new()
        .api_key([0u8; 16])
        .data(b"x")
        .build()
        .unwrap();

    let parsed = FlatBatch::parse(batch.as_bytes()).unwrap();
    assert_eq!(parsed.version(), 100); // default v1.0
}

#[test]
fn test_version_custom() {
    let batch = BatchBuilder::new()
        .api_key([0u8; 16])
        .version(201)
        .data(b"x")
        .build()
        .unwrap();

    let parsed = FlatBatch::parse(batch.as_bytes()).unwrap();
    assert_eq!(parsed.version(), 201);
}

#[test]
fn test_version_zero() {
    let batch = BatchBuilder::new()
        .api_key([0u8; 16])
        .version(0)
        .data(b"x")
        .build()
        .unwrap();

    let parsed = FlatBatch::parse(batch.as_bytes()).unwrap();
    assert_eq!(parsed.version(), 0);
}

#[test]
fn test_version_max() {
    let batch = BatchBuilder::new()
        .api_key([0u8; 16])
        .version(255)
        .data(b"x")
        .build()
        .unwrap();

    let parsed = FlatBatch::parse(batch.as_bytes()).unwrap();
    assert_eq!(parsed.version(), 255);
}

// =============================================================================
// Batch ID tests
// =============================================================================

#[test]
fn test_batch_id_default() {
    let batch = BatchBuilder::new()
        .api_key([0u8; 16])
        .data(b"x")
        .build()
        .unwrap();

    let parsed = FlatBatch::parse(batch.as_bytes()).unwrap();
    assert_eq!(parsed.batch_id(), 0);
}

#[test]
fn test_batch_id_custom() {
    let batch = BatchBuilder::new()
        .api_key([0u8; 16])
        .batch_id(123456789)
        .data(b"x")
        .build()
        .unwrap();

    let parsed = FlatBatch::parse(batch.as_bytes()).unwrap();
    assert_eq!(parsed.batch_id(), 123456789);
}

#[test]
fn test_batch_id_large() {
    let batch = BatchBuilder::new()
        .api_key([0u8; 16])
        .batch_id(u64::MAX)
        .data(b"x")
        .build()
        .unwrap();

    let parsed = FlatBatch::parse(batch.as_bytes()).unwrap();
    assert_eq!(parsed.batch_id(), u64::MAX);
}

// =============================================================================
// Source IP tests
// =============================================================================

#[test]
fn test_source_ip_not_present() {
    let batch = BatchBuilder::new()
        .api_key([0u8; 16])
        .data(b"x")
        .build()
        .unwrap();

    let parsed = FlatBatch::parse(batch.as_bytes()).unwrap();
    assert!(!parsed.has_source_ip());
    assert!(parsed.source_ip().unwrap().is_none());
}

#[test]
fn test_source_ip_present() {
    let source_ip = [
        0x20, 0x01, 0x0d, 0xb8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x01,
    ];

    let batch = BatchBuilder::new()
        .api_key([0u8; 16])
        .data(b"x")
        .source_ip(source_ip)
        .build()
        .unwrap();

    let parsed = FlatBatch::parse(batch.as_bytes()).unwrap();
    assert!(parsed.has_source_ip());
    assert_eq!(parsed.source_ip().unwrap(), Some(&source_ip));
}

#[test]
fn test_source_ip_slice_valid() {
    let ip_slice: &[u8] = &[
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0xff, 192, 168, 1, 1,
    ];

    let batch = BatchBuilder::new()
        .api_key([0u8; 16])
        .data(b"x")
        .source_ip_slice(ip_slice)
        .unwrap()
        .build()
        .unwrap();

    let parsed = FlatBatch::parse(batch.as_bytes()).unwrap();
    assert!(parsed.has_source_ip());
    assert_eq!(parsed.source_ip_bytes().unwrap(), Some(ip_slice));
}

#[test]
fn test_source_ip_slice_too_short() {
    let result = BatchBuilder::new()
        .api_key([0u8; 16])
        .data(b"x")
        .source_ip_slice(&[0u8; 4]); // IPv4 length, not IPv6

    assert!(matches!(
        result,
        Err(BuilderError::InvalidSourceIpLength(4))
    ));
}

#[test]
fn test_source_ip_clear() {
    let batch = BatchBuilder::new()
        .api_key([0u8; 16])
        .data(b"x")
        .source_ip([1u8; 16])
        .clear_source_ip()
        .build()
        .unwrap();

    let parsed = FlatBatch::parse(batch.as_bytes()).unwrap();
    assert!(!parsed.has_source_ip());
}

// =============================================================================
// Error cases
// =============================================================================

#[test]
fn test_missing_api_key() {
    let result = BatchBuilder::new().data(b"data").build();

    assert!(matches!(result, Err(BuilderError::MissingApiKey)));
}

#[test]
fn test_missing_data() {
    let result = BatchBuilder::new().api_key([0u8; 16]).build();

    assert!(matches!(result, Err(BuilderError::MissingData)));
}

#[test]
fn test_data_too_large() {
    let huge_data = vec![0u8; 20 * 1024 * 1024]; // 20 MB, exceeds 16 MB limit

    let result = BatchBuilder::new()
        .api_key([0u8; 16])
        .data(&huge_data)
        .build();

    assert!(matches!(result, Err(BuilderError::DataTooLarge { .. })));
}

// =============================================================================
// BuiltBatch tests
// =============================================================================

#[test]
fn test_built_batch_as_bytes() {
    let batch = BatchBuilder::new()
        .api_key([0u8; 16])
        .data(b"test")
        .build()
        .unwrap();

    let bytes = batch.as_bytes();
    assert!(!bytes.is_empty());
}

#[test]
fn test_built_batch_into_bytes() {
    let batch = BatchBuilder::new()
        .api_key([0u8; 16])
        .data(b"test")
        .build()
        .unwrap();

    let bytes = batch.into_bytes();
    assert!(!bytes.is_empty());
}

#[test]
fn test_built_batch_len() {
    let batch = BatchBuilder::new()
        .api_key([0u8; 16])
        .data(b"test")
        .build()
        .unwrap();

    assert!(batch.len() > 0);
    assert!(!batch.is_empty());
}

#[test]
fn test_built_batch_as_ref() {
    let batch = BatchBuilder::new()
        .api_key([0u8; 16])
        .data(b"test")
        .build()
        .unwrap();

    let slice: &[u8] = batch.as_ref();
    assert!(!slice.is_empty());
}

// =============================================================================
// Builder trait tests
// =============================================================================

#[test]
fn test_builder_clone() {
    let builder = BatchBuilder::new()
        .api_key([0u8; 16])
        .schema_type(SchemaType::Event);

    let cloned = builder.clone();

    let batch1 = builder.data(b"data1").build().unwrap();
    let batch2 = cloned.data(b"data2").build().unwrap();

    let parsed1 = FlatBatch::parse(batch1.as_bytes()).unwrap();
    let parsed2 = FlatBatch::parse(batch2.as_bytes()).unwrap();

    assert_eq!(parsed1.data().unwrap(), b"data1");
    assert_eq!(parsed2.data().unwrap(), b"data2");
}

#[test]
fn test_builder_default() {
    let batch = BatchBuilder::default()
        .api_key([0u8; 16])
        .data(b"default test")
        .build()
        .unwrap();

    let parsed = FlatBatch::parse(batch.as_bytes()).unwrap();
    assert_eq!(parsed.schema_type(), SchemaType::Unknown);
    assert_eq!(parsed.version(), 100);
}

// =============================================================================
// Edge cases
// =============================================================================

#[test]
fn test_multiple_builds_same_data() {
    let api_key = [0x99u8; 16];
    let data = b"same data";

    let batch1 = BatchBuilder::new()
        .api_key(api_key)
        .data(data)
        .build()
        .unwrap();

    let batch2 = BatchBuilder::new()
        .api_key(api_key)
        .data(data)
        .build()
        .unwrap();

    // Both should produce identical wire format
    assert_eq!(batch1.as_bytes(), batch2.as_bytes());
}

#[test]
fn test_field_order_independence() {
    let api_key = [0x11u8; 16];
    let data = b"order test";

    // Set fields in different orders
    let batch1 = BatchBuilder::new()
        .api_key(api_key)
        .schema_type(SchemaType::Event)
        .version(101)
        .data(data)
        .build()
        .unwrap();

    let batch2 = BatchBuilder::new()
        .data(data)
        .version(101)
        .schema_type(SchemaType::Event)
        .api_key(api_key)
        .build()
        .unwrap();

    // Should produce identical wire format
    assert_eq!(batch1.as_bytes(), batch2.as_bytes());
}
