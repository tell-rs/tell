//! Tests for LazyBatch

use super::*;
use tell_protocol::{BatchBuilder, BatchType, SchemaType, SourceId};
use tell_protocol::{BatchEncoder, EncodedLogEntry, LogEncoder, LogLevelValue};

fn create_test_batch() -> Batch {
    let source_id = SourceId::new("test");
    let mut builder = BatchBuilder::new(BatchType::Log, source_id);
    builder.set_workspace_id(42);
    builder.add(b"Log message one", 1);
    builder.add(b"Log message two", 1);
    builder.add(b"Log message three", 1);
    builder.finish()
}

fn create_batch_with_binary() -> Batch {
    let source_id = SourceId::new("test");
    let mut builder = BatchBuilder::new(BatchType::Log, source_id);
    // Invalid UTF-8 sequence
    builder.add(&[0x48, 0x65, 0x6c, 0x6c, 0x6f, 0xff, 0xfe], 1);
    builder.add(b"Valid UTF-8", 1);
    builder.finish()
}

#[test]
fn test_lazy_batch_creation() {
    let batch = create_test_batch();
    let msg_count = batch.message_count();

    let lazy = LazyBatch::new(batch);

    assert!(!lazy.is_decoded());
    assert_eq!(lazy.batch().message_count(), msg_count);
}

#[test]
fn test_decoded_on_first_access() {
    let lazy = LazyBatch::new(create_test_batch());

    assert!(!lazy.is_decoded());

    let decoded = lazy.decoded_messages().unwrap();
    assert_eq!(decoded.len(), 3);

    assert!(lazy.is_decoded());
}

#[test]
fn test_cached_after_first_decode() {
    let lazy = LazyBatch::new(create_test_batch());

    // First decode
    let decoded1 = lazy.decoded_messages().unwrap();
    let ptr1 = decoded1 as *const DecodedMessages;

    // Second access - should be same pointer (cached)
    let decoded2 = lazy.decoded_messages().unwrap();
    let ptr2 = decoded2 as *const DecodedMessages;

    assert_eq!(ptr1, ptr2, "Should return cached reference");
}

#[test]
fn test_decoded_message_content() {
    let lazy = LazyBatch::new(create_test_batch());
    let decoded = lazy.decoded_messages().unwrap();

    assert_eq!(decoded.get(0), Some("Log message one"));
    assert_eq!(decoded.get(1), Some("Log message two"));
    assert_eq!(decoded.get(2), Some("Log message three"));
    assert_eq!(decoded.get(3), None);
}

#[test]
fn test_decoded_messages_iteration() {
    let lazy = LazyBatch::new(create_test_batch());
    let decoded = lazy.decoded_messages().unwrap();

    let messages: Vec<&str> = decoded.iter().collect();
    assert_eq!(
        messages,
        vec!["Log message one", "Log message two", "Log message three"]
    );
}

#[test]
fn test_invalid_utf8_handling() {
    let lazy = LazyBatch::new(create_batch_with_binary());
    let decoded = lazy.decoded_messages().unwrap();

    // First message has invalid UTF-8, should contain replacement chars
    let first = decoded.get(0).unwrap();
    assert!(first.starts_with("Hello"));
    assert!(first.contains('\u{FFFD}')); // Replacement character

    // Second message is valid
    assert_eq!(decoded.get(1), Some("Valid UTF-8"));
}

#[test]
fn test_into_batch() {
    let batch = create_test_batch();
    let workspace_id = batch.workspace_id();

    let lazy = LazyBatch::new(batch);
    let recovered = lazy.into_batch();

    assert_eq!(recovered.workspace_id(), workspace_id);
}

#[test]
fn test_into_lazy_extension() {
    let batch = create_test_batch();
    let lazy = batch.into_lazy();

    assert!(!lazy.is_decoded());
}

#[test]
fn test_empty_batch() {
    let source_id = SourceId::new("test");
    let builder = BatchBuilder::new(BatchType::Log, source_id);
    let batch = builder.finish();

    let lazy = LazyBatch::new(batch);
    let decoded = lazy.decoded_messages().unwrap();

    assert!(decoded.is_empty());
    assert_eq!(decoded.len(), 0);
}

#[test]
fn test_decoded_messages_is_empty() {
    let messages = DecodedMessages::new(vec![]);
    assert!(messages.is_empty());

    let messages = DecodedMessages::new(vec!["test".to_string()]);
    assert!(!messages.is_empty());
}

#[test]
fn test_batch_reference_access() {
    let batch = create_test_batch();
    let batch_type = batch.batch_type();
    let workspace_id = batch.workspace_id();

    let lazy = LazyBatch::new(batch);

    // Can still access batch properties through reference
    assert_eq!(lazy.batch().batch_type(), batch_type);
    assert_eq!(lazy.batch().workspace_id(), workspace_id);
}

// =============================================================================
// Decoded Logs Tests
// =============================================================================

fn create_flatbuffer_log_batch() -> Batch {
    // Build a log entry using the encoder
    let log_entry = EncodedLogEntry {
        service: Some("test-service".to_string()),
        source: Some("test-host".to_string()),
        timestamp: 1700000000000,
        level: LogLevelValue::Info,
        payload: br#"{"message": "User logged in", "user_id": 12345}"#.to_vec(),
        ..Default::default()
    };

    // Encode log data
    let log_data = LogEncoder::encode_logs(&[log_entry]);

    // Wrap in batch with schema type
    let mut batch_encoder = BatchEncoder::new();
    batch_encoder.set_schema_type(SchemaType::Log);
    let batch_data = batch_encoder.encode(&log_data);

    // Create batch with FlatBuffer message
    let source_id = SourceId::new("test");
    let mut builder = BatchBuilder::new(BatchType::Log, source_id);
    builder.set_workspace_id(42);
    builder.add(&batch_data, 1);
    builder.finish()
}

#[test]
fn test_decoded_logs_not_initially_decoded() {
    let lazy = LazyBatch::new(create_test_batch());

    assert!(!lazy.is_logs_decoded());
}

#[test]
fn test_decoded_logs_empty_for_non_flatbuffer() {
    // Plain text batch (not FlatBuffer encoded)
    let lazy = LazyBatch::new(create_test_batch());

    let logs = lazy.decoded_logs().unwrap();

    // Should return empty collection for non-FlatBuffer data
    assert!(logs.is_empty());
}

#[test]
fn test_decoded_logs_flatbuffer_parsing() {
    let lazy = LazyBatch::new(create_flatbuffer_log_batch());

    let logs = lazy.decoded_logs().unwrap();

    assert_eq!(logs.len(), 1);
    assert!(lazy.is_logs_decoded());

    let entry = logs.get(0).unwrap();
    assert_eq!(entry.service.as_deref(), Some("test-service"));
    assert_eq!(entry.source.as_deref(), Some("test-host"));
    assert_eq!(entry.timestamp, 1700000000000);
}

#[test]
fn test_decoded_logs_cached() {
    let lazy = LazyBatch::new(create_flatbuffer_log_batch());

    // First decode
    let logs1 = lazy.decoded_logs().unwrap();
    let ptr1 = logs1 as *const DecodedLogs;

    // Second access - should be cached
    let logs2 = lazy.decoded_logs().unwrap();
    let ptr2 = logs2 as *const DecodedLogs;

    assert_eq!(ptr1, ptr2, "Should return cached reference");
}

#[test]
fn test_decoded_logs_payload_access() {
    let lazy = LazyBatch::new(create_flatbuffer_log_batch());

    let logs = lazy.decoded_logs().unwrap();
    let entry = logs.get(0).unwrap();

    // Should be able to get payload as string
    let payload = entry.payload_str();
    assert!(payload.contains("User logged in"));
    assert!(payload.contains("12345"));
}

#[test]
fn test_owned_log_entry_clone() {
    let entry = OwnedLogEntry {
        event_type: LogEventType::Log,
        session_id: Some([0u8; 16]),
        level: LogLevel::Info,
        timestamp: 12345,
        source: Some("host".to_string()),
        service: Some("svc".to_string()),
        payload: vec![1, 2, 3],
    };

    let cloned = entry.clone();

    assert_eq!(cloned.timestamp, entry.timestamp);
    assert_eq!(cloned.service, entry.service);
    assert_eq!(cloned.payload, entry.payload);
}

#[test]
fn test_decoded_logs_iteration() {
    let lazy = LazyBatch::new(create_flatbuffer_log_batch());

    let logs = lazy.decoded_logs().unwrap();
    let services: Vec<_> = logs.iter().filter_map(|e| e.service.as_deref()).collect();

    assert_eq!(services, vec!["test-service"]);
}

#[test]
fn test_decoded_logs_empty() {
    let logs = DecodedLogs::empty();

    assert!(logs.is_empty());
    assert_eq!(logs.len(), 0);
    assert!(logs.get(0).is_none());
}

#[test]
fn test_both_caches_independent() {
    let lazy = LazyBatch::new(create_flatbuffer_log_batch());

    // Decode raw messages first
    let _messages = lazy.decoded_messages().unwrap();
    assert!(lazy.is_decoded());
    assert!(!lazy.is_logs_decoded());

    // Then decode structured logs
    let _logs = lazy.decoded_logs().unwrap();
    assert!(lazy.is_decoded());
    assert!(lazy.is_logs_decoded());
}
