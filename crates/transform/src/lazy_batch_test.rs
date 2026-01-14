//! Tests for LazyBatch

use super::*;
use cdp_protocol::{BatchBuilder, BatchType, SourceId};

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
    assert_eq!(messages, vec!["Log message one", "Log message two", "Log message three"]);
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
