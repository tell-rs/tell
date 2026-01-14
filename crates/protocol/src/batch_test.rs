//! Tests for Batch and BatchBuilder

use crate::batch::BatchBuilder;
use crate::schema::BatchType;
use crate::source::SourceId;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

// =============================================================================
// BatchBuilder::new tests
// =============================================================================

#[test]
fn test_batch_builder_new() {
    let source_id = SourceId::new("tcp_main");
    let builder = BatchBuilder::new(BatchType::Event, source_id);

    assert!(builder.is_empty());
    assert_eq!(builder.count(), 0);
    assert_eq!(builder.message_count(), 0);
    assert!(!builder.is_full());
}

#[test]
fn test_batch_builder_with_capacity() {
    let source_id = SourceId::new("tcp_main");
    let builder = BatchBuilder::with_capacity(BatchType::Log, source_id, 1024);

    assert!(builder.is_empty());
    assert_eq!(builder.buffer_size(), 0);
}

// =============================================================================
// BatchBuilder::add tests
// =============================================================================

#[test]
fn test_batch_builder_add_single_message() {
    let source_id = SourceId::new("test");
    let mut builder = BatchBuilder::new(BatchType::Event, source_id);

    let is_full = builder.add(b"message1", 1);

    assert!(!is_full);
    assert_eq!(builder.count(), 1);
    assert_eq!(builder.message_count(), 1);
    assert_eq!(builder.buffer_size(), 8); // "message1" length
}

#[test]
fn test_batch_builder_add_multiple_messages() {
    let source_id = SourceId::new("test");
    let mut builder = BatchBuilder::new(BatchType::Event, source_id);

    builder.add(b"msg1", 1);
    builder.add(b"msg2", 1);
    builder.add(b"msg3", 1);

    assert_eq!(builder.count(), 3);
    assert_eq!(builder.message_count(), 3);
    assert_eq!(builder.buffer_size(), 12); // 4 + 4 + 4
}

#[test]
fn test_batch_builder_add_with_item_count() {
    let source_id = SourceId::new("test");
    let mut builder = BatchBuilder::new(BatchType::Event, source_id);

    // One message containing 10 events
    builder.add(b"batch_of_events", 10);

    assert_eq!(builder.count(), 10);
    assert_eq!(builder.message_count(), 1);
}

#[test]
fn test_batch_builder_add_returns_full() {
    let source_id = SourceId::new("test");
    let mut builder = BatchBuilder::new(BatchType::Event, source_id);
    builder.set_max_items(3);

    assert!(!builder.add(b"m1", 1));
    assert!(!builder.add(b"m2", 1));
    assert!(builder.add(b"m3", 1)); // Should return true when full

    assert!(builder.is_full());
}

#[test]
fn test_batch_builder_add_raw() {
    let source_id = SourceId::new("test");
    let mut builder = BatchBuilder::new(BatchType::Log, source_id);

    builder.add_raw(b"raw_message");

    assert_eq!(builder.count(), 1);
    assert_eq!(builder.message_count(), 1);
}

// =============================================================================
// BatchBuilder::set_* tests
// =============================================================================

#[test]
fn test_batch_builder_set_workspace_id() {
    let source_id = SourceId::new("test");
    let mut builder = BatchBuilder::new(BatchType::Event, source_id);

    builder.set_workspace_id(42);
    builder.add(b"data", 1);

    let batch = builder.finish();
    assert_eq!(batch.workspace_id(), 42);
}

#[test]
fn test_batch_builder_set_source_ip_v4() {
    let source_id = SourceId::new("test");
    let mut builder = BatchBuilder::new(BatchType::Event, source_id);

    let ip = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 100));
    builder.set_source_ip(ip);
    builder.add(b"data", 1);

    let batch = builder.finish();
    assert_eq!(batch.source_ip(), ip);
}

#[test]
fn test_batch_builder_set_source_ip_v6() {
    let source_id = SourceId::new("test");
    let mut builder = BatchBuilder::new(BatchType::Event, source_id);

    let ip = IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1));
    builder.set_source_ip(ip);
    builder.add(b"data", 1);

    let batch = builder.finish();
    assert_eq!(batch.source_ip(), ip);
}

#[test]
fn test_batch_builder_set_max_items() {
    let source_id = SourceId::new("test");
    let mut builder = BatchBuilder::new(BatchType::Event, source_id);

    builder.set_max_items(5);

    for i in 0..4 {
        assert!(!builder.add(format!("msg{}", i).as_bytes(), 1));
    }
    assert!(builder.add(b"msg4", 1)); // 5th item makes it full
}

// =============================================================================
// BatchBuilder::reset tests
// =============================================================================

#[test]
fn test_batch_builder_reset() {
    let source_id = SourceId::new("test");
    let mut builder = BatchBuilder::new(BatchType::Event, source_id);

    builder.set_workspace_id(99);
    builder.set_source_ip(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
    builder.add(b"message1", 5);
    builder.add(b"message2", 3);

    builder.reset();

    assert!(builder.is_empty());
    assert_eq!(builder.count(), 0);
    assert_eq!(builder.message_count(), 0);
    assert_eq!(builder.buffer_size(), 0);
}

#[test]
fn test_batch_builder_reset_with_type() {
    let source_id = SourceId::new("test");
    let mut builder = BatchBuilder::new(BatchType::Event, source_id);

    builder.add(b"event_data", 1);
    builder.reset_with_type(BatchType::Log);
    builder.add(b"log_data", 1);

    let batch = builder.finish();
    assert_eq!(batch.batch_type(), BatchType::Log);
}

// =============================================================================
// BatchBuilder::finish tests
// =============================================================================

#[test]
fn test_batch_builder_finish_empty() {
    let source_id = SourceId::new("test");
    let builder = BatchBuilder::new(BatchType::Event, source_id);

    let batch = builder.finish();

    assert!(batch.is_empty());
    assert_eq!(batch.count(), 0);
    assert_eq!(batch.message_count(), 0);
}

#[test]
fn test_batch_builder_finish_with_data() {
    let source_id = SourceId::new("tcp_main");
    let mut builder = BatchBuilder::new(BatchType::Event, source_id);

    builder.set_workspace_id(123);
    builder.add(b"first", 2);
    builder.add(b"second", 3);

    let batch = builder.finish();

    assert_eq!(batch.count(), 5);
    assert_eq!(batch.message_count(), 2);
    assert_eq!(batch.workspace_id(), 123);
    assert_eq!(batch.batch_type(), BatchType::Event);
    assert_eq!(batch.source_id().as_str(), "tcp_main");
}

// =============================================================================
// Batch::get_message tests
// =============================================================================

#[test]
fn test_batch_get_message_valid_index() {
    let source_id = SourceId::new("test");
    let mut builder = BatchBuilder::new(BatchType::Event, source_id);

    builder.add(b"message0", 1);
    builder.add(b"message1", 1);
    builder.add(b"message2", 1);

    let batch = builder.finish();

    assert_eq!(batch.get_message(0), Some(b"message0".as_slice()));
    assert_eq!(batch.get_message(1), Some(b"message1".as_slice()));
    assert_eq!(batch.get_message(2), Some(b"message2".as_slice()));
}

#[test]
fn test_batch_get_message_out_of_bounds() {
    let source_id = SourceId::new("test");
    let mut builder = BatchBuilder::new(BatchType::Event, source_id);

    builder.add(b"only_message", 1);

    let batch = builder.finish();

    assert_eq!(batch.get_message(0), Some(b"only_message".as_slice()));
    assert_eq!(batch.get_message(1), None);
    assert_eq!(batch.get_message(100), None);
}

#[test]
fn test_batch_get_message_empty_batch() {
    let source_id = SourceId::new("test");
    let builder = BatchBuilder::new(BatchType::Event, source_id);

    let batch = builder.finish();

    assert_eq!(batch.get_message(0), None);
}

#[test]
fn test_batch_get_message_binary_data() {
    let source_id = SourceId::new("test");
    let mut builder = BatchBuilder::new(BatchType::Event, source_id);

    let binary: Vec<u8> = (0u8..=255).collect();
    builder.add(&binary, 1);

    let batch = builder.finish();

    assert_eq!(batch.get_message(0), Some(binary.as_slice()));
}

// =============================================================================
// Batch::messages iterator tests
// =============================================================================

#[test]
fn test_batch_messages_iterator() {
    let source_id = SourceId::new("test");
    let mut builder = BatchBuilder::new(BatchType::Log, source_id);

    builder.add(b"first", 1);
    builder.add(b"second", 1);
    builder.add(b"third", 1);

    let batch = builder.finish();
    let messages: Vec<&[u8]> = batch.messages().collect();

    assert_eq!(messages.len(), 3);
    assert_eq!(messages[0], b"first");
    assert_eq!(messages[1], b"second");
    assert_eq!(messages[2], b"third");
}

#[test]
fn test_batch_messages_iterator_empty() {
    let source_id = SourceId::new("test");
    let builder = BatchBuilder::new(BatchType::Log, source_id);

    let batch = builder.finish();
    let messages: Vec<&[u8]> = batch.messages().collect();

    assert!(messages.is_empty());
}

// =============================================================================
// Batch clone tests (zero-copy verification)
// =============================================================================

#[test]
fn test_batch_clone_shares_buffer() {
    let source_id = SourceId::new("test");
    let mut builder = BatchBuilder::new(BatchType::Event, source_id);

    builder.add(b"shared_data", 1);

    let batch1 = builder.finish();
    let batch2 = batch1.clone();

    // Both batches should have the same data
    assert_eq!(batch1.get_message(0), batch2.get_message(0));

    // The underlying buffer should be the same (Bytes uses Arc internally)
    // We can verify by checking the pointer addresses
    let ptr1 = batch1.get_message(0).unwrap().as_ptr();
    let ptr2 = batch2.get_message(0).unwrap().as_ptr();
    assert_eq!(ptr1, ptr2, "Cloned batch should share buffer memory");
}

#[test]
fn test_batch_clone_independence() {
    let source_id = SourceId::new("test");
    let mut builder = BatchBuilder::new(BatchType::Event, source_id);

    builder.add(b"data", 1);

    let batch1 = builder.finish();
    let mut batch2 = batch1.clone();

    // Modifying pattern_ids on one shouldn't affect the other
    batch2.set_pattern_ids(vec![1, 2, 3]);

    assert!(batch1.pattern_ids().is_none());
    assert_eq!(batch2.pattern_ids(), Some(&[1u64, 2, 3][..]));
}

// =============================================================================
// Batch accessor tests
// =============================================================================

#[test]
fn test_batch_buffer() {
    let source_id = SourceId::new("test");
    let mut builder = BatchBuilder::new(BatchType::Event, source_id);

    builder.add(b"hello", 1);
    builder.add(b"world", 1);

    let batch = builder.finish();

    assert_eq!(batch.buffer().as_ref(), b"helloworld");
    assert_eq!(batch.total_bytes(), 10);
}

#[test]
fn test_batch_offsets_and_lengths() {
    let source_id = SourceId::new("test");
    let mut builder = BatchBuilder::new(BatchType::Event, source_id);

    builder.add(b"aa", 1); // offset 0, len 2
    builder.add(b"bbbb", 1); // offset 2, len 4
    builder.add(b"c", 1); // offset 6, len 1

    let batch = builder.finish();

    assert_eq!(batch.offsets(), &[0, 2, 6]);
    assert_eq!(batch.lengths(), &[2, 4, 1]);
}

#[test]
fn test_batch_pattern_ids() {
    let source_id = SourceId::new("test");
    let mut builder = BatchBuilder::new(BatchType::Log, source_id);

    builder.add(b"log1", 1);
    builder.add(b"log2", 1);

    let mut batch = builder.finish();

    // Initially no pattern IDs
    assert!(batch.pattern_ids().is_none());

    // Set pattern IDs
    batch.set_pattern_ids(vec![100, 200]);

    assert_eq!(batch.pattern_ids(), Some(&[100u64, 200][..]));
}

// =============================================================================
// Batch type tests
// =============================================================================

#[test]
fn test_batch_type_event() {
    let source_id = SourceId::new("test");
    let mut builder = BatchBuilder::new(BatchType::Event, source_id);
    builder.add(b"x", 1);

    let batch = builder.finish();
    assert_eq!(batch.batch_type(), BatchType::Event);
}

#[test]
fn test_batch_type_log() {
    let source_id = SourceId::new("test");
    let mut builder = BatchBuilder::new(BatchType::Log, source_id);
    builder.add(b"x", 1);

    let batch = builder.finish();
    assert_eq!(batch.batch_type(), BatchType::Log);
}

#[test]
fn test_batch_type_syslog() {
    let source_id = SourceId::new("test");
    let mut builder = BatchBuilder::new(BatchType::Syslog, source_id);
    builder.add(b"x", 1);

    let batch = builder.finish();
    assert_eq!(batch.batch_type(), BatchType::Syslog);
}

// =============================================================================
// Default source IP tests
// =============================================================================

#[test]
fn test_batch_default_source_ip() {
    let source_id = SourceId::new("test");
    let mut builder = BatchBuilder::new(BatchType::Event, source_id);
    builder.add(b"x", 1);

    let batch = builder.finish();

    // Default should be unspecified IPv4
    assert_eq!(batch.source_ip(), IpAddr::V4(Ipv4Addr::UNSPECIFIED));
}

// =============================================================================
// Large batch tests
// =============================================================================

#[test]
fn test_batch_large_message_count() {
    let source_id = SourceId::new("test");
    let mut builder = BatchBuilder::new(BatchType::Event, source_id);
    builder.set_max_items(1000);

    for i in 0..500 {
        builder.add(format!("message_{:04}", i).as_bytes(), 1);
    }

    let batch = builder.finish();

    assert_eq!(batch.count(), 500);
    assert_eq!(batch.message_count(), 500);

    // Verify first and last messages
    assert_eq!(batch.get_message(0), Some(b"message_0000".as_slice()));
    assert_eq!(batch.get_message(499), Some(b"message_0499".as_slice()));
}

#[test]
fn test_batch_large_single_message() {
    let source_id = SourceId::new("test");
    let mut builder = BatchBuilder::new(BatchType::Event, source_id);

    // Create a 1MB message
    let large_data = vec![0xABu8; 1024 * 1024];
    builder.add(&large_data, 1);

    let batch = builder.finish();

    assert_eq!(batch.count(), 1);
    assert_eq!(batch.total_bytes(), 1024 * 1024);
    assert_eq!(batch.get_message(0).unwrap().len(), 1024 * 1024);
}

// =============================================================================
// Builder reuse tests
// =============================================================================

#[test]
fn test_batch_builder_reuse_after_reset() {
    let source_id = SourceId::new("test");
    let mut builder = BatchBuilder::new(BatchType::Event, source_id);

    // First batch
    builder.set_workspace_id(1);
    builder.add(b"batch1_msg", 1);
    let batch1 = builder.finish();

    // Need to create a new builder since finish() consumes it
    let mut builder = BatchBuilder::new(BatchType::Event, SourceId::new("test"));

    // Second batch
    builder.set_workspace_id(2);
    builder.add(b"batch2_msg", 1);
    let batch2 = builder.finish();

    assert_eq!(batch1.workspace_id(), 1);
    assert_eq!(batch2.workspace_id(), 2);
    assert_eq!(batch1.get_message(0), Some(b"batch1_msg".as_slice()));
    assert_eq!(batch2.get_message(0), Some(b"batch2_msg".as_slice()));
}
