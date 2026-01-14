//! Tests for TapPoint

use super::*;
use cdp_protocol::{BatchBuilder, BatchType, SourceId};

/// Helper to create a test batch
fn make_batch(workspace_id: u32, source_id: &str, batch_type: BatchType) -> Arc<Batch> {
    let mut builder = BatchBuilder::new(batch_type, SourceId::new(source_id.to_string()));
    builder.set_workspace_id(workspace_id);
    Arc::new(builder.finish())
}

// ============================================================================
// Basic operations
// ============================================================================

#[test]
fn test_new_tap_point_has_no_subscribers() {
    let tap = TapPoint::new();
    assert!(!tap.has_subscribers());
    assert_eq!(tap.subscriber_count(), 0);
}

#[tokio::test]
async fn test_subscribe_adds_subscriber() {
    let tap = TapPoint::new();
    let request = SubscribeRequest::new();

    let result = tap.subscribe(&request);
    assert!(result.is_ok());

    assert!(tap.has_subscribers());
    assert_eq!(tap.subscriber_count(), 1);
}

#[tokio::test]
async fn test_unsubscribe_removes_subscriber() {
    let tap = TapPoint::new();
    let request = SubscribeRequest::new();

    let (id, _rx) = tap.subscribe(&request).unwrap();
    assert_eq!(tap.subscriber_count(), 1);

    tap.unsubscribe(id).unwrap();
    assert!(!tap.has_subscribers());
    assert_eq!(tap.subscriber_count(), 0);
}

// ============================================================================
// Tap behavior
// ============================================================================

#[tokio::test]
async fn test_tap_with_no_subscribers() {
    let tap = TapPoint::new();
    let batch = make_batch(1, "tcp", BatchType::Event);

    // Should be a complete no-op (zero cost)
    tap.tap(batch);

    let stats = tap.stats();
    assert_eq!(stats.tap_count, 0); // No work done when no subscribers
    assert_eq!(stats.sent_count, 0);
}

#[tokio::test]
async fn test_tap_with_subscriber() {
    let tap = TapPoint::new();
    let request = SubscribeRequest::new();

    let (_id, mut rx) = tap.subscribe(&request).unwrap();

    let batch = make_batch(1, "tcp", BatchType::Event);
    tap.tap(batch);

    // Should receive the batch
    let received = rx.try_recv();
    assert!(received.is_ok());

    let stats = tap.stats();
    assert_eq!(stats.tap_count, 1);
    assert_eq!(stats.sent_count, 1);
}

#[tokio::test]
async fn test_tap_filtered() {
    let tap = TapPoint::new();
    let request = SubscribeRequest::new().with_workspaces(vec![42]);

    let (_id, mut rx) = tap.subscribe(&request).unwrap();

    // Matching batch
    tap.tap(make_batch(42, "tcp", BatchType::Event));
    assert!(rx.try_recv().is_ok());

    // Non-matching batch
    tap.tap(make_batch(99, "tcp", BatchType::Event));
    assert!(rx.try_recv().is_err());
}

// ============================================================================
// Replay buffer integration
// ============================================================================

#[tokio::test]
async fn test_tap_stores_in_replay_buffer() {
    let tap = TapPoint::with_replay_capacity(10);

    // Subscribe first (replay buffer only fills when subscribers exist)
    let request = SubscribeRequest::new();
    let (_id, mut rx) = tap.subscribe(&request).unwrap();

    // Now tap some batches
    for i in 0..5 {
        tap.tap(make_batch(i, "tcp", BatchType::Event));
    }

    // Drain the receiver
    for _ in 0..5 {
        let _ = rx.try_recv();
    }

    let stats = tap.stats();
    assert_eq!(stats.replay_buffer_size, 5);
}

// ============================================================================
// Statistics
// ============================================================================

#[tokio::test]
async fn test_stats() {
    let tap = TapPoint::new();

    // Initial stats
    let stats = tap.stats();
    assert_eq!(stats.tap_count, 0);
    assert_eq!(stats.sent_count, 0);
    assert_eq!(stats.subscriber_count, 0);
    assert_eq!(stats.replay_buffer_size, 0);

    // Add subscriber
    let request = SubscribeRequest::new();
    let (_id, _rx) = tap.subscribe(&request).unwrap();

    // Tap some batches
    for i in 0..10 {
        tap.tap(make_batch(i, "tcp", BatchType::Event));
    }

    let stats = tap.stats();
    assert_eq!(stats.tap_count, 10);
    assert_eq!(stats.sent_count, 10);
    assert_eq!(stats.subscriber_count, 1);
    assert_eq!(stats.replay_buffer_size, 10);
}

// ============================================================================
// Cleanup
// ============================================================================

#[tokio::test]
async fn test_cleanup_removes_disconnected() {
    let tap = TapPoint::new();
    let request = SubscribeRequest::new();

    let (_id, rx) = tap.subscribe(&request).unwrap();
    assert!(tap.has_subscribers());

    // Drop receiver (simulates client disconnect)
    drop(rx);

    // Still registered
    assert!(tap.has_subscribers());

    // Run cleanup
    let removed = tap.cleanup();
    assert_eq!(removed, 1);
    assert!(!tap.has_subscribers());
}

// ============================================================================
// Multiple subscribers
// ============================================================================

#[tokio::test]
async fn test_multiple_subscribers() {
    let tap = TapPoint::new();

    let req1 = SubscribeRequest::new().with_workspaces(vec![1]);
    let req2 = SubscribeRequest::new().with_workspaces(vec![2]);
    let req3 = SubscribeRequest::new(); // matches all

    let (_id1, mut rx1) = tap.subscribe(&req1).unwrap();
    let (_id2, mut rx2) = tap.subscribe(&req2).unwrap();
    let (_id3, mut rx3) = tap.subscribe(&req3).unwrap();

    assert_eq!(tap.subscriber_count(), 3);

    // Batch matching subscriber 1 and 3
    tap.tap(make_batch(1, "tcp", BatchType::Event));
    assert!(rx1.try_recv().is_ok());
    assert!(rx2.try_recv().is_err());
    assert!(rx3.try_recv().is_ok());

    // Batch matching subscriber 2 and 3
    tap.tap(make_batch(2, "tcp", BatchType::Event));
    assert!(rx1.try_recv().is_err());
    assert!(rx2.try_recv().is_ok());
    assert!(rx3.try_recv().is_ok());
}
