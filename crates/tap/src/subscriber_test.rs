//! Tests for subscriber management

use super::*;
use cdp_protocol::{BatchBuilder, BatchType, SourceId};

/// Helper to create a test batch
fn make_batch(workspace_id: u32, source_id: &str, batch_type: BatchType) -> Batch {
    let mut builder = BatchBuilder::new(batch_type, SourceId::new(source_id.to_string()));
    builder.set_workspace_id(workspace_id);
    builder.finish()
}

// ============================================================================
// SubscriberManager tests
// ============================================================================

#[tokio::test]
async fn test_subscribe_creates_subscriber() {
    let manager = SubscriberManager::new();
    let request = SubscribeRequest::new();

    let result = manager.subscribe(&request);
    assert!(result.is_ok());

    let (id, _rx) = result.unwrap();
    assert!(id > 0);
    assert_eq!(manager.count(), 1);
}

#[tokio::test]
async fn test_subscribe_unique_ids() {
    let manager = SubscriberManager::new();
    let request = SubscribeRequest::new();

    let (id1, _rx1) = manager.subscribe(&request).unwrap();
    let (id2, _rx2) = manager.subscribe(&request).unwrap();

    assert_ne!(id1, id2);
    assert_eq!(manager.count(), 2);
}

#[tokio::test]
async fn test_unsubscribe_removes_subscriber() {
    let manager = SubscriberManager::new();
    let request = SubscribeRequest::new();

    let (id, _rx) = manager.subscribe(&request).unwrap();
    assert_eq!(manager.count(), 1);

    manager.unsubscribe(id).unwrap();
    assert_eq!(manager.count(), 0);
}

#[tokio::test]
async fn test_unsubscribe_not_found() {
    let manager = SubscriberManager::new();
    let result = manager.unsubscribe(999);

    assert!(matches!(
        result,
        Err(TapError::SubscriberNotFound { id: 999 })
    ));
}

#[tokio::test]
async fn test_has_subscribers() {
    let manager = SubscriberManager::new();
    assert!(!manager.has_subscribers());

    let request = SubscribeRequest::new();
    let (id, _rx) = manager.subscribe(&request).unwrap();
    assert!(manager.has_subscribers());

    manager.unsubscribe(id).unwrap();
    assert!(!manager.has_subscribers());
}

// ============================================================================
// Broadcast tests
// ============================================================================

#[tokio::test]
async fn test_broadcast_to_matching_subscriber() {
    let manager = SubscriberManager::new();
    let request = SubscribeRequest::new().with_workspaces(vec![42]);

    let (_id, mut rx) = manager.subscribe(&request).unwrap();

    // Broadcast matching batch
    let batch = Arc::new(make_batch(42, "tcp", BatchType::Event));
    let sent = manager.broadcast(batch);
    assert_eq!(sent, 1);

    // Verify received
    let received = rx.try_recv();
    assert!(received.is_ok());
}

#[tokio::test]
async fn test_broadcast_filters_non_matching() {
    let manager = SubscriberManager::new();
    let request = SubscribeRequest::new().with_workspaces(vec![42]);

    let (_id, mut rx) = manager.subscribe(&request).unwrap();

    // Broadcast non-matching batch
    let batch = Arc::new(make_batch(99, "tcp", BatchType::Event));
    let sent = manager.broadcast(batch);
    assert_eq!(sent, 0);

    // Verify nothing received
    let received = rx.try_recv();
    assert!(received.is_err());
}

#[tokio::test]
async fn test_broadcast_to_multiple_subscribers() {
    let manager = SubscriberManager::new();

    // Two subscribers with different filters
    let req1 = SubscribeRequest::new().with_workspaces(vec![1, 2]);
    let req2 = SubscribeRequest::new().with_workspaces(vec![2, 3]);

    let (_id1, mut rx1) = manager.subscribe(&req1).unwrap();
    let (_id2, mut rx2) = manager.subscribe(&req2).unwrap();

    // Batch matching both
    let batch = Arc::new(make_batch(2, "tcp", BatchType::Event));
    let sent = manager.broadcast(batch);
    assert_eq!(sent, 2);

    assert!(rx1.try_recv().is_ok());
    assert!(rx2.try_recv().is_ok());

    // Batch matching only first
    let batch = Arc::new(make_batch(1, "tcp", BatchType::Event));
    let sent = manager.broadcast(batch);
    assert_eq!(sent, 1);

    assert!(rx1.try_recv().is_ok());
    assert!(rx2.try_recv().is_err());
}

// ============================================================================
// Cleanup tests
// ============================================================================

#[tokio::test]
async fn test_cleanup_disconnected() {
    let manager = SubscriberManager::new();
    let request = SubscribeRequest::new();

    let (id, rx) = manager.subscribe(&request).unwrap();
    assert_eq!(manager.count(), 1);

    // Drop the receiver (simulates client disconnect)
    drop(rx);

    // Subscriber still exists until cleanup
    assert_eq!(manager.count(), 1);

    // Run cleanup
    let removed = manager.cleanup_disconnected();
    assert_eq!(removed, 1);
    assert_eq!(manager.count(), 0);

    // Make sure subscriber was actually removed
    let _ = id; // silence unused warning
}

// ============================================================================
// Rate limiting tests
// ============================================================================

#[tokio::test]
async fn test_rate_limit_counter_reset() {
    let manager = SubscriberManager::new();
    let request = SubscribeRequest::new().with_rate_limit(1);

    let (_id, mut rx) = manager.subscribe(&request).unwrap();

    let batch = Arc::new(make_batch(1, "tcp", BatchType::Event));

    // First batch should succeed
    let sent = manager.broadcast(Arc::clone(&batch));
    assert_eq!(sent, 1);
    assert!(rx.try_recv().is_ok());

    // Second batch should be rate limited
    let sent = manager.broadcast(Arc::clone(&batch));
    assert_eq!(sent, 0);
    assert!(rx.try_recv().is_err());

    // Reset counters
    manager.reset_rate_counters();

    // Now batch should succeed again
    let sent = manager.broadcast(batch);
    assert_eq!(sent, 1);
    assert!(rx.try_recv().is_ok());
}

// ============================================================================
// Subscriber tests
// ============================================================================

#[test]
fn test_subscriber_sampling_always() {
    let filter = TapFilter::new();
    let (tx, _rx) = mpsc::channel(10);
    let subscriber = Subscriber::new(filter, tx, None, None);

    // Without sample_rate, should always sample
    for _ in 0..100 {
        assert!(subscriber.should_sample());
    }
}

#[test]
fn test_subscriber_sampling_zero() {
    let filter = TapFilter::new();
    let (tx, _rx) = mpsc::channel(10);
    let subscriber = Subscriber::new(filter, tx, Some(0.0), None);

    // With 0% sample rate, should never sample
    for _ in 0..100 {
        assert!(!subscriber.should_sample());
    }
}

#[test]
fn test_subscriber_sampling_full() {
    let filter = TapFilter::new();
    let (tx, _rx) = mpsc::channel(10);
    let subscriber = Subscriber::new(filter, tx, Some(1.0), None);

    // With 100% sample rate, should always sample
    for _ in 0..100 {
        assert!(subscriber.should_sample());
    }
}

#[test]
fn test_subscriber_is_connected() {
    let filter = TapFilter::new();
    let (tx, rx) = mpsc::channel(10);
    let subscriber = Subscriber::new(filter, tx, None, None);

    assert!(subscriber.is_connected());

    // Drop receiver
    drop(rx);

    assert!(!subscriber.is_connected());
}
