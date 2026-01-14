//! Tests for replay buffer

use super::*;
use cdp_protocol::{BatchBuilder, BatchType, SourceId};

/// Helper to create a test batch with a unique identifier
fn make_batch(id: u32) -> Arc<Batch> {
    let mut builder = BatchBuilder::new(BatchType::Event, SourceId::new(format!("test_{}", id)));
    builder.set_workspace_id(id);
    Arc::new(builder.finish())
}

// ============================================================================
// Basic operations
// ============================================================================

#[test]
fn test_new_buffer_is_empty() {
    let buffer = ReplayBuffer::new();
    assert!(buffer.is_empty());
    assert_eq!(buffer.len(), 0);
    assert_eq!(buffer.total_written(), 0);
}

#[test]
fn test_push_increments_count() {
    let buffer = ReplayBuffer::new();

    buffer.push(make_batch(1));
    assert_eq!(buffer.len(), 1);
    assert_eq!(buffer.total_written(), 1);

    buffer.push(make_batch(2));
    assert_eq!(buffer.len(), 2);
    assert_eq!(buffer.total_written(), 2);
}

#[test]
fn test_last_n_returns_requested_count() {
    let buffer = ReplayBuffer::new();

    for i in 0..10 {
        buffer.push(make_batch(i));
    }

    let result = buffer.last_n(5);
    assert_eq!(result.len(), 5);

    // Should be the last 5 (ids 5-9)
    for (i, batch) in result.iter().enumerate() {
        assert_eq!(batch.workspace_id(), (5 + i) as u32);
    }
}

#[test]
fn test_last_n_returns_all_if_fewer_available() {
    let buffer = ReplayBuffer::new();

    buffer.push(make_batch(1));
    buffer.push(make_batch(2));
    buffer.push(make_batch(3));

    let result = buffer.last_n(10);
    assert_eq!(result.len(), 3);
}

#[test]
fn test_last_n_empty_buffer() {
    let buffer = ReplayBuffer::new();
    let result = buffer.last_n(10);
    assert!(result.is_empty());
}

#[test]
fn test_last_n_zero() {
    let buffer = ReplayBuffer::new();
    buffer.push(make_batch(1));

    let result = buffer.last_n(0);
    assert!(result.is_empty());
}

// ============================================================================
// Ring buffer behavior
// ============================================================================

#[test]
fn test_ring_buffer_wraps() {
    let buffer = ReplayBuffer::with_capacity(5);

    // Push 8 items into a buffer of size 5
    for i in 0..8 {
        buffer.push(make_batch(i));
    }

    // Should have exactly 5 items
    assert_eq!(buffer.len(), 5);
    assert_eq!(buffer.total_written(), 8);

    // last_n should return the most recent 5 (ids 3-7)
    let result = buffer.last_n(5);
    assert_eq!(result.len(), 5);

    for (i, batch) in result.iter().enumerate() {
        assert_eq!(batch.workspace_id(), (3 + i) as u32);
    }
}

#[test]
fn test_ring_buffer_partial_after_wrap() {
    let buffer = ReplayBuffer::with_capacity(5);

    // Push 8 items
    for i in 0..8 {
        buffer.push(make_batch(i));
    }

    // Request 3 items
    let result = buffer.last_n(3);
    assert_eq!(result.len(), 3);

    // Should be the last 3 (ids 5-7)
    for (i, batch) in result.iter().enumerate() {
        assert_eq!(batch.workspace_id(), (5 + i) as u32);
    }
}

#[test]
fn test_ring_buffer_exactly_full() {
    let buffer = ReplayBuffer::with_capacity(5);

    // Push exactly 5 items
    for i in 0..5 {
        buffer.push(make_batch(i));
    }

    assert_eq!(buffer.len(), 5);

    let result = buffer.last_n(5);
    for (i, batch) in result.iter().enumerate() {
        assert_eq!(batch.workspace_id(), i as u32);
    }
}

// ============================================================================
// Clear operation
// ============================================================================

#[test]
fn test_clear() {
    let buffer = ReplayBuffer::new();

    buffer.push(make_batch(1));
    buffer.push(make_batch(2));
    assert_eq!(buffer.len(), 2);

    buffer.clear();
    assert!(buffer.is_empty());
    assert_eq!(buffer.total_written(), 0);
    assert!(buffer.last_n(10).is_empty());
}

// ============================================================================
// Capacity tests
// ============================================================================

#[test]
fn test_capacity() {
    let buffer = ReplayBuffer::with_capacity(100);
    assert_eq!(buffer.capacity(), 100);
}

#[test]
fn test_default_capacity() {
    let buffer = ReplayBuffer::new();
    assert_eq!(buffer.capacity(), 1000); // DEFAULT_CAPACITY
}

#[test]
fn test_max_capacity_enforced() {
    let buffer = ReplayBuffer::with_capacity(1_000_000);
    // Should be clamped to MAX_CAPACITY
    assert_eq!(buffer.capacity(), 100_000);
}

// ============================================================================
// Concurrency tests
// ============================================================================

#[tokio::test]
async fn test_concurrent_push_and_read() {
    let buffer = Arc::new(ReplayBuffer::with_capacity(100));

    // Spawn writers
    let buffer_clone = Arc::clone(&buffer);
    let writer = tokio::spawn(async move {
        for i in 0..50 {
            buffer_clone.push(make_batch(i));
            tokio::task::yield_now().await;
        }
    });

    // Spawn reader
    let buffer_clone = Arc::clone(&buffer);
    let reader = tokio::spawn(async move {
        for _ in 0..20 {
            let _ = buffer_clone.last_n(10);
            tokio::task::yield_now().await;
        }
    });

    // Wait for both
    writer.await.unwrap();
    reader.await.unwrap();

    // Verify final state
    assert_eq!(buffer.total_written(), 50);
}
