//! Router tests
//!
//! Comprehensive tests for the async router including routing, backpressure,
//! metrics, and multi-sink fan-out.

use std::sync::Arc;
use std::time::Duration;

use crossfire::mpsc as cf_mpsc;
use tell_protocol::{Batch, BatchBuilder, BatchType, SourceId};
use tell_routing::{RoutingTable, SinkId};
use tokio::sync::mpsc;
use tokio::time::timeout;

use crate::{Router, SinkHandle};

/// Helper to create a test batch
fn create_test_batch(source_id: &str, message_count: usize) -> Batch {
    let mut builder = BatchBuilder::new(BatchType::Event, SourceId::new(source_id));
    builder.set_workspace_id(1);

    for i in 0..message_count {
        let msg = format!("test message {}", i);
        builder.add(msg.as_bytes(), 1);
    }

    builder.finish()
}

/// Helper to create a routing table with default sink
fn create_single_sink_table() -> (RoutingTable, SinkId) {
    let mut table = RoutingTable::new();
    let sink_id = table.register_sink("default");
    table.set_default(vec![sink_id]);
    (table, sink_id)
}

/// Helper to create a routing table with multiple sinks
fn create_multi_sink_table() -> (RoutingTable, Vec<SinkId>) {
    let mut table = RoutingTable::new();
    let sink1 = table.register_sink("sink1");
    let sink2 = table.register_sink("sink2");
    let sink3 = table.register_sink("sink3");

    // Route "tcp" source to sink1 and sink2
    table.add_route(SourceId::new("tcp"), vec![sink1, sink2]);
    // Route "syslog" source to all three
    table.add_route(SourceId::new("syslog"), vec![sink1, sink2, sink3]);
    // Default to sink1
    table.set_default(vec![sink1]);

    (table, vec![sink1, sink2, sink3])
}

// ============================================================================
// Basic Router Tests
// ============================================================================

#[tokio::test]
async fn test_router_new() {
    let (table, _) = create_single_sink_table();
    let router = Router::new(table);

    assert_eq!(router.sink_count(), 0); // No sinks registered yet
}

#[tokio::test]
async fn test_register_sink() {
    let (table, sink_id) = create_single_sink_table();
    let mut router = Router::new(table);

    let (tx, _rx) = mpsc::channel(10);
    let handle = SinkHandle::new(sink_id, "test_sink", tx);
    router.register_sink(handle);

    assert_eq!(router.sink_count(), 1);
    assert!(router.has_sink(sink_id));
}

#[tokio::test]
async fn test_register_multiple_sinks() {
    let (table, sink_ids) = create_multi_sink_table();
    let mut router = Router::new(table);

    for (i, &sink_id) in sink_ids.iter().enumerate() {
        let (tx, _rx) = mpsc::channel(10);
        let handle = SinkHandle::new(sink_id, format!("sink_{}", i), tx);
        router.register_sink(handle);
    }

    assert_eq!(router.sink_count(), 3);
    for &sink_id in &sink_ids {
        assert!(router.has_sink(sink_id));
    }
}

#[tokio::test]
async fn test_unregister_sink() {
    let (table, sink_id) = create_single_sink_table();
    let mut router = Router::new(table);

    let (tx, _rx) = mpsc::channel(10);
    let handle = SinkHandle::new(sink_id, "test_sink", tx);
    router.register_sink(handle);

    assert!(router.has_sink(sink_id));

    let removed = router.unregister_sink(sink_id);
    assert!(removed.is_some());
    assert!(!router.has_sink(sink_id));
    assert_eq!(router.sink_count(), 0);
}

#[tokio::test]
async fn test_unregister_nonexistent_sink() {
    let (table, _) = create_single_sink_table();
    let mut router = Router::new(table);

    let removed = router.unregister_sink(SinkId::new(999));
    assert!(removed.is_none());
}

// ============================================================================
// Routing Tests
// ============================================================================

#[tokio::test]
async fn test_route_to_single_sink() {
    let (table, sink_id) = create_single_sink_table();
    let mut router = Router::new(table);

    let (tx, mut rx) = mpsc::channel(10);
    let handle = SinkHandle::new(sink_id, "test_sink", tx);
    router.register_sink(handle);

    let batch = create_test_batch("any_source", 5);
    let success_count = router.route(batch).await;

    assert_eq!(success_count, 1);

    // Verify batch was received
    let received = timeout(Duration::from_millis(100), rx.recv())
        .await
        .expect("timeout waiting for batch")
        .expect("channel closed");

    assert_eq!(received.count(), 5);
}

#[tokio::test]
async fn test_route_to_multiple_sinks() {
    let (table, sink_ids) = create_multi_sink_table();
    let mut router = Router::new(table);

    let mut receivers = vec![];
    for (i, &sink_id) in sink_ids.iter().enumerate() {
        let (tx, rx) = mpsc::channel(10);
        let handle = SinkHandle::new(sink_id, format!("sink_{}", i), tx);
        router.register_sink(handle);
        receivers.push(rx);
    }

    // Route to "tcp" source - should go to sink1 and sink2 (indices 0, 1)
    let batch = create_test_batch("tcp", 3);
    let success_count = router.route(batch).await;

    assert_eq!(success_count, 2);

    // Verify sink1 and sink2 received the batch
    let batch1 = timeout(Duration::from_millis(100), receivers[0].recv())
        .await
        .expect("timeout")
        .expect("channel closed");
    assert_eq!(batch1.count(), 3);

    let batch2 = timeout(Duration::from_millis(100), receivers[1].recv())
        .await
        .expect("timeout")
        .expect("channel closed");
    assert_eq!(batch2.count(), 3);

    // sink3 should not have received anything
    let result = timeout(Duration::from_millis(50), receivers[2].recv()).await;
    assert!(result.is_err()); // Timeout - nothing received
}

#[tokio::test]
async fn test_route_with_default_fallback() {
    let (table, sink_ids) = create_multi_sink_table();
    let mut router = Router::new(table);

    let mut receivers = vec![];
    for (i, &sink_id) in sink_ids.iter().enumerate() {
        let (tx, rx) = mpsc::channel(10);
        let handle = SinkHandle::new(sink_id, format!("sink_{}", i), tx);
        router.register_sink(handle);
        receivers.push(rx);
    }

    // Route to unknown source - should use default (sink1 only)
    let batch = create_test_batch("unknown_source", 2);
    let success_count = router.route(batch).await;

    assert_eq!(success_count, 1);

    // Only sink1 should have received it
    let batch1 = timeout(Duration::from_millis(100), receivers[0].recv())
        .await
        .expect("timeout")
        .expect("channel closed");
    assert_eq!(batch1.count(), 2);

    // Others should not have received anything
    assert!(
        timeout(Duration::from_millis(50), receivers[1].recv())
            .await
            .is_err()
    );
    assert!(
        timeout(Duration::from_millis(50), receivers[2].recv())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn test_route_no_matching_sinks() {
    let mut table = RoutingTable::new();
    table.register_sink("unused");
    // No default sinks, no routes

    let router = Router::new(table);

    let batch = create_test_batch("any_source", 1);
    let success_count = router.route(batch).await;

    assert_eq!(success_count, 0);

    let snapshot = router.metrics().snapshot();
    assert_eq!(snapshot.batches_received, 1);
    assert_eq!(snapshot.batches_dropped, 1);
    assert_eq!(snapshot.batches_routed, 0);
}

#[tokio::test]
async fn test_route_unregistered_sink() {
    let (table, _sink_id) = create_single_sink_table();
    let router = Router::new(table);

    // Don't register the sink - batch should be dropped
    let batch = create_test_batch("any", 1);
    let success_count = router.route(batch).await;

    assert_eq!(success_count, 0);

    let snapshot = router.metrics().snapshot();
    assert_eq!(snapshot.batches_received, 1);
    assert_eq!(snapshot.batches_dropped, 1);
    assert_eq!(snapshot.sink_sends_failed, 1);
}

// ============================================================================
// Backpressure Tests
// ============================================================================

#[tokio::test]
async fn test_backpressure_channel_full() {
    let (table, sink_id) = create_single_sink_table();
    let mut router = Router::new(table);

    // Create channel with capacity 1
    let (tx, rx) = mpsc::channel(1);
    let handle = SinkHandle::new(sink_id, "small_channel", tx);
    router.register_sink(handle);

    // Don't consume from rx - let channel fill up

    // First batch should succeed
    let batch1 = create_test_batch("src", 1);
    let success1 = router.route(batch1).await;
    assert_eq!(success1, 1);

    // Second batch should fail (backpressure)
    let batch2 = create_test_batch("src", 1);
    let success2 = router.route(batch2).await;
    assert_eq!(success2, 0);

    let snapshot = router.metrics().snapshot();
    assert_eq!(snapshot.batches_received, 2);
    assert_eq!(snapshot.batches_routed, 1);
    assert_eq!(snapshot.batches_dropped, 1);
    assert_eq!(snapshot.backpressure_events, 1);
    assert_eq!(snapshot.sink_sends_success, 1);
    assert_eq!(snapshot.sink_sends_failed, 1);

    // Keep rx alive to prevent channel close
    drop(rx);
}

#[tokio::test]
async fn test_backpressure_partial_delivery() {
    let (table, sink_ids) = create_multi_sink_table();
    let mut router = Router::new(table);

    // sink1: small capacity (will get full)
    let (tx1, rx1) = mpsc::channel(1);
    let handle1 = SinkHandle::new(sink_ids[0], "small", tx1);
    router.register_sink(handle1);

    // sink2: larger capacity
    let (tx2, mut rx2) = mpsc::channel(100);
    let handle2 = SinkHandle::new(sink_ids[1], "large", tx2);
    router.register_sink(handle2);

    // Route to "tcp" which goes to sink1 and sink2
    // First batch goes to both
    let batch1 = create_test_batch("tcp", 1);
    let success1 = router.route(batch1).await;
    assert_eq!(success1, 2);

    // Second batch - sink1 is full, but sink2 should still receive it
    let batch2 = create_test_batch("tcp", 1);
    let success2 = router.route(batch2).await;
    assert_eq!(success2, 1); // Only sink2 succeeded

    let snapshot = router.metrics().snapshot();
    assert_eq!(snapshot.backpressure_events, 1);
    assert_eq!(snapshot.batches_routed, 2); // Both batches routed to at least one sink

    // Verify sink2 received both batches
    let _ = rx2.recv().await.unwrap();
    let _ = rx2.recv().await.unwrap();

    drop(rx1);
}

#[tokio::test]
async fn test_route_blocking_waits_for_capacity() {
    let (table, sink_id) = create_single_sink_table();
    let mut router = Router::new(table);

    let (tx, mut rx) = mpsc::channel(1);
    let handle = SinkHandle::new(sink_id, "test", tx);
    router.register_sink(handle);

    // Send first batch (fills channel)
    let batch1 = create_test_batch("src", 1);
    router.route(batch1).await;

    // Use a larger channel to avoid the race condition
    // The test verifies that route_blocking actually waits
    let (notify_tx, _notify_rx) = mpsc::channel::<()>(1);

    // Spawn task that will:
    // 1. Wait a bit
    // 2. Consume from rx to make capacity
    // 3. Keep rx alive until we're done
    let consume_task = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        // Consume first batch to make room
        let first = rx.recv().await;
        // Signal that we consumed
        let _ = notify_tx.send(()).await;
        // Wait for second batch
        let second = rx.recv().await;
        (first, second)
    });

    // route_blocking should wait for capacity
    let batch2 = create_test_batch("src", 2);
    let success = router.route_blocking(batch2).await;
    assert_eq!(success, 1);

    // Wait for consume task to finish
    let (first, second) = consume_task.await.unwrap();

    // Verify both batches were received
    assert_eq!(first.unwrap().count(), 1);
    assert_eq!(second.unwrap().count(), 2);
}

// ============================================================================
// Closed Channel Tests
// ============================================================================

#[tokio::test]
async fn test_route_to_closed_channel() {
    let (table, sink_id) = create_single_sink_table();
    let mut router = Router::new(table);

    let (tx, rx) = mpsc::channel(10);
    let handle = SinkHandle::new(sink_id, "closing_sink", tx);
    router.register_sink(handle);

    // Close the channel by dropping the receiver
    drop(rx);

    let batch = create_test_batch("src", 1);
    let success_count = router.route(batch).await;

    assert_eq!(success_count, 0);

    let snapshot = router.metrics().snapshot();
    assert_eq!(snapshot.batches_received, 1);
    assert_eq!(snapshot.batches_dropped, 1);
    assert_eq!(snapshot.sink_sends_failed, 1);
}

#[tokio::test]
async fn test_partial_closed_channels() {
    let (table, sink_ids) = create_multi_sink_table();
    let mut router = Router::new(table);

    // sink1: will be closed
    let (tx1, rx1) = mpsc::channel(10);
    let handle1 = SinkHandle::new(sink_ids[0], "closing", tx1);
    router.register_sink(handle1);

    // sink2: stays open
    let (tx2, mut rx2) = mpsc::channel(10);
    let handle2 = SinkHandle::new(sink_ids[1], "open", tx2);
    router.register_sink(handle2);

    // Close sink1
    drop(rx1);

    // Route to "tcp" (goes to sink1 and sink2)
    let batch = create_test_batch("tcp", 5);
    let success_count = router.route(batch).await;

    assert_eq!(success_count, 1); // Only sink2 succeeded

    // Verify sink2 received the batch
    let received = rx2.recv().await.unwrap();
    assert_eq!(received.count(), 5);
}

// ============================================================================
// Metrics Tests
// ============================================================================

#[tokio::test]
async fn test_metrics_accuracy() {
    let (table, sink_id) = create_single_sink_table();
    let mut router = Router::new(table);

    let (tx, mut rx) = mpsc::channel(100);
    let handle = SinkHandle::new(sink_id, "test", tx);
    router.register_sink(handle);

    // Route 10 batches with varying sizes
    for i in 1..=10 {
        let batch = create_test_batch("src", i);
        router.route(batch).await;
    }

    let snapshot = router.metrics().snapshot();
    assert_eq!(snapshot.batches_received, 10);
    assert_eq!(snapshot.batches_routed, 10);
    assert_eq!(snapshot.batches_dropped, 0);
    // Sum of 1..=10 = 55
    assert_eq!(snapshot.messages_processed, 55);
    assert_eq!(snapshot.sink_sends_success, 10);
    assert_eq!(snapshot.sink_sends_failed, 0);
    assert_eq!(snapshot.backpressure_events, 0);

    // Consume all batches
    for _ in 0..10 {
        rx.recv().await.unwrap();
    }
}

#[tokio::test]
async fn test_metrics_with_fan_out() {
    let (table, sink_ids) = create_multi_sink_table();
    let mut router = Router::new(table);

    let mut receivers = vec![];
    for (i, &sink_id) in sink_ids.iter().enumerate() {
        let (tx, rx) = mpsc::channel(100);
        let handle = SinkHandle::new(sink_id, format!("sink_{}", i), tx);
        router.register_sink(handle);
        receivers.push(rx);
    }

    // Route to "syslog" which goes to all 3 sinks
    let batch = create_test_batch("syslog", 10);
    router.route(batch).await;

    let snapshot = router.metrics().snapshot();
    assert_eq!(snapshot.batches_received, 1);
    assert_eq!(snapshot.batches_routed, 1);
    assert_eq!(snapshot.sink_sends_success, 3); // Sent to 3 sinks
}

// ============================================================================
// Arc Fan-out Tests (Zero-copy verification)
// ============================================================================

#[tokio::test]
async fn test_arc_fanout_same_batch() {
    let (table, sink_ids) = create_multi_sink_table();
    let mut router = Router::new(table);

    let (tx1, mut rx1) = mpsc::channel(10);
    let handle1 = SinkHandle::new(sink_ids[0], "sink1", tx1);
    router.register_sink(handle1);

    let (tx2, mut rx2) = mpsc::channel(10);
    let handle2 = SinkHandle::new(sink_ids[1], "sink2", tx2);
    router.register_sink(handle2);

    // Route to "tcp" (goes to sink1 and sink2)
    let batch = create_test_batch("tcp", 5);
    router.route(batch).await;

    let received1 = rx1.recv().await.unwrap();
    let received2 = rx2.recv().await.unwrap();

    // Both receivers should have the same Arc (pointing to same batch)
    // We can verify this by checking Arc::ptr_eq
    assert!(Arc::ptr_eq(&received1, &received2));

    // Both should have the same content
    assert_eq!(received1.count(), received2.count());
    assert_eq!(received1.total_bytes(), received2.total_bytes());
}

// ============================================================================
// Run Loop Tests
// ============================================================================

#[tokio::test]
async fn test_router_run_processes_batches() {
    let (table, sink_id) = create_single_sink_table();
    let mut router = Router::new(table);

    let (sink_tx, mut sink_rx) = mpsc::channel(100);
    let handle = SinkHandle::new(sink_id, "test", sink_tx);
    router.register_sink(handle);

    let (source_tx, source_rx) = cf_mpsc::bounded_async(100);

    // Spawn router
    let router_handle = tokio::spawn(router.run(source_rx));

    // Send batches
    for i in 1..=5 {
        let batch = create_test_batch("src", i);
        source_tx.send(batch).await.unwrap();
    }

    // Close source channel to signal shutdown
    drop(source_tx);

    // Wait for router to finish
    timeout(Duration::from_secs(1), router_handle)
        .await
        .expect("router didn't shut down in time")
        .expect("router panicked");

    // Verify all batches were received
    let mut count = 0;
    while let Ok(Some(_)) = timeout(Duration::from_millis(100), sink_rx.recv()).await {
        count += 1;
    }
    assert_eq!(count, 5);
}

#[tokio::test]
async fn test_router_graceful_shutdown() {
    let (table, sink_id) = create_single_sink_table();
    let mut router = Router::new(table);

    let (sink_tx, _sink_rx) = mpsc::channel(100);
    let handle = SinkHandle::new(sink_id, "test", sink_tx);
    router.register_sink(handle);

    let (source_tx, source_rx) = cf_mpsc::bounded_async(100);

    // Spawn router
    let router_handle = tokio::spawn(router.run(source_rx));

    // Immediately close source
    drop(source_tx);

    // Router should shut down gracefully
    let result = timeout(Duration::from_secs(1), router_handle).await;
    assert!(result.is_ok());
}

// ============================================================================
// Edge Cases
// ============================================================================

#[tokio::test]
async fn test_empty_batch() {
    let (table, sink_id) = create_single_sink_table();
    let mut router = Router::new(table);

    let (tx, mut rx) = mpsc::channel(10);
    let handle = SinkHandle::new(sink_id, "test", tx);
    router.register_sink(handle);

    // Create empty batch
    let builder = BatchBuilder::new(BatchType::Event, SourceId::new("src"));
    let batch = builder.finish();
    assert!(batch.is_empty());

    let success = router.route(batch).await;
    assert_eq!(success, 1);

    // Empty batch should still be routed
    let received = rx.recv().await.unwrap();
    assert!(received.is_empty());
}

#[tokio::test]
async fn test_router_debug() {
    let (table, _) = create_single_sink_table();
    let router = Router::new(table);

    let debug = format!("{:?}", router);
    assert!(debug.contains("Router"));
    assert!(debug.contains("sink_count"));
    assert!(debug.contains("route_count"));
}

#[tokio::test]
async fn test_sparse_sink_registration() {
    let mut table = RoutingTable::new();
    let _sink0 = table.register_sink("sink0");
    let _sink1 = table.register_sink("sink1");
    let sink2 = table.register_sink("sink2"); // We'll only register this one
    table.set_default(vec![sink2]);

    let mut router = Router::new(table);

    // Only register sink2 (index 2), leaving 0 and 1 empty
    let (tx, mut rx) = mpsc::channel(10);
    let handle = SinkHandle::new(sink2, "sink2", tx);
    router.register_sink(handle);

    assert_eq!(router.sink_count(), 1);
    assert!(!router.has_sink(SinkId::new(0)));
    assert!(!router.has_sink(SinkId::new(1)));
    assert!(router.has_sink(sink2));

    // Routing should work
    let batch = create_test_batch("src", 1);
    let success = router.route(batch).await;
    assert_eq!(success, 1);

    rx.recv().await.unwrap();
}

#[tokio::test]
async fn test_high_sink_id() {
    let mut table = RoutingTable::new();
    let sink = table.register_sink("high");
    table.set_default(vec![sink]);

    let mut router = Router::new(table);

    // Manually create a handle with a high ID
    let high_id = SinkId::new(1000);
    let (tx, _rx) = mpsc::channel(10);
    let handle = SinkHandle::new(high_id, "high_sink", tx);
    router.register_sink(handle);

    assert!(router.has_sink(high_id));
    // Vector should have been extended
    assert!(!router.has_sink(SinkId::new(999)));
}

// ============================================================================
// Transformer Tests
// ============================================================================

#[tokio::test]
async fn test_router_without_transformers() {
    let (table, _sink_id) = create_single_sink_table();
    let router = Router::new(table);

    // By default, no transformers should be configured
    assert!(!router.has_transformers());
    assert!(router.transformer_names().is_empty());
}

#[tokio::test]
async fn test_router_with_noop_transformer() {
    use tell_transform::{Chain, NoopTransformer};

    let (table, sink_id) = create_single_sink_table();
    let mut router = Router::new(table);

    // Set up noop transformer
    let chain = Chain::new(vec![Box::new(NoopTransformer)]);
    router.set_transformers(chain);

    assert!(router.has_transformers());
    assert_eq!(router.transformer_names(), vec!["noop"]);

    // Register sink
    let (tx, mut rx) = mpsc::channel(10);
    let handle = SinkHandle::new(sink_id, "test", tx);
    router.register_sink(handle);

    // Route a batch - should pass through unchanged
    let batch = create_test_batch("src", 5);
    let original_count = batch.count();
    let success = router.route(batch).await;
    assert_eq!(success, 1);

    // Verify batch was received unchanged
    let received = rx.recv().await.unwrap();
    assert_eq!(received.count(), original_count);

    // Check transform metrics
    let snapshot = router.metrics().snapshot();
    assert_eq!(snapshot.transforms_success, 1);
    assert_eq!(snapshot.transforms_failed, 0);
    assert!(snapshot.transform_duration_ns > 0);
}

#[tokio::test]
async fn test_router_empty_chain_not_enabled() {
    use tell_transform::Chain;

    let (table, _) = create_single_sink_table();
    let mut router = Router::new(table);

    // Empty chain should not be enabled
    let chain = Chain::new(vec![]);
    router.set_transformers(chain);

    assert!(!router.has_transformers());
}

#[tokio::test]
async fn test_transform_metrics_accumulate() {
    use tell_transform::{Chain, NoopTransformer};

    let (table, sink_id) = create_single_sink_table();
    let mut router = Router::new(table);

    let chain = Chain::new(vec![Box::new(NoopTransformer)]);
    router.set_transformers(chain);

    let (tx, mut rx) = mpsc::channel(100);
    let handle = SinkHandle::new(sink_id, "test", tx);
    router.register_sink(handle);

    // Route multiple batches
    for i in 1..=5 {
        let batch = create_test_batch("src", i);
        router.route(batch).await;
    }

    let snapshot = router.metrics().snapshot();
    assert_eq!(snapshot.transforms_success, 5);
    assert_eq!(snapshot.transforms_failed, 0);
    assert!(snapshot.transform_duration_ns > 0);

    // Consume all batches
    for _ in 0..5 {
        rx.recv().await.unwrap();
    }
}
