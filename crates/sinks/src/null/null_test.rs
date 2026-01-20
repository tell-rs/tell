//! Tests for the null sink

use super::{MetricsSnapshot, NullSink, NullSinkMetrics};
use std::sync::Arc;
use tell_protocol::{Batch, BatchBuilder, BatchType, SourceId};
use tokio::sync::mpsc;

/// Helper to create a test batch with the given message count
fn create_test_batch(message_count: usize) -> Batch {
    let source_id = SourceId::new("test");
    let mut builder = BatchBuilder::new(BatchType::Event, source_id);

    for i in 0..message_count {
        let msg = format!("test message {}", i);
        builder.add(msg.as_bytes(), 1);
    }

    builder.finish()
}

// ============================================================================
// Metrics Tests
// ============================================================================

#[test]
fn test_metrics_new() {
    let metrics = NullSinkMetrics::new();
    let snapshot = metrics.snapshot();

    assert_eq!(snapshot.batches_received, 0);
    assert_eq!(snapshot.messages_received, 0);
    assert_eq!(snapshot.bytes_received, 0);
}

#[test]
fn test_metrics_record_batch() {
    let metrics = NullSinkMetrics::new();

    metrics.record_batch(100, 5000);
    metrics.record_batch(200, 10000);

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.batches_received, 2);
    assert_eq!(snapshot.messages_received, 300);
    assert_eq!(snapshot.bytes_received, 15000);
}

#[test]
fn test_metrics_accessors() {
    let metrics = NullSinkMetrics::new();
    metrics.record_batch(50, 2500);

    assert_eq!(metrics.batches_received(), 1);
    assert_eq!(metrics.messages_received(), 50);
    assert_eq!(metrics.bytes_received(), 2500);
}

#[test]
fn test_metrics_reset() {
    let metrics = NullSinkMetrics::new();

    metrics.record_batch(100, 5000);
    metrics.reset();

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.batches_received, 0);
    assert_eq!(snapshot.messages_received, 0);
    assert_eq!(snapshot.bytes_received, 0);
}

#[test]
fn test_metrics_snapshot_default() {
    let snapshot = MetricsSnapshot::default();

    assert_eq!(snapshot.batches_received, 0);
    assert_eq!(snapshot.messages_received, 0);
    assert_eq!(snapshot.bytes_received, 0);
}

#[test]
fn test_metrics_snapshot_equality() {
    let snapshot1 = MetricsSnapshot {
        batches_received: 10,
        messages_received: 100,
        bytes_received: 5000,
    };

    let snapshot2 = MetricsSnapshot {
        batches_received: 10,
        messages_received: 100,
        bytes_received: 5000,
    };

    let snapshot3 = MetricsSnapshot {
        batches_received: 20,
        messages_received: 100,
        bytes_received: 5000,
    };

    assert_eq!(snapshot1, snapshot2);
    assert_ne!(snapshot1, snapshot3);
}

// ============================================================================
// Sink Tests
// ============================================================================

#[tokio::test]
async fn test_null_sink_receives_batches() {
    let (tx, rx) = mpsc::channel::<Arc<Batch>>(10);
    let sink = NullSink::new(rx);

    // Send some batches
    let batch1 = Arc::new(create_test_batch(10));
    let batch2 = Arc::new(create_test_batch(20));

    tx.send(batch1).await.unwrap();
    tx.send(batch2).await.unwrap();

    // Close channel to stop sink
    drop(tx);

    // Run sink and get final metrics
    let snapshot = sink.run().await;

    assert_eq!(snapshot.batches_received, 2);
    assert_eq!(snapshot.messages_received, 30);
}

#[tokio::test]
async fn test_null_sink_handles_empty_channel() {
    let (tx, rx) = mpsc::channel::<Arc<Batch>>(10);

    // Drop sender immediately
    drop(tx);

    let sink = NullSink::new(rx);
    let snapshot = sink.run().await;

    assert_eq!(snapshot.batches_received, 0);
}

#[tokio::test]
async fn test_null_sink_with_name() {
    let (_tx, rx) = mpsc::channel::<Arc<Batch>>(10);
    let sink = NullSink::with_name(rx, "benchmark_sink");

    assert_eq!(sink.name(), "benchmark_sink");
}

#[tokio::test]
async fn test_null_sink_default_name() {
    let (_tx, rx) = mpsc::channel::<Arc<Batch>>(10);
    let sink = NullSink::new(rx);

    assert_eq!(sink.name(), "null");
}

#[tokio::test]
async fn test_null_sink_with_callback() {
    let (tx, rx) = mpsc::channel::<Arc<Batch>>(10);
    let sink = NullSink::new(rx);

    let batch = Arc::new(create_test_batch(5));
    tx.send(batch).await.unwrap();
    drop(tx);

    let mut callback_count = 0;
    let metrics = sink
        .run_with_callback(|_batch| {
            callback_count += 1;
        })
        .await;

    assert_eq!(callback_count, 1);
    assert_eq!(metrics.batches_received, 1);
}

#[tokio::test]
async fn test_null_sink_callback_receives_batch_data() {
    let (tx, rx) = mpsc::channel::<Arc<Batch>>(10);
    let sink = NullSink::new(rx);

    let batch = Arc::new(create_test_batch(7));
    tx.send(batch).await.unwrap();
    drop(tx);

    let mut observed_count = 0;
    let _metrics = sink
        .run_with_callback(|batch| {
            observed_count = batch.count();
        })
        .await;

    assert_eq!(observed_count, 7);
}

#[tokio::test]
async fn test_null_sink_concurrent_sends() {
    let (tx, rx) = mpsc::channel::<Arc<Batch>>(100);
    let sink = NullSink::new(rx);

    // Spawn multiple senders
    let tx1 = tx.clone();
    let tx2 = tx.clone();

    let handle1 = tokio::spawn(async move {
        for _ in 0..10 {
            let batch = Arc::new(create_test_batch(5));
            tx1.send(batch).await.unwrap();
        }
    });

    let handle2 = tokio::spawn(async move {
        for _ in 0..10 {
            let batch = Arc::new(create_test_batch(5));
            tx2.send(batch).await.unwrap();
        }
    });

    handle1.await.unwrap();
    handle2.await.unwrap();
    drop(tx);

    let snapshot = sink.run().await;

    assert_eq!(snapshot.batches_received, 20);
    assert_eq!(snapshot.messages_received, 100);
}

#[tokio::test]
async fn test_null_sink_tracks_bytes() {
    let (tx, rx) = mpsc::channel::<Arc<Batch>>(10);
    let sink = NullSink::new(rx);

    let batch = Arc::new(create_test_batch(10));
    let expected_bytes = batch.total_bytes() as u64;

    tx.send(batch).await.unwrap();
    drop(tx);

    let snapshot = sink.run().await;
    assert_eq!(snapshot.bytes_received, expected_bytes);
}

#[tokio::test]
async fn test_null_sink_metrics_accessor() {
    let (tx, rx) = mpsc::channel::<Arc<Batch>>(10);
    let sink = NullSink::new(rx);

    // Metrics should be accessible before running
    let initial_snapshot = sink.metrics().snapshot();
    assert_eq!(initial_snapshot.batches_received, 0);

    drop(tx);
    let _ = sink.run().await;
}

#[tokio::test]
async fn test_null_sink_large_batch() {
    let (tx, rx) = mpsc::channel::<Arc<Batch>>(10);
    let sink = NullSink::new(rx);

    // Create a large batch
    let batch = Arc::new(create_test_batch(10000));
    tx.send(batch).await.unwrap();
    drop(tx);

    let snapshot = sink.run().await;

    assert_eq!(snapshot.batches_received, 1);
    assert_eq!(snapshot.messages_received, 10000);
}

#[tokio::test]
async fn test_null_sink_many_small_batches() {
    let (tx, rx) = mpsc::channel::<Arc<Batch>>(1000);
    let sink = NullSink::new(rx);

    // Send many small batches
    for _ in 0..100 {
        let batch = Arc::new(create_test_batch(1));
        tx.send(batch).await.unwrap();
    }
    drop(tx);

    let snapshot = sink.run().await;

    assert_eq!(snapshot.batches_received, 100);
    assert_eq!(snapshot.messages_received, 100);
}
