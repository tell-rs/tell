//! Stdout sink tests

use std::sync::Arc;

use tell_protocol::{Batch, BatchBuilder, BatchType, SourceId};
use tokio::sync::mpsc;

use super::{MetricsSnapshot, StdoutConfig, StdoutSink, StdoutSinkMetrics};

/// Helper to create a test batch
fn create_test_batch(source_id: &str, message_count: usize) -> Batch {
    let mut builder = BatchBuilder::new(BatchType::Event, SourceId::new(source_id));
    builder.set_workspace_id(42);

    for i in 0..message_count {
        let msg = format!("test message {}", i);
        builder.add(msg.as_bytes(), 1);
    }

    builder.finish()
}

// ============================================================================
// StdoutConfig Tests
// ============================================================================

#[test]
fn test_config_default() {
    let config = StdoutConfig::default();

    assert!(config.color);
    assert!(!config.show_batch_headers);
    assert_eq!(config.max_messages, 0);
}

#[test]
fn test_config_no_color() {
    let config = StdoutConfig::no_color();

    assert!(!config.color);
    assert!(!config.show_batch_headers);
    assert_eq!(config.max_messages, 0);
}

#[test]
fn test_config_with_headers() {
    let config = StdoutConfig::with_headers();

    assert!(config.color);
    assert!(config.show_batch_headers);
    assert_eq!(config.max_messages, 0);
}

// ============================================================================
// StdoutSinkMetrics Tests
// ============================================================================

#[test]
fn test_metrics_new() {
    let metrics = StdoutSinkMetrics::new();
    let snapshot = metrics.snapshot();

    assert_eq!(snapshot.batches_received, 0);
    assert_eq!(snapshot.messages_received, 0);
    assert_eq!(snapshot.bytes_received, 0);
}

#[test]
fn test_metrics_record_batch() {
    let metrics = StdoutSinkMetrics::new();

    metrics.record_batch(100, 5000);
    metrics.record_batch(200, 10000);

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.batches_received, 2);
    assert_eq!(snapshot.messages_received, 300);
    assert_eq!(snapshot.bytes_received, 15000);
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
    let s1 = MetricsSnapshot {
        batches_received: 10,
        messages_received: 100,
        bytes_received: 5000,
    };

    let s2 = MetricsSnapshot {
        batches_received: 10,
        messages_received: 100,
        bytes_received: 5000,
    };

    let s3 = MetricsSnapshot {
        batches_received: 20,
        messages_received: 100,
        bytes_received: 5000,
    };

    assert_eq!(s1, s2);
    assert_ne!(s1, s3);
}

// ============================================================================
// StdoutSink Tests
// ============================================================================

#[tokio::test]
async fn test_sink_creation() {
    let (_tx, rx) = mpsc::channel::<Arc<Batch>>(10);
    let sink = StdoutSink::new(rx);

    assert_eq!(sink.name(), "stdout");
}

#[tokio::test]
async fn test_sink_with_config() {
    let (_tx, rx) = mpsc::channel::<Arc<Batch>>(10);
    let config = StdoutConfig::with_headers();
    let sink = StdoutSink::with_config(rx, config);

    assert_eq!(sink.name(), "stdout");
}

#[tokio::test]
async fn test_sink_with_name_and_config() {
    let (_tx, rx) = mpsc::channel::<Arc<Batch>>(10);
    let config = StdoutConfig::default();
    let sink = StdoutSink::with_name_and_config(rx, "custom_stdout", config);

    assert_eq!(sink.name(), "custom_stdout");
}

#[tokio::test]
async fn test_sink_run_empty_channel() {
    let (tx, rx) = mpsc::channel::<Arc<Batch>>(10);
    let sink = StdoutSink::new(rx);

    // Close channel immediately
    drop(tx);

    let snapshot = sink.run().await;

    assert_eq!(snapshot.batches_received, 0);
    assert_eq!(snapshot.messages_received, 0);
    assert_eq!(snapshot.bytes_received, 0);
}

#[tokio::test]
async fn test_sink_run_single_batch() {
    let (tx, rx) = mpsc::channel::<Arc<Batch>>(10);
    let sink = StdoutSink::new(rx);

    let batch = Arc::new(create_test_batch("test_source", 5));
    tx.send(batch).await.unwrap();
    drop(tx);

    let snapshot = sink.run().await;

    assert_eq!(snapshot.batches_received, 1);
    assert_eq!(snapshot.messages_received, 5);
    assert!(snapshot.bytes_received > 0);
}

#[tokio::test]
async fn test_sink_run_multiple_batches() {
    let (tx, rx) = mpsc::channel::<Arc<Batch>>(10);
    let sink = StdoutSink::new(rx);

    for i in 1..=5 {
        let batch = Arc::new(create_test_batch("test_source", i * 2));
        tx.send(batch).await.unwrap();
    }
    drop(tx);

    let snapshot = sink.run().await;

    assert_eq!(snapshot.batches_received, 5);
    // Sum of 2, 4, 6, 8, 10 = 30
    assert_eq!(snapshot.messages_received, 30);
}

#[tokio::test]
async fn test_sink_metrics_during_run() {
    let (tx, rx) = mpsc::channel::<Arc<Batch>>(10);
    let sink = StdoutSink::new(rx);

    // Initially metrics should be zero
    assert_eq!(sink.metrics().snapshot().batches_received, 0);

    let batch = Arc::new(create_test_batch("test", 10));
    tx.send(batch).await.unwrap();
    drop(tx);

    sink.run().await;
}

#[tokio::test]
async fn test_sink_with_headers_config() {
    let (tx, rx) = mpsc::channel::<Arc<Batch>>(10);
    let config = StdoutConfig::with_headers();
    let sink = StdoutSink::with_config(rx, config);

    let batch = Arc::new(create_test_batch("headers_test", 15));
    tx.send(batch).await.unwrap();
    drop(tx);

    let snapshot = sink.run().await;
    assert_eq!(snapshot.batches_received, 1);
    assert_eq!(snapshot.messages_received, 15);
}

#[tokio::test]
async fn test_sink_with_no_color_config() {
    let (tx, rx) = mpsc::channel::<Arc<Batch>>(10);
    let config = StdoutConfig::no_color();
    let sink = StdoutSink::with_config(rx, config);

    let batch = Arc::new(create_test_batch("no_color_test", 3));
    tx.send(batch).await.unwrap();
    drop(tx);

    let snapshot = sink.run().await;
    assert_eq!(snapshot.batches_received, 1);
    assert_eq!(snapshot.messages_received, 3);
}

#[tokio::test]
async fn test_sink_concurrent_senders() {
    let (tx, rx) = mpsc::channel::<Arc<Batch>>(100);
    let sink = StdoutSink::new(rx);

    let tx1 = tx.clone();
    let tx2 = tx.clone();

    let handle1 = tokio::spawn(async move {
        for _ in 0..5 {
            let batch = Arc::new(create_test_batch("sender1", 2));
            tx1.send(batch).await.unwrap();
        }
    });

    let handle2 = tokio::spawn(async move {
        for _ in 0..5 {
            let batch = Arc::new(create_test_batch("sender2", 3));
            tx2.send(batch).await.unwrap();
        }
    });

    handle1.await.unwrap();
    handle2.await.unwrap();
    drop(tx);

    let snapshot = sink.run().await;

    assert_eq!(snapshot.batches_received, 10);
    // 5 batches with 2 messages + 5 batches with 3 messages = 10 + 15 = 25
    assert_eq!(snapshot.messages_received, 25);
}

#[tokio::test]
async fn test_sink_different_batch_types() {
    let (tx, rx) = mpsc::channel::<Arc<Batch>>(10);
    let sink = StdoutSink::new(rx);

    // Create batches with different types
    let mut builder1 = BatchBuilder::new(BatchType::Event, SourceId::new("events"));
    builder1.add(b"event data", 1);
    let batch1 = Arc::new(builder1.finish());

    let mut builder2 = BatchBuilder::new(BatchType::Log, SourceId::new("logs"));
    builder2.add(b"log data", 1);
    let batch2 = Arc::new(builder2.finish());

    tx.send(batch1).await.unwrap();
    tx.send(batch2).await.unwrap();
    drop(tx);

    let snapshot = sink.run().await;
    assert_eq!(snapshot.batches_received, 2);
}
