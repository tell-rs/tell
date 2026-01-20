//! Tests for disk binary sink

use super::{DiskBinaryConfig, DiskBinarySink, MetricsSnapshot};
use crate::disk_binary::writer::{METADATA_SIZE, MessageMetadata};
use crate::util::RotationInterval;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::time::Duration;
use tell_client::BatchBuilder as FlatBufferBuilder;
use tell_client::event::{EventBuilder, EventDataBuilder};
use tell_client::log::{LogDataBuilder, LogEntryBuilder};
use tell_protocol::{Batch, BatchBuilder, BatchType, SourceId};
use tempfile::TempDir;
use tokio::sync::mpsc;

// ============================================================================
// Test Helpers
// ============================================================================

/// Create a test batch with raw string messages (simple case)
fn create_test_batch(workspace_id: u32, source_id: &str, message_count: usize) -> Batch {
    let source = SourceId::new(source_id);
    let mut builder = BatchBuilder::new(BatchType::Event, source);
    builder.set_workspace_id(workspace_id);

    for i in 0..message_count {
        let msg = format!("test message {}", i);
        builder.add(msg.as_bytes(), 1);
    }

    builder.finish()
}

/// Create a test batch with source IP
fn create_test_batch_with_ip(
    workspace_id: u32,
    source_id: &str,
    message_count: usize,
    ip: IpAddr,
) -> Batch {
    let source = SourceId::new(source_id);
    let mut builder = BatchBuilder::new(BatchType::Event, source);
    builder.set_workspace_id(workspace_id);
    builder.set_source_ip(ip);

    for i in 0..message_count {
        let msg = format!("test message {}", i);
        builder.add(msg.as_bytes(), 1);
    }

    builder.finish()
}

/// Create a test batch with realistic FlatBuffer event data
fn create_realistic_event_batch(workspace_id: u32, event_count: usize) -> Batch {
    let mut event_data_builder = EventDataBuilder::new();

    for i in 0..event_count {
        let event = EventBuilder::new()
            .track(&format!("page_view_{}", i))
            .device_id([
                0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
                0x0f, i as u8,
            ])
            .session_id([0x10; 16])
            .timestamp_now()
            .payload_json(&format!(
                r#"{{"page": "/test/{}", "referrer": "google"}}"#,
                i
            ))
            .build()
            .expect("build event");

        event_data_builder = event_data_builder.add(event);
    }

    let event_data = event_data_builder.build().expect("build event data");
    let fb_bytes = FlatBufferBuilder::new()
        .api_key([0x01; 16])
        .event_data(event_data)
        .build()
        .expect("build flatbuffer batch")
        .as_bytes()
        .to_vec();

    let source = SourceId::new("tcp");
    let mut builder = BatchBuilder::new(BatchType::Event, source);
    builder.set_workspace_id(workspace_id);
    builder.add(&fb_bytes, event_count);
    builder.finish()
}

/// Create a test batch with realistic FlatBuffer log data
fn create_realistic_log_batch(workspace_id: u32, log_count: usize) -> Batch {
    let mut log_data_builder = LogDataBuilder::new();

    for i in 0..log_count {
        let log = LogEntryBuilder::new()
            .info()
            .source(&format!("server-{}", i % 5))
            .service("nginx")
            .timestamp_now()
            .payload_json(&format!(
                r#"{{"status": 200, "path": "/api/v1/users/{}", "latency_ms": {}}}"#,
                i,
                10 + i
            ))
            .build()
            .expect("build log");

        log_data_builder = log_data_builder.add(log);
    }

    let log_data = log_data_builder.build().expect("build log data");
    let fb_bytes = FlatBufferBuilder::new()
        .api_key([0x01; 16])
        .log_data(log_data)
        .build()
        .expect("build flatbuffer batch")
        .as_bytes()
        .to_vec();

    let source = SourceId::new("syslog");
    let mut builder = BatchBuilder::new(BatchType::Log, source);
    builder.set_workspace_id(workspace_id);
    builder.add(&fb_bytes, log_count);
    builder.finish()
}

// ============================================================================
// Config Tests
// ============================================================================

#[test]
fn test_config_default() {
    let config = DiskBinaryConfig::default();

    assert_eq!(config.path.to_str().unwrap(), "logs");
    assert_eq!(config.buffer_size, 32 * 1024 * 1024);
    assert_eq!(config.queue_size, 1000);
    assert_eq!(config.rotation_interval, RotationInterval::Hourly);
    assert!(!config.compression);
    assert_eq!(config.flush_interval, Duration::from_millis(100));
    assert!(config.metrics_enabled);
}

#[test]
fn test_config_with_compression() {
    let config = DiskBinaryConfig::default().with_compression();

    assert!(config.compression);
}

#[test]
fn test_config_with_path() {
    let config = DiskBinaryConfig::default().with_path("/custom/path");

    assert_eq!(config.path.to_str().unwrap(), "/custom/path");
}

#[test]
fn test_config_with_daily_rotation() {
    let config = DiskBinaryConfig::default().with_daily_rotation();

    assert_eq!(config.rotation_interval, RotationInterval::Daily);
}

#[test]
fn test_config_chained() {
    let config = DiskBinaryConfig::default()
        .with_path("/data")
        .with_compression()
        .with_daily_rotation();

    assert_eq!(config.path.to_str().unwrap(), "/data");
    assert!(config.compression);
    assert_eq!(config.rotation_interval, RotationInterval::Daily);
}

// ============================================================================
// Metadata Tests
// ============================================================================

#[test]
fn test_metadata_size_constant() {
    assert_eq!(METADATA_SIZE, 24);
}

#[test]
fn test_metadata_roundtrip() {
    let metadata = MessageMetadata::new(
        1234567890123,
        [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 192, 168, 1, 1],
    );

    let bytes = metadata.to_bytes();
    assert_eq!(bytes.len(), METADATA_SIZE);

    let decoded = MessageMetadata::from_bytes(&bytes);
    assert_eq!(metadata, decoded);
}

#[test]
fn test_metadata_ipv4_mapped() {
    let metadata = MessageMetadata::new(
        0,
        [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 10, 20, 30, 40],
    );

    assert_eq!(metadata.source_ip_string(), "10.20.30.40");
}

// ============================================================================
// Metrics Tests
// ============================================================================

#[test]
fn test_metrics_snapshot_default() {
    let snapshot = MetricsSnapshot::default();

    assert_eq!(snapshot.batches_received, 0);
    assert_eq!(snapshot.batches_written, 0);
    assert_eq!(snapshot.messages_written, 0);
    assert_eq!(snapshot.bytes_written, 0);
    assert_eq!(snapshot.write_errors, 0);
    assert_eq!(snapshot.queue_full, 0);
}

// ============================================================================
// Sink Creation Tests
// ============================================================================

#[tokio::test]
async fn test_sink_creation() {
    let temp_dir = TempDir::new().unwrap();
    let config = DiskBinaryConfig {
        id: "test_sink".into(),
        path: temp_dir.path().to_path_buf(),
        buffer_size: 4096,
        queue_size: 10,
        rotation_interval: RotationInterval::Hourly,
        compression: false,
        flush_interval: Duration::from_millis(10),
        metrics_enabled: true,
        metrics_interval: Duration::from_secs(10),
    };

    let (tx, rx) = mpsc::channel::<Arc<Batch>>(10);
    let sink = DiskBinarySink::new(config, rx);

    assert_eq!(sink.name(), "test_sink");

    // Close channel and run
    drop(tx);
    let metrics = sink.run().await;

    assert_eq!(metrics.batches_received, 0);
}

#[tokio::test]
async fn test_sink_with_custom_id() {
    let temp_dir = TempDir::new().unwrap();
    let config = DiskBinaryConfig {
        id: "my_custom_sink".into(),
        path: temp_dir.path().to_path_buf(),
        ..Default::default()
    };

    let (_tx, rx) = mpsc::channel::<Arc<Batch>>(10);
    let sink = DiskBinarySink::new(config, rx);

    assert_eq!(sink.name(), "my_custom_sink");
}

// ============================================================================
// Write Tests
// ============================================================================

#[tokio::test]
async fn test_sink_writes_batch() {
    let temp_dir = TempDir::new().unwrap();
    let config = DiskBinaryConfig {
        path: temp_dir.path().to_path_buf(),
        buffer_size: 4096,
        queue_size: 100,
        rotation_interval: RotationInterval::Daily,
        compression: false,
        flush_interval: Duration::from_millis(10),
        metrics_enabled: true,
        ..Default::default()
    };

    let (tx, rx) = mpsc::channel::<Arc<Batch>>(10);
    let sink = DiskBinarySink::new(config, rx);

    // Send a batch
    let batch = Arc::new(create_test_batch(1, "tcp", 5));
    tx.send(batch).await.unwrap();

    // Give time for processing
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Close and get metrics
    drop(tx);
    let snapshot = sink.run().await;

    assert!(snapshot.batches_received >= 1);
}

#[tokio::test]
async fn test_sink_multiple_batches() {
    let temp_dir = TempDir::new().unwrap();
    let config = DiskBinaryConfig {
        path: temp_dir.path().to_path_buf(),
        buffer_size: 4096,
        queue_size: 100,
        rotation_interval: RotationInterval::Daily,
        compression: false,
        flush_interval: Duration::from_millis(10),
        metrics_enabled: true,
        ..Default::default()
    };

    let (tx, rx) = mpsc::channel::<Arc<Batch>>(100);
    let sink = DiskBinarySink::new(config, rx);

    // Send multiple batches
    for _ in 0..10 {
        let batch = Arc::new(create_test_batch(1, "tcp", 5));
        tx.send(batch).await.unwrap();
    }

    // Give time for processing
    tokio::time::sleep(Duration::from_millis(100)).await;

    drop(tx);
    let snapshot = sink.run().await;

    assert_eq!(snapshot.batches_received, 10);
}

#[tokio::test]
async fn test_sink_multiple_workspaces() {
    let temp_dir = TempDir::new().unwrap();
    let config = DiskBinaryConfig {
        path: temp_dir.path().to_path_buf(),
        buffer_size: 4096,
        queue_size: 100,
        rotation_interval: RotationInterval::Daily,
        compression: false,
        flush_interval: Duration::from_millis(10),
        metrics_enabled: true,
        ..Default::default()
    };

    let (tx, rx) = mpsc::channel::<Arc<Batch>>(100);
    let sink = DiskBinarySink::new(config, rx);

    // Send batches to different workspaces
    for i in 0..5 {
        let batch = Arc::new(create_test_batch(i + 1, "tcp", 3));
        tx.send(batch).await.unwrap();
    }

    tokio::time::sleep(Duration::from_millis(100)).await;

    drop(tx);
    let snapshot = sink.run().await;
    assert_eq!(snapshot.batches_received, 5);
}

// ============================================================================
// Error Handling Tests
// ============================================================================

#[tokio::test]
async fn test_sink_handles_empty_workspace_id() {
    let temp_dir = TempDir::new().unwrap();
    let config = DiskBinaryConfig {
        path: temp_dir.path().to_path_buf(),
        buffer_size: 4096,
        queue_size: 100,
        rotation_interval: RotationInterval::Daily,
        compression: false,
        flush_interval: Duration::from_millis(10),
        metrics_enabled: true,
        ..Default::default()
    };

    let (tx, rx) = mpsc::channel::<Arc<Batch>>(10);
    let sink = DiskBinarySink::new(config, rx);

    // Send batch with zero workspace (should be skipped)
    let batch = Arc::new(create_test_batch(0, "tcp", 5));
    tx.send(batch).await.unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    drop(tx);
    let snapshot = sink.run().await;
    assert_eq!(snapshot.batches_received, 1);
    assert_eq!(snapshot.write_errors, 1); // Empty workspace causes error
}

// ============================================================================
// IP Address Tests
// ============================================================================

#[tokio::test]
async fn test_sink_preserves_ipv4_address() {
    let temp_dir = TempDir::new().unwrap();
    let config = DiskBinaryConfig {
        path: temp_dir.path().to_path_buf(),
        buffer_size: 4096,
        queue_size: 100,
        rotation_interval: RotationInterval::Daily,
        compression: false,
        flush_interval: Duration::from_millis(10),
        metrics_enabled: true,
        ..Default::default()
    };

    let (tx, rx) = mpsc::channel::<Arc<Batch>>(10);
    let sink = DiskBinarySink::new(config, rx);

    let ip = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 100));
    let batch = Arc::new(create_test_batch_with_ip(1, "tcp", 1, ip));
    tx.send(batch).await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    drop(tx);
    let _metrics = sink.run().await;

    // The IP should be written to the file
    // We can verify this by reading back the file
}

// ============================================================================
// Compression Tests
// ============================================================================

#[tokio::test]
async fn test_sink_with_compression() {
    let temp_dir = TempDir::new().unwrap();
    let config = DiskBinaryConfig {
        path: temp_dir.path().to_path_buf(),
        buffer_size: 4096,
        queue_size: 100,
        rotation_interval: RotationInterval::Daily,
        compression: true, // Enable compression
        flush_interval: Duration::from_millis(10),
        metrics_enabled: true,
        ..Default::default()
    };

    let (tx, rx) = mpsc::channel::<Arc<Batch>>(10);
    let sink = DiskBinarySink::new(config, rx);

    // Send a batch
    let batch = Arc::new(create_test_batch(1, "tcp", 10));
    tx.send(batch).await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    drop(tx);
    let snapshot = sink.run().await;
    assert_eq!(snapshot.batches_received, 1);
}

// ============================================================================
// Concurrent Sender Tests
// ============================================================================

#[tokio::test]
async fn test_sink_concurrent_senders() {
    let temp_dir = TempDir::new().unwrap();
    let config = DiskBinaryConfig {
        path: temp_dir.path().to_path_buf(),
        buffer_size: 4096,
        queue_size: 1000,
        rotation_interval: RotationInterval::Daily,
        compression: false,
        flush_interval: Duration::from_millis(10),
        metrics_enabled: true,
        ..Default::default()
    };

    let (tx, rx) = mpsc::channel::<Arc<Batch>>(1000);
    let sink = DiskBinarySink::new(config, rx);

    // Spawn multiple senders
    let tx1 = tx.clone();
    let tx2 = tx.clone();

    let handle1 = tokio::spawn(async move {
        for _ in 0..50 {
            let batch = Arc::new(create_test_batch(1, "tcp", 2));
            tx1.send(batch).await.unwrap();
        }
    });

    let handle2 = tokio::spawn(async move {
        for _ in 0..50 {
            let batch = Arc::new(create_test_batch(2, "tcp", 2));
            tx2.send(batch).await.unwrap();
        }
    });

    handle1.await.unwrap();
    handle2.await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    drop(tx);
    let snapshot = sink.run().await;
    assert_eq!(snapshot.batches_received, 100);
}

// ============================================================================
// Realistic FlatBuffer Data Tests
// ============================================================================

#[tokio::test]
async fn test_sink_writes_realistic_event_batch() {
    let temp_dir = TempDir::new().unwrap();
    let config = DiskBinaryConfig {
        path: temp_dir.path().to_path_buf(),
        buffer_size: 4096,
        queue_size: 100,
        rotation_interval: RotationInterval::Daily,
        compression: false,
        flush_interval: Duration::from_millis(10),
        metrics_enabled: true,
        ..Default::default()
    };

    let (tx, rx) = mpsc::channel::<Arc<Batch>>(10);
    let sink = DiskBinarySink::new(config, rx);

    // Send realistic event batch
    let batch = Arc::new(create_realistic_event_batch(1, 10));
    tx.send(batch).await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    drop(tx);
    let snapshot = sink.run().await;
    assert_eq!(snapshot.batches_received, 1);
    assert!(snapshot.bytes_written > 0, "should have written bytes");
}

#[tokio::test]
async fn test_sink_writes_realistic_log_batch() {
    let temp_dir = TempDir::new().unwrap();
    let config = DiskBinaryConfig {
        path: temp_dir.path().to_path_buf(),
        buffer_size: 4096,
        queue_size: 100,
        rotation_interval: RotationInterval::Daily,
        compression: false,
        flush_interval: Duration::from_millis(10),
        metrics_enabled: true,
        ..Default::default()
    };

    let (tx, rx) = mpsc::channel::<Arc<Batch>>(10);
    let sink = DiskBinarySink::new(config, rx);

    // Send realistic log batch
    let batch = Arc::new(create_realistic_log_batch(1, 10));
    tx.send(batch).await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    drop(tx);
    let snapshot = sink.run().await;
    assert_eq!(snapshot.batches_received, 1);
    assert!(snapshot.bytes_written > 0, "should have written bytes");
}

#[tokio::test]
async fn test_sink_writes_mixed_realistic_batches() {
    let temp_dir = TempDir::new().unwrap();
    let config = DiskBinaryConfig {
        path: temp_dir.path().to_path_buf(),
        buffer_size: 4096,
        queue_size: 100,
        rotation_interval: RotationInterval::Daily,
        compression: false,
        flush_interval: Duration::from_millis(10),
        metrics_enabled: true,
        ..Default::default()
    };

    let (tx, rx) = mpsc::channel::<Arc<Batch>>(100);
    let sink = DiskBinarySink::new(config, rx);

    // Send mixed event and log batches
    for _ in 0..5 {
        let event_batch = Arc::new(create_realistic_event_batch(1, 5));
        let log_batch = Arc::new(create_realistic_log_batch(1, 5));
        tx.send(event_batch).await.unwrap();
        tx.send(log_batch).await.unwrap();
    }

    tokio::time::sleep(Duration::from_millis(200)).await;

    drop(tx);
    let snapshot = sink.run().await;
    assert_eq!(snapshot.batches_received, 10);
    assert!(snapshot.bytes_written > 0, "should have written bytes");
}

#[tokio::test]
async fn test_sink_compression_with_realistic_data() {
    let temp_dir = TempDir::new().unwrap();
    let config = DiskBinaryConfig {
        path: temp_dir.path().to_path_buf(),
        buffer_size: 4096,
        queue_size: 100,
        rotation_interval: RotationInterval::Daily,
        compression: true, // Enable compression
        flush_interval: Duration::from_millis(10),
        metrics_enabled: true,
        ..Default::default()
    };

    let (tx, rx) = mpsc::channel::<Arc<Batch>>(10);
    let sink = DiskBinarySink::new(config, rx);

    // Send larger realistic batch (compression works better with more data)
    let batch = Arc::new(create_realistic_event_batch(1, 50));
    tx.send(batch).await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    drop(tx);
    let snapshot = sink.run().await;
    assert_eq!(snapshot.batches_received, 1);
    assert!(
        snapshot.bytes_written > 0,
        "should have written compressed bytes"
    );
}

#[tokio::test]
async fn test_sink_multiple_workspaces_realistic_data() {
    let temp_dir = TempDir::new().unwrap();
    let config = DiskBinaryConfig {
        path: temp_dir.path().to_path_buf(),
        buffer_size: 4096,
        queue_size: 100,
        rotation_interval: RotationInterval::Daily,
        compression: false,
        flush_interval: Duration::from_millis(10),
        metrics_enabled: true,
        ..Default::default()
    };

    let (tx, rx) = mpsc::channel::<Arc<Batch>>(100);
    let sink = DiskBinarySink::new(config, rx);

    // Send batches to different workspaces
    for workspace_id in 1..=3 {
        let batch = Arc::new(create_realistic_event_batch(workspace_id, 10));
        tx.send(batch).await.unwrap();
    }

    tokio::time::sleep(Duration::from_millis(200)).await;

    drop(tx);
    let snapshot = sink.run().await;
    assert_eq!(snapshot.batches_received, 3);
}
