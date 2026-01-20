//! Tests for atomic rotation system

use crate::util::atomic_rotation::{
    AtomicRotationSink, ChainMetrics, RotationConfig, RotationInterval,
};
use crate::util::chain_writer::PlainTextWriter;
use bytes::BytesMut;
use chrono::Local;
use std::time::Duration;
use tempfile::TempDir;

// ============================================================================
// RotationInterval Tests
// ============================================================================

#[test]
fn test_rotation_interval_date_formats() {
    let now = Local::now();

    let hourly = RotationInterval::Hourly.current_bucket(now);
    assert!(hourly.contains('/'), "hourly should have date/hour format");

    let daily = RotationInterval::Daily.current_bucket(now);
    assert!(daily.contains('-'), "daily should have date format");

    let monthly = RotationInterval::Monthly.current_bucket(now);
    assert!(
        monthly.contains('-'),
        "monthly should have year-month format"
    );
}

#[test]
fn test_rotation_interval_needs_rotation_same_bucket() {
    let now = Local::now();
    let bucket = RotationInterval::Hourly.current_bucket(now);

    // Same bucket should not need rotation
    assert!(!RotationInterval::Hourly.needs_rotation(&bucket, now));
}

#[test]
fn test_rotation_interval_needs_rotation_different_bucket() {
    let now = Local::now();
    let old_bucket = "2020-01-01/00"; // Ancient bucket

    // Different bucket should need rotation
    assert!(RotationInterval::Hourly.needs_rotation(old_bucket, now));
}

// ============================================================================
// RotationConfig Tests
// ============================================================================

#[test]
fn test_rotation_config_default() {
    let config = RotationConfig::default();

    assert_eq!(config.base_path.to_str().unwrap(), "logs");
    assert_eq!(config.rotation_interval, RotationInterval::Hourly);
    assert_eq!(config.file_prefix, "data");
    assert_eq!(config.buffer_size, 32 * 1024 * 1024);
    assert_eq!(config.queue_size, 1000);
    assert_eq!(config.flush_interval, Duration::from_millis(100));
}

#[test]
fn test_rotation_config_custom() {
    let config = RotationConfig {
        base_path: "/custom/path".into(),
        rotation_interval: RotationInterval::Daily,
        file_prefix: "events".into(),
        buffer_size: 16 * 1024 * 1024,
        queue_size: 500,
        flush_interval: Duration::from_millis(50),
        ..Default::default()
    };

    assert_eq!(config.base_path.to_str().unwrap(), "/custom/path");
    assert_eq!(config.rotation_interval, RotationInterval::Daily);
    assert_eq!(config.file_prefix, "events");
}

// ============================================================================
// ChainMetrics Tests
// ============================================================================

#[test]
fn test_chain_metrics_new() {
    let metrics = ChainMetrics::new();

    assert_eq!(
        metrics
            .batches_queued
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    assert_eq!(
        metrics
            .items_queued
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    assert_eq!(
        metrics
            .queue_full
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
}

#[test]
fn test_chain_metrics_record_queued() {
    let metrics = ChainMetrics::new();

    metrics.record_queued(100);
    metrics.record_queued(50);

    assert_eq!(
        metrics
            .batches_queued
            .load(std::sync::atomic::Ordering::Relaxed),
        2
    );
    assert_eq!(
        metrics
            .items_queued
            .load(std::sync::atomic::Ordering::Relaxed),
        150
    );
}

#[test]
fn test_chain_metrics_record_queue_full() {
    let metrics = ChainMetrics::new();

    metrics.record_queue_full();
    metrics.record_queue_full();

    assert_eq!(
        metrics
            .queue_full
            .load(std::sync::atomic::Ordering::Relaxed),
        2
    );
}

// ============================================================================
// AtomicRotationSink Tests
// ============================================================================

#[tokio::test]
async fn test_sink_creation() {
    let temp_dir = TempDir::new().unwrap();
    let config = RotationConfig {
        base_path: temp_dir.path().to_path_buf(),
        rotation_interval: RotationInterval::Hourly,
        file_prefix: "test".into(),
        buffer_size: 4096,
        queue_size: 10,
        flush_interval: Duration::from_millis(10),
        ..Default::default()
    };

    let writer = PlainTextWriter::new(4096);
    let sink = AtomicRotationSink::new(config, writer);

    assert_eq!(sink.workspace_count(), 0);
}

#[tokio::test]
async fn test_sink_submit_creates_workspace() {
    let temp_dir = TempDir::new().unwrap();
    let config = RotationConfig {
        base_path: temp_dir.path().to_path_buf(),
        rotation_interval: RotationInterval::Hourly,
        file_prefix: "test".into(),
        buffer_size: 4096,
        queue_size: 10,
        flush_interval: Duration::from_millis(10),
        ..Default::default()
    };

    let writer = PlainTextWriter::new(4096);
    let sink = AtomicRotationSink::new(config, writer);

    // Submit should create workspace
    let mut buffer = BytesMut::with_capacity(100);
    buffer.extend_from_slice(b"test data");

    let result = sink.submit("workspace_1", buffer, 1);
    assert!(result.is_ok());

    assert_eq!(sink.workspace_count(), 1);
}

#[tokio::test]
async fn test_sink_submit_multiple_workspaces() {
    let temp_dir = TempDir::new().unwrap();
    let config = RotationConfig {
        base_path: temp_dir.path().to_path_buf(),
        rotation_interval: RotationInterval::Hourly,
        file_prefix: "test".into(),
        buffer_size: 4096,
        queue_size: 10,
        flush_interval: Duration::from_millis(10),
        ..Default::default()
    };

    let writer = PlainTextWriter::new(4096);
    let sink = AtomicRotationSink::new(config, writer);

    // Submit to multiple workspaces
    for i in 0..5 {
        let mut buffer = BytesMut::with_capacity(100);
        buffer.extend_from_slice(format!("data for workspace {}", i).as_bytes());
        let result = sink.submit(&format!("workspace_{}", i), buffer, 1);
        assert!(result.is_ok());
    }

    assert_eq!(sink.workspace_count(), 5);
}

#[tokio::test]
async fn test_sink_submit_same_workspace_multiple_times() {
    let temp_dir = TempDir::new().unwrap();
    let config = RotationConfig {
        base_path: temp_dir.path().to_path_buf(),
        rotation_interval: RotationInterval::Hourly,
        file_prefix: "test".into(),
        buffer_size: 4096,
        queue_size: 100,
        flush_interval: Duration::from_millis(10),
        ..Default::default()
    };

    let writer = PlainTextWriter::new(4096);
    let sink = AtomicRotationSink::new(config, writer);

    // Submit multiple times to same workspace
    for i in 0..10 {
        let mut buffer = BytesMut::with_capacity(100);
        buffer.extend_from_slice(format!("batch {}", i).as_bytes());
        let result = sink.submit("workspace_1", buffer, 1);
        assert!(result.is_ok());
    }

    // Should still be just 1 workspace
    assert_eq!(sink.workspace_count(), 1);

    // Check metrics
    assert_eq!(
        sink.metrics()
            .batches_submitted
            .load(std::sync::atomic::Ordering::Relaxed),
        10
    );
}

#[tokio::test]
async fn test_sink_get_put_buffer() {
    let temp_dir = TempDir::new().unwrap();
    let config = RotationConfig {
        base_path: temp_dir.path().to_path_buf(),
        buffer_size: 1024,
        ..Default::default()
    };

    let writer = PlainTextWriter::new(1024);
    let sink = AtomicRotationSink::new(config, writer);

    // Get buffer from pool
    let buffer = sink.get_buffer();
    assert!(buffer.capacity() >= 1024);

    // Put buffer back
    sink.put_buffer(buffer);
}

#[tokio::test]
async fn test_sink_metrics() {
    let temp_dir = TempDir::new().unwrap();
    let config = RotationConfig {
        base_path: temp_dir.path().to_path_buf(),
        rotation_interval: RotationInterval::Hourly,
        file_prefix: "test".into(),
        buffer_size: 4096,
        queue_size: 100,
        flush_interval: Duration::from_millis(10),
        ..Default::default()
    };

    let writer = PlainTextWriter::new(4096);
    let sink = AtomicRotationSink::new(config, writer);

    // Submit some data
    let mut buffer = BytesMut::with_capacity(100);
    buffer.extend_from_slice(b"test");
    sink.submit("ws1", buffer, 5).unwrap();

    let mut buffer = BytesMut::with_capacity(100);
    buffer.extend_from_slice(b"test");
    sink.submit("ws2", buffer, 10).unwrap();

    let metrics = sink.metrics();
    assert_eq!(
        metrics
            .batches_submitted
            .load(std::sync::atomic::Ordering::Relaxed),
        2
    );
    assert_eq!(
        metrics
            .items_submitted
            .load(std::sync::atomic::Ordering::Relaxed),
        15
    );
    assert_eq!(
        metrics
            .workspace_count
            .load(std::sync::atomic::Ordering::Relaxed),
        2
    );
}

#[tokio::test]
async fn test_sink_stop() {
    let temp_dir = TempDir::new().unwrap();
    let config = RotationConfig {
        base_path: temp_dir.path().to_path_buf(),
        rotation_interval: RotationInterval::Hourly,
        file_prefix: "test".into(),
        buffer_size: 4096,
        queue_size: 100,
        flush_interval: Duration::from_millis(10),
        ..Default::default()
    };

    let writer = PlainTextWriter::new(4096);
    let sink = AtomicRotationSink::new(config, writer);

    // Create some workspaces
    for i in 0..3 {
        let mut buffer = BytesMut::with_capacity(100);
        buffer.extend_from_slice(b"test");
        sink.submit(&format!("ws{}", i), buffer, 1).unwrap();
    }

    assert_eq!(sink.workspace_count(), 3);

    // Stop should clear all
    sink.stop().await;

    assert_eq!(sink.workspace_count(), 0);
}

#[tokio::test]
async fn test_sink_creates_directory_structure() {
    let temp_dir = TempDir::new().unwrap();
    let config = RotationConfig {
        base_path: temp_dir.path().to_path_buf(),
        rotation_interval: RotationInterval::Hourly,
        file_prefix: "data".into(),
        buffer_size: 4096,
        queue_size: 10,
        flush_interval: Duration::from_millis(10),
        ..Default::default()
    };

    let writer = PlainTextWriter::new(4096);
    let sink = AtomicRotationSink::new(config, writer);

    // Submit to create workspace
    let mut buffer = BytesMut::with_capacity(100);
    buffer.extend_from_slice(b"hello world\n");
    sink.submit("my_workspace", buffer, 1).unwrap();

    // Give writer task time to process
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Stop to flush
    sink.stop().await;

    // Verify directory structure was created
    let workspace_dir = temp_dir.path().join("my_workspace");
    assert!(workspace_dir.exists(), "workspace directory should exist");
}

#[tokio::test]
async fn test_sink_writes_data_to_file() {
    let temp_dir = TempDir::new().unwrap();
    let config = RotationConfig {
        base_path: temp_dir.path().to_path_buf(),
        rotation_interval: RotationInterval::Daily,
        file_prefix: "data".into(),
        buffer_size: 4096,
        queue_size: 10,
        flush_interval: Duration::from_millis(10),
        ..Default::default()
    };

    let writer = PlainTextWriter::new(4096);
    let sink = AtomicRotationSink::new(config, writer);

    // Submit data
    let test_data = b"hello world from test\n";
    let mut buffer = BytesMut::with_capacity(100);
    buffer.extend_from_slice(test_data);
    sink.submit("test_ws", buffer, 1).unwrap();

    // Give writer task time to process and flush
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Stop to ensure final flush
    sink.stop().await;

    // Find and read the output file
    let workspace_dir = temp_dir.path().join("test_ws");
    assert!(workspace_dir.exists());

    // Find the data file (in date subdirectory)
    let mut found_data = false;
    for entry in walkdir::WalkDir::new(&workspace_dir)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        if entry.file_type().is_file() && entry.path().to_string_lossy().contains("data") {
            let content = std::fs::read(entry.path()).unwrap();
            if content == test_data {
                found_data = true;
                break;
            }
        }
    }

    assert!(found_data, "should find written data in output file");
}

// ============================================================================
// Backpressure Tests
// ============================================================================

#[tokio::test]
async fn test_sink_backpressure_returns_buffer() {
    let temp_dir = TempDir::new().unwrap();
    let config = RotationConfig {
        base_path: temp_dir.path().to_path_buf(),
        rotation_interval: RotationInterval::Hourly,
        file_prefix: "test".into(),
        buffer_size: 4096,
        queue_size: 2,                           // Very small queue
        flush_interval: Duration::from_secs(10), // Long flush interval
        ..Default::default()
    };

    let writer = PlainTextWriter::new(4096);
    let sink = AtomicRotationSink::new(config, writer);

    // Fill the queue
    for _ in 0..10 {
        let mut buffer = BytesMut::with_capacity(100);
        buffer.extend_from_slice(b"test");
        let _ = sink.submit("ws1", buffer, 1);
    }

    // Some should have been rejected
    let rejected = sink
        .metrics()
        .batches_rejected
        .load(std::sync::atomic::Ordering::Relaxed);
    assert!(rejected > 0, "should have some rejections with small queue");
}
