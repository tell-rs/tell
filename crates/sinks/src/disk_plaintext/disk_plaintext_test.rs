use super::*;
use cdp_protocol::{BatchBuilder, BatchType, SourceId};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use tempfile::TempDir;

// =============================================================================
// Config tests
// =============================================================================

#[test]
fn test_config_defaults() {
    let config = DiskPlaintextConfig::default();
    assert_eq!(config.path, PathBuf::from("logs"));
    assert_eq!(config.buffer_size, 8 * 1024 * 1024);
    assert_eq!(config.queue_size, 10000);
    assert!(!config.compression);
    assert!(config.metrics_enabled);
}

#[test]
fn test_config_with_compression() {
    let config = DiskPlaintextConfig::default().with_compression();
    assert!(config.compression);
}

#[test]
fn test_config_with_path() {
    let config = DiskPlaintextConfig::default().with_path("/custom/path");
    assert_eq!(config.path, PathBuf::from("/custom/path"));
}

#[test]
fn test_config_with_hourly_rotation() {
    let config = DiskPlaintextConfig::default().with_hourly_rotation();
    assert!(matches!(config.rotation_interval, RotationInterval::Hourly));
}

#[test]
fn test_config_with_daily_rotation() {
    let config = DiskPlaintextConfig::default().with_daily_rotation();
    assert!(matches!(config.rotation_interval, RotationInterval::Daily));
}

#[test]
fn test_config_chained_builders() {
    let config = DiskPlaintextConfig::default()
        .with_path("/data/logs")
        .with_compression()
        .with_hourly_rotation();

    assert_eq!(config.path, PathBuf::from("/data/logs"));
    assert!(config.compression);
    assert!(matches!(config.rotation_interval, RotationInterval::Hourly));
}

// =============================================================================
// Metrics tests
// =============================================================================

#[test]
fn test_metrics_new() {
    let metrics = DiskPlaintextMetrics::new();
    let snapshot = metrics.snapshot();

    assert_eq!(snapshot.batches_received, 0);
    assert_eq!(snapshot.batches_written, 0);
    assert_eq!(snapshot.lines_written, 0);
    assert_eq!(snapshot.bytes_written, 0);
    assert_eq!(snapshot.write_errors, 0);
    assert_eq!(snapshot.queue_full, 0);
}

#[test]
fn test_metrics_record_batch() {
    let metrics = DiskPlaintextMetrics::new();

    metrics.record_batch(10, 1000);
    metrics.record_batch(20, 2000);

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.batches_written, 2);
    assert_eq!(snapshot.lines_written, 30);
    assert_eq!(snapshot.bytes_written, 3000);
}

#[test]
fn test_metrics_record_received() {
    let metrics = DiskPlaintextMetrics::new();

    metrics.record_received();
    metrics.record_received();
    metrics.record_received();

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.batches_received, 3);
}

#[test]
fn test_metrics_record_error() {
    let metrics = DiskPlaintextMetrics::new();

    metrics.record_error();
    metrics.record_error();

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.write_errors, 2);
}

#[test]
fn test_metrics_record_queue_full() {
    let metrics = DiskPlaintextMetrics::new();

    metrics.record_queue_full();

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.queue_full, 1);
}

#[test]
fn test_metrics_snapshot_default() {
    let snapshot = MetricsSnapshot::default();
    assert_eq!(snapshot.batches_received, 0);
    assert_eq!(snapshot.batches_written, 0);
    assert_eq!(snapshot.lines_written, 0);
    assert_eq!(snapshot.bytes_written, 0);
    assert_eq!(snapshot.write_errors, 0);
    assert_eq!(snapshot.queue_full, 0);
}

// =============================================================================
// Sink creation tests
// =============================================================================

#[test]
fn test_sink_creation() {
    let (_, rx) = mpsc::channel::<Arc<Batch>>(10);
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let config = DiskPlaintextConfig::default().with_path(temp_dir.path());
    let sink = DiskPlaintextSink::new(config, rx);

    assert_eq!(sink.name(), "disk_plaintext");
    assert_eq!(sink.metrics().snapshot().batches_received, 0);
}

#[test]
fn test_sink_with_custom_name() {
    let (_, rx) = mpsc::channel::<Arc<Batch>>(10);
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let config = DiskPlaintextConfig::default().with_path(temp_dir.path());
    let sink = DiskPlaintextSink::with_name(config, rx, "custom_plaintext");

    assert_eq!(sink.name(), "custom_plaintext");
}

#[test]
fn test_sink_with_compression() {
    let (_, rx) = mpsc::channel::<Arc<Batch>>(10);
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let config = DiskPlaintextConfig::default()
        .with_path(temp_dir.path())
        .with_compression();
    let sink = DiskPlaintextSink::new(config, rx);

    assert_eq!(sink.name(), "disk_plaintext");
}

// =============================================================================
// Sink run tests
// =============================================================================

#[tokio::test]
async fn test_sink_run_empty_channel() {
    let (tx, rx) = mpsc::channel::<Arc<Batch>>(10);
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let config = DiskPlaintextConfig::default().with_path(temp_dir.path());
    let sink = DiskPlaintextSink::new(config, rx);

    // Close channel immediately
    drop(tx);

    // Should complete without error
    let snapshot = sink.run().await;
    assert_eq!(snapshot.batches_received, 0);
}

#[tokio::test]
async fn test_sink_writes_batch() {
    let (tx, rx) = mpsc::channel::<Arc<Batch>>(10);
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let config = DiskPlaintextConfig::default().with_path(temp_dir.path());
    let sink = DiskPlaintextSink::new(config, rx);

    // Create and send a batch
    let source_id = SourceId::new("test");
    let mut builder = BatchBuilder::new(BatchType::Event, source_id);
    builder.set_workspace_id(42);
    builder.add(b"test message content", 1);
    let batch = Arc::new(builder.finish());

    tx.send(batch).await.expect("failed to send");
    drop(tx);

    let snapshot = sink.run().await;

    assert_eq!(snapshot.batches_received, 1);
    assert_eq!(snapshot.batches_written, 1);
    assert_eq!(snapshot.lines_written, 1);
    assert!(snapshot.bytes_written > 0);
}

#[tokio::test]
async fn test_sink_multiple_batches() {
    let (tx, rx) = mpsc::channel::<Arc<Batch>>(10);
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let config = DiskPlaintextConfig::default().with_path(temp_dir.path());
    let sink = DiskPlaintextSink::new(config, rx);

    // Send multiple batches
    for i in 0..5 {
        let source_id = SourceId::new("test");
        let mut builder = BatchBuilder::new(BatchType::Log, source_id);
        builder.set_workspace_id(100);
        builder.add(format!("log message {}", i).as_bytes(), 1);
        let batch = Arc::new(builder.finish());
        tx.send(batch).await.expect("failed to send");
    }
    drop(tx);

    let snapshot = sink.run().await;

    assert_eq!(snapshot.batches_received, 5);
    assert_eq!(snapshot.batches_written, 5);
    assert_eq!(snapshot.lines_written, 5);
}

#[tokio::test]
async fn test_sink_multiple_messages_per_batch() {
    let (tx, rx) = mpsc::channel::<Arc<Batch>>(10);
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let config = DiskPlaintextConfig::default().with_path(temp_dir.path());
    let sink = DiskPlaintextSink::new(config, rx);

    let source_id = SourceId::new("test");
    let mut builder = BatchBuilder::new(BatchType::Event, source_id);
    builder.set_workspace_id(42);
    builder.add(b"message 1", 1);
    builder.add(b"message 2", 1);
    builder.add(b"message 3", 1);
    let batch = Arc::new(builder.finish());

    tx.send(batch).await.expect("failed to send");
    drop(tx);

    let snapshot = sink.run().await;

    assert_eq!(snapshot.batches_received, 1);
    assert_eq!(snapshot.batches_written, 1);
    assert_eq!(snapshot.lines_written, 3);
}

#[tokio::test]
async fn test_sink_skips_missing_workspace_id() {
    let (tx, rx) = mpsc::channel::<Arc<Batch>>(10);
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let config = DiskPlaintextConfig::default().with_path(temp_dir.path());
    let sink = DiskPlaintextSink::new(config, rx);

    // Create batch without setting workspace_id (defaults to 0)
    let source_id = SourceId::new("test");
    let mut builder = BatchBuilder::new(BatchType::Event, source_id);
    // workspace_id is 0 by default
    builder.add(b"test message", 1);
    let batch = Arc::new(builder.finish());

    tx.send(batch).await.expect("failed to send");
    drop(tx);

    let snapshot = sink.run().await;

    assert_eq!(snapshot.batches_received, 1);
    assert_eq!(snapshot.batches_written, 0); // Skipped due to missing workspace
    assert_eq!(snapshot.write_errors, 1);
}

#[tokio::test]
async fn test_sink_multiple_workspaces() {
    let (tx, rx) = mpsc::channel::<Arc<Batch>>(10);
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let config = DiskPlaintextConfig::default().with_path(temp_dir.path());
    let sink = DiskPlaintextSink::new(config, rx);

    // Send batches for different workspaces
    for workspace_id in [100, 200, 300] {
        let source_id = SourceId::new("test");
        let mut builder = BatchBuilder::new(BatchType::Event, source_id);
        builder.set_workspace_id(workspace_id);
        builder.add(format!("msg for workspace {}", workspace_id).as_bytes(), 1);
        let batch = Arc::new(builder.finish());
        tx.send(batch).await.expect("failed to send");
    }
    drop(tx);

    let snapshot = sink.run().await;

    assert_eq!(snapshot.batches_received, 3);
    assert_eq!(snapshot.batches_written, 3);
}

// =============================================================================
// Format tests
// =============================================================================

#[test]
fn test_format_source_ip_v4() {
    let ip = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 100));
    let formatted = format_source_ip(ip);
    assert_eq!(formatted, "192.168.1.100");
}

#[test]
fn test_format_source_ip_v6() {
    let ip = IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1));
    let formatted = format_source_ip(ip);
    assert_eq!(formatted, "2001:db8::1");
}

#[test]
fn test_format_source_ip_v4_mapped() {
    // IPv4-mapped IPv6 address ::ffff:192.168.1.1
    let ip = IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0xffff, 0xc0a8, 0x0101));
    let formatted = format_source_ip(ip);
    assert_eq!(formatted, "192.168.1.1");
}

// =============================================================================
// Integration tests with file verification
// =============================================================================

#[tokio::test]
async fn test_sink_creates_directory_structure() {
    let (tx, rx) = mpsc::channel::<Arc<Batch>>(10);
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let config = DiskPlaintextConfig::default().with_path(temp_dir.path());
    let sink = DiskPlaintextSink::new(config, rx);

    let source_id = SourceId::new("test");
    let mut builder = BatchBuilder::new(BatchType::Event, source_id);
    builder.set_workspace_id(12345);
    builder.add(b"test data", 1);
    let batch = Arc::new(builder.finish());

    tx.send(batch).await.expect("failed to send");
    drop(tx);

    let _ = sink.run().await;

    // Check that workspace directory was created
    let workspace_dir = temp_dir.path().join("12345");
    assert!(workspace_dir.exists(), "workspace directory should exist");
}

#[tokio::test]
async fn test_sink_writes_readable_content() {
    let (tx, rx) = mpsc::channel::<Arc<Batch>>(10);
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let config = DiskPlaintextConfig::default().with_path(temp_dir.path());
    let sink = DiskPlaintextSink::new(config, rx);

    let source_id = SourceId::new("test");
    let mut builder = BatchBuilder::new(BatchType::Event, source_id);
    builder.set_workspace_id(999);
    builder.set_source_ip(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
    builder.add(b"hello world", 1);
    let batch = Arc::new(builder.finish());

    tx.send(batch).await.expect("failed to send");
    drop(tx);

    let _ = sink.run().await;

    // Find and read the log file
    let workspace_dir = temp_dir.path().join("999");
    assert!(workspace_dir.exists());

    // Walk the directory to find .log files
    let mut found_log = false;
    for entry in walkdir::WalkDir::new(&workspace_dir)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        if entry
            .path()
            .extension()
            .map(|e| e == "log")
            .unwrap_or(false)
        {
            let content = std::fs::read_to_string(entry.path()).expect("failed to read log");
            assert!(content.contains("[EVENT]"), "should contain batch type");
            assert!(
                content.contains("workspace=999"),
                "should contain workspace"
            );
            assert!(
                content.contains("source_ip=10.0.0.1"),
                "should contain source IP"
            );
            assert!(content.contains("hello world"), "should contain payload");
            found_log = true;
            break;
        }
    }

    assert!(found_log, "should have found a log file");
}

#[tokio::test]
async fn test_sink_escapes_special_characters() {
    let (tx, rx) = mpsc::channel::<Arc<Batch>>(10);
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let config = DiskPlaintextConfig::default().with_path(temp_dir.path());
    let sink = DiskPlaintextSink::new(config, rx);

    let source_id = SourceId::new("test");
    let mut builder = BatchBuilder::new(BatchType::Log, source_id);
    builder.set_workspace_id(888);
    // Message with newlines and tabs
    builder.add(b"line1\nline2\tindented\r\nwindows", 1);
    let batch = Arc::new(builder.finish());

    tx.send(batch).await.expect("failed to send");
    drop(tx);

    let _ = sink.run().await;

    // Find and read the log file
    let workspace_dir = temp_dir.path().join("888");

    for entry in walkdir::WalkDir::new(&workspace_dir)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        if entry
            .path()
            .extension()
            .map(|e| e == "log")
            .unwrap_or(false)
        {
            let content = std::fs::read_to_string(entry.path()).expect("failed to read log");
            // Should be escaped, not literal newlines
            assert!(
                content.contains("\\n"),
                "newlines should be escaped: {}",
                content
            );
            assert!(
                content.contains("\\t"),
                "tabs should be escaped: {}",
                content
            );
            assert!(
                content.contains("\\r"),
                "carriage returns should be escaped: {}",
                content
            );
            // The actual file should only have one line per message
            let lines: Vec<&str> = content.lines().collect();
            assert_eq!(lines.len(), 1, "should be single line per message");
            break;
        }
    }
}

#[tokio::test]
async fn test_sink_handles_binary_data() {
    let (tx, rx) = mpsc::channel::<Arc<Batch>>(10);
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let config = DiskPlaintextConfig::default().with_path(temp_dir.path());
    let sink = DiskPlaintextSink::new(config, rx);

    let source_id = SourceId::new("test");
    let mut builder = BatchBuilder::new(BatchType::Event, source_id);
    builder.set_workspace_id(777);
    // Binary data that's not valid UTF-8
    builder.add(&[0x00, 0x01, 0x02, 0xff, 0xfe, 0xfd], 1);
    let batch = Arc::new(builder.finish());

    tx.send(batch).await.expect("failed to send");
    drop(tx);

    let _ = sink.run().await;

    // Find and read the log file
    let workspace_dir = temp_dir.path().join("777");

    for entry in walkdir::WalkDir::new(&workspace_dir)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        if entry
            .path()
            .extension()
            .map(|e| e == "log")
            .unwrap_or(false)
        {
            let content = std::fs::read_to_string(entry.path()).expect("failed to read log");
            // Should contain hex representation
            assert!(
                content.contains("0x"),
                "binary data should be hex encoded: {}",
                content
            );
            assert!(
                content.contains("000102fffefd"),
                "should contain hex bytes: {}",
                content
            );
            break;
        }
    }
}

#[tokio::test]
async fn test_sink_concurrent_senders() {
    let (tx, rx) = mpsc::channel::<Arc<Batch>>(100);
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let config = DiskPlaintextConfig::default().with_path(temp_dir.path());
    let sink = DiskPlaintextSink::new(config, rx);

    // Spawn multiple senders
    let mut handles = Vec::new();
    for i in 0..5 {
        let tx_clone = tx.clone();
        let handle = tokio::spawn(async move {
            for j in 0..10 {
                let source_id = SourceId::new("test");
                let mut builder = BatchBuilder::new(BatchType::Event, source_id);
                builder.set_workspace_id(42);
                builder.add(format!("sender {} msg {}", i, j).as_bytes(), 1);
                let batch = Arc::new(builder.finish());
                tx_clone.send(batch).await.expect("failed to send");
            }
        });
        handles.push(handle);
    }

    // Wait for all senders
    for handle in handles {
        handle.await.expect("sender failed");
    }
    drop(tx);

    let snapshot = sink.run().await;

    assert_eq!(snapshot.batches_received, 50);
    assert_eq!(snapshot.batches_written, 50);
}
