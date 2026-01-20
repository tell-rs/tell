//! Tests for the Parquet sink

use super::*;
use std::net::Ipv4Addr;
use tell_protocol::{BatchBuilder, BatchType, SourceId};
use tempfile::tempdir;

// =============================================================================
// Configuration Tests
// =============================================================================

#[test]
fn test_config_defaults() {
    let config = ParquetConfig::default();
    assert_eq!(config.path, PathBuf::from("parquet"));
    assert_eq!(config.rotation_interval, RotationInterval::Hourly);
    assert_eq!(config.compression, Compression::Snappy);
    assert_eq!(config.row_group_size, 100_000);
    assert_eq!(config.buffer_size, 10_000);
    assert_eq!(config.flush_interval, Duration::from_secs(60));
}

#[test]
fn test_config_with_path() {
    let config = ParquetConfig::default().with_path("/data/parquet");
    assert_eq!(config.path, PathBuf::from("/data/parquet"));
}

#[test]
fn test_config_with_zstd() {
    let config = ParquetConfig::default().with_zstd();
    assert_eq!(config.compression, Compression::Zstd);
}

#[test]
fn test_config_with_lz4() {
    let config = ParquetConfig::default().with_lz4();
    assert_eq!(config.compression, Compression::Lz4);
}

#[test]
fn test_config_uncompressed() {
    let config = ParquetConfig::default().uncompressed();
    assert_eq!(config.compression, Compression::None);
}

#[test]
fn test_config_with_daily_rotation() {
    let config = ParquetConfig::default().with_daily_rotation();
    assert_eq!(config.rotation_interval, RotationInterval::Daily);
}

#[test]
fn test_config_with_buffer_size() {
    let config = ParquetConfig::default().with_buffer_size(5000);
    assert_eq!(config.buffer_size, 5000);
}

#[test]
fn test_config_chaining() {
    let config = ParquetConfig::default()
        .with_path("/custom/path")
        .with_zstd()
        .with_daily_rotation()
        .with_buffer_size(1000);

    assert_eq!(config.path, PathBuf::from("/custom/path"));
    assert_eq!(config.compression, Compression::Zstd);
    assert_eq!(config.rotation_interval, RotationInterval::Daily);
    assert_eq!(config.buffer_size, 1000);
}

// =============================================================================
// Rotation Interval Tests
// =============================================================================

#[test]
fn test_rotation_interval_equality() {
    assert_eq!(RotationInterval::Hourly, RotationInterval::Hourly);
    assert_eq!(RotationInterval::Daily, RotationInterval::Daily);
    assert_ne!(RotationInterval::Hourly, RotationInterval::Daily);
}

// =============================================================================
// Metrics Tests
// =============================================================================

#[test]
fn test_metrics_new() {
    let metrics = ParquetSinkMetrics::new();
    assert_eq!(metrics.batches_received.load(Ordering::Relaxed), 0);
    assert_eq!(metrics.event_rows_written.load(Ordering::Relaxed), 0);
    assert_eq!(metrics.log_rows_written.load(Ordering::Relaxed), 0);
    assert_eq!(metrics.bytes_written.load(Ordering::Relaxed), 0);
    assert_eq!(metrics.files_created.load(Ordering::Relaxed), 0);
    assert_eq!(metrics.write_errors.load(Ordering::Relaxed), 0);
    assert_eq!(metrics.decode_errors.load(Ordering::Relaxed), 0);
}

#[test]
fn test_metrics_record_batch() {
    let metrics = ParquetSinkMetrics::new();
    metrics.record_batch();
    metrics.record_batch();
    metrics.record_batch();
    assert_eq!(metrics.batches_received.load(Ordering::Relaxed), 3);
}

#[test]
fn test_metrics_record_event_rows() {
    let metrics = ParquetSinkMetrics::new();
    metrics.record_event_rows(100);
    metrics.record_event_rows(50);
    assert_eq!(metrics.event_rows_written.load(Ordering::Relaxed), 150);
}

#[test]
fn test_metrics_record_log_rows() {
    let metrics = ParquetSinkMetrics::new();
    metrics.record_log_rows(200);
    metrics.record_log_rows(75);
    assert_eq!(metrics.log_rows_written.load(Ordering::Relaxed), 275);
}

#[test]
fn test_metrics_record_bytes() {
    let metrics = ParquetSinkMetrics::new();
    metrics.record_bytes(5000);
    metrics.record_bytes(3000);
    assert_eq!(metrics.bytes_written.load(Ordering::Relaxed), 8000);
}

#[test]
fn test_metrics_record_file_created() {
    let metrics = ParquetSinkMetrics::new();
    metrics.record_file_created();
    metrics.record_file_created();
    assert_eq!(metrics.files_created.load(Ordering::Relaxed), 2);
}

#[test]
fn test_metrics_record_error() {
    let metrics = ParquetSinkMetrics::new();
    metrics.record_error();
    assert_eq!(metrics.write_errors.load(Ordering::Relaxed), 1);
}

#[test]
fn test_metrics_record_decode_error() {
    let metrics = ParquetSinkMetrics::new();
    metrics.record_decode_error();
    metrics.record_decode_error();
    assert_eq!(metrics.decode_errors.load(Ordering::Relaxed), 2);
}

#[test]
fn test_metrics_snapshot() {
    let metrics = ParquetSinkMetrics::new();
    metrics.record_batch();
    metrics.record_event_rows(100);
    metrics.record_log_rows(50);
    metrics.record_bytes(5000);
    metrics.record_file_created();
    metrics.record_error();
    metrics.record_decode_error();

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.batches_received, 1);
    assert_eq!(snapshot.event_rows_written, 100);
    assert_eq!(snapshot.log_rows_written, 50);
    assert_eq!(snapshot.bytes_written, 5000);
    assert_eq!(snapshot.files_created, 1);
    assert_eq!(snapshot.write_errors, 1);
    assert_eq!(snapshot.decode_errors, 1);
}

// =============================================================================
// Sink Creation Tests
// =============================================================================

#[test]
fn test_sink_creation() {
    let (_, rx) = mpsc::channel::<Arc<Batch>>(10);
    let config = ParquetConfig::default();
    let sink = ParquetSink::new(config.clone(), rx);

    assert_eq!(sink.config().path, config.path);
    assert_eq!(sink.config().compression, config.compression);
}

#[test]
fn test_sink_metrics_reference() {
    let (_, rx) = mpsc::channel::<Arc<Batch>>(10);
    let config = ParquetConfig::default();
    let sink = ParquetSink::new(config, rx);

    let metrics = sink.metrics();
    assert_eq!(metrics.batches_received.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn test_sink_run_empty_channel() {
    let (tx, rx) = mpsc::channel::<Arc<Batch>>(10);
    let config = ParquetConfig::default()
        .with_buffer_size(10)
        .with_path(tempdir().unwrap().path());

    let sink = ParquetSink::new(config, rx);

    // Close channel immediately
    drop(tx);

    // Should complete without error
    let result = sink.run().await;
    assert!(result.is_ok());
}

// =============================================================================
// Writer Key Tests
// =============================================================================

#[test]
fn test_writer_key_hourly() {
    let timestamp = DateTime::parse_from_rfc3339("2025-01-15T14:30:00Z")
        .unwrap()
        .with_timezone(&Utc);

    let key = WriterKey::new(42u32, timestamp, RotationInterval::Hourly);

    assert_eq!(key.workspace_id, 42u32);
    assert_eq!(key.date, "2025-01-15");
    assert_eq!(key.hour, Some(14));
}

#[test]
fn test_writer_key_daily() {
    let timestamp = DateTime::parse_from_rfc3339("2025-01-15T14:30:00Z")
        .unwrap()
        .with_timezone(&Utc);

    let key = WriterKey::new(42u32, timestamp, RotationInterval::Daily);

    assert_eq!(key.workspace_id, 42u32);
    assert_eq!(key.date, "2025-01-15");
    assert_eq!(key.hour, None);
}

#[test]
fn test_writer_key_path_hourly() {
    let timestamp = DateTime::parse_from_rfc3339("2025-01-15T09:00:00Z")
        .unwrap()
        .with_timezone(&Utc);

    let key = WriterKey::new(42u32, timestamp, RotationInterval::Hourly);
    let base = PathBuf::from("/data");
    let path = key.path(&base);

    assert_eq!(path, PathBuf::from("/data/42/2025-01-15/09"));
}

#[test]
fn test_writer_key_path_daily() {
    let timestamp = DateTime::parse_from_rfc3339("2025-01-15T09:00:00Z")
        .unwrap()
        .with_timezone(&Utc);

    let key = WriterKey::new(42u32, timestamp, RotationInterval::Daily);
    let base = PathBuf::from("/data");
    let path = key.path(&base);

    assert_eq!(path, PathBuf::from("/data/42/2025-01-15"));
}

#[test]
fn test_writer_key_equality() {
    let timestamp = DateTime::parse_from_rfc3339("2025-01-15T14:30:00Z")
        .unwrap()
        .with_timezone(&Utc);

    let key1 = WriterKey::new(42u32, timestamp, RotationInterval::Hourly);
    let key2 = WriterKey::new(42u32, timestamp, RotationInterval::Hourly);
    let key3 = WriterKey::new(43u32, timestamp, RotationInterval::Hourly);

    assert_eq!(key1, key2);
    assert_ne!(key1, key3);
}

#[test]
fn test_writer_key_hash() {
    use std::collections::HashSet;

    let timestamp = DateTime::parse_from_rfc3339("2025-01-15T14:30:00Z")
        .unwrap()
        .with_timezone(&Utc);

    let key1 = WriterKey::new(42u32, timestamp, RotationInterval::Hourly);
    let key2 = WriterKey::new(42u32, timestamp, RotationInterval::Hourly);
    let key3 = WriterKey::new(43u32, timestamp, RotationInterval::Hourly);

    let mut set = HashSet::new();
    set.insert(key1.clone());
    set.insert(key2);
    set.insert(key3);

    assert_eq!(set.len(), 2); // key1 and key2 are the same
}

// =============================================================================
// Row Buffer Tests
// =============================================================================

#[test]
fn test_row_buffer_new() {
    let config = ParquetConfig::default().with_buffer_size(100);
    let buffer = RowBuffer::new(config);

    assert!(buffer.is_empty());
    assert_eq!(buffer.event_count(), 0);
    assert_eq!(buffer.log_count(), 0);
    assert!(!buffer.should_flush());
}

#[test]
fn test_row_buffer_add_event() {
    let config = ParquetConfig::default().with_buffer_size(100);
    let mut buffer = RowBuffer::new(config);

    let row = EventRow {
        batch_timestamp: 1234567890100,
        timestamp: 1234567890000,
        event_type: "track".to_string(),
        device_id: None,
        session_id: None,
        event_name: None,
        payload: vec![],
        source_ip: vec![0; 16],
        workspace_id: 1,
    };

    buffer.add_event(row);

    assert!(!buffer.is_empty());
    assert_eq!(buffer.event_count(), 1);
    assert_eq!(buffer.log_count(), 0);
}

#[test]
fn test_row_buffer_add_log() {
    let config = ParquetConfig::default().with_buffer_size(100);
    let mut buffer = RowBuffer::new(config);

    let row = LogRow {
        batch_timestamp: 1234567890100,
        timestamp: 1234567890000,
        event_type: "log".to_string(),
        level: "info".to_string(),
        session_id: None,
        source: None,
        service: None,
        payload: vec![],
        source_ip: vec![0; 16],
        workspace_id: 1,
    };

    buffer.add_log(row);

    assert!(!buffer.is_empty());
    assert_eq!(buffer.event_count(), 0);
    assert_eq!(buffer.log_count(), 1);
}

#[test]
fn test_row_buffer_should_flush() {
    let config = ParquetConfig::default().with_buffer_size(3);
    let mut buffer = RowBuffer::new(config);

    assert!(!buffer.should_flush());

    for i in 0..3 {
        buffer.add_event(EventRow {
            batch_timestamp: 100 + i as i64,
            timestamp: i as i64,
            event_type: "track".to_string(),
            device_id: None,
            session_id: None,
            event_name: None,
            payload: vec![],
            source_ip: vec![0; 16],
            workspace_id: 1,
        });
    }

    assert!(buffer.should_flush());
}

#[test]
fn test_row_buffer_take_events() {
    let config = ParquetConfig::default().with_buffer_size(100);
    let mut buffer = RowBuffer::new(config);

    buffer.add_event(EventRow {
        batch_timestamp: 101,
        timestamp: 1,
        event_type: "track".to_string(),
        device_id: None,
        session_id: None,
        event_name: None,
        payload: vec![],
        source_ip: vec![0; 16],
        workspace_id: 1,
    });

    buffer.add_event(EventRow {
        batch_timestamp: 102,
        timestamp: 2,
        event_type: "identify".to_string(),
        device_id: None,
        session_id: None,
        event_name: None,
        payload: vec![],
        source_ip: vec![0; 16],
        workspace_id: 1,
    });

    let events = buffer.take_events();
    assert_eq!(events.len(), 2);
    assert_eq!(buffer.event_count(), 0);
}

#[test]
fn test_row_buffer_take_logs() {
    let config = ParquetConfig::default().with_buffer_size(100);
    let mut buffer = RowBuffer::new(config);

    buffer.add_log(LogRow {
        batch_timestamp: 101,
        timestamp: 1,
        event_type: "log".to_string(),
        level: "info".to_string(),
        session_id: None,
        source: None,
        service: None,
        payload: vec![],
        source_ip: vec![0; 16],
        workspace_id: 1,
    });

    let logs = buffer.take_logs();
    assert_eq!(logs.len(), 1);
    assert_eq!(buffer.log_count(), 0);
}

// =============================================================================
// Extract Source IP Tests
// =============================================================================

#[test]
fn test_extract_source_ip_v4() {
    let mut builder = BatchBuilder::new(BatchType::Event, SourceId::from("test"));
    builder.set_source_ip(std::net::IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)));
    builder.set_workspace_id(1);
    let batch = builder.finish();

    let ip = extract_source_ip(&batch);

    // IPv4-mapped IPv6
    assert_eq!(ip[0..10], [0u8; 10]);
    assert_eq!(ip[10], 0xff);
    assert_eq!(ip[11], 0xff);
    assert_eq!(ip[12], 192);
    assert_eq!(ip[13], 168);
    assert_eq!(ip[14], 1);
    assert_eq!(ip[15], 1);
}

#[test]
fn test_extract_source_ip_v6() {
    use std::net::Ipv6Addr;

    let mut builder = BatchBuilder::new(BatchType::Event, SourceId::from("test"));
    builder.set_source_ip(std::net::IpAddr::V6(Ipv6Addr::new(
        0x2001, 0xdb8, 0, 0, 0, 0, 0, 1,
    )));
    builder.set_workspace_id(1);
    let batch = builder.finish();

    let ip = extract_source_ip(&batch);

    // Full IPv6
    assert_eq!(ip[0], 0x20);
    assert_eq!(ip[1], 0x01);
    assert_eq!(ip[2], 0x0d);
    assert_eq!(ip[3], 0xb8);
}

// =============================================================================
// Error Tests
// =============================================================================

#[test]
fn test_error_display_io() {
    let err = ParquetSinkError::Io(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        "file not found",
    ));
    assert!(err.to_string().contains("I/O error"));
}

#[test]
fn test_error_display_create_dir() {
    let err = ParquetSinkError::CreateDir {
        path: "/some/path".to_string(),
        source: std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied"),
    };
    assert!(err.to_string().contains("failed to create directory"));
    assert!(err.to_string().contains("/some/path"));
}

#[test]
fn test_error_display_config() {
    let err = ParquetSinkError::Config("invalid compression".to_string());
    assert!(err.to_string().contains("invalid configuration"));
}

// =============================================================================
// Integration Tests
// =============================================================================

#[tokio::test]
async fn test_sink_processes_syslog_batch() {
    let dir = tempdir().unwrap();
    let config = ParquetConfig::default()
        .with_path(dir.path())
        .with_buffer_size(1)
        .uncompressed();

    let (tx, rx) = mpsc::channel::<Arc<Batch>>(10);
    let sink = ParquetSink::new(config, rx);

    // Create a syslog batch
    let mut builder = BatchBuilder::new(BatchType::Syslog, SourceId::from("syslog"));
    builder.set_workspace_id(42);
    builder.add_raw(b"<14>Jan 15 10:30:00 host app: test message");
    let batch = builder.finish();

    tx.send(Arc::new(batch)).await.unwrap();
    drop(tx);

    let result = sink.run().await;
    assert!(result.is_ok());

    let metrics = result.unwrap();
    assert_eq!(metrics.batches_received, 1);
}

#[tokio::test]
async fn test_sink_skips_zero_workspace_id() {
    let dir = tempdir().unwrap();
    let config = ParquetConfig::default()
        .with_path(dir.path())
        .with_buffer_size(1);

    let (tx, rx) = mpsc::channel::<Arc<Batch>>(10);
    let sink = ParquetSink::new(config, rx);

    // Create batch with zero workspace_id (default)
    let builder = BatchBuilder::new(BatchType::Event, SourceId::from("test"));
    // workspace_id defaults to 0
    let batch = builder.finish();

    tx.send(Arc::new(batch)).await.unwrap();
    drop(tx);

    let result = sink.run().await;
    assert!(result.is_ok());

    let metrics = result.unwrap();
    // Batch was received but no rows written
    assert_eq!(metrics.batches_received, 1);
    assert_eq!(metrics.event_rows_written, 0);
}

#[tokio::test]
async fn test_sink_handles_metric_batch_type() {
    let dir = tempdir().unwrap();
    let config = ParquetConfig::default()
        .with_path(dir.path())
        .with_buffer_size(1);

    let (tx, rx) = mpsc::channel::<Arc<Batch>>(10);
    let sink = ParquetSink::new(config, rx);

    // Create batch with metric type (unsupported)
    let mut builder = BatchBuilder::new(BatchType::Metric, SourceId::from("test"));
    builder.set_workspace_id(42);
    let batch = builder.finish();

    tx.send(Arc::new(batch)).await.unwrap();
    drop(tx);

    let result = sink.run().await;
    assert!(result.is_ok());

    let metrics = result.unwrap();
    assert_eq!(metrics.batches_received, 1);
    // No rows written for unsupported batch type
    assert_eq!(metrics.event_rows_written, 0);
    assert_eq!(metrics.log_rows_written, 0);
}
