//! Tests for Arrow IPC sink

use super::*;
use arrow::array::{Int64Array, StringArray, UInt64Array};
use arrow::ipc::reader::FileReader;
use std::fs::File;
use tempfile::tempdir;

// =============================================================================
// Test Data Helpers
// =============================================================================

fn sample_events() -> Vec<EventRow> {
    vec![
        EventRow {
            timestamp: 1700000000000,
            batch_timestamp: 1700000000100,
            workspace_id: 42,
            event_type: "track".to_string(),
            event_name: Some("page_view".to_string()),
            device_id: Some(vec![1; 16]),
            session_id: Some(vec![2; 16]),
            source_ip: vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 192, 168, 1, 1],
            payload: b"{\"page\": \"/home\"}".to_vec(),
        },
        EventRow {
            timestamp: 1700000001000,
            batch_timestamp: 1700000001100,
            workspace_id: 42,
            event_type: "identify".to_string(),
            event_name: None,
            device_id: Some(vec![1; 16]),
            session_id: None,
            source_ip: vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 10, 0, 0, 1],
            payload: b"{\"user_id\": \"u123\"}".to_vec(),
        },
    ]
}

fn sample_logs() -> Vec<LogRow> {
    vec![
        LogRow {
            timestamp: 1700000000000,
            batch_timestamp: 1700000000100,
            workspace_id: 42,
            level: "info".to_string(),
            event_type: "log".to_string(),
            source: Some("web-server-1".to_string()),
            service: Some("nginx".to_string()),
            session_id: Some(vec![3; 16]),
            source_ip: vec![0; 16],
            payload: b"GET /api/health 200".to_vec(),
        },
        LogRow {
            timestamp: 1700000002000,
            batch_timestamp: 1700000002100,
            workspace_id: 42,
            level: "error".to_string(),
            event_type: "log".to_string(),
            source: Some("api-server-1".to_string()),
            service: Some("api".to_string()),
            session_id: None,
            source_ip: vec![0; 16],
            payload: b"Connection refused to database".to_vec(),
        },
    ]
}

fn sample_snapshots() -> Vec<SnapshotRow> {
    vec![
        SnapshotRow {
            timestamp: 1700000000000,
            batch_timestamp: 1700000000100,
            workspace_id: 42,
            source: "github".to_string(),
            entity: "user/repo".to_string(),
            source_ip: vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 192, 168, 1, 1],
            payload: b"{\"stars\": 100}".to_vec(),
        },
        SnapshotRow {
            timestamp: 1700000001000,
            batch_timestamp: 1700000001100,
            workspace_id: 42,
            source: "stripe".to_string(),
            entity: "acct_123".to_string(),
            source_ip: vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 10, 0, 0, 1],
            payload: b"{\"balance\": 5000}".to_vec(),
        },
    ]
}

// =============================================================================
// Writer Tests
// =============================================================================

#[test]
fn test_write_events_empty() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("empty.arrow");

    let result = ArrowIpcWriter::write_events(&path, vec![]);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 0);
    assert!(!path.exists());
}

#[test]
fn test_write_events() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("events.arrow");

    let events = sample_events();
    let result = ArrowIpcWriter::write_events(&path, events);

    assert!(result.is_ok());
    assert!(path.exists());
    assert!(result.unwrap() > 0);
}

#[test]
fn test_write_logs_empty() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("empty_logs.arrow");

    let result = ArrowIpcWriter::write_logs(&path, vec![]);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 0);
    assert!(!path.exists());
}

#[test]
fn test_write_logs() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("logs.arrow");

    let logs = sample_logs();
    let result = ArrowIpcWriter::write_logs(&path, logs);

    assert!(result.is_ok());
    assert!(path.exists());
    assert!(result.unwrap() > 0);
}

#[test]
fn test_write_snapshots_empty() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("empty_snapshots.arrow");

    let result = ArrowIpcWriter::write_snapshots(&path, vec![]);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 0);
    assert!(!path.exists());
}

#[test]
fn test_write_snapshots() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("snapshots.arrow");

    let snapshots = sample_snapshots();
    let result = ArrowIpcWriter::write_snapshots(&path, snapshots);

    assert!(result.is_ok());
    assert!(path.exists());
    assert!(result.unwrap() > 0);
}

// =============================================================================
// Read Back Tests - Verify Arrow IPC files can be read
// =============================================================================

#[test]
fn test_events_roundtrip() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("events_roundtrip.arrow");

    let events = sample_events();
    let count = events.len();
    ArrowIpcWriter::write_events(&path, events).unwrap();

    // Read back the file
    let file = File::open(&path).unwrap();
    let reader = FileReader::try_new(file, None).unwrap();

    let mut total_rows = 0;
    for batch in reader {
        let batch = batch.unwrap();
        total_rows += batch.num_rows();
        assert_eq!(batch.num_columns(), 9);
    }
    assert_eq!(total_rows, count);
}

#[test]
fn test_logs_roundtrip() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("logs_roundtrip.arrow");

    let logs = sample_logs();
    let count = logs.len();
    ArrowIpcWriter::write_logs(&path, logs).unwrap();

    // Read back the file
    let file = File::open(&path).unwrap();
    let reader = FileReader::try_new(file, None).unwrap();

    let mut total_rows = 0;
    for batch in reader {
        let batch = batch.unwrap();
        total_rows += batch.num_rows();
        assert_eq!(batch.num_columns(), 10);
    }
    assert_eq!(total_rows, count);
}

#[test]
fn test_snapshots_roundtrip() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("snapshots_roundtrip.arrow");

    let snapshots = sample_snapshots();
    let count = snapshots.len();
    ArrowIpcWriter::write_snapshots(&path, snapshots).unwrap();

    // Read back the file
    let file = File::open(&path).unwrap();
    let reader = FileReader::try_new(file, None).unwrap();

    let mut total_rows = 0;
    for batch in reader {
        let batch = batch.unwrap();
        total_rows += batch.num_rows();
        assert_eq!(batch.num_columns(), 7);
    }
    assert_eq!(total_rows, count);
}

#[test]
fn test_events_column_values() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("events_values.arrow");

    let events = vec![EventRow {
        timestamp: 1234567890000,
        batch_timestamp: 1234567890100,
        workspace_id: 99,
        event_type: "track".to_string(),
        event_name: Some("click".to_string()),
        device_id: Some(vec![0xAB; 16]),
        session_id: None,
        source_ip: vec![0; 16],
        payload: b"test".to_vec(),
    }];

    ArrowIpcWriter::write_events(&path, events).unwrap();

    // Read back and verify values
    let file = File::open(&path).unwrap();
    let reader = FileReader::try_new(file, None).unwrap();
    let batch = reader.into_iter().next().unwrap().unwrap();

    // Check timestamp (column 0)
    let timestamps = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(timestamps.value(0), 1234567890000);

    // Check batch_timestamp (column 1)
    let batch_timestamps = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(batch_timestamps.value(0), 1234567890100);

    // Check workspace_id (column 2)
    let workspace_ids = batch
        .column(2)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(workspace_ids.value(0), 99);

    // Check event_type (column 3)
    let event_types = batch
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(event_types.value(0), "track");
}

#[test]
fn test_logs_column_values() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("logs_values.arrow");

    let logs = vec![LogRow {
        timestamp: 9876543210000,
        batch_timestamp: 9876543210100,
        workspace_id: 77,
        level: "warning".to_string(),
        event_type: "log".to_string(),
        source: Some("host-1".to_string()),
        service: Some("svc".to_string()),
        session_id: Some(vec![0xCD; 16]),
        source_ip: vec![1; 16],
        payload: b"warn msg".to_vec(),
    }];

    ArrowIpcWriter::write_logs(&path, logs).unwrap();

    // Read back and verify values
    let file = File::open(&path).unwrap();
    let reader = FileReader::try_new(file, None).unwrap();
    let batch = reader.into_iter().next().unwrap().unwrap();

    // Check timestamp (column 0)
    let timestamps = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(timestamps.value(0), 9876543210000);

    // Check level (column 3)
    let levels = batch
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(levels.value(0), "warning");

    // Check source (column 5)
    let sources = batch
        .column(5)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(sources.value(0), "host-1");
}

#[test]
fn test_snapshots_column_values() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("snapshots_values.arrow");

    let snapshots = vec![SnapshotRow {
        timestamp: 1234567890000,
        batch_timestamp: 1234567890100,
        workspace_id: 99,
        source: "linear".to_string(),
        entity: "issue_abc".to_string(),
        source_ip: vec![0; 16],
        payload: b"{\"status\": \"done\"}".to_vec(),
    }];

    ArrowIpcWriter::write_snapshots(&path, snapshots).unwrap();

    // Read back and verify values
    let file = File::open(&path).unwrap();
    let reader = FileReader::try_new(file, None).unwrap();
    let batch = reader.into_iter().next().unwrap().unwrap();

    // Check timestamp (column 0)
    let timestamps = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(timestamps.value(0), 1234567890000);

    // Check source (column 3)
    let sources = batch
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(sources.value(0), "linear");

    // Check entity (column 4)
    let entities = batch
        .column(4)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(entities.value(0), "issue_abc");
}

// =============================================================================
// Large Batch Tests
// =============================================================================

#[test]
fn test_write_large_batch() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("large_batch.arrow");

    // Create 10,000 events
    let events: Vec<EventRow> = (0..10_000)
        .map(|i| EventRow {
            timestamp: 1700000000000 + i as i64,
            batch_timestamp: 1700000000100 + i as i64,
            workspace_id: 1,
            event_type: "track".to_string(),
            event_name: Some(format!("event_{}", i)),
            device_id: Some(vec![i as u8; 16]),
            session_id: Some(vec![(i + 1) as u8; 16]),
            source_ip: vec![0; 16],
            payload: format!("{{\"index\": {}}}", i).into_bytes(),
        })
        .collect();

    let result = ArrowIpcWriter::write_events(&path, events);

    assert!(result.is_ok());
    assert!(path.exists());

    // Verify file size is reasonable
    let metadata = std::fs::metadata(&path).unwrap();
    assert!(metadata.len() > 0);

    // Read back and verify row count
    let file = File::open(&path).unwrap();
    let reader = FileReader::try_new(file, None).unwrap();
    let mut total_rows = 0;
    for batch in reader {
        total_rows += batch.unwrap().num_rows();
    }
    assert_eq!(total_rows, 10_000);
}

// =============================================================================
// Append Tests
// =============================================================================

#[test]
fn test_append_events() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("append_events.arrow");

    // First write
    let events1 = sample_events();
    let result1 = ArrowIpcWriter::append_events(&path, events1);
    assert!(result1.is_ok());

    // Second write (overwrites for now, like parquet)
    let events2 = sample_events();
    let result2 = ArrowIpcWriter::append_events(&path, events2);
    assert!(result2.is_ok());

    assert!(path.exists());
}

#[test]
fn test_append_logs() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("append_logs.arrow");

    let logs1 = sample_logs();
    let result1 = ArrowIpcWriter::append_logs(&path, logs1);
    assert!(result1.is_ok());

    let logs2 = sample_logs();
    let result2 = ArrowIpcWriter::append_logs(&path, logs2);
    assert!(result2.is_ok());

    assert!(path.exists());
}

#[test]
fn test_append_snapshots() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("append_snapshots.arrow");

    let snapshots1 = sample_snapshots();
    let result1 = ArrowIpcWriter::append_snapshots(&path, snapshots1);
    assert!(result1.is_ok());

    let snapshots2 = sample_snapshots();
    let result2 = ArrowIpcWriter::append_snapshots(&path, snapshots2);
    assert!(result2.is_ok());

    assert!(path.exists());
}

// =============================================================================
// Config Tests
// =============================================================================

#[test]
fn test_config_default() {
    let config = ArrowIpcConfig::default();
    assert_eq!(config.path, PathBuf::from("arrow_ipc"));
    assert_eq!(config.rotation_interval, RotationInterval::Daily);
    assert_eq!(config.buffer_size, 10_000);
    assert_eq!(config.flush_interval, Duration::from_secs(60));
}

#[test]
fn test_config_builder() {
    let config = ArrowIpcConfig::default()
        .with_path("/custom/path")
        .with_daily_rotation()
        .with_buffer_size(5000)
        .with_flush_interval(Duration::from_secs(30));

    assert_eq!(config.path, PathBuf::from("/custom/path"));
    assert_eq!(config.rotation_interval, RotationInterval::Daily);
    assert_eq!(config.buffer_size, 5000);
    assert_eq!(config.flush_interval, Duration::from_secs(30));
}

// =============================================================================
// Metrics Tests
// =============================================================================

#[test]
fn test_metrics_new() {
    let metrics = ArrowIpcSinkMetrics::new();
    let snapshot = metrics.snapshot();

    assert_eq!(snapshot.batches_received, 0);
    assert_eq!(snapshot.event_rows_written, 0);
    assert_eq!(snapshot.log_rows_written, 0);
    assert_eq!(snapshot.snapshot_rows_written, 0);
    assert_eq!(snapshot.bytes_written, 0);
    assert_eq!(snapshot.files_created, 0);
    assert_eq!(snapshot.write_errors, 0);
    assert_eq!(snapshot.decode_errors, 0);
}

#[test]
fn test_metrics_recording() {
    let metrics = ArrowIpcSinkMetrics::new();

    metrics.record_batch();
    metrics.record_event_rows(10);
    metrics.record_log_rows(5);
    metrics.record_snapshot_rows(3);
    metrics.record_bytes(1000);
    metrics.record_file_created();
    metrics.record_error();
    metrics.record_decode_error();

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.batches_received, 1);
    assert_eq!(snapshot.event_rows_written, 10);
    assert_eq!(snapshot.log_rows_written, 5);
    assert_eq!(snapshot.snapshot_rows_written, 3);
    assert_eq!(snapshot.bytes_written, 1000);
    assert_eq!(snapshot.files_created, 1);
    assert_eq!(snapshot.write_errors, 1);
    assert_eq!(snapshot.decode_errors, 1);
}

// =============================================================================
// Writer Key Tests
// =============================================================================

#[test]
fn test_writer_key_hourly() {
    use chrono::TimeZone;
    let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 14, 30, 0).unwrap();
    let key = WriterKey::new(42, timestamp, RotationInterval::Hourly);

    assert_eq!(key.workspace_id, 42);
    assert_eq!(key.date, "2024-01-15");
    assert_eq!(key.hour, Some(14));
}

#[test]
fn test_writer_key_daily() {
    use chrono::TimeZone;
    let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 14, 30, 0).unwrap();
    let key = WriterKey::new(42, timestamp, RotationInterval::Daily);

    assert_eq!(key.workspace_id, 42);
    assert_eq!(key.date, "2024-01-15");
    assert_eq!(key.hour, None);
}

#[test]
fn test_writer_key_path_hourly() {
    use chrono::TimeZone;
    let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 14, 30, 0).unwrap();
    let key = WriterKey::new(42, timestamp, RotationInterval::Hourly);
    let base = Path::new("/data");

    assert_eq!(key.path(base), PathBuf::from("/data/42/2024-01-15/14"));
}

#[test]
fn test_writer_key_path_daily() {
    use chrono::TimeZone;
    let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 14, 30, 0).unwrap();
    let key = WriterKey::new(42, timestamp, RotationInterval::Daily);
    let base = Path::new("/data");

    assert_eq!(key.path(base), PathBuf::from("/data/42/2024-01-15"));
}

// =============================================================================
// Row Buffer Tests
// =============================================================================

#[test]
fn test_row_buffer_new() {
    let buffer = RowBuffer::new(100);
    assert!(buffer.is_empty());
}

#[test]
fn test_row_buffer_add_events() {
    let mut buffer = RowBuffer::new(2);

    buffer.add_event(EventRow {
        timestamp: 1,
        batch_timestamp: 1,
        workspace_id: 1,
        event_type: "track".to_string(),
        event_name: None,
        device_id: None,
        session_id: None,
        source_ip: vec![0; 16],
        payload: vec![],
    });

    assert!(!buffer.is_empty());
    assert!(!buffer.should_flush());

    buffer.add_event(EventRow {
        timestamp: 2,
        batch_timestamp: 2,
        workspace_id: 1,
        event_type: "track".to_string(),
        event_name: None,
        device_id: None,
        session_id: None,
        source_ip: vec![0; 16],
        payload: vec![],
    });

    assert!(buffer.should_flush());
}

#[test]
fn test_row_buffer_take_events() {
    let mut buffer = RowBuffer::new(100);

    buffer.add_event(EventRow {
        timestamp: 1,
        batch_timestamp: 1,
        workspace_id: 1,
        event_type: "track".to_string(),
        event_name: None,
        device_id: None,
        session_id: None,
        source_ip: vec![0; 16],
        payload: vec![],
    });

    let events = buffer.take_events();
    assert_eq!(events.len(), 1);
    assert!(buffer.is_empty());
}

// =============================================================================
// Error Tests
// =============================================================================

#[test]
fn test_error_display() {
    let io_error = ArrowIpcSinkError::Io(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        "file not found",
    ));
    assert!(io_error.to_string().contains("I/O error"));

    let config_error = ArrowIpcSinkError::Config("invalid buffer size".to_string());
    assert!(config_error.to_string().contains("invalid configuration"));
}

// =============================================================================
// File Size Comparison - Arrow IPC should be larger than Parquet (no compression)
// =============================================================================

#[test]
fn test_arrow_ipc_file_size() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("size_test.arrow");

    // Write 1000 events
    let events: Vec<EventRow> = (0..1000)
        .map(|i| EventRow {
            timestamp: 1700000000000 + i as i64,
            batch_timestamp: 1700000000100 + i as i64,
            workspace_id: 1,
            event_type: "track".to_string(),
            event_name: Some(format!("event_{}", i)),
            device_id: Some(vec![i as u8; 16]),
            session_id: None,
            source_ip: vec![0; 16],
            payload: format!("{{\"index\": {}}}", i).into_bytes(),
        })
        .collect();

    let bytes = ArrowIpcWriter::write_events(&path, events).unwrap();

    // Arrow IPC is uncompressed, so files are larger
    // File should be > 100KB for 1000 events with payloads
    assert!(bytes > 50_000, "expected > 50KB, got {} bytes", bytes);
}
