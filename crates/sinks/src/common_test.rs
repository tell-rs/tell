//! Tests for common sink types and utilities

use crate::{SinkConfig, SinkError, SinkMetrics};

#[test]
fn test_metrics_new() {
    let metrics = SinkMetrics::new();
    let snapshot = metrics.snapshot();

    assert_eq!(snapshot.batches_received, 0);
    assert_eq!(snapshot.batches_written, 0);
    assert_eq!(snapshot.messages_written, 0);
    assert_eq!(snapshot.bytes_written, 0);
    assert_eq!(snapshot.write_errors, 0);
    assert_eq!(snapshot.flush_count, 0);
}

#[test]
fn test_metrics_batch_received() {
    let metrics = SinkMetrics::new();

    metrics.batch_received();
    metrics.batch_received();
    metrics.batch_received();

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.batches_received, 3);
}

#[test]
fn test_metrics_batch_written() {
    let metrics = SinkMetrics::new();

    metrics.batch_written(100, 5000);
    metrics.batch_written(200, 10000);

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.batches_written, 2);
    assert_eq!(snapshot.messages_written, 300);
    assert_eq!(snapshot.bytes_written, 15000);
}

#[test]
fn test_metrics_batch_tracking() {
    let metrics = SinkMetrics::new();

    metrics.batch_received();
    metrics.batch_received();
    metrics.batch_written(100, 5000);
    metrics.batch_written(200, 10000);
    metrics.write_error();

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.batches_received, 2);
    assert_eq!(snapshot.batches_written, 2);
    assert_eq!(snapshot.messages_written, 300);
    assert_eq!(snapshot.bytes_written, 15000);
    assert_eq!(snapshot.write_errors, 1);
}

#[test]
fn test_metrics_write_error() {
    let metrics = SinkMetrics::new();

    metrics.write_error();
    metrics.write_error();

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.write_errors, 2);
}

#[test]
fn test_metrics_flush() {
    let metrics = SinkMetrics::new();

    metrics.flush();
    metrics.flush();
    metrics.flush();

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.flush_count, 3);
}

#[test]
fn test_metrics_reset() {
    let metrics = SinkMetrics::new();

    metrics.batch_received();
    metrics.batch_written(50, 1000);
    metrics.write_error();
    metrics.flush();
    metrics.reset();

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.batches_received, 0);
    assert_eq!(snapshot.batches_written, 0);
    assert_eq!(snapshot.messages_written, 0);
    assert_eq!(snapshot.bytes_written, 0);
    assert_eq!(snapshot.write_errors, 0);
    assert_eq!(snapshot.flush_count, 0);
}

#[test]
fn test_sink_config_default() {
    let config = SinkConfig::default();

    assert!(config.id.is_empty());
    assert!(config.enabled);
    assert_eq!(config.queue_size, 1000);
    assert!(config.metrics_enabled);
    assert_eq!(config.metrics_interval_secs, 10);
}

#[test]
fn test_sink_config_custom() {
    let config = SinkConfig {
        id: "my_sink".into(),
        enabled: false,
        queue_size: 500,
        metrics_enabled: false,
        metrics_interval_secs: 30,
    };

    assert_eq!(config.id, "my_sink");
    assert!(!config.enabled);
    assert_eq!(config.queue_size, 500);
    assert!(!config.metrics_enabled);
    assert_eq!(config.metrics_interval_secs, 30);
}

#[test]
fn test_sink_error_init() {
    let err = SinkError::init("failed to connect");
    assert!(matches!(err, SinkError::Init(_)));
    assert!(err.to_string().contains("failed to connect"));
}

#[test]
fn test_sink_error_write() {
    let err = SinkError::write("disk full");
    assert!(matches!(err, SinkError::Write(_)));
    assert!(err.to_string().contains("disk full"));
}

#[test]
fn test_sink_error_config() {
    let err = SinkError::config("invalid path");
    assert!(matches!(err, SinkError::Config(_)));
    assert!(err.to_string().contains("invalid path"));
}

#[test]
fn test_sink_error_io() {
    let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
    let err: SinkError = io_err.into();
    assert!(matches!(err, SinkError::Io(_)));
}

#[test]
fn test_sink_error_display() {
    let err = SinkError::Init("test error".into());
    assert_eq!(err.to_string(), "failed to initialize sink: test error");

    let err = SinkError::Write("write failed".into());
    assert_eq!(err.to_string(), "write failed: write failed");

    let err = SinkError::Flush("flush failed".into());
    assert_eq!(err.to_string(), "flush failed: flush failed");

    let err = SinkError::Connection("timeout".into());
    assert_eq!(err.to_string(), "connection error: timeout");

    let err = SinkError::Config("bad config".into());
    assert_eq!(err.to_string(), "configuration error: bad config");

    let err = SinkError::Serialization("invalid data".into());
    assert_eq!(err.to_string(), "serialization error: invalid data");

    let err = SinkError::ChannelClosed;
    assert_eq!(err.to_string(), "channel closed");
}
