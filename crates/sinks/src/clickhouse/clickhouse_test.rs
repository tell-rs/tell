//! Tests for the ClickHouse sink (CDP v1.1 schema)

use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::Duration;

use cdp_client::event::{EventBuilder, EventDataBuilder, EventType};
use cdp_client::log::{LogEntryBuilder, LogDataBuilder};
use cdp_client::BatchBuilder as FlatBufferBuilder;
use cdp_protocol::{Batch, BatchBuilder, BatchType, SourceId};
use tokio::sync::mpsc;

use super::*;

// =============================================================================
// Test Data Helpers
// =============================================================================

/// Create a realistic FlatBuffer event batch with Track events
fn create_test_event_batch(event_count: usize) -> Vec<u8> {
    let mut event_data_builder = EventDataBuilder::new();

    for i in 0..event_count {
        let event = EventBuilder::new()
            .track(&format!("test_event_{}", i))
            .device_id([0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
                        0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, i as u8])
            .session_id([0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
                         0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, i as u8])
            .timestamp_now()
            .payload_json(&format!(r#"{{"index": {}, "page": "/test"}}"#, i))
            .build()
            .expect("build event");

        event_data_builder = event_data_builder.add(event);
    }

    let event_data = event_data_builder.build().expect("build event data");

    FlatBufferBuilder::new()
        .api_key([0x01; 16])
        .event_data(event_data)
        .build()
        .expect("build batch")
        .as_bytes()
        .to_vec()
}

/// Create a realistic FlatBuffer event batch with Identify events
fn create_test_identify_batch() -> Vec<u8> {
    let event = EventBuilder::new()
        .identify()
        .device_id([0x01; 16])
        .session_id([0x02; 16])
        .timestamp_now()
        .payload_json(r#"{"email": "test@example.com", "name": "Test User", "traits": {"plan": "premium"}}"#)
        .build()
        .expect("build identify event");

    let event_data = EventDataBuilder::new()
        .add(event)
        .build()
        .expect("build event data");

    FlatBufferBuilder::new()
        .api_key([0x01; 16])
        .event_data(event_data)
        .build()
        .expect("build batch")
        .as_bytes()
        .to_vec()
}

/// Create a realistic FlatBuffer event batch with Context events
fn create_test_context_batch() -> Vec<u8> {
    let event = EventBuilder::new()
        .event_type(EventType::Context)
        .device_id([0x01; 16])
        .session_id([0x02; 16])
        .timestamp_now()
        .payload_json(r#"{
            "device_type": "mobile",
            "device_model": "iPhone 14 Pro",
            "operating_system": "iOS",
            "os_version": "16.4",
            "app_version": "2.1.0",
            "timezone": "America/New_York",
            "locale": "en_US"
        }"#)
        .build()
        .expect("build context event");

    let event_data = EventDataBuilder::new()
        .add(event)
        .build()
        .expect("build event data");

    FlatBufferBuilder::new()
        .api_key([0x01; 16])
        .event_data(event_data)
        .build()
        .expect("build batch")
        .as_bytes()
        .to_vec()
}

/// Create a realistic FlatBuffer log batch
fn create_test_log_batch(log_count: usize) -> Vec<u8> {
    let mut log_data_builder = LogDataBuilder::new();

    for i in 0..log_count {
        let log = LogEntryBuilder::new()
            .info()
            .source(&format!("server-{}", i % 3))
            .service("api-gateway")
            .session_id([0x20 + i as u8; 16])
            .timestamp_now()
            .payload_json(&format!(r#"{{"request_id": "req-{}", "status": 200}}"#, i))
            .build()
            .expect("build log");

        log_data_builder = log_data_builder.add(log);
    }

    let log_data = log_data_builder.build().expect("build log data");

    FlatBufferBuilder::new()
        .api_key([0x01; 16])
        .log_data(log_data)
        .build()
        .expect("build batch")
        .as_bytes()
        .to_vec()
}

// =============================================================================
// Configuration Tests
// =============================================================================

#[test]
fn test_config_defaults() {
    let config = ClickHouseConfig::default();
    assert_eq!(config.url, "http://localhost:8123");
    assert_eq!(config.database, "default");
    assert!(config.username.is_none());
    assert!(config.password.is_none());
    assert_eq!(config.tables.events, "events_v1");
    assert_eq!(config.tables.users, "users_v1");
    assert_eq!(config.tables.user_devices, "user_devices");
    assert_eq!(config.tables.user_traits, "user_traits");
    assert_eq!(config.tables.context, "context_v1");
    assert_eq!(config.tables.logs, "logs_v1");
    assert_eq!(config.batch_size, DEFAULT_BATCH_SIZE);
    assert_eq!(config.flush_interval, DEFAULT_FLUSH_INTERVAL);
    assert_eq!(config.connection_timeout, DEFAULT_CONNECTION_TIMEOUT);
    assert_eq!(config.retry_attempts, DEFAULT_RETRY_ATTEMPTS);
}

#[test]
fn test_config_with_url() {
    let config = ClickHouseConfig::default().with_url("http://clickhouse:8123");
    assert_eq!(config.url, "http://clickhouse:8123");
}

#[test]
fn test_config_with_database() {
    let config = ClickHouseConfig::default().with_database("cdp_prod");
    assert_eq!(config.database, "cdp_prod");
}

#[test]
fn test_config_with_credentials() {
    let config = ClickHouseConfig::default().with_credentials("admin", "secret123");
    assert_eq!(config.username, Some("admin".to_string()));
    assert_eq!(config.password, Some("secret123".to_string()));
}

#[test]
fn test_config_with_tables() {
    let tables = TableNames {
        events: "custom_events".into(),
        users: "custom_users".into(),
        user_devices: "custom_devices".into(),
        user_traits: "custom_traits".into(),
        context: "custom_context".into(),
        logs: "custom_logs".into(),
        snapshots: "custom_snapshots".into(),
    };
    let config = ClickHouseConfig::default().with_tables(tables);
    assert_eq!(config.tables.events, "custom_events");
    assert_eq!(config.tables.users, "custom_users");
    assert_eq!(config.tables.logs, "custom_logs");
    assert_eq!(config.tables.snapshots, "custom_snapshots");
}

#[test]
fn test_config_with_batch_size() {
    let config = ClickHouseConfig::default().with_batch_size(5000);
    assert_eq!(config.batch_size, 5000);
}

#[test]
fn test_config_with_flush_interval() {
    let config = ClickHouseConfig::default().with_flush_interval(Duration::from_secs(10));
    assert_eq!(config.flush_interval, Duration::from_secs(10));
}

#[test]
fn test_config_with_retry_attempts() {
    let config = ClickHouseConfig::default().with_retry_attempts(5);
    assert_eq!(config.retry_attempts, 5);
}

#[test]
fn test_config_chaining() {
    let config = ClickHouseConfig::default()
        .with_url("http://ch.example.com:8123")
        .with_database("analytics")
        .with_credentials("user", "pass")
        .with_batch_size(2500)
        .with_flush_interval(Duration::from_secs(3))
        .with_retry_attempts(5);

    assert_eq!(config.url, "http://ch.example.com:8123");
    assert_eq!(config.database, "analytics");
    assert_eq!(config.username, Some("user".to_string()));
    assert_eq!(config.password, Some("pass".to_string()));
    assert_eq!(config.batch_size, 2500);
    assert_eq!(config.flush_interval, Duration::from_secs(3));
    assert_eq!(config.retry_attempts, 5);
}

// =============================================================================
// Table Names Tests
// =============================================================================

#[test]
fn test_table_names_default() {
    let tables = TableNames::default();
    assert_eq!(tables.events, "events_v1");
    assert_eq!(tables.users, "users_v1");
    assert_eq!(tables.user_devices, "user_devices");
    assert_eq!(tables.user_traits, "user_traits");
    assert_eq!(tables.context, "context_v1");
    assert_eq!(tables.logs, "logs_v1");
    assert_eq!(tables.snapshots, "snapshots_v1");
}

// =============================================================================
// Metrics Tests
// =============================================================================

#[test]
fn test_metrics_new() {
    let metrics = ClickHouseMetrics::new();
    let snapshot = metrics.snapshot();

    assert_eq!(snapshot.batches_received, 0);
    assert_eq!(snapshot.events_written, 0);
    assert_eq!(snapshot.users_written, 0);
    assert_eq!(snapshot.user_devices_written, 0);
    assert_eq!(snapshot.user_traits_written, 0);
    assert_eq!(snapshot.context_written, 0);
    assert_eq!(snapshot.logs_written, 0);
    assert_eq!(snapshot.batches_written, 0);
    assert_eq!(snapshot.write_errors, 0);
    assert_eq!(snapshot.retry_count, 0);
    assert_eq!(snapshot.decode_errors, 0);
}

#[test]
fn test_metrics_record_batch_received() {
    let metrics = ClickHouseMetrics::new();

    metrics.record_batch_received();
    metrics.record_batch_received();
    metrics.record_batch_received();

    assert_eq!(metrics.snapshot().batches_received, 3);
}

#[test]
fn test_metrics_record_events_written() {
    let metrics = ClickHouseMetrics::new();

    metrics.record_events_written(100);
    metrics.record_events_written(50);

    assert_eq!(metrics.snapshot().events_written, 150);
}

#[test]
fn test_metrics_record_users_written() {
    let metrics = ClickHouseMetrics::new();

    metrics.record_users_written(10);
    metrics.record_users_written(5);

    assert_eq!(metrics.snapshot().users_written, 15);
}

#[test]
fn test_metrics_record_user_devices_written() {
    let metrics = ClickHouseMetrics::new();

    metrics.record_user_devices_written(20);

    assert_eq!(metrics.snapshot().user_devices_written, 20);
}

#[test]
fn test_metrics_record_user_traits_written() {
    let metrics = ClickHouseMetrics::new();

    metrics.record_user_traits_written(30);
    metrics.record_user_traits_written(15);

    assert_eq!(metrics.snapshot().user_traits_written, 45);
}

#[test]
fn test_metrics_record_context_written() {
    let metrics = ClickHouseMetrics::new();

    metrics.record_context_written(25);

    assert_eq!(metrics.snapshot().context_written, 25);
}

#[test]
fn test_metrics_record_logs_written() {
    let metrics = ClickHouseMetrics::new();

    metrics.record_logs_written(200);
    metrics.record_logs_written(75);

    assert_eq!(metrics.snapshot().logs_written, 275);
}

#[test]
fn test_metrics_record_batch_written() {
    let metrics = ClickHouseMetrics::new();

    metrics.record_batch_written();
    metrics.record_batch_written();

    assert_eq!(metrics.snapshot().batches_written, 2);
}

#[test]
fn test_metrics_record_error() {
    let metrics = ClickHouseMetrics::new();

    metrics.record_error();

    assert_eq!(metrics.snapshot().write_errors, 1);
}

#[test]
fn test_metrics_record_retry() {
    let metrics = ClickHouseMetrics::new();

    metrics.record_retry();
    metrics.record_retry();

    assert_eq!(metrics.snapshot().retry_count, 2);
}

#[test]
fn test_metrics_record_decode_error() {
    let metrics = ClickHouseMetrics::new();

    metrics.record_decode_error();
    metrics.record_decode_error();
    metrics.record_decode_error();

    assert_eq!(metrics.snapshot().decode_errors, 3);
}

#[test]
fn test_metrics_snapshot_comprehensive() {
    let metrics = ClickHouseMetrics::new();

    metrics.record_batch_received();
    metrics.record_batch_received();
    metrics.record_events_written(100);
    metrics.record_users_written(10);
    metrics.record_user_devices_written(10);
    metrics.record_user_traits_written(25);
    metrics.record_context_written(5);
    metrics.record_logs_written(50);
    metrics.record_batch_written();
    metrics.record_error();
    metrics.record_retry();
    metrics.record_decode_error();

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.batches_received, 2);
    assert_eq!(snapshot.events_written, 100);
    assert_eq!(snapshot.users_written, 10);
    assert_eq!(snapshot.user_devices_written, 10);
    assert_eq!(snapshot.user_traits_written, 25);
    assert_eq!(snapshot.context_written, 5);
    assert_eq!(snapshot.logs_written, 50);
    assert_eq!(snapshot.batches_written, 1);
    assert_eq!(snapshot.write_errors, 1);
    assert_eq!(snapshot.retry_count, 1);
    assert_eq!(snapshot.decode_errors, 1);
}

// =============================================================================
// Sink Creation Tests
// =============================================================================

#[test]
fn test_sink_creation() {
    let (_, rx) = mpsc::channel::<Arc<Batch>>(10);
    let config = ClickHouseConfig::default();
    let sink = ClickHouseSink::new(config.clone(), rx);

    assert_eq!(sink.config().url, config.url);
    assert_eq!(sink.config().database, config.database);
    assert_eq!(sink.metrics().snapshot().batches_received, 0);
}

#[test]
fn test_sink_metrics_reference() {
    let (_, rx) = mpsc::channel::<Arc<Batch>>(10);
    let config = ClickHouseConfig::default();
    let sink = ClickHouseSink::new(config, rx);

    let metrics = sink.metrics();
    assert_eq!(metrics.snapshot().batches_received, 0);
}

#[test]
fn test_sink_config_reference() {
    let (_, rx) = mpsc::channel::<Arc<Batch>>(10);
    let config = ClickHouseConfig::default()
        .with_database("test_db")
        .with_batch_size(500);

    let sink = ClickHouseSink::new(config, rx);

    assert_eq!(sink.config().database, "test_db");
    assert_eq!(sink.config().batch_size, 500);
}

// =============================================================================
// TableBatches Tests
// =============================================================================

#[test]
fn test_table_batches_new() {
    let batches = TableBatches::new(100);
    assert!(batches.is_empty());
    assert!(!batches.any_needs_flush(100));
}

#[test]
fn test_table_batches_is_empty() {
    let mut batches = TableBatches::new(100);
    assert!(batches.is_empty());

    batches.track.push(EventRow {
        timestamp: 0,
        event_name: String::new(),
        device_id: [0; 16],
        session_id: [0; 16],
        properties: String::new(),
        raw: String::new(),
        source_ip: [0; 16],
    });

    assert!(!batches.is_empty());
}

#[test]
fn test_table_batches_any_needs_flush() {
    let mut batches = TableBatches::new(100);
    assert!(!batches.any_needs_flush(2));

    // Add 2 items, threshold is 2
    batches.track.push(EventRow {
        timestamp: 0,
        event_name: String::new(),
        device_id: [0; 16],
        session_id: [0; 16],
        properties: String::new(),
        raw: String::new(),
        source_ip: [0; 16],
    });
    batches.track.push(EventRow {
        timestamp: 1,
        event_name: String::new(),
        device_id: [0; 16],
        session_id: [0; 16],
        properties: String::new(),
        raw: String::new(),
        source_ip: [0; 16],
    });

    assert!(batches.any_needs_flush(2));
}

// =============================================================================
// Batch Processing Tests
// =============================================================================

#[test]
fn test_process_batch_handles_event_batch() {
    let (_, rx) = mpsc::channel::<Arc<Batch>>(10);
    let config = ClickHouseConfig::default();
    let mut sink = ClickHouseSink::new(config, rx);

    // Create event batch
    let mut builder = BatchBuilder::new(BatchType::Event, SourceId::from("test"));
    builder.set_workspace_id(42);
    let batch = builder.finish();

    let result = sink.process_batch(&batch);
    assert!(result.is_ok());
}

#[test]
fn test_process_batch_handles_log_batch() {
    let (_, rx) = mpsc::channel::<Arc<Batch>>(10);
    let config = ClickHouseConfig::default();
    let mut sink = ClickHouseSink::new(config, rx);

    // Create log batch
    let mut builder = BatchBuilder::new(BatchType::Log, SourceId::from("test"));
    builder.set_workspace_id(42);
    let batch = builder.finish();

    let result = sink.process_batch(&batch);
    assert!(result.is_ok());
}

#[test]
fn test_process_batch_handles_syslog_batch() {
    let (_, rx) = mpsc::channel::<Arc<Batch>>(10);
    let config = ClickHouseConfig::default();
    let mut sink = ClickHouseSink::new(config, rx);

    // Create syslog batch
    let mut builder = BatchBuilder::new(BatchType::Syslog, SourceId::from("syslog"));
    builder.set_workspace_id(42);
    builder.add_raw(b"<14>Jan 15 10:30:00 host app: test message");
    let batch = builder.finish();

    let result = sink.process_batch(&batch);
    assert!(result.is_ok());

    // Check that log was added to buffer
    assert!(!sink.batches.logs.is_empty());
}

#[test]
fn test_process_batch_skips_metric_batch() {
    let (_, rx) = mpsc::channel::<Arc<Batch>>(10);
    let config = ClickHouseConfig::default();
    let mut sink = ClickHouseSink::new(config, rx);

    // Create metric batch (unsupported)
    let mut builder = BatchBuilder::new(BatchType::Metric, SourceId::from("test"));
    builder.set_workspace_id(42);
    let batch = builder.finish();

    let result = sink.process_batch(&batch);
    assert!(result.is_ok());
    // All batches should be empty - metrics are skipped
    assert!(sink.batches.is_empty());
}

#[test]
fn test_process_batch_skips_trace_batch() {
    let (_, rx) = mpsc::channel::<Arc<Batch>>(10);
    let config = ClickHouseConfig::default();
    let mut sink = ClickHouseSink::new(config, rx);

    // Create trace batch (unsupported)
    let mut builder = BatchBuilder::new(BatchType::Trace, SourceId::from("test"));
    builder.set_workspace_id(42);
    let batch = builder.finish();

    let result = sink.process_batch(&batch);
    assert!(result.is_ok());
    // All batches should be empty - traces are skipped
    assert!(sink.batches.is_empty());
}

// =============================================================================
// Helper Function Tests
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

    assert_eq!(ip[0], 0x20);
    assert_eq!(ip[1], 0x01);
    assert_eq!(ip[2], 0x0d);
    assert_eq!(ip[3], 0xb8);
}

#[test]
fn test_generate_user_id_from_email() {
    // Valid email should produce non-empty user_id
    let user_id = generate_user_id_from_email("user@example.com");
    assert!(!user_id.is_empty());

    // Same email should produce same user_id (deterministic)
    let user_id2 = generate_user_id_from_email("user@example.com");
    assert_eq!(user_id, user_id2);

    // Different emails should produce different user_ids
    let user_id3 = generate_user_id_from_email("other@example.com");
    assert_ne!(user_id, user_id3);

    // Empty email should produce empty user_id
    let empty_id = generate_user_id_from_email("");
    assert!(empty_id.is_empty());

    // Whitespace-only email should produce empty user_id
    let whitespace_id = generate_user_id_from_email("   ");
    assert!(whitespace_id.is_empty());
}

#[test]
fn test_generate_user_id_normalized() {
    // Case-insensitive normalization
    let id1 = generate_user_id_from_email("User@Example.COM");
    let id2 = generate_user_id_from_email("user@example.com");
    assert_eq!(id1, id2);

    // Whitespace trimming
    let id3 = generate_user_id_from_email("  user@example.com  ");
    assert_eq!(id1, id3);
}

// JSON extraction tests are in crate::util::json

#[test]
fn test_normalize_locale() {
    // Short locale gets padded
    let locale = normalize_locale("en");
    assert_eq!(locale.len(), 5);

    // Exact length stays same
    let locale = normalize_locale("en_US");
    assert_eq!(locale, "en_US");

    // Long locale gets truncated
    let locale = normalize_locale("en_US_extra");
    assert_eq!(locale.len(), 5);
    assert_eq!(locale, "en_US");
}

// =============================================================================
// Error Tests
// =============================================================================

#[test]
fn test_error_display_insert() {
    let err = ClickHouseSinkError::InsertError("connection refused".to_string());
    assert!(err.to_string().contains("insert error"));
    assert!(err.to_string().contains("connection refused"));
}

#[test]
fn test_error_display_decode() {
    let err = ClickHouseSinkError::DecodeError("invalid FlatBuffer".to_string());
    assert!(err.to_string().contains("decode"));
    assert!(err.to_string().contains("invalid FlatBuffer"));
}

#[test]
fn test_error_display_config() {
    let err = ClickHouseSinkError::ConfigError("invalid URL".to_string());
    assert!(err.to_string().contains("configuration error"));
    assert!(err.to_string().contains("invalid URL"));
}

// =============================================================================
// Integration Tests (Run without actual ClickHouse)
// =============================================================================

#[tokio::test]
async fn test_sink_run_empty_channel() {
    let (tx, rx) = mpsc::channel::<Arc<Batch>>(10);
    let config = ClickHouseConfig::default().with_batch_size(10);

    let sink = ClickHouseSink::new(config, rx);

    // Close channel immediately
    drop(tx);

    // Should complete without error
    let result = sink.run().await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_sink_processes_syslog_to_buffer() {
    let (tx, rx) = mpsc::channel::<Arc<Batch>>(10);
    let config = ClickHouseConfig::default().with_batch_size(1000);

    let mut sink = ClickHouseSink::new(config, rx);

    // Create and process a syslog batch
    let mut builder = BatchBuilder::new(BatchType::Syslog, SourceId::from("syslog"));
    builder.set_workspace_id(42);
    builder.add_raw(b"<14>Jan 15 10:30:00 host app: test message");
    let batch = builder.finish();

    let result = sink.process_batch(&batch);
    assert!(result.is_ok());

    // Verify log was added to buffer
    assert_eq!(sink.batches.logs.len(), 1);
    assert_eq!(sink.batches.logs[0].level, "info");

    drop(tx);
}

// =============================================================================
// Row Type Tests
// =============================================================================

#[test]
fn test_event_row_creation() {
    let row = EventRow {
        timestamp: 1700000000000,
        event_name: "page_view".to_string(),
        device_id: [0x01; 16],
        session_id: [0x02; 16],
        properties: r#"{"page": "/home"}"#.to_string(),
        raw: r#"{"page": "/home"}"#.to_string(),
        source_ip: [0; 16],
    };

    assert_eq!(row.timestamp, 1700000000000);
    assert_eq!(row.event_name, "page_view");
}

#[test]
fn test_user_row_creation() {
    let row = UserRow {
        user_id: "abc-123".to_string(),
        email: "user@example.com".to_string(),
        name: "Test User".to_string(),
        updated_at: 1700000000000,
    };

    assert_eq!(row.user_id, "abc-123");
    assert_eq!(row.email, "user@example.com");
}

#[test]
fn test_user_device_row_creation() {
    let row = UserDeviceRow {
        user_id: "abc-123".to_string(),
        device_id: [0x03; 16],
        linked_at: 1700000000000,
    };

    assert_eq!(row.user_id, "abc-123");
    assert_eq!(row.device_id, [0x03; 16]);
}

#[test]
fn test_user_trait_row_creation() {
    let row = UserTraitRow {
        user_id: "abc-123".to_string(),
        trait_key: "plan".to_string(),
        trait_value: "premium".to_string(),
        updated_at: 1700000000000,
    };

    assert_eq!(row.trait_key, "plan");
    assert_eq!(row.trait_value, "premium");
}

#[test]
fn test_context_row_creation() {
    let row = ContextRow {
        timestamp: 1700000000000,
        device_id: [0x01; 16],
        session_id: [0x02; 16],
        device_type: "mobile".to_string(),
        device_model: "iPhone 14 Pro".to_string(),
        operating_system: "iOS".to_string(),
        os_version: "16.4".to_string(),
        app_version: "2.1.0".to_string(),
        app_build: "1234".to_string(),
        timezone: "America/New_York".to_string(),
        locale: "en_US".to_string(),
        country: "US".to_string(),
        region: "NY".to_string(),
        city: "New York".to_string(),
        properties: "{}".to_string(),
        source_ip: [0; 16],
    };

    assert_eq!(row.device_type, "mobile");
    assert_eq!(row.operating_system, "iOS");
}

#[test]
fn test_log_row_creation() {
    let row = LogRow {
        timestamp: 1700000000000,
        level: "info".to_string(),
        source: "web-server-1".to_string(),
        service: "nginx".to_string(),
        session_id: [0x03; 16],
        source_ip: [0; 16],
        pattern_id: Some(12345),
        message: "GET /api/health 200".to_string(),
        raw: "GET /api/health 200".to_string(),
    };

    assert_eq!(row.level, "info");
    assert_eq!(row.service, "nginx");
    assert_eq!(row.pattern_id, Some(12345));
}

#[test]
fn test_log_row_without_pattern_id() {
    let row = LogRow {
        timestamp: 1700000000000,
        level: "error".to_string(),
        source: "api-server".to_string(),
        service: "api".to_string(),
        session_id: [0; 16],
        source_ip: [0; 16],
        pattern_id: None,
        message: "Connection refused".to_string(),
        raw: "Connection refused".to_string(),
    };

    assert_eq!(row.pattern_id, None);
}

// =============================================================================
// FlatBuffer Decode Integration Tests
// =============================================================================

#[test]
fn test_process_batch_decodes_track_events() {
    let (_, rx) = mpsc::channel::<Arc<Batch>>(10);
    let config = ClickHouseConfig::default();
    let mut sink = ClickHouseSink::new(config, rx);

    // Create batch with realistic FlatBuffer event data
    let fb_data = create_test_event_batch(3);
    let mut builder = BatchBuilder::new(BatchType::Event, SourceId::from("test"));
    builder.set_workspace_id(42);
    builder.add(&fb_data, 3);
    let batch = builder.finish();

    let result = sink.process_batch(&batch);
    assert!(result.is_ok());

    // Verify events were decoded and added to buffer
    assert_eq!(sink.batches.track.len(), 3, "should have decoded 3 track events");

    // Verify event content
    assert!(sink.batches.track[0].event_name.starts_with("test_event_"));
    assert!(!sink.batches.track[0].properties.is_empty());
}

#[test]
fn test_process_batch_decodes_identify_events() {
    let (_, rx) = mpsc::channel::<Arc<Batch>>(10);
    let config = ClickHouseConfig::default();
    let mut sink = ClickHouseSink::new(config, rx);

    // Create batch with Identify event
    let fb_data = create_test_identify_batch();
    let mut builder = BatchBuilder::new(BatchType::Event, SourceId::from("test"));
    builder.set_workspace_id(42);
    builder.add(&fb_data, 1);
    let batch = builder.finish();

    let result = sink.process_batch(&batch);
    assert!(result.is_ok());

    // Verify user was added to buffer
    assert!(!sink.batches.users.is_empty(), "should have decoded user from identify");

    // Verify user data
    let user = &sink.batches.users[0];
    assert_eq!(user.email, "test@example.com");
    assert_eq!(user.name, "Test User");
}

#[test]
fn test_process_batch_decodes_context_events() {
    let (_, rx) = mpsc::channel::<Arc<Batch>>(10);
    let config = ClickHouseConfig::default();
    let mut sink = ClickHouseSink::new(config, rx);

    // Create batch with Context event
    let fb_data = create_test_context_batch();
    let mut builder = BatchBuilder::new(BatchType::Event, SourceId::from("test"));
    builder.set_workspace_id(42);
    builder.add(&fb_data, 1);
    let batch = builder.finish();

    let result = sink.process_batch(&batch);
    assert!(result.is_ok());

    // Verify context was added to buffer
    assert!(!sink.batches.context.is_empty(), "should have decoded context");

    // Verify context data
    let ctx = &sink.batches.context[0];
    assert_eq!(ctx.device_type, "mobile");
    assert_eq!(ctx.device_model, "iPhone 14 Pro");
    assert_eq!(ctx.operating_system, "iOS");
}

#[test]
fn test_process_batch_decodes_log_entries() {
    let (_, rx) = mpsc::channel::<Arc<Batch>>(10);
    let config = ClickHouseConfig::default();
    let mut sink = ClickHouseSink::new(config, rx);

    // Create batch with realistic FlatBuffer log data
    let fb_data = create_test_log_batch(5);
    let mut builder = BatchBuilder::new(BatchType::Log, SourceId::from("test"));
    builder.set_workspace_id(42);
    builder.add(&fb_data, 5);
    let batch = builder.finish();

    let result = sink.process_batch(&batch);
    assert!(result.is_ok());

    // Verify logs were decoded and added to buffer
    assert_eq!(sink.batches.logs.len(), 5, "should have decoded 5 log entries");

    // Verify log content
    assert_eq!(sink.batches.logs[0].level, "info");
    assert_eq!(sink.batches.logs[0].service, "api-gateway");
    assert!(sink.batches.logs[0].source.starts_with("server-"));
}

#[test]
fn test_process_batch_multiple_messages() {
    let (_, rx) = mpsc::channel::<Arc<Batch>>(10);
    let config = ClickHouseConfig::default();
    let mut sink = ClickHouseSink::new(config, rx);

    // Create batch with multiple FlatBuffer messages
    let mut builder = BatchBuilder::new(BatchType::Event, SourceId::from("test"));
    builder.set_workspace_id(42);

    // Add 3 separate FlatBuffer messages
    let fb_data1 = create_test_event_batch(2);
    let fb_data2 = create_test_event_batch(3);
    let fb_data3 = create_test_identify_batch();

    builder.add(&fb_data1, 2);
    builder.add(&fb_data2, 3);
    builder.add(&fb_data3, 1);

    let batch = builder.finish();

    let result = sink.process_batch(&batch);
    assert!(result.is_ok());

    // Verify all messages were decoded
    assert_eq!(sink.batches.track.len(), 5, "should have 2 + 3 = 5 track events");
    assert!(!sink.batches.users.is_empty(), "should have user from identify");
}

#[test]
fn test_process_batch_handles_decode_error_gracefully() {
    let (_, rx) = mpsc::channel::<Arc<Batch>>(10);
    let config = ClickHouseConfig::default();
    let mut sink = ClickHouseSink::new(config, rx);

    // Create batch with invalid FlatBuffer data
    let mut builder = BatchBuilder::new(BatchType::Event, SourceId::from("test"));
    builder.set_workspace_id(42);
    builder.add(b"not valid flatbuffer data", 1);
    let batch = builder.finish();

    // Should not panic, just skip invalid data
    let result = sink.process_batch(&batch);
    assert!(result.is_ok());

    // Decode error should be recorded
    assert_eq!(
        sink.metrics().snapshot().decode_errors,
        1,
        "should record decode error"
    );

    // Buffers should be empty
    assert!(sink.batches.is_empty());
}

#[tokio::test]
async fn test_sink_processes_realistic_event_batch() {
    let (tx, rx) = mpsc::channel::<Arc<Batch>>(10);
    let config = ClickHouseConfig::default().with_batch_size(1000);

    let mut sink = ClickHouseSink::new(config, rx);

    // Create and process a realistic event batch
    let fb_data = create_test_event_batch(10);
    let mut builder = BatchBuilder::new(BatchType::Event, SourceId::from("test"));
    builder.set_workspace_id(42);
    builder.set_source_ip(std::net::IpAddr::V4(Ipv4Addr::new(192, 168, 1, 100)));
    builder.add(&fb_data, 10);
    let batch = builder.finish();

    let result = sink.process_batch(&batch);
    assert!(result.is_ok());

    // Verify events were decoded correctly
    assert_eq!(sink.batches.track.len(), 10);

    // Verify source IP was preserved
    for row in &sink.batches.track {
        // IPv4-mapped IPv6: ::ffff:192.168.1.100
        assert_eq!(row.source_ip[12], 192);
        assert_eq!(row.source_ip[13], 168);
        assert_eq!(row.source_ip[14], 1);
        assert_eq!(row.source_ip[15], 100);
    }

    drop(tx);
}

#[tokio::test]
async fn test_sink_processes_realistic_log_batch() {
    let (tx, rx) = mpsc::channel::<Arc<Batch>>(10);
    let config = ClickHouseConfig::default().with_batch_size(1000);

    let mut sink = ClickHouseSink::new(config, rx);

    // Create and process a realistic log batch
    let fb_data = create_test_log_batch(10);
    let mut builder = BatchBuilder::new(BatchType::Log, SourceId::from("test"));
    builder.set_workspace_id(42);
    builder.add(&fb_data, 10);
    let batch = builder.finish();

    let result = sink.process_batch(&batch);
    assert!(result.is_ok());

    // Verify logs were decoded correctly
    assert_eq!(sink.batches.logs.len(), 10);

    // Verify log fields
    for log in &sink.batches.logs {
        assert_eq!(log.level, "info");
        assert_eq!(log.service, "api-gateway");
    }

    drop(tx);
}
