//! Tests for LogEntryBuilder and LogDataBuilder

use crate::BuilderError;
use crate::log::{LogDataBuilder, LogEntryBuilder, LogEventType, LogLevel};

// =============================================================================
// LogEntryBuilder basic tests
// =============================================================================

#[test]
fn test_build_minimal_log() {
    let log = LogEntryBuilder::new().build().unwrap();

    assert!(!log.is_empty());
}

#[test]
fn test_build_error_log() {
    let log = LogEntryBuilder::new()
        .error()
        .source("web-01.prod")
        .service("api-gateway")
        .timestamp(1234567890000)
        .payload_json(r#"{"error": "connection timeout"}"#)
        .build()
        .unwrap();

    assert!(!log.is_empty());
}

#[test]
fn test_build_info_log() {
    let log = LogEntryBuilder::new()
        .info()
        .source("worker-1")
        .service("batch-processor")
        .payload_json(r#"{"processed": 1000}"#)
        .build()
        .unwrap();

    assert!(!log.is_empty());
}

#[test]
fn test_build_full_log() {
    let log = LogEntryBuilder::new()
        .event_type(LogEventType::Log)
        .session_id([0x01; 16])
        .level(LogLevel::Warning)
        .timestamp(1700000000000)
        .source("server-01.prod.us-east-1")
        .service("payment-service")
        .payload_json(r#"{"retry_count": 3, "last_error": "timeout"}"#)
        .build()
        .unwrap();

    assert!(!log.is_empty());
}

// =============================================================================
// LogLevel convenience methods
// =============================================================================

#[test]
fn test_log_level_emergency() {
    let log = LogEntryBuilder::new().emergency().build().unwrap();
    assert!(!log.is_empty());
}

#[test]
fn test_log_level_alert() {
    let log = LogEntryBuilder::new().alert().build().unwrap();
    assert!(!log.is_empty());
}

#[test]
fn test_log_level_critical() {
    let log = LogEntryBuilder::new().critical().build().unwrap();
    assert!(!log.is_empty());
}

#[test]
fn test_log_level_error() {
    let log = LogEntryBuilder::new().error().build().unwrap();
    assert!(!log.is_empty());
}

#[test]
fn test_log_level_warning() {
    let log = LogEntryBuilder::new().warning().build().unwrap();
    assert!(!log.is_empty());
}

#[test]
fn test_log_level_notice() {
    let log = LogEntryBuilder::new().notice().build().unwrap();
    assert!(!log.is_empty());
}

#[test]
fn test_log_level_info() {
    let log = LogEntryBuilder::new().info().build().unwrap();
    assert!(!log.is_empty());
}

#[test]
fn test_log_level_debug() {
    let log = LogEntryBuilder::new().debug().build().unwrap();
    assert!(!log.is_empty());
}

#[test]
fn test_log_level_trace() {
    let log = LogEntryBuilder::new().trace().build().unwrap();
    assert!(!log.is_empty());
}

// =============================================================================
// LogEventType tests
// =============================================================================

#[test]
fn test_log_event_type_log() {
    let log = LogEntryBuilder::new()
        .event_type(LogEventType::Log)
        .build()
        .unwrap();

    assert!(!log.is_empty());
}

#[test]
fn test_log_event_type_enrich() {
    let log = LogEntryBuilder::new()
        .event_type(LogEventType::Enrich)
        .build()
        .unwrap();

    assert!(!log.is_empty());
}

// =============================================================================
// Session ID tests
// =============================================================================

#[test]
fn test_session_id_array() {
    let log = LogEntryBuilder::new()
        .session_id([0x42; 16])
        .build()
        .unwrap();

    assert!(!log.is_empty());
}

#[test]
fn test_session_id_slice_valid() {
    let id: [u8; 16] = [0x01; 16];
    let log = LogEntryBuilder::new()
        .session_id_slice(&id)
        .unwrap()
        .build()
        .unwrap();

    assert!(!log.is_empty());
}

#[test]
fn test_session_id_slice_too_short() {
    let result = LogEntryBuilder::new().session_id_slice(&[0x01; 10]);

    assert!(matches!(
        result,
        Err(BuilderError::InvalidUuidLength {
            field: "session_id",
            len: 10
        })
    ));
}

#[test]
fn test_session_id_slice_too_long() {
    let result = LogEntryBuilder::new().session_id_slice(&[0x01; 20]);

    assert!(matches!(
        result,
        Err(BuilderError::InvalidUuidLength {
            field: "session_id",
            len: 20
        })
    ));
}

// =============================================================================
// Timestamp tests
// =============================================================================

#[test]
fn test_timestamp_explicit() {
    let log = LogEntryBuilder::new()
        .timestamp(1700000000000)
        .build()
        .unwrap();

    assert!(!log.is_empty());
}

#[test]
fn test_timestamp_now() {
    let log = LogEntryBuilder::new().timestamp_now().build().unwrap();

    assert!(!log.is_empty());
}

#[test]
fn test_timestamp_zero() {
    let log = LogEntryBuilder::new().timestamp(0).build().unwrap();

    assert!(!log.is_empty());
}

#[test]
fn test_timestamp_max() {
    let log = LogEntryBuilder::new().timestamp(u64::MAX).build().unwrap();

    assert!(!log.is_empty());
}

// =============================================================================
// Source tests
// =============================================================================

#[test]
fn test_source_short() {
    let log = LogEntryBuilder::new().source("web-01").build().unwrap();

    assert!(!log.is_empty());
}

#[test]
fn test_source_long_valid() {
    let source = "a".repeat(256);
    let log = LogEntryBuilder::new().source(&source).build().unwrap();

    assert!(!log.is_empty());
}

#[test]
fn test_source_too_long() {
    let source = "a".repeat(257);
    let result = LogEntryBuilder::new().source(&source).build();

    assert!(matches!(
        result,
        Err(BuilderError::SourceTooLong { len: 257, max: 256 })
    ));
}

#[test]
fn test_source_with_dots() {
    let log = LogEntryBuilder::new()
        .source("server-01.prod.us-east-1.example.com")
        .build()
        .unwrap();

    assert!(!log.is_empty());
}

// =============================================================================
// Service tests
// =============================================================================

#[test]
fn test_service_short() {
    let log = LogEntryBuilder::new().service("api").build().unwrap();

    assert!(!log.is_empty());
}

#[test]
fn test_service_long_valid() {
    let service = "b".repeat(256);
    let log = LogEntryBuilder::new().service(&service).build().unwrap();

    assert!(!log.is_empty());
}

#[test]
fn test_service_too_long() {
    let service = "b".repeat(257);
    let result = LogEntryBuilder::new().service(&service).build();

    assert!(matches!(
        result,
        Err(BuilderError::ServiceTooLong { len: 257, max: 256 })
    ));
}

#[test]
fn test_service_with_hyphens() {
    let log = LogEntryBuilder::new()
        .service("payment-gateway-v2")
        .build()
        .unwrap();

    assert!(!log.is_empty());
}

// =============================================================================
// Payload tests
// =============================================================================

#[test]
fn test_payload_empty() {
    let log = LogEntryBuilder::new().payload(b"").build().unwrap();

    assert!(!log.is_empty());
}

#[test]
fn test_payload_json() {
    let log = LogEntryBuilder::new()
        .payload_json(r#"{"key": "value", "count": 42}"#)
        .build()
        .unwrap();

    assert!(!log.is_empty());
}

#[test]
fn test_payload_binary() {
    let binary_data: Vec<u8> = (0u8..=255).collect();
    let log = LogEntryBuilder::new()
        .payload(&binary_data)
        .build()
        .unwrap();

    assert!(!log.is_empty());
}

#[test]
fn test_payload_large_valid() {
    let large = vec![0xABu8; 1024 * 1024]; // 1 MB
    let log = LogEntryBuilder::new().payload(&large).build().unwrap();

    assert!(!log.is_empty());
}

#[test]
fn test_payload_too_large() {
    let huge = vec![0xABu8; 1024 * 1024 + 1]; // 1 MB + 1 byte
    let result = LogEntryBuilder::new().payload(&huge).build();

    assert!(matches!(result, Err(BuilderError::PayloadTooLarge { .. })));
}

#[test]
fn test_payload_owned() {
    let data = vec![1, 2, 3, 4, 5];
    let log = LogEntryBuilder::new().payload_owned(data).build().unwrap();

    assert!(!log.is_empty());
}

// =============================================================================
// LogDataBuilder tests
// =============================================================================

#[test]
fn test_log_data_single() {
    let log = LogEntryBuilder::new().info().build().unwrap();

    let log_data = LogDataBuilder::new().add(log).build().unwrap();

    assert!(!log_data.is_empty());
}

#[test]
fn test_log_data_multiple() {
    let log1 = LogEntryBuilder::new()
        .error()
        .source("server-1")
        .build()
        .unwrap();

    let log2 = LogEntryBuilder::new()
        .warning()
        .source("server-1")
        .build()
        .unwrap();

    let log3 = LogEntryBuilder::new()
        .info()
        .source("server-2")
        .payload_json(r#"{"status": "ok"}"#)
        .build()
        .unwrap();

    let log_data = LogDataBuilder::new()
        .add(log1)
        .add(log2)
        .add(log3)
        .build()
        .unwrap();

    assert!(!log_data.is_empty());
}

#[test]
fn test_log_data_extend() {
    let logs: Vec<_> = (0..5)
        .map(|i| {
            LogEntryBuilder::new()
                .info()
                .source(&format!("server-{}", i))
                .build()
                .unwrap()
        })
        .collect();

    let log_data = LogDataBuilder::new().extend(logs).build().unwrap();

    assert!(!log_data.is_empty());
}

#[test]
fn test_log_data_empty_error() {
    let result = LogDataBuilder::new().build();

    assert!(matches!(result, Err(BuilderError::EmptyLogData)));
}

#[test]
fn test_log_data_len() {
    let log1 = LogEntryBuilder::new().build().unwrap();
    let log2 = LogEntryBuilder::new().build().unwrap();

    let builder = LogDataBuilder::new().add(log1).add(log2);

    assert_eq!(builder.len(), 2);
    assert!(!builder.is_empty());
}

#[test]
fn test_log_data_is_empty() {
    let builder = LogDataBuilder::new();

    assert!(builder.is_empty());
    assert_eq!(builder.len(), 0);
}

// =============================================================================
// BuiltLogEntry/BuiltLogData tests
// =============================================================================

#[test]
fn test_built_log_entry_as_bytes() {
    let log = LogEntryBuilder::new().build().unwrap();
    let bytes = log.as_bytes();

    assert!(!bytes.is_empty());
}

#[test]
fn test_built_log_entry_into_vec() {
    let log = LogEntryBuilder::new().build().unwrap();
    let vec = log.into_vec();

    assert!(!vec.is_empty());
}

#[test]
fn test_built_log_data_as_bytes() {
    let log = LogEntryBuilder::new().build().unwrap();
    let log_data = LogDataBuilder::new().add(log).build().unwrap();

    let bytes = log_data.as_bytes();
    assert!(!bytes.is_empty());
}

#[test]
fn test_built_log_data_into_vec() {
    let log = LogEntryBuilder::new().build().unwrap();
    let log_data = LogDataBuilder::new().add(log).build().unwrap();

    let vec = log_data.into_vec();
    assert!(!vec.is_empty());
}

// =============================================================================
// Builder clone/default tests
// =============================================================================

#[test]
fn test_log_entry_builder_clone() {
    let builder = LogEntryBuilder::new().error().source("server-1");

    let cloned = builder.clone();

    let log1 = builder.service("service1").build().unwrap();
    let log2 = cloned.service("service2").build().unwrap();

    assert!(!log1.is_empty());
    assert!(!log2.is_empty());
}

#[test]
fn test_log_entry_builder_default() {
    let log = LogEntryBuilder::default().build().unwrap();

    assert!(!log.is_empty());
}

#[test]
fn test_log_data_builder_clone() {
    let log = LogEntryBuilder::new().build().unwrap();
    let builder = LogDataBuilder::new().add(log);

    let _cloned = builder.clone();
}

#[test]
fn test_log_data_builder_default() {
    let builder = LogDataBuilder::default();

    assert!(builder.is_empty());
}

// =============================================================================
// Large batch tests
// =============================================================================

#[test]
fn test_log_data_100_entries() {
    let logs: Vec<_> = (0..100)
        .map(|i| {
            LogEntryBuilder::new()
                .level(LogLevel::from_u8((i % 9) as u8))
                .source(&format!("server-{}", i % 10))
                .service(&format!("service-{}", i % 5))
                .timestamp(1700000000000 + i as u64)
                .payload_json(&format!(r#"{{"index": {}}}"#, i))
                .build()
                .unwrap()
        })
        .collect();

    let log_data = LogDataBuilder::new().extend(logs).build().unwrap();

    assert!(!log_data.is_empty());
    // Should be reasonably sized
    assert!(log_data.len() < 100_000);
}

// =============================================================================
// Roundtrip tests with tell-protocol decoder
// =============================================================================

#[test]
fn test_log_data_roundtrip_single_log() {
    use tell_protocol::decode_log_data;

    let log = LogEntryBuilder::new()
        .error()
        .source("web-01.prod")
        .service("api-gateway")
        .session_id([
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
            0x0f, 0x10,
        ])
        .timestamp(1700000000000)
        .payload_json(r#"{"error": "connection timeout", "retry": 3}"#)
        .build()
        .unwrap();

    let log_data = LogDataBuilder::new().add(log).build().unwrap();

    // Decode using tell-protocol
    let decoded = decode_log_data(log_data.as_bytes()).expect("should decode log data");

    assert_eq!(decoded.len(), 1, "should have decoded 1 log entry");
    let entry = &decoded[0];
    assert_eq!(entry.level, tell_protocol::LogLevel::Error);
    assert_eq!(entry.source, Some("web-01.prod"));
    assert_eq!(entry.service, Some("api-gateway"));
    assert_eq!(entry.timestamp, 1700000000000);
    assert_eq!(
        entry.session_id,
        Some(&[
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
            0x0f, 0x10
        ])
    );
    assert_eq!(
        std::str::from_utf8(entry.payload).unwrap(),
        r#"{"error": "connection timeout", "retry": 3}"#
    );
}

#[test]
fn test_log_data_roundtrip_multiple_logs() {
    use tell_protocol::decode_log_data;

    let log1 = LogEntryBuilder::new()
        .info()
        .source("server-1")
        .timestamp(1000)
        .build()
        .unwrap();

    let log2 = LogEntryBuilder::new()
        .warning()
        .source("server-2")
        .timestamp(2000)
        .build()
        .unwrap();

    let log3 = LogEntryBuilder::new()
        .critical()
        .source("server-3")
        .timestamp(3000)
        .build()
        .unwrap();

    let log_data = LogDataBuilder::new()
        .add(log1)
        .add(log2)
        .add(log3)
        .build()
        .unwrap();

    let decoded = decode_log_data(log_data.as_bytes()).expect("should decode log data");

    assert_eq!(decoded.len(), 3, "should have decoded 3 log entries");

    assert_eq!(decoded[0].level, tell_protocol::LogLevel::Info);
    assert_eq!(decoded[0].source, Some("server-1"));
    assert_eq!(decoded[0].timestamp, 1000);

    assert_eq!(decoded[1].level, tell_protocol::LogLevel::Warning);
    assert_eq!(decoded[1].source, Some("server-2"));
    assert_eq!(decoded[1].timestamp, 2000);

    assert_eq!(decoded[2].level, tell_protocol::LogLevel::Fatal);
    assert_eq!(decoded[2].source, Some("server-3"));
    assert_eq!(decoded[2].timestamp, 3000);
}

#[test]
fn test_full_log_batch_roundtrip() {
    use crate::BatchBuilder;
    use tell_protocol::{FlatBatch, SchemaType, decode_log_data};

    let log = LogEntryBuilder::new()
        .error()
        .source("app-server")
        .service("payments")
        .timestamp(1700000000000)
        .payload_json(r#"{"txn_id": "12345"}"#)
        .build()
        .unwrap();

    let log_data = LogDataBuilder::new().add(log).build().unwrap();

    let batch = BatchBuilder::new()
        .api_key([0x99; 16])
        .log_data(log_data)
        .build()
        .unwrap();

    // Parse the outer Batch
    let fb = FlatBatch::parse(batch.as_bytes()).expect("should parse batch");
    assert_eq!(fb.schema_type(), SchemaType::Log);
    assert_eq!(fb.api_key().unwrap(), &[0x99; 16]);

    // Decode the inner LogData
    let data = fb.data().expect("should have data");
    let decoded = decode_log_data(data).expect("should decode log data");

    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].level, tell_protocol::LogLevel::Error);
    assert_eq!(decoded[0].source, Some("app-server"));
    assert_eq!(decoded[0].service, Some("payments"));
}
