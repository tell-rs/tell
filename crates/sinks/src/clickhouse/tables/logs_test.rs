//! Tests for log table row types

use uuid::Uuid;

use super::logs::{LogLevelEnum, LogRow};

#[test]
fn test_log_row_creation() {
    let row = LogRow {
        timestamp: 1700000000000,
        level: LogLevelEnum::Info,
        source: "web-server-1".to_string(),
        service: "nginx".to_string(),
        session_id: Uuid::from_bytes([0x03; 16]),
        source_ip: [0; 16],
        pattern_id: Some(12345),
        message: "GET /api/health 200".to_string(),
        raw: "GET /api/health 200".to_string(),
    };

    assert_eq!(row.level, LogLevelEnum::Info);
    assert_eq!(row.service, "nginx");
    assert_eq!(row.pattern_id, Some(12345));
}

#[test]
fn test_log_row_without_pattern_id() {
    let row = LogRow {
        timestamp: 1700000000000,
        level: LogLevelEnum::Error,
        source: "api-server".to_string(),
        service: "api".to_string(),
        session_id: Uuid::nil(),
        source_ip: [0; 16],
        pattern_id: None,
        message: "Connection refused".to_string(),
        raw: "Connection refused".to_string(),
    };

    assert_eq!(row.pattern_id, None);
    assert_eq!(row.level, LogLevelEnum::Error);
}
