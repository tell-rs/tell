//! Tests for protocol error types

use crate::error::ProtocolError;

#[test]
fn test_error_creation_too_short() {
    let err = ProtocolError::too_short(100, 50);
    assert!(matches!(
        err,
        ProtocolError::MessageTooShort {
            expected: 100,
            actual: 50
        }
    ));
}

#[test]
fn test_error_creation_invalid_flatbuffer() {
    let err = ProtocolError::invalid_flatbuffer("bad vtable");
    assert!(matches!(err, ProtocolError::InvalidFlatBuffer(_)));
}

#[test]
fn test_error_creation_missing_field() {
    let err = ProtocolError::missing_field("api_key");
    assert!(matches!(err, ProtocolError::MissingField("api_key")));
}

#[test]
fn test_error_creation_invalid_api_key() {
    let err = ProtocolError::invalid_api_key_length(8);
    assert!(matches!(
        err,
        ProtocolError::InvalidApiKeyLength {
            expected: 16,
            actual: 8
        }
    ));
}

#[test]
fn test_error_creation_invalid_source_ip() {
    let err = ProtocolError::invalid_source_ip_length(4);
    assert!(matches!(
        err,
        ProtocolError::InvalidSourceIpLength {
            expected: 16,
            actual: 4
        }
    ));
}

#[test]
fn test_error_display_too_short() {
    let err = ProtocolError::too_short(100, 50);
    assert_eq!(
        err.to_string(),
        "message too short: expected at least 100 bytes, got 50"
    );
}

#[test]
fn test_error_display_invalid_schema_type() {
    let err = ProtocolError::InvalidSchemaType(255);
    assert_eq!(err.to_string(), "invalid schema type: 255");
}

#[test]
fn test_error_display_empty_data() {
    let err = ProtocolError::EmptyData;
    assert_eq!(err.to_string(), "empty data payload");
}

#[test]
fn test_error_display_missing_field() {
    let err = ProtocolError::missing_field("data");
    assert_eq!(err.to_string(), "missing required field: data");
}

#[test]
fn test_error_display_buffer_overflow() {
    let err = ProtocolError::BufferOverflow {
        size: 1000,
        max: 500,
    };
    assert_eq!(
        err.to_string(),
        "buffer overflow: message size 1000 exceeds maximum 500"
    );
}

#[test]
fn test_error_display_batch_full() {
    let err = ProtocolError::BatchFull {
        count: 500,
        max: 500,
    };
    assert_eq!(
        err.to_string(),
        "batch is full: contains 500 items (max 500)"
    );
}

#[test]
fn test_is_recoverable_true_cases() {
    assert!(ProtocolError::InvalidSchemaType(255).is_recoverable());
    assert!(ProtocolError::EmptyData.is_recoverable());
    assert!(
        ProtocolError::BatchFull {
            count: 500,
            max: 500
        }
        .is_recoverable()
    );
}

#[test]
fn test_is_recoverable_false_cases() {
    assert!(!ProtocolError::too_short(100, 50).is_recoverable());
    assert!(!ProtocolError::invalid_flatbuffer("bad").is_recoverable());
    assert!(!ProtocolError::missing_field("api_key").is_recoverable());
    assert!(!ProtocolError::invalid_api_key_length(8).is_recoverable());
    assert!(!ProtocolError::invalid_source_ip_length(4).is_recoverable());
    assert!(
        !ProtocolError::BufferOverflow {
            size: 1000,
            max: 500
        }
        .is_recoverable()
    );
}
