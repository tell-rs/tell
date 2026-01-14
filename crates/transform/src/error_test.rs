//! Tests for transform error types

use super::*;

#[test]
fn test_error_creation() {
    let err = TransformError::decode("invalid flatbuffer");
    assert!(matches!(err, TransformError::DecodeError(_)));

    let err = TransformError::failed("pattern matching failed");
    assert!(matches!(err, TransformError::TransformFailed(_)));

    let err = TransformError::config("missing pattern file");
    assert!(matches!(err, TransformError::Config(_)));

    let err = TransformError::not_initialized("pattern_matcher");
    assert!(matches!(err, TransformError::NotInitialized(_)));
}

#[test]
fn test_error_display() {
    let err = TransformError::decode("bad data");
    assert_eq!(err.to_string(), "failed to decode message: bad data");

    let err = TransformError::failed("logic error");
    assert_eq!(err.to_string(), "transform failed: logic error");

    let err = TransformError::config("invalid threshold");
    assert_eq!(err.to_string(), "invalid configuration: invalid threshold");

    let err = TransformError::not_initialized("pattern_matcher");
    assert_eq!(
        err.to_string(),
        "transformer not initialized: pattern_matcher"
    );

    let err = TransformError::Cancelled;
    assert_eq!(err.to_string(), "operation cancelled");
}

#[test]
fn test_io_error_conversion() {
    let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
    let err: TransformError = io_err.into();
    assert!(matches!(err, TransformError::Io(_)));
    assert!(err.to_string().contains("file not found"));
}
