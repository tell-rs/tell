//! Tests for tap server
//!
//! Note: Full integration tests require starting the server,
//! which is better done in a separate integration test file.

use super::*;
use tell_protocol::{BatchBuilder, BatchType, SourceId};

/// Helper to create a test batch
fn make_batch(workspace_id: u32, source_id: &str, batch_type: BatchType) -> Batch {
    let mut builder = BatchBuilder::new(batch_type, SourceId::new(source_id.to_string()));
    builder.set_workspace_id(workspace_id);
    builder.finish()
}

// ============================================================================
// Config tests
// ============================================================================

#[test]
fn test_default_config() {
    let config = TapServerConfig::default();
    assert_eq!(config.socket_path.to_str().unwrap(), DEFAULT_SOCKET_PATH);
    assert_eq!(config.max_connections, 100);
    assert_eq!(config.heartbeat_interval_secs, 30);
}

#[test]
fn test_config_with_socket_path() {
    let config = TapServerConfig::default().with_socket_path("/tmp/custom.sock");
    assert_eq!(config.socket_path.to_str().unwrap(), "/tmp/custom.sock");
}

// ============================================================================
// Envelope conversion tests
// ============================================================================

#[test]
fn test_batch_to_envelope() {
    let batch = make_batch(42, "tcp_main", BatchType::Event);
    let envelope = batch_to_envelope(&batch);

    assert_eq!(envelope.workspace_id, 42);
    assert_eq!(envelope.source_id, "tcp_main");
    assert_eq!(envelope.batch_type, 1); // Event = 1
}

#[test]
fn test_batch_to_envelope_preserves_metadata() {
    let mut builder = BatchBuilder::new(BatchType::Log, SourceId::new("syslog".to_string()));
    builder.set_workspace_id(123);
    let batch = builder.finish();

    let envelope = batch_to_envelope(&batch);

    assert_eq!(envelope.workspace_id, 123);
    assert_eq!(envelope.source_id, "syslog");
    assert_eq!(envelope.batch_type, 2); // Log = 2
    assert_eq!(envelope.count, 0); // Empty batch
}
