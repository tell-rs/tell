//! Tests for TCP Debug Source

use std::sync::Arc;
use std::sync::atomic::Ordering;

use tell_auth::ApiKeyStore;

use crate::tcp_debug::{TcpDebugSource, TcpDebugSourceConfig, TcpDebugSourceMetrics};
use crate::ShardedSender;

#[test]
fn test_config_defaults() {
    let config = TcpDebugSourceConfig::default();

    assert_eq!(config.port, 50001);
    assert_eq!(config.address, "0.0.0.0");
    assert_eq!(config.id, "tcp_debug");
    assert!(config.nodelay);
    assert_eq!(config.socket_buffer_size, 256 * 1024);
}

#[test]
fn test_config_bind_address() {
    let config = TcpDebugSourceConfig {
        address: "127.0.0.1".into(),
        port: 50002,
        ..Default::default()
    };
    assert_eq!(config.bind_address(), "127.0.0.1:50002");
}

#[test]
fn test_config_source_id() {
    let config = TcpDebugSourceConfig {
        id: "my_debug".into(),
        ..Default::default()
    };
    assert_eq!(config.source_id().as_str(), "my_debug");
}

#[test]
fn test_metrics_new() {
    let metrics = TcpDebugSourceMetrics::new();

    assert_eq!(metrics.auth_failures.load(Ordering::Relaxed), 0);
    assert_eq!(metrics.parse_errors.load(Ordering::Relaxed), 0);
    assert_eq!(metrics.oversized_messages.load(Ordering::Relaxed), 0);
}

#[test]
fn test_metrics_tracking() {
    let metrics = TcpDebugSourceMetrics::new();

    metrics.auth_failure();
    metrics.parse_error();
    metrics.oversized_message();

    assert_eq!(metrics.auth_failures.load(Ordering::Relaxed), 1);
    assert_eq!(metrics.parse_errors.load(Ordering::Relaxed), 1);
    assert_eq!(metrics.oversized_messages.load(Ordering::Relaxed), 1);
    assert_eq!(metrics.base.errors.load(Ordering::Relaxed), 3);
}

#[tokio::test]
async fn test_source_creation() {
    let config = TcpDebugSourceConfig::default();
    let auth_store = Arc::new(ApiKeyStore::new());
    let (tx, _rx) = crossfire::mpsc::bounded_async(100);
    let sharded_sender = ShardedSender::new(vec![tx]);

    let source = TcpDebugSource::new(config, auth_store, sharded_sender);

    assert!(!source.is_running());
}

#[tokio::test]
async fn test_source_stop() {
    let config = TcpDebugSourceConfig::default();
    let auth_store = Arc::new(ApiKeyStore::new());
    let (tx, _rx) = crossfire::mpsc::bounded_async(100);
    let sharded_sender = ShardedSender::new(vec![tx]);

    let source = TcpDebugSource::new(config, auth_store, sharded_sender);

    source.stop();
    assert!(!source.is_running());
}
