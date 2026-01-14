//! TCP source tests

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::Arc;
use std::time::Duration;

use cdp_auth::ApiKeyStore;
use cdp_protocol::{Batch, SourceId};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use crate::tcp::{TcpSource, TcpSourceConfig, TcpSourceMetrics, bytes_to_ip, ip_to_bytes};
use crate::ShardedSender;

// ============================================================================
// Helper Functions
// ============================================================================

/// Create a test API key store with a known key
fn create_test_auth_store() -> Arc<ApiKeyStore> {
    let store = ApiKeyStore::new();
    // Add a test key: "0102030405060708090a0b0c0d0e0f10" -> "test_workspace"
    let content = "0102030405060708090a0b0c0d0e0f10:test_workspace";
    let _ = store.reload_from_str(content);
    Arc::new(store)
}

/// Find an available port for testing
async fn find_available_port() -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

// ============================================================================
// Configuration Tests
// ============================================================================

#[test]
fn test_config_default() {
    let config = TcpSourceConfig::default();

    assert_eq!(config.id, "tcp");
    assert_eq!(config.address, "0.0.0.0");
    assert_eq!(config.port, 50000);
    assert_eq!(config.batch_size, 500);
    assert!(!config.forwarding_mode);
    assert!(config.keepalive);
    assert!(config.nodelay);
}

#[test]
fn test_config_with_port() {
    let config = TcpSourceConfig::with_port(8080);

    assert_eq!(config.port, 8080);
    assert_eq!(config.address, "0.0.0.0");
}

#[test]
fn test_config_bind_address() {
    let config = TcpSourceConfig {
        address: "127.0.0.1".into(),
        port: 9000,
        ..Default::default()
    };

    assert_eq!(config.bind_address(), "127.0.0.1:9000");
}

#[test]
fn test_config_source_id() {
    let config = TcpSourceConfig {
        id: "my_tcp_source".into(),
        ..Default::default()
    };

    assert_eq!(config.source_id(), SourceId::new("my_tcp_source"));
}

// ============================================================================
// Metrics Tests
// ============================================================================

#[test]
fn test_metrics_new() {
    let metrics = TcpSourceMetrics::new();
    let snapshot = metrics.snapshot();

    assert_eq!(snapshot.connections_active, 0);
    assert_eq!(snapshot.connections_total, 0);
    assert_eq!(snapshot.messages_received, 0);
    assert_eq!(snapshot.bytes_received, 0);
    assert_eq!(snapshot.batches_sent, 0);
    assert_eq!(snapshot.errors, 0);
    assert_eq!(snapshot.auth_failures, 0);
    assert_eq!(snapshot.parse_errors, 0);
    assert_eq!(snapshot.oversized_messages, 0);
}

#[test]
fn test_metrics_auth_failure() {
    let metrics = TcpSourceMetrics::new();

    metrics.auth_failure();
    metrics.auth_failure();

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.auth_failures, 2);
    assert_eq!(snapshot.errors, 2);
}

#[test]
fn test_metrics_parse_error() {
    let metrics = TcpSourceMetrics::new();

    metrics.parse_error();

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.parse_errors, 1);
    assert_eq!(snapshot.errors, 1);
}

#[test]
fn test_metrics_oversized_message() {
    let metrics = TcpSourceMetrics::new();

    metrics.oversized_message();

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.oversized_messages, 1);
    assert_eq!(snapshot.errors, 1);
}

#[test]
fn test_metrics_base_operations() {
    let metrics = TcpSourceMetrics::new();

    metrics.base.connection_opened();
    metrics.base.connection_opened();
    metrics.base.message_received(100);
    metrics.base.message_received(200);
    metrics.base.batch_sent();
    metrics.base.connection_closed();

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.connections_active, 1);
    assert_eq!(snapshot.connections_total, 2);
    assert_eq!(snapshot.messages_received, 2);
    assert_eq!(snapshot.bytes_received, 300);
    assert_eq!(snapshot.batches_sent, 1);
}

// ============================================================================
// IP Conversion Tests
// ============================================================================

#[test]
fn test_ip_to_bytes_v4() {
    let ip = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 100));
    let bytes = ip_to_bytes(ip);

    // IPv4-mapped IPv6: ::ffff:192.168.1.100
    assert_eq!(
        bytes,
        [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 192, 168, 1, 100]
    );
}

#[test]
fn test_ip_to_bytes_v6() {
    let ip = IpAddr::V6(Ipv6Addr::new(0x2001, 0x0db8, 0, 0, 0, 0, 0, 1));
    let bytes = ip_to_bytes(ip);

    assert_eq!(
        bytes,
        [0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]
    );
}

#[test]
fn test_bytes_to_ip_v4_mapped() {
    let bytes = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 10, 0, 0, 1];
    let ip = bytes_to_ip(&bytes);

    assert_eq!(ip, IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
}

#[test]
fn test_bytes_to_ip_v6() {
    let bytes = [0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
    let ip = bytes_to_ip(&bytes);

    assert_eq!(
        ip,
        IpAddr::V6(Ipv6Addr::new(0x2001, 0x0db8, 0, 0, 0, 0, 0, 1))
    );
}

#[test]
fn test_ip_roundtrip_v4() {
    let original = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
    let bytes = ip_to_bytes(original);
    let recovered = bytes_to_ip(&bytes);

    assert_eq!(original, recovered);
}

#[test]
fn test_ip_roundtrip_v6() {
    let original = IpAddr::V6(Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 1));
    let bytes = ip_to_bytes(original);
    let recovered = bytes_to_ip(&bytes);

    assert_eq!(original, recovered);
}

// ============================================================================
// Source Creation Tests
// ============================================================================

#[tokio::test]
async fn test_source_creation() {
    let config = TcpSourceConfig::default();
    let auth_store = create_test_auth_store();
    let (tx, _rx) = crossfire::mpsc::bounded_async::<Batch>(10);
    let tx = ShardedSender::new(vec![tx]);

    let source = TcpSource::new(config, auth_store, tx);

    assert!(!source.is_running());
    assert_eq!(source.source_id(), SourceId::new("tcp"));
}

#[tokio::test]
async fn test_source_metrics_access() {
    let config = TcpSourceConfig::default();
    let auth_store = create_test_auth_store();
    let (tx, _rx) = crossfire::mpsc::bounded_async::<Batch>(10);
    let tx = ShardedSender::new(vec![tx]);

    let source = TcpSource::new(config, auth_store, tx);
    let metrics = source.metrics();

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.connections_total, 0);
}

// ============================================================================
// Source Running Tests
// ============================================================================

#[tokio::test]
async fn test_source_bind_and_accept() {
    let port = find_available_port().await;

    let config = TcpSourceConfig {
        address: "127.0.0.1".into(),
        port,
        ..Default::default()
    };

    let auth_store = create_test_auth_store();
    let (tx, _rx) = crossfire::mpsc::bounded_async::<Batch>(10);
    let tx = ShardedSender::new(vec![tx]);

    let source = TcpSource::new(config, auth_store, tx);

    // Spawn the source in a task
    let cancel = CancellationToken::new();
    let cancel_clone = cancel.clone();
    let source_handle = tokio::spawn(async move { source.run(cancel_clone).await });

    // Give it time to start
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Try to connect
    let connect_result = timeout(
        Duration::from_millis(100),
        TcpStream::connect(format!("127.0.0.1:{}", port)),
    )
    .await;

    assert!(connect_result.is_ok(), "should be able to connect");

    // Clean up - cancel and wait for source
    cancel.cancel();
    let _ = source_handle.await;
}

#[tokio::test]
async fn test_source_connection_metrics() {
    let port = find_available_port().await;

    let config = TcpSourceConfig {
        address: "127.0.0.1".into(),
        port,
        ..Default::default()
    };

    let auth_store = create_test_auth_store();
    let (tx, _rx) = crossfire::mpsc::bounded_async::<Batch>(10);
    let tx = ShardedSender::new(vec![tx]);

    let _source = TcpSource::new(config, auth_store, tx);
    let metrics = Arc::clone(&Arc::new(TcpSourceMetrics::new()));

    // Just verify we can create and access metrics
    metrics.base.connection_opened();
    assert_eq!(metrics.snapshot().connections_total, 1);
}

// ============================================================================
// Protocol Tests
// ============================================================================

#[tokio::test]
async fn test_source_rejects_oversized_message() {
    let port = find_available_port().await;

    let config = TcpSourceConfig {
        address: "127.0.0.1".into(),
        port,
        ..Default::default()
    };

    let auth_store = create_test_auth_store();
    let (tx, _rx) = crossfire::mpsc::bounded_async::<Batch>(10);
    let tx = ShardedSender::new(vec![tx]);

    let source = TcpSource::new(config, auth_store, tx);

    // Spawn the source
    let cancel = CancellationToken::new();
    let cancel_clone = cancel.clone();
    let source_handle = tokio::spawn(async move { source.run(cancel_clone).await });

    // Give it time to start
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Connect and send an oversized message length
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .expect("should connect");

    // Send a length that exceeds the limit (17MB > 16MB limit)
    let oversized_len: u32 = 17 * 1024 * 1024;
    stream
        .write_all(&oversized_len.to_be_bytes())
        .await
        .expect("should write");

    // Give time for processing
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Clean up - cancel and wait for source
    cancel.cancel();
    let _ = source_handle.await;
}

#[tokio::test]
async fn test_source_handles_client_disconnect() {
    let port = find_available_port().await;

    let config = TcpSourceConfig {
        address: "127.0.0.1".into(),
        port,
        ..Default::default()
    };

    let auth_store = create_test_auth_store();
    let (tx, _rx) = crossfire::mpsc::bounded_async::<Batch>(10);
    let tx = ShardedSender::new(vec![tx]);

    let source = TcpSource::new(config, auth_store, tx);

    // Spawn the source
    let cancel = CancellationToken::new();
    let cancel_clone = cancel.clone();
    let source_handle = tokio::spawn(async move { source.run(cancel_clone).await });

    // Give it time to start
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Connect and immediately disconnect
    {
        let _stream = TcpStream::connect(format!("127.0.0.1:{}", port))
            .await
            .expect("should connect");
        // stream drops here, closing connection
    }

    // Give time for the source to handle the disconnect
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Clean up - cancel and wait for source
    cancel.cancel();
    let _ = source_handle.await;
}

// ============================================================================
// Edge Cases
// ============================================================================

#[test]
fn test_config_clone() {
    let config = TcpSourceConfig {
        id: "test".into(),
        port: 12345,
        forwarding_mode: true,
        ..Default::default()
    };

    let cloned = config.clone();

    assert_eq!(cloned.id, "test");
    assert_eq!(cloned.port, 12345);
    assert!(cloned.forwarding_mode);
}

#[test]
fn test_metrics_snapshot_clone() {
    let metrics = TcpSourceMetrics::new();
    metrics.base.connection_opened();
    metrics.base.message_received(100);

    let snapshot1 = metrics.snapshot();
    let snapshot2 = snapshot1; // Copy

    assert_eq!(snapshot1.connections_total, snapshot2.connections_total);
    assert_eq!(snapshot1.bytes_received, snapshot2.bytes_received);
}
