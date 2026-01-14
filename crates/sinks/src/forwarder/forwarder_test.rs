use super::*;
use cdp_client::BatchBuilder as FlatBufferBuilder;
use cdp_protocol::{BatchBuilder, BatchType, SchemaType, SourceId};
use std::net::{Ipv4Addr, Ipv6Addr};
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;

// =============================================================================
// Config tests
// =============================================================================

#[test]
fn test_config_new_valid() {
    let config = ForwarderConfig::new("localhost:8081", "0123456789abcdef0123456789abcdef");
    assert!(config.is_ok());

    let config = config.unwrap();
    assert_eq!(config.target, "localhost:8081");
    assert_eq!(config.api_key, "0123456789abcdef0123456789abcdef");
    assert_eq!(config.api_key_bytes[0], 0x01);
    assert_eq!(config.api_key_bytes[15], 0xef);
}

#[test]
fn test_config_new_invalid_api_key_short() {
    let result = ForwarderConfig::new("localhost:8081", "0123456789abcdef");
    assert!(result.is_err());

    let err = result.unwrap_err();
    assert!(matches!(err, ForwarderError::InvalidApiKey(_)));
}

#[test]
fn test_config_new_invalid_api_key_not_hex() {
    let result = ForwarderConfig::new("localhost:8081", "GHIJKLMNOPQRSTUV0123456789abcdef");
    assert!(result.is_err());
}

#[test]
fn test_config_defaults() {
    let config =
        ForwarderConfig::new("localhost:8081", "0123456789abcdef0123456789abcdef").unwrap();

    assert_eq!(config.connection_timeout, Duration::from_secs(10));
    assert_eq!(config.write_timeout, Duration::from_secs(5));
    assert_eq!(config.retry_attempts, 3);
    assert_eq!(config.retry_interval, Duration::from_secs(1));
    assert_eq!(config.reconnect_interval, Duration::from_secs(5));
}

#[test]
fn test_config_builders() {
    let config = ForwarderConfig::new("localhost:8081", "0123456789abcdef0123456789abcdef")
        .unwrap()
        .with_connection_timeout(Duration::from_secs(30))
        .with_write_timeout(Duration::from_secs(10))
        .with_retry_attempts(5)
        .with_retry_interval(Duration::from_millis(500))
        .with_reconnect_interval(Duration::from_secs(10));

    assert_eq!(config.connection_timeout, Duration::from_secs(30));
    assert_eq!(config.write_timeout, Duration::from_secs(10));
    assert_eq!(config.retry_attempts, 5);
    assert_eq!(config.retry_interval, Duration::from_millis(500));
    assert_eq!(config.reconnect_interval, Duration::from_secs(10));
}

// =============================================================================
// API key decoding tests
// =============================================================================

#[test]
fn test_decode_api_key_valid() {
    let hex = "0123456789abcdef0123456789abcdef";
    let result = decode_api_key(hex);
    assert!(result.is_ok());

    let bytes = result.unwrap();
    assert_eq!(bytes[0], 0x01);
    assert_eq!(bytes[1], 0x23);
    assert_eq!(bytes[2], 0x45);
    assert_eq!(bytes[7], 0xef);
    assert_eq!(bytes[15], 0xef);
}

#[test]
fn test_decode_api_key_uppercase() {
    let hex = "0123456789ABCDEF0123456789ABCDEF";
    let result = decode_api_key(hex);
    assert!(result.is_ok());
}

#[test]
fn test_decode_api_key_too_short() {
    let hex = "0123456789abcdef";
    let result = decode_api_key(hex);
    assert!(result.is_err());
}

#[test]
fn test_decode_api_key_too_long() {
    let hex = "0123456789abcdef0123456789abcdef00";
    let result = decode_api_key(hex);
    assert!(result.is_err());
}

#[test]
fn test_decode_api_key_invalid_hex() {
    let hex = "0123456789abcdefGHIJKLMNOPQRSTUV";
    let result = decode_api_key(hex);
    assert!(result.is_err());
}

// =============================================================================
// IP conversion tests
// =============================================================================

#[test]
fn test_ip_to_bytes_v4() {
    let ip = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 100));
    let bytes = ip_to_bytes(ip);

    // Should be IPv4-mapped IPv6: ::ffff:192.168.1.100
    assert_eq!(bytes[0..10], [0u8; 10]);
    assert_eq!(bytes[10], 0xff);
    assert_eq!(bytes[11], 0xff);
    assert_eq!(bytes[12], 192);
    assert_eq!(bytes[13], 168);
    assert_eq!(bytes[14], 1);
    assert_eq!(bytes[15], 100);
}

#[test]
fn test_ip_to_bytes_v6() {
    let ip = IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1));
    let bytes = ip_to_bytes(ip);

    assert_eq!(bytes[0], 0x20);
    assert_eq!(bytes[1], 0x01);
    assert_eq!(bytes[2], 0x0d);
    assert_eq!(bytes[3], 0xb8);
    assert_eq!(bytes[14], 0x00);
    assert_eq!(bytes[15], 0x01);
}

#[test]
fn test_ip_to_bytes_localhost_v4() {
    let ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
    let bytes = ip_to_bytes(ip);

    assert_eq!(bytes[10], 0xff);
    assert_eq!(bytes[11], 0xff);
    assert_eq!(bytes[12], 127);
    assert_eq!(bytes[13], 0);
    assert_eq!(bytes[14], 0);
    assert_eq!(bytes[15], 1);
}

// =============================================================================
// Metrics tests
// =============================================================================

#[test]
fn test_metrics_new() {
    let metrics = ForwarderMetrics::new();
    let snapshot = metrics.snapshot();

    assert_eq!(snapshot.batches_received, 0);
    assert_eq!(snapshot.batches_sent, 0);
    assert_eq!(snapshot.batches_failed, 0);
    assert_eq!(snapshot.bytes_sent, 0);
    assert_eq!(snapshot.messages_forwarded, 0);
    assert_eq!(snapshot.reconnect_count, 0);
}

#[test]
fn test_metrics_record_received() {
    let metrics = ForwarderMetrics::new();

    metrics.record_received();
    metrics.record_received();
    metrics.record_received();

    assert_eq!(metrics.snapshot().batches_received, 3);
}

#[test]
fn test_metrics_record_sent() {
    let metrics = ForwarderMetrics::new();

    metrics.record_sent(100, 5000);
    metrics.record_sent(200, 10000);

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.batches_sent, 2);
    assert_eq!(snapshot.messages_forwarded, 300);
    assert_eq!(snapshot.bytes_sent, 15000);
}

#[test]
fn test_metrics_record_failed() {
    let metrics = ForwarderMetrics::new();

    metrics.record_failed();
    metrics.record_failed();

    assert_eq!(metrics.snapshot().batches_failed, 2);
}

#[test]
fn test_metrics_record_reconnect() {
    let metrics = ForwarderMetrics::new();

    metrics.record_reconnect();

    assert_eq!(metrics.snapshot().reconnect_count, 1);
}

#[test]
fn test_metrics_snapshot_default() {
    let snapshot = MetricsSnapshot::default();

    assert_eq!(snapshot.batches_received, 0);
    assert_eq!(snapshot.batches_sent, 0);
    assert_eq!(snapshot.batches_failed, 0);
    assert_eq!(snapshot.bytes_sent, 0);
    assert_eq!(snapshot.messages_forwarded, 0);
    assert_eq!(snapshot.reconnect_count, 0);
}

// =============================================================================
// Sink creation tests
// =============================================================================

#[test]
fn test_sink_creation() {
    let (_, rx) = mpsc::channel::<Arc<Batch>>(10);
    let config =
        ForwarderConfig::new("localhost:8081", "0123456789abcdef0123456789abcdef").unwrap();
    let sink = ForwarderSink::new(config, rx);

    assert_eq!(sink.name(), "forwarder");
    assert_eq!(sink.metrics().snapshot().batches_received, 0);
}

#[test]
fn test_sink_with_custom_name() {
    let (_, rx) = mpsc::channel::<Arc<Batch>>(10);
    let config =
        ForwarderConfig::new("localhost:8081", "0123456789abcdef0123456789abcdef").unwrap();
    let sink = ForwarderSink::with_name(config, rx, "custom_forwarder");

    assert_eq!(sink.name(), "custom_forwarder");
}

// =============================================================================
// Sink run tests
// =============================================================================

#[tokio::test]
async fn test_sink_run_empty_channel() {
    let (tx, rx) = mpsc::channel::<Arc<Batch>>(10);
    let config = ForwarderConfig::new("localhost:19999", "0123456789abcdef0123456789abcdef")
        .unwrap()
        .with_connection_timeout(Duration::from_millis(100));
    let sink = ForwarderSink::new(config, rx);

    // Close channel immediately
    drop(tx);

    // Should complete without hanging
    let snapshot = sink.run().await;
    assert_eq!(snapshot.batches_received, 0);
}

#[tokio::test]
async fn test_sink_connection_failure_continues() {
    let (tx, rx) = mpsc::channel::<Arc<Batch>>(10);
    // Use a port that's unlikely to have a listener
    let config = ForwarderConfig::new("127.0.0.1:19998", "0123456789abcdef0123456789abcdef")
        .unwrap()
        .with_connection_timeout(Duration::from_millis(50))
        .with_retry_attempts(1)
        .with_reconnect_interval(Duration::from_millis(10));
    let sink = ForwarderSink::new(config, rx);

    // Create a batch with a valid FlatBuffer message
    let fb_msg = create_test_flatbuffer_message();

    let source_id = SourceId::new("test");
    let mut builder = BatchBuilder::new(BatchType::Event, source_id);
    builder.set_workspace_id(42);
    builder.add(&fb_msg, 1);
    let batch = Arc::new(builder.finish());

    tx.send(batch).await.expect("failed to send");
    drop(tx);

    // Should complete (batch will fail but sink won't hang)
    let snapshot = sink.run().await;

    // Batch was received but sending failed
    assert_eq!(snapshot.batches_received, 1);
}

// =============================================================================
// Integration test with mock server
// =============================================================================

#[tokio::test]
async fn test_sink_sends_to_server() {
    // Start a mock server
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("failed to bind");
    let addr = listener.local_addr().expect("failed to get addr");

    // Spawn server task to receive one message
    let server_handle = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("failed to accept");

        // Read length prefix (4 bytes, big-endian)
        let mut len_buf = [0u8; 4];
        socket
            .read_exact(&mut len_buf)
            .await
            .expect("failed to read length");
        let msg_len = u32::from_be_bytes(len_buf) as usize;

        // Read message
        let mut msg_buf = vec![0u8; msg_len];
        socket
            .read_exact(&mut msg_buf)
            .await
            .expect("failed to read message");

        msg_buf
    });

    // Create sink
    let (tx, rx) = mpsc::channel::<Arc<Batch>>(10);
    let config = ForwarderConfig::new(
        format!("127.0.0.1:{}", addr.port()),
        "0123456789abcdef0123456789abcdef",
    )
    .unwrap()
    .with_connection_timeout(Duration::from_secs(5));

    let sink = ForwarderSink::new(config, rx);

    // Run sink in background
    let sink_handle = tokio::spawn(async move { sink.run().await });

    // Create and send a test batch with a real FlatBuffer message
    let fb_msg = create_test_flatbuffer_message();

    let source_id = SourceId::new("test");
    let mut builder = BatchBuilder::new(BatchType::Event, source_id);
    builder.set_workspace_id(42);
    builder.set_source_ip(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
    builder.add(&fb_msg, 1);
    let batch = Arc::new(builder.finish());

    tx.send(batch).await.expect("failed to send");
    drop(tx);

    // Wait for sink to complete
    let snapshot = sink_handle.await.expect("sink task failed");

    assert_eq!(snapshot.batches_received, 1);
    assert_eq!(snapshot.batches_sent, 1);
    assert_eq!(snapshot.messages_forwarded, 1);

    // Verify server received something
    let received = server_handle.await.expect("server task failed");
    assert!(!received.is_empty());

    // Verify the received message is a valid FlatBuffer
    let parsed = FlatBatch::parse(&received);
    assert!(
        parsed.is_ok(),
        "received message should be valid FlatBuffer"
    );
}

#[tokio::test]
async fn test_sink_multiple_messages_in_batch() {
    // Start a mock server
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("failed to bind");
    let addr = listener.local_addr().expect("failed to get addr");

    // Spawn server task to receive multiple messages
    let server_handle = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("failed to accept");

        let mut message_count = 0;

        // Read 3 messages
        for _ in 0..3 {
            let mut len_buf = [0u8; 4];
            if socket.read_exact(&mut len_buf).await.is_ok() {
                let msg_len = u32::from_be_bytes(len_buf) as usize;
                let mut msg_buf = vec![0u8; msg_len];
                if socket.read_exact(&mut msg_buf).await.is_ok() {
                    message_count += 1;
                }
            }
        }

        message_count
    });

    // Create sink
    let (tx, rx) = mpsc::channel::<Arc<Batch>>(10);
    let config = ForwarderConfig::new(
        format!("127.0.0.1:{}", addr.port()),
        "0123456789abcdef0123456789abcdef",
    )
    .unwrap();

    let sink = ForwarderSink::new(config, rx);

    let sink_handle = tokio::spawn(async move { sink.run().await });

    // Create batch with multiple messages
    let source_id = SourceId::new("test");
    let mut builder = BatchBuilder::new(BatchType::Event, source_id);
    builder.set_workspace_id(42);
    builder.set_source_ip(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));

    let fb_msg = create_test_flatbuffer_message();
    builder.add(&fb_msg, 1);
    builder.add(&fb_msg, 1);
    builder.add(&fb_msg, 1);

    let batch = Arc::new(builder.finish());

    tx.send(batch).await.expect("failed to send");
    drop(tx);

    let snapshot = sink_handle.await.expect("sink task failed");

    assert_eq!(snapshot.batches_received, 1);
    assert_eq!(snapshot.batches_sent, 1);
    assert_eq!(snapshot.messages_forwarded, 3);

    let count = server_handle.await.expect("server task failed");
    assert_eq!(count, 3);
}

#[tokio::test]
async fn test_sink_preserves_source_ip() {
    // Start a mock server
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("failed to bind");
    let addr = listener.local_addr().expect("failed to get addr");

    // Spawn server task
    let server_handle = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("failed to accept");

        let mut len_buf = [0u8; 4];
        socket.read_exact(&mut len_buf).await.expect("read length");
        let msg_len = u32::from_be_bytes(len_buf) as usize;

        let mut msg_buf = vec![0u8; msg_len];
        socket.read_exact(&mut msg_buf).await.expect("read message");

        msg_buf
    });

    // Create sink
    let (tx, rx) = mpsc::channel::<Arc<Batch>>(10);
    let config = ForwarderConfig::new(
        format!("127.0.0.1:{}", addr.port()),
        "aabbccddeeff00112233445566778899",
    )
    .unwrap();

    let sink = ForwarderSink::new(config, rx);
    let sink_handle = tokio::spawn(async move { sink.run().await });

    // Create batch with specific source IP
    let fb_msg = create_test_flatbuffer_message();
    let source_id = SourceId::new("test");
    let mut builder = BatchBuilder::new(BatchType::Event, source_id);
    builder.set_workspace_id(42);
    builder.set_source_ip(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 100)));
    builder.add(&fb_msg, 1);
    let batch = Arc::new(builder.finish());

    tx.send(batch).await.expect("failed to send");
    drop(tx);

    let _ = sink_handle.await.expect("sink task failed");
    let received = server_handle.await.expect("server task failed");

    // Parse and verify the source_ip was set
    let parsed = FlatBatch::parse(&received).expect("parse received");
    assert!(parsed.has_source_ip());

    let source_ip = parsed.source_ip().expect("get source_ip").expect("has ip");
    // Should be IPv4-mapped: ::ffff:192.168.1.100
    assert_eq!(source_ip[10], 0xff);
    assert_eq!(source_ip[11], 0xff);
    assert_eq!(source_ip[12], 192);
    assert_eq!(source_ip[13], 168);
    assert_eq!(source_ip[14], 1);
    assert_eq!(source_ip[15], 100);
}

#[tokio::test]
async fn test_sink_uses_soc_api_key() {
    // Start a mock server
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("failed to bind");
    let addr = listener.local_addr().expect("failed to get addr");

    let server_handle = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("failed to accept");

        let mut len_buf = [0u8; 4];
        socket.read_exact(&mut len_buf).await.expect("read length");
        let msg_len = u32::from_be_bytes(len_buf) as usize;

        let mut msg_buf = vec![0u8; msg_len];
        socket.read_exact(&mut msg_buf).await.expect("read message");

        msg_buf
    });

    // Create sink with specific target API key
    let target_api_key = "aabbccddeeff00112233445566778899";
    let (tx, rx) = mpsc::channel::<Arc<Batch>>(10);
    let config =
        ForwarderConfig::new(format!("127.0.0.1:{}", addr.port()), target_api_key).unwrap();

    let sink = ForwarderSink::new(config, rx);
    let sink_handle = tokio::spawn(async move { sink.run().await });

    // Create batch (original has different API key)
    let fb_msg = create_test_flatbuffer_message(); // has [0x01; 16] as api_key
    let source_id = SourceId::new("test");
    let mut builder = BatchBuilder::new(BatchType::Event, source_id);
    builder.set_workspace_id(42);
    builder.add(&fb_msg, 1);
    let batch = Arc::new(builder.finish());

    tx.send(batch).await.expect("failed to send");
    drop(tx);

    let _ = sink_handle.await.expect("sink task failed");
    let received = server_handle.await.expect("server task failed");

    // Parse and verify the API key was replaced with target key
    let parsed = FlatBatch::parse(&received).expect("parse received");
    let api_key = parsed.api_key().expect("get api_key");

    // Should be the target API key, not the original
    let expected: [u8; 16] = [
        0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88,
        0x99,
    ];
    assert_eq!(api_key, &expected);
}

// =============================================================================
// Error tests
// =============================================================================

#[test]
fn test_error_display() {
    let err = ForwarderError::InvalidApiKey("too short".into());
    assert!(err.to_string().contains("too short"));

    let err = ForwarderError::ConnectionFailed {
        target: "localhost:8081".into(),
        source: std::io::Error::new(ErrorKind::ConnectionRefused, "refused"),
    };
    assert!(err.to_string().contains("localhost:8081"));

    let err = ForwarderError::RetriesExhausted {
        attempts: 3,
        last_error: "timeout".into(),
    };
    assert!(err.to_string().contains("3"));
    assert!(err.to_string().contains("timeout"));

    let err = ForwarderError::NoConnection;
    assert!(err.to_string().contains("no connection"));

    let err = ForwarderError::Timeout;
    assert!(err.to_string().contains("timed out"));

    let err = ForwarderError::BuildError("missing data".into());
    assert!(err.to_string().contains("missing data"));
}

// =============================================================================
// Helper functions
// =============================================================================

/// Create a valid FlatBuffer message using cdp-client::BatchBuilder
fn create_test_flatbuffer_message() -> Vec<u8> {
    FlatBufferBuilder::new()
        .api_key([0x01; 16])
        .schema_type(SchemaType::Event)
        .version(100)
        .data(b"test event payload data")
        .build()
        .expect("build test message")
        .as_bytes()
        .to_vec()
}
