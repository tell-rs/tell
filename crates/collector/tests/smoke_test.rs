//! Smoke tests for the CDP Collector
//!
//! These tests verify end-to-end functionality by sending data through
//! actual sources and verifying it reaches the expected destinations.

use std::sync::Arc;
use std::time::Duration;

use cdp_auth::{ApiKeyStore, WorkspaceId};
use cdp_client::BatchBuilder;
use cdp_client::event::{EventBuilder, EventDataBuilder};
use cdp_client::test::{SyslogTcpTestClient, SyslogUdpTestClient, TcpTestClient};
use cdp_protocol::{Batch, BatchType};
use cdp_sources::tcp::{TcpSource, TcpSourceConfig};
use cdp_sources::{ShardedSender, SyslogTcpSource, SyslogTcpSourceConfig, SyslogUdpSource, SyslogUdpSourceConfig};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

/// Test API key (all zeros for simplicity)
const TEST_API_KEY: [u8; 16] = [0x01; 16];

/// Test port for TCP source (high port unlikely to conflict)
const TEST_TCP_PORT: u16 = 51234;

/// Create a test auth store with a single API key
fn create_test_auth_store() -> Arc<ApiKeyStore> {
    let store = ApiKeyStore::new();
    store.insert(TEST_API_KEY, WorkspaceId::new(1));
    Arc::new(store)
}

/// Build a test event batch
fn build_test_batch() -> cdp_client::batch::BuiltBatch {
    // Build an event
    let event = EventBuilder::new()
        .track("test_event")
        .device_id([0x01; 16])
        .timestamp_now()
        .payload_json(r#"{"test": true}"#)
        .build()
        .expect("failed to build event");

    // Batch the event
    let event_data = EventDataBuilder::new()
        .add(event)
        .build()
        .expect("failed to build event data");

    // Wrap in Batch
    BatchBuilder::new()
        .api_key(TEST_API_KEY)
        .event_data(event_data)
        .build()
        .expect("failed to build batch")
}

#[tokio::test]
async fn test_tcp_source_receives_batch() {
    // Set up auth store
    let auth_store = create_test_auth_store();

    // Create channel for receiving batches
    let (tx, mut batch_rx) = crossfire::mpsc::bounded_async::<Batch>(100);
    let batch_tx = ShardedSender::new(vec![tx]);

    // Create TCP source config
    let config = TcpSourceConfig {
        id: "tcp_test".to_string(),
        address: "127.0.0.1".to_string(),
        port: TEST_TCP_PORT,
        batch_size: 1, // Flush immediately
        flush_interval: Duration::from_millis(10),
        ..Default::default()
    };

    // Start TCP source in background
    let source = TcpSource::new(config.clone(), auth_store, batch_tx);
    let source_handle = tokio::spawn(async move {
        // Ignore errors on shutdown
        let _ = source.run(CancellationToken::new()).await;
    });

    // Give the source time to start listening
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Connect and send a batch
    let addr = format!("127.0.0.1:{}", TEST_TCP_PORT);
    let mut client = TcpTestClient::connect(&addr)
        .await
        .expect("failed to connect to TCP source");

    let batch = build_test_batch();
    client.send(&batch).await.expect("failed to send batch");
    client.flush().await.expect("failed to flush");

    // Wait for batch to arrive
    let received = timeout(Duration::from_secs(2), batch_rx.recv())
        .await
        .expect("timeout waiting for batch")
        .expect("channel closed");

    // Verify batch properties
    assert_eq!(received.batch_type(), BatchType::Event);
    assert_eq!(received.workspace_id(), 1);
    assert!(!received.is_empty());

    // Clean up
    source_handle.abort();
}

#[tokio::test]
async fn test_tcp_source_rejects_invalid_api_key() {
    // Set up auth store (empty - no valid keys)
    let auth_store = Arc::new(ApiKeyStore::new());

    // Create channel for receiving batches
    let (tx, mut batch_rx) = crossfire::mpsc::bounded_async::<Batch>(100);
    let batch_tx = ShardedSender::new(vec![tx]);

    // Use different port to avoid conflicts
    let test_port = TEST_TCP_PORT + 1;

    // Create TCP source config
    let config = TcpSourceConfig {
        id: "tcp_test_auth".to_string(),
        address: "127.0.0.1".to_string(),
        port: test_port,
        batch_size: 1,
        flush_interval: Duration::from_millis(10),
        ..Default::default()
    };

    // Start TCP source in background
    let source = TcpSource::new(config, auth_store, batch_tx);
    let source_handle = tokio::spawn(async move {
        let _ = source.run(CancellationToken::new()).await;
    });

    // Give the source time to start listening
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Connect and send a batch with an invalid key
    let addr = format!("127.0.0.1:{}", test_port);
    let mut client = TcpTestClient::connect(&addr)
        .await
        .expect("failed to connect");

    let batch = build_test_batch();
    client.send(&batch).await.expect("failed to send");
    client.flush().await.expect("failed to flush");

    // Should NOT receive the batch (invalid auth)
    let result = timeout(Duration::from_millis(500), batch_rx.recv()).await;

    // Either timeout or no message received (channel closed)
    assert!(
        result.is_err() || result.unwrap().is_err(),
        "batch should have been rejected due to invalid API key"
    );

    // Clean up
    source_handle.abort();
}

#[tokio::test]
async fn test_tcp_source_handles_multiple_batches() {
    // Set up auth store
    let auth_store = create_test_auth_store();

    // Create channel for receiving batches
    let (tx, mut batch_rx) = crossfire::mpsc::bounded_async::<Batch>(100);
    let batch_tx = ShardedSender::new(vec![tx]);

    // Use different port to avoid conflicts
    let test_port = TEST_TCP_PORT + 2;

    // Create TCP source config
    let config = TcpSourceConfig {
        id: "tcp_test_multi".to_string(),
        address: "127.0.0.1".to_string(),
        port: test_port,
        batch_size: 10, // Larger batch size
        flush_interval: Duration::from_millis(50),
        ..Default::default()
    };

    // Start TCP source in background
    let source = TcpSource::new(config, auth_store, batch_tx);
    let source_handle = tokio::spawn(async move {
        let _ = source.run(CancellationToken::new()).await;
    });

    // Give the source time to start listening
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Connect and send multiple batches
    let addr = format!("127.0.0.1:{}", test_port);
    let mut client = TcpTestClient::connect(&addr)
        .await
        .expect("failed to connect");

    let batch = build_test_batch();
    for _ in 0..5 {
        client.send(&batch).await.expect("failed to send");
    }
    client.flush().await.expect("failed to flush");

    // Close connection to trigger flush on the source side
    client.close().await.expect("failed to close");

    // Wait for batches to be processed (flush interval + processing time)
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Should receive at least one aggregated batch
    let mut total_received = 0;
    // Keep trying until timeout - batches may arrive in multiple chunks
    let deadline = tokio::time::Instant::now() + Duration::from_millis(500);
    while tokio::time::Instant::now() < deadline {
        match timeout(Duration::from_millis(100), batch_rx.recv()).await {
            Ok(Ok(received)) => {
                assert_eq!(received.batch_type(), BatchType::Event);
                total_received += received.message_count();
            }
            _ => break,
        }
    }

    // We sent 5 messages
    assert!(
        total_received >= 5,
        "expected at least 5 messages, got {}",
        total_received
    );

    // Clean up
    source_handle.abort();
}

// =============================================================================
// Syslog TCP Source Tests
// =============================================================================

/// Test port for Syslog TCP source
const TEST_SYSLOG_TCP_PORT: u16 = 51240;

/// Test workspace ID for syslog sources
const TEST_WORKSPACE_ID: u32 = 42;

#[tokio::test]
async fn test_syslog_tcp_receives_rfc3164_message() {
    // Create channel for receiving batches
    let (tx, mut batch_rx) = crossfire::mpsc::bounded_async::<Batch>(100);
    let batch_tx = ShardedSender::new(vec![tx]);

    // Create Syslog TCP source config
    let config = SyslogTcpSourceConfig {
        id: "syslog_tcp_test".to_string(),
        address: "127.0.0.1".to_string(),
        port: TEST_SYSLOG_TCP_PORT,
        workspace_id: TEST_WORKSPACE_ID,
        batch_size: 1, // Flush immediately
        flush_interval: Duration::from_millis(10),
        ..Default::default()
    };

    // Start source in background
    let source = Arc::new(SyslogTcpSource::new(config, batch_tx));
    let source_clone = Arc::clone(&source);
    let source_handle = tokio::spawn(async move {
        let _ = source_clone.run(CancellationToken::new()).await;
    });

    // Give the source time to start listening
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Connect and send an RFC 3164 message
    let addr = format!("127.0.0.1:{}", TEST_SYSLOG_TCP_PORT);
    let mut client = SyslogTcpTestClient::connect(&addr)
        .await
        .expect("failed to connect to syslog TCP source");

    let msg = SyslogTcpTestClient::rfc3164(134, "testhost", "myapp", "Test message from RFC 3164");
    client.send(&msg).await.expect("failed to send message");
    client.flush().await.expect("failed to flush");

    // Wait for batch to arrive
    let received = timeout(Duration::from_secs(2), batch_rx.recv())
        .await
        .expect("timeout waiting for batch")
        .expect("channel closed");

    // Verify batch properties
    assert_eq!(received.batch_type(), BatchType::Syslog);
    assert_eq!(received.workspace_id(), TEST_WORKSPACE_ID);
    assert!(!received.is_empty());

    // Verify message content is preserved (raw syslog message)
    let message = received.get_message(0).expect("no message in batch");
    let message_str = std::str::from_utf8(message).expect("invalid UTF-8");
    assert!(message_str.contains("<134>"), "should contain priority");
    assert!(message_str.contains("testhost"), "should contain hostname");
    assert!(message_str.contains("myapp"), "should contain app name");
    assert!(
        message_str.contains("Test message from RFC 3164"),
        "should contain message"
    );

    // Clean up
    source.stop();
    let _ = timeout(Duration::from_millis(500), source_handle).await;
}

#[tokio::test]
async fn test_syslog_tcp_receives_rfc5424_message() {
    // Create channel for receiving batches
    let (tx, mut batch_rx) = crossfire::mpsc::bounded_async::<Batch>(100);
    let batch_tx = ShardedSender::new(vec![tx]);

    // Use different port
    let test_port = TEST_SYSLOG_TCP_PORT + 1;

    // Create Syslog TCP source config
    let config = SyslogTcpSourceConfig {
        id: "syslog_tcp_test_5424".to_string(),
        address: "127.0.0.1".to_string(),
        port: test_port,
        workspace_id: TEST_WORKSPACE_ID,
        batch_size: 1,
        flush_interval: Duration::from_millis(10),
        ..Default::default()
    };

    // Start source in background
    let source = Arc::new(SyslogTcpSource::new(config, batch_tx));
    let source_clone = Arc::clone(&source);
    let source_handle = tokio::spawn(async move {
        let _ = source_clone.run(CancellationToken::new()).await;
    });

    // Give the source time to start listening
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Connect and send an RFC 5424 message
    let addr = format!("127.0.0.1:{}", test_port);
    let mut client = SyslogTcpTestClient::connect(&addr)
        .await
        .expect("failed to connect");

    let msg = SyslogTcpTestClient::rfc5424(
        165,
        "testhost.example.com",
        "myapp",
        "1234",
        "ID47",
        "Test message from RFC 5424",
    );
    client.send(&msg).await.expect("failed to send message");
    client.flush().await.expect("failed to flush");

    // Wait for batch to arrive
    let received = timeout(Duration::from_secs(2), batch_rx.recv())
        .await
        .expect("timeout waiting for batch")
        .expect("channel closed");

    // Verify batch properties
    assert_eq!(received.batch_type(), BatchType::Syslog);
    assert_eq!(received.workspace_id(), TEST_WORKSPACE_ID);

    // Verify message content (RFC 5424 format)
    let message = received.get_message(0).expect("no message in batch");
    let message_str = std::str::from_utf8(message).expect("invalid UTF-8");
    assert!(
        message_str.contains("<165>1"),
        "should contain priority and version"
    );
    assert!(
        message_str.contains("testhost.example.com"),
        "should contain hostname"
    );
    assert!(message_str.contains("myapp"), "should contain app name");
    assert!(message_str.contains("1234"), "should contain proc ID");
    assert!(message_str.contains("ID47"), "should contain message ID");
    assert!(
        message_str.contains("Test message from RFC 5424"),
        "should contain message"
    );

    // Clean up
    source.stop();
    let _ = timeout(Duration::from_millis(500), source_handle).await;
}

#[tokio::test]
async fn test_syslog_tcp_handles_multiple_messages() {
    // Create channel for receiving batches
    let (tx, mut batch_rx) = crossfire::mpsc::bounded_async::<Batch>(100);
    let batch_tx = ShardedSender::new(vec![tx]);

    // Use different port
    let test_port = TEST_SYSLOG_TCP_PORT + 2;

    // Create Syslog TCP source config
    let config = SyslogTcpSourceConfig {
        id: "syslog_tcp_test_multi".to_string(),
        address: "127.0.0.1".to_string(),
        port: test_port,
        workspace_id: TEST_WORKSPACE_ID,
        batch_size: 10,
        flush_interval: Duration::from_millis(50),
        ..Default::default()
    };

    // Start source in background
    let source = Arc::new(SyslogTcpSource::new(config, batch_tx));
    let source_clone = Arc::clone(&source);
    let source_handle = tokio::spawn(async move {
        let _ = source_clone.run(CancellationToken::new()).await;
    });

    // Give the source time to start listening
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Connect and send multiple messages
    let addr = format!("127.0.0.1:{}", test_port);
    let mut client = SyslogTcpTestClient::connect(&addr)
        .await
        .expect("failed to connect");

    for i in 0..5 {
        let msg = SyslogTcpTestClient::rfc3164(134, "testhost", "myapp", &format!("Message {}", i));
        client.send(&msg).await.expect("failed to send message");
    }
    client.flush().await.expect("failed to flush");

    // Wait for processing
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Should receive messages
    let mut total_received = 0;
    while let Ok(Ok(received)) = timeout(Duration::from_millis(100), batch_rx.recv()).await {
        assert_eq!(received.batch_type(), BatchType::Syslog);
        total_received += received.message_count();
    }

    assert!(
        total_received >= 5,
        "expected at least 5 messages, got {}",
        total_received
    );

    // Clean up
    source.stop();
    let _ = timeout(Duration::from_millis(500), source_handle).await;
}

// =============================================================================
// Syslog UDP Source Tests
// =============================================================================

/// Test port for Syslog UDP source
const TEST_SYSLOG_UDP_PORT: u16 = 51250;

#[tokio::test]
async fn test_syslog_udp_receives_rfc3164_message() {
    // Create channel for receiving batches
    let (tx, mut batch_rx) = crossfire::mpsc::bounded_async::<Batch>(100);
    let batch_tx = ShardedSender::new(vec![tx]);

    // Create Syslog UDP source config
    let config = SyslogUdpSourceConfig {
        id: "syslog_udp_test".to_string(),
        address: "127.0.0.1".to_string(),
        port: TEST_SYSLOG_UDP_PORT,
        workspace_id: TEST_WORKSPACE_ID,
        batch_size: 1, // Flush immediately
        flush_interval: Duration::from_millis(10),
        num_workers: 1,
        ..Default::default()
    };

    // Start source in background
    let source = Arc::new(SyslogUdpSource::new(config, batch_tx));
    let source_clone = Arc::clone(&source);
    let source_handle = tokio::spawn(async move {
        let _ = source_clone.run(CancellationToken::new()).await;
    });

    // Give the source time to start listening
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Send an RFC 3164 message via UDP
    let client = SyslogUdpTestClient::new()
        .await
        .expect("failed to create UDP client");

    let msg = SyslogUdpTestClient::rfc3164(134, "udphost", "udpapp", "UDP test message RFC 3164");
    let addr = format!("127.0.0.1:{}", TEST_SYSLOG_UDP_PORT);
    client
        .send_to(&msg, &addr)
        .await
        .expect("failed to send message");

    // Wait for batch to arrive
    let received = timeout(Duration::from_secs(2), batch_rx.recv())
        .await
        .expect("timeout waiting for batch")
        .expect("channel closed");

    // Verify batch properties
    assert_eq!(received.batch_type(), BatchType::Syslog);
    assert_eq!(received.workspace_id(), TEST_WORKSPACE_ID);
    assert!(!received.is_empty());

    // Verify message content
    let message = received.get_message(0).expect("no message in batch");
    let message_str = std::str::from_utf8(message).expect("invalid UTF-8");
    assert!(message_str.contains("<134>"), "should contain priority");
    assert!(message_str.contains("udphost"), "should contain hostname");
    assert!(message_str.contains("udpapp"), "should contain app name");
    assert!(
        message_str.contains("UDP test message RFC 3164"),
        "should contain message"
    );

    // Clean up
    source.stop();
    let _ = timeout(Duration::from_millis(500), source_handle).await;
}

#[tokio::test]
async fn test_syslog_udp_receives_rfc5424_message() {
    // Create channel for receiving batches
    let (tx, mut batch_rx) = crossfire::mpsc::bounded_async::<Batch>(100);
    let batch_tx = ShardedSender::new(vec![tx]);

    // Use different port
    let test_port = TEST_SYSLOG_UDP_PORT + 1;

    // Create Syslog UDP source config
    let config = SyslogUdpSourceConfig {
        id: "syslog_udp_test_5424".to_string(),
        address: "127.0.0.1".to_string(),
        port: test_port,
        workspace_id: TEST_WORKSPACE_ID,
        batch_size: 1,
        flush_interval: Duration::from_millis(10),
        num_workers: 1,
        ..Default::default()
    };

    // Start source in background
    let source = Arc::new(SyslogUdpSource::new(config, batch_tx));
    let source_clone = Arc::clone(&source);
    let source_handle = tokio::spawn(async move {
        let _ = source_clone.run(CancellationToken::new()).await;
    });

    // Give the source time to start listening
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Send an RFC 5424 message via UDP
    let client = SyslogUdpTestClient::new()
        .await
        .expect("failed to create UDP client");

    let msg = SyslogUdpTestClient::rfc5424(
        165,
        "udphost.example.com",
        "udpapp",
        "5678",
        "MSGID",
        "UDP test message RFC 5424",
    );
    let addr = format!("127.0.0.1:{}", test_port);
    client
        .send_to(&msg, &addr)
        .await
        .expect("failed to send message");

    // Wait for batch to arrive
    let received = timeout(Duration::from_secs(2), batch_rx.recv())
        .await
        .expect("timeout waiting for batch")
        .expect("channel closed");

    // Verify batch properties
    assert_eq!(received.batch_type(), BatchType::Syslog);
    assert_eq!(received.workspace_id(), TEST_WORKSPACE_ID);

    // Verify message content (RFC 5424 format)
    let message = received.get_message(0).expect("no message in batch");
    let message_str = std::str::from_utf8(message).expect("invalid UTF-8");
    assert!(
        message_str.contains("<165>1"),
        "should contain priority and version"
    );
    assert!(
        message_str.contains("udphost.example.com"),
        "should contain hostname"
    );
    assert!(message_str.contains("udpapp"), "should contain app name");
    assert!(message_str.contains("5678"), "should contain proc ID");
    assert!(message_str.contains("MSGID"), "should contain message ID");
    assert!(
        message_str.contains("UDP test message RFC 5424"),
        "should contain message"
    );

    // Clean up
    source.stop();
    let _ = timeout(Duration::from_millis(500), source_handle).await;
}

#[tokio::test]
async fn test_syslog_udp_handles_multiple_messages() {
    // Create channel for receiving batches
    let (tx, mut batch_rx) = crossfire::mpsc::bounded_async::<Batch>(100);
    let batch_tx = ShardedSender::new(vec![tx]);

    // Use different port
    let test_port = TEST_SYSLOG_UDP_PORT + 2;

    // Create Syslog UDP source config
    let config = SyslogUdpSourceConfig {
        id: "syslog_udp_test_multi".to_string(),
        address: "127.0.0.1".to_string(),
        port: test_port,
        workspace_id: TEST_WORKSPACE_ID,
        batch_size: 10,
        flush_interval: Duration::from_millis(50),
        num_workers: 1,
        ..Default::default()
    };

    // Start source in background
    let source = Arc::new(SyslogUdpSource::new(config, batch_tx));
    let source_clone = Arc::clone(&source);
    let source_handle = tokio::spawn(async move {
        let _ = source_clone.run(CancellationToken::new()).await;
    });

    // Give the source time to start listening
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Send multiple messages via UDP
    let client = SyslogUdpTestClient::new()
        .await
        .expect("failed to create UDP client");

    let addr = format!("127.0.0.1:{}", test_port);
    for i in 0..5 {
        let msg =
            SyslogUdpTestClient::rfc3164(134, "udphost", "udpapp", &format!("UDP Message {}", i));
        client
            .send_to(&msg, &addr)
            .await
            .expect("failed to send message");
    }

    // Wait for processing (UDP is fire-and-forget, give some time)
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Should receive messages
    let mut total_received = 0;
    while let Ok(Ok(received)) = timeout(Duration::from_millis(100), batch_rx.recv()).await {
        assert_eq!(received.batch_type(), BatchType::Syslog);
        total_received += received.message_count();
    }

    // Note: UDP may drop packets under load, but locally we should receive all
    assert!(
        total_received >= 3,
        "expected at least 3 messages (UDP may drop), got {}",
        total_received
    );

    // Clean up
    source.stop();
    let _ = timeout(Duration::from_millis(500), source_handle).await;
}

#[tokio::test]
async fn test_syslog_udp_captures_source_ip() {
    // Create channel for receiving batches
    let (tx, mut batch_rx) = crossfire::mpsc::bounded_async::<Batch>(100);
    let batch_tx = ShardedSender::new(vec![tx]);

    // Use different port
    let test_port = TEST_SYSLOG_UDP_PORT + 3;

    // Create Syslog UDP source config
    let config = SyslogUdpSourceConfig {
        id: "syslog_udp_test_ip".to_string(),
        address: "127.0.0.1".to_string(),
        port: test_port,
        workspace_id: TEST_WORKSPACE_ID,
        batch_size: 1,
        flush_interval: Duration::from_millis(10),
        num_workers: 1,
        ..Default::default()
    };

    // Start source in background
    let source = Arc::new(SyslogUdpSource::new(config, batch_tx));
    let source_clone = Arc::clone(&source);
    let source_handle = tokio::spawn(async move {
        let _ = source_clone.run(CancellationToken::new()).await;
    });

    // Give the source time to start listening
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Send a message via UDP
    let client = SyslogUdpTestClient::new()
        .await
        .expect("failed to create UDP client");

    let msg = SyslogUdpTestClient::rfc3164(134, "host", "app", "IP test");
    let addr = format!("127.0.0.1:{}", test_port);
    client
        .send_to(&msg, &addr)
        .await
        .expect("failed to send message");

    // Wait for batch to arrive
    let received = timeout(Duration::from_secs(2), batch_rx.recv())
        .await
        .expect("timeout waiting for batch")
        .expect("channel closed");

    // Verify source IP is captured (should be 127.0.0.1)
    let source_ip = received.source_ip();
    assert!(
        source_ip.is_loopback(),
        "source IP should be loopback, got {}",
        source_ip
    );

    // Clean up
    source.stop();
    let _ = timeout(Duration::from_millis(500), source_handle).await;
}
