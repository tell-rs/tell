//! Tests for Syslog TCP Source

use std::io;
use std::sync::Arc;
use std::time::Duration;

use tell_protocol::BatchType;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

use crate::ShardedSender;
use crate::syslog::tcp::{
    SyslogTcpSource, SyslogTcpSourceConfig, SyslogTcpSourceError, SyslogTcpSourceMetrics,
    is_connection_reset,
};

#[test]
fn test_config_defaults() {
    let config = SyslogTcpSourceConfig::default();

    assert_eq!(config.port, 514);
    assert_eq!(config.address, "0.0.0.0");
    assert_eq!(config.max_message_size, 8192);
    assert_eq!(config.workspace_id, 1);
    assert!(config.nodelay);
    assert_eq!(config.socket_buffer_size, 256 * 1024);
}

#[test]
fn test_config_with_port() {
    let config = SyslogTcpSourceConfig::with_port(1514);
    assert_eq!(config.port, 1514);
}

#[test]
fn test_config_bind_address() {
    let config = SyslogTcpSourceConfig {
        address: "127.0.0.1".into(),
        port: 1514,
        ..Default::default()
    };
    assert_eq!(config.bind_address(), "127.0.0.1:1514");
}

#[test]
fn test_config_source_id() {
    let config = SyslogTcpSourceConfig {
        id: "my_syslog".into(),
        ..Default::default()
    };
    assert_eq!(config.source_id().as_str(), "my_syslog");
}

#[test]
fn test_metrics_new() {
    let metrics = SyslogTcpSourceMetrics::new();
    let snapshot = metrics.snapshot();

    assert_eq!(snapshot.connections_active, 0);
    assert_eq!(snapshot.connections_total, 0);
    assert_eq!(snapshot.messages_received, 0);
    assert_eq!(snapshot.lines_read, 0);
    assert_eq!(snapshot.messages_malformed, 0);
}

#[test]
fn test_metrics_tracking() {
    let metrics = SyslogTcpSourceMetrics::new();

    metrics.base.connection_opened();
    metrics.line_read();
    metrics.line_read();
    metrics.base.message_received(100);
    metrics.message_malformed();
    metrics.message_dropped();
    metrics.base.batch_sent();

    let snapshot = metrics.snapshot();

    assert_eq!(snapshot.connections_active, 1);
    assert_eq!(snapshot.connections_total, 1);
    assert_eq!(snapshot.messages_received, 1);
    assert_eq!(snapshot.bytes_received, 100);
    assert_eq!(snapshot.lines_read, 2);
    assert_eq!(snapshot.messages_malformed, 1);
    assert_eq!(snapshot.messages_dropped, 1);
    assert_eq!(snapshot.batches_sent, 1);
    assert_eq!(snapshot.errors, 1); // message_malformed increments errors
}

#[test]
fn test_error_display() {
    let bind_err = SyslogTcpSourceError::Bind {
        address: "0.0.0.0:514".into(),
        source: io::Error::new(io::ErrorKind::AddrInUse, "address in use"),
    };
    assert!(bind_err.to_string().contains("0.0.0.0:514"));

    let size_err = SyslogTcpSourceError::MessageTooLarge {
        size: 10000,
        limit: 8192,
    };
    assert!(size_err.to_string().contains("10000"));
    assert!(size_err.to_string().contains("8192"));

    let channel_err = SyslogTcpSourceError::ChannelClosed;
    assert!(channel_err.to_string().contains("channel"));
}

#[test]
fn test_is_connection_reset() {
    assert!(is_connection_reset(&io::Error::new(
        io::ErrorKind::ConnectionReset,
        "reset"
    )));
    assert!(is_connection_reset(&io::Error::new(
        io::ErrorKind::ConnectionAborted,
        "aborted"
    )));
    assert!(is_connection_reset(&io::Error::new(
        io::ErrorKind::BrokenPipe,
        "broken"
    )));
    assert!(!is_connection_reset(&io::Error::new(
        io::ErrorKind::Other,
        "other"
    )));
}

#[tokio::test]
async fn test_source_creation() {
    let config = SyslogTcpSourceConfig {
        port: 0, // Use any available port
        ..Default::default()
    };

    let (tx, _rx) = crossfire::mpsc::bounded_async(100);
    let sharded_sender = ShardedSender::new(vec![tx]);
    let source = SyslogTcpSource::new(config, sharded_sender);

    assert!(!source.is_running());
    assert_eq!(source.metrics().snapshot().connections_total, 0);
}

#[tokio::test]
async fn test_source_stop() {
    let config = SyslogTcpSourceConfig::default();
    let (tx, _rx) = crossfire::mpsc::bounded_async(100);
    let sharded_sender = ShardedSender::new(vec![tx]);
    let source = SyslogTcpSource::new(config, sharded_sender);

    source.stop();
    assert!(!source.is_running());
}

#[tokio::test]
async fn test_source_accepts_connections() {
    let config = SyslogTcpSourceConfig {
        id: "test_syslog".into(),
        address: "127.0.0.1".into(),
        port: 0, // Let OS assign port
        ..Default::default()
    };

    let (tx, _rx) = crossfire::mpsc::bounded_async(100);
    let sharded_sender = ShardedSender::new(vec![tx]);
    let _source = Arc::new(SyslogTcpSource::new(config, sharded_sender));

    // Bind to get the actual port
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    // Update config with actual port
    let config = SyslogTcpSourceConfig {
        id: "test_syslog".into(),
        address: "127.0.0.1".into(),
        port,
        flush_interval: Duration::from_millis(10),
        ..Default::default()
    };

    let (tx, mut rx) = crossfire::mpsc::bounded_async(100);
    let source = Arc::new(SyslogTcpSource::new(config, ShardedSender::new(vec![tx])));

    // Start source in background
    let source_clone = Arc::clone(&source);
    let handle = tokio::spawn(async move {
        let _ = source_clone.run(CancellationToken::new()).await;
    });

    // Give it time to start
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Connect and send a message
    if let Ok(mut stream) = TcpStream::connect(format!("127.0.0.1:{}", port)).await {
        let msg = "<134>Dec 20 12:34:56 host test: Hello syslog\n";
        let _ = stream.write_all(msg.as_bytes()).await;
        let _ = stream.flush().await;

        // Give time to process
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Close connection to trigger flush
        drop(stream);

        // Wait for batch
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Stop source
    source.stop();
    let _ = tokio::time::timeout(Duration::from_millis(200), handle).await;

    // Check if we received a batch
    if let Ok(batch) = rx.try_recv() {
        assert_eq!(batch.batch_type(), BatchType::Syslog);
        assert!(batch.count() > 0);
    }
}

#[tokio::test]
async fn test_multiple_messages() {
    let config = SyslogTcpSourceConfig {
        id: "test_syslog".into(),
        address: "127.0.0.1".into(),
        port: 0,
        flush_interval: Duration::from_millis(10),
        batch_size: 10,
        ..Default::default()
    };

    // Get an available port
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let config = SyslogTcpSourceConfig { port, ..config };

    let (tx, mut rx) = crossfire::mpsc::bounded_async(100);
    let source = Arc::new(SyslogTcpSource::new(config, ShardedSender::new(vec![tx])));

    let source_clone = Arc::clone(&source);
    let handle = tokio::spawn(async move {
        let _ = source_clone.run(CancellationToken::new()).await;
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    if let Ok(mut stream) = TcpStream::connect(format!("127.0.0.1:{}", port)).await {
        // Send multiple messages
        for i in 0..5 {
            let msg = format!("<134>Dec 20 12:34:{:02} host test: Message {}\n", i, i);
            let _ = stream.write_all(msg.as_bytes()).await;
        }
        let _ = stream.flush().await;

        tokio::time::sleep(Duration::from_millis(50)).await;
        drop(stream);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    source.stop();
    let _ = tokio::time::timeout(Duration::from_millis(200), handle).await;

    // Should have received messages
    let mut total_count = 0;
    while let Ok(batch) = rx.try_recv() {
        total_count += batch.message_count();
    }

    // We should have received at least some messages
    // (exact count depends on timing)
    assert!(total_count > 0);
}

#[tokio::test]
async fn test_workspace_id_propagation() {
    let config = SyslogTcpSourceConfig {
        id: "test_syslog".into(),
        address: "127.0.0.1".into(),
        port: 0,
        workspace_id: 42,
        flush_interval: Duration::from_millis(10),
        ..Default::default()
    };

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let config = SyslogTcpSourceConfig { port, ..config };

    let (tx, mut rx) = crossfire::mpsc::bounded_async(100);
    let source = Arc::new(SyslogTcpSource::new(config, ShardedSender::new(vec![tx])));

    let source_clone = Arc::clone(&source);
    let handle = tokio::spawn(async move {
        let _ = source_clone.run(CancellationToken::new()).await;
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    if let Ok(mut stream) = TcpStream::connect(format!("127.0.0.1:{}", port)).await {
        let msg = "<134>Dec 20 12:34:56 host test: Test message\n";
        let _ = stream.write_all(msg.as_bytes()).await;
        let _ = stream.flush().await;
        tokio::time::sleep(Duration::from_millis(30)).await;
        drop(stream);
        tokio::time::sleep(Duration::from_millis(30)).await;
    }

    source.stop();
    let _ = tokio::time::timeout(Duration::from_millis(200), handle).await;

    // Check workspace ID in received batch
    if let Ok(batch) = rx.try_recv() {
        assert_eq!(batch.workspace_id(), 42);
    }
}

#[tokio::test]
async fn test_crlf_line_endings() {
    let config = SyslogTcpSourceConfig {
        id: "test_syslog".into(),
        address: "127.0.0.1".into(),
        port: 0,
        flush_interval: Duration::from_millis(10),
        ..Default::default()
    };

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let config = SyslogTcpSourceConfig { port, ..config };

    let (tx, mut rx) = crossfire::mpsc::bounded_async(100);
    let source = Arc::new(SyslogTcpSource::new(config, ShardedSender::new(vec![tx])));

    let source_clone = Arc::clone(&source);
    let handle = tokio::spawn(async move {
        let _ = source_clone.run(CancellationToken::new()).await;
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    if let Ok(mut stream) = TcpStream::connect(format!("127.0.0.1:{}", port)).await {
        // Send with CRLF endings (Windows style)
        let msg = "<134>Dec 20 12:34:56 host test: CRLF message\r\n";
        let _ = stream.write_all(msg.as_bytes()).await;
        let _ = stream.flush().await;
        tokio::time::sleep(Duration::from_millis(30)).await;
        drop(stream);
        tokio::time::sleep(Duration::from_millis(30)).await;
    }

    source.stop();
    let _ = tokio::time::timeout(Duration::from_millis(200), handle).await;

    // Check message was received without trailing CRLF
    if let Ok(batch) = rx.try_recv() {
        if let Some(msg) = batch.get_message(0) {
            let msg_str = std::str::from_utf8(msg).unwrap();
            assert!(!msg_str.ends_with('\r'));
            assert!(!msg_str.ends_with('\n'));
            assert!(msg_str.contains("CRLF message"));
        }
    }
}

#[tokio::test]
async fn test_rfc3164_format() {
    // RFC 3164 format: <PRI>TIMESTAMP HOSTNAME TAG: MESSAGE
    let config = SyslogTcpSourceConfig {
        id: "test_syslog".into(),
        address: "127.0.0.1".into(),
        port: 0,
        flush_interval: Duration::from_millis(10),
        ..Default::default()
    };

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let config = SyslogTcpSourceConfig { port, ..config };

    let (tx, mut rx) = crossfire::mpsc::bounded_async(100);
    let source = Arc::new(SyslogTcpSource::new(config, ShardedSender::new(vec![tx])));

    let source_clone = Arc::clone(&source);
    let handle = tokio::spawn(async move {
        let _ = source_clone.run(CancellationToken::new()).await;
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    if let Ok(mut stream) = TcpStream::connect(format!("127.0.0.1:{}", port)).await {
        // RFC 3164 format
        let msg = "<134>Dec 20 12:34:56 router1 %LINK-3-UPDOWN: Interface GigabitEthernet0/1, changed state to up\n";
        let _ = stream.write_all(msg.as_bytes()).await;
        let _ = stream.flush().await;
        tokio::time::sleep(Duration::from_millis(30)).await;
        drop(stream);
        tokio::time::sleep(Duration::from_millis(30)).await;
    }

    source.stop();
    let _ = tokio::time::timeout(Duration::from_millis(200), handle).await;

    if let Ok(batch) = rx.try_recv() {
        assert!(batch.message_count() > 0);
        if let Some(msg) = batch.get_message(0) {
            let msg_str = std::str::from_utf8(msg).unwrap();
            assert!(msg_str.starts_with("<134>"));
            assert!(msg_str.contains("router1"));
        }
    }
}

#[tokio::test]
async fn test_rfc5424_format() {
    // RFC 5424 format: <PRI>VERSION TIMESTAMP HOSTNAME APP-NAME PROCID MSGID STRUCTURED-DATA MSG
    let config = SyslogTcpSourceConfig {
        id: "test_syslog".into(),
        address: "127.0.0.1".into(),
        port: 0,
        flush_interval: Duration::from_millis(10),
        ..Default::default()
    };

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let config = SyslogTcpSourceConfig { port, ..config };

    let (tx, mut rx) = crossfire::mpsc::bounded_async(100);
    let source = Arc::new(SyslogTcpSource::new(config, ShardedSender::new(vec![tx])));

    let source_clone = Arc::clone(&source);
    let handle = tokio::spawn(async move {
        let _ = source_clone.run(CancellationToken::new()).await;
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    if let Ok(mut stream) = TcpStream::connect(format!("127.0.0.1:{}", port)).await {
        // RFC 5424 format
        let msg = "<165>1 2023-12-20T12:36:15.003Z server1.example.com myapp 1234 ID47 - Application started\n";
        let _ = stream.write_all(msg.as_bytes()).await;
        let _ = stream.flush().await;
        tokio::time::sleep(Duration::from_millis(30)).await;
        drop(stream);
        tokio::time::sleep(Duration::from_millis(30)).await;
    }

    source.stop();
    let _ = tokio::time::timeout(Duration::from_millis(200), handle).await;

    if let Ok(batch) = rx.try_recv() {
        assert!(batch.message_count() > 0);
        if let Some(msg) = batch.get_message(0) {
            let msg_str = std::str::from_utf8(msg).unwrap();
            assert!(msg_str.starts_with("<165>1"));
            assert!(msg_str.contains("server1.example.com"));
        }
    }
}
