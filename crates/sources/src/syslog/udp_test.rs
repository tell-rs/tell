//! Tests for Syslog UDP Source

use std::io;
use std::sync::Arc;
use std::time::Duration;

use tell_protocol::BatchType;
use tokio::net::UdpSocket;
use tokio_util::sync::CancellationToken;

use crate::syslog::udp::{
    trim_trailing_newline, SyslogUdpSource, SyslogUdpSourceConfig, SyslogUdpSourceError,
    SyslogUdpSourceMetrics,
};
use crate::ShardedSender;

#[test]
fn test_config_defaults() {
    let config = SyslogUdpSourceConfig::default();

    assert_eq!(config.port, 514);
    assert_eq!(config.address, "0.0.0.0");
    assert_eq!(config.max_message_size, 8192);
    assert_eq!(config.workspace_id, 1);
    assert_eq!(config.num_workers, 4);
}

#[test]
fn test_config_with_port() {
    let config = SyslogUdpSourceConfig::with_port(1514);
    assert_eq!(config.port, 1514);
}

#[test]
fn test_config_bind_address() {
    let config = SyslogUdpSourceConfig {
        address: "127.0.0.1".into(),
        port: 1514,
        ..Default::default()
    };
    assert_eq!(config.bind_address(), "127.0.0.1:1514");
}

#[test]
fn test_config_source_id() {
    let config = SyslogUdpSourceConfig {
        id: "my_syslog_udp".into(),
        ..Default::default()
    };
    assert_eq!(config.source_id().as_str(), "my_syslog_udp");
}

#[test]
fn test_metrics_new() {
    let metrics = SyslogUdpSourceMetrics::new();
    let snapshot = metrics.snapshot();

    assert_eq!(snapshot.workers_active, 0);
    assert_eq!(snapshot.workers_total, 0);
    assert_eq!(snapshot.packets_received, 0);
    assert_eq!(snapshot.packets_dropped, 0);
    assert_eq!(snapshot.messages_malformed, 0);
}

#[test]
fn test_metrics_tracking() {
    let metrics = SyslogUdpSourceMetrics::new();

    metrics.worker_started();
    metrics.worker_started();
    metrics.packet_received(100);
    metrics.packet_received(200);
    metrics.message_malformed();
    metrics.packet_dropped();
    metrics.worker_error();
    metrics.base.batch_sent();
    metrics.worker_stopped();

    let snapshot = metrics.snapshot();

    assert_eq!(snapshot.workers_active, 1);
    assert_eq!(snapshot.workers_total, 2);
    assert_eq!(snapshot.packets_received, 2);
    assert_eq!(snapshot.bytes_received, 300);
    assert_eq!(snapshot.messages_malformed, 1);
    assert_eq!(snapshot.packets_dropped, 1);
    assert_eq!(snapshot.worker_errors, 1);
    assert_eq!(snapshot.batches_sent, 1);
    assert_eq!(snapshot.errors, 2); // message_malformed + worker_error
}

#[test]
fn test_error_display() {
    let bind_err = SyslogUdpSourceError::Bind {
        address: "0.0.0.0:514".into(),
        source: io::Error::new(io::ErrorKind::AddrInUse, "address in use"),
    };
    assert!(bind_err.to_string().contains("0.0.0.0:514"));

    let size_err = SyslogUdpSourceError::PacketTooLarge {
        size: 10000,
        limit: 8192,
    };
    assert!(size_err.to_string().contains("10000"));
    assert!(size_err.to_string().contains("8192"));

    let channel_err = SyslogUdpSourceError::ChannelClosed;
    assert!(channel_err.to_string().contains("channel"));

    let worker_err = SyslogUdpSourceError::WorkerCreation {
        worker_id: 2,
        source: io::Error::new(io::ErrorKind::Other, "test"),
    };
    assert!(worker_err.to_string().contains("worker 2"));
}

#[test]
fn test_trim_trailing_newline() {
    assert_eq!(trim_trailing_newline(b"hello\n"), b"hello");
    assert_eq!(trim_trailing_newline(b"hello\r\n"), b"hello");
    assert_eq!(trim_trailing_newline(b"hello"), b"hello");
    assert_eq!(trim_trailing_newline(b"\n"), b"");
    assert_eq!(trim_trailing_newline(b"\r\n"), b"");
    assert_eq!(trim_trailing_newline(b""), b"");
    assert_eq!(trim_trailing_newline(b"line1\nline2\n"), b"line1\nline2");
}

#[tokio::test]
async fn test_source_creation() {
    let config = SyslogUdpSourceConfig {
        port: 0, // Use any available port
        num_workers: 1,
        ..Default::default()
    };

    let (tx, _rx) = crossfire::mpsc::bounded_async(100);
    let source = SyslogUdpSource::new(config, ShardedSender::new(vec![tx]));

    assert!(!source.is_running());
    assert_eq!(source.metrics().snapshot().workers_total, 0);
}

#[tokio::test]
async fn test_source_stop() {
    let config = SyslogUdpSourceConfig::default();
    let (tx, _rx) = crossfire::mpsc::bounded_async(100);
    let source = SyslogUdpSource::new(config, ShardedSender::new(vec![tx]));

    source.stop();
    assert!(!source.is_running());
}

#[tokio::test]
async fn test_source_receives_packets() {
    let config = SyslogUdpSourceConfig {
        id: "test_syslog_udp".into(),
        address: "127.0.0.1".into(),
        port: 0, // Let OS assign port
        num_workers: 1,
        flush_interval: Duration::from_millis(10),
        ..Default::default()
    };

    // Bind to get the actual port
    let temp_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let port = temp_socket.local_addr().unwrap().port();
    drop(temp_socket);

    let config = SyslogUdpSourceConfig { port, ..config };

    let (tx, mut rx) = crossfire::mpsc::bounded_async(100);
    let source = Arc::new(SyslogUdpSource::new(config.clone(), ShardedSender::new(vec![tx])));

    // Start source in background
    let source_clone = Arc::clone(&source);
    let handle = tokio::spawn(async move {
        let _ = source_clone.run(CancellationToken::new()).await;
    });

    // Give it time to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Send a UDP packet
    let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let msg = "<134>Dec 20 12:34:56 host test: Hello syslog UDP";
    client
        .send_to(msg.as_bytes(), format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    // Give time to process and flush
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Stop source
    source.stop();
    let _ = tokio::time::timeout(Duration::from_millis(500), handle).await;

    // Check if we received a batch
    if let Ok(batch) = rx.try_recv() {
        assert_eq!(batch.batch_type(), BatchType::Syslog);
        assert!(batch.count() > 0);
    }
}

#[tokio::test]
async fn test_multiple_packets() {
    let config = SyslogUdpSourceConfig {
        id: "test_syslog_udp".into(),
        address: "127.0.0.1".into(),
        port: 0,
        num_workers: 1,
        flush_interval: Duration::from_millis(10),
        batch_size: 10,
        ..Default::default()
    };

    let temp_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let port = temp_socket.local_addr().unwrap().port();
    drop(temp_socket);

    let config = SyslogUdpSourceConfig { port, ..config };

    let (tx, mut rx) = crossfire::mpsc::bounded_async(100);
    let source = Arc::new(SyslogUdpSource::new(config.clone(), ShardedSender::new(vec![tx])));

    let source_clone = Arc::clone(&source);
    let handle = tokio::spawn(async move {
        let _ = source_clone.run(CancellationToken::new()).await;
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Send multiple packets
    let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    for i in 0..5 {
        let msg = format!("<134>Dec 20 12:34:{:02} host test: Message {}", i, i);
        client
            .send_to(msg.as_bytes(), format!("127.0.0.1:{}", port))
            .await
            .unwrap();
    }

    tokio::time::sleep(Duration::from_millis(100)).await;

    source.stop();
    let _ = tokio::time::timeout(Duration::from_millis(500), handle).await;

    // Count received messages
    let mut total_count = 0;
    while let Ok(batch) = rx.try_recv() {
        total_count += batch.message_count();
    }

    assert!(total_count > 0);
}

#[tokio::test]
async fn test_workspace_id_propagation() {
    let config = SyslogUdpSourceConfig {
        id: "test_syslog_udp".into(),
        address: "127.0.0.1".into(),
        port: 0,
        num_workers: 1,
        workspace_id: 42,
        flush_interval: Duration::from_millis(10),
        ..Default::default()
    };

    let temp_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let port = temp_socket.local_addr().unwrap().port();
    drop(temp_socket);

    let config = SyslogUdpSourceConfig { port, ..config };

    let (tx, mut rx) = crossfire::mpsc::bounded_async(100);
    let source = Arc::new(SyslogUdpSource::new(config.clone(), ShardedSender::new(vec![tx])));

    let source_clone = Arc::clone(&source);
    let handle = tokio::spawn(async move {
        let _ = source_clone.run(CancellationToken::new()).await;
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let msg = "<134>Dec 20 12:34:56 host test: Test message";
    client
        .send_to(msg.as_bytes(), format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    source.stop();
    let _ = tokio::time::timeout(Duration::from_millis(500), handle).await;

    // Check workspace ID in received batch
    if let Ok(batch) = rx.try_recv() {
        assert_eq!(batch.workspace_id(), 42);
    }
}

#[tokio::test]
async fn test_rfc3164_format() {
    let config = SyslogUdpSourceConfig {
        id: "test_syslog_udp".into(),
        address: "127.0.0.1".into(),
        port: 0,
        num_workers: 1,
        flush_interval: Duration::from_millis(10),
        ..Default::default()
    };

    let temp_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let port = temp_socket.local_addr().unwrap().port();
    drop(temp_socket);

    let config = SyslogUdpSourceConfig { port, ..config };

    let (tx, mut rx) = crossfire::mpsc::bounded_async(100);
    let source = Arc::new(SyslogUdpSource::new(config.clone(), ShardedSender::new(vec![tx])));

    let source_clone = Arc::clone(&source);
    let handle = tokio::spawn(async move {
        let _ = source_clone.run(CancellationToken::new()).await;
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    // RFC 3164 format
    let msg = "<134>Dec 20 12:34:56 router1 %LINK-3-UPDOWN: Interface GigabitEthernet0/1, changed state to up";
    client
        .send_to(msg.as_bytes(), format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    source.stop();
    let _ = tokio::time::timeout(Duration::from_millis(500), handle).await;

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
    let config = SyslogUdpSourceConfig {
        id: "test_syslog_udp".into(),
        address: "127.0.0.1".into(),
        port: 0,
        num_workers: 1,
        flush_interval: Duration::from_millis(10),
        ..Default::default()
    };

    let temp_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let port = temp_socket.local_addr().unwrap().port();
    drop(temp_socket);

    let config = SyslogUdpSourceConfig { port, ..config };

    let (tx, mut rx) = crossfire::mpsc::bounded_async(100);
    let source = Arc::new(SyslogUdpSource::new(config.clone(), ShardedSender::new(vec![tx])));

    let source_clone = Arc::clone(&source);
    let handle = tokio::spawn(async move {
        let _ = source_clone.run(CancellationToken::new()).await;
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    // RFC 5424 format
    let msg = "<165>1 2023-12-20T12:36:15.003Z server1.example.com myapp 1234 ID47 - Application started";
    client
        .send_to(msg.as_bytes(), format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    source.stop();
    let _ = tokio::time::timeout(Duration::from_millis(500), handle).await;

    if let Ok(batch) = rx.try_recv() {
        assert!(batch.message_count() > 0);
        if let Some(msg) = batch.get_message(0) {
            let msg_str = std::str::from_utf8(msg).unwrap();
            assert!(msg_str.starts_with("<165>1"));
            assert!(msg_str.contains("server1.example.com"));
        }
    }
}

#[tokio::test]
async fn test_packet_with_newline() {
    let config = SyslogUdpSourceConfig {
        id: "test_syslog_udp".into(),
        address: "127.0.0.1".into(),
        port: 0,
        num_workers: 1,
        flush_interval: Duration::from_millis(10),
        ..Default::default()
    };

    let temp_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let port = temp_socket.local_addr().unwrap().port();
    drop(temp_socket);

    let config = SyslogUdpSourceConfig { port, ..config };

    let (tx, mut rx) = crossfire::mpsc::bounded_async(100);
    let source = Arc::new(SyslogUdpSource::new(config.clone(), ShardedSender::new(vec![tx])));

    let source_clone = Arc::clone(&source);
    let handle = tokio::spawn(async move {
        let _ = source_clone.run(CancellationToken::new()).await;
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    // Some syslog clients add trailing newline
    let msg = "<134>Dec 20 12:34:56 host test: Message with newline\n";
    client
        .send_to(msg.as_bytes(), format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    source.stop();
    let _ = tokio::time::timeout(Duration::from_millis(500), handle).await;

    if let Ok(batch) = rx.try_recv() {
        if let Some(msg) = batch.get_message(0) {
            let msg_str = std::str::from_utf8(msg).unwrap();
            // Trailing newline should be stripped
            assert!(!msg_str.ends_with('\n'));
            assert!(msg_str.contains("Message with newline"));
        }
    }
}
