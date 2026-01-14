//! Tests for the tap wire protocol

use super::*;
use bytes::Bytes;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

// ============================================================================
// TapMessage roundtrip tests
// ============================================================================

#[test]
fn test_heartbeat_roundtrip() {
    let msg = TapMessage::Heartbeat;
    let encoded = msg.encode();

    // Skip length prefix (4 bytes)
    let payload = encoded.slice(4..);
    let decoded = TapMessage::decode(payload).unwrap();

    assert_eq!(decoded, TapMessage::Heartbeat);
}

#[test]
fn test_error_roundtrip() {
    let msg = TapMessage::Error("something went wrong".into());
    let encoded = msg.encode();

    let payload = encoded.slice(4..);
    let decoded = TapMessage::decode(payload).unwrap();

    assert_eq!(decoded, TapMessage::Error("something went wrong".into()));
}

#[test]
fn test_error_with_unicode() {
    let msg = TapMessage::Error("error: 日本語 emoji 🎉".into());
    let encoded = msg.encode();

    let payload = encoded.slice(4..);
    let decoded = TapMessage::decode(payload).unwrap();

    assert_eq!(decoded, TapMessage::Error("error: 日本語 emoji 🎉".into()));
}

#[test]
fn test_subscribe_empty_roundtrip() {
    let req = SubscribeRequest::new();
    let msg = TapMessage::Subscribe(req.clone());
    let encoded = msg.encode();

    let payload = encoded.slice(4..);
    let decoded = TapMessage::decode(payload).unwrap();

    assert_eq!(decoded, TapMessage::Subscribe(req));
}

#[test]
fn test_subscribe_full_roundtrip() {
    let req = SubscribeRequest::new()
        .with_workspaces(vec![1, 2, 3])
        .with_sources(vec!["tcp_main".into(), "syslog_udp".into()])
        .with_types(vec![1, 2]) // Event, Log
        .with_last_n(100)
        .with_sample_rate(0.01)
        .with_rate_limit(1000);

    let msg = TapMessage::Subscribe(req.clone());
    let encoded = msg.encode();

    let payload = encoded.slice(4..);
    let decoded = TapMessage::decode(payload).unwrap();

    assert_eq!(decoded, TapMessage::Subscribe(req));
}

#[test]
fn test_subscribe_partial_filters() {
    let req = SubscribeRequest {
        workspace_ids: Some(vec![42]),
        source_ids: None,
        batch_types: Some(vec![1]),
        last_n: 50,
        sample_rate: None,
        max_batches_per_sec: Some(500),
    };

    let msg = TapMessage::Subscribe(req.clone());
    let encoded = msg.encode();

    let payload = encoded.slice(4..);
    let decoded = TapMessage::decode(payload).unwrap();

    assert_eq!(decoded, TapMessage::Subscribe(req));
}

#[test]
fn test_batch_ipv4_roundtrip() {
    let envelope = TapEnvelope {
        workspace_id: 42,
        source_id: "tcp_main".into(),
        batch_type: 1, // Event
        source_ip: IpAddr::V4(Ipv4Addr::new(192, 168, 1, 100)),
        count: 3,
        offsets: vec![0, 100, 250],
        lengths: vec![100, 150, 75],
        payload: Bytes::from_static(b"fake flatbuffer data here..."),
    };

    let msg = TapMessage::Batch(envelope.clone());
    let encoded = msg.encode();

    let payload = encoded.slice(4..);
    let decoded = TapMessage::decode(payload).unwrap();

    assert_eq!(decoded, TapMessage::Batch(envelope));
}

#[test]
fn test_batch_ipv6_roundtrip() {
    let envelope = TapEnvelope {
        workspace_id: 1,
        source_id: "syslog".into(),
        batch_type: 2, // Log
        source_ip: IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1)),
        count: 1,
        offsets: vec![0],
        lengths: vec![64],
        payload: Bytes::from_static(b"log data"),
    };

    let msg = TapMessage::Batch(envelope.clone());
    let encoded = msg.encode();

    let payload = encoded.slice(4..);
    let decoded = TapMessage::decode(payload).unwrap();

    assert_eq!(decoded, TapMessage::Batch(envelope));
}

#[test]
fn test_batch_empty_payload() {
    let envelope = TapEnvelope {
        workspace_id: 1,
        source_id: "test".into(),
        batch_type: 0,
        source_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
        count: 0,
        offsets: vec![],
        lengths: vec![],
        payload: Bytes::new(),
    };

    let msg = TapMessage::Batch(envelope.clone());
    let encoded = msg.encode();

    let payload = encoded.slice(4..);
    let decoded = TapMessage::decode(payload).unwrap();

    assert_eq!(decoded, TapMessage::Batch(envelope));
}

#[test]
fn test_batch_large_payload() {
    let large_payload = Bytes::from(vec![0xAB; 1024 * 1024]); // 1MB

    let envelope = TapEnvelope {
        workspace_id: 999,
        source_id: "bulk".into(),
        batch_type: 1,
        source_ip: IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
        count: 500,
        offsets: (0..500).map(|i| i * 2048).collect(),
        lengths: vec![2048; 500],
        payload: large_payload.clone(),
    };

    let msg = TapMessage::Batch(envelope.clone());
    let encoded = msg.encode();

    let payload = encoded.slice(4..);
    let decoded = TapMessage::decode(payload).unwrap();

    match decoded {
        TapMessage::Batch(e) => {
            assert_eq!(e.workspace_id, 999);
            assert_eq!(e.count, 500);
            assert_eq!(e.payload.len(), 1024 * 1024);
        }
        _ => panic!("expected Batch message"),
    }
}

// ============================================================================
// Length prefix tests
// ============================================================================

#[test]
fn test_length_prefix_reading() {
    let buf = [0x00, 0x00, 0x01, 0x00]; // 256 in big-endian
    assert_eq!(read_length_prefix(&buf), Some(256));

    let buf = [0x00, 0x00, 0x00, 0x01];
    assert_eq!(read_length_prefix(&buf), Some(1));

    let buf = [0xFF, 0xFF, 0xFF, 0xFF];
    assert_eq!(read_length_prefix(&buf), Some(u32::MAX));

    // Too short
    let buf = [0x00, 0x00, 0x00];
    assert_eq!(read_length_prefix(&buf), None);
}

#[test]
fn test_encoded_message_has_valid_length() {
    let msg = TapMessage::Subscribe(SubscribeRequest::new().with_workspaces(vec![1, 2, 3]));
    let encoded = msg.encode();

    // First 4 bytes are length
    let len = read_length_prefix(&encoded).unwrap();
    assert_eq!(len as usize, encoded.len() - 4);
}

// ============================================================================
// Error cases
// ============================================================================

#[test]
fn test_decode_empty_message() {
    let result = TapMessage::decode(Bytes::new());
    assert!(result.is_err());
}

#[test]
fn test_decode_unknown_message_type() {
    let mut buf = bytes::BytesMut::new();
    buf.put_u8(0xFF); // Unknown type
    let result = TapMessage::decode(buf.freeze());
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("unknown message type")
    );
}

#[test]
fn test_decode_truncated_subscribe() {
    let mut buf = bytes::BytesMut::new();
    buf.put_u8(0x01); // Subscribe
    buf.put_u8(1); // workspace_ids = Some
    buf.put_u32(2); // len = 2
    buf.put_u32(1); // first element
    // Missing second element

    let result = TapMessage::decode(buf.freeze());
    assert!(result.is_err());
}

#[test]
fn test_decode_truncated_batch() {
    let mut buf = bytes::BytesMut::new();
    buf.put_u8(0x02); // Batch
    buf.put_u32(42); // workspace_id
    // Missing rest

    let result = TapMessage::decode(buf.freeze());
    assert!(result.is_err());
}

#[test]
fn test_decode_invalid_ip_version() {
    let mut buf = bytes::BytesMut::new();
    buf.put_u8(0x02); // Batch
    buf.put_u32(42); // workspace_id
    buf.put_u32(4); // source_id length
    buf.put_slice(b"test");
    buf.put_u8(1); // batch_type
    buf.put_u8(5); // Invalid IP version (not 4 or 6)

    let result = TapMessage::decode(buf.freeze());
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("invalid IP version")
    );
}

// ============================================================================
// SubscribeRequest builder tests
// ============================================================================

#[test]
fn test_subscribe_request_builder() {
    let req = SubscribeRequest::new()
        .with_workspaces(vec![1, 2])
        .with_sources(vec!["a".into()])
        .with_types(vec![1])
        .with_last_n(10)
        .with_sample_rate(0.5)
        .with_rate_limit(100);

    assert_eq!(req.workspace_ids, Some(vec![1, 2]));
    assert_eq!(req.source_ids, Some(vec!["a".into()]));
    assert_eq!(req.batch_types, Some(vec![1]));
    assert_eq!(req.last_n, 10);
    assert_eq!(req.sample_rate, Some(0.5));
    assert_eq!(req.max_batches_per_sec, Some(100));
}

#[test]
fn test_sample_rate_clamping() {
    let req = SubscribeRequest::new().with_sample_rate(1.5);
    assert_eq!(req.sample_rate, Some(1.0));

    let req = SubscribeRequest::new().with_sample_rate(-0.5);
    assert_eq!(req.sample_rate, Some(0.0));

    let req = SubscribeRequest::new().with_sample_rate(0.5);
    assert_eq!(req.sample_rate, Some(0.5));
}

// ============================================================================
// Edge cases
// ============================================================================

#[test]
fn test_empty_string_source_id() {
    let envelope = TapEnvelope {
        workspace_id: 1,
        source_id: "".into(),
        batch_type: 1,
        source_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
        count: 0,
        offsets: vec![],
        lengths: vec![],
        payload: Bytes::new(),
    };

    let msg = TapMessage::Batch(envelope.clone());
    let encoded = msg.encode();
    let payload = encoded.slice(4..);
    let decoded = TapMessage::decode(payload).unwrap();

    assert_eq!(decoded, TapMessage::Batch(envelope));
}

#[test]
fn test_many_offsets() {
    let n = 10_000;
    let envelope = TapEnvelope {
        workspace_id: 1,
        source_id: "bulk".into(),
        batch_type: 1,
        source_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
        count: n as u32,
        offsets: (0..n).map(|i| i as u32 * 100).collect(),
        lengths: vec![100; n],
        payload: Bytes::from(vec![0u8; n * 100]),
    };

    let msg = TapMessage::Batch(envelope.clone());
    let encoded = msg.encode();
    let payload = encoded.slice(4..);
    let decoded = TapMessage::decode(payload).unwrap();

    match decoded {
        TapMessage::Batch(e) => {
            assert_eq!(e.offsets.len(), n);
            assert_eq!(e.lengths.len(), n);
        }
        _ => panic!("expected Batch"),
    }
}
