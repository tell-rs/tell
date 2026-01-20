//! HTTP source tests

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tell_auth::{ApiKeyStore, WorkspaceId};
use tell_protocol::SourceId;
use tower::ServiceExt;

use super::handlers::HandlerState;
use super::metrics::HttpSourceMetrics;
use super::*;
use crate::ShardedSender;

/// Test context that keeps the channel receiver alive
struct TestContext {
    state: Arc<HandlerState>,
    #[allow(dead_code)]
    _rx: Box<dyn std::any::Any + Send>, // Keep receiver alive so channel doesn't disconnect
}

/// Create a test handler state with mock dependencies
fn test_state() -> TestContext {
    let auth_store = Arc::new(ApiKeyStore::new());

    // Add a test API key (hex encoded)
    let test_key = [
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
        0x10,
    ];
    auth_store.insert(test_key, WorkspaceId::new(1));

    // Create a crossfire channel - keep receiver alive
    let (tx, rx) = crossfire::mpsc::bounded_async(100);
    let batch_sender = ShardedSender::new(vec![tx]);

    let state = Arc::new(HandlerState {
        auth_store,
        batch_sender,
        source_id: SourceId::new("test-http"),
        metrics: Arc::new(HttpSourceMetrics::new()),
        max_payload_size: 16 * 1024 * 1024,
    });

    TestContext {
        state,
        _rx: Box::new(rx),
    }
}

/// Get test API key as hex string
fn test_api_key_hex() -> String {
    "0102030405060708090a0b0c0d0e0f10".to_string()
}

// =============================================================================
// Health Check Tests
// =============================================================================

#[tokio::test]
async fn test_health_check() {
    let ctx = test_state();
    let app = build_router(ctx.state);

    let request = Request::builder()
        .method("GET")
        .uri("/health")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), 1024)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["status"], "ok");
}

// =============================================================================
// Event Ingestion Tests
// =============================================================================

#[tokio::test]
async fn test_ingest_single_event() {
    let ctx = test_state();
    let app = build_router(ctx.state);

    let event = r#"{"type":"track","event":"page_view","device_id":"550e8400-e29b-41d4-a716-446655440000"}"#;

    let request = Request::builder()
        .method("POST")
        .uri("/v1/events")
        .header("content-type", "application/x-ndjson")
        .header("authorization", format!("Bearer {}", test_api_key_hex()))
        .body(Body::from(event))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::ACCEPTED);

    let body = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["accepted"], 1);
}

#[tokio::test]
async fn test_ingest_multiple_events() {
    let ctx = test_state();
    let app = build_router(ctx.state);

    let events = r#"{"type":"track","event":"page_view","device_id":"550e8400-e29b-41d4-a716-446655440000"}
{"type":"track","event":"button_click","device_id":"550e8400-e29b-41d4-a716-446655440000"}
{"type":"identify","device_id":"550e8400-e29b-41d4-a716-446655440000","user_id":"user_123"}"#;

    let request = Request::builder()
        .method("POST")
        .uri("/v1/events")
        .header("content-type", "application/x-ndjson")
        .header("authorization", format!("Bearer {}", test_api_key_hex()))
        .body(Body::from(events))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::ACCEPTED);

    let body = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["accepted"], 3);
}

#[tokio::test]
async fn test_ingest_events_with_properties() {
    let ctx = test_state();
    let app = build_router(ctx.state);

    let event = r#"{"type":"track","event":"purchase","device_id":"550e8400-e29b-41d4-a716-446655440000","properties":{"item":"widget","price":99.99}}"#;

    let request = Request::builder()
        .method("POST")
        .uri("/v1/events")
        .header("content-type", "application/x-ndjson")
        .header("authorization", format!("Bearer {}", test_api_key_hex()))
        .body(Body::from(event))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::ACCEPTED);
}

#[tokio::test]
async fn test_ingest_events_missing_auth() {
    let ctx = test_state();
    let app = build_router(ctx.state);

    let event = r#"{"type":"track","event":"page_view","device_id":"550e8400-e29b-41d4-a716-446655440000"}"#;

    let request = Request::builder()
        .method("POST")
        .uri("/v1/events")
        .header("content-type", "application/x-ndjson")
        .body(Body::from(event))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_ingest_events_invalid_auth() {
    let ctx = test_state();
    let app = build_router(ctx.state);

    let event = r#"{"type":"track","event":"page_view","device_id":"550e8400-e29b-41d4-a716-446655440000"}"#;

    let request = Request::builder()
        .method("POST")
        .uri("/v1/events")
        .header("content-type", "application/x-ndjson")
        .header("authorization", "Bearer invalid_key_1234567890123456")
        .body(Body::from(event))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_ingest_events_empty_body() {
    let ctx = test_state();
    let app = build_router(ctx.state);

    let request = Request::builder()
        .method("POST")
        .uri("/v1/events")
        .header("content-type", "application/x-ndjson")
        .header("authorization", format!("Bearer {}", test_api_key_hex()))
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_ingest_events_invalid_json() {
    let ctx = test_state();
    let app = build_router(ctx.state);

    let event = r#"{"type":"track","event":"page_view","device_id":}"#; // Invalid JSON

    let request = Request::builder()
        .method("POST")
        .uri("/v1/events")
        .header("content-type", "application/x-ndjson")
        .header("authorization", format!("Bearer {}", test_api_key_hex()))
        .body(Body::from(event))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    // Should return 400 since all lines failed
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_ingest_events_partial_failure() {
    let ctx = test_state();
    let app = build_router(ctx.state);

    // First line valid, second line invalid JSON
    let events = r#"{"type":"track","event":"page_view","device_id":"550e8400-e29b-41d4-a716-446655440000"}
{"invalid json"#;

    let request = Request::builder()
        .method("POST")
        .uri("/v1/events")
        .header("content-type", "application/x-ndjson")
        .header("authorization", format!("Bearer {}", test_api_key_hex()))
        .body(Body::from(events))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    // Should return 207 Multi-Status for partial success
    assert_eq!(response.status(), StatusCode::MULTI_STATUS);

    let body = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["accepted"], 1);
    assert_eq!(json["rejected"], 1);
}

// =============================================================================
// Log Ingestion Tests
// =============================================================================

#[tokio::test]
async fn test_ingest_single_log() {
    let ctx = test_state();
    let app = build_router(ctx.state);

    let log = r#"{"level":"info","message":"Application started"}"#;

    let request = Request::builder()
        .method("POST")
        .uri("/v1/logs")
        .header("content-type", "application/x-ndjson")
        .header("authorization", format!("Bearer {}", test_api_key_hex()))
        .body(Body::from(log))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::ACCEPTED);

    let body = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["accepted"], 1);
}

#[tokio::test]
async fn test_ingest_multiple_logs() {
    let ctx = test_state();
    let app = build_router(ctx.state);

    let logs = r#"{"level":"info","message":"Starting service","service":"api"}
{"level":"warning","message":"High memory usage","service":"api"}
{"level":"error","message":"Connection failed","service":"api","data":{"retry":3}}"#;

    let request = Request::builder()
        .method("POST")
        .uri("/v1/logs")
        .header("content-type", "application/x-ndjson")
        .header("authorization", format!("Bearer {}", test_api_key_hex()))
        .body(Body::from(logs))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::ACCEPTED);

    let body = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["accepted"], 3);
}

#[tokio::test]
async fn test_ingest_logs_with_session_id() {
    let ctx = test_state();
    let app = build_router(ctx.state);

    let log = r#"{"level":"debug","message":"User action","session_id":"550e8400-e29b-41d4-a716-446655440000"}"#;

    let request = Request::builder()
        .method("POST")
        .uri("/v1/logs")
        .header("content-type", "application/x-ndjson")
        .header("authorization", format!("Bearer {}", test_api_key_hex()))
        .body(Body::from(log))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::ACCEPTED);
}

// =============================================================================
// X-API-Key Header Tests
// =============================================================================

#[tokio::test]
async fn test_ingest_with_x_api_key_header() {
    let ctx = test_state();
    let app = build_router(ctx.state);

    let event = r#"{"type":"track","event":"page_view","device_id":"550e8400-e29b-41d4-a716-446655440000"}"#;

    let request = Request::builder()
        .method("POST")
        .uri("/v1/events")
        .header("content-type", "application/x-ndjson")
        .header("x-api-key", test_api_key_hex())
        .body(Body::from(event))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::ACCEPTED);
}

// =============================================================================
// JSONL to FlatBuffer Round-Trip Tests
// =============================================================================

use super::encoder::{encode_events, encode_logs};
use super::json_types::{JsonEvent, JsonLogEntry};
use tell_protocol::{
    EventType, FlatBatch, LogLevel, SchemaType, decode_event_data, decode_log_data,
};

/// Test API key bytes for encoder tests
fn test_api_key_bytes() -> [u8; 16] {
    [
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
        0x10,
    ]
}

#[test]
fn test_event_encoding_round_trip() {
    let event = JsonEvent {
        event_type: "track".to_string(),
        event: Some("page_view".to_string()),
        device_id: "550e8400-e29b-41d4-a716-446655440000".to_string(),
        session_id: Some("660e8400-e29b-41d4-a716-446655440001".to_string()),
        user_id: Some("user_123".to_string()),
        group_id: None,
        timestamp: Some(1700000000000),
        properties: Some(serde_json::json!({"page": "/home", "referrer": "google"})),
        traits: None,
        context: None,
    };

    let api_key = test_api_key_bytes();
    let encoded = encode_events(&[event], &api_key).expect("encoding should succeed");

    // Parse the batch wrapper
    let batch = FlatBatch::parse(&encoded).expect("FlatBatch parse should succeed");

    // Verify batch fields
    assert_eq!(batch.schema_type(), SchemaType::Event);
    assert_eq!(batch.api_key().expect("api_key"), &api_key);

    // Decode the inner EventData
    let data = batch.data().expect("data should exist");
    let events = decode_event_data(data).expect("decode_event_data should succeed");

    assert_eq!(events.len(), 1);
    let decoded = &events[0];

    // Verify decoded event
    assert_eq!(decoded.event_type, EventType::Track);
    assert_eq!(decoded.timestamp, 1700000000000);
    assert_eq!(decoded.event_name, Some("page_view"));

    // Verify device_id (UUID bytes)
    let device_id = decoded.device_id.expect("device_id should exist");
    assert_eq!(
        device_id,
        &[
            0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44,
            0x00, 0x00
        ]
    );

    // Verify session_id
    let session_id = decoded.session_id.expect("session_id should exist");
    assert_eq!(
        session_id,
        &[
            0x66, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44,
            0x00, 0x01
        ]
    );

    // Verify payload contains user_id and properties
    let payload: serde_json::Value =
        serde_json::from_slice(decoded.payload).expect("payload should be valid JSON");
    assert_eq!(payload["user_id"], "user_123");
    assert_eq!(payload["page"], "/home");
    assert_eq!(payload["referrer"], "google");
}

#[test]
fn test_all_event_types_encoding() {
    let event_types = ["track", "identify", "group", "alias", "enrich", "context"];
    let expected_types = [
        EventType::Track,
        EventType::Identify,
        EventType::Group,
        EventType::Alias,
        EventType::Enrich,
        EventType::Context,
    ];

    for (type_str, expected_type) in event_types.iter().zip(expected_types.iter()) {
        let event = JsonEvent {
            event_type: type_str.to_string(),
            event: Some("test_event".to_string()),
            device_id: "550e8400-e29b-41d4-a716-446655440000".to_string(),
            session_id: None,
            user_id: None,
            group_id: None,
            timestamp: Some(1700000000000),
            properties: None,
            traits: None,
            context: None,
        };

        let encoded =
            encode_events(&[event], &test_api_key_bytes()).expect("encoding should succeed");

        let batch = FlatBatch::parse(&encoded).expect("FlatBatch parse should succeed");
        let data = batch.data().expect("data should exist");
        let events = decode_event_data(data).expect("decode should succeed");

        assert_eq!(
            events[0].event_type, *expected_type,
            "event type mismatch for {}",
            type_str
        );
    }
}

#[test]
fn test_multiple_events_encoding() {
    let events = vec![
        JsonEvent {
            event_type: "track".to_string(),
            event: Some("event_1".to_string()),
            device_id: "550e8400-e29b-41d4-a716-446655440000".to_string(),
            session_id: None,
            user_id: None,
            group_id: None,
            timestamp: Some(1700000000000),
            properties: None,
            traits: None,
            context: None,
        },
        JsonEvent {
            event_type: "identify".to_string(),
            event: None,
            device_id: "550e8400-e29b-41d4-a716-446655440001".to_string(),
            session_id: None,
            user_id: Some("user_456".to_string()),
            group_id: None,
            timestamp: Some(1700000001000),
            properties: None,
            traits: None,
            context: None,
        },
        JsonEvent {
            event_type: "track".to_string(),
            event: Some("event_3".to_string()),
            device_id: "550e8400-e29b-41d4-a716-446655440002".to_string(),
            session_id: None,
            user_id: None,
            group_id: None,
            timestamp: Some(1700000002000),
            properties: None,
            traits: None,
            context: None,
        },
    ];

    let encoded = encode_events(&events, &test_api_key_bytes()).expect("encoding should succeed");

    let batch = FlatBatch::parse(&encoded).expect("FlatBatch parse should succeed");
    let data = batch.data().expect("data should exist");
    let decoded_events = decode_event_data(data).expect("decode should succeed");

    assert_eq!(decoded_events.len(), 3);
    assert_eq!(decoded_events[0].event_name, Some("event_1"));
    assert_eq!(decoded_events[1].event_type, EventType::Identify);
    assert_eq!(decoded_events[2].timestamp, 1700000002000);
}

#[test]
fn test_log_encoding_round_trip() {
    let log = JsonLogEntry {
        level: "error".to_string(),
        message: "Connection failed".to_string(),
        timestamp: Some(1700000000000),
        source: Some("web-01".to_string()),
        service: Some("api-gateway".to_string()),
        session_id: Some("770e8400-e29b-41d4-a716-446655440002".to_string()),
        data: Some(serde_json::json!({"retry": 3, "error_code": "ECONNREFUSED"})),
        log_type: "log".to_string(),
    };

    let encoded = encode_logs(&[log], &test_api_key_bytes()).expect("encoding should succeed");

    // Parse batch wrapper
    let batch = FlatBatch::parse(&encoded).expect("FlatBatch parse should succeed");
    assert_eq!(batch.schema_type(), SchemaType::Log);

    // Decode inner LogData
    let data = batch.data().expect("data should exist");
    let logs = decode_log_data(data).expect("decode_log_data should succeed");

    assert_eq!(logs.len(), 1);
    let decoded = &logs[0];

    // Verify log fields
    assert_eq!(decoded.level, LogLevel::Error);
    assert_eq!(decoded.timestamp, 1700000000000);
    assert_eq!(decoded.source, Some("web-01"));
    assert_eq!(decoded.service, Some("api-gateway"));

    // Verify session_id
    let session_id = decoded.session_id.expect("session_id should exist");
    assert_eq!(
        session_id,
        &[
            0x77, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44,
            0x00, 0x02
        ]
    );

    // Verify payload
    let payload: serde_json::Value =
        serde_json::from_slice(decoded.payload).expect("payload should be valid JSON");
    assert_eq!(payload["message"], "Connection failed");
    assert_eq!(payload["retry"], 3);
    assert_eq!(payload["error_code"], "ECONNREFUSED");
}

#[test]
fn test_all_log_levels_encoding() {
    // Test level strings and their expected decoded values
    // The encoder maps strings to LogLevelValue which must match decoder's LogLevel
    let test_cases = [
        ("trace", LogLevel::Trace),
        ("debug", LogLevel::Debug),
        ("info", LogLevel::Info),
        ("warning", LogLevel::Warning),
        ("error", LogLevel::Error),
        ("fatal", LogLevel::Fatal),
        // Aliases that map to the same levels
        ("warn", LogLevel::Warning),
        ("err", LogLevel::Error),
        ("critical", LogLevel::Fatal),
    ];

    for (level_str, expected_level) in test_cases.iter() {
        let log = JsonLogEntry {
            level: level_str.to_string(),
            message: "test".to_string(),
            timestamp: Some(1700000000000),
            source: None,
            service: None,
            session_id: None,
            data: None,
            log_type: "log".to_string(),
        };

        let encoded = encode_logs(&[log], &test_api_key_bytes()).expect("encoding should succeed");

        let batch = FlatBatch::parse(&encoded).expect("parse should succeed");
        let data = batch.data().expect("data should exist");
        let logs = decode_log_data(data).expect("decode should succeed");

        assert_eq!(
            logs[0].level, *expected_level,
            "level mismatch for {}",
            level_str
        );
    }
}

#[test]
fn test_multiple_logs_encoding() {
    let logs = vec![
        JsonLogEntry {
            level: "info".to_string(),
            message: "Starting service".to_string(),
            timestamp: Some(1700000000000),
            source: None,
            service: Some("api".to_string()),
            session_id: None,
            data: None,
            log_type: "log".to_string(),
        },
        JsonLogEntry {
            level: "warning".to_string(),
            message: "High memory".to_string(),
            timestamp: Some(1700000001000),
            source: None,
            service: Some("api".to_string()),
            session_id: None,
            data: None,
            log_type: "log".to_string(),
        },
    ];

    let encoded = encode_logs(&logs, &test_api_key_bytes()).expect("encoding should succeed");

    let batch = FlatBatch::parse(&encoded).expect("parse should succeed");
    let data = batch.data().expect("data should exist");
    let decoded_logs = decode_log_data(data).expect("decode should succeed");

    assert_eq!(decoded_logs.len(), 2);
    assert_eq!(decoded_logs[0].level, LogLevel::Info);
    assert_eq!(decoded_logs[1].level, LogLevel::Warning);
}

#[test]
fn test_event_with_complex_properties() {
    let event = JsonEvent {
        event_type: "track".to_string(),
        event: Some("purchase".to_string()),
        device_id: "550e8400-e29b-41d4-a716-446655440000".to_string(),
        session_id: None,
        user_id: Some("buyer_123".to_string()),
        group_id: None,
        timestamp: Some(1700000000000),
        properties: Some(serde_json::json!({
            "items": [
                {"sku": "ITEM-001", "price": 29.99, "qty": 2},
                {"sku": "ITEM-002", "price": 15.50, "qty": 1}
            ],
            "total": 75.48,
            "currency": "USD",
            "nested": {"deep": {"value": true}}
        })),
        traits: None,
        context: None,
    };

    let encoded = encode_events(&[event], &test_api_key_bytes()).expect("encoding should succeed");

    let batch = FlatBatch::parse(&encoded).expect("parse should succeed");
    let data = batch.data().expect("data should exist");
    let events = decode_event_data(data).expect("decode should succeed");

    let payload: serde_json::Value =
        serde_json::from_slice(events[0].payload).expect("payload should be valid JSON");

    assert_eq!(payload["items"][0]["sku"], "ITEM-001");
    assert_eq!(payload["items"][1]["price"], 15.50);
    assert_eq!(payload["total"], 75.48);
    assert_eq!(payload["nested"]["deep"]["value"], true);
}

#[test]
fn test_empty_events_returns_error() {
    let result = encode_events(&[], &test_api_key_bytes());
    assert!(result.is_err());
}

#[test]
fn test_empty_logs_returns_error() {
    let result = encode_logs(&[], &test_api_key_bytes());
    assert!(result.is_err());
}

// =============================================================================
// DoS Protection Tests
// =============================================================================

#[tokio::test]
async fn test_dos_deeply_nested_json_rejected() {
    let ctx = test_state();
    let app = build_router(ctx.state);

    // Create deeply nested JSON (35 levels, exceeds 32 limit)
    let mut nested = String::from(r#"{"a":"#);
    for _ in 0..35 {
        nested.push_str(r#"{"b":"#);
    }
    nested.push_str(r#""deep""#);
    for _ in 0..35 {
        nested.push('}');
    }
    nested.push('}');

    // Wrap in a valid event structure
    let event = format!(
        r#"{{"type":"track","event":"test","device_id":"550e8400-e29b-41d4-a716-446655440000","properties":{}}}"#,
        nested
    );

    let request = Request::builder()
        .method("POST")
        .uri("/v1/events")
        .header("content-type", "application/x-ndjson")
        .header("authorization", format!("Bearer {}", test_api_key_hex()))
        .body(Body::from(event))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    // Should be rejected (either 400 or 207 with error)
    assert!(
        response.status() == StatusCode::BAD_REQUEST
            || response.status() == StatusCode::MULTI_STATUS,
        "deeply nested JSON should be rejected"
    );
}

#[tokio::test]
async fn test_dos_moderate_nesting_allowed() {
    let ctx = test_state();
    let app = build_router(ctx.state);

    // Create moderately nested JSON (10 levels, under 32 limit)
    let event = r#"{"type":"track","event":"test","device_id":"550e8400-e29b-41d4-a716-446655440000","properties":{"a":{"b":{"c":{"d":{"e":"value"}}}}}}"#;

    let request = Request::builder()
        .method("POST")
        .uri("/v1/events")
        .header("content-type", "application/x-ndjson")
        .header("authorization", format!("Bearer {}", test_api_key_hex()))
        .body(Body::from(event))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::ACCEPTED);
}

// =============================================================================
// API Key Security Tests
// =============================================================================

#[tokio::test]
async fn test_api_key_must_be_32_hex_chars() {
    let ctx = test_state();
    let app = build_router(ctx.state);

    let event =
        r#"{"type":"track","event":"test","device_id":"550e8400-e29b-41d4-a716-446655440000"}"#;

    // Too short (only 16 chars)
    let request = Request::builder()
        .method("POST")
        .uri("/v1/events")
        .header("content-type", "application/x-ndjson")
        .header("authorization", "Bearer 0102030405060708")
        .body(Body::from(event))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_api_key_rejects_non_hex() {
    let ctx = test_state();
    let app = build_router(ctx.state);

    let event =
        r#"{"type":"track","event":"test","device_id":"550e8400-e29b-41d4-a716-446655440000"}"#;

    // Contains non-hex characters (z, y, x, w)
    let request = Request::builder()
        .method("POST")
        .uri("/v1/events")
        .header("content-type", "application/x-ndjson")
        .header("authorization", "Bearer zyxw030405060708090a0b0c0d0e0f10")
        .body(Body::from(event))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}
