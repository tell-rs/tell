//! Tests for filter transformer

use super::*;
use cdp_protocol::{BatchBuilder, BatchType, SourceId};
use std::sync::atomic::Ordering;

fn make_json_batch(messages: &[&str]) -> Batch {
    let mut builder = BatchBuilder::new(BatchType::Log, SourceId::new("test"));
    for msg in messages {
        builder.add_raw(msg.as_bytes());
    }
    builder.finish()
}

#[tokio::test]
async fn test_filter_transformer_new() {
    let config = FilterConfig::new()
        .with_condition(Condition::eq("level", "debug"));
    let transformer = FilterTransformer::new(config).unwrap();
    assert_eq!(transformer.name(), "filter");
    assert!(transformer.enabled());
}

#[tokio::test]
async fn test_filter_transformer_disabled() {
    let config = FilterConfig::new()
        .with_condition(Condition::eq("level", "debug"))
        .disabled();
    let transformer = FilterTransformer::new(config).unwrap();
    assert!(!transformer.enabled());
}

#[tokio::test]
async fn test_filter_drop_matching() {
    let config = FilterConfig::new()
        .with_action(FilterAction::Drop)
        .with_condition(Condition::eq("level", "debug"));
    let transformer = FilterTransformer::new(config).unwrap();

    let batch = make_json_batch(&[
        r#"{"level": "debug", "msg": "debug log"}"#,
        r#"{"level": "info", "msg": "info log"}"#,
        r#"{"level": "debug", "msg": "another debug"}"#,
        r#"{"level": "error", "msg": "error log"}"#,
    ]);

    let result = transformer.transform(batch).await.unwrap();

    // Should drop debug logs, keep info and error
    assert_eq!(result.message_count(), 2);

    let metrics = transformer.metrics();
    assert_eq!(metrics.messages_received.load(Ordering::Relaxed), 4);
    assert_eq!(metrics.messages_passed.load(Ordering::Relaxed), 2);
    assert_eq!(metrics.messages_dropped.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn test_filter_keep_matching() {
    let config = FilterConfig::new()
        .with_action(FilterAction::Keep)
        .with_condition(Condition::eq("level", "error"));
    let transformer = FilterTransformer::new(config).unwrap();

    let batch = make_json_batch(&[
        r#"{"level": "debug", "msg": "debug log"}"#,
        r#"{"level": "info", "msg": "info log"}"#,
        r#"{"level": "error", "msg": "error log"}"#,
    ]);

    let result = transformer.transform(batch).await.unwrap();

    // Should keep only error logs
    assert_eq!(result.message_count(), 1);
}

#[tokio::test]
async fn test_filter_contains() {
    let config = FilterConfig::new()
        .with_action(FilterAction::Drop)
        .with_condition(Condition::contains("path", "/health"));
    let transformer = FilterTransformer::new(config).unwrap();

    let batch = make_json_batch(&[
        r#"{"path": "/api/health", "status": 200}"#,
        r#"{"path": "/api/users", "status": 200}"#,
        r#"{"path": "/healthcheck", "status": 200}"#,
    ]);

    let result = transformer.transform(batch).await.unwrap();

    // Should drop paths containing /health
    assert_eq!(result.message_count(), 1);
}

#[tokio::test]
async fn test_filter_starts_with() {
    let config = FilterConfig::new()
        .with_action(FilterAction::Drop)
        .with_condition(Condition::starts_with("ip", "10."));
    let transformer = FilterTransformer::new(config).unwrap();

    let batch = make_json_batch(&[
        r#"{"ip": "10.0.0.1", "msg": "internal"}"#,
        r#"{"ip": "192.168.1.1", "msg": "external"}"#,
        r#"{"ip": "10.1.2.3", "msg": "internal"}"#,
    ]);

    let result = transformer.transform(batch).await.unwrap();

    // Should drop IPs starting with 10.
    assert_eq!(result.message_count(), 1);
}

#[tokio::test]
async fn test_filter_exists() {
    let config = FilterConfig::new()
        .with_action(FilterAction::Keep)
        .with_condition(Condition::exists("user_id"));
    let transformer = FilterTransformer::new(config).unwrap();

    let batch = make_json_batch(&[
        r#"{"user_id": "123", "msg": "user event"}"#,
        r#"{"msg": "anonymous event"}"#,
        r#"{"user_id": "456", "msg": "another user"}"#,
    ]);

    let result = transformer.transform(batch).await.unwrap();

    // Should keep only events with user_id
    assert_eq!(result.message_count(), 2);
}

#[tokio::test]
async fn test_filter_regex() {
    let config = FilterConfig::new()
        .with_action(FilterAction::Keep)
        .with_condition(Condition::regex("path", r"^/api/v\d+/").unwrap());
    let transformer = FilterTransformer::new(config).unwrap();

    let batch = make_json_batch(&[
        r#"{"path": "/api/v1/users", "status": 200}"#,
        r#"{"path": "/api/v2/orders", "status": 200}"#,
        r#"{"path": "/health", "status": 200}"#,
        r#"{"path": "/api/legacy", "status": 200}"#,
    ]);

    let result = transformer.transform(batch).await.unwrap();

    // Should keep only versioned API paths
    assert_eq!(result.message_count(), 2);
}

#[tokio::test]
async fn test_filter_numeric_gt() {
    let config = FilterConfig::new()
        .with_action(FilterAction::Keep)
        .with_condition(Condition::new("status", Operator::Gte, Some("400".to_string())));
    let transformer = FilterTransformer::new(config).unwrap();

    let batch = make_json_batch(&[
        r#"{"path": "/api/users", "status": 200}"#,
        r#"{"path": "/api/error", "status": 500}"#,
        r#"{"path": "/api/notfound", "status": 404}"#,
        r#"{"path": "/api/ok", "status": 201}"#,
    ]);

    let result = transformer.transform(batch).await.unwrap();

    // Should keep only status >= 400
    assert_eq!(result.message_count(), 2);
}

#[tokio::test]
async fn test_filter_multiple_conditions_all() {
    let config = FilterConfig::new()
        .with_action(FilterAction::Drop)
        .with_match_mode(MatchMode::All)
        .with_condition(Condition::eq("level", "debug"))
        .with_condition(Condition::eq("env", "production"));
    let transformer = FilterTransformer::new(config).unwrap();

    let batch = make_json_batch(&[
        r#"{"level": "debug", "env": "production", "msg": "drop this"}"#,
        r#"{"level": "debug", "env": "staging", "msg": "keep - wrong env"}"#,
        r#"{"level": "info", "env": "production", "msg": "keep - wrong level"}"#,
    ]);

    let result = transformer.transform(batch).await.unwrap();

    // Should drop only when BOTH conditions match
    assert_eq!(result.message_count(), 2);
}

#[tokio::test]
async fn test_filter_multiple_conditions_any() {
    let config = FilterConfig::new()
        .with_action(FilterAction::Drop)
        .with_match_mode(MatchMode::Any)
        .with_condition(Condition::eq("level", "debug"))
        .with_condition(Condition::eq("level", "trace"));
    let transformer = FilterTransformer::new(config).unwrap();

    let batch = make_json_batch(&[
        r#"{"level": "debug", "msg": "drop"}"#,
        r#"{"level": "trace", "msg": "drop"}"#,
        r#"{"level": "info", "msg": "keep"}"#,
        r#"{"level": "error", "msg": "keep"}"#,
    ]);

    let result = transformer.transform(batch).await.unwrap();

    // Should drop if ANY condition matches
    assert_eq!(result.message_count(), 2);
}

#[tokio::test]
async fn test_filter_nested_field() {
    let config = FilterConfig::new()
        .with_action(FilterAction::Drop)
        .with_condition(Condition::eq("user.role", "admin"));
    let transformer = FilterTransformer::new(config).unwrap();

    let batch = make_json_batch(&[
        r#"{"user": {"role": "admin", "name": "alice"}}"#,
        r#"{"user": {"role": "user", "name": "bob"}}"#,
    ]);

    let result = transformer.transform(batch).await.unwrap();

    // Should drop admin users
    assert_eq!(result.message_count(), 1);
}

#[tokio::test]
async fn test_filter_non_json_passthrough() {
    let config = FilterConfig::new()
        .with_action(FilterAction::Drop)
        .with_condition(Condition::eq("level", "debug"));
    let transformer = FilterTransformer::new(config).unwrap();

    // Non-JSON messages don't match any JSON condition
    let mut builder = BatchBuilder::new(BatchType::Log, SourceId::new("test"));
    builder.add_raw(b"plain text log");
    builder.add_raw(r#"{"level": "debug"}"#.as_bytes());
    let batch = builder.finish();

    let result = transformer.transform(batch).await.unwrap();

    // Plain text doesn't match, so it passes through. JSON debug is dropped.
    assert_eq!(result.message_count(), 1);
}

#[tokio::test]
async fn test_filter_empty_batch() {
    let config = FilterConfig::new()
        .with_condition(Condition::eq("level", "debug"));
    let transformer = FilterTransformer::new(config).unwrap();

    let batch = make_json_batch(&[]);
    let result = transformer.transform(batch).await.unwrap();

    assert_eq!(result.message_count(), 0);
}

#[tokio::test]
async fn test_filter_drop_rate() {
    let config = FilterConfig::new()
        .with_action(FilterAction::Drop)
        .with_condition(Condition::eq("drop", "true"));
    let transformer = FilterTransformer::new(config).unwrap();

    let batch = make_json_batch(&[
        r#"{"drop": "true"}"#,
        r#"{"drop": "true"}"#,
        r#"{"drop": "false"}"#,
        r#"{"drop": "false"}"#,
    ]);

    let _result = transformer.transform(batch).await.unwrap();

    let metrics = transformer.metrics();
    let drop_rate = metrics.drop_rate();
    assert!((drop_rate - 0.5).abs() < 0.001);
}

#[tokio::test]
async fn test_filter_factory() {
    let factory = FilterFactory;
    assert_eq!(factory.name(), "filter");

    let default_config = factory.default_config().unwrap();
    assert!(default_config.contains_key("action"));
    assert!(default_config.contains_key("match"));
}

#[tokio::test]
async fn test_filter_ne_missing_field() {
    let config = FilterConfig::new()
        .with_action(FilterAction::Keep)
        .with_condition(Condition::ne("level", "debug"));
    let transformer = FilterTransformer::new(config).unwrap();

    let batch = make_json_batch(&[
        r#"{"level": "debug"}"#,
        r#"{"level": "info"}"#,
        r#"{"other": "field"}"#,  // Missing level field
    ]);

    let result = transformer.transform(batch).await.unwrap();

    // Missing field is "not equal" to "debug", so it passes
    assert_eq!(result.message_count(), 2);
}
