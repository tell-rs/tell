//! Tests for reduce transformer

use super::*;
use tell_protocol::{BatchBuilder, BatchType, SourceId};

fn make_test_batch(messages: &[&[u8]]) -> Batch {
    let mut builder = BatchBuilder::new(BatchType::Log, SourceId::new("test"));
    for msg in messages {
        builder.add_raw(msg);
    }
    builder.finish()
}

fn make_json_batch(messages: &[&str]) -> Batch {
    let mut builder = BatchBuilder::new(BatchType::Log, SourceId::new("test"));
    for msg in messages {
        builder.add_raw(msg.as_bytes());
    }
    builder.finish()
}

#[tokio::test]
async fn test_reduce_transformer_new() {
    let config = ReduceConfig::default();
    let transformer = ReduceTransformer::new(config).unwrap();
    assert_eq!(transformer.name(), "reduce");
    assert!(transformer.enabled());
}

#[tokio::test]
async fn test_reduce_transformer_disabled() {
    let config = ReduceConfig::default().disabled();
    let transformer = ReduceTransformer::new(config).unwrap();
    assert!(!transformer.enabled());
}

#[tokio::test]
async fn test_reduce_empty_batch() {
    let config = ReduceConfig::default();
    let transformer = ReduceTransformer::new(config).unwrap();

    let batch = make_test_batch(&[]);
    let result = transformer.transform(batch).await.unwrap();

    assert_eq!(result.message_count(), 0);
}

#[tokio::test]
async fn test_reduce_single_message_passthrough() {
    let config = ReduceConfig::default().with_min_events(2);
    let transformer = ReduceTransformer::new(config).unwrap();

    let batch = make_test_batch(&[b"single message"]);
    let _result = transformer.transform(batch).await.unwrap();

    // Single message goes into state, not output yet (waiting for window)
    assert_eq!(transformer.group_count().await, 1);
}

#[tokio::test]
async fn test_reduce_identical_messages_grouped() {
    let config = ReduceConfig::default()
        .with_max_events(3)
        .with_min_events(2);
    let transformer = ReduceTransformer::new(config).unwrap();

    // Send 3 identical messages - should trigger max_events flush
    let batch = make_test_batch(&[b"same", b"same", b"same"]);
    let result = transformer.transform(batch).await.unwrap();

    // Should output 1 reduced message
    assert_eq!(result.message_count(), 1);

    // Check that the output contains count metadata
    let output = result.get_message(0).unwrap();
    let output_str = String::from_utf8_lossy(output);
    assert!(output_str.contains("_reduced_count"));
}

#[tokio::test]
async fn test_reduce_different_messages_different_groups() {
    let config = ReduceConfig::default()
        .with_max_events(2)
        .with_min_events(1);
    let transformer = ReduceTransformer::new(config).unwrap();

    // Send different messages - should create different groups
    let batch = make_test_batch(&[b"msg_a", b"msg_a", b"msg_b", b"msg_b"]);
    let result = transformer.transform(batch).await.unwrap();

    // Both groups hit max_events, should output 2 messages
    assert_eq!(result.message_count(), 2);
}

#[tokio::test]
async fn test_reduce_group_by_json_field() {
    let config = ReduceConfig::default()
        .with_group_by(vec!["error_code".to_string()])
        .with_max_events(2)
        .with_min_events(1);
    let transformer = ReduceTransformer::new(config).unwrap();

    let batch = make_json_batch(&[
        r#"{"error_code": "E001", "msg": "first"}"#,
        r#"{"error_code": "E001", "msg": "second"}"#,
        r#"{"error_code": "E002", "msg": "other"}"#,
    ]);

    let result = transformer.transform(batch).await.unwrap();

    // E001 group hits max_events=2, E002 stays in state
    assert_eq!(result.message_count(), 1);
    assert_eq!(transformer.group_count().await, 1);
}

#[tokio::test]
async fn test_reduce_metrics() {
    let config = ReduceConfig::default()
        .with_max_events(3)
        .with_min_events(1);
    let transformer = ReduceTransformer::new(config).unwrap();

    let batch = make_test_batch(&[b"a", b"a", b"a", b"b"]);
    let _result = transformer.transform(batch).await.unwrap();

    let metrics = transformer.metrics();
    assert_eq!(metrics.batches_processed.load(Ordering::Relaxed), 1);
    assert_eq!(metrics.messages_received.load(Ordering::Relaxed), 4);
    assert_eq!(metrics.groups_created.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn test_reduce_flush_all() {
    let config = ReduceConfig::default().with_max_events(100); // High so nothing auto-flushes
    let transformer = ReduceTransformer::new(config).unwrap();

    let batch = make_test_batch(&[b"a", b"b", b"c"]);
    let _result = transformer.transform(batch).await.unwrap();

    assert_eq!(transformer.group_count().await, 3);

    let flushed = transformer.flush_all().await;
    assert_eq!(flushed.len(), 3);
    assert_eq!(transformer.group_count().await, 0);
}

#[tokio::test]
async fn test_reduce_factory() {
    let factory = ReduceFactory;
    assert_eq!(factory.name(), "reduce");

    let default_config = factory.default_config().unwrap();
    assert!(default_config.contains_key("window_ms"));
    assert!(default_config.contains_key("max_events"));
    assert!(default_config.contains_key("min_events"));
}

#[tokio::test]
async fn test_reduce_factory_create() {
    let factory = ReduceFactory;
    let mut config = HashMap::new();
    config.insert("window_ms".to_string(), toml::Value::Integer(1000));
    config.insert("max_events".to_string(), toml::Value::Integer(50));
    config.insert(
        "group_by".to_string(),
        toml::Value::Array(vec![toml::Value::String("field".to_string())]),
    );

    let transformer = factory.create(&config).unwrap();
    assert_eq!(transformer.name(), "reduce");
}

#[tokio::test]
async fn test_reduce_json_enrichment() {
    let config = ReduceConfig::default()
        .with_max_events(2)
        .with_min_events(1);
    let transformer = ReduceTransformer::new(config).unwrap();

    let batch = make_json_batch(&[r#"{"msg": "test"}"#, r#"{"msg": "test"}"#]);

    let result = transformer.transform(batch).await.unwrap();
    assert_eq!(result.message_count(), 1);

    let output = result.get_message(0).unwrap();
    let json: serde_json::Value = serde_json::from_slice(output).unwrap();

    assert_eq!(json["msg"], "test");
    assert_eq!(json["_reduced_count"], 2);
    assert!(json.get("_reduced_span_ms").is_some());
}

#[tokio::test]
async fn test_reduce_non_json_enrichment() {
    let config = ReduceConfig::default()
        .with_max_events(2)
        .with_min_events(1);
    let transformer = ReduceTransformer::new(config).unwrap();

    let batch = make_test_batch(&[b"plain text", b"plain text"]);

    let result = transformer.transform(batch).await.unwrap();
    assert_eq!(result.message_count(), 1);

    let output = result.get_message(0).unwrap();
    let output_str = String::from_utf8_lossy(output);
    assert!(output_str.contains("plain text"));
    assert!(output_str.contains("_reduced_count=2"));
}

#[tokio::test]
async fn test_reduce_below_min_events_passthrough() {
    let config = ReduceConfig::default()
        .with_max_events(5)
        .with_min_events(3); // Only reduce if 3+ events
    let transformer = ReduceTransformer::new(config).unwrap();

    // Send only 2 identical messages - below min_events threshold
    let batch = make_json_batch(&[r#"{"msg": "unique"}"#, r#"{"msg": "unique"}"#]);

    // Force flush to get output
    let _result = transformer.transform(batch).await.unwrap();
    let flushed = transformer.flush_all().await;

    assert_eq!(flushed.len(), 1);
    assert_eq!(flushed[0].count, 2); // 2 events, below min_events=3
}

#[tokio::test]
async fn test_reduce_nested_json_group_by() {
    let config = ReduceConfig::default()
        .with_group_by(vec!["error.code".to_string()])
        .with_max_events(2)
        .with_min_events(1);
    let transformer = ReduceTransformer::new(config).unwrap();

    let batch = make_json_batch(&[
        r#"{"error": {"code": "E001"}, "msg": "a"}"#,
        r#"{"error": {"code": "E001"}, "msg": "b"}"#,
    ]);

    let result = transformer.transform(batch).await.unwrap();
    assert_eq!(result.message_count(), 1);

    let output = result.get_message(0).unwrap();
    let json: serde_json::Value = serde_json::from_slice(output).unwrap();
    assert_eq!(json["_reduced_count"], 2);
}
