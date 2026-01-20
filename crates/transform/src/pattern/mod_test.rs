//! Tests for PatternTransformer

use super::*;
use tell_protocol::{BatchBuilder, SourceId};

fn create_log_batch(messages: &[&str]) -> Batch {
    let source_id = SourceId::new("test");
    let mut builder = BatchBuilder::new(BatchType::Log, source_id);
    builder.set_workspace_id(1);

    for msg in messages {
        builder.add(msg.as_bytes(), 1);
    }

    builder.finish()
}

fn create_event_batch() -> Batch {
    let source_id = SourceId::new("test");
    let mut builder = BatchBuilder::new(BatchType::Event, source_id);
    builder.add(b"event data", 1);
    builder.finish()
}

#[tokio::test]
async fn test_pattern_transformer_creation() {
    let config = PatternConfig::default();
    let transformer = PatternTransformer::new(config);

    assert!(transformer.is_ok());
}

#[tokio::test]
async fn test_invalid_config_rejected() {
    let mut config = PatternConfig::default();
    config.similarity_threshold = 2.0; // Invalid

    let result = PatternTransformer::new(config);
    assert!(result.is_err());
}

#[tokio::test]
async fn test_transformer_name() {
    let transformer = PatternTransformer::new(PatternConfig::default()).unwrap();
    assert_eq!(transformer.name(), "pattern_matcher");
}

#[tokio::test]
async fn test_transformer_enabled() {
    let transformer = PatternTransformer::new(PatternConfig::default()).unwrap();
    assert!(transformer.enabled());

    let transformer = PatternTransformer::new(PatternConfig::default().disabled()).unwrap();
    assert!(!transformer.enabled());
}

#[tokio::test]
async fn test_disabled_transformer_passthrough() {
    let config = PatternConfig::default().disabled();
    let transformer = PatternTransformer::new(config).unwrap();

    let batch = create_log_batch(&["test message"]);
    let result = transformer.transform(batch).await.unwrap();

    // Should not have pattern IDs
    assert!(result.pattern_ids().is_none());
}

#[tokio::test]
async fn test_process_log_batch() {
    let transformer = PatternTransformer::new(PatternConfig::default()).unwrap();

    let batch = create_log_batch(&[
        "User 123 logged in",
        "User 456 logged in",
        "Connection error",
    ]);

    let result = transformer.transform(batch).await.unwrap();

    // Should have pattern IDs
    let pattern_ids = result.pattern_ids().unwrap();
    assert_eq!(pattern_ids.len(), 3);

    // First two should match same pattern
    assert_eq!(pattern_ids[0], pattern_ids[1]);

    // Third should be different
    assert_ne!(pattern_ids[0], pattern_ids[2]);
}

#[tokio::test]
async fn test_event_batch_passthrough() {
    let transformer = PatternTransformer::new(PatternConfig::default()).unwrap();

    let batch = create_event_batch();
    let result = transformer.transform(batch).await.unwrap();

    // Event batches should not get pattern IDs
    assert!(result.pattern_ids().is_none());
}

#[tokio::test]
async fn test_pattern_count() {
    let transformer = PatternTransformer::new(PatternConfig::default()).unwrap();

    assert_eq!(transformer.pattern_count(), 0);

    // Use very different messages to ensure they create different patterns
    let batch = create_log_batch(&["ERROR connection failed", "INFO startup complete"]);
    transformer.transform(batch).await.unwrap();

    // At least one pattern should be created
    assert!(transformer.pattern_count() >= 1);
}

#[tokio::test]
async fn test_get_pattern() {
    let transformer = PatternTransformer::new(PatternConfig::default()).unwrap();

    let batch = create_log_batch(&["Error in module XYZ"]);
    let result = transformer.transform(batch).await.unwrap();

    let pattern_ids = result.pattern_ids().unwrap();
    let pattern = transformer.get_pattern(pattern_ids[0]);

    assert!(pattern.is_some());
    let p = pattern.unwrap();
    assert!(p.template.contains("Error"));
}

#[tokio::test]
async fn test_metrics() {
    let transformer = PatternTransformer::new(PatternConfig::default()).unwrap();

    let batch = create_log_batch(&["msg1", "msg2", "msg3"]);
    transformer.transform(batch).await.unwrap();

    let metrics = transformer.metrics();
    assert_eq!(metrics.batches_processed.load(Ordering::Relaxed), 1);
    assert_eq!(metrics.messages_processed.load(Ordering::Relaxed), 3);
}

#[tokio::test]
async fn test_cache_effectiveness() {
    let transformer = PatternTransformer::new(PatternConfig::default()).unwrap();

    // Process same messages multiple times
    for _ in 0..3 {
        let batch = create_log_batch(&["Repeated message 123"]);
        transformer.transform(batch).await.unwrap();
    }

    let metrics = transformer.metrics();

    // First lookup is cache miss, subsequent are hits
    let hits = metrics.cache_hits.load(Ordering::Relaxed);
    let misses = metrics.cache_misses.load(Ordering::Relaxed);

    assert!(hits >= 2, "Should have at least 2 cache hits");
    assert!(
        misses >= 1,
        "Should have at least 1 cache miss (first lookup)"
    );
}

#[tokio::test]
async fn test_pattern_factory() {
    let factory = PatternFactory;
    let config = TransformerConfig::new();

    let transformer = factory.create(&config);
    assert!(transformer.is_ok());

    let t = transformer.unwrap();
    assert_eq!(t.name(), "pattern_matcher");
}

#[tokio::test]
async fn test_pattern_factory_with_config() {
    let factory = PatternFactory;

    let mut config = TransformerConfig::new();
    config.insert("similarity_threshold".to_string(), toml::Value::Float(0.7));
    config.insert("max_child_nodes".to_string(), toml::Value::Integer(50));

    let transformer = factory.create(&config);
    assert!(transformer.is_ok());
}

#[tokio::test]
async fn test_pattern_factory_invalid_config() {
    let factory = PatternFactory;

    let mut config = TransformerConfig::new();
    config.insert("similarity_threshold".to_string(), toml::Value::Float(2.0));

    let transformer = factory.create(&config);
    assert!(transformer.is_err());
}

#[tokio::test]
async fn test_pattern_factory_default_config() {
    let factory = PatternFactory;
    let defaults = factory.default_config();

    assert!(defaults.is_some());
    let config = defaults.unwrap();
    assert!(config.contains_key("similarity_threshold"));
    assert!(config.contains_key("max_child_nodes"));
    assert!(config.contains_key("cache_size"));
}

#[tokio::test]
async fn test_empty_batch() {
    let transformer = PatternTransformer::new(PatternConfig::default()).unwrap();

    let source_id = SourceId::new("test");
    let builder = BatchBuilder::new(BatchType::Log, source_id);
    let batch = builder.finish();

    let result = transformer.transform(batch).await.unwrap();

    // Empty batch should have empty pattern_ids
    let pattern_ids = result.pattern_ids();
    assert!(pattern_ids.is_none() || pattern_ids.unwrap().is_empty());
}

#[tokio::test]
async fn test_similar_messages_same_pattern() {
    let transformer = PatternTransformer::new(PatternConfig::default()).unwrap();

    let batch = create_log_batch(&[
        "Request from 192.168.1.1 took 100ms",
        "Request from 10.0.0.1 took 50ms",
        "Request from 172.16.0.1 took 200ms",
    ]);

    let result = transformer.transform(batch).await.unwrap();
    let pattern_ids = result.pattern_ids().unwrap();

    // All should match the same pattern
    assert_eq!(pattern_ids[0], pattern_ids[1]);
    assert_eq!(pattern_ids[1], pattern_ids[2]);
}

#[test]
fn test_metrics_cache_hit_rate() {
    let metrics = PatternMetrics::default();

    // No lookups = 0% hit rate
    assert_eq!(metrics.cache_hit_rate(), 0.0);

    // 50% hit rate
    metrics.cache_hits.store(5, Ordering::Relaxed);
    metrics.cache_misses.store(5, Ordering::Relaxed);
    assert_eq!(metrics.cache_hit_rate(), 0.5);

    // 100% hit rate
    metrics.cache_hits.store(10, Ordering::Relaxed);
    metrics.cache_misses.store(0, Ordering::Relaxed);
    assert_eq!(metrics.cache_hit_rate(), 1.0);
}

#[tokio::test]
async fn test_multiple_batches() {
    let transformer = PatternTransformer::new(PatternConfig::default()).unwrap();

    // Process multiple batches
    for i in 0..5 {
        let batch = create_log_batch(&[&format!("Batch {} message", i)]);
        transformer.transform(batch).await.unwrap();
    }

    let metrics = transformer.metrics();
    assert_eq!(metrics.batches_processed.load(Ordering::Relaxed), 5);
    assert_eq!(metrics.messages_processed.load(Ordering::Relaxed), 5);
}
