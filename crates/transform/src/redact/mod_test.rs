//! Tests for redact transformer

use super::*;
use std::sync::atomic::Ordering;
use tell_protocol::{BatchBuilder, BatchType, SourceId};

fn make_json_batch(messages: &[&str]) -> Batch {
    let mut builder = BatchBuilder::new(BatchType::Log, SourceId::new("test"));
    for msg in messages {
        builder.add_raw(msg.as_bytes());
    }
    builder.finish()
}

#[tokio::test]
async fn test_redact_transformer_new() {
    let config = RedactConfig::new().with_pattern(PatternType::Email);
    let transformer = RedactTransformer::new(config).unwrap();
    assert_eq!(transformer.name(), "redact");
    assert!(transformer.enabled());
}

#[tokio::test]
async fn test_redact_transformer_disabled() {
    let config = RedactConfig::new()
        .with_pattern(PatternType::Email)
        .disabled();
    let transformer = RedactTransformer::new(config).unwrap();
    assert!(!transformer.enabled());
}

#[tokio::test]
async fn test_redact_email_default_strategy() {
    let config = RedactConfig::new()
        .with_pattern(PatternType::Email)
        .with_scan_all();
    let transformer = RedactTransformer::new(config).unwrap();

    let batch = make_json_batch(&[r#"{"email": "user@example.com", "name": "John"}"#]);

    let result = transformer.transform(batch).await.unwrap();
    let msg = result.get_message(0).unwrap();
    let json: serde_json::Value = serde_json::from_slice(msg).unwrap();

    assert_eq!(json["email"], "[REDACTED]");
    assert_eq!(json["name"], "John");
}

#[tokio::test]
async fn test_redact_email_hash_strategy() {
    let config = RedactConfig::new()
        .with_strategy(RedactStrategy::Hash)
        .with_hash_key("test-secret-key")
        .with_pattern(PatternType::Email)
        .with_scan_all();
    let transformer = RedactTransformer::new(config).unwrap();

    let batch = make_json_batch(&[r#"{"email": "user@example.com", "name": "John"}"#]);

    let result = transformer.transform(batch).await.unwrap();
    let msg = result.get_message(0).unwrap();
    let json: serde_json::Value = serde_json::from_slice(msg).unwrap();

    let email_value = json["email"].as_str().unwrap();
    assert!(email_value.starts_with("usr_"));
    assert!(!email_value.contains("@"));
    assert_eq!(json["name"], "John");
}

#[tokio::test]
async fn test_redact_deterministic_hash() {
    let config = RedactConfig::new()
        .with_strategy(RedactStrategy::Hash)
        .with_hash_key("test-secret-key")
        .with_pattern(PatternType::Email)
        .with_scan_all();
    let transformer = RedactTransformer::new(config).unwrap();

    // Process same email twice
    let batch1 = make_json_batch(&[r#"{"email": "user@example.com"}"#]);
    let batch2 = make_json_batch(&[r#"{"email": "user@example.com"}"#]);

    let result1 = transformer.transform(batch1).await.unwrap();
    let result2 = transformer.transform(batch2).await.unwrap();

    let json1: serde_json::Value = serde_json::from_slice(result1.get_message(0).unwrap()).unwrap();
    let json2: serde_json::Value = serde_json::from_slice(result2.get_message(0).unwrap()).unwrap();

    // Same input should produce same hash
    assert_eq!(json1["email"], json2["email"]);
}

#[tokio::test]
async fn test_redact_multiple_patterns() {
    let config = RedactConfig::new()
        .with_strategy(RedactStrategy::Hash)
        .with_hash_key("test-key")
        .with_patterns(vec![
            PatternType::Email,
            PatternType::Phone,
            PatternType::Ipv4,
        ])
        .with_scan_all();
    let transformer = RedactTransformer::new(config).unwrap();

    let batch = make_json_batch(&[
        r#"{"email": "user@example.com", "phone": "+1-555-123-4567", "ip": "192.168.1.1"}"#,
    ]);

    let result = transformer.transform(batch).await.unwrap();
    let msg = result.get_message(0).unwrap();
    let json: serde_json::Value = serde_json::from_slice(msg).unwrap();

    assert!(json["email"].as_str().unwrap().starts_with("usr_"));
    assert!(json["phone"].as_str().unwrap().starts_with("phn_"));
    assert!(json["ip"].as_str().unwrap().starts_with("ip4_"));
}

#[tokio::test]
async fn test_redact_targeted_field() {
    let config = RedactConfig::new().with_field(TargetedField {
        path: "user.email".to_string(),
        pattern: "email".to_string(),
        strategy: None,
    });
    let transformer = RedactTransformer::new(config).unwrap();

    let batch = make_json_batch(&[r#"{"user": {"email": "user@example.com", "name": "John"}}"#]);

    let result = transformer.transform(batch).await.unwrap();
    let msg = result.get_message(0).unwrap();
    let json: serde_json::Value = serde_json::from_slice(msg).unwrap();

    assert_eq!(json["user"]["email"], "[REDACTED]");
    assert_eq!(json["user"]["name"], "John");
}

#[tokio::test]
async fn test_redact_targeted_field_with_strategy_override() {
    let config = RedactConfig::new()
        .with_strategy(RedactStrategy::Redact) // Default is redact
        .with_hash_key("test-key") // But we have a key for overrides
        .with_field(TargetedField {
            path: "email".to_string(),
            pattern: "email".to_string(),
            strategy: Some(RedactStrategy::Hash), // Override to hash
        });

    // Need to manually set hasher since strategy is Redact but field uses Hash
    let mut transformer = RedactTransformer::new(config).unwrap();
    transformer.hasher = Some(PseudonymHasher::new("test-key"));

    let batch = make_json_batch(&[r#"{"email": "user@example.com"}"#]);

    let result = transformer.transform(batch).await.unwrap();
    let msg = result.get_message(0).unwrap();
    let json: serde_json::Value = serde_json::from_slice(msg).unwrap();

    assert!(json["email"].as_str().unwrap().starts_with("usr_"));
}

#[tokio::test]
async fn test_redact_ssn_us() {
    let config = RedactConfig::new()
        .with_pattern(PatternType::SsnUs)
        .with_scan_all();
    let transformer = RedactTransformer::new(config).unwrap();

    let batch = make_json_batch(&[r#"{"ssn": "123-45-6789", "name": "John"}"#]);

    let result = transformer.transform(batch).await.unwrap();
    let msg = result.get_message(0).unwrap();
    let json: serde_json::Value = serde_json::from_slice(msg).unwrap();

    assert_eq!(json["ssn"], "[REDACTED]");
}

#[tokio::test]
async fn test_redact_cpr_dk() {
    let config = RedactConfig::new()
        .with_pattern(PatternType::CprDk)
        .with_scan_all();
    let transformer = RedactTransformer::new(config).unwrap();

    let batch = make_json_batch(&[r#"{"cpr": "010190-1234"}"#]);

    let result = transformer.transform(batch).await.unwrap();
    let msg = result.get_message(0).unwrap();
    let json: serde_json::Value = serde_json::from_slice(msg).unwrap();

    assert_eq!(json["cpr"], "[REDACTED]");
}

#[tokio::test]
async fn test_redact_credit_card() {
    let config = RedactConfig::new()
        .with_pattern(PatternType::CreditCard)
        .with_scan_all();
    let transformer = RedactTransformer::new(config).unwrap();

    let batch = make_json_batch(&[r#"{"card": "4111-1111-1111-1111"}"#]);

    let result = transformer.transform(batch).await.unwrap();
    let msg = result.get_message(0).unwrap();
    let json: serde_json::Value = serde_json::from_slice(msg).unwrap();

    assert_eq!(json["card"], "[REDACTED]");
}

#[tokio::test]
async fn test_redact_no_match_passthrough() {
    let config = RedactConfig::new()
        .with_pattern(PatternType::Email)
        .with_scan_all();
    let transformer = RedactTransformer::new(config).unwrap();

    let batch = make_json_batch(&[r#"{"name": "John", "age": 30}"#]);

    let result = transformer.transform(batch).await.unwrap();
    let msg = result.get_message(0).unwrap();
    let json: serde_json::Value = serde_json::from_slice(msg).unwrap();

    assert_eq!(json["name"], "John");
    assert_eq!(json["age"], 30);
}

#[tokio::test]
async fn test_redact_metrics() {
    let config = RedactConfig::new()
        .with_pattern(PatternType::Email)
        .with_scan_all();
    let transformer = RedactTransformer::new(config).unwrap();

    let batch = make_json_batch(&[r#"{"email": "a@b.com"}"#, r#"{"email": "c@d.com"}"#]);

    let _result = transformer.transform(batch).await.unwrap();

    let metrics = transformer.metrics();
    assert_eq!(metrics.batches_processed.load(Ordering::Relaxed), 1);
    assert_eq!(metrics.messages_processed.load(Ordering::Relaxed), 2);
    assert_eq!(metrics.patterns_matched.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn test_redact_factory() {
    let factory = RedactFactory;
    assert_eq!(factory.name(), "redact");

    let default_config = factory.default_config().unwrap();
    assert!(default_config.contains_key("strategy"));
}

#[tokio::test]
async fn test_redact_non_json_scan() {
    let config = RedactConfig::new()
        .with_pattern(PatternType::Email)
        .with_scan_all();
    let transformer = RedactTransformer::new(config).unwrap();

    let mut builder = BatchBuilder::new(BatchType::Log, SourceId::new("test"));
    builder.add_raw(b"Contact user@example.com for help");
    let batch = builder.finish();

    let result = transformer.transform(batch).await.unwrap();
    let msg = std::str::from_utf8(result.get_message(0).unwrap()).unwrap();

    assert!(msg.contains("[REDACTED]"));
    assert!(!msg.contains("user@example.com"));
}

#[tokio::test]
async fn test_redact_empty_batch() {
    let config = RedactConfig::new().with_pattern(PatternType::Email);
    let transformer = RedactTransformer::new(config).unwrap();

    let batch = make_json_batch(&[]);
    let result = transformer.transform(batch).await.unwrap();

    assert_eq!(result.message_count(), 0);
}

#[tokio::test]
async fn test_redact_validation_hash_requires_key() {
    let config = RedactConfig::new()
        .with_strategy(RedactStrategy::Hash)
        .with_pattern(PatternType::Email);
    // Missing hash_key

    let result = RedactTransformer::new(config);
    assert!(result.is_err());
}

#[tokio::test]
async fn test_redact_validation_requires_pattern() {
    let config = RedactConfig::new();
    // No patterns

    let result = RedactTransformer::new(config);
    assert!(result.is_err());
}
