//! Tests for redact config

use super::*;

#[test]
fn test_default_config() {
    let config = RedactConfig::default();
    assert!(config.enabled);
    assert_eq!(config.strategy, RedactStrategy::Redact);
    assert!(config.hash_key.is_none());
    assert!(config.patterns.is_empty());
    assert!(config.fields.is_empty());
    assert!(!config.scan_all);
}

#[test]
fn test_config_builder() {
    let config = RedactConfig::new()
        .with_strategy(RedactStrategy::Hash)
        .with_hash_key("secret")
        .with_pattern(PatternType::Email)
        .with_scan_all();

    assert_eq!(config.strategy, RedactStrategy::Hash);
    assert_eq!(config.hash_key, Some("secret".to_string()));
    assert_eq!(config.patterns.len(), 1);
    assert!(config.scan_all);
}

#[test]
fn test_pattern_type_from_str() {
    assert_eq!(PatternType::parse("email"), Some(PatternType::Email));
    assert_eq!(PatternType::parse("phone"), Some(PatternType::Phone));
    assert_eq!(PatternType::parse("ssn_us"), Some(PatternType::SsnUs));
    assert_eq!(PatternType::parse("cpr_dk"), Some(PatternType::CprDk));
    assert_eq!(PatternType::parse("ipv4"), Some(PatternType::Ipv4));
    assert_eq!(PatternType::parse("ipv6"), Some(PatternType::Ipv6));
    assert_eq!(PatternType::parse("unknown"), None);
}

#[test]
fn test_pattern_type_hash_prefix() {
    assert_eq!(PatternType::Email.hash_prefix(), "usr_");
    assert_eq!(PatternType::Phone.hash_prefix(), "phn_");
    assert_eq!(PatternType::CreditCard.hash_prefix(), "cc_");
    assert_eq!(PatternType::Ipv4.hash_prefix(), "ip4_");
    assert_eq!(PatternType::Ipv6.hash_prefix(), "ip6_");
}

#[test]
fn test_validate_hash_requires_key() {
    let config = RedactConfig::new()
        .with_strategy(RedactStrategy::Hash)
        .with_pattern(PatternType::Email);

    let result = config.validate();
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("hash_key"));
}

#[test]
fn test_validate_requires_pattern() {
    let config = RedactConfig::new();

    let result = config.validate();
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("at least one"));
}

#[test]
fn test_validate_ok() {
    let config = RedactConfig::new()
        .with_pattern(PatternType::Email);

    let result = config.validate();
    assert!(result.is_ok());
}

#[test]
fn test_validate_hash_with_key_ok() {
    let config = RedactConfig::new()
        .with_strategy(RedactStrategy::Hash)
        .with_hash_key("secret")
        .with_pattern(PatternType::Email);

    let result = config.validate();
    assert!(result.is_ok());
}

#[test]
fn test_try_from_simple() {
    let toml = r#"
type = "redact"
strategy = "redact"
patterns = ["email", "phone"]
"#;
    let instance: tell_config::TransformerInstanceConfig = toml::from_str(toml).unwrap();
    let config = RedactConfig::try_from(&instance).unwrap();

    assert_eq!(config.strategy, RedactStrategy::Redact);
    assert_eq!(config.patterns.len(), 2);
    assert!(config.patterns.contains(&PatternType::Email));
    assert!(config.patterns.contains(&PatternType::Phone));
}

#[test]
fn test_try_from_hash_strategy() {
    let toml = r#"
type = "redact"
strategy = "hash"
hash_key = "my-secret-key"
patterns = ["email"]
"#;
    let instance: tell_config::TransformerInstanceConfig = toml::from_str(toml).unwrap();
    let config = RedactConfig::try_from(&instance).unwrap();

    assert_eq!(config.strategy, RedactStrategy::Hash);
    assert_eq!(config.hash_key, Some("my-secret-key".to_string()));
}

#[test]
fn test_try_from_with_fields() {
    let toml = r#"
type = "redact"
patterns = ["email"]

[[fields]]
path = "user.email"
pattern = "email"

[[fields]]
path = "ip_address"
pattern = "ipv4"
strategy = "hash"
"#;
    let instance: tell_config::TransformerInstanceConfig = toml::from_str(toml).unwrap();
    let config = RedactConfig::try_from(&instance).unwrap();

    assert_eq!(config.fields.len(), 2);
    assert_eq!(config.fields[0].path, "user.email");
    assert_eq!(config.fields[0].pattern, "email");
    assert_eq!(config.fields[1].strategy, Some(RedactStrategy::Hash));
}

#[test]
fn test_try_from_with_custom_patterns() {
    let toml = r#"
type = "redact"

[[custom_patterns]]
name = "employee_id"
regex = "EMP-\\d{6}"
prefix = "emp_"
"#;
    let instance: tell_config::TransformerInstanceConfig = toml::from_str(toml).unwrap();
    let config = RedactConfig::try_from(&instance).unwrap();

    assert_eq!(config.custom_patterns.len(), 1);
    assert_eq!(config.custom_patterns[0].name, "employee_id");
    assert_eq!(config.custom_patterns[0].prefix, "emp_");
}

#[test]
fn test_try_from_disabled() {
    let toml = r#"
type = "redact"
enabled = false
patterns = ["email"]
"#;
    let instance: tell_config::TransformerInstanceConfig = toml::from_str(toml).unwrap();
    let config = RedactConfig::try_from(&instance).unwrap();

    assert!(!config.enabled);
}

#[test]
fn test_try_from_invalid_strategy() {
    let toml = r#"
type = "redact"
strategy = "invalid"
patterns = ["email"]
"#;
    let instance: tell_config::TransformerInstanceConfig = toml::from_str(toml).unwrap();
    let result = RedactConfig::try_from(&instance);

    assert!(result.is_err());
    assert!(result.unwrap_err().contains("unknown strategy"));
}

#[test]
fn test_try_from_invalid_pattern() {
    let toml = r#"
type = "redact"
patterns = ["email", "invalid_pattern"]
"#;
    let instance: tell_config::TransformerInstanceConfig = toml::from_str(toml).unwrap();
    let result = RedactConfig::try_from(&instance);

    assert!(result.is_err());
    assert!(result.unwrap_err().contains("unknown pattern type"));
}

#[test]
fn test_try_from_scan_all() {
    let toml = r#"
type = "redact"
patterns = ["email"]
scan_all = true
"#;
    let instance: tell_config::TransformerInstanceConfig = toml::from_str(toml).unwrap();
    let config = RedactConfig::try_from(&instance).unwrap();

    assert!(config.scan_all);
}
