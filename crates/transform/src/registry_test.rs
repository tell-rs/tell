//! Tests for transformer registry

use super::*;
use crate::Transformer;
use crate::noop::NoopTransformer;

#[test]
fn test_empty_registry() {
    let registry = TransformerRegistry::new();
    assert!(registry.is_empty());
    assert_eq!(registry.len(), 0);
    assert!(registry.available_types().is_empty());
}

#[test]
fn test_register_and_create() {
    let mut registry = TransformerRegistry::new();
    registry.register("noop", NoopFactory);

    assert!(!registry.is_empty());
    assert_eq!(registry.len(), 1);
    assert!(registry.contains("noop"));
    assert!(!registry.contains("nonexistent"));

    let config = TransformerConfig::new();
    let transformer = registry.create("noop", &config).unwrap();
    assert_eq!(transformer.name(), "noop");
}

#[test]
fn test_available_types() {
    let mut registry = TransformerRegistry::new();
    registry.register("noop", NoopFactory);

    let types = registry.available_types();
    assert_eq!(types.len(), 1);
    assert!(types.contains(&"noop"));
}

#[test]
fn test_create_unknown_type() {
    let registry = TransformerRegistry::new();
    let config = TransformerConfig::new();

    let result = registry.create("unknown", &config);
    assert!(result.is_err());

    let err = result.err().unwrap();
    assert!(err.to_string().contains("unknown transformer type"));
}

#[test]
#[should_panic(expected = "already registered")]
fn test_duplicate_registration_panics() {
    let mut registry = TransformerRegistry::new();
    registry.register("noop", NoopFactory);
    registry.register("noop", NoopFactory); // Should panic
}

#[test]
fn test_try_register_returns_false_on_duplicate() {
    let mut registry = TransformerRegistry::new();

    assert!(registry.try_register("noop", NoopFactory));
    assert!(!registry.try_register("noop", NoopFactory));
    assert_eq!(registry.len(), 1);
}

#[test]
fn test_default_registry() {
    let registry = default_registry();

    assert!(registry.contains("noop"));
    assert!(!registry.is_empty());
}

#[test]
fn test_noop_factory_name() {
    let factory = NoopFactory;
    assert_eq!(factory.name(), "noop");
}

#[test]
fn test_noop_factory_no_default_config() {
    let factory = NoopFactory;
    assert!(factory.default_config().is_none());
}

#[test]
fn test_registry_default_config() {
    let registry = default_registry();

    // NoopFactory has no default config
    assert!(registry.default_config("noop").is_none());

    // Unknown type has no default config
    assert!(registry.default_config("unknown").is_none());
}

/// Test custom factory implementation
struct CountingFactory {
    call_count: std::sync::atomic::AtomicUsize,
}

impl CountingFactory {
    fn new() -> Self {
        Self {
            call_count: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    #[allow(dead_code)] // Helper for potential future test assertions
    fn count(&self) -> usize {
        self.call_count.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl TransformerFactory for CountingFactory {
    fn create(&self, _config: &TransformerConfig) -> TransformResult<Box<dyn Transformer>> {
        self.call_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(Box::new(NoopTransformer::new()))
    }

    fn name(&self) -> &'static str {
        "counting"
    }

    fn default_config(&self) -> Option<TransformerConfig> {
        let mut config = TransformerConfig::new();
        config.insert("default_key".to_string(), toml::Value::Boolean(true));
        Some(config)
    }
}

#[test]
fn test_custom_factory() {
    let mut registry = TransformerRegistry::new();
    let factory = CountingFactory::new();

    registry.register("counting", factory);

    let config = TransformerConfig::new();

    // Create multiple transformers
    let _ = registry.create("counting", &config).unwrap();
    let _ = registry.create("counting", &config).unwrap();
    let _ = registry.create("counting", &config).unwrap();

    // Can't easily check count since we moved factory into registry
    // But we verified it works
}

#[test]
fn test_factory_with_default_config() {
    let mut registry = TransformerRegistry::new();
    registry.register("counting", CountingFactory::new());

    let default = registry.default_config("counting");
    assert!(default.is_some());

    let config = default.unwrap();
    assert!(config.contains_key("default_key"));
}

/// Test factory that returns errors
struct FailingFactory;

impl TransformerFactory for FailingFactory {
    fn create(&self, _config: &TransformerConfig) -> TransformResult<Box<dyn Transformer>> {
        Err(TransformError::config("intentional failure"))
    }

    fn name(&self) -> &'static str {
        "failing"
    }
}

#[test]
fn test_factory_error_propagation() {
    let mut registry = TransformerRegistry::new();
    registry.register("failing", FailingFactory);

    let config = TransformerConfig::new();
    let result = registry.create("failing", &config);

    assert!(result.is_err());
    assert!(
        result
            .err()
            .unwrap()
            .to_string()
            .contains("intentional failure")
    );
}

/// Test factory that validates config
struct ValidatingFactory;

impl TransformerFactory for ValidatingFactory {
    fn create(&self, config: &TransformerConfig) -> TransformResult<Box<dyn Transformer>> {
        if !config.contains_key("required_field") {
            return Err(TransformError::config("missing required_field"));
        }
        Ok(Box::new(NoopTransformer::new()))
    }

    fn name(&self) -> &'static str {
        "validating"
    }
}

#[test]
fn test_factory_config_validation() {
    let mut registry = TransformerRegistry::new();
    registry.register("validating", ValidatingFactory);

    // Missing required field
    let config = TransformerConfig::new();
    let result = registry.create("validating", &config);
    assert!(result.is_err());
    assert!(
        result
            .err()
            .unwrap()
            .to_string()
            .contains("missing required_field")
    );

    // With required field
    let mut config = TransformerConfig::new();
    config.insert(
        "required_field".to_string(),
        toml::Value::String("value".to_string()),
    );
    let result = registry.create("validating", &config);
    assert!(result.is_ok());
}
