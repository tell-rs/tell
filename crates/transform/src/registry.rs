//! Transformer Registry - Dynamic transformer creation
//!
//! The registry maps transformer type names to factory functions,
//! enabling configuration-driven transformer instantiation.
//!
//! # Design
//!
//! - **Compile-time extensibility**: Users implement `TransformerFactory` trait
//! - **Type-safe**: Factories return `Box<dyn Transformer>`
//! - **Config-driven**: TOML/YAML config specifies transformer type by name
//!
//! # Example
//!
//! ```ignore
//! let mut registry = TransformerRegistry::new();
//! registry.register("noop", NoopFactory);
//! registry.register("pattern_matcher", PatternMatcherFactory);
//!
//! // From config
//! let transformer = registry.create("noop", &config)?;
//! ```

use crate::{TransformError, TransformResult, Transformer};
use std::collections::HashMap;

#[cfg(test)]
#[path = "registry_test.rs"]
mod tests;

/// Configuration passed to transformer factories
///
/// This is a generic key-value map that factories interpret
/// according to their specific needs.
pub type TransformerConfig = HashMap<String, toml::Value>;

/// Factory trait for creating transformers
///
/// Implement this trait to register custom transformers with the registry.
pub trait TransformerFactory: Send + Sync {
    /// Create a transformer instance from configuration
    ///
    /// # Arguments
    /// * `config` - Configuration map from TOML
    ///
    /// # Errors
    /// Returns `TransformError::Config` if configuration is invalid
    fn create(&self, config: &TransformerConfig) -> TransformResult<Box<dyn Transformer>>;

    /// Human-readable name for this factory (for error messages)
    fn name(&self) -> &'static str;

    /// Default configuration for this transformer type
    ///
    /// Returns None if no defaults exist.
    fn default_config(&self) -> Option<TransformerConfig> {
        None
    }
}

/// Registry for transformer factories
///
/// Maps transformer type names (e.g., "noop", "pattern_matcher") to their
/// factory implementations.
pub struct TransformerRegistry {
    factories: HashMap<String, Box<dyn TransformerFactory>>,
}

impl TransformerRegistry {
    /// Create a new empty registry
    pub fn new() -> Self {
        Self {
            factories: HashMap::new(),
        }
    }

    /// Register a transformer factory
    ///
    /// # Arguments
    /// * `type_name` - The name used in config files (e.g., "pattern_matcher")
    /// * `factory` - Factory that creates this transformer type
    ///
    /// # Panics
    /// Panics if a factory is already registered with this name.
    /// Use `try_register` for fallible registration.
    pub fn register<F: TransformerFactory + 'static>(&mut self, type_name: &str, factory: F) {
        if self.factories.contains_key(type_name) {
            panic!("Transformer factory '{}' already registered", type_name);
        }
        self.factories
            .insert(type_name.to_string(), Box::new(factory));
    }

    /// Try to register a transformer factory
    ///
    /// Returns `false` if a factory is already registered with this name.
    pub fn try_register<F: TransformerFactory + 'static>(
        &mut self,
        type_name: &str,
        factory: F,
    ) -> bool {
        if self.factories.contains_key(type_name) {
            return false;
        }
        self.factories
            .insert(type_name.to_string(), Box::new(factory));
        true
    }

    /// Create a transformer from its type name and configuration
    ///
    /// # Arguments
    /// * `type_name` - The transformer type (e.g., "noop", "pattern_matcher")
    /// * `config` - Configuration for this transformer instance
    ///
    /// # Errors
    /// - `TransformError::Config` if the type is not registered
    /// - `TransformError::Config` if factory fails to create transformer
    pub fn create(
        &self,
        type_name: &str,
        config: &TransformerConfig,
    ) -> TransformResult<Box<dyn Transformer>> {
        let factory = self.factories.get(type_name).ok_or_else(|| {
            TransformError::config(format!(
                "unknown transformer type '{}', available: [{}]",
                type_name,
                self.available_types().join(", ")
            ))
        })?;

        factory.create(config)
    }

    /// Check if a transformer type is registered
    pub fn contains(&self, type_name: &str) -> bool {
        self.factories.contains_key(type_name)
    }

    /// Get list of registered transformer types
    pub fn available_types(&self) -> Vec<&str> {
        self.factories.keys().map(|s| s.as_str()).collect()
    }

    /// Get default configuration for a transformer type
    ///
    /// Returns None if the type is not registered or has no defaults.
    pub fn default_config(&self, type_name: &str) -> Option<TransformerConfig> {
        self.factories
            .get(type_name)
            .and_then(|f| f.default_config())
    }

    /// Get the number of registered factories
    pub fn len(&self) -> usize {
        self.factories.len()
    }

    /// Check if the registry is empty
    pub fn is_empty(&self) -> bool {
        self.factories.is_empty()
    }
}

impl Default for TransformerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Factory for NoopTransformer
///
/// This is built-in and always available.
pub struct NoopFactory;

impl TransformerFactory for NoopFactory {
    fn create(&self, _config: &TransformerConfig) -> TransformResult<Box<dyn Transformer>> {
        Ok(Box::new(crate::noop::NoopTransformer::new()))
    }

    fn name(&self) -> &'static str {
        "noop"
    }
}

/// Create a registry with all built-in transformers registered
///
/// Includes:
/// - `noop` - Pass-through transformer
/// - `pattern_matcher` - Log pattern extraction (when pattern module is ready)
pub fn default_registry() -> TransformerRegistry {
    let mut registry = TransformerRegistry::new();
    registry.register("noop", NoopFactory);
    // TODO: register pattern_matcher when implemented
    registry
}
