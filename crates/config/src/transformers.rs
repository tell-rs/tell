//! Transformer configuration
//!
//! Defines transformer instances that can be applied per-route.
//! Each transformer has a type and type-specific configuration.
//!
//! # Example
//!
//! ```toml
//! [[routing.rules]]
//! match = { source_type = "syslog" }
//! sinks = ["clickhouse"]
//!
//! [[routing.rules.transformers]]
//! type = "pattern_matcher"
//! similarity_threshold = 0.5
//! cache_size = 100000
//!
//! [[routing.rules.transformers]]
//! type = "noop"
//! ```

use serde::Deserialize;
use std::collections::HashMap;
use std::path::PathBuf;

/// Configuration for a single transformer instance
///
/// Each transformer has a type that determines its behavior,
/// plus type-specific configuration options.
#[derive(Debug, Clone, Deserialize)]
pub struct TransformerInstanceConfig {
    /// Transformer type (e.g., "noop", "pattern_matcher")
    #[serde(rename = "type")]
    pub transformer_type: String,

    /// Whether this transformer is enabled (default: true)
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Type-specific configuration options
    /// These are passed to the transformer factory
    #[serde(flatten)]
    pub options: HashMap<String, toml::Value>,
}

fn default_true() -> bool {
    true
}

impl TransformerInstanceConfig {
    /// Create a new noop transformer config
    pub fn noop() -> Self {
        Self {
            transformer_type: "noop".to_string(),
            enabled: true,
            options: HashMap::new(),
        }
    }

    /// Create a new pattern matcher config with defaults
    pub fn pattern_matcher() -> Self {
        Self {
            transformer_type: "pattern_matcher".to_string(),
            enabled: true,
            options: HashMap::new(),
        }
    }

    /// Create a new reduce transformer config with defaults
    pub fn reduce() -> Self {
        Self {
            transformer_type: "reduce".to_string(),
            enabled: true,
            options: HashMap::new(),
        }
    }

    /// Create a new filter transformer config with defaults
    pub fn filter() -> Self {
        Self {
            transformer_type: "filter".to_string(),
            enabled: true,
            options: HashMap::new(),
        }
    }

    /// Create a new redact transformer config with defaults
    pub fn redact() -> Self {
        Self {
            transformer_type: "redact".to_string(),
            enabled: true,
            options: HashMap::new(),
        }
    }

    /// Get an array option as Vec<String>
    pub fn get_string_array(&self, key: &str) -> Option<Vec<String>> {
        self.options.get(key).and_then(|v| {
            v.as_array().map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
        })
    }

    /// Get an option as a specific type
    pub fn get_bool(&self, key: &str) -> Option<bool> {
        self.options.get(key).and_then(|v| v.as_bool())
    }

    /// Get an option as f64
    pub fn get_float(&self, key: &str) -> Option<f64> {
        self.options.get(key).and_then(|v| v.as_float())
    }

    /// Get an option as i64
    pub fn get_int(&self, key: &str) -> Option<i64> {
        self.options.get(key).and_then(|v| v.as_integer())
    }

    /// Get an option as string
    pub fn get_str(&self, key: &str) -> Option<&str> {
        self.options.get(key).and_then(|v| v.as_str())
    }

    /// Get an option as PathBuf
    pub fn get_path(&self, key: &str) -> Option<PathBuf> {
        self.get_str(key).map(PathBuf::from)
    }
}

/// Known transformer types for validation
pub const KNOWN_TRANSFORMER_TYPES: &[&str] = &["noop", "pattern_matcher", "reduce", "filter", "redact"];

/// Check if a transformer type is known
pub fn is_known_transformer_type(transformer_type: &str) -> bool {
    KNOWN_TRANSFORMER_TYPES.contains(&transformer_type)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_noop() {
        let toml = r#"
type = "noop"
"#;
        let config: TransformerInstanceConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.transformer_type, "noop");
        assert!(config.enabled);
        assert!(config.options.is_empty());
    }

    #[test]
    fn test_deserialize_pattern_matcher() {
        let toml = r#"
type = "pattern_matcher"
similarity_threshold = 0.5
cache_size = 100000
max_child_nodes = 50
"#;
        let config: TransformerInstanceConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.transformer_type, "pattern_matcher");
        assert!(config.enabled);
        assert_eq!(config.get_float("similarity_threshold"), Some(0.5));
        assert_eq!(config.get_int("cache_size"), Some(100000));
        assert_eq!(config.get_int("max_child_nodes"), Some(50));
    }

    #[test]
    fn test_deserialize_with_persistence() {
        let toml = r#"
type = "pattern_matcher"
persistence_enabled = true
persistence_file = "/var/lib/cdp/patterns.json"
"#;
        let config: TransformerInstanceConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.get_bool("persistence_enabled"), Some(true));
        assert_eq!(
            config.get_str("persistence_file"),
            Some("/var/lib/cdp/patterns.json")
        );
        assert_eq!(
            config.get_path("persistence_file"),
            Some(PathBuf::from("/var/lib/cdp/patterns.json"))
        );
    }

    #[test]
    fn test_deserialize_disabled() {
        let toml = r#"
type = "pattern_matcher"
enabled = false
"#;
        let config: TransformerInstanceConfig = toml::from_str(toml).unwrap();
        assert!(!config.enabled);
    }

    #[test]
    fn test_default_enabled() {
        let toml = r#"
type = "noop"
"#;
        let config: TransformerInstanceConfig = toml::from_str(toml).unwrap();
        assert!(config.enabled);
    }

    #[test]
    fn test_noop_constructor() {
        let config = TransformerInstanceConfig::noop();
        assert_eq!(config.transformer_type, "noop");
        assert!(config.enabled);
    }

    #[test]
    fn test_pattern_matcher_constructor() {
        let config = TransformerInstanceConfig::pattern_matcher();
        assert_eq!(config.transformer_type, "pattern_matcher");
        assert!(config.enabled);
    }

    #[test]
    fn test_known_transformer_types() {
        assert!(is_known_transformer_type("noop"));
        assert!(is_known_transformer_type("pattern_matcher"));
        assert!(!is_known_transformer_type("unknown"));
    }

    #[test]
    fn test_get_missing_option() {
        let config = TransformerInstanceConfig::noop();
        assert_eq!(config.get_bool("missing"), None);
        assert_eq!(config.get_float("missing"), None);
        assert_eq!(config.get_int("missing"), None);
        assert_eq!(config.get_str("missing"), None);
    }
}
