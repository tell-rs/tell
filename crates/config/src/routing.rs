//! Routing configuration
//!
//! Defines how sources connect to sinks. Routes are evaluated in order;
//! first match wins. Unmatched traffic goes to the default sinks.
//!
//! # Example
//!
//! ```toml
//! [routing]
//! default = ["stdout"]
//!
//! [[routing.rules]]
//! match = { source = "tcp" }
//! sinks = ["clickhouse", "disk_plaintext"]
//!
//! [[routing.rules]]
//! match = { source = "tcp_debug" }
//! sinks = ["stdout"]
//!
//! [[routing.rules]]
//! match = { source_type = "syslog" }
//! sinks = ["disk_plaintext"]
//!
//! # With transformers
//! [[routing.rules.transformers]]
//! type = "pattern_matcher"
//! similarity_threshold = 0.5
//! ```

use crate::transformers::TransformerInstanceConfig;
use serde::Deserialize;

/// Routing configuration - defines how sources connect to sinks
///
/// The routing system provides flexible source-to-sink mapping:
/// - Routes are evaluated in order
/// - First matching rule wins
/// - Unmatched traffic goes to default sinks
/// - Empty default = unmatched traffic is dropped
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct RoutingConfig {
    /// Default sinks for unmatched traffic
    /// If empty, unmatched traffic is dropped
    pub default: Vec<String>,

    /// Routing rules (evaluated in order, first match wins)
    pub rules: Vec<RoutingRule>,
}

impl RoutingConfig {
    /// Check if any routing rules are configured
    pub fn has_rules(&self) -> bool {
        !self.rules.is_empty()
    }

    /// Check if default sinks are configured
    pub fn has_default(&self) -> bool {
        !self.default.is_empty()
    }

    /// Get all sink names referenced in routing (for validation)
    pub fn referenced_sinks(&self) -> Vec<&str> {
        let mut sinks: Vec<&str> = self.default.iter().map(|s| s.as_str()).collect();

        for rule in &self.rules {
            for sink in &rule.sinks {
                if !sinks.contains(&sink.as_str()) {
                    sinks.push(sink.as_str());
                }
            }
        }

        sinks
    }

    /// Get all source names/types referenced in routing (for validation)
    pub fn referenced_sources(&self) -> Vec<&str> {
        let mut sources: Vec<&str> = Vec::new();

        for rule in &self.rules {
            if let Some(ref source) = rule.match_condition.source
                && !sources.contains(&source.as_str())
            {
                sources.push(source.as_str());
            }
            if let Some(ref source_type) = rule.match_condition.source_type
                && !sources.contains(&source_type.as_str())
            {
                sources.push(source_type.as_str());
            }
        }

        sources
    }
}

/// A single routing rule
///
/// Rules have a match condition, optional transformers, and target sinks.
/// When traffic matches the condition, transformers are applied in order,
/// then the batch is sent to all listed sinks.
#[derive(Debug, Clone, Deserialize)]
pub struct RoutingRule {
    /// Match conditions (source name or type)
    #[serde(rename = "match")]
    pub match_condition: MatchCondition,

    /// Transformers to apply before routing (in order)
    /// Each transformer processes the batch before passing to the next
    #[serde(default)]
    pub transformers: Vec<TransformerInstanceConfig>,

    /// Target sinks for matched traffic
    pub sinks: Vec<String>,
}

impl RoutingRule {
    /// Check if this rule has any transformers configured
    pub fn has_transformers(&self) -> bool {
        !self.transformers.is_empty()
    }

    /// Get enabled transformers only
    pub fn enabled_transformers(&self) -> impl Iterator<Item = &TransformerInstanceConfig> {
        self.transformers.iter().filter(|t| t.enabled)
    }
}

/// Conditions for matching traffic
///
/// At least one condition should be specified. If multiple are specified,
/// all must match (AND logic).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct MatchCondition {
    /// Match by exact source name (e.g., "tcp", "tcp_debug", "syslog_tcp")
    pub source: Option<String>,

    /// Match by source type prefix (e.g., "tcp" matches "tcp" and "tcp_debug")
    pub source_type: Option<String>,
}

impl MatchCondition {
    /// Check if this condition matches a given source
    pub fn matches(&self, source_name: &str, source_type: &str) -> bool {
        // If source is specified, it must match exactly
        if let Some(ref expected_source) = self.source
            && source_name != expected_source
        {
            return false;
        }

        // If source_type is specified, source type must match
        if let Some(ref expected_type) = self.source_type
            && source_type != expected_type
        {
            return false;
        }

        // If no conditions specified, don't match anything
        // (prevents accidental catch-all rules)
        self.source.is_some() || self.source_type.is_some()
    }

    /// Check if any condition is specified
    pub fn is_empty(&self) -> bool {
        self.source.is_none() && self.source_type.is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = RoutingConfig::default();
        assert!(config.default.is_empty());
        assert!(config.rules.is_empty());
        assert!(!config.has_rules());
        assert!(!config.has_default());
    }

    #[test]
    fn test_deserialize_empty() {
        let config: RoutingConfig = toml::from_str("").unwrap();
        assert!(config.default.is_empty());
        assert!(config.rules.is_empty());
    }

    #[test]
    fn test_deserialize_default_only() {
        let toml = r#"
default = ["stdout", "disk_plaintext"]
"#;
        let config: RoutingConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.default, vec!["stdout", "disk_plaintext"]);
        assert!(config.rules.is_empty());
        assert!(config.has_default());
        assert!(!config.has_rules());
    }

    #[test]
    fn test_deserialize_with_rules() {
        let toml = r#"
default = ["stdout"]

[[rules]]
match = { source = "tcp" }
sinks = ["clickhouse", "disk_plaintext"]

[[rules]]
match = { source = "tcp_debug" }
sinks = ["stdout"]
"#;
        let config: RoutingConfig = toml::from_str(toml).unwrap();

        assert_eq!(config.default, vec!["stdout"]);
        assert_eq!(config.rules.len(), 2);
        assert!(config.has_default());
        assert!(config.has_rules());

        // First rule
        assert_eq!(
            config.rules[0].match_condition.source,
            Some("tcp".to_string())
        );
        assert_eq!(config.rules[0].sinks, vec!["clickhouse", "disk_plaintext"]);

        // Second rule
        assert_eq!(
            config.rules[1].match_condition.source,
            Some("tcp_debug".to_string())
        );
        assert_eq!(config.rules[1].sinks, vec!["stdout"]);
    }

    #[test]
    fn test_deserialize_source_type_match() {
        let toml = r#"
[[rules]]
match = { source_type = "syslog" }
sinks = ["disk_plaintext"]
"#;
        let config: RoutingConfig = toml::from_str(toml).unwrap();

        assert_eq!(config.rules.len(), 1);
        assert_eq!(
            config.rules[0].match_condition.source_type,
            Some("syslog".to_string())
        );
        assert!(config.rules[0].match_condition.source.is_none());
    }

    #[test]
    fn test_deserialize_combined_match() {
        let toml = r#"
[[rules]]
match = { source = "syslog_tcp", source_type = "syslog" }
sinks = ["clickhouse"]
"#;
        let config: RoutingConfig = toml::from_str(toml).unwrap();

        let condition = &config.rules[0].match_condition;
        assert_eq!(condition.source, Some("syslog_tcp".to_string()));
        assert_eq!(condition.source_type, Some("syslog".to_string()));
    }

    #[test]
    fn test_referenced_sinks() {
        let toml = r#"
default = ["stdout"]

[[rules]]
match = { source = "tcp" }
sinks = ["clickhouse", "disk_plaintext"]

[[rules]]
match = { source = "tcp_debug" }
sinks = ["stdout", "null"]
"#;
        let config: RoutingConfig = toml::from_str(toml).unwrap();

        let sinks = config.referenced_sinks();
        assert!(sinks.contains(&"stdout"));
        assert!(sinks.contains(&"clickhouse"));
        assert!(sinks.contains(&"disk_plaintext"));
        assert!(sinks.contains(&"null"));
    }

    #[test]
    fn test_referenced_sources() {
        let toml = r#"
[[rules]]
match = { source = "tcp" }
sinks = ["clickhouse"]

[[rules]]
match = { source_type = "syslog" }
sinks = ["disk_plaintext"]

[[rules]]
match = { source = "tcp_debug" }
sinks = ["stdout"]
"#;
        let config: RoutingConfig = toml::from_str(toml).unwrap();

        let sources = config.referenced_sources();
        assert!(sources.contains(&"tcp"));
        assert!(sources.contains(&"tcp_debug"));
        assert!(sources.contains(&"syslog"));
    }

    #[test]
    fn test_match_condition_source_exact() {
        let condition = MatchCondition {
            source: Some("tcp".to_string()),
            source_type: None,
        };

        assert!(condition.matches("tcp", "tcp"));
        assert!(!condition.matches("tcp_debug", "tcp"));
        assert!(!condition.matches("syslog_tcp", "syslog"));
    }

    #[test]
    fn test_match_condition_source_type() {
        let condition = MatchCondition {
            source: None,
            source_type: Some("syslog".to_string()),
        };

        assert!(condition.matches("syslog_tcp", "syslog"));
        assert!(condition.matches("syslog_udp", "syslog"));
        assert!(!condition.matches("tcp", "tcp"));
    }

    #[test]
    fn test_match_condition_combined() {
        let condition = MatchCondition {
            source: Some("syslog_tcp".to_string()),
            source_type: Some("syslog".to_string()),
        };

        // Both must match
        assert!(condition.matches("syslog_tcp", "syslog"));
        assert!(!condition.matches("syslog_udp", "syslog")); // source doesn't match
        assert!(!condition.matches("syslog_tcp", "tcp")); // type doesn't match
    }

    #[test]
    fn test_match_condition_empty() {
        let condition = MatchCondition::default();

        assert!(condition.is_empty());
        // Empty condition should not match anything
        assert!(!condition.matches("tcp", "tcp"));
        assert!(!condition.matches("syslog_tcp", "syslog"));
    }

    #[test]
    fn test_match_condition_is_empty() {
        let empty = MatchCondition::default();
        assert!(empty.is_empty());

        let with_source = MatchCondition {
            source: Some("tcp".to_string()),
            source_type: None,
        };
        assert!(!with_source.is_empty());

        let with_type = MatchCondition {
            source: None,
            source_type: Some("syslog".to_string()),
        };
        assert!(!with_type.is_empty());
    }

    // ========================================================================
    // Transformer Tests
    // ========================================================================

    #[test]
    fn test_deserialize_rule_without_transformers() {
        let toml = r#"
[[rules]]
match = { source = "tcp" }
sinks = ["clickhouse"]
"#;
        let config: RoutingConfig = toml::from_str(toml).unwrap();
        assert!(config.rules[0].transformers.is_empty());
        assert!(!config.rules[0].has_transformers());
    }

    #[test]
    fn test_deserialize_rule_with_single_transformer() {
        let toml = r#"
[[rules]]
match = { source_type = "syslog" }
sinks = ["clickhouse"]

[[rules.transformers]]
type = "pattern_matcher"
similarity_threshold = 0.5
"#;
        let config: RoutingConfig = toml::from_str(toml).unwrap();
        assert!(config.rules[0].has_transformers());
        assert_eq!(config.rules[0].transformers.len(), 1);
        assert_eq!(
            config.rules[0].transformers[0].transformer_type,
            "pattern_matcher"
        );
        assert_eq!(
            config.rules[0].transformers[0].get_float("similarity_threshold"),
            Some(0.5)
        );
    }

    #[test]
    fn test_deserialize_rule_with_multiple_transformers() {
        let toml = r#"
[[rules]]
match = { source_type = "syslog" }
sinks = ["clickhouse"]

[[rules.transformers]]
type = "pattern_matcher"
similarity_threshold = 0.5

[[rules.transformers]]
type = "noop"
"#;
        let config: RoutingConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.rules[0].transformers.len(), 2);
        assert_eq!(
            config.rules[0].transformers[0].transformer_type,
            "pattern_matcher"
        );
        assert_eq!(config.rules[0].transformers[1].transformer_type, "noop");
    }

    #[test]
    fn test_deserialize_transformer_with_all_options() {
        let toml = r#"
[[rules]]
match = { source = "tcp" }
sinks = ["disk_plaintext"]

[[rules.transformers]]
type = "pattern_matcher"
enabled = true
similarity_threshold = 0.6
cache_size = 50000
max_child_nodes = 75
persistence_enabled = true
persistence_file = "/var/lib/tell/patterns.json"
"#;
        let config: RoutingConfig = toml::from_str(toml).unwrap();
        let t = &config.rules[0].transformers[0];

        assert_eq!(t.transformer_type, "pattern_matcher");
        assert!(t.enabled);
        assert_eq!(t.get_float("similarity_threshold"), Some(0.6));
        assert_eq!(t.get_int("cache_size"), Some(50000));
        assert_eq!(t.get_int("max_child_nodes"), Some(75));
        assert_eq!(t.get_bool("persistence_enabled"), Some(true));
        assert_eq!(
            t.get_str("persistence_file"),
            Some("/var/lib/tell/patterns.json")
        );
    }

    #[test]
    fn test_deserialize_disabled_transformer() {
        let toml = r#"
[[rules]]
match = { source = "tcp" }
sinks = ["stdout"]

[[rules.transformers]]
type = "pattern_matcher"
enabled = false
"#;
        let config: RoutingConfig = toml::from_str(toml).unwrap();
        assert!(!config.rules[0].transformers[0].enabled);
    }

    #[test]
    fn test_enabled_transformers_filter() {
        let toml = r#"
[[rules]]
match = { source = "tcp" }
sinks = ["stdout"]

[[rules.transformers]]
type = "pattern_matcher"
enabled = true

[[rules.transformers]]
type = "noop"
enabled = false

[[rules.transformers]]
type = "another"
enabled = true
"#;
        let config: RoutingConfig = toml::from_str(toml).unwrap();
        let enabled: Vec<_> = config.rules[0].enabled_transformers().collect();
        assert_eq!(enabled.len(), 2);
        assert_eq!(enabled[0].transformer_type, "pattern_matcher");
        assert_eq!(enabled[1].transformer_type, "another");
    }

    #[test]
    fn test_multiple_rules_with_different_transformers() {
        let toml = r#"
default = ["stdout"]

[[rules]]
match = { source_type = "syslog" }
sinks = ["clickhouse"]

[[rules.transformers]]
type = "pattern_matcher"

[[rules]]
match = { source = "tcp" }
sinks = ["disk_binary"]
# No transformers - raw passthrough
"#;
        let config: RoutingConfig = toml::from_str(toml).unwrap();

        // First rule has transformer
        assert!(config.rules[0].has_transformers());
        assert_eq!(config.rules[0].transformers.len(), 1);

        // Second rule has no transformers
        assert!(!config.rules[1].has_transformers());
        assert!(config.rules[1].transformers.is_empty());
    }
}
