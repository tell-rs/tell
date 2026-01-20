//! Connector configuration types
//!
//! Generic configuration for pull-based connectors (GitHub, Stripe, etc.)
//! Connector-specific config parsing is handled by each connector crate.
//!
//! # Example
//!
//! ```toml
//! [connectors.github_main]
//! type = "github"
//! entities = ["user/tell", "user/director"]
//! schedule = "0 */6 * * *"
//! metrics = ["stars", "forks"]
//!
//! [connectors.stripe_prod]
//! type = "stripe"
//! schedule = "0 * * * *"
//! api_key = "${STRIPE_KEY}"
//! ```

use serde::Deserialize;
use std::collections::HashMap;

/// Container for all connector configurations
///
/// Connectors are stored as a map of name -> raw config.
/// Each connector type parses its own config from the raw TOML.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct ConnectorsConfig {
    /// Named connector instances
    #[serde(flatten)]
    connectors: HashMap<String, RawConnectorConfig>,
}

impl ConnectorsConfig {
    /// Get a connector config by name
    pub fn get(&self, name: &str) -> Option<&RawConnectorConfig> {
        self.connectors.get(name)
    }

    /// Check if a connector exists
    pub fn contains(&self, name: &str) -> bool {
        self.connectors.contains_key(name)
    }

    /// Iterate over all connectors
    pub fn iter(&self) -> impl Iterator<Item = (&String, &RawConnectorConfig)> {
        self.connectors.iter()
    }

    /// Get the number of configured connectors
    pub fn len(&self) -> usize {
        self.connectors.len()
    }

    /// Check if no connectors are configured
    pub fn is_empty(&self) -> bool {
        self.connectors.is_empty()
    }

    /// Get all connector names
    pub fn names(&self) -> impl Iterator<Item = &String> {
        self.connectors.keys()
    }

    /// Get connectors filtered by type
    pub fn by_type(
        &self,
        connector_type: &str,
    ) -> impl Iterator<Item = (&String, &RawConnectorConfig)> {
        self.connectors
            .iter()
            .filter(move |(_, c)| c.connector_type == connector_type)
    }
}

/// Raw connector configuration
///
/// Contains the connector type and raw config values.
/// Each connector implementation parses its specific config from `config`.
#[derive(Debug, Clone, Deserialize)]
pub struct RawConnectorConfig {
    /// Connector type (e.g., "github", "stripe")
    #[serde(rename = "type")]
    pub connector_type: String,

    /// Whether this connector is enabled
    /// Default: true
    #[serde(default = "default_enabled")]
    pub enabled: bool,

    /// Cron schedule expression (e.g., "0 */6 * * *")
    /// If not set, connector only runs via CLI
    pub schedule: Option<String>,

    /// Raw connector-specific configuration
    /// Parsed by the connector implementation
    #[serde(flatten)]
    pub config: toml::Value,
}

fn default_enabled() -> bool {
    true
}

impl RawConnectorConfig {
    /// Check if this connector is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Check if this connector has a schedule
    pub fn has_schedule(&self) -> bool {
        self.schedule.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_connectors() {
        let config: ConnectorsConfig = toml::from_str("").unwrap();
        assert!(config.is_empty());
        assert_eq!(config.len(), 0);
    }

    #[test]
    fn test_single_connector() {
        let toml = r#"
[github_main]
type = "github"
entities = ["user/tell"]
schedule = "0 */6 * * *"
"#;
        let config: ConnectorsConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.len(), 1);
        assert!(config.contains("github_main"));

        let connector = config.get("github_main").unwrap();
        assert_eq!(connector.connector_type, "github");
        assert!(connector.is_enabled());
        assert_eq!(connector.schedule, Some("0 */6 * * *".into()));
    }

    #[test]
    fn test_multiple_connectors() {
        let toml = r#"
[github_main]
type = "github"
entities = ["user/tell", "user/director"]
schedule = "0 */6 * * *"

[github_oss]
type = "github"
entities = ["rust-lang/rust"]
schedule = "0 0 * * *"

[stripe_prod]
type = "stripe"
schedule = "0 * * * *"
api_key = "sk_live_xxx"
"#;
        let config: ConnectorsConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.len(), 3);

        // Filter by type
        let github_connectors: Vec<_> = config.by_type("github").collect();
        assert_eq!(github_connectors.len(), 2);

        let stripe_connectors: Vec<_> = config.by_type("stripe").collect();
        assert_eq!(stripe_connectors.len(), 1);
    }

    #[test]
    fn test_disabled_connector() {
        let toml = r#"
[github_disabled]
type = "github"
enabled = false
entities = ["user/test"]
"#;
        let config: ConnectorsConfig = toml::from_str(toml).unwrap();
        let connector = config.get("github_disabled").unwrap();
        assert!(!connector.is_enabled());
    }

    #[test]
    fn test_connector_without_schedule() {
        let toml = r#"
[github_adhoc]
type = "github"
entities = ["user/test"]
"#;
        let config: ConnectorsConfig = toml::from_str(toml).unwrap();
        let connector = config.get("github_adhoc").unwrap();
        assert!(!connector.has_schedule());
    }

    #[test]
    fn test_connector_with_metrics_filter() {
        let toml = r#"
[github_filtered]
type = "github"
entities = ["user/tell"]
metrics = ["stars", "forks"]
schedule = "0 * * * *"
"#;
        let config: ConnectorsConfig = toml::from_str(toml).unwrap();
        let connector = config.get("github_filtered").unwrap();

        // Raw config should contain the metrics array
        let metrics = connector.config.get("metrics").unwrap();
        assert!(metrics.is_array());
    }

    #[test]
    fn test_connector_names() {
        let toml = r#"
[a]
type = "github"
entities = []

[b]
type = "stripe"

[c]
type = "github"
entities = []
"#;
        let config: ConnectorsConfig = toml::from_str(toml).unwrap();
        let names: Vec<_> = config.names().collect();
        assert_eq!(names.len(), 3);
    }
}
