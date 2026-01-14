//! Connector configuration types
//!
//! Each connector defines its own config struct here.
//! Configs are parsed from raw TOML values provided by the config crate.

use crate::error::ConnectorError;
use crate::resilience::ResilienceConfig;
use serde::Deserialize;

/// GitHub connector configuration
///
/// Pulls repository metrics from the GitHub API.
///
/// # Example
///
/// ```toml
/// [connectors.github_main]
/// type = "github"
/// entities = ["user/repo", "org/another-repo"]
/// schedule = "0 0 */6 * * *"  # every 6 hours (6-field cron with seconds)
/// token = "ghp_xxx"           # optional, increases rate limits
/// metrics = ["stars", "forks", "open_issues"]  # optional, default: all
/// workspace_id = 1            # optional, default: 1
/// timeout_secs = 30           # optional, request timeout
/// max_retries = 3             # optional, retry attempts
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct GitHubConnectorConfig {
    /// GitHub API token (optional for public repos)
    pub token: Option<String>,

    /// API base URL (default: https://api.github.com)
    pub api_url: String,

    /// Entities to pull (owner/repo format)
    pub entities: Vec<String>,

    /// Metrics to include (default: all)
    /// Available: stars, forks, watchers, open_issues, size, default_branch
    pub metrics: Option<Vec<String>>,

    /// Workspace ID for routing (default: 1)
    pub workspace_id: u32,

    /// Request timeout in seconds (default: 30)
    pub timeout_secs: u64,

    /// Maximum retry attempts for transient failures (default: 3)
    pub max_retries: u32,
}

impl Default for GitHubConnectorConfig {
    fn default() -> Self {
        Self {
            token: None,
            api_url: "https://api.github.com".to_string(),
            entities: Vec::new(),
            metrics: None, // None = all metrics
            workspace_id: 1,
            timeout_secs: 30,
            max_retries: 3,
        }
    }
}

impl GitHubConnectorConfig {
    /// Parse config from raw TOML value
    pub fn from_toml(value: &toml::Value) -> Result<Self, ConnectorError> {
        let config: GitHubConnectorConfig = value
            .clone()
            .try_into()
            .map_err(|e: toml::de::Error| {
                ConnectorError::ConfigError(format!("Invalid GitHub config: {}", e))
            })?;
        Ok(config)
    }

    /// Check if a metric should be included
    pub fn include_metric(&self, metric: &str) -> bool {
        match &self.metrics {
            None => true,
            Some(metrics) => metrics.iter().any(|m| m == metric),
        }
    }

    /// Build resilience config from these settings
    pub fn resilience_config(&self) -> ResilienceConfig {
        ResilienceConfig {
            timeout_secs: self.timeout_secs,
            max_retries: self.max_retries,
            ..Default::default()
        }
    }
}

/// Shopify connector configuration
///
/// Pulls store metrics from the Shopify Admin API.
///
/// # Example
///
/// ```toml
/// [connectors.shopify_main]
/// type = "shopify"
/// store = "mystore.myshopify.com"
/// access_token = "shpat_xxx"
/// entities = ["orders", "products", "customers"]
/// schedule = "0 0 */6 * * *"
/// metrics = ["orders_total", "revenue_30d"]  # optional
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ShopifyConnectorConfig {
    /// Store domain (e.g., mystore.myshopify.com)
    pub store: String,

    /// Shopify Admin API access token
    pub access_token: Option<String>,

    /// API version (default: 2024-01)
    pub api_version: String,

    /// Entities to pull (orders, products, customers, inventory)
    pub entities: Vec<String>,

    /// Metrics to include (default: all)
    pub metrics: Option<Vec<String>>,

    /// Workspace ID for routing (default: 1)
    pub workspace_id: u32,

    /// Request timeout in seconds (default: 30)
    pub timeout_secs: u64,

    /// Maximum retry attempts (default: 3)
    pub max_retries: u32,
}

impl Default for ShopifyConnectorConfig {
    fn default() -> Self {
        Self {
            store: String::new(),
            access_token: None,
            api_version: "2024-01".to_string(),
            entities: Vec::new(),
            metrics: None,
            workspace_id: 1,
            timeout_secs: 30,
            max_retries: 3,
        }
    }
}

impl ShopifyConnectorConfig {
    /// Parse config from raw TOML value
    pub fn from_toml(value: &toml::Value) -> Result<Self, ConnectorError> {
        let config: ShopifyConnectorConfig = value
            .clone()
            .try_into()
            .map_err(|e: toml::de::Error| {
                ConnectorError::ConfigError(format!("Invalid Shopify config: {}", e))
            })?;

        if config.store.is_empty() {
            return Err(ConnectorError::ConfigError(
                "Shopify store domain is required".to_string(),
            ));
        }

        Ok(config)
    }

    /// Check if a metric should be included
    pub fn include_metric(&self, metric: &str) -> bool {
        match &self.metrics {
            None => true,
            Some(metrics) => metrics.iter().any(|m| m == metric),
        }
    }

    /// Build resilience config from these settings
    pub fn resilience_config(&self) -> ResilienceConfig {
        ResilienceConfig {
            timeout_secs: self.timeout_secs,
            max_retries: self.max_retries,
            ..Default::default()
        }
    }
}

/// All available Shopify metrics
pub const SHOPIFY_METRICS: &[&str] = &[
    // Order metrics
    "orders_total",
    "orders_today",
    "orders_30d",
    "revenue_today",
    "revenue_30d",
    "average_order_value",
    // Product metrics
    "products_total",
    "products_active",
    // Customer metrics
    "customers_total",
    "customers_30d",
    // Operations metrics
    "unfulfilled_orders",
    "refunds_30d",
    "refund_amount_30d",
    "abandoned_checkouts",
    "inventory_items",
    "low_stock_count",
];

/// All available GitHub metrics
pub const GITHUB_METRICS: &[&str] = &[
    // Basic (1 API call)
    "stars",
    "forks",
    "watchers",
    "open_issues",
    "size",
    "default_branch",
    // Extended: commits (+1 API call)
    "commits",
    "last_commit_at",
    // Extended: issues (+2 API calls)
    "issues_open",
    "issues_closed",
    // Extended: PRs (+3 API calls)
    "prs_open",
    "prs_closed",
    "prs_merged",
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_github_config_defaults() {
        let config = GitHubConnectorConfig::default();
        assert!(config.token.is_none());
        assert_eq!(config.api_url, "https://api.github.com");
        assert!(config.entities.is_empty());
        assert!(config.metrics.is_none());
    }

    #[test]
    fn test_github_config_from_toml() {
        let toml_str = r#"
entities = ["user/repo1", "user/repo2"]
token = "ghp_test"
metrics = ["stars", "forks"]
"#;
        let value: toml::Value = toml::from_str(toml_str).unwrap();
        let config = GitHubConnectorConfig::from_toml(&value).unwrap();

        assert_eq!(config.entities, vec!["user/repo1", "user/repo2"]);
        assert_eq!(config.token, Some("ghp_test".into()));
        assert_eq!(config.metrics, Some(vec!["stars".into(), "forks".into()]));
    }

    #[test]
    fn test_github_config_include_metric_all() {
        let config = GitHubConnectorConfig::default();
        assert!(config.include_metric("stars"));
        assert!(config.include_metric("forks"));
        assert!(config.include_metric("anything"));
    }

    #[test]
    fn test_github_config_include_metric_filtered() {
        let config = GitHubConnectorConfig {
            metrics: Some(vec!["stars".into(), "forks".into()]),
            ..Default::default()
        };
        assert!(config.include_metric("stars"));
        assert!(config.include_metric("forks"));
        assert!(!config.include_metric("watchers"));
        assert!(!config.include_metric("open_issues"));
    }

    #[test]
    fn test_github_config_custom_api_url() {
        let toml_str = r#"
entities = ["org/repo"]
api_url = "https://github.example.com/api/v3"
"#;
        let value: toml::Value = toml::from_str(toml_str).unwrap();
        let config = GitHubConnectorConfig::from_toml(&value).unwrap();

        assert_eq!(config.api_url, "https://github.example.com/api/v3");
    }
}
