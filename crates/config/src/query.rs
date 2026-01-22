//! Query backend configuration
//!
//! Configuration for the analytics query backend (ClickHouse or Polars).

use std::path::PathBuf;

use serde::Deserialize;

/// Query backend type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum QueryBackend {
    /// ClickHouse backend (production)
    Clickhouse,
    /// Polars backend (local/edge, reads parquet files)
    #[default]
    Polars,
}

/// Query configuration
///
/// Configures the backend used for analytics queries. If not specified,
/// the backend is auto-detected:
/// - If a ClickHouse sink exists, use ClickHouse
/// - Otherwise, use Polars with default data directory
///
/// # Example
///
/// ```toml
/// # Explicit ClickHouse
/// [query]
/// backend = "clickhouse"
/// clickhouse_url = "http://localhost:8123"
/// clickhouse_database = "analytics"
///
/// # Or reference an existing sink
/// [query]
/// backend = "clickhouse"
/// clickhouse_sink = "clickhouse_prod"  # Uses [sinks.clickhouse_prod] config
///
/// # Or Polars for local/edge
/// [query]
/// backend = "polars"
/// data_dir = "./data"
/// ```
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct QueryConfig {
    /// Query backend type
    /// Default: auto-detect (clickhouse if sink exists, else polars)
    pub backend: Option<QueryBackend>,

    /// ClickHouse HTTP URL (when backend = clickhouse)
    /// Default: "http://localhost:8123"
    pub clickhouse_url: Option<String>,

    /// ClickHouse database name
    /// Default: "default"
    pub clickhouse_database: Option<String>,

    /// ClickHouse username
    /// Default: "default"
    pub clickhouse_username: Option<String>,

    /// ClickHouse password
    /// Default: ""
    pub clickhouse_password: Option<String>,

    /// Reference to a ClickHouse sink config to use
    /// If set, uses that sink's connection settings
    pub clickhouse_sink: Option<String>,

    /// Data directory for Polars backend (parquet files)
    /// Default: "./data"
    pub data_dir: Option<PathBuf>,
}

impl QueryConfig {
    /// Get the effective backend, with auto-detection
    pub fn effective_backend(&self, has_clickhouse_sink: bool) -> QueryBackend {
        self.backend.unwrap_or(if has_clickhouse_sink {
            QueryBackend::Clickhouse
        } else {
            QueryBackend::Polars
        })
    }

    /// Get the ClickHouse URL
    pub fn clickhouse_url(&self) -> String {
        self.clickhouse_url
            .clone()
            .unwrap_or_else(|| "http://localhost:8123".to_string())
    }

    /// Get the ClickHouse database
    pub fn clickhouse_database(&self) -> String {
        self.clickhouse_database
            .clone()
            .unwrap_or_else(|| "default".to_string())
    }

    /// Get the data directory for Polars
    pub fn data_dir(&self) -> PathBuf {
        self.data_dir
            .clone()
            .unwrap_or_else(|| PathBuf::from("./data"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = QueryConfig::default();
        assert!(config.backend.is_none());
        assert!(config.clickhouse_url.is_none());
        assert!(config.data_dir.is_none());
    }

    #[test]
    fn test_auto_detect_polars() {
        let config = QueryConfig::default();
        assert_eq!(config.effective_backend(false), QueryBackend::Polars);
    }

    #[test]
    fn test_auto_detect_clickhouse() {
        let config = QueryConfig::default();
        assert_eq!(config.effective_backend(true), QueryBackend::Clickhouse);
    }

    #[test]
    fn test_explicit_backend() {
        let toml = r#"
backend = "clickhouse"
"#;
        let config: QueryConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.backend, Some(QueryBackend::Clickhouse));
        // Explicit takes precedence
        assert_eq!(config.effective_backend(false), QueryBackend::Clickhouse);
    }

    #[test]
    fn test_clickhouse_config() {
        let toml = r#"
backend = "clickhouse"
clickhouse_url = "http://ch.example.com:8123"
clickhouse_database = "analytics"
"#;
        let config: QueryConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.clickhouse_url(), "http://ch.example.com:8123");
        assert_eq!(config.clickhouse_database(), "analytics");
    }

    #[test]
    fn test_polars_config() {
        let toml = r#"
backend = "polars"
data_dir = "/var/lib/tell/data"
"#;
        let config: QueryConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.data_dir(), PathBuf::from("/var/lib/tell/data"));
    }

    #[test]
    fn test_sink_reference() {
        let toml = r#"
backend = "clickhouse"
clickhouse_sink = "clickhouse_prod"
"#;
        let config: QueryConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.clickhouse_sink, Some("clickhouse_prod".to_string()));
    }
}
