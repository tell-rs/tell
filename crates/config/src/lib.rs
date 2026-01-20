//! Tell Configuration
//!
//! TOML-based configuration loading with sensible defaults.
//! Minimal config should just work - only specify what you need to change.
//!
//! # Parsing
//!
//! Use the `FromStr` trait to parse configuration:
//!
//! ```
//! use tell_config::Config;
//! use std::str::FromStr;
//!
//! let config = Config::from_str("[sinks.stdout]\ntype = \"stdout\"").unwrap();
//! ```
//!
//! # Example Minimal Config
//!
//! ```toml
//! [sources.tcp]
//! port = 50000
//!
//! [sinks.stdout]
//!
//! [sinks.disk_plaintext]
//! path = "logs/"
//! ```
//!
//! # Example Full Config
//!
//! See `configs/example.toml` for all available options.

mod api;
mod auth;
mod connectors;
mod error;
mod global;
mod logging;
mod metrics;
mod routing;
mod sinks;
mod sources;
mod transformers;
mod validation;

use std::fs;
use std::path::Path;
use std::str::FromStr;

pub use api::ApiConfig;
pub use auth::{AuthConfig, LocalAuthConfig, WorkOsConfig};
pub use connectors::{ConnectorsConfig, RawConnectorConfig};
pub use error::{ConfigError, Result};
pub use global::GlobalConfig;
pub use logging::LogConfig;
pub use metrics::{MetricsConfig, MetricsFormat};
pub use routing::{MatchCondition, RoutingConfig, RoutingRule};
pub use transformers::{
    is_known_transformer_type, TransformerInstanceConfig, KNOWN_TRANSFORMER_TYPES,
};
pub use sinks::{
    ArrowIpcSinkConfig, ClickHouseNativeSinkConfig, ClickHouseSinkConfig, Compression,
    DiskBinarySinkConfig, DiskPlaintextSinkConfig, ForwarderSinkConfig, NullSinkConfig,
    ParquetCompression, ParquetDataFormat, ParquetSinkConfig, RotationInterval, SinkConfig,
    SinkMetricsConfig, SinksConfig, StdoutSinkConfig,
};
pub use sources::{
    SourcesConfig, SyslogTcpSourceConfig, SyslogUdpSourceConfig, TcpDebugSourceConfig,
    TcpSourceConfig,
};

use serde::Deserialize;

/// Main configuration structure
///
/// All sections are optional with sensible defaults.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct Config {
    /// Global settings (buffer sizes, worker counts, etc.)
    pub global: GlobalConfig,

    /// Logging configuration
    pub log: LogConfig,

    /// Metrics reporting configuration
    pub metrics: MetricsConfig,

    /// Authentication configuration
    pub auth: AuthConfig,

    /// API client configuration (for CLI commands)
    pub api: ApiConfig,

    /// Data sources (TCP, Syslog, etc.)
    pub sources: SourcesConfig,

    /// Data sinks (ClickHouse, disk, etc.)
    pub sinks: SinksConfig,

    /// Routing rules (source → sink mapping)
    pub routing: RoutingConfig,

    /// Pull-based connectors (GitHub, Stripe, etc.)
    pub connectors: ConnectorsConfig,
}

impl Config {
    /// Load configuration from a TOML file
    ///
    /// # Errors
    ///
    /// Returns error if file cannot be read or contains invalid TOML.
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        let contents = fs::read_to_string(path).map_err(|e| ConfigError::IoError {
            path: path.display().to_string(),
            source: e,
        })?;

        Self::from_str(&contents)
    }

    /// Parse configuration from a TOML string
    ///
    /// Prefer using the `FromStr` trait implementation.
    fn parse(s: &str) -> Result<Self> {
        let config: Config = toml::from_str(s).map_err(ConfigError::ParseError)?;
        config.validate()?;
        Ok(config)
    }

    /// Validate the configuration
    ///
    /// Checks for:
    /// - Referenced sinks exist in routing rules
    /// - Required fields are present for enabled components
    /// - Port conflicts
    fn validate(&self) -> Result<()> {
        validation::validate_config(self)
    }

    /// Get list of enabled source names
    pub fn enabled_sources(&self) -> Vec<&str> {
        let mut sources = Vec::new();

        for (i, tcp) in self.sources.tcp.iter().enumerate() {
            if tcp.enabled {
                // Generate name based on index or use port as identifier
                sources.push(if i == 0 { "tcp" } else { "tcp_alt" });
            }
        }

        if let Some(ref debug) = self.sources.tcp_debug
            && debug.enabled
        {
            sources.push("tcp_debug");
        }

        if let Some(ref syslog) = self.sources.syslog_tcp
            && syslog.enabled
        {
            sources.push("syslog_tcp");
        }

        if let Some(ref syslog) = self.sources.syslog_udp
            && syslog.enabled
        {
            sources.push("syslog_udp");
        }

        sources
    }

    /// Get list of enabled sink names
    pub fn enabled_sinks(&self) -> Vec<String> {
        self.sinks
            .iter()
            .filter(|(_, sink)| sink.is_enabled())
            .map(|(name, _)| name.clone())
            .collect()
    }
}

impl FromStr for Config {
    type Err = ConfigError;

    fn from_str(s: &str) -> Result<Self> {
        Self::parse(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_empty_config_uses_defaults() {
        let config = Config::from_str("").unwrap();
        assert!(config.global.buffer_size > 0);
        assert!(config.sources.tcp.is_empty());
    }

    #[test]
    fn test_minimal_config() {
        let toml = r#"
[[sources.tcp]]
port = 50000

[sinks.stdout]
type = "stdout"

[routing]
default = ["stdout"]
"#;
        let config = Config::from_str(toml).unwrap();
        assert_eq!(config.sources.tcp.len(), 1);
        assert_eq!(config.sources.tcp[0].port, 50000);
        assert!(config.sources.tcp[0].enabled); // enabled by default when specified
    }

    #[test]
    fn test_full_config_parse() {
        let toml = r#"
[global]
buffer_size = 524288
api_keys_file = "keys.conf"

[log]
level = "debug"

[metrics]
enabled = true
interval = "5s"

[[sources.tcp]]
port = 50000
forwarding_mode = false

[[sources.tcp]]
port = 8081
forwarding_mode = true

[sources.tcp_debug]
port = 50001

[sinks.stdout]
type = "stdout"
enabled = true

[sinks.disk_plaintext]
type = "disk_plaintext"
path = "logs/"
rotation = "daily"

[sinks.clickhouse]
type = "clickhouse"
host = "localhost:9000"
enabled = false

[routing]
default = ["stdout"]

[[routing.rules]]
match = { source = "tcp" }
sinks = ["disk_plaintext", "clickhouse"]
"#;
        let config = Config::from_str(toml).unwrap();

        assert_eq!(config.global.buffer_size, 524288);
        assert_eq!(config.sources.tcp.len(), 2);
        assert!(config.sources.tcp_debug.is_some());
        assert!(config.sinks.get("stdout").is_some());
        assert!(config.sinks.get("disk_plaintext").is_some());
        assert_eq!(config.routing.default, vec!["stdout"]);
        assert_eq!(config.routing.rules.len(), 1);
    }

    #[test]
    fn test_invalid_toml() {
        let result = Config::from_str("invalid { toml");
        assert!(result.is_err());
    }
}
