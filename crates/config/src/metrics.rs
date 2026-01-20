//! Metrics reporting configuration
//!
//! Controls how Tell reports internal metrics.
//!
//! # Defaults
//!
//! Metrics are enabled by default with sensible settings:
//! - `enabled`: true
//! - `interval`: 60s
//! - `format`: human
//! - All include flags: true
//!
//! This means a minimal config gets full observability out of the box.

use serde::Deserialize;
use std::time::Duration;

/// Metrics output format
#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum MetricsFormat {
    /// Human-readable output (default)
    #[default]
    Human,
    /// JSON structured output
    Json,
}

/// Metrics configuration
///
/// # Example
///
/// ```toml
/// [metrics]
/// # All fields optional - defaults to enabled with human format
/// enabled = true
/// interval = "60s"
/// format = "human"
/// include_pipeline = true
/// include_sources = true
/// include_sinks = true
/// include_transforms = true
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct MetricsConfig {
    /// Enable metrics reporting
    /// Default: true
    pub enabled: bool,

    /// Reporting interval
    /// Default: 1h
    #[serde(with = "humantime_serde")]
    pub interval: Duration,

    /// Output format (human, json)
    /// Default: human
    pub format: MetricsFormat,

    /// Include pipeline metrics (batches routed, dropped, etc.)
    /// Default: true
    pub include_pipeline: bool,

    /// Include source metrics (connections, bytes received, etc.)
    /// Default: true
    pub include_sources: bool,

    /// Include sink metrics (writes, bytes written, etc.)
    /// Default: true
    pub include_sinks: bool,

    /// Include transformer metrics (batches processed, timing, etc.)
    /// Default: true
    pub include_transforms: bool,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval: Duration::from_secs(3600), // 1 hour
            format: MetricsFormat::Human,
            include_pipeline: true,
            include_sources: true,
            include_sinks: true,
            include_transforms: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = MetricsConfig::default();
        assert!(config.enabled);
        assert_eq!(config.interval, Duration::from_secs(3600)); // 1 hour
        assert_eq!(config.format, MetricsFormat::Human);
        assert!(config.include_pipeline);
        assert!(config.include_sources);
        assert!(config.include_sinks);
        assert!(config.include_transforms);
    }

    #[test]
    fn test_deserialize_empty() {
        let config: MetricsConfig = toml::from_str("").unwrap();
        assert!(config.enabled);
        assert_eq!(config.interval, Duration::from_secs(3600)); // 1 hour
    }

    #[test]
    fn test_deserialize_disabled() {
        let toml = r#"
enabled = false
"#;
        let config: MetricsConfig = toml::from_str(toml).unwrap();
        assert!(!config.enabled);
    }

    #[test]
    fn test_deserialize_full() {
        let toml = r#"
enabled = true
interval = "5s"
format = "json"
include_pipeline = true
include_sources = false
include_sinks = true
include_transforms = false
"#;
        let config: MetricsConfig = toml::from_str(toml).unwrap();
        assert!(config.enabled);
        assert_eq!(config.interval, Duration::from_secs(5));
        assert_eq!(config.format, MetricsFormat::Json);
        assert!(config.include_pipeline);
        assert!(!config.include_sources);
        assert!(config.include_sinks);
        assert!(!config.include_transforms);
    }

    #[test]
    fn test_deserialize_interval_variants() {
        for (s, expected) in [
            ("100ms", Duration::from_millis(100)),
            ("1s", Duration::from_secs(1)),
            ("30s", Duration::from_secs(30)),
            ("1m", Duration::from_secs(60)),
            ("5m", Duration::from_secs(300)),
        ] {
            let toml = format!("interval = \"{}\"", s);
            let config: MetricsConfig = toml::from_str(&toml).unwrap();
            assert_eq!(config.interval, expected, "Failed for {}", s);
        }
    }

    #[test]
    fn test_format_variants() {
        let human: MetricsConfig = toml::from_str("format = \"human\"").unwrap();
        assert_eq!(human.format, MetricsFormat::Human);

        let json: MetricsConfig = toml::from_str("format = \"json\"").unwrap();
        assert_eq!(json.format, MetricsFormat::Json);
    }
}
