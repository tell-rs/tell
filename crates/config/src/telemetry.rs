//! Telemetry configuration
//!
//! Controls product telemetry collection and reporting.
//!
//! # Defaults
//!
//! Telemetry is enabled by default:
//! - `enabled`: true
//! - `interval`: 7 days (weekly reporting)
//!
//! Set `enabled = false` to opt out.
//!
//! The endpoint is hardcoded in the telemetry crate (t.tell.rs).
//!
//! # What's Collected
//!
//! - Deployment info: version, OS, arch, anonymous install ID
//! - Configuration shape: which sources/sinks/connectors are enabled (not values)
//! - Runtime metrics: messages processed, uptime, error counts
//! - Feature usage: boards count, API keys count, CLI commands used
//!
//! Use `tell telemetry show` to see exactly what would be sent.

use serde::Deserialize;
use std::time::Duration;

/// Telemetry configuration
///
/// # Example
///
/// ```toml
/// [telemetry]
/// # Opt out of telemetry
/// enabled = false
///
/// # Or customize interval
/// enabled = true
/// interval = "1d"
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct TelemetryConfig {
    /// Enable telemetry reporting
    /// Default: true
    pub enabled: bool,

    /// Reporting interval (how often to send telemetry)
    /// Default: 7 days
    #[serde(with = "humantime_serde")]
    pub interval: Duration,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval: Duration::from_secs(7 * 24 * 60 * 60), // 7 days
        }
    }
}

impl TelemetryConfig {
    /// Check if telemetry is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = TelemetryConfig::default();
        assert!(config.enabled);
        assert_eq!(config.interval, Duration::from_secs(7 * 24 * 60 * 60));
    }

    #[test]
    fn test_deserialize_empty() {
        let config: TelemetryConfig = toml::from_str("").unwrap();
        assert!(config.enabled);
    }

    #[test]
    fn test_deserialize_disabled() {
        let toml = r#"
enabled = false
"#;
        let config: TelemetryConfig = toml::from_str(toml).unwrap();
        assert!(!config.enabled);
    }

    #[test]
    fn test_deserialize_full() {
        let toml = r#"
enabled = true
interval = "1d"
"#;
        let config: TelemetryConfig = toml::from_str(toml).unwrap();
        assert!(config.enabled);
        assert_eq!(config.interval, Duration::from_secs(24 * 60 * 60));
    }

    #[test]
    fn test_deserialize_interval_variants() {
        for (s, expected) in [
            ("1d", Duration::from_secs(24 * 60 * 60)),
            ("7d", Duration::from_secs(7 * 24 * 60 * 60)),
            ("1h", Duration::from_secs(60 * 60)),
            ("30d", Duration::from_secs(30 * 24 * 60 * 60)),
        ] {
            let toml = format!("interval = \"{}\"", s);
            let config: TelemetryConfig = toml::from_str(&toml).unwrap();
            assert_eq!(config.interval, expected, "Failed for {}", s);
        }
    }
}
