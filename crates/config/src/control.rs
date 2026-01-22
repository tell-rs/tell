//! Control/admin endpoint configuration
//!
//! Lightweight HTTP endpoint for health checks and metrics.
//! Separate from the main API server - minimal overhead.

use serde::Deserialize;

/// Control endpoint configuration
///
/// The control endpoint provides health checks and metrics for monitoring.
/// Runs on a separate port from ingestion and the main API.
///
/// # Example
///
/// ```toml
/// [control]
/// enabled = true
/// host = "127.0.0.1"
/// port = 9090
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ControlConfig {
    /// Enable the control endpoint
    /// Default: true
    pub enabled: bool,

    /// Host to bind to
    /// Default: "127.0.0.1" (localhost only for security)
    pub host: String,

    /// Port to listen on
    /// Default: 9090
    pub port: u16,
}

impl Default for ControlConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            host: "127.0.0.1".to_string(),
            port: 9090,
        }
    }
}

impl ControlConfig {
    /// Get the socket address string
    pub fn addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ControlConfig::default();
        assert!(config.enabled);
        assert_eq!(config.host, "127.0.0.1");
        assert_eq!(config.port, 9090);
    }

    #[test]
    fn test_addr() {
        let config = ControlConfig::default();
        assert_eq!(config.addr(), "127.0.0.1:9090");
    }

    #[test]
    fn test_disabled() {
        let toml = r#"
enabled = false
"#;
        let config: ControlConfig = toml::from_str(toml).unwrap();
        assert!(!config.enabled);
    }

    #[test]
    fn test_custom_port() {
        let toml = r#"
port = 8081
host = "0.0.0.0"
"#;
        let config: ControlConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.port, 8081);
        assert_eq!(config.host, "0.0.0.0");
    }
}
