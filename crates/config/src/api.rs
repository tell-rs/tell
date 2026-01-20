//! API configuration
//!
//! Configuration for CLI commands that connect to the Tell API server.

use serde::Deserialize;

/// API client configuration
///
/// # Example
///
/// ```toml
/// [api]
/// url = "http://localhost:3000"
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ApiConfig {
    /// API server URL
    /// Default: "http://localhost:3000"
    pub url: String,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            url: "http://localhost:3000".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ApiConfig::default();
        assert_eq!(config.url, "http://localhost:3000");
    }

    #[test]
    fn test_custom_url() {
        let toml = r#"
url = "https://api.tell.io"
"#;
        let config: ApiConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.url, "https://api.tell.io");
    }
}
