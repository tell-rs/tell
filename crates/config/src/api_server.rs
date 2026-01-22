//! API server configuration
//!
//! Configuration for the Tell API server.

use std::path::{Path, PathBuf};

use serde::Deserialize;

/// API server configuration
///
/// The API server runs alongside the collector by default. Disable with `enabled = false`.
///
/// # Example
///
/// ```toml
/// [api_server]
/// enabled = true                      # default
/// host = "0.0.0.0"                    # default
/// port = 3000                         # default
/// audit_logging = false               # default
/// control_db = "~/.tell/control.db"   # default
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ApiServerConfig {
    /// Enable the API server
    /// Default: true
    pub enabled: bool,

    /// Host to bind to
    /// Default: "0.0.0.0"
    pub host: String,

    /// Port to listen on
    /// Default: 3000
    pub port: u16,

    /// Enable audit logging (logs all requests to stdout)
    /// Default: false
    pub audit_logging: bool,

    /// Path to control plane database (SQLite/Turso)
    /// Stores workspaces, boards, sharing, invites
    /// Default: "~/.tell/control.db" (expanded at runtime)
    pub control_db: Option<PathBuf>,
}

impl Default for ApiServerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            host: "0.0.0.0".to_string(),
            port: 3000,
            audit_logging: false,
            control_db: None, // Resolved to ~/.tell/control.db at runtime
        }
    }
}

impl ApiServerConfig {
    /// Get the control database path, expanding ~ to home directory
    pub fn control_db_path(&self) -> PathBuf {
        if let Some(ref path) = self.control_db {
            expand_tilde(path)
        } else {
            // Default: ~/.tell/control.db
            dirs::home_dir()
                .map(|h| h.join(".tell").join("control.db"))
                .unwrap_or_else(|| PathBuf::from("./data/control.db"))
        }
    }
}

/// Expand ~ to home directory
fn expand_tilde(path: &Path) -> PathBuf {
    path.to_str()
        .and_then(|s| s.strip_prefix("~/"))
        .and_then(|stripped| dirs::home_dir().map(|home| home.join(stripped)))
        .unwrap_or_else(|| path.to_path_buf())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ApiServerConfig::default();
        assert!(config.enabled);
        assert_eq!(config.host, "0.0.0.0");
        assert_eq!(config.port, 3000);
        assert!(!config.audit_logging);
        assert!(config.control_db.is_none());
    }

    #[test]
    fn test_disabled() {
        let toml = r#"
enabled = false
"#;
        let config: ApiServerConfig = toml::from_str(toml).unwrap();
        assert!(!config.enabled);
    }

    #[test]
    fn test_audit_logging_enabled() {
        let toml = r#"
audit_logging = true
"#;
        let config: ApiServerConfig = toml::from_str(toml).unwrap();
        assert!(config.audit_logging);
    }

    #[test]
    fn test_custom_control_db() {
        let toml = r#"
control_db = "/var/lib/tell/control.db"
"#;
        let config: ApiServerConfig = toml::from_str(toml).unwrap();
        assert_eq!(
            config.control_db,
            Some(PathBuf::from("/var/lib/tell/control.db"))
        );
    }

    #[test]
    fn test_expand_tilde() {
        let path = PathBuf::from("~/test/path");
        let expanded = expand_tilde(&path);
        assert!(!expanded.to_str().unwrap().starts_with("~"));
    }
}
