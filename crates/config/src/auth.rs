//! Authentication configuration
//!
//! Supports two authentication modes:
//! - **Local**: Self-hosted with local user database (default)
//! - **WorkOS**: Cloud deployment with WorkOS SSO

use std::path::PathBuf;
use std::time::Duration;

use serde::Deserialize;

/// Authentication configuration
///
/// # Example
///
/// ## Local auth (default for self-hosted)
/// ```toml
/// [auth]
/// provider = "local"
/// jwt_secret = "your-secret-key-at-least-32-characters-long"
///
/// [auth.local]
/// db_path = "data/users.db"
/// ```
///
/// ## WorkOS auth (for cloud)
/// ```toml
/// [auth]
/// provider = "workos"
///
/// [auth.workos]
/// api_key = "sk_..."
/// client_id = "client_..."
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct AuthConfig {
    /// Auth provider: "local" or "workos"
    /// Default: "local"
    pub provider: String,

    /// JWT secret for signing tokens (required for local auth)
    /// Must be at least 32 characters
    pub jwt_secret: Option<String>,

    /// JWT token expiration time
    /// Default: 24 hours
    #[serde(with = "humantime_serde")]
    pub jwt_expires_in: Duration,

    /// Local auth configuration
    #[serde(default)]
    pub local: LocalAuthConfig,

    /// WorkOS configuration
    #[serde(default)]
    pub workos: Option<WorkOsConfig>,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            provider: "local".to_string(),
            jwt_secret: None,
            jwt_expires_in: Duration::from_secs(24 * 60 * 60), // 24 hours
            local: LocalAuthConfig::default(),
            workos: None,
        }
    }
}

impl AuthConfig {
    /// Check if using local auth
    pub fn is_local(&self) -> bool {
        self.provider == "local"
    }

    /// Check if using WorkOS auth
    pub fn is_workos(&self) -> bool {
        self.provider == "workos"
    }

    /// Get the JWT secret, returning error if not set for local auth
    pub fn jwt_secret_bytes(&self) -> Option<&[u8]> {
        self.jwt_secret.as_ref().map(|s| s.as_bytes())
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<(), String> {
        match self.provider.as_str() {
            "local" => {
                // JWT secret required for local auth
                if self.jwt_secret.is_none() {
                    return Err("auth.jwt_secret is required for local auth".to_string());
                }
                let secret = self.jwt_secret.as_ref().unwrap();
                if secret.len() < 32 {
                    return Err(
                        "auth.jwt_secret must be at least 32 characters for security".to_string(),
                    );
                }
            }
            "workos" => {
                // WorkOS config required
                let workos = self.workos.as_ref().ok_or("auth.workos section is required when provider = \"workos\"")?;
                if workos.api_key.is_empty() {
                    return Err("auth.workos.api_key is required".to_string());
                }
                if workos.client_id.is_empty() {
                    return Err("auth.workos.client_id is required".to_string());
                }
            }
            other => {
                return Err(format!(
                    "unknown auth provider '{}', expected 'local' or 'workos'",
                    other
                ));
            }
        }
        Ok(())
    }
}

/// Local authentication configuration
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct LocalAuthConfig {
    /// Path to SQLite database for user storage
    /// Default: "data/users.db"
    pub db_path: PathBuf,

    /// Allow registration of new users (after initial setup)
    /// Default: false (admin must create users)
    pub allow_registration: bool,
}

impl Default for LocalAuthConfig {
    fn default() -> Self {
        Self {
            db_path: PathBuf::from("data/users.db"),
            allow_registration: false,
        }
    }
}

/// WorkOS configuration for enterprise SSO
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct WorkOsConfig {
    /// WorkOS API key
    #[serde(default)]
    pub api_key: String,

    /// WorkOS client ID
    #[serde(default)]
    pub client_id: String,

    /// WorkOS redirect URI
    /// Default: derived from server URL
    pub redirect_uri: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = AuthConfig::default();
        assert!(config.is_local());
        assert!(!config.is_workos());
        assert_eq!(config.jwt_expires_in, Duration::from_secs(24 * 60 * 60));
    }

    #[test]
    fn test_local_config() {
        let toml = r#"
provider = "local"
jwt_secret = "this-is-a-very-long-secret-key-for-testing"
jwt_expires_in = "12h"

[local]
db_path = "/var/lib/tell/users.db"
allow_registration = true
"#;
        let config: AuthConfig = toml::from_str(toml).unwrap();
        assert!(config.is_local());
        assert_eq!(config.jwt_expires_in, Duration::from_secs(12 * 60 * 60));
        assert_eq!(
            config.local.db_path,
            PathBuf::from("/var/lib/tell/users.db")
        );
        assert!(config.local.allow_registration);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_workos_config() {
        let toml = r#"
provider = "workos"

[workos]
api_key = "sk_test_123"
client_id = "client_123"
"#;
        let config: AuthConfig = toml::from_str(toml).unwrap();
        assert!(config.is_workos());
        assert_eq!(config.workos.as_ref().unwrap().api_key, "sk_test_123");
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validation_local_missing_secret() {
        let config = AuthConfig::default();
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("jwt_secret"));
    }

    #[test]
    fn test_validation_local_short_secret() {
        let config = AuthConfig {
            jwt_secret: Some("short".to_string()),
            ..Default::default()
        };
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("32 characters"));
    }

    #[test]
    fn test_validation_workos_missing_config() {
        let config = AuthConfig {
            provider: "workos".to_string(),
            ..Default::default()
        };
        let result = config.validate();
        assert!(result.is_err());
    }

    #[test]
    fn test_validation_unknown_provider() {
        let config = AuthConfig {
            provider: "unknown".to_string(),
            ..Default::default()
        };
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("unknown"));
    }
}
