//! JWT token claims
//!
//! Defines the structure of JWT tokens used for authentication.

use serde::{Deserialize, Serialize};

/// JWT claims for API tokens
///
/// Token format: `tell_<jwt>`
/// Where JWT contains these claims.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenClaims {
    /// Token ID (for revocation)
    #[serde(rename = "tid")]
    pub token_id: String,

    /// User ID
    #[serde(rename = "uid")]
    pub user_id: String,

    /// User email
    #[serde(rename = "email", default)]
    pub email: String,

    /// User's role (viewer, editor, admin, platform)
    #[serde(rename = "role", default = "default_role")]
    pub role: String,

    /// Workspace ID (scope of this token)
    #[serde(rename = "wid")]
    pub workspace_id: String,

    /// Explicit permissions granted to this token (optional, for fine-grained control)
    #[serde(default)]
    pub permissions: Vec<String>,

    // Standard JWT claims
    /// Subject (user ID)
    #[serde(rename = "sub", skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,

    /// Expiration time (Unix timestamp)
    #[serde(rename = "exp")]
    pub expires_at: i64,

    /// Issued at (Unix timestamp)
    #[serde(rename = "iat")]
    pub issued_at: i64,

    /// Not before (Unix timestamp)
    #[serde(rename = "nbf", skip_serializing_if = "Option::is_none")]
    pub not_before: Option<i64>,

    /// Issuer
    #[serde(rename = "iss", skip_serializing_if = "Option::is_none")]
    pub issuer: Option<String>,

    /// JWT ID
    #[serde(rename = "jti", skip_serializing_if = "Option::is_none")]
    pub jwt_id: Option<String>,
}

fn default_role() -> String {
    "viewer".to_string()
}

impl TokenClaims {
    /// Check if the token has expired
    pub fn is_expired(&self) -> bool {
        let now = chrono::Utc::now().timestamp();
        self.expires_at < now
    }

    /// Check if the token is not yet valid
    pub fn is_not_yet_valid(&self) -> bool {
        if let Some(nbf) = self.not_before {
            let now = chrono::Utc::now().timestamp();
            nbf > now
        } else {
            false
        }
    }

    /// Validate token timing
    pub fn validate_timing(&self) -> Result<(), &'static str> {
        if self.is_expired() {
            return Err("token expired");
        }
        if self.is_not_yet_valid() {
            return Err("token not yet valid");
        }
        Ok(())
    }
}

/// Token prefix for Tell API keys
pub const TOKEN_PREFIX: &str = "tell_";

/// Check if a string looks like a Tell API token
pub fn is_api_token_format(token: &str) -> bool {
    token.starts_with(TOKEN_PREFIX) && token.len() > TOKEN_PREFIX.len() + 10
}

/// Extract JWT from prefixed token (removes "tell_" prefix)
pub fn extract_jwt(token: &str) -> Option<&str> {
    if is_api_token_format(token) {
        Some(&token[TOKEN_PREFIX.len()..])
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_api_token_format() {
        assert!(is_api_token_format(
            "tell_eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"
        ));
        assert!(!is_api_token_format("bearer_something"));
        assert!(!is_api_token_format("tell_")); // Too short
    }

    #[test]
    fn test_extract_jwt() {
        let token = "tell_eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9";
        assert_eq!(
            extract_jwt(token),
            Some("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9")
        );
        assert_eq!(extract_jwt("invalid"), None);
    }

    #[test]
    fn test_token_expired() {
        let claims = TokenClaims {
            token_id: "tid".to_string(),
            user_id: "uid".to_string(),
            email: "test@example.com".to_string(),
            role: "viewer".to_string(),
            workspace_id: "wid".to_string(),
            permissions: vec![],
            subject: None,
            expires_at: 0, // Expired (Unix epoch)
            issued_at: 0,
            not_before: None,
            issuer: None,
            jwt_id: None,
        };
        assert!(claims.is_expired());
    }
}
