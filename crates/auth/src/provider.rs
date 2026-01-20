//! Authentication providers
//!
//! Defines the `AuthProvider` trait for validating tokens and extracting user info.
//! Currently supports local JWT validation; WorkOS can be added later.

use async_trait::async_trait;
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use tracing::debug;

use crate::claims::{extract_jwt, is_api_token_format, TokenClaims};
use crate::error::{AuthError, Result};
use crate::user::UserInfo;

/// Authentication provider trait
///
/// Implement this trait to add new authentication backends.
/// Currently supported: `LocalJwtProvider`
/// Future: WorkOS
#[async_trait]
pub trait AuthProvider: Send + Sync {
    /// Validate a token and return user information
    ///
    /// # Errors
    ///
    /// Returns `AuthError` if:
    /// - Token format is invalid
    /// - Token signature verification fails
    /// - Token has expired
    /// - Token claims are invalid
    async fn validate(&self, token: &str) -> Result<UserInfo>;

    /// Provider name for logging/debugging
    fn name(&self) -> &'static str;
}

/// Local JWT provider using HMAC-SHA256
///
/// Validates JWT tokens signed with a shared secret.
/// This is the default provider for self-hosted deployments.
///
/// # Example
///
/// ```
/// use tell_auth::LocalJwtProvider;
///
/// let provider = LocalJwtProvider::new(b"your-secret-key-at-least-32-bytes!");
/// ```
pub struct LocalJwtProvider {
    decoding_key: DecodingKey,
    validation: Validation,
    issuer: Option<String>,
}

impl std::fmt::Debug for LocalJwtProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalJwtProvider")
            .field("algorithm", &"HS256")
            .field("issuer", &self.issuer)
            .finish()
    }
}

impl LocalJwtProvider {
    /// Create a new provider with HMAC-SHA256 secret
    ///
    /// # Arguments
    ///
    /// * `secret` - Secret key for HMAC-SHA256 (should be at least 32 bytes)
    #[must_use]
    pub fn new(secret: &[u8]) -> Self {
        let mut validation = Validation::new(Algorithm::HS256);
        // We validate expiration manually for better error messages
        validation.validate_exp = true;
        validation.validate_nbf = true;
        // Don't require specific claims - we check them ourselves
        validation.required_spec_claims.clear();

        Self {
            decoding_key: DecodingKey::from_secret(secret),
            validation,
            issuer: None,
        }
    }

    /// Create a provider with a specific issuer requirement
    ///
    /// Tokens must have an `iss` claim matching this issuer.
    #[must_use]
    pub fn with_issuer(mut self, issuer: impl Into<String>) -> Self {
        let issuer = issuer.into();
        self.validation.set_issuer(&[&issuer]);
        self.issuer = Some(issuer);
        self
    }
}

#[async_trait]
impl AuthProvider for LocalJwtProvider {
    async fn validate(&self, token: &str) -> Result<UserInfo> {
        // Check token format (must be tell_<jwt>)
        if !is_api_token_format(token) {
            return Err(AuthError::InvalidTokenFormat);
        }

        // Extract JWT part
        let jwt = extract_jwt(token).ok_or(AuthError::InvalidTokenFormat)?;

        // Decode and verify JWT
        let token_data = decode::<TokenClaims>(jwt, &self.decoding_key, &self.validation)
            .map_err(|e| {
                debug!("JWT validation failed: {:?}", e);
                match e.kind() {
                    jsonwebtoken::errors::ErrorKind::ExpiredSignature => AuthError::TokenExpired,
                    jsonwebtoken::errors::ErrorKind::ImmatureSignature => {
                        AuthError::TokenNotYetValid
                    }
                    jsonwebtoken::errors::ErrorKind::InvalidSignature => AuthError::InvalidSignature,
                    _ => AuthError::InvalidClaims(e.to_string()),
                }
            })?;

        let claims = token_data.claims;

        // Convert claims to UserInfo
        Ok(UserInfo::from_claims(&claims))
    }

    fn name(&self) -> &'static str {
        "local"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{create_test_token, TEST_SECRET};
    use crate::Role;

    #[tokio::test]
    async fn test_valid_token() {
        let provider = LocalJwtProvider::new(TEST_SECRET);
        let token = create_test_token("user-1", "test@example.com", Role::Editor, None);

        let user = provider.validate(&token).await.unwrap();
        assert_eq!(user.id, "user-1");
        assert_eq!(user.email, "test@example.com");
        assert_eq!(user.parsed_role(), Role::Editor);
    }

    #[tokio::test]
    async fn test_invalid_format() {
        let provider = LocalJwtProvider::new(TEST_SECRET);

        let result = provider.validate("not_a_tell_token").await;
        assert!(matches!(result, Err(AuthError::InvalidTokenFormat)));
    }

    #[tokio::test]
    async fn test_invalid_signature() {
        let provider = LocalJwtProvider::new(TEST_SECRET);

        // Create token with different secret
        let other_secret = b"different-secret-key-32-bytes!!!";
        let token = create_test_token_with_secret(
            "user-1",
            "test@example.com",
            Role::Editor,
            None,
            other_secret,
        );

        let result = provider.validate(&token).await;
        assert!(matches!(result, Err(AuthError::InvalidSignature)));
    }

    #[tokio::test]
    async fn test_expired_token() {
        let provider = LocalJwtProvider::new(TEST_SECRET);
        let token = create_expired_test_token("user-1", "test@example.com", Role::Viewer);

        let result = provider.validate(&token).await;
        assert!(matches!(result, Err(AuthError::TokenExpired)));
    }

    #[tokio::test]
    async fn test_issuer_validation() {
        let provider = LocalJwtProvider::new(TEST_SECRET).with_issuer("tell");
        let token = create_test_token_with_issuer(
            "user-1",
            "test@example.com",
            Role::Editor,
            Some("tell"),
        );

        let user = provider.validate(&token).await.unwrap();
        assert_eq!(user.id, "user-1");
    }

    #[tokio::test]
    async fn test_wrong_issuer() {
        let provider = LocalJwtProvider::new(TEST_SECRET).with_issuer("tell");
        let token = create_test_token_with_issuer(
            "user-1",
            "test@example.com",
            Role::Editor,
            Some("wrong-issuer"),
        );

        let result = provider.validate(&token).await;
        assert!(matches!(result, Err(AuthError::InvalidClaims(_))));
    }

    // Test helper functions - these call into test_utils
    fn create_test_token_with_secret(
        user_id: &str,
        email: &str,
        role: Role,
        workspace_id: Option<&str>,
        secret: &[u8],
    ) -> String {
        crate::test_utils::create_test_token_with_options(
            user_id,
            email,
            role,
            workspace_id,
            None,
            secret,
            chrono::Duration::hours(1),
        )
    }

    fn create_expired_test_token(user_id: &str, email: &str, role: Role) -> String {
        crate::test_utils::create_test_token_with_options(
            user_id,
            email,
            role,
            None,
            None,
            TEST_SECRET,
            chrono::Duration::hours(-1), // Already expired
        )
    }

    fn create_test_token_with_issuer(
        user_id: &str,
        email: &str,
        role: Role,
        issuer: Option<&str>,
    ) -> String {
        crate::test_utils::create_test_token_with_options(
            user_id,
            email,
            role,
            None,
            issuer,
            TEST_SECRET,
            chrono::Duration::hours(1),
        )
    }
}
