//! API key provider for HTTP API authentication
//!
//! Validates `tell_<jwt>` format API keys for programmatic access.
//! These are different from streaming hex keys used by the collector.

use std::sync::Arc;

use async_trait::async_trait;
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};
use tracing::debug;

use crate::claims::{TokenClaims, extract_jwt, is_api_token_format};
use crate::error::{AuthError, Result};
use crate::provider::AuthProvider;
use crate::user::UserInfo;
use crate::user_store::LocalUserStore;

/// API key provider for HTTP API authentication
///
/// Validates JWT-based API keys with format `tell_<jwt>`.
/// Supports optional revocation checking via LocalUserStore.
///
/// # Example
///
/// ```ignore
/// let provider = ApiKeyProvider::new(b"your-secret-at-least-32-bytes!!");
///
/// // With revocation checking
/// let store = Arc::new(LocalUserStore::open("users.db").await?);
/// let provider = ApiKeyProvider::new(b"secret").with_revocation_store(store);
///
/// // Validate an API key
/// let user = provider.validate("tell_eyJ...").await?;
/// ```
pub struct ApiKeyProvider {
    decoding_key: DecodingKey,
    validation: Validation,
    /// Optional store for revocation checking
    revocation_store: Option<Arc<LocalUserStore>>,
}

impl std::fmt::Debug for ApiKeyProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApiKeyProvider")
            .field("has_revocation_store", &self.revocation_store.is_some())
            .finish()
    }
}

impl ApiKeyProvider {
    /// Create a new API key provider
    ///
    /// # Arguments
    ///
    /// * `secret` - JWT signing secret (must be at least 32 bytes)
    ///
    /// # Panics
    ///
    /// Panics if secret is less than 32 bytes.
    pub fn new(secret: &[u8]) -> Self {
        assert!(secret.len() >= 32, "JWT secret must be at least 32 bytes");

        let decoding_key = DecodingKey::from_secret(secret);

        let mut validation = Validation::new(Algorithm::HS256);
        validation.validate_exp = true;
        validation.validate_nbf = true;
        validation.required_spec_claims.clear();
        validation.set_issuer(&["tell"]);

        Self {
            decoding_key,
            validation,
            revocation_store: None,
        }
    }

    /// Add a revocation store for checking if tokens are revoked
    pub fn with_revocation_store(mut self, store: Arc<LocalUserStore>) -> Self {
        self.revocation_store = Some(store);
        self
    }

    /// Validate an API key and return user info
    pub async fn validate_key(&self, token: &str) -> Result<UserInfo> {
        // Check token format
        if !is_api_token_format(token) {
            return Err(AuthError::InvalidTokenFormat);
        }

        // Extract JWT
        let jwt = extract_jwt(token).ok_or(AuthError::InvalidTokenFormat)?;

        // Decode and validate JWT
        let token_data =
            decode::<TokenClaims>(jwt, &self.decoding_key, &self.validation).map_err(|e| {
                debug!("API key JWT validation failed: {:?}", e);
                match e.kind() {
                    jsonwebtoken::errors::ErrorKind::ExpiredSignature => AuthError::TokenExpired,
                    jsonwebtoken::errors::ErrorKind::ImmatureSignature => {
                        AuthError::TokenNotYetValid
                    }
                    jsonwebtoken::errors::ErrorKind::InvalidSignature => {
                        AuthError::InvalidSignature
                    }
                    _ => AuthError::InvalidClaims(e.to_string()),
                }
            })?;

        let claims = token_data.claims;

        // Check revocation if store is configured
        if let Some(store) = &self.revocation_store
            && store.is_token_revoked(&claims.token_id).await?
        {
            return Err(AuthError::TokenRevoked);
        }

        // Build user info from claims
        let mut user = UserInfo::new(&claims.user_id, &claims.email);
        user.role = claims.role.clone();

        // Add API key metadata
        user.metadata
            .insert("token_id".to_string(), claims.token_id.clone());
        user.metadata
            .insert("auth_method".to_string(), "api_key".to_string());
        user.metadata
            .insert("workspace_id".to_string(), claims.workspace_id.clone());

        Ok(user)
    }
}

#[async_trait]
impl AuthProvider for ApiKeyProvider {
    async fn validate(&self, token: &str) -> Result<UserInfo> {
        self.validate_key(token).await
    }

    fn name(&self) -> &'static str {
        "apikey"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::roles::Role;
    use crate::test_utils;

    #[tokio::test]
    async fn test_validate_api_key() {
        let provider = ApiKeyProvider::new(test_utils::TEST_SECRET);

        // Create a valid API key token
        let token = test_utils::admin_token("api-user-1", "api@example.com");

        let user = provider.validate_key(&token).await.unwrap();
        assert_eq!(user.id, "api-user-1");
        assert_eq!(user.email, "api@example.com");
        assert_eq!(
            user.metadata.get("auth_method"),
            Some(&"api_key".to_string())
        );
    }

    #[tokio::test]
    async fn test_invalid_format() {
        let provider = ApiKeyProvider::new(test_utils::TEST_SECRET);

        // Missing tell_ prefix
        let result = provider.validate_key("invalid_token").await;
        assert!(matches!(result, Err(AuthError::InvalidTokenFormat)));

        // Wrong prefix
        let result = provider.validate_key("cdp_something").await;
        assert!(matches!(result, Err(AuthError::InvalidTokenFormat)));
    }

    #[tokio::test]
    async fn test_invalid_signature() {
        let provider = ApiKeyProvider::new(test_utils::TEST_SECRET);

        // Token signed with different secret
        let other_provider = ApiKeyProvider::new(b"different-secret-at-least-32-bytes!");
        let _ = other_provider; // Just to show it's different

        // Manually construct a token with wrong signature
        let result = provider.validate_key("tell_eyJhbGciOiJIUzI1NiJ9.eyJ0b2tlbl9pZCI6InRlc3QiLCJ1c2VyX2lkIjoidXNlci0xIiwiZW1haWwiOiJ0ZXN0QGV4YW1wbGUuY29tIiwicm9sZSI6ImFkbWluIiwid29ya3NwYWNlX2lkIjoiMSIsInBlcm1pc3Npb25zIjpbXSwiZXhwIjoxOTk5OTk5OTk5LCJpYXQiOjF9.wrong_signature").await;
        assert!(matches!(result, Err(AuthError::InvalidSignature)));
    }

    #[tokio::test]
    async fn test_with_revocation_store() {
        let store = Arc::new(LocalUserStore::in_memory().await.unwrap());
        let provider =
            ApiKeyProvider::new(test_utils::TEST_SECRET).with_revocation_store(Arc::clone(&store));

        // Create a token - the token_id will be "test-token-api-user"
        let token = test_utils::create_test_token("api-user", "api@example.com", Role::Admin, None);

        // Should work initially
        let user = provider.validate_key(&token).await.unwrap();
        assert_eq!(user.id, "api-user");

        // Get the token_id from the validated user
        let token_id = user.metadata.get("token_id").unwrap().clone();

        // Revoke the token
        store.revoke_token(&token_id, Some("test")).await.unwrap();

        // Should fail after revocation
        let result = provider.validate_key(&token).await;
        assert!(matches!(result, Err(AuthError::TokenRevoked)));
    }
}
