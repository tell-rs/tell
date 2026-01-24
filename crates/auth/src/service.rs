//! Authentication service with session management
//!
//! Orchestrates JWT validation, session management, and token revocation.
//! This is the main entry point for authentication operations.

use std::sync::Arc;
use std::time::Duration as StdDuration;

use chrono::Duration;
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, encode};
use tokio::sync::watch;
use tracing::{debug, info, warn};

use crate::claims::{TOKEN_PREFIX, TokenClaims, extract_jwt, is_api_token_format};
use crate::error::{AuthError, Result};
use crate::user::UserInfo;
use crate::user_store::{Session, StoredUser};
use crate::user_store_trait::UserStore;

/// Authentication response with token and user info
#[derive(Debug, Clone)]
pub struct AuthResponse {
    /// JWT token (tell_<jwt>)
    pub token: String,
    /// Authenticated user information
    pub user: UserInfo,
    /// When the token expires (Unix timestamp)
    pub expires_at: i64,
    /// Session ID (for session management)
    pub session_id: String,
}

/// Configuration for the auth service
#[derive(Debug, Clone)]
pub struct AuthServiceConfig {
    /// JWT signing secret (must be at least 32 bytes)
    pub jwt_secret: Vec<u8>,
    /// Token time-to-live
    pub token_ttl: StdDuration,
    /// Issuer claim for tokens
    pub issuer: Option<String>,
    /// Whether to require session validation (stateful mode)
    pub require_session: bool,
}

impl Default for AuthServiceConfig {
    fn default() -> Self {
        Self {
            jwt_secret: vec![],
            token_ttl: StdDuration::from_secs(24 * 60 * 60), // 24 hours
            issuer: Some("tell".to_string()),
            require_session: true,
        }
    }
}

impl AuthServiceConfig {
    /// Create a new config with the given secret
    pub fn new(secret: impl Into<Vec<u8>>) -> Self {
        Self {
            jwt_secret: secret.into(),
            ..Default::default()
        }
    }

    /// Set token TTL
    pub fn with_ttl(mut self, ttl: StdDuration) -> Self {
        self.token_ttl = ttl;
        self
    }

    /// Set issuer
    pub fn with_issuer(mut self, issuer: impl Into<String>) -> Self {
        self.issuer = Some(issuer.into());
        self
    }

    /// Disable session requirement (stateless JWT mode)
    pub fn stateless(mut self) -> Self {
        self.require_session = false;
        self
    }
}

/// Authentication service with session management
///
/// Provides:
/// - Login with email/password
/// - Token validation with session checks
/// - Token refresh
/// - Logout with session/token revocation
///
/// # Example
///
/// ```ignore
/// let store = LocalUserStore::open("users.db").await?;
/// let config = AuthServiceConfig::new(b"your-secret-key-at-least-32-bytes!");
/// let auth = AuthService::new(Arc::new(store), config);
///
/// // Login
/// let response = auth.login("user@example.com", "password", None, None).await?;
///
/// // Verify token on subsequent requests
/// let user = auth.verify_token(&response.token).await?;
///
/// // Logout
/// auth.logout(&response.token).await?;
/// ```
pub struct AuthService {
    store: Arc<dyn UserStore>,
    config: AuthServiceConfig,
    encoding_key: EncodingKey,
    decoding_key: DecodingKey,
    validation: Validation,
}

impl std::fmt::Debug for AuthService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthService")
            .field("require_session", &self.config.require_session)
            .field("issuer", &self.config.issuer)
            .finish()
    }
}

impl AuthService {
    /// Create a new auth service
    ///
    /// # Panics
    ///
    /// Panics if the JWT secret is less than 32 bytes.
    pub fn new(store: Arc<dyn UserStore>, config: AuthServiceConfig) -> Self {
        assert!(
            config.jwt_secret.len() >= 32,
            "JWT secret must be at least 32 bytes"
        );

        let encoding_key = EncodingKey::from_secret(&config.jwt_secret);
        let decoding_key = DecodingKey::from_secret(&config.jwt_secret);

        let mut validation = Validation::new(Algorithm::HS256);
        validation.validate_exp = true;
        validation.validate_nbf = true;
        validation.required_spec_claims.clear();

        if let Some(ref issuer) = config.issuer {
            validation.set_issuer(&[issuer]);
        }

        Self {
            store,
            config,
            encoding_key,
            decoding_key,
            validation,
        }
    }

    /// Login with email and password
    ///
    /// Verifies credentials, creates a session, and returns a JWT token.
    pub async fn login(
        &self,
        email: &str,
        password: &str,
        ip_address: Option<&str>,
        user_agent: Option<&str>,
    ) -> Result<AuthResponse> {
        // Verify credentials
        let user = self
            .store
            .verify_credentials(email, password)
            .await?
            .ok_or(AuthError::InvalidClaims("invalid email or password".into()))?;

        // Generate token
        let (token, claims) = self.generate_token(&user)?;

        // Create session
        let ttl = Duration::from_std(self.config.token_ttl).unwrap_or_else(|_| Duration::hours(24));

        let session = self
            .store
            .create_session(&user.id, &token, ttl, ip_address, user_agent)
            .await?;

        info!(
            user_id = %user.id,
            email = %user.email,
            session_id = %session.id,
            "User logged in"
        );

        Ok(AuthResponse {
            token,
            user: user.to_user_info(),
            expires_at: claims.expires_at,
            session_id: session.id,
        })
    }

    /// Verify a token and return user information
    ///
    /// Validates:
    /// 1. Token format (tell_<jwt>)
    /// 2. JWT signature and claims
    /// 3. Session exists and is not expired (if require_session)
    /// 4. Token is not revoked
    pub async fn verify_token(&self, token: &str) -> Result<UserInfo> {
        // Check token format
        if !is_api_token_format(token) {
            return Err(AuthError::InvalidTokenFormat);
        }

        // Extract and validate JWT
        let jwt = extract_jwt(token).ok_or(AuthError::InvalidTokenFormat)?;
        let claims = self.validate_jwt(jwt)?;

        // Check revocation
        if self.store.is_token_revoked(&claims.token_id).await? {
            return Err(AuthError::TokenRevoked);
        }

        // Check session (if required)
        if self.config.require_session {
            self.store.validate_session(token).await?;
        }

        // Get user info
        let user = self
            .store
            .get_by_id(&claims.user_id)
            .await?
            .ok_or(AuthError::InvalidClaims("user not found".into()))?;

        Ok(user.to_user_info())
    }

    /// Refresh a token
    ///
    /// Generates a new token with extended expiry, updates the session.
    pub async fn refresh_token(&self, token: &str) -> Result<AuthResponse> {
        // Verify current token
        let user_info = self.verify_token(token).await?;

        // Get the user
        let user = self
            .store
            .get_by_id(&user_info.id)
            .await?
            .ok_or(AuthError::InvalidClaims("user not found".into()))?;

        // Get session info for IP/UA
        let session = self.store.get_session_by_token(token).await?;

        // Delete old session
        self.store.delete_session_by_token(token).await?;

        // Generate new token
        let (new_token, claims) = self.generate_token(&user)?;

        // Create new session
        let ttl = Duration::from_std(self.config.token_ttl).unwrap_or_else(|_| Duration::hours(24));

        let ip_address = session.as_ref().and_then(|s| s.ip_address.as_deref());
        let user_agent = session.as_ref().and_then(|s| s.user_agent.as_deref());

        let new_session = self
            .store
            .create_session(&user.id, &new_token, ttl, ip_address, user_agent)
            .await?;

        debug!(
            user_id = %user.id,
            session_id = %new_session.id,
            "Token refreshed"
        );

        Ok(AuthResponse {
            token: new_token,
            user: user.to_user_info(),
            expires_at: claims.expires_at,
            session_id: new_session.id,
        })
    }

    /// Logout - delete session and optionally revoke token
    ///
    /// # Arguments
    ///
    /// * `token` - The token to invalidate
    /// * `revoke` - If true, add token to revocation blacklist (prevents reuse even if JWT is still valid)
    pub async fn logout(&self, token: &str, revoke: bool) -> Result<()> {
        // Delete session
        self.store.delete_session_by_token(token).await?;

        // Optionally revoke token
        if revoke
            && let Some(jwt) = extract_jwt(token)
            && let Ok(claims) = self.validate_jwt(jwt)
        {
            self.store
                .revoke_token(&claims.token_id, Some("logout"))
                .await?;
        }

        info!("User logged out");
        Ok(())
    }

    /// Logout from all devices - delete all sessions for user
    pub async fn logout_all(&self, user_id: &str) -> Result<u64> {
        let count = self.store.delete_user_sessions(user_id).await?;
        info!(user_id = %user_id, sessions = count, "Logged out from all devices");
        Ok(count)
    }

    /// List active sessions for a user
    pub async fn list_sessions(&self, user_id: &str) -> Result<Vec<Session>> {
        self.store.list_user_sessions(user_id).await
    }

    /// Get the underlying user store
    pub fn store(&self) -> &dyn UserStore {
        self.store.as_ref()
    }

    /// Run cleanup tasks (expired sessions, old revocations)
    pub async fn cleanup(&self) -> Result<()> {
        self.store.cleanup_all().await
    }

    /// Start a background cleanup task
    ///
    /// Returns a shutdown sender - drop it or send to stop the task.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let (shutdown_tx, handle) = auth_service.start_cleanup_task(Duration::from_secs(3600));
    ///
    /// // Later, to stop:
    /// drop(shutdown_tx);
    /// handle.await;
    /// ```
    pub fn start_cleanup_task(
        self: &Arc<Self>,
        interval: StdDuration,
    ) -> (watch::Sender<()>, tokio::task::JoinHandle<()>) {
        let (shutdown_tx, mut shutdown_rx) = watch::channel(());
        let service = Arc::clone(self);

        let handle = tokio::spawn(async move {
            info!("Auth cleanup task started (interval: {:?})", interval);

            loop {
                tokio::select! {
                    _ = tokio::time::sleep(interval) => {
                        if let Err(e) = service.cleanup().await {
                            warn!("Auth cleanup failed: {}", e);
                        } else {
                            debug!("Auth cleanup completed");
                        }
                    }
                    _ = shutdown_rx.changed() => {
                        info!("Auth cleanup task shutting down");
                        break;
                    }
                }
            }
        });

        (shutdown_tx, handle)
    }

    // =========================================================================
    // Internal methods
    // =========================================================================

    /// Generate a JWT token for a user
    fn generate_token(&self, user: &StoredUser) -> Result<(String, TokenClaims)> {
        let now = chrono::Utc::now();
        let ttl = Duration::from_std(self.config.token_ttl).unwrap_or_else(|_| Duration::hours(24));
        let expires_at = now + ttl;

        let token_id = uuid::Uuid::new_v4().to_string();

        let claims = TokenClaims {
            token_id: token_id.clone(),
            user_id: user.id.clone(),
            email: user.email.clone(),
            role: user.role.as_str().to_string(),
            workspace_id: "1".to_string(), // Default workspace
            permissions: vec![],
            subject: Some(user.id.clone()),
            expires_at: expires_at.timestamp(),
            issued_at: now.timestamp(),
            not_before: Some(now.timestamp()),
            issuer: self.config.issuer.clone(),
            jwt_id: Some(token_id),
        };

        let jwt = encode(&Header::default(), &claims, &self.encoding_key)
            .map_err(|e| AuthError::InvalidClaims(format!("failed to encode JWT: {}", e)))?;

        let token = format!("{}{}", TOKEN_PREFIX, jwt);

        Ok((token, claims))
    }

    /// Validate JWT and return claims
    fn validate_jwt(&self, jwt: &str) -> Result<TokenClaims> {
        let token_data =
            decode::<TokenClaims>(jwt, &self.decoding_key, &self.validation).map_err(|e| {
                debug!("JWT validation failed: {:?}", e);
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

        Ok(token_data.claims)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::roles::Role;
    use crate::user_store::LocalUserStore;

    async fn create_test_service() -> (AuthService, Arc<LocalUserStore>) {
        let store = Arc::new(LocalUserStore::in_memory().await.unwrap());
        let config = AuthServiceConfig::new(b"test-secret-key-at-least-32-bytes!");
        let service = AuthService::new(Arc::clone(&store) as Arc<dyn UserStore>, config);
        (service, store)
    }

    #[tokio::test]
    async fn test_login_and_verify() {
        let (service, store) = create_test_service().await;

        // Create user
        store
            .create_user("test@example.com", "password123", Role::Editor)
            .await
            .unwrap();

        // Login
        let response = service
            .login("test@example.com", "password123", Some("127.0.0.1"), None)
            .await
            .unwrap();

        assert!(response.token.starts_with("tell_"));
        assert_eq!(response.user.email, "test@example.com");
        assert_eq!(response.user.parsed_role(), Role::Editor);

        // Verify token
        let user = service.verify_token(&response.token).await.unwrap();
        assert_eq!(user.email, "test@example.com");
    }

    #[tokio::test]
    async fn test_login_invalid_credentials() {
        let (service, store) = create_test_service().await;

        store
            .create_user("test@example.com", "password123", Role::Viewer)
            .await
            .unwrap();

        // Wrong password
        let result = service
            .login("test@example.com", "wrong_password", None, None)
            .await;
        assert!(result.is_err());

        // Non-existent user
        let result = service
            .login("nobody@example.com", "password", None, None)
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_logout() {
        let (service, store) = create_test_service().await;

        store
            .create_user("test@example.com", "password", Role::Viewer)
            .await
            .unwrap();

        let response = service
            .login("test@example.com", "password", None, None)
            .await
            .unwrap();

        // Logout without revocation
        service.logout(&response.token, false).await.unwrap();

        // Token should fail (session gone)
        let result = service.verify_token(&response.token).await;
        assert!(matches!(result, Err(AuthError::SessionNotFound)));
    }

    #[tokio::test]
    async fn test_logout_with_revocation() {
        let (service, store) = create_test_service().await;

        store
            .create_user("test@example.com", "password", Role::Viewer)
            .await
            .unwrap();

        let response = service
            .login("test@example.com", "password", None, None)
            .await
            .unwrap();

        // Logout with revocation
        service.logout(&response.token, true).await.unwrap();

        // Even if we recreate session, token should be revoked
        // (In practice this wouldn't happen, but tests the revocation check)
    }

    #[tokio::test]
    async fn test_refresh_token() {
        let (service, store) = create_test_service().await;

        store
            .create_user("test@example.com", "password", Role::Editor)
            .await
            .unwrap();

        let response = service
            .login("test@example.com", "password", Some("127.0.0.1"), None)
            .await
            .unwrap();

        // Refresh
        let new_response = service.refresh_token(&response.token).await.unwrap();

        assert_ne!(new_response.token, response.token);
        assert_eq!(new_response.user.email, "test@example.com");

        // Old token should be invalid (session deleted)
        let result = service.verify_token(&response.token).await;
        assert!(result.is_err());

        // New token should work
        let user = service.verify_token(&new_response.token).await.unwrap();
        assert_eq!(user.email, "test@example.com");
    }

    #[tokio::test]
    async fn test_logout_all() {
        let (service, store) = create_test_service().await;

        let user = store
            .create_user("test@example.com", "password", Role::Viewer)
            .await
            .unwrap();

        // Login multiple times (simulating multiple devices)
        let r1 = service
            .login("test@example.com", "password", None, None)
            .await
            .unwrap();
        let r2 = service
            .login("test@example.com", "password", None, None)
            .await
            .unwrap();
        let r3 = service
            .login("test@example.com", "password", None, None)
            .await
            .unwrap();

        // All should be valid
        assert!(service.verify_token(&r1.token).await.is_ok());
        assert!(service.verify_token(&r2.token).await.is_ok());
        assert!(service.verify_token(&r3.token).await.is_ok());

        // Logout from all devices
        let count = service.logout_all(&user.id).await.unwrap();
        assert_eq!(count, 3);

        // All should be invalid now
        assert!(service.verify_token(&r1.token).await.is_err());
        assert!(service.verify_token(&r2.token).await.is_err());
        assert!(service.verify_token(&r3.token).await.is_err());
    }

    #[tokio::test]
    async fn test_stateless_mode() {
        let store = Arc::new(LocalUserStore::in_memory().await.unwrap());
        let config = AuthServiceConfig::new(b"test-secret-key-at-least-32-bytes!").stateless();
        let service = AuthService::new(Arc::clone(&store) as Arc<dyn UserStore>, config);

        store
            .create_user("test@example.com", "password", Role::Viewer)
            .await
            .unwrap();

        let response = service
            .login("test@example.com", "password", None, None)
            .await
            .unwrap();

        // Delete session manually
        store
            .delete_session_by_token(&response.token)
            .await
            .unwrap();

        // In stateless mode, token should still be valid (no session check)
        let user = service.verify_token(&response.token).await.unwrap();
        assert_eq!(user.email, "test@example.com");
    }

    #[tokio::test]
    async fn test_invalid_token_format() {
        let (service, _) = create_test_service().await;

        // Not tell_ prefix
        let result = service.verify_token("invalid_token").await;
        assert!(matches!(result, Err(AuthError::InvalidTokenFormat)));

        // Wrong prefix
        let result = service.verify_token("cdp_something").await;
        assert!(matches!(result, Err(AuthError::InvalidTokenFormat)));
    }
}
