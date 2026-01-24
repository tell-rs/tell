//! ControlPlane-backed user store adapter
//!
//! Implements [`UserStore`] using `tell-control`'s Turso-backed database.
//! This unifies auth and control plane into a single database.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Duration;

use crate::error::{AuthError, Result};
use crate::password::{hash_password, verify_password};
use crate::roles::Role;
use crate::user_store::{Session, StoredUser};
use crate::user_store_trait::UserStore;

/// UserStore implementation backed by ControlPlane's Turso database
///
/// This adapter allows AuthService to use the unified control plane database
/// instead of a separate SQLite database for users/sessions.
pub struct ControlPlaneUserStore {
    control_plane: Arc<tell_control::ControlPlane>,
}

impl ControlPlaneUserStore {
    /// Create a new adapter wrapping a ControlPlane instance
    pub fn new(control_plane: Arc<tell_control::ControlPlane>) -> Self {
        Self { control_plane }
    }

    /// Convert control plane User to auth StoredUser
    fn to_stored_user(user: tell_control::User) -> StoredUser {
        StoredUser {
            id: user.id,
            email: user.email,
            password_hash: user.password_hash,
            role: Role::parse(&user.role).unwrap_or(Role::Viewer),
            created_at: user.created_at,
            last_login: user.last_login,
        }
    }

    /// Convert control plane Session to auth Session
    fn to_auth_session(session: tell_control::Session) -> Session {
        Session {
            id: session.id,
            user_id: session.user_id,
            token: session.token,
            expires_at: session.expires_at,
            created_at: session.created_at,
            ip_address: session.ip_address,
            user_agent: session.user_agent,
        }
    }

    /// Map control plane errors to auth errors
    fn map_error(e: tell_control::ControlError) -> AuthError {
        match &e {
            tell_control::ControlError::NotFound { .. } => AuthError::SessionNotFound,
            tell_control::ControlError::AlreadyExists { entity, .. } => {
                AuthError::InvalidClaims(format!("{} already exists", entity))
            }
            _ => AuthError::DatabaseError(e.to_string()),
        }
    }
}

#[async_trait]
impl UserStore for ControlPlaneUserStore {
    async fn is_empty(&self) -> Result<bool> {
        self.control_plane
            .users()
            .is_empty()
            .await
            .map_err(Self::map_error)
    }

    async fn create_user(&self, email: &str, password: &str, role: Role) -> Result<StoredUser> {
        let password_hash = hash_password(password)?;

        let user = tell_control::User::new(email, password_hash, role.as_str());

        self.control_plane
            .users()
            .create(&user)
            .await
            .map_err(Self::map_error)?;

        Ok(Self::to_stored_user(user))
    }

    async fn get_by_id(&self, id: &str) -> Result<Option<StoredUser>> {
        self.control_plane
            .users()
            .get_by_id(id)
            .await
            .map(|opt| opt.map(Self::to_stored_user))
            .map_err(Self::map_error)
    }

    async fn get_by_email(&self, email: &str) -> Result<Option<StoredUser>> {
        self.control_plane
            .users()
            .get_by_email(email)
            .await
            .map(|opt| opt.map(Self::to_stored_user))
            .map_err(Self::map_error)
    }

    async fn verify_credentials(&self, email: &str, password: &str) -> Result<Option<StoredUser>> {
        let user = match self.get_by_email(email).await? {
            Some(u) => u,
            None => return Ok(None),
        };

        if verify_password(password, &user.password_hash)? {
            // Update last login
            self.update_last_login(&user.id).await?;
            Ok(Some(user))
        } else {
            Ok(None)
        }
    }

    async fn update_last_login(&self, user_id: &str) -> Result<()> {
        self.control_plane
            .users()
            .update_last_login(user_id)
            .await
            .map_err(Self::map_error)
    }

    async fn list_all(&self) -> Result<Vec<StoredUser>> {
        self.control_plane
            .users()
            .list()
            .await
            .map(|users| users.into_iter().map(Self::to_stored_user).collect())
            .map_err(Self::map_error)
    }

    async fn update(&self, user: &StoredUser) -> Result<bool> {
        // Update role (email update would require a separate method in UserRepo)
        self.control_plane
            .users()
            .update_role(&user.id, user.role.as_str())
            .await
            .map_err(Self::map_error)?;
        Ok(true)
    }

    async fn update_password(&self, user_id: &str, new_password: &str) -> Result<bool> {
        let password_hash = crate::password::hash_password(new_password)?;
        self.control_plane
            .users()
            .update_password(user_id, &password_hash)
            .await
            .map_err(Self::map_error)?;
        Ok(true)
    }

    async fn delete(&self, user_id: &str) -> Result<bool> {
        self.control_plane
            .users()
            .delete(user_id)
            .await
            .map_err(Self::map_error)?;
        Ok(true)
    }

    async fn create_session(
        &self,
        user_id: &str,
        token: &str,
        ttl: Duration,
        ip_address: Option<&str>,
        user_agent: Option<&str>,
    ) -> Result<Session> {
        let expires_at = chrono::Utc::now() + ttl;
        let session = tell_control::Session::new(user_id, token, expires_at)
            .with_client_info(ip_address.map(String::from), user_agent.map(String::from));

        self.control_plane
            .users()
            .create_session(&session)
            .await
            .map_err(Self::map_error)?;

        Ok(Self::to_auth_session(session))
    }

    async fn validate_session(&self, token: &str) -> Result<Session> {
        let session = self
            .get_session_by_token(token)
            .await?
            .ok_or(AuthError::SessionNotFound)?;

        // Check if expired
        if session.expires_at < chrono::Utc::now() {
            // Clean up expired session
            let _ = self.delete_session_by_token(token).await;
            return Err(AuthError::SessionExpired);
        }

        Ok(session)
    }

    async fn get_session_by_token(&self, token: &str) -> Result<Option<Session>> {
        self.control_plane
            .users()
            .get_session_by_token(token)
            .await
            .map(|opt| opt.map(Self::to_auth_session))
            .map_err(Self::map_error)
    }

    async fn delete_session_by_token(&self, token: &str) -> Result<bool> {
        self.control_plane
            .users()
            .delete_session(token)
            .await
            .map_err(Self::map_error)
    }

    async fn delete_user_sessions(&self, user_id: &str) -> Result<u64> {
        self.control_plane
            .users()
            .delete_sessions_for_user(user_id)
            .await
            .map_err(Self::map_error)
    }

    async fn list_user_sessions(&self, user_id: &str) -> Result<Vec<Session>> {
        self.control_plane
            .users()
            .get_sessions_for_user(user_id)
            .await
            .map(|sessions| sessions.into_iter().map(Self::to_auth_session).collect())
            .map_err(Self::map_error)
    }

    async fn revoke_token(&self, token_id: &str, reason: Option<&str>) -> Result<()> {
        self.control_plane
            .users()
            .revoke_token(token_id, reason)
            .await
            .map_err(Self::map_error)
    }

    async fn is_token_revoked(&self, token_id: &str) -> Result<bool> {
        self.control_plane
            .users()
            .is_token_revoked(token_id)
            .await
            .map_err(Self::map_error)
    }

    async fn cleanup_expired_sessions(&self) -> Result<u64> {
        self.control_plane
            .users()
            .cleanup_expired_sessions()
            .await
            .map_err(Self::map_error)
    }

    async fn cleanup_old_revoked_tokens(&self) -> Result<u64> {
        self.control_plane
            .users()
            .cleanup_old_revoked_tokens()
            .await
            .map_err(Self::map_error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn setup() -> ControlPlaneUserStore {
        let cp = tell_control::ControlPlane::new_memory().await.unwrap();
        ControlPlaneUserStore::new(Arc::new(cp))
    }

    #[tokio::test]
    async fn test_create_and_get_user() {
        let store = setup().await;

        let user = store
            .create_user("test@example.com", "password123", Role::Editor)
            .await
            .unwrap();

        assert_eq!(user.email, "test@example.com");
        assert_eq!(user.role, Role::Editor);

        let fetched = store.get_by_email("test@example.com").await.unwrap();
        assert!(fetched.is_some());
        assert_eq!(fetched.unwrap().email, "test@example.com");
    }

    #[tokio::test]
    async fn test_verify_credentials() {
        let store = setup().await;

        store
            .create_user("test@example.com", "correct_password", Role::Viewer)
            .await
            .unwrap();

        // Correct password
        let result = store
            .verify_credentials("test@example.com", "correct_password")
            .await
            .unwrap();
        assert!(result.is_some());

        // Wrong password
        let result = store
            .verify_credentials("test@example.com", "wrong_password")
            .await
            .unwrap();
        assert!(result.is_none());

        // Non-existent user
        let result = store
            .verify_credentials("noone@example.com", "password")
            .await
            .unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_is_empty() {
        let store = setup().await;

        assert!(store.is_empty().await.unwrap());

        store
            .create_user("test@example.com", "password", Role::Admin)
            .await
            .unwrap();

        assert!(!store.is_empty().await.unwrap());
    }

    #[tokio::test]
    async fn test_session_crud() {
        let store = setup().await;

        let user = store
            .create_user("test@example.com", "password", Role::Viewer)
            .await
            .unwrap();

        // Create session
        let session = store
            .create_session(
                &user.id,
                "test-token-123",
                Duration::hours(24),
                Some("127.0.0.1"),
                Some("Mozilla/5.0"),
            )
            .await
            .unwrap();

        assert_eq!(session.user_id, user.id);
        assert_eq!(session.token, "test-token-123");

        // Validate session
        let validated = store.validate_session("test-token-123").await.unwrap();
        assert_eq!(validated.user_id, user.id);

        // Delete session
        assert!(
            store
                .delete_session_by_token("test-token-123")
                .await
                .unwrap()
        );
        assert!(
            store
                .get_session_by_token("test-token-123")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_expired_session() {
        let store = setup().await;

        let user = store
            .create_user("test@example.com", "password", Role::Viewer)
            .await
            .unwrap();

        // Create already-expired session
        store
            .create_session(&user.id, "expired-token", Duration::hours(-1), None, None)
            .await
            .unwrap();

        // Validate should fail
        let result = store.validate_session("expired-token").await;
        assert!(matches!(result, Err(AuthError::SessionExpired)));
    }

    #[tokio::test]
    async fn test_token_revocation() {
        let store = setup().await;

        assert!(!store.is_token_revoked("token-123").await.unwrap());

        store
            .revoke_token("token-123", Some("logout"))
            .await
            .unwrap();

        assert!(store.is_token_revoked("token-123").await.unwrap());
    }

    #[tokio::test]
    async fn test_duplicate_email_fails() {
        let store = setup().await;

        store
            .create_user("test@example.com", "password1", Role::Viewer)
            .await
            .unwrap();

        let result = store
            .create_user("test@example.com", "password2", Role::Editor)
            .await;

        assert!(result.is_err());
    }
}
