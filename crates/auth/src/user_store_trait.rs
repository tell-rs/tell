//! User store trait for abstracting storage backends
//!
//! This trait allows AuthService to work with different storage implementations:
//! - `LocalUserStore` (SQLite via SQLx) - original self-contained implementation
//! - `ControlPlane` (Turso) - unified control plane database

use async_trait::async_trait;
use chrono::Duration;

use crate::error::Result;
use crate::roles::Role;

// Re-export the concrete types from user_store module
pub use crate::user_store::{Session, StoredUser};

/// Abstract user store operations
///
/// Implement this trait to provide user storage for AuthService.
#[async_trait]
pub trait UserStore: Send + Sync {
    // =========================================================================
    // User Operations
    // =========================================================================

    /// Check if any users exist
    async fn is_empty(&self) -> Result<bool>;

    /// Create a new user
    async fn create_user(&self, email: &str, password: &str, role: Role) -> Result<StoredUser>;

    /// Get user by ID
    async fn get_by_id(&self, id: &str) -> Result<Option<StoredUser>>;

    /// Get user by email
    async fn get_by_email(&self, email: &str) -> Result<Option<StoredUser>>;

    /// Verify credentials and return user if valid
    async fn verify_credentials(&self, email: &str, password: &str) -> Result<Option<StoredUser>>;

    /// Update user's last login timestamp
    async fn update_last_login(&self, user_id: &str) -> Result<()>;

    /// List all users
    async fn list_all(&self) -> Result<Vec<StoredUser>>;

    /// Update user (email, role)
    async fn update(&self, user: &StoredUser) -> Result<bool>;

    /// Update user's password
    async fn update_password(&self, user_id: &str, new_password: &str) -> Result<bool>;

    /// Delete user by ID
    async fn delete(&self, user_id: &str) -> Result<bool>;

    // =========================================================================
    // Session Operations
    // =========================================================================

    /// Create a new session
    async fn create_session(
        &self,
        user_id: &str,
        token: &str,
        ttl: Duration,
        ip_address: Option<&str>,
        user_agent: Option<&str>,
    ) -> Result<Session>;

    /// Validate session by token (checks expiry)
    async fn validate_session(&self, token: &str) -> Result<Session>;

    /// Get session by token
    async fn get_session_by_token(&self, token: &str) -> Result<Option<Session>>;

    /// Delete session by token
    async fn delete_session_by_token(&self, token: &str) -> Result<bool>;

    /// Delete all sessions for user
    async fn delete_user_sessions(&self, user_id: &str) -> Result<u64>;

    /// List user's sessions
    async fn list_user_sessions(&self, user_id: &str) -> Result<Vec<Session>>;

    // =========================================================================
    // Token Revocation
    // =========================================================================

    /// Revoke a token
    async fn revoke_token(&self, token_id: &str, reason: Option<&str>) -> Result<()>;

    /// Check if token is revoked
    async fn is_token_revoked(&self, token_id: &str) -> Result<bool>;

    // =========================================================================
    // Cleanup
    // =========================================================================

    /// Cleanup expired sessions
    async fn cleanup_expired_sessions(&self) -> Result<u64>;

    /// Cleanup old revoked tokens
    async fn cleanup_old_revoked_tokens(&self) -> Result<u64>;

    /// Run all cleanup tasks
    async fn cleanup_all(&self) -> Result<()> {
        self.cleanup_expired_sessions().await?;
        self.cleanup_old_revoked_tokens().await?;
        Ok(())
    }
}
