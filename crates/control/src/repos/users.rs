//! User repository
//!
//! CRUD operations for users, sessions, and token revocation.

use chrono::{DateTime, Utc};
use tracing::{debug, info};
use turso::Database;

use crate::error::{ControlError, Result};

// =============================================================================
// User Types
// =============================================================================

/// Stored user record
#[derive(Debug, Clone)]
pub struct User {
    /// Unique user ID (UUID)
    pub id: String,
    /// Email address (unique)
    pub email: String,
    /// Argon2 password hash
    pub password_hash: String,
    /// User's global role (viewer, editor, admin, platform)
    pub role: String,
    /// When the user was created
    pub created_at: DateTime<Utc>,
    /// Last login timestamp
    pub last_login: Option<DateTime<Utc>>,
}

impl User {
    /// Create a new user (generates UUID)
    pub fn new(
        email: impl Into<String>,
        password_hash: impl Into<String>,
        role: impl Into<String>,
    ) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            email: email.into(),
            password_hash: password_hash.into(),
            role: role.into(),
            created_at: Utc::now(),
            last_login: None,
        }
    }
}

/// User session record
#[derive(Debug, Clone)]
pub struct Session {
    /// Session ID (UUID)
    pub id: String,
    /// User ID
    pub user_id: String,
    /// JWT token
    pub token: String,
    /// When the session expires
    pub expires_at: DateTime<Utc>,
    /// When the session was created
    pub created_at: DateTime<Utc>,
    /// Client IP address
    pub ip_address: Option<String>,
    /// Client user agent
    pub user_agent: Option<String>,
}

impl Session {
    /// Create a new session
    pub fn new(
        user_id: impl Into<String>,
        token: impl Into<String>,
        expires_at: DateTime<Utc>,
    ) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            user_id: user_id.into(),
            token: token.into(),
            expires_at,
            created_at: Utc::now(),
            ip_address: None,
            user_agent: None,
        }
    }

    /// Add client info
    pub fn with_client_info(mut self, ip: Option<String>, ua: Option<String>) -> Self {
        self.ip_address = ip;
        self.user_agent = ua;
        self
    }

    /// Check if session is expired
    pub fn is_expired(&self) -> bool {
        self.expires_at < Utc::now()
    }
}

// =============================================================================
// User Repository
// =============================================================================

/// User repository
///
/// Operates on the control database.
pub struct UserRepo<'a> {
    db: &'a Database,
}

impl<'a> UserRepo<'a> {
    /// Create a new user repository
    pub fn new(db: &'a Database) -> Self {
        Self { db }
    }

    // =========================================================================
    // User CRUD
    // =========================================================================

    /// Create a new user
    pub async fn create(&self, user: &User) -> Result<()> {
        let conn = self.db.connect()?;

        let result = conn
            .execute(
                r#"
            INSERT INTO users (id, email, password_hash, role, created_at, last_login)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            "#,
                [
                    user.id.as_str(),
                    user.email.as_str(),
                    user.password_hash.as_str(),
                    user.role.as_str(),
                    user.created_at.to_rfc3339().as_str(),
                    user.last_login
                        .map(|d| d.to_rfc3339())
                        .as_deref()
                        .unwrap_or(""),
                ],
            )
            .await;

        if let Err(e) = result {
            if e.to_string().contains("UNIQUE constraint") {
                return Err(ControlError::already_exists("user", &user.email));
            }
            return Err(e.into());
        }

        info!(email = %user.email, role = %user.role, "Created user");
        Ok(())
    }

    /// Get user by ID
    pub async fn get_by_id(&self, id: &str) -> Result<Option<User>> {
        let conn = self.db.connect()?;

        let mut rows = conn
            .query("SELECT * FROM users WHERE id = ?1", [id])
            .await?;

        if let Some(row) = rows.next().await? {
            Ok(Some(Self::row_to_user(&row)?))
        } else {
            Ok(None)
        }
    }

    /// Get user by email
    pub async fn get_by_email(&self, email: &str) -> Result<Option<User>> {
        let conn = self.db.connect()?;

        let mut rows = conn
            .query("SELECT * FROM users WHERE email = ?1", [email])
            .await?;

        if let Some(row) = rows.next().await? {
            Ok(Some(Self::row_to_user(&row)?))
        } else {
            Ok(None)
        }
    }

    /// Update user's last login timestamp
    pub async fn update_last_login(&self, user_id: &str) -> Result<()> {
        let conn = self.db.connect()?;
        let now = Utc::now().to_rfc3339();

        conn.execute(
            "UPDATE users SET last_login = ?1 WHERE id = ?2",
            [now.as_str(), user_id],
        )
        .await?;

        Ok(())
    }

    /// Update user's password
    pub async fn update_password(&self, user_id: &str, password_hash: &str) -> Result<()> {
        let conn = self.db.connect()?;

        let result = conn
            .execute(
                "UPDATE users SET password_hash = ?1 WHERE id = ?2",
                [password_hash, user_id],
            )
            .await?;

        if result == 0 {
            return Err(ControlError::not_found("user", user_id));
        }

        Ok(())
    }

    /// Update user's role
    pub async fn update_role(&self, user_id: &str, role: &str) -> Result<()> {
        let conn = self.db.connect()?;

        let result = conn
            .execute("UPDATE users SET role = ?1 WHERE id = ?2", [role, user_id])
            .await?;

        if result == 0 {
            return Err(ControlError::not_found("user", user_id));
        }

        Ok(())
    }

    /// Delete user
    pub async fn delete(&self, id: &str) -> Result<()> {
        let conn = self.db.connect()?;

        let result = conn
            .execute("DELETE FROM users WHERE id = ?1", [id])
            .await?;

        if result == 0 {
            return Err(ControlError::not_found("user", id));
        }

        debug!(id, "Deleted user");
        Ok(())
    }

    /// Check if any users exist
    pub async fn is_empty(&self) -> Result<bool> {
        let conn = self.db.connect()?;

        let mut rows = conn
            .query("SELECT COUNT(*) as count FROM users", ())
            .await?;

        if let Some(row) = rows.next().await? {
            let count: i64 = row.get(0)?;
            Ok(count == 0)
        } else {
            Ok(true)
        }
    }

    /// Count users
    pub async fn count(&self) -> Result<u64> {
        let conn = self.db.connect()?;

        let mut rows = conn
            .query("SELECT COUNT(*) as count FROM users", ())
            .await?;

        if let Some(row) = rows.next().await? {
            let count: i64 = row.get(0)?;
            Ok(count as u64)
        } else {
            Ok(0)
        }
    }

    /// List all users
    pub async fn list(&self) -> Result<Vec<User>> {
        let conn = self.db.connect()?;

        let mut rows = conn
            .query("SELECT * FROM users ORDER BY created_at DESC", ())
            .await?;

        let mut users = Vec::new();
        while let Some(row) = rows.next().await? {
            users.push(Self::row_to_user(&row)?);
        }

        Ok(users)
    }

    // =========================================================================
    // Session Management
    // =========================================================================

    /// Create a new session
    pub async fn create_session(&self, session: &Session) -> Result<()> {
        let conn = self.db.connect()?;

        conn.execute(
            r#"
            INSERT INTO auth_sessions (id, user_id, token, expires_at, created_at, ip_address, user_agent)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            "#,
            [
                session.id.as_str(),
                session.user_id.as_str(),
                session.token.as_str(),
                session.expires_at.to_rfc3339().as_str(),
                session.created_at.to_rfc3339().as_str(),
                session.ip_address.as_deref().unwrap_or(""),
                session.user_agent.as_deref().unwrap_or(""),
            ],
        )
        .await?;

        debug!(user_id = %session.user_id, "Created session");
        Ok(())
    }

    /// Get session by token
    pub async fn get_session_by_token(&self, token: &str) -> Result<Option<Session>> {
        let conn = self.db.connect()?;

        let mut rows = conn
            .query("SELECT * FROM auth_sessions WHERE token = ?1", [token])
            .await?;

        if let Some(row) = rows.next().await? {
            Ok(Some(Self::row_to_session(&row)?))
        } else {
            Ok(None)
        }
    }

    /// Get sessions for user
    pub async fn get_sessions_for_user(&self, user_id: &str) -> Result<Vec<Session>> {
        let conn = self.db.connect()?;

        let mut rows = conn
            .query(
                "SELECT * FROM auth_sessions WHERE user_id = ?1 ORDER BY created_at DESC",
                [user_id],
            )
            .await?;

        let mut sessions = Vec::new();
        while let Some(row) = rows.next().await? {
            sessions.push(Self::row_to_session(&row)?);
        }

        Ok(sessions)
    }

    /// Delete session by token
    pub async fn delete_session(&self, token: &str) -> Result<bool> {
        let conn = self.db.connect()?;

        let result = conn
            .execute("DELETE FROM auth_sessions WHERE token = ?1", [token])
            .await?;

        Ok(result > 0)
    }

    /// Delete all sessions for user
    pub async fn delete_sessions_for_user(&self, user_id: &str) -> Result<u64> {
        let conn = self.db.connect()?;

        let result = conn
            .execute("DELETE FROM auth_sessions WHERE user_id = ?1", [user_id])
            .await?;

        Ok(result)
    }

    /// Cleanup expired sessions
    pub async fn cleanup_expired_sessions(&self) -> Result<u64> {
        let conn = self.db.connect()?;
        let now = Utc::now().to_rfc3339();

        let result = conn
            .execute(
                "DELETE FROM auth_sessions WHERE expires_at < ?1",
                [now.as_str()],
            )
            .await?;

        if result > 0 {
            debug!(count = result, "Cleaned up expired sessions");
        }

        Ok(result)
    }

    // =========================================================================
    // Token Revocation
    // =========================================================================

    /// Revoke a token
    pub async fn revoke_token(&self, token_id: &str, reason: Option<&str>) -> Result<()> {
        let conn = self.db.connect()?;
        let now = Utc::now().to_rfc3339();

        conn.execute(
            r#"
            INSERT OR REPLACE INTO revoked_tokens (token_id, revoked_at, reason)
            VALUES (?1, ?2, ?3)
            "#,
            [token_id, now.as_str(), reason.unwrap_or("")],
        )
        .await?;

        debug!(token_id, "Revoked token");
        Ok(())
    }

    /// Check if token is revoked
    pub async fn is_token_revoked(&self, token_id: &str) -> Result<bool> {
        let conn = self.db.connect()?;

        let mut rows = conn
            .query(
                "SELECT 1 FROM revoked_tokens WHERE token_id = ?1",
                [token_id],
            )
            .await?;

        Ok(rows.next().await?.is_some())
    }

    /// Cleanup old revoked tokens (older than 30 days)
    pub async fn cleanup_old_revoked_tokens(&self) -> Result<u64> {
        let conn = self.db.connect()?;
        let cutoff = (Utc::now() - chrono::Duration::days(30)).to_rfc3339();

        let result = conn
            .execute(
                "DELETE FROM revoked_tokens WHERE revoked_at < ?1",
                [cutoff.as_str()],
            )
            .await?;

        if result > 0 {
            debug!(count = result, "Cleaned up old revoked tokens");
        }

        Ok(result)
    }

    // =========================================================================
    // Row Converters
    // =========================================================================

    fn row_to_user(row: &turso::Row) -> Result<User> {
        let id: String = row.get(0)?;
        let email: String = row.get(1)?;
        let password_hash: String = row.get(2)?;
        let role: String = row.get(3)?;
        let created_at_str: String = row.get(4)?;
        let last_login_str: String = row.get(5)?;

        let created_at = DateTime::parse_from_rfc3339(&created_at_str)
            .map(|dt| dt.with_timezone(&Utc))
            .map_err(|e| ControlError::invalid("created_at", e.to_string()))?;

        let last_login = if last_login_str.is_empty() {
            None
        } else {
            Some(
                DateTime::parse_from_rfc3339(&last_login_str)
                    .map(|dt| dt.with_timezone(&Utc))
                    .map_err(|e| ControlError::invalid("last_login", e.to_string()))?,
            )
        };

        Ok(User {
            id,
            email,
            password_hash,
            role,
            created_at,
            last_login,
        })
    }

    fn row_to_session(row: &turso::Row) -> Result<Session> {
        let id: String = row.get(0)?;
        let user_id: String = row.get(1)?;
        let token: String = row.get(2)?;
        let expires_at_str: String = row.get(3)?;
        let created_at_str: String = row.get(4)?;
        let ip_address: String = row.get(5)?;
        let user_agent: String = row.get(6)?;

        let expires_at = DateTime::parse_from_rfc3339(&expires_at_str)
            .map(|dt| dt.with_timezone(&Utc))
            .map_err(|e| ControlError::invalid("expires_at", e.to_string()))?;

        let created_at = DateTime::parse_from_rfc3339(&created_at_str)
            .map(|dt| dt.with_timezone(&Utc))
            .map_err(|e| ControlError::invalid("created_at", e.to_string()))?;

        Ok(Session {
            id,
            user_id,
            token,
            expires_at,
            created_at,
            ip_address: if ip_address.is_empty() {
                None
            } else {
                Some(ip_address)
            },
            user_agent: if user_agent.is_empty() {
                None
            } else {
                Some(user_agent)
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ControlPlane;

    async fn setup() -> ControlPlane {
        ControlPlane::new_memory().await.unwrap()
    }

    #[tokio::test]
    async fn test_create_and_get_user() {
        let cp = setup().await;
        let repo = UserRepo::new(cp.control_db());

        let user = User::new("test@example.com", "hashed_password", "editor");
        repo.create(&user).await.unwrap();

        let found = repo.get_by_email("test@example.com").await.unwrap();
        assert!(found.is_some());

        let found = found.unwrap();
        assert_eq!(found.email, "test@example.com");
        assert_eq!(found.role, "editor");
    }

    #[tokio::test]
    async fn test_duplicate_email_fails() {
        let cp = setup().await;
        let repo = UserRepo::new(cp.control_db());

        let user1 = User::new("test@example.com", "hash1", "viewer");
        repo.create(&user1).await.unwrap();

        let user2 = User::new("test@example.com", "hash2", "editor");
        let result = repo.create(&user2).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_is_empty() {
        let cp = setup().await;
        let repo = UserRepo::new(cp.control_db());

        assert!(repo.is_empty().await.unwrap());

        let user = User::new("test@example.com", "hash", "viewer");
        repo.create(&user).await.unwrap();

        assert!(!repo.is_empty().await.unwrap());
    }

    #[tokio::test]
    async fn test_session_crud() {
        let cp = setup().await;
        let repo = UserRepo::new(cp.control_db());

        // Create user first
        let user = User::new("test@example.com", "hash", "viewer");
        repo.create(&user).await.unwrap();

        // Create session
        let expires = Utc::now() + chrono::Duration::hours(24);
        let session = Session::new(&user.id, "jwt_token_here", expires);
        repo.create_session(&session).await.unwrap();

        // Find session
        let found = repo.get_session_by_token("jwt_token_here").await.unwrap();
        assert!(found.is_some());
        assert!(!found.unwrap().is_expired());

        // Delete session
        let deleted = repo.delete_session("jwt_token_here").await.unwrap();
        assert!(deleted);

        let found = repo.get_session_by_token("jwt_token_here").await.unwrap();
        assert!(found.is_none());
    }

    #[tokio::test]
    async fn test_token_revocation() {
        let cp = setup().await;
        let repo = UserRepo::new(cp.control_db());

        assert!(!repo.is_token_revoked("token-123").await.unwrap());

        repo.revoke_token("token-123", Some("logout"))
            .await
            .unwrap();

        assert!(repo.is_token_revoked("token-123").await.unwrap());
    }

    #[tokio::test]
    async fn test_cleanup_expired_sessions() {
        let cp = setup().await;
        let repo = UserRepo::new(cp.control_db());

        // Create user
        let user = User::new("test@example.com", "hash", "viewer");
        repo.create(&user).await.unwrap();

        // Create expired session
        let expired = Utc::now() - chrono::Duration::hours(1);
        let session = Session::new(&user.id, "expired_token", expired);
        repo.create_session(&session).await.unwrap();

        // Create valid session
        let valid = Utc::now() + chrono::Duration::hours(24);
        let session2 = Session::new(&user.id, "valid_token", valid);
        repo.create_session(&session2).await.unwrap();

        // Cleanup
        let cleaned = repo.cleanup_expired_sessions().await.unwrap();
        assert_eq!(cleaned, 1);

        // Verify
        assert!(
            repo.get_session_by_token("expired_token")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            repo.get_session_by_token("valid_token")
                .await
                .unwrap()
                .is_some()
        );
    }
}
