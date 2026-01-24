//! Local user store backed by SQLite
//!
//! Provides user management for self-hosted deployments.
//! Includes session management, token revocation, and workspace membership.

use std::path::Path;

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use sqlx::Row;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};
use tracing::{debug, info};

use crate::error::{AuthError, Result};
use crate::password::{hash_password, verify_password};
use crate::roles::{Permission, Role};
use crate::user::UserInfo;

/// Stored user record
#[derive(Debug, Clone)]
pub struct StoredUser {
    /// User ID (UUID)
    pub id: String,
    /// Email address (unique)
    pub email: String,
    /// Argon2 password hash
    pub password_hash: String,
    /// User's role
    pub role: Role,
    /// When the user was created
    pub created_at: DateTime<Utc>,
    /// When the user last logged in
    pub last_login: Option<DateTime<Utc>>,
}

impl StoredUser {
    /// Convert to UserInfo (for API responses)
    pub fn to_user_info(&self) -> UserInfo {
        UserInfo::with_role(&self.id, &self.email, self.role)
    }
}

/// Session record for stateful token validation
#[derive(Debug, Clone)]
pub struct Session {
    /// Session ID (UUID)
    pub id: String,
    /// User ID this session belongs to
    pub user_id: String,
    /// Token associated with this session (JWT or hash)
    pub token: String,
    /// When the session expires
    pub expires_at: DateTime<Utc>,
    /// When the session was created
    pub created_at: DateTime<Utc>,
    /// IP address of the client (optional)
    pub ip_address: Option<String>,
    /// User agent of the client (optional)
    pub user_agent: Option<String>,
}

/// Membership status in a workspace
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MembershipStatus {
    /// User has been invited but hasn't accepted yet
    Invited,
    /// User is an active member
    Active,
    /// User has been removed from the workspace
    Removed,
}

impl MembershipStatus {
    /// Parse status from string
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "invited" | "pending" => Some(Self::Invited),
            "active" => Some(Self::Active),
            "removed" | "inactive" => Some(Self::Removed),
            _ => None,
        }
    }

    /// Convert to string
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Invited => "invited",
            Self::Active => "active",
            Self::Removed => "removed",
        }
    }
}

impl std::fmt::Display for MembershipStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Workspace membership record
#[derive(Debug, Clone)]
pub struct WorkspaceMembership {
    /// User ID
    pub user_id: String,
    /// Workspace ID
    pub workspace_id: String,
    /// User's role in this workspace
    pub role: Role,
    /// Membership status
    pub status: MembershipStatus,
    /// When the membership was created
    pub created_at: DateTime<Utc>,
    /// When the membership was last updated
    pub updated_at: DateTime<Utc>,
}

impl WorkspaceMembership {
    /// Check if this is an active membership
    pub fn is_active(&self) -> bool {
        self.status == MembershipStatus::Active
    }

    /// Check if the member has a specific permission
    pub fn has_permission(&self, permission: Permission) -> bool {
        self.is_active() && self.role.has_permission(permission)
    }

    /// Check if the member is an admin (or higher)
    pub fn is_admin(&self) -> bool {
        self.is_active() && self.role >= Role::Admin
    }
}

// Re-export WorkspaceAccess from context module
pub use crate::context::WorkspaceAccess;

/// Local user store backed by SQLite
pub struct LocalUserStore {
    pool: SqlitePool,
}

impl LocalUserStore {
    /// Open or create a user store at the given path
    ///
    /// Creates the database and tables if they don't exist.
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();

        // Create parent directories if needed
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                AuthError::InvalidClaims(format!(
                    "failed to create directory {}: {}",
                    parent.display(),
                    e
                ))
            })?;
        }

        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true);

        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(options)
            .await
            .map_err(|e| AuthError::InvalidClaims(format!("failed to open database: {}", e)))?;

        let store = Self { pool };
        store.init_schema().await?;

        info!("User store opened at {}", path.display());
        Ok(store)
    }

    /// Create an in-memory store (for testing)
    #[cfg(test)]
    pub async fn in_memory() -> Result<Self> {
        let pool = SqlitePool::connect(":memory:")
            .await
            .map_err(|e| AuthError::InvalidClaims(format!("failed to create memory db: {}", e)))?;

        let store = Self { pool };
        store.init_schema().await?;
        Ok(store)
    }

    /// Initialize database schema
    async fn init_schema(&self) -> Result<()> {
        // Users table
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS users (
                id TEXT PRIMARY KEY,
                email TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL DEFAULT 'viewer',
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                last_login TEXT
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| AuthError::DatabaseError(format!("failed to create users table: {}", e)))?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_users_email ON users(email)")
            .execute(&self.pool)
            .await
            .map_err(|e| AuthError::DatabaseError(format!("failed to create index: {}", e)))?;

        // Sessions table (for stateful token validation)
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS auth_sessions (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                token TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                ip_address TEXT,
                user_agent TEXT,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| AuthError::DatabaseError(format!("failed to create sessions table: {}", e)))?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON auth_sessions(user_id)")
            .execute(&self.pool)
            .await
            .map_err(|e| AuthError::DatabaseError(format!("failed to create index: {}", e)))?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_sessions_token ON auth_sessions(token)")
            .execute(&self.pool)
            .await
            .map_err(|e| AuthError::DatabaseError(format!("failed to create index: {}", e)))?;

        // Revoked tokens table (for token invalidation)
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS revoked_tokens (
                token_id TEXT PRIMARY KEY,
                revoked_at TEXT NOT NULL DEFAULT (datetime('now')),
                reason TEXT
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| {
            AuthError::DatabaseError(format!("failed to create revoked_tokens table: {}", e))
        })?;

        // Workspace memberships table (for workspace isolation)
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS workspace_memberships (
                user_id TEXT NOT NULL,
                workspace_id TEXT NOT NULL,
                role TEXT NOT NULL DEFAULT 'viewer',
                status TEXT NOT NULL DEFAULT 'active',
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now')),
                PRIMARY KEY (user_id, workspace_id),
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| {
            AuthError::DatabaseError(format!(
                "failed to create workspace_memberships table: {}",
                e
            ))
        })?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_memberships_workspace ON workspace_memberships(workspace_id)",
        )
        .execute(&self.pool)
        .await
        .map_err(|e| AuthError::DatabaseError(format!("failed to create index: {}", e)))?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_memberships_user ON workspace_memberships(user_id)",
        )
        .execute(&self.pool)
        .await
        .map_err(|e| AuthError::DatabaseError(format!("failed to create index: {}", e)))?;

        debug!("User store schema initialized");
        Ok(())
    }

    /// Check if any users exist
    pub async fn is_empty(&self) -> Result<bool> {
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM users")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| AuthError::InvalidClaims(format!("failed to count users: {}", e)))?;

        Ok(count == 0)
    }

    /// Get user count
    pub async fn count(&self) -> Result<u64> {
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM users")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| AuthError::InvalidClaims(format!("failed to count users: {}", e)))?;

        Ok(count as u64)
    }

    /// Create a new user
    ///
    /// Returns error if email already exists.
    pub async fn create_user(&self, email: &str, password: &str, role: Role) -> Result<StoredUser> {
        let id = uuid::Uuid::new_v4().to_string();
        let password_hash = hash_password(password)?;
        let role_str = role.as_str();
        let created_at = Utc::now();

        sqlx::query(
            r#"
            INSERT INTO users (id, email, password_hash, role, created_at)
            VALUES (?, ?, ?, ?, ?)
            "#,
        )
        .bind(&id)
        .bind(email)
        .bind(&password_hash)
        .bind(role_str)
        .bind(created_at.to_rfc3339())
        .execute(&self.pool)
        .await
        .map_err(|e| {
            if e.to_string().contains("UNIQUE constraint") {
                AuthError::InvalidClaims(format!("user with email '{}' already exists", email))
            } else {
                AuthError::InvalidClaims(format!("failed to create user: {}", e))
            }
        })?;

        info!("Created user: {} ({})", email, role_str);

        Ok(StoredUser {
            id,
            email: email.to_string(),
            password_hash,
            role,
            created_at,
            last_login: None,
        })
    }

    /// Get user by email
    pub async fn get_by_email(&self, email: &str) -> Result<Option<StoredUser>> {
        let row = sqlx::query(
            r#"
            SELECT id, email, password_hash, role, created_at, last_login
            FROM users WHERE email = ?
            "#,
        )
        .bind(email)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| AuthError::InvalidClaims(format!("failed to query user: {}", e)))?;

        match row {
            Some(row) => {
                let role_str: String = row.get("role");
                let created_str: String = row.get("created_at");
                let last_login_str: Option<String> = row.get("last_login");

                Ok(Some(StoredUser {
                    id: row.get("id"),
                    email: row.get("email"),
                    password_hash: row.get("password_hash"),
                    role: Role::parse(&role_str).unwrap_or(Role::Viewer),
                    created_at: DateTime::parse_from_rfc3339(&created_str)
                        .map(|dt| dt.with_timezone(&Utc))
                        .unwrap_or_else(|_| Utc::now()),
                    last_login: last_login_str.and_then(|s| {
                        DateTime::parse_from_rfc3339(&s)
                            .map(|dt| dt.with_timezone(&Utc))
                            .ok()
                    }),
                }))
            }
            None => Ok(None),
        }
    }

    /// Verify email and password, returning user if valid
    pub async fn verify_credentials(
        &self,
        email: &str,
        password: &str,
    ) -> Result<Option<StoredUser>> {
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

    /// Update user's last login time
    async fn update_last_login(&self, user_id: &str) -> Result<()> {
        let now = Utc::now().to_rfc3339();

        sqlx::query("UPDATE users SET last_login = ? WHERE id = ?")
            .bind(&now)
            .bind(user_id)
            .execute(&self.pool)
            .await
            .map_err(|e| AuthError::InvalidClaims(format!("failed to update last_login: {}", e)))?;

        Ok(())
    }

    /// List all users
    pub async fn list_users(&self) -> Result<Vec<StoredUser>> {
        let rows = sqlx::query(
            r#"
            SELECT id, email, password_hash, role, created_at, last_login
            FROM users ORDER BY created_at
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| AuthError::InvalidClaims(format!("failed to list users: {}", e)))?;

        let mut users = Vec::with_capacity(rows.len());
        for row in rows {
            let role_str: String = row.get("role");
            let created_str: String = row.get("created_at");
            let last_login_str: Option<String> = row.get("last_login");

            users.push(StoredUser {
                id: row.get("id"),
                email: row.get("email"),
                password_hash: row.get("password_hash"),
                role: Role::parse(&role_str).unwrap_or(Role::Viewer),
                created_at: DateTime::parse_from_rfc3339(&created_str)
                    .map(|dt| dt.with_timezone(&Utc))
                    .unwrap_or_else(|_| Utc::now()),
                last_login: last_login_str.and_then(|s| {
                    DateTime::parse_from_rfc3339(&s)
                        .map(|dt| dt.with_timezone(&Utc))
                        .ok()
                }),
            });
        }

        Ok(users)
    }

    /// Delete a user by ID
    pub async fn delete_user(&self, user_id: &str) -> Result<bool> {
        let result = sqlx::query("DELETE FROM users WHERE id = ?")
            .bind(user_id)
            .execute(&self.pool)
            .await
            .map_err(|e| AuthError::InvalidClaims(format!("failed to delete user: {}", e)))?;

        Ok(result.rows_affected() > 0)
    }

    /// Update user's role
    pub async fn update_role(&self, user_id: &str, role: Role) -> Result<bool> {
        let result = sqlx::query("UPDATE users SET role = ? WHERE id = ?")
            .bind(role.as_str())
            .bind(user_id)
            .execute(&self.pool)
            .await
            .map_err(|e| AuthError::InvalidClaims(format!("failed to update role: {}", e)))?;

        Ok(result.rows_affected() > 0)
    }

    /// Get a user by ID
    pub async fn get_by_id(&self, user_id: &str) -> Result<Option<StoredUser>> {
        let row = sqlx::query(
            "SELECT id, email, password_hash, role, created_at, last_login FROM users WHERE id = ?",
        )
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| AuthError::InvalidClaims(format!("failed to get user: {}", e)))?;

        match row {
            Some(row) => {
                use sqlx::Row;
                let role_str: String = row.get("role");
                let created_str: String = row.get("created_at");
                let last_login_str: Option<String> = row.get("last_login");

                Ok(Some(StoredUser {
                    id: row.get("id"),
                    email: row.get("email"),
                    password_hash: row.get("password_hash"),
                    role: Role::parse(&role_str).unwrap_or(Role::Viewer),
                    created_at: DateTime::parse_from_rfc3339(&created_str)
                        .map(|dt| dt.with_timezone(&Utc))
                        .unwrap_or_else(|_| Utc::now()),
                    last_login: last_login_str.and_then(|s| {
                        DateTime::parse_from_rfc3339(&s)
                            .map(|dt| dt.with_timezone(&Utc))
                            .ok()
                    }),
                }))
            }
            None => Ok(None),
        }
    }

    /// Update a user's information (email, role)
    pub async fn update(&self, user: &StoredUser) -> Result<bool> {
        let result = sqlx::query("UPDATE users SET email = ?, role = ? WHERE id = ?")
            .bind(&user.email)
            .bind(user.role.as_str())
            .bind(&user.id)
            .execute(&self.pool)
            .await
            .map_err(|e| AuthError::InvalidClaims(format!("failed to update user: {}", e)))?;

        Ok(result.rows_affected() > 0)
    }

    /// Update a user's password
    pub async fn update_password(&self, user_id: &str, new_password: &str) -> Result<bool> {
        let password_hash = hash_password(new_password)?;

        let result = sqlx::query("UPDATE users SET password_hash = ? WHERE id = ?")
            .bind(&password_hash)
            .bind(user_id)
            .execute(&self.pool)
            .await
            .map_err(|e| AuthError::InvalidClaims(format!("failed to update password: {}", e)))?;

        Ok(result.rows_affected() > 0)
    }

    /// Alias for list_users (for API compatibility)
    pub async fn list_all(&self) -> Result<Vec<StoredUser>> {
        self.list_users().await
    }

    /// Alias for delete_user (for API compatibility)
    pub async fn delete(&self, user_id: &str) -> Result<bool> {
        self.delete_user(user_id).await
    }

    // =========================================================================
    // Session Management
    // =========================================================================

    /// Create a new session for a user
    ///
    /// Returns the session ID.
    pub async fn create_session(
        &self,
        user_id: &str,
        token: &str,
        expires_in: Duration,
        ip_address: Option<&str>,
        user_agent: Option<&str>,
    ) -> Result<Session> {
        let id = uuid::Uuid::new_v4().to_string();
        let now = Utc::now();
        let expires_at = now + expires_in;

        sqlx::query(
            r#"
            INSERT INTO auth_sessions (id, user_id, token, expires_at, created_at, ip_address, user_agent)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(&id)
        .bind(user_id)
        .bind(token)
        .bind(expires_at.to_rfc3339())
        .bind(now.to_rfc3339())
        .bind(ip_address)
        .bind(user_agent)
        .execute(&self.pool)
        .await
        .map_err(|e| AuthError::DatabaseError(format!("failed to create session: {}", e)))?;

        debug!("Created session {} for user {}", id, user_id);

        Ok(Session {
            id,
            user_id: user_id.to_string(),
            token: token.to_string(),
            expires_at,
            created_at: now,
            ip_address: ip_address.map(String::from),
            user_agent: user_agent.map(String::from),
        })
    }

    /// Get session by token
    pub async fn get_session_by_token(&self, token: &str) -> Result<Option<Session>> {
        let row = sqlx::query(
            r#"
            SELECT id, user_id, token, expires_at, created_at, ip_address, user_agent
            FROM auth_sessions WHERE token = ?
            "#,
        )
        .bind(token)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| AuthError::DatabaseError(format!("failed to query session: {}", e)))?;

        match row {
            Some(row) => {
                let expires_str: String = row.get("expires_at");
                let created_str: String = row.get("created_at");

                Ok(Some(Session {
                    id: row.get("id"),
                    user_id: row.get("user_id"),
                    token: row.get("token"),
                    expires_at: DateTime::parse_from_rfc3339(&expires_str)
                        .map(|dt| dt.with_timezone(&Utc))
                        .unwrap_or_else(|_| Utc::now()),
                    created_at: DateTime::parse_from_rfc3339(&created_str)
                        .map(|dt| dt.with_timezone(&Utc))
                        .unwrap_or_else(|_| Utc::now()),
                    ip_address: row.get("ip_address"),
                    user_agent: row.get("user_agent"),
                }))
            }
            None => Ok(None),
        }
    }

    /// Validate a session token
    ///
    /// Returns the session if valid, or appropriate error if not.
    pub async fn validate_session(&self, token: &str) -> Result<Session> {
        let session = self
            .get_session_by_token(token)
            .await?
            .ok_or(AuthError::SessionNotFound)?;

        // Check if expired
        if session.expires_at < Utc::now() {
            // Clean up expired session
            let _ = self.delete_session(&session.id).await;
            return Err(AuthError::SessionExpired);
        }

        Ok(session)
    }

    /// Delete a session by ID
    pub async fn delete_session(&self, session_id: &str) -> Result<bool> {
        let result = sqlx::query("DELETE FROM auth_sessions WHERE id = ?")
            .bind(session_id)
            .execute(&self.pool)
            .await
            .map_err(|e| AuthError::DatabaseError(format!("failed to delete session: {}", e)))?;

        Ok(result.rows_affected() > 0)
    }

    /// Delete a session by token
    pub async fn delete_session_by_token(&self, token: &str) -> Result<bool> {
        let result = sqlx::query("DELETE FROM auth_sessions WHERE token = ?")
            .bind(token)
            .execute(&self.pool)
            .await
            .map_err(|e| AuthError::DatabaseError(format!("failed to delete session: {}", e)))?;

        Ok(result.rows_affected() > 0)
    }

    /// Delete all sessions for a user (logout from all devices)
    pub async fn delete_user_sessions(&self, user_id: &str) -> Result<u64> {
        let result = sqlx::query("DELETE FROM auth_sessions WHERE user_id = ?")
            .bind(user_id)
            .execute(&self.pool)
            .await
            .map_err(|e| AuthError::DatabaseError(format!("failed to delete sessions: {}", e)))?;

        let count = result.rows_affected();
        if count > 0 {
            info!("Deleted {} sessions for user {}", count, user_id);
        }
        Ok(count)
    }

    /// List active sessions for a user
    pub async fn list_user_sessions(&self, user_id: &str) -> Result<Vec<Session>> {
        let rows = sqlx::query(
            r#"
            SELECT id, user_id, token, expires_at, created_at, ip_address, user_agent
            FROM auth_sessions
            WHERE user_id = ? AND expires_at > ?
            ORDER BY created_at DESC
            "#,
        )
        .bind(user_id)
        .bind(Utc::now().to_rfc3339())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| AuthError::DatabaseError(format!("failed to list sessions: {}", e)))?;

        let mut sessions = Vec::with_capacity(rows.len());
        for row in rows {
            let expires_str: String = row.get("expires_at");
            let created_str: String = row.get("created_at");

            sessions.push(Session {
                id: row.get("id"),
                user_id: row.get("user_id"),
                token: row.get("token"),
                expires_at: DateTime::parse_from_rfc3339(&expires_str)
                    .map(|dt| dt.with_timezone(&Utc))
                    .unwrap_or_else(|_| Utc::now()),
                created_at: DateTime::parse_from_rfc3339(&created_str)
                    .map(|dt| dt.with_timezone(&Utc))
                    .unwrap_or_else(|_| Utc::now()),
                ip_address: row.get("ip_address"),
                user_agent: row.get("user_agent"),
            });
        }

        Ok(sessions)
    }

    // =========================================================================
    // Token Revocation
    // =========================================================================

    /// Revoke a token by its ID (e.g., JWT jti claim)
    pub async fn revoke_token(&self, token_id: &str, reason: Option<&str>) -> Result<()> {
        sqlx::query(
            r#"
            INSERT OR IGNORE INTO revoked_tokens (token_id, revoked_at, reason)
            VALUES (?, ?, ?)
            "#,
        )
        .bind(token_id)
        .bind(Utc::now().to_rfc3339())
        .bind(reason)
        .execute(&self.pool)
        .await
        .map_err(|e| AuthError::DatabaseError(format!("failed to revoke token: {}", e)))?;

        info!("Revoked token: {}", token_id);
        Ok(())
    }

    /// Check if a token has been revoked
    pub async fn is_token_revoked(&self, token_id: &str) -> Result<bool> {
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM revoked_tokens WHERE token_id = ?")
                .bind(token_id)
                .fetch_one(&self.pool)
                .await
                .map_err(|e| {
                    AuthError::DatabaseError(format!("failed to check revocation: {}", e))
                })?;

        Ok(count > 0)
    }

    // =========================================================================
    // Cleanup
    // =========================================================================

    /// Clean up expired sessions
    ///
    /// Returns the number of sessions deleted.
    pub async fn cleanup_expired_sessions(&self) -> Result<u64> {
        let result = sqlx::query("DELETE FROM auth_sessions WHERE expires_at < ?")
            .bind(Utc::now().to_rfc3339())
            .execute(&self.pool)
            .await
            .map_err(|e| AuthError::DatabaseError(format!("failed to cleanup sessions: {}", e)))?;

        let count = result.rows_affected();
        if count > 0 {
            info!("Cleaned up {} expired sessions", count);
        }
        Ok(count)
    }

    /// Clean up old revocation entries (older than specified days)
    ///
    /// Returns the number of entries deleted.
    pub async fn cleanup_old_revocations(&self, days: i64) -> Result<u64> {
        let cutoff = Utc::now() - Duration::days(days);

        let result = sqlx::query("DELETE FROM revoked_tokens WHERE revoked_at < ?")
            .bind(cutoff.to_rfc3339())
            .execute(&self.pool)
            .await
            .map_err(|e| {
                AuthError::DatabaseError(format!("failed to cleanup revocations: {}", e))
            })?;

        let count = result.rows_affected();
        if count > 0 {
            info!("Cleaned up {} old revocation entries", count);
        }
        Ok(count)
    }

    /// Run all cleanup tasks
    pub async fn cleanup_all(&self) -> Result<()> {
        self.cleanup_expired_sessions().await?;
        self.cleanup_old_revocations(30).await?; // Keep revocations for 30 days
        Ok(())
    }

    // =========================================================================
    // Workspace Membership
    // =========================================================================

    /// Add a user to a workspace
    ///
    /// If the user is already a member, updates their role and status.
    pub async fn add_workspace_member(
        &self,
        user_id: &str,
        workspace_id: &str,
        role: Role,
        status: MembershipStatus,
    ) -> Result<WorkspaceMembership> {
        let now = Utc::now();

        sqlx::query(
            r#"
            INSERT INTO workspace_memberships (user_id, workspace_id, role, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (user_id, workspace_id) DO UPDATE SET
                role = excluded.role,
                status = excluded.status,
                updated_at = excluded.updated_at
            "#,
        )
        .bind(user_id)
        .bind(workspace_id)
        .bind(role.as_str())
        .bind(status.as_str())
        .bind(now.to_rfc3339())
        .bind(now.to_rfc3339())
        .execute(&self.pool)
        .await
        .map_err(|e| AuthError::DatabaseError(format!("failed to add workspace member: {}", e)))?;

        info!(
            "Added user {} to workspace {} with role {}",
            user_id, workspace_id, role
        );

        Ok(WorkspaceMembership {
            user_id: user_id.to_string(),
            workspace_id: workspace_id.to_string(),
            role,
            status,
            created_at: now,
            updated_at: now,
        })
    }

    /// Get a user's membership in a workspace
    pub async fn get_workspace_membership(
        &self,
        user_id: &str,
        workspace_id: &str,
    ) -> Result<Option<WorkspaceMembership>> {
        let row = sqlx::query(
            r#"
            SELECT user_id, workspace_id, role, status, created_at, updated_at
            FROM workspace_memberships
            WHERE user_id = ? AND workspace_id = ?
            "#,
        )
        .bind(user_id)
        .bind(workspace_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| AuthError::DatabaseError(format!("failed to get membership: {}", e)))?;

        match row {
            Some(row) => {
                let role_str: String = row.get("role");
                let status_str: String = row.get("status");
                let created_str: String = row.get("created_at");
                let updated_str: String = row.get("updated_at");

                Ok(Some(WorkspaceMembership {
                    user_id: row.get("user_id"),
                    workspace_id: row.get("workspace_id"),
                    role: Role::parse(&role_str).unwrap_or(Role::Viewer),
                    status: MembershipStatus::parse(&status_str)
                        .unwrap_or(MembershipStatus::Active),
                    created_at: DateTime::parse_from_rfc3339(&created_str)
                        .map(|dt| dt.with_timezone(&Utc))
                        .unwrap_or_else(|_| Utc::now()),
                    updated_at: DateTime::parse_from_rfc3339(&updated_str)
                        .map(|dt| dt.with_timezone(&Utc))
                        .unwrap_or_else(|_| Utc::now()),
                }))
            }
            None => Ok(None),
        }
    }

    /// Validate that a user has access to a workspace
    ///
    /// Returns a WorkspaceAccess context if the user is an active member.
    pub async fn validate_workspace_access(
        &self,
        user_id: &str,
        workspace_id: &str,
    ) -> Result<WorkspaceAccess> {
        let membership = self
            .get_workspace_membership(user_id, workspace_id)
            .await?
            .ok_or_else(|| {
                AuthError::WorkspaceAccessDenied(format!(
                    "user {} is not a member of workspace {}",
                    user_id, workspace_id
                ))
            })?;

        if !membership.is_active() {
            return Err(AuthError::WorkspaceAccessDenied(format!(
                "user {} membership in workspace {} is not active (status: {})",
                user_id, workspace_id, membership.status
            )));
        }

        Ok(WorkspaceAccess::new(workspace_id, membership.role))
    }

    /// Check if a user is a member of a workspace (any status)
    pub async fn is_workspace_member(&self, user_id: &str, workspace_id: &str) -> Result<bool> {
        let membership = self.get_workspace_membership(user_id, workspace_id).await?;
        Ok(membership.is_some_and(|m| m.is_active()))
    }

    /// Check if a user is an admin of a workspace
    pub async fn is_workspace_admin(&self, user_id: &str, workspace_id: &str) -> Result<bool> {
        let membership = self.get_workspace_membership(user_id, workspace_id).await?;
        Ok(membership.is_some_and(|m| m.is_admin()))
    }

    /// Update a user's role in a workspace
    pub async fn update_workspace_member_role(
        &self,
        user_id: &str,
        workspace_id: &str,
        role: Role,
    ) -> Result<bool> {
        let now = Utc::now();

        let result = sqlx::query(
            r#"
            UPDATE workspace_memberships
            SET role = ?, updated_at = ?
            WHERE user_id = ? AND workspace_id = ?
            "#,
        )
        .bind(role.as_str())
        .bind(now.to_rfc3339())
        .bind(user_id)
        .bind(workspace_id)
        .execute(&self.pool)
        .await
        .map_err(|e| AuthError::DatabaseError(format!("failed to update member role: {}", e)))?;

        if result.rows_affected() > 0 {
            info!(
                "Updated user {} role in workspace {} to {}",
                user_id, workspace_id, role
            );
        }

        Ok(result.rows_affected() > 0)
    }

    /// Remove a user from a workspace (soft delete - sets status to removed)
    pub async fn remove_workspace_member(&self, user_id: &str, workspace_id: &str) -> Result<bool> {
        let now = Utc::now();

        let result = sqlx::query(
            r#"
            UPDATE workspace_memberships
            SET status = 'removed', updated_at = ?
            WHERE user_id = ? AND workspace_id = ?
            "#,
        )
        .bind(now.to_rfc3339())
        .bind(user_id)
        .bind(workspace_id)
        .execute(&self.pool)
        .await
        .map_err(|e| AuthError::DatabaseError(format!("failed to remove member: {}", e)))?;

        if result.rows_affected() > 0 {
            info!("Removed user {} from workspace {}", user_id, workspace_id);
        }

        Ok(result.rows_affected() > 0)
    }

    /// Hard delete a workspace membership
    pub async fn delete_workspace_member(&self, user_id: &str, workspace_id: &str) -> Result<bool> {
        let result =
            sqlx::query("DELETE FROM workspace_memberships WHERE user_id = ? AND workspace_id = ?")
                .bind(user_id)
                .bind(workspace_id)
                .execute(&self.pool)
                .await
                .map_err(|e| AuthError::DatabaseError(format!("failed to delete member: {}", e)))?;

        Ok(result.rows_affected() > 0)
    }

    /// List all members of a workspace
    pub async fn list_workspace_members(
        &self,
        workspace_id: &str,
    ) -> Result<Vec<WorkspaceMembership>> {
        let rows = sqlx::query(
            r#"
            SELECT user_id, workspace_id, role, status, created_at, updated_at
            FROM workspace_memberships
            WHERE workspace_id = ? AND status = 'active'
            ORDER BY created_at
            "#,
        )
        .bind(workspace_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| AuthError::DatabaseError(format!("failed to list members: {}", e)))?;

        let mut members = Vec::with_capacity(rows.len());
        for row in rows {
            let role_str: String = row.get("role");
            let status_str: String = row.get("status");
            let created_str: String = row.get("created_at");
            let updated_str: String = row.get("updated_at");

            members.push(WorkspaceMembership {
                user_id: row.get("user_id"),
                workspace_id: row.get("workspace_id"),
                role: Role::parse(&role_str).unwrap_or(Role::Viewer),
                status: MembershipStatus::parse(&status_str).unwrap_or(MembershipStatus::Active),
                created_at: DateTime::parse_from_rfc3339(&created_str)
                    .map(|dt| dt.with_timezone(&Utc))
                    .unwrap_or_else(|_| Utc::now()),
                updated_at: DateTime::parse_from_rfc3339(&updated_str)
                    .map(|dt| dt.with_timezone(&Utc))
                    .unwrap_or_else(|_| Utc::now()),
            });
        }

        Ok(members)
    }

    /// List all workspaces a user is a member of
    pub async fn list_user_workspaces(&self, user_id: &str) -> Result<Vec<WorkspaceMembership>> {
        let rows = sqlx::query(
            r#"
            SELECT user_id, workspace_id, role, status, created_at, updated_at
            FROM workspace_memberships
            WHERE user_id = ? AND status = 'active'
            ORDER BY created_at
            "#,
        )
        .bind(user_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| AuthError::DatabaseError(format!("failed to list workspaces: {}", e)))?;

        let mut workspaces = Vec::with_capacity(rows.len());
        for row in rows {
            let role_str: String = row.get("role");
            let status_str: String = row.get("status");
            let created_str: String = row.get("created_at");
            let updated_str: String = row.get("updated_at");

            workspaces.push(WorkspaceMembership {
                user_id: row.get("user_id"),
                workspace_id: row.get("workspace_id"),
                role: Role::parse(&role_str).unwrap_or(Role::Viewer),
                status: MembershipStatus::parse(&status_str).unwrap_or(MembershipStatus::Active),
                created_at: DateTime::parse_from_rfc3339(&created_str)
                    .map(|dt| dt.with_timezone(&Utc))
                    .unwrap_or_else(|_| Utc::now()),
                updated_at: DateTime::parse_from_rfc3339(&updated_str)
                    .map(|dt| dt.with_timezone(&Utc))
                    .unwrap_or_else(|_| Utc::now()),
            });
        }

        Ok(workspaces)
    }

    /// Count members in a workspace
    pub async fn count_workspace_members(&self, workspace_id: &str) -> Result<u64> {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM workspace_memberships WHERE workspace_id = ? AND status = 'active'",
        )
        .bind(workspace_id)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| AuthError::DatabaseError(format!("failed to count members: {}", e)))?;

        Ok(count as u64)
    }
}

// Implement MembershipProvider trait for LocalUserStore
use crate::membership::{Membership, MembershipProvider, MembershipStatus as ProviderStatus};

#[async_trait::async_trait]
impl MembershipProvider for LocalUserStore {
    async fn get_membership(
        &self,
        user_id: &str,
        workspace_id: &str,
    ) -> Result<Option<Membership>> {
        let membership = self.get_workspace_membership(user_id, workspace_id).await?;

        Ok(membership.map(|m| Membership {
            user_id: m.user_id,
            workspace_id: m.workspace_id,
            role: m.role,
            status: match m.status {
                MembershipStatus::Invited => ProviderStatus::Invited,
                MembershipStatus::Active => ProviderStatus::Active,
                MembershipStatus::Removed => ProviderStatus::Removed,
            },
        }))
    }
}

// Implement UserStore trait for LocalUserStore
use crate::user_store_trait::UserStore;

#[async_trait::async_trait]
impl UserStore for LocalUserStore {
    async fn is_empty(&self) -> Result<bool> {
        LocalUserStore::is_empty(self).await
    }

    async fn create_user(&self, email: &str, password: &str, role: Role) -> Result<StoredUser> {
        LocalUserStore::create_user(self, email, password, role).await
    }

    async fn get_by_id(&self, id: &str) -> Result<Option<StoredUser>> {
        LocalUserStore::get_by_id(self, id).await
    }

    async fn get_by_email(&self, email: &str) -> Result<Option<StoredUser>> {
        LocalUserStore::get_by_email(self, email).await
    }

    async fn verify_credentials(&self, email: &str, password: &str) -> Result<Option<StoredUser>> {
        LocalUserStore::verify_credentials(self, email, password).await
    }

    async fn update_last_login(&self, user_id: &str) -> Result<()> {
        LocalUserStore::update_last_login(self, user_id).await
    }

    async fn list_all(&self) -> Result<Vec<StoredUser>> {
        LocalUserStore::list_users(self).await
    }

    async fn update(&self, user: &StoredUser) -> Result<bool> {
        LocalUserStore::update(self, user).await
    }

    async fn update_password(&self, user_id: &str, new_password: &str) -> Result<bool> {
        LocalUserStore::update_password(self, user_id, new_password).await
    }

    async fn delete(&self, user_id: &str) -> Result<bool> {
        LocalUserStore::delete(self, user_id).await
    }

    async fn create_session(
        &self,
        user_id: &str,
        token: &str,
        ttl: Duration,
        ip_address: Option<&str>,
        user_agent: Option<&str>,
    ) -> Result<Session> {
        LocalUserStore::create_session(self, user_id, token, ttl, ip_address, user_agent).await
    }

    async fn validate_session(&self, token: &str) -> Result<Session> {
        LocalUserStore::validate_session(self, token).await
    }

    async fn get_session_by_token(&self, token: &str) -> Result<Option<Session>> {
        LocalUserStore::get_session_by_token(self, token).await
    }

    async fn delete_session_by_token(&self, token: &str) -> Result<bool> {
        LocalUserStore::delete_session_by_token(self, token).await
    }

    async fn delete_user_sessions(&self, user_id: &str) -> Result<u64> {
        LocalUserStore::delete_user_sessions(self, user_id).await
    }

    async fn list_user_sessions(&self, user_id: &str) -> Result<Vec<Session>> {
        LocalUserStore::list_user_sessions(self, user_id).await
    }

    async fn revoke_token(&self, token_id: &str, reason: Option<&str>) -> Result<()> {
        LocalUserStore::revoke_token(self, token_id, reason).await
    }

    async fn is_token_revoked(&self, token_id: &str) -> Result<bool> {
        LocalUserStore::is_token_revoked(self, token_id).await
    }

    async fn cleanup_expired_sessions(&self) -> Result<u64> {
        LocalUserStore::cleanup_expired_sessions(self).await
    }

    async fn cleanup_old_revoked_tokens(&self) -> Result<u64> {
        LocalUserStore::cleanup_old_revocations(self, 30).await
    }

    async fn cleanup_all(&self) -> Result<()> {
        LocalUserStore::cleanup_all(self).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_and_get_user() {
        let store = LocalUserStore::in_memory().await.unwrap();

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
        let store = LocalUserStore::in_memory().await.unwrap();

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
    async fn test_duplicate_email() {
        let store = LocalUserStore::in_memory().await.unwrap();

        store
            .create_user("test@example.com", "password", Role::Viewer)
            .await
            .unwrap();

        let result = store
            .create_user("test@example.com", "other_password", Role::Admin)
            .await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("already exists"));
    }

    #[tokio::test]
    async fn test_is_empty() {
        let store = LocalUserStore::in_memory().await.unwrap();

        assert!(store.is_empty().await.unwrap());

        store
            .create_user("test@example.com", "password", Role::Admin)
            .await
            .unwrap();

        assert!(!store.is_empty().await.unwrap());
    }

    #[tokio::test]
    async fn test_count() {
        let store = LocalUserStore::in_memory().await.unwrap();

        assert_eq!(store.count().await.unwrap(), 0);

        store
            .create_user("user1@example.com", "password", Role::Viewer)
            .await
            .unwrap();
        store
            .create_user("user2@example.com", "password", Role::Editor)
            .await
            .unwrap();

        assert_eq!(store.count().await.unwrap(), 2);
    }

    #[tokio::test]
    async fn test_list_users() {
        let store = LocalUserStore::in_memory().await.unwrap();

        store
            .create_user("admin@example.com", "password", Role::Admin)
            .await
            .unwrap();
        store
            .create_user("user@example.com", "password", Role::Viewer)
            .await
            .unwrap();

        let users = store.list_users().await.unwrap();
        assert_eq!(users.len(), 2);
    }

    #[tokio::test]
    async fn test_delete_user() {
        let store = LocalUserStore::in_memory().await.unwrap();

        let user = store
            .create_user("test@example.com", "password", Role::Viewer)
            .await
            .unwrap();

        assert!(store.delete_user(&user.id).await.unwrap());
        assert!(
            store
                .get_by_email("test@example.com")
                .await
                .unwrap()
                .is_none()
        );

        // Delete non-existent
        assert!(!store.delete_user("non-existent").await.unwrap());
    }

    #[tokio::test]
    async fn test_update_role() {
        let store = LocalUserStore::in_memory().await.unwrap();

        let user = store
            .create_user("test@example.com", "password", Role::Viewer)
            .await
            .unwrap();

        assert!(store.update_role(&user.id, Role::Admin).await.unwrap());

        let updated = store
            .get_by_email("test@example.com")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(updated.role, Role::Admin);
    }

    // =========================================================================
    // Session Tests
    // =========================================================================

    #[tokio::test]
    async fn test_create_and_get_session() {
        let store = LocalUserStore::in_memory().await.unwrap();

        let user = store
            .create_user("test@example.com", "password", Role::Viewer)
            .await
            .unwrap();

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
        assert_eq!(session.ip_address, Some("127.0.0.1".to_string()));

        // Retrieve by token
        let fetched = store
            .get_session_by_token("test-token-123")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(fetched.id, session.id);
    }

    #[tokio::test]
    async fn test_validate_session() {
        let store = LocalUserStore::in_memory().await.unwrap();

        let user = store
            .create_user("test@example.com", "password", Role::Viewer)
            .await
            .unwrap();

        // Create valid session
        store
            .create_session(&user.id, "valid-token", Duration::hours(1), None, None)
            .await
            .unwrap();

        // Validate should succeed
        let session = store.validate_session("valid-token").await.unwrap();
        assert_eq!(session.user_id, user.id);

        // Non-existent session should fail
        let result = store.validate_session("non-existent").await;
        assert!(matches!(result, Err(AuthError::SessionNotFound)));
    }

    #[tokio::test]
    async fn test_validate_expired_session() {
        let store = LocalUserStore::in_memory().await.unwrap();

        let user = store
            .create_user("test@example.com", "password", Role::Viewer)
            .await
            .unwrap();

        // Create session that's already expired (negative duration)
        store
            .create_session(&user.id, "expired-token", Duration::hours(-1), None, None)
            .await
            .unwrap();

        // Validate should fail with SessionExpired
        let result = store.validate_session("expired-token").await;
        assert!(matches!(result, Err(AuthError::SessionExpired)));

        // Session should be cleaned up
        let fetched = store.get_session_by_token("expired-token").await.unwrap();
        assert!(fetched.is_none());
    }

    #[tokio::test]
    async fn test_delete_session() {
        let store = LocalUserStore::in_memory().await.unwrap();

        let user = store
            .create_user("test@example.com", "password", Role::Viewer)
            .await
            .unwrap();

        let session = store
            .create_session(&user.id, "delete-me", Duration::hours(1), None, None)
            .await
            .unwrap();

        assert!(store.delete_session(&session.id).await.unwrap());
        assert!(
            store
                .get_session_by_token("delete-me")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_delete_user_sessions() {
        let store = LocalUserStore::in_memory().await.unwrap();

        let user = store
            .create_user("test@example.com", "password", Role::Viewer)
            .await
            .unwrap();

        // Create multiple sessions
        store
            .create_session(&user.id, "token-1", Duration::hours(1), None, None)
            .await
            .unwrap();
        store
            .create_session(&user.id, "token-2", Duration::hours(1), None, None)
            .await
            .unwrap();
        store
            .create_session(&user.id, "token-3", Duration::hours(1), None, None)
            .await
            .unwrap();

        let sessions = store.list_user_sessions(&user.id).await.unwrap();
        assert_eq!(sessions.len(), 3);

        // Delete all
        let deleted = store.delete_user_sessions(&user.id).await.unwrap();
        assert_eq!(deleted, 3);

        let sessions = store.list_user_sessions(&user.id).await.unwrap();
        assert_eq!(sessions.len(), 0);
    }

    // =========================================================================
    // Revocation Tests
    // =========================================================================

    #[tokio::test]
    async fn test_revoke_token() {
        let store = LocalUserStore::in_memory().await.unwrap();

        // Token not revoked initially
        assert!(!store.is_token_revoked("token-123").await.unwrap());

        // Revoke it
        store
            .revoke_token("token-123", Some("user logout"))
            .await
            .unwrap();

        // Now it should be revoked
        assert!(store.is_token_revoked("token-123").await.unwrap());

        // Revoking again should not error (INSERT OR IGNORE)
        store.revoke_token("token-123", None).await.unwrap();
    }

    #[tokio::test]
    async fn test_cleanup_expired_sessions() {
        let store = LocalUserStore::in_memory().await.unwrap();

        let user = store
            .create_user("test@example.com", "password", Role::Viewer)
            .await
            .unwrap();

        // Create expired session
        store
            .create_session(&user.id, "expired", Duration::hours(-1), None, None)
            .await
            .unwrap();

        // Create valid session
        store
            .create_session(&user.id, "valid", Duration::hours(1), None, None)
            .await
            .unwrap();

        // Cleanup
        let cleaned = store.cleanup_expired_sessions().await.unwrap();
        assert_eq!(cleaned, 1);

        // Only valid session should remain
        assert!(
            store
                .get_session_by_token("expired")
                .await
                .unwrap()
                .is_none()
        );
        assert!(store.get_session_by_token("valid").await.unwrap().is_some());
    }

    // =========================================================================
    // Workspace Membership Tests
    // =========================================================================

    #[tokio::test]
    async fn test_add_workspace_member() {
        let store = LocalUserStore::in_memory().await.unwrap();

        let user = store
            .create_user("test@example.com", "password", Role::Viewer)
            .await
            .unwrap();

        // Add user to workspace
        let membership = store
            .add_workspace_member(
                &user.id,
                "workspace-1",
                Role::Editor,
                MembershipStatus::Active,
            )
            .await
            .unwrap();

        assert_eq!(membership.user_id, user.id);
        assert_eq!(membership.workspace_id, "workspace-1");
        assert_eq!(membership.role, Role::Editor);
        assert_eq!(membership.status, MembershipStatus::Active);
    }

    #[tokio::test]
    async fn test_get_workspace_membership() {
        let store = LocalUserStore::in_memory().await.unwrap();

        let user = store
            .create_user("test@example.com", "password", Role::Viewer)
            .await
            .unwrap();

        // No membership initially
        let membership = store
            .get_workspace_membership(&user.id, "workspace-1")
            .await
            .unwrap();
        assert!(membership.is_none());

        // Add membership
        store
            .add_workspace_member(
                &user.id,
                "workspace-1",
                Role::Admin,
                MembershipStatus::Active,
            )
            .await
            .unwrap();

        // Now it should exist
        let membership = store
            .get_workspace_membership(&user.id, "workspace-1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(membership.role, Role::Admin);
    }

    #[tokio::test]
    async fn test_validate_workspace_access() {
        let store = LocalUserStore::in_memory().await.unwrap();

        let user = store
            .create_user("test@example.com", "password", Role::Viewer)
            .await
            .unwrap();

        // No access initially
        let result = store
            .validate_workspace_access(&user.id, "workspace-1")
            .await;
        assert!(result.is_err());

        // Add as active member
        store
            .add_workspace_member(
                &user.id,
                "workspace-1",
                Role::Editor,
                MembershipStatus::Active,
            )
            .await
            .unwrap();

        // Now access should work
        let access = store
            .validate_workspace_access(&user.id, "workspace-1")
            .await
            .unwrap();
        assert_eq!(access.id, "workspace-1");
        assert_eq!(access.role, Role::Editor);
        assert!(access.can_create());
        assert!(!access.is_admin());
    }

    #[tokio::test]
    async fn test_validate_workspace_access_inactive() {
        let store = LocalUserStore::in_memory().await.unwrap();

        let user = store
            .create_user("test@example.com", "password", Role::Viewer)
            .await
            .unwrap();

        // Add as invited (not active)
        store
            .add_workspace_member(
                &user.id,
                "workspace-1",
                Role::Editor,
                MembershipStatus::Invited,
            )
            .await
            .unwrap();

        // Access should fail for inactive membership
        let result = store
            .validate_workspace_access(&user.id, "workspace-1")
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_workspace_isolation() {
        let store = LocalUserStore::in_memory().await.unwrap();

        let user1 = store
            .create_user("user1@example.com", "password", Role::Viewer)
            .await
            .unwrap();

        let user2 = store
            .create_user("user2@example.com", "password", Role::Viewer)
            .await
            .unwrap();

        // User1 has access to workspace-1
        store
            .add_workspace_member(
                &user1.id,
                "workspace-1",
                Role::Admin,
                MembershipStatus::Active,
            )
            .await
            .unwrap();

        // User2 has access to workspace-2
        store
            .add_workspace_member(
                &user2.id,
                "workspace-2",
                Role::Admin,
                MembershipStatus::Active,
            )
            .await
            .unwrap();

        // User1 can access workspace-1
        assert!(
            store
                .validate_workspace_access(&user1.id, "workspace-1")
                .await
                .is_ok()
        );

        // User1 cannot access workspace-2
        assert!(
            store
                .validate_workspace_access(&user1.id, "workspace-2")
                .await
                .is_err()
        );

        // User2 can access workspace-2
        assert!(
            store
                .validate_workspace_access(&user2.id, "workspace-2")
                .await
                .is_ok()
        );

        // User2 cannot access workspace-1
        assert!(
            store
                .validate_workspace_access(&user2.id, "workspace-1")
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_update_workspace_member_role() {
        let store = LocalUserStore::in_memory().await.unwrap();

        let user = store
            .create_user("test@example.com", "password", Role::Viewer)
            .await
            .unwrap();

        store
            .add_workspace_member(
                &user.id,
                "workspace-1",
                Role::Viewer,
                MembershipStatus::Active,
            )
            .await
            .unwrap();

        // Upgrade to admin
        assert!(
            store
                .update_workspace_member_role(&user.id, "workspace-1", Role::Admin)
                .await
                .unwrap()
        );

        let membership = store
            .get_workspace_membership(&user.id, "workspace-1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(membership.role, Role::Admin);
    }

    #[tokio::test]
    async fn test_remove_workspace_member() {
        let store = LocalUserStore::in_memory().await.unwrap();

        let user = store
            .create_user("test@example.com", "password", Role::Viewer)
            .await
            .unwrap();

        store
            .add_workspace_member(
                &user.id,
                "workspace-1",
                Role::Editor,
                MembershipStatus::Active,
            )
            .await
            .unwrap();

        // Remove (soft delete)
        assert!(
            store
                .remove_workspace_member(&user.id, "workspace-1")
                .await
                .unwrap()
        );

        // Membership still exists but is not active
        let membership = store
            .get_workspace_membership(&user.id, "workspace-1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(membership.status, MembershipStatus::Removed);

        // Access should be denied
        assert!(
            store
                .validate_workspace_access(&user.id, "workspace-1")
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_list_workspace_members() {
        let store = LocalUserStore::in_memory().await.unwrap();

        let user1 = store
            .create_user("user1@example.com", "password", Role::Viewer)
            .await
            .unwrap();
        let user2 = store
            .create_user("user2@example.com", "password", Role::Viewer)
            .await
            .unwrap();

        store
            .add_workspace_member(
                &user1.id,
                "workspace-1",
                Role::Admin,
                MembershipStatus::Active,
            )
            .await
            .unwrap();
        store
            .add_workspace_member(
                &user2.id,
                "workspace-1",
                Role::Editor,
                MembershipStatus::Active,
            )
            .await
            .unwrap();

        let members = store.list_workspace_members("workspace-1").await.unwrap();
        assert_eq!(members.len(), 2);
    }

    #[tokio::test]
    async fn test_list_user_workspaces() {
        let store = LocalUserStore::in_memory().await.unwrap();

        let user = store
            .create_user("test@example.com", "password", Role::Viewer)
            .await
            .unwrap();

        store
            .add_workspace_member(
                &user.id,
                "workspace-1",
                Role::Admin,
                MembershipStatus::Active,
            )
            .await
            .unwrap();
        store
            .add_workspace_member(
                &user.id,
                "workspace-2",
                Role::Editor,
                MembershipStatus::Active,
            )
            .await
            .unwrap();
        store
            .add_workspace_member(
                &user.id,
                "workspace-3",
                Role::Viewer,
                MembershipStatus::Invited,
            )
            .await
            .unwrap();

        // Only active memberships
        let workspaces = store.list_user_workspaces(&user.id).await.unwrap();
        assert_eq!(workspaces.len(), 2);
    }

    #[tokio::test]
    async fn test_count_workspace_members() {
        let store = LocalUserStore::in_memory().await.unwrap();

        let user1 = store
            .create_user("user1@example.com", "password", Role::Viewer)
            .await
            .unwrap();
        let user2 = store
            .create_user("user2@example.com", "password", Role::Viewer)
            .await
            .unwrap();

        assert_eq!(
            store.count_workspace_members("workspace-1").await.unwrap(),
            0
        );

        store
            .add_workspace_member(
                &user1.id,
                "workspace-1",
                Role::Admin,
                MembershipStatus::Active,
            )
            .await
            .unwrap();
        store
            .add_workspace_member(
                &user2.id,
                "workspace-1",
                Role::Editor,
                MembershipStatus::Active,
            )
            .await
            .unwrap();

        assert_eq!(
            store.count_workspace_members("workspace-1").await.unwrap(),
            2
        );
    }

    #[tokio::test]
    async fn test_workspace_access_permissions() {
        let store = LocalUserStore::in_memory().await.unwrap();

        let user = store
            .create_user("test@example.com", "password", Role::Viewer)
            .await
            .unwrap();

        // Add as viewer
        store
            .add_workspace_member(
                &user.id,
                "workspace-1",
                Role::Viewer,
                MembershipStatus::Active,
            )
            .await
            .unwrap();

        let access = store
            .validate_workspace_access(&user.id, "workspace-1")
            .await
            .unwrap();

        // Viewer cannot create or manage
        assert!(!access.can_create());
        assert!(!access.can_manage());
        assert!(!access.is_admin());

        // Upgrade to admin
        store
            .update_workspace_member_role(&user.id, "workspace-1", Role::Admin)
            .await
            .unwrap();

        let access = store
            .validate_workspace_access(&user.id, "workspace-1")
            .await
            .unwrap();

        // Admin can do everything
        assert!(access.can_create());
        assert!(access.can_manage());
        assert!(access.is_admin());
    }

    #[tokio::test]
    async fn test_membership_upsert() {
        let store = LocalUserStore::in_memory().await.unwrap();

        let user = store
            .create_user("test@example.com", "password", Role::Viewer)
            .await
            .unwrap();

        // First add
        store
            .add_workspace_member(
                &user.id,
                "workspace-1",
                Role::Viewer,
                MembershipStatus::Invited,
            )
            .await
            .unwrap();

        let membership = store
            .get_workspace_membership(&user.id, "workspace-1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(membership.role, Role::Viewer);
        assert_eq!(membership.status, MembershipStatus::Invited);

        // Second add (upsert) - should update role and status
        store
            .add_workspace_member(
                &user.id,
                "workspace-1",
                Role::Admin,
                MembershipStatus::Active,
            )
            .await
            .unwrap();

        let membership = store
            .get_workspace_membership(&user.id, "workspace-1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(membership.role, Role::Admin);
        assert_eq!(membership.status, MembershipStatus::Active);
    }

    #[tokio::test]
    async fn test_membership_status_parsing() {
        assert_eq!(
            MembershipStatus::parse("invited"),
            Some(MembershipStatus::Invited)
        );
        assert_eq!(
            MembershipStatus::parse("pending"),
            Some(MembershipStatus::Invited)
        );
        assert_eq!(
            MembershipStatus::parse("active"),
            Some(MembershipStatus::Active)
        );
        assert_eq!(
            MembershipStatus::parse("removed"),
            Some(MembershipStatus::Removed)
        );
        assert_eq!(
            MembershipStatus::parse("inactive"),
            Some(MembershipStatus::Removed)
        );
        assert_eq!(MembershipStatus::parse("invalid"), None);
    }
}
