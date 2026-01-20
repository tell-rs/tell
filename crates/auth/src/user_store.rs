//! Local user store backed by SQLite
//!
//! Provides user management for self-hosted deployments.

use std::path::Path;

use chrono::{DateTime, Utc};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};
use sqlx::Row;
use tracing::{debug, info};

use crate::error::{AuthError, Result};
use crate::password::{hash_password, verify_password};
use crate::roles::Role;
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
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS users (
                id TEXT PRIMARY KEY,
                email TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL DEFAULT 'viewer',
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                last_login TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| AuthError::InvalidClaims(format!("failed to create schema: {}", e)))?;

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
    pub async fn verify_credentials(&self, email: &str, password: &str) -> Result<Option<StoredUser>> {
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
        assert!(store.get_by_email("test@example.com").await.unwrap().is_none());

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

        let updated = store.get_by_email("test@example.com").await.unwrap().unwrap();
        assert_eq!(updated.role, Role::Admin);
    }
}
