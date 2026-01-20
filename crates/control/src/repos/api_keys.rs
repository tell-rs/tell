//! API key repository
//!
//! CRUD operations for user API keys.

use chrono::{DateTime, Utc};
use turso::Database;

use crate::error::Result;
use crate::models::UserApiKey;

/// Repository for managing user API keys
pub struct ApiKeyRepo<'a> {
    db: &'a Database,
}

impl<'a> ApiKeyRepo<'a> {
    /// Create a new API key repository
    pub fn new(db: &'a Database) -> Self {
        Self { db }
    }

    /// Create a new API key
    pub async fn create(&self, api_key: &UserApiKey) -> Result<()> {
        let conn = self.db.connect()?;

        let permissions_json = serde_json::to_string(&api_key.permissions)?;

        conn.execute(
            r#"
            INSERT INTO user_api_keys (
                id, user_id, workspace_id, name, description, key_hash, key_prefix,
                permissions, active, last_used_at, expires_at, created_at, updated_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
            "#,
            [
                api_key.id.as_str(),
                api_key.user_id.as_str(),
                api_key.workspace_id.as_str(),
                api_key.name.as_str(),
                api_key.description.as_deref().unwrap_or(""),
                api_key.key_hash.as_str(),
                api_key.key_prefix.as_str(),
                permissions_json.as_str(),
                if api_key.active { "1" } else { "0" },
                api_key
                    .last_used_at
                    .map(|dt| dt.to_rfc3339())
                    .unwrap_or_default()
                    .as_str(),
                api_key
                    .expires_at
                    .map(|dt| dt.to_rfc3339())
                    .unwrap_or_default()
                    .as_str(),
                api_key.created_at.to_rfc3339().as_str(),
                api_key.updated_at.to_rfc3339().as_str(),
            ],
        )
        .await?;

        Ok(())
    }

    /// Get an API key by ID
    pub async fn get_by_id(&self, id: &str) -> Result<Option<UserApiKey>> {
        let conn = self.db.connect()?;

        let mut rows = conn
            .query("SELECT * FROM user_api_keys WHERE id = ?1", [id])
            .await?;

        if let Some(row) = rows.next().await? {
            Ok(Some(row_to_api_key(&row)?))
        } else {
            Ok(None)
        }
    }

    /// Get an API key by its hash (for authentication)
    pub async fn get_by_hash(&self, key_hash: &str) -> Result<Option<UserApiKey>> {
        let conn = self.db.connect()?;

        let mut rows = conn
            .query(
                "SELECT * FROM user_api_keys WHERE key_hash = ?1 AND active = 1",
                [key_hash],
            )
            .await?;

        if let Some(row) = rows.next().await? {
            Ok(Some(row_to_api_key(&row)?))
        } else {
            Ok(None)
        }
    }

    /// List API keys for a user
    pub async fn list_for_user(&self, user_id: &str) -> Result<Vec<UserApiKey>> {
        let conn = self.db.connect()?;

        let mut rows = conn
            .query(
                "SELECT * FROM user_api_keys WHERE user_id = ?1 ORDER BY created_at DESC",
                [user_id],
            )
            .await?;

        let mut api_keys = Vec::new();
        while let Some(row) = rows.next().await? {
            api_keys.push(row_to_api_key(&row)?);
        }

        Ok(api_keys)
    }

    /// List API keys for a workspace
    pub async fn list_for_workspace(&self, workspace_id: &str) -> Result<Vec<UserApiKey>> {
        let conn = self.db.connect()?;

        let mut rows = conn
            .query(
                "SELECT * FROM user_api_keys WHERE workspace_id = ?1 ORDER BY created_at DESC",
                [workspace_id],
            )
            .await?;

        let mut api_keys = Vec::new();
        while let Some(row) = rows.next().await? {
            api_keys.push(row_to_api_key(&row)?);
        }

        Ok(api_keys)
    }

    /// List API keys for a user in a specific workspace
    pub async fn list_for_user_in_workspace(
        &self,
        user_id: &str,
        workspace_id: &str,
    ) -> Result<Vec<UserApiKey>> {
        let conn = self.db.connect()?;

        let mut rows = conn
            .query(
                "SELECT * FROM user_api_keys WHERE user_id = ?1 AND workspace_id = ?2 ORDER BY created_at DESC",
                [user_id, workspace_id],
            )
            .await?;

        let mut api_keys = Vec::new();
        while let Some(row) = rows.next().await? {
            api_keys.push(row_to_api_key(&row)?);
        }

        Ok(api_keys)
    }

    /// Update last used timestamp
    pub async fn touch(&self, id: &str) -> Result<()> {
        let conn = self.db.connect()?;
        let now = Utc::now().to_rfc3339();

        conn.execute(
            "UPDATE user_api_keys SET last_used_at = ?1 WHERE id = ?2",
            [now.as_str(), id],
        )
        .await?;

        Ok(())
    }

    /// Revoke (deactivate) an API key
    pub async fn revoke(&self, id: &str) -> Result<()> {
        let conn = self.db.connect()?;
        let now = Utc::now().to_rfc3339();

        conn.execute(
            "UPDATE user_api_keys SET active = 0, updated_at = ?1 WHERE id = ?2",
            [now.as_str(), id],
        )
        .await?;

        Ok(())
    }

    /// Delete an API key permanently
    pub async fn delete(&self, id: &str) -> Result<()> {
        let conn = self.db.connect()?;

        conn.execute("DELETE FROM user_api_keys WHERE id = ?1", [id])
            .await?;

        Ok(())
    }
}

/// Convert a database row to a UserApiKey
fn row_to_api_key(row: &turso::Row) -> Result<UserApiKey> {
    let empty = String::new();
    let default_perms = "[]".to_string();

    // Get values and bind them to extend their lifetime
    let v0 = row.get_value(0)?;
    let v1 = row.get_value(1)?;
    let v2 = row.get_value(2)?;
    let v3 = row.get_value(3)?;
    let v4 = row.get_value(4)?;
    let v5 = row.get_value(5)?;
    let v6 = row.get_value(6)?;
    let v7 = row.get_value(7)?;
    let v8 = row.get_value(8)?;
    let v9 = row.get_value(9)?;
    let v10 = row.get_value(10)?;
    let v11 = row.get_value(11)?;
    let v12 = row.get_value(12)?;

    let id = v0.as_text().unwrap_or(&empty).clone();
    let user_id = v1.as_text().unwrap_or(&empty).clone();
    let workspace_id = v2.as_text().unwrap_or(&empty).clone();
    let name = v3.as_text().unwrap_or(&empty).clone();
    let description_str = v4.as_text().unwrap_or(&empty);
    let description = if description_str.is_empty() {
        None
    } else {
        Some(description_str.clone())
    };
    let key_hash = v5.as_text().unwrap_or(&empty).clone();
    let key_prefix = v6.as_text().unwrap_or(&empty).clone();
    let permissions_json = v7.as_text().unwrap_or(&default_perms);
    let permissions: Vec<String> = serde_json::from_str(permissions_json).unwrap_or_default();
    let active = *v8.as_integer().unwrap_or(&0) == 1;
    let last_used_str = v9.as_text().unwrap_or(&empty);
    let last_used_at = parse_optional_datetime(last_used_str);
    let expires_str = v10.as_text().unwrap_or(&empty);
    let expires_at = parse_optional_datetime(expires_str);
    let created_at = parse_datetime(v11.as_text().unwrap_or(&empty))?;
    let updated_at = parse_datetime(v12.as_text().unwrap_or(&empty))?;

    Ok(UserApiKey {
        id,
        user_id,
        workspace_id,
        name,
        description,
        key_hash,
        key_prefix,
        permissions,
        active,
        last_used_at,
        expires_at,
        created_at,
        updated_at,
    })
}

fn parse_datetime(s: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|e| crate::error::ControlError::invalid("datetime", e.to_string()))
}

fn parse_optional_datetime(s: &str) -> Option<DateTime<Utc>> {
    if s.is_empty() {
        None
    } else {
        DateTime::parse_from_rfc3339(s)
            .map(|dt| dt.with_timezone(&Utc))
            .ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use turso::Builder;

    async fn setup_db() -> Database {
        let db = Builder::new_local(":memory:").build().await.unwrap();
        let conn = db.connect().unwrap();

        conn.execute(
            r#"
            CREATE TABLE IF NOT EXISTS user_api_keys (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                workspace_id TEXT NOT NULL,
                name TEXT NOT NULL,
                description TEXT,
                key_hash TEXT NOT NULL,
                key_prefix TEXT NOT NULL,
                permissions TEXT DEFAULT '[]',
                active INTEGER DEFAULT 1,
                last_used_at TEXT,
                expires_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            "#,
            (),
        )
        .await
        .unwrap();

        db
    }

    #[tokio::test]
    async fn test_create_and_get_api_key() {
        let db = setup_db().await;
        let repo = ApiKeyRepo::new(&db);

        let (key, api_key) = UserApiKey::new(
            "user_123",
            "workspace_456",
            "Test API Key",
            vec!["read".to_string(), "write".to_string()],
            None,
        );

        repo.create(&api_key).await.unwrap();

        let fetched = repo.get_by_id(&api_key.id).await.unwrap().unwrap();
        assert_eq!(fetched.name, "Test API Key");
        assert_eq!(fetched.user_id, "user_123");
        assert_eq!(fetched.workspace_id, "workspace_456");
        assert!(fetched.active);
        assert!(fetched.verify_key(&key));
    }

    #[tokio::test]
    async fn test_list_for_user() {
        let db = setup_db().await;
        let repo = ApiKeyRepo::new(&db);

        let (_, key1) = UserApiKey::new("user_1", "ws_1", "Key 1", vec![], None);
        let (_, key2) = UserApiKey::new("user_1", "ws_2", "Key 2", vec![], None);
        let (_, key3) = UserApiKey::new("user_2", "ws_1", "Key 3", vec![], None);

        repo.create(&key1).await.unwrap();
        repo.create(&key2).await.unwrap();
        repo.create(&key3).await.unwrap();

        let user1_keys = repo.list_for_user("user_1").await.unwrap();
        assert_eq!(user1_keys.len(), 2);

        let user2_keys = repo.list_for_user("user_2").await.unwrap();
        assert_eq!(user2_keys.len(), 1);
    }

    #[tokio::test]
    async fn test_revoke_api_key() {
        let db = setup_db().await;
        let repo = ApiKeyRepo::new(&db);

        let (_, api_key) = UserApiKey::new("user_1", "ws_1", "Test Key", vec![], None);
        repo.create(&api_key).await.unwrap();

        // Verify active
        let fetched = repo.get_by_id(&api_key.id).await.unwrap().unwrap();
        assert!(fetched.active);

        // Revoke
        repo.revoke(&api_key.id).await.unwrap();

        // Verify inactive
        let fetched = repo.get_by_id(&api_key.id).await.unwrap().unwrap();
        assert!(!fetched.active);
    }
}
