//! Sharing repository
//!
//! CRUD operations for shared links.

use chrono::{DateTime, Utc};
use turso::Database;

use crate::error::{ControlError, Result};
use crate::models::{ResourceType, SharedLink};

/// Sharing repository
///
/// Operates on the control database (shared links are global).
pub struct SharingRepo<'a> {
    db: &'a Database,
}

impl<'a> SharingRepo<'a> {
    /// Create a new sharing repository
    pub fn new(db: &'a Database) -> Self {
        Self { db }
    }

    // =========================================================================
    // SharedLink CRUD
    // =========================================================================

    /// Create a new shared link
    pub async fn create(&self, link: &SharedLink) -> Result<()> {
        let conn = self.db.connect()?;

        let expires_at = link.expires_at.map(|dt| dt.to_rfc3339());
        let created_at = link.created_at.to_rfc3339();

        conn.execute(
            r#"
            INSERT INTO shared_links (hash, resource_type, resource_id, workspace_id, created_by, expires_at, created_at)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            "#,
            [
                link.hash.as_str(),
                link.resource_type.as_str(),
                link.resource_id.as_str(),
                link.workspace_id.as_str(),
                link.created_by.as_str(),
                expires_at.as_deref().unwrap_or(""),
                created_at.as_str(),
            ],
        )
        .await?;

        Ok(())
    }

    /// Get a shared link by hash
    pub async fn get_by_hash(&self, hash: &str) -> Result<Option<SharedLink>> {
        let conn = self.db.connect()?;

        let mut rows = conn
            .query("SELECT * FROM shared_links WHERE hash = ?1", [hash])
            .await?;

        if let Some(row) = rows.next().await? {
            Ok(Some(Self::row_to_link(&row)?))
        } else {
            Ok(None)
        }
    }

    /// Get shared link for a specific resource
    pub async fn get_for_resource(
        &self,
        resource_type: ResourceType,
        resource_id: &str,
    ) -> Result<Option<SharedLink>> {
        let conn = self.db.connect()?;

        let mut rows = conn
            .query(
                "SELECT * FROM shared_links WHERE resource_type = ?1 AND resource_id = ?2",
                [resource_type.as_str(), resource_id],
            )
            .await?;

        if let Some(row) = rows.next().await? {
            Ok(Some(Self::row_to_link(&row)?))
        } else {
            Ok(None)
        }
    }

    /// List all shared links for a workspace
    pub async fn list_for_workspace(&self, workspace_id: &str) -> Result<Vec<SharedLink>> {
        let conn = self.db.connect()?;

        let mut rows = conn
            .query(
                "SELECT * FROM shared_links WHERE workspace_id = ?1 ORDER BY created_at DESC",
                [workspace_id],
            )
            .await?;

        let mut links = Vec::new();
        while let Some(row) = rows.next().await? {
            links.push(Self::row_to_link(&row)?);
        }

        Ok(links)
    }

    /// List shared links created by a user
    pub async fn list_for_user(&self, user_id: &str) -> Result<Vec<SharedLink>> {
        let conn = self.db.connect()?;

        let mut rows = conn
            .query(
                "SELECT * FROM shared_links WHERE created_by = ?1 ORDER BY created_at DESC",
                [user_id],
            )
            .await?;

        let mut links = Vec::new();
        while let Some(row) = rows.next().await? {
            links.push(Self::row_to_link(&row)?);
        }

        Ok(links)
    }

    /// Delete a shared link by hash
    pub async fn delete(&self, hash: &str) -> Result<()> {
        let conn = self.db.connect()?;

        let affected = conn
            .execute("DELETE FROM shared_links WHERE hash = ?1", [hash])
            .await?;

        if affected == 0 {
            return Err(ControlError::not_found("shared_link", hash));
        }

        Ok(())
    }

    /// Delete shared link for a resource (revoke sharing)
    pub async fn delete_for_resource(
        &self,
        resource_type: ResourceType,
        resource_id: &str,
    ) -> Result<()> {
        let conn = self.db.connect()?;

        conn.execute(
            "DELETE FROM shared_links WHERE resource_type = ?1 AND resource_id = ?2",
            [resource_type.as_str(), resource_id],
        )
        .await?;

        // Note: We don't error if no rows deleted - resource may not have been shared

        Ok(())
    }

    /// Delete all expired links (cleanup)
    pub async fn delete_expired(&self) -> Result<u64> {
        let conn = self.db.connect()?;

        let now = Utc::now().to_rfc3339();

        let affected = conn
            .execute(
                "DELETE FROM shared_links WHERE expires_at != '' AND expires_at < ?1",
                [now.as_str()],
            )
            .await?;

        Ok(affected)
    }

    // =========================================================================
    // Row conversion helper
    // =========================================================================

    fn row_to_link(row: &turso::Row) -> Result<SharedLink> {
        let hash = row
            .get_value(0)?
            .as_text()
            .unwrap_or(&String::new())
            .clone();
        let resource_type_str = row
            .get_value(1)?
            .as_text()
            .unwrap_or(&String::new())
            .clone();
        let resource_id = row
            .get_value(2)?
            .as_text()
            .unwrap_or(&String::new())
            .clone();
        let workspace_id = row
            .get_value(3)?
            .as_text()
            .unwrap_or(&String::new())
            .clone();
        let created_by = row
            .get_value(4)?
            .as_text()
            .unwrap_or(&String::new())
            .clone();
        let expires_at_str = row.get_value(5)?.as_text().cloned();
        let created_at_str = row
            .get_value(6)?
            .as_text()
            .unwrap_or(&String::new())
            .clone();

        let resource_type =
            ResourceType::from_str(&resource_type_str).unwrap_or(ResourceType::Board);

        let expires_at = expires_at_str
            .filter(|s| !s.is_empty())
            .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
            .map(|dt| dt.with_timezone(&Utc));

        let created_at = DateTime::parse_from_rfc3339(&created_at_str)
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now());

        Ok(SharedLink {
            hash,
            resource_type,
            resource_id,
            workspace_id,
            created_by,
            expires_at,
            created_at,
        })
    }
}
