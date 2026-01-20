//! Workspace repository
//!
//! CRUD operations for workspaces and memberships.

use chrono::{DateTime, Utc};
use turso::Database;

use crate::error::{ControlError, Result};
use crate::models::{
    MemberRole, MemberStatus, Workspace, WorkspaceMembership, WorkspaceSettings, WorkspaceStatus,
};

/// Workspace repository
pub struct WorkspaceRepo<'a> {
    db: &'a Database,
}

impl<'a> WorkspaceRepo<'a> {
    /// Create a new workspace repository
    pub fn new(db: &'a Database) -> Self {
        Self { db }
    }

    // =========================================================================
    // Workspace CRUD
    // =========================================================================

    /// Create a new workspace
    pub async fn create(&self, workspace: &Workspace) -> Result<()> {
        let conn = self.db.connect()?;

        let settings_json = serde_json::to_string(&workspace.settings)?;
        let created_at = workspace.created_at.to_rfc3339();
        let updated_at = workspace.updated_at.to_rfc3339();

        conn.execute(
            r#"
            INSERT INTO workspaces (id, name, slug, clickhouse_database, settings, status, created_at, updated_at)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            "#,
            [
                &workspace.id,
                &workspace.name,
                &workspace.slug,
                &workspace.clickhouse_database,
                &settings_json,
                workspace.status.as_str(),
                &created_at,
                &updated_at,
            ],
        )
        .await?;

        Ok(())
    }

    /// Get a workspace by ID
    pub async fn get_by_id(&self, id: &str) -> Result<Option<Workspace>> {
        let conn = self.db.connect()?;

        let mut rows = conn
            .query("SELECT * FROM workspaces WHERE id = ?1", [id])
            .await?;

        if let Some(row) = rows.next().await? {
            Ok(Some(Self::row_to_workspace(&row)?))
        } else {
            Ok(None)
        }
    }

    /// Get a workspace by slug
    pub async fn get_by_slug(&self, slug: &str) -> Result<Option<Workspace>> {
        let conn = self.db.connect()?;

        let mut rows = conn
            .query("SELECT * FROM workspaces WHERE slug = ?1", [slug])
            .await?;

        if let Some(row) = rows.next().await? {
            Ok(Some(Self::row_to_workspace(&row)?))
        } else {
            Ok(None)
        }
    }

    /// List all workspaces
    pub async fn list_all(&self) -> Result<Vec<Workspace>> {
        let conn = self.db.connect()?;

        let mut rows = conn
            .query("SELECT * FROM workspaces ORDER BY created_at DESC", ())
            .await?;

        let mut workspaces = Vec::new();
        while let Some(row) = rows.next().await? {
            workspaces.push(Self::row_to_workspace(&row)?);
        }

        Ok(workspaces)
    }

    /// List workspaces for a user (via membership)
    pub async fn list_for_user(&self, user_id: &str) -> Result<Vec<Workspace>> {
        let conn = self.db.connect()?;

        let mut rows = conn
            .query(
                r#"
                SELECT w.* FROM workspaces w
                INNER JOIN workspace_memberships m ON w.id = m.workspace_id
                WHERE m.user_id = ?1 AND m.status = 'active'
                ORDER BY w.created_at DESC
                "#,
                [user_id],
            )
            .await?;

        let mut workspaces = Vec::new();
        while let Some(row) = rows.next().await? {
            workspaces.push(Self::row_to_workspace(&row)?);
        }

        Ok(workspaces)
    }

    /// Update a workspace
    pub async fn update(&self, workspace: &Workspace) -> Result<()> {
        let conn = self.db.connect()?;

        let settings_json = serde_json::to_string(&workspace.settings)?;
        let updated_at = Utc::now().to_rfc3339();

        let affected = conn
            .execute(
                r#"
                UPDATE workspaces
                SET name = ?1, slug = ?2, settings = ?3, status = ?4, updated_at = ?5
                WHERE id = ?6
                "#,
                [
                    &workspace.name,
                    &workspace.slug,
                    &settings_json,
                    workspace.status.as_str(),
                    &updated_at,
                    &workspace.id,
                ],
            )
            .await?;

        if affected == 0 {
            return Err(ControlError::not_found("workspace", &workspace.id));
        }

        Ok(())
    }

    /// Delete a workspace
    pub async fn delete(&self, id: &str) -> Result<()> {
        let conn = self.db.connect()?;

        let affected = conn
            .execute("DELETE FROM workspaces WHERE id = ?1", [id])
            .await?;

        if affected == 0 {
            return Err(ControlError::not_found("workspace", id));
        }

        Ok(())
    }

    // =========================================================================
    // Membership CRUD
    // =========================================================================

    /// Add a member to a workspace
    pub async fn add_member(&self, membership: &WorkspaceMembership) -> Result<()> {
        let conn = self.db.connect()?;

        let created_at = membership.created_at.to_rfc3339();

        conn.execute(
            r#"
            INSERT INTO workspace_memberships (user_id, workspace_id, role, status, created_at)
            VALUES (?1, ?2, ?3, ?4, ?5)
            "#,
            [
                &membership.user_id,
                &membership.workspace_id,
                membership.role.as_str(),
                membership.status.as_str(),
                &created_at,
            ],
        )
        .await?;

        Ok(())
    }

    /// Get a specific membership
    pub async fn get_member(
        &self,
        workspace_id: &str,
        user_id: &str,
    ) -> Result<Option<WorkspaceMembership>> {
        let conn = self.db.connect()?;

        let mut rows = conn
            .query(
                "SELECT * FROM workspace_memberships WHERE workspace_id = ?1 AND user_id = ?2",
                [workspace_id, user_id],
            )
            .await?;

        if let Some(row) = rows.next().await? {
            Ok(Some(Self::row_to_membership(&row)?))
        } else {
            Ok(None)
        }
    }

    /// List all members of a workspace
    pub async fn list_members(&self, workspace_id: &str) -> Result<Vec<WorkspaceMembership>> {
        let conn = self.db.connect()?;

        let mut rows = conn
            .query(
                "SELECT * FROM workspace_memberships WHERE workspace_id = ?1 ORDER BY created_at",
                [workspace_id],
            )
            .await?;

        let mut members = Vec::new();
        while let Some(row) = rows.next().await? {
            members.push(Self::row_to_membership(&row)?);
        }

        Ok(members)
    }

    /// Update a member's role
    pub async fn update_member_role(
        &self,
        workspace_id: &str,
        user_id: &str,
        role: MemberRole,
    ) -> Result<()> {
        let conn = self.db.connect()?;

        let affected = conn
            .execute(
                "UPDATE workspace_memberships SET role = ?1 WHERE workspace_id = ?2 AND user_id = ?3",
                [role.as_str(), workspace_id, user_id],
            )
            .await?;

        if affected == 0 {
            return Err(ControlError::not_found(
                "membership",
                format!("{}:{}", workspace_id, user_id),
            ));
        }

        Ok(())
    }

    /// Remove a member from a workspace
    pub async fn remove_member(&self, workspace_id: &str, user_id: &str) -> Result<()> {
        let conn = self.db.connect()?;

        let affected = conn
            .execute(
                "DELETE FROM workspace_memberships WHERE workspace_id = ?1 AND user_id = ?2",
                [workspace_id, user_id],
            )
            .await?;

        if affected == 0 {
            return Err(ControlError::not_found(
                "membership",
                format!("{}:{}", workspace_id, user_id),
            ));
        }

        Ok(())
    }

    // =========================================================================
    // Row conversion helpers
    // =========================================================================

    fn row_to_workspace(row: &turso::Row) -> Result<Workspace> {
        let id = row
            .get_value(0)?
            .as_text()
            .unwrap_or(&String::new())
            .clone();
        let name = row
            .get_value(1)?
            .as_text()
            .unwrap_or(&String::new())
            .clone();
        let slug = row
            .get_value(2)?
            .as_text()
            .unwrap_or(&String::new())
            .clone();
        let clickhouse_database = row
            .get_value(3)?
            .as_text()
            .unwrap_or(&String::new())
            .clone();
        let settings_json = row
            .get_value(4)?
            .as_text()
            .unwrap_or(&String::from("{}"))
            .clone();
        let status_str = row
            .get_value(5)?
            .as_text()
            .unwrap_or(&String::from("active"))
            .clone();
        let created_at_str = row
            .get_value(6)?
            .as_text()
            .unwrap_or(&String::new())
            .clone();
        let updated_at_str = row
            .get_value(7)?
            .as_text()
            .unwrap_or(&String::new())
            .clone();

        let settings: WorkspaceSettings = serde_json::from_str(&settings_json).unwrap_or_default();
        let status = WorkspaceStatus::parse(&status_str);
        let created_at = DateTime::parse_from_rfc3339(&created_at_str)
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now());
        let updated_at = DateTime::parse_from_rfc3339(&updated_at_str)
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now());

        Ok(Workspace {
            id,
            name,
            slug,
            clickhouse_database,
            settings,
            status,
            created_at,
            updated_at,
        })
    }

    fn row_to_membership(row: &turso::Row) -> Result<WorkspaceMembership> {
        let user_id = row
            .get_value(0)?
            .as_text()
            .unwrap_or(&String::new())
            .clone();
        let workspace_id = row
            .get_value(1)?
            .as_text()
            .unwrap_or(&String::new())
            .clone();
        let role_str = row
            .get_value(2)?
            .as_text()
            .unwrap_or(&String::from("viewer"))
            .clone();
        let status_str = row
            .get_value(3)?
            .as_text()
            .unwrap_or(&String::from("active"))
            .clone();
        let created_at_str = row
            .get_value(4)?
            .as_text()
            .unwrap_or(&String::new())
            .clone();

        let role = MemberRole::parse(&role_str).unwrap_or(MemberRole::Viewer);
        let status = MemberStatus::parse(&status_str);
        let created_at = DateTime::parse_from_rfc3339(&created_at_str)
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now());

        Ok(WorkspaceMembership {
            user_id,
            workspace_id,
            role,
            status,
            created_at,
        })
    }
}
