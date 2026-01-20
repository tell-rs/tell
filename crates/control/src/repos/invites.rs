//! Workspace invite repository
//!
//! CRUD operations for workspace invitations.

use chrono::{DateTime, Utc};
use turso::Database;

use crate::error::Result;
use crate::models::{InviteStatus, MemberRole, WorkspaceInvite};

/// Repository for managing workspace invites
pub struct InviteRepo<'a> {
    db: &'a Database,
}

impl<'a> InviteRepo<'a> {
    /// Create a new invite repository
    pub fn new(db: &'a Database) -> Self {
        Self { db }
    }

    /// Create a new invite
    pub async fn create(&self, invite: &WorkspaceInvite) -> Result<()> {
        let conn = self.db.connect()?;

        conn.execute(
            r#"
            INSERT INTO workspace_invites (
                id, email, workspace_id, role, token, status,
                created_by, expires_at, created_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
            "#,
            [
                invite.id.as_str(),
                invite.email.as_str(),
                invite.workspace_id.as_str(),
                invite.role.as_str(),
                invite.token.as_str(),
                invite.status.as_str(),
                invite.created_by.as_str(),
                invite.expires_at.to_rfc3339().as_str(),
                invite.created_at.to_rfc3339().as_str(),
            ],
        )
        .await?;

        Ok(())
    }

    /// Get an invite by ID
    pub async fn get_by_id(&self, id: &str) -> Result<Option<WorkspaceInvite>> {
        let conn = self.db.connect()?;

        let mut rows = conn
            .query("SELECT * FROM workspace_invites WHERE id = ?1", [id])
            .await?;

        if let Some(row) = rows.next().await? {
            Ok(Some(row_to_invite(&row)?))
        } else {
            Ok(None)
        }
    }

    /// Get an invite by token
    pub async fn get_by_token(&self, token: &str) -> Result<Option<WorkspaceInvite>> {
        let conn = self.db.connect()?;

        let mut rows = conn
            .query("SELECT * FROM workspace_invites WHERE token = ?1", [token])
            .await?;

        if let Some(row) = rows.next().await? {
            Ok(Some(row_to_invite(&row)?))
        } else {
            Ok(None)
        }
    }

    /// Get pending invite for an email in a workspace
    pub async fn get_pending_for_email(
        &self,
        email: &str,
        workspace_id: &str,
    ) -> Result<Option<WorkspaceInvite>> {
        let conn = self.db.connect()?;

        let mut rows = conn
            .query(
                "SELECT * FROM workspace_invites WHERE email = ?1 AND workspace_id = ?2 AND status = 'pending'",
                [email, workspace_id],
            )
            .await?;

        if let Some(row) = rows.next().await? {
            Ok(Some(row_to_invite(&row)?))
        } else {
            Ok(None)
        }
    }

    /// List invites for a workspace
    pub async fn list_for_workspace(&self, workspace_id: &str) -> Result<Vec<WorkspaceInvite>> {
        let conn = self.db.connect()?;

        let mut rows = conn
            .query(
                "SELECT * FROM workspace_invites WHERE workspace_id = ?1 ORDER BY created_at DESC",
                [workspace_id],
            )
            .await?;

        let mut invites = Vec::new();
        while let Some(row) = rows.next().await? {
            invites.push(row_to_invite(&row)?);
        }

        Ok(invites)
    }

    /// List pending invites for a workspace
    pub async fn list_pending_for_workspace(&self, workspace_id: &str) -> Result<Vec<WorkspaceInvite>> {
        let conn = self.db.connect()?;

        let mut rows = conn
            .query(
                "SELECT * FROM workspace_invites WHERE workspace_id = ?1 AND status = 'pending' ORDER BY created_at DESC",
                [workspace_id],
            )
            .await?;

        let mut invites = Vec::new();
        while let Some(row) = rows.next().await? {
            invites.push(row_to_invite(&row)?);
        }

        Ok(invites)
    }

    /// Update invite status
    pub async fn update_status(&self, id: &str, status: InviteStatus) -> Result<()> {
        let conn = self.db.connect()?;

        conn.execute(
            "UPDATE workspace_invites SET status = ?1 WHERE id = ?2",
            [status.as_str(), id],
        )
        .await?;

        Ok(())
    }

    /// Delete an invite
    pub async fn delete(&self, id: &str) -> Result<()> {
        let conn = self.db.connect()?;

        conn.execute("DELETE FROM workspace_invites WHERE id = ?1", [id])
            .await?;

        Ok(())
    }

    /// Delete expired invites (cleanup)
    pub async fn delete_expired(&self) -> Result<u64> {
        let conn = self.db.connect()?;
        let now = Utc::now().to_rfc3339();

        let result = conn
            .execute(
                "DELETE FROM workspace_invites WHERE status = 'pending' AND expires_at < ?1",
                [now.as_str()],
            )
            .await?;

        Ok(result)
    }
}

/// Convert a database row to a WorkspaceInvite
fn row_to_invite(row: &turso::Row) -> Result<WorkspaceInvite> {
    let empty = String::new();

    let v0 = row.get_value(0)?;
    let v1 = row.get_value(1)?;
    let v2 = row.get_value(2)?;
    let v3 = row.get_value(3)?;
    let v4 = row.get_value(4)?;
    let v5 = row.get_value(5)?;
    let v6 = row.get_value(6)?;
    let v7 = row.get_value(7)?;
    let v8 = row.get_value(8)?;

    let id = v0.as_text().unwrap_or(&empty).clone();
    let email = v1.as_text().unwrap_or(&empty).clone();
    let workspace_id = v2.as_text().unwrap_or(&empty).clone();
    let role_str = v3.as_text().unwrap_or(&empty);
    let role = MemberRole::from_str(role_str).unwrap_or(MemberRole::Viewer);
    let token = v4.as_text().unwrap_or(&empty).clone();
    let status_str = v5.as_text().unwrap_or(&empty);
    let status = InviteStatus::parse(status_str).unwrap_or(InviteStatus::Pending);
    let created_by = v6.as_text().unwrap_or(&empty).clone();
    let expires_at = parse_datetime(v7.as_text().unwrap_or(&empty))?;
    let created_at = parse_datetime(v8.as_text().unwrap_or(&empty))?;

    Ok(WorkspaceInvite {
        id,
        email,
        workspace_id,
        role,
        token,
        status,
        created_by,
        expires_at,
        created_at,
    })
}

fn parse_datetime(s: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|e| crate::error::ControlError::invalid("datetime", e.to_string()))
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
            CREATE TABLE IF NOT EXISTS workspace_invites (
                id TEXT PRIMARY KEY,
                email TEXT NOT NULL,
                workspace_id TEXT NOT NULL,
                role TEXT NOT NULL,
                token TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL DEFAULT 'pending',
                created_by TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            "#,
            (),
        )
        .await
        .unwrap();

        db
    }

    #[tokio::test]
    async fn test_create_and_get_invite() {
        let db = setup_db().await;
        let repo = InviteRepo::new(&db);

        let invite = WorkspaceInvite::new(
            "user@example.com",
            "ws_123",
            MemberRole::Editor,
            "admin_456",
        );
        let token = invite.token.clone();

        repo.create(&invite).await.unwrap();

        // Get by ID
        let fetched = repo.get_by_id(&invite.id).await.unwrap().unwrap();
        assert_eq!(fetched.email, "user@example.com");
        assert_eq!(fetched.role, MemberRole::Editor);

        // Get by token
        let fetched = repo.get_by_token(&token).await.unwrap().unwrap();
        assert_eq!(fetched.id, invite.id);
    }

    #[tokio::test]
    async fn test_list_for_workspace() {
        let db = setup_db().await;
        let repo = InviteRepo::new(&db);

        let invite1 = WorkspaceInvite::new("user1@example.com", "ws_1", MemberRole::Viewer, "admin");
        let invite2 = WorkspaceInvite::new("user2@example.com", "ws_1", MemberRole::Editor, "admin");
        let invite3 = WorkspaceInvite::new("user3@example.com", "ws_2", MemberRole::Admin, "admin");

        repo.create(&invite1).await.unwrap();
        repo.create(&invite2).await.unwrap();
        repo.create(&invite3).await.unwrap();

        let ws1_invites = repo.list_for_workspace("ws_1").await.unwrap();
        assert_eq!(ws1_invites.len(), 2);

        let ws2_invites = repo.list_for_workspace("ws_2").await.unwrap();
        assert_eq!(ws2_invites.len(), 1);
    }

    #[tokio::test]
    async fn test_update_status() {
        let db = setup_db().await;
        let repo = InviteRepo::new(&db);

        let invite = WorkspaceInvite::new("user@example.com", "ws_1", MemberRole::Viewer, "admin");
        repo.create(&invite).await.unwrap();

        // Update to accepted
        repo.update_status(&invite.id, InviteStatus::Accepted)
            .await
            .unwrap();

        let fetched = repo.get_by_id(&invite.id).await.unwrap().unwrap();
        assert_eq!(fetched.status, InviteStatus::Accepted);
    }

    #[tokio::test]
    async fn test_get_pending_for_email() {
        let db = setup_db().await;
        let repo = InviteRepo::new(&db);

        let invite = WorkspaceInvite::new("user@example.com", "ws_1", MemberRole::Viewer, "admin");
        repo.create(&invite).await.unwrap();

        // Should find pending
        let found = repo
            .get_pending_for_email("user@example.com", "ws_1")
            .await
            .unwrap();
        assert!(found.is_some());

        // Accept it
        repo.update_status(&invite.id, InviteStatus::Accepted)
            .await
            .unwrap();

        // Should not find it anymore
        let found = repo
            .get_pending_for_email("user@example.com", "ws_1")
            .await
            .unwrap();
        assert!(found.is_none());
    }
}
