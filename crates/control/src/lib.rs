//! Tell Control Plane
//!
//! Turso-backed persistence for workspaces, boards, users, and sharing.
//!
//! # Architecture
//!
//! Following the Go cdp-api pattern, we have two database layers:
//!
//! | Database | Scope | Contains |
//! |----------|-------|----------|
//! | **Control DB** | Global singleton | Users, workspaces, memberships, shared_links |
//! | **Workspace DBs** | Per-workspace | Boards, metrics, settings |
//!
//! # Usage
//!
//! ```ignore
//! use tell_control::ControlPlane;
//!
//! // File-based (production)
//! let cp = ControlPlane::new("data").await?;
//!
//! // In-memory (testing)
//! let cp = ControlPlane::new_memory().await?;
//!
//! // Access repositories
//! let workspaces = cp.workspaces();
//! let ws = workspaces.get_by_slug("my-workspace").await?;
//! ```
//!
//! # Auth Integration
//!
//! This crate works with `tell-auth` for authentication:
//! - Users are created/validated via `tell-auth`
//! - Workspace membership maps users to workspaces with roles
//! - Permission checks use membership roles
//!
//! See `tell-auth/src/roles.rs` for role definitions.

pub mod db;
pub mod error;
pub mod models;
pub mod repos;

// Re-exports
pub use db::ControlPlane;
pub use error::{ControlError, Result};
pub use models::{
    Block, BlockPosition, Board, BoardLayout, BoardSettings, InviteStatus, MemberRole,
    MemberStatus, MetricBlock, NoteBlock, ResourceType, SharedLink, UserApiKey, VisualizationType,
    Workspace, WorkspaceInvite, WorkspaceMembership, WorkspaceSettings, WorkspaceStatus,
};
pub use repos::{ApiKeyRepo, BoardRepo, InviteRepo, SharingRepo, WorkspaceRepo};

impl ControlPlane {
    /// Get workspace repository for the control database
    pub fn workspaces(&self) -> WorkspaceRepo<'_> {
        WorkspaceRepo::new(self.control_db())
    }

    /// Get sharing repository for the control database
    ///
    /// Shared links are stored globally in the control database.
    pub fn sharing(&self) -> SharingRepo<'_> {
        SharingRepo::new(self.control_db())
    }

    /// Get board repository for a workspace database
    ///
    /// Note: The caller must provide the workspace database reference.
    /// Use `workspace_db(workspace_id).await?` to get it first.
    pub fn boards<'a>(db: &'a turso::Database) -> BoardRepo<'a> {
        BoardRepo::new(db)
    }

    /// Get API key repository for the control database
    ///
    /// API keys are stored globally in the control database.
    pub fn api_keys(&self) -> ApiKeyRepo<'_> {
        ApiKeyRepo::new(self.control_db())
    }

    /// Get invite repository for the control database
    ///
    /// Workspace invites are stored globally in the control database.
    pub fn invites(&self) -> InviteRepo<'_> {
        InviteRepo::new(self.control_db())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_workspace_crud() {
        // Create in-memory control plane
        let cp = ControlPlane::new_memory().await.unwrap();
        let repo = cp.workspaces();

        // Create workspace
        let ws = Workspace::new("Test Workspace", "test-workspace");
        repo.create(&ws).await.unwrap();

        // Get by ID
        let fetched = repo.get_by_id(&ws.id).await.unwrap();
        assert!(fetched.is_some());
        let fetched = fetched.unwrap();
        assert_eq!(fetched.name, "Test Workspace");
        assert_eq!(fetched.slug, "test-workspace");

        // Get by slug
        let by_slug = repo.get_by_slug("test-workspace").await.unwrap();
        assert!(by_slug.is_some());
        assert_eq!(by_slug.unwrap().id, ws.id);

        // Update
        let mut updated = fetched.clone();
        updated.name = "Updated Name".to_string();
        repo.update(&updated).await.unwrap();

        let refetched = repo.get_by_id(&ws.id).await.unwrap().unwrap();
        assert_eq!(refetched.name, "Updated Name");

        // List all
        let all = repo.list_all().await.unwrap();
        assert_eq!(all.len(), 1);

        // Delete
        repo.delete(&ws.id).await.unwrap();
        let deleted = repo.get_by_id(&ws.id).await.unwrap();
        assert!(deleted.is_none());
    }

    #[tokio::test]
    async fn test_membership_crud() {
        let cp = ControlPlane::new_memory().await.unwrap();
        let repo = cp.workspaces();

        // Create workspace first
        let ws = Workspace::new("Test Workspace", "test-workspace");
        repo.create(&ws).await.unwrap();

        // Add member
        let membership = WorkspaceMembership::new("user_123", &ws.id, MemberRole::Editor);
        repo.add_member(&membership).await.unwrap();

        // Get member
        let fetched = repo.get_member(&ws.id, "user_123").await.unwrap();
        assert!(fetched.is_some());
        let fetched = fetched.unwrap();
        assert_eq!(fetched.role, MemberRole::Editor);

        // List members
        let members = repo.list_members(&ws.id).await.unwrap();
        assert_eq!(members.len(), 1);

        // Update role
        repo.update_member_role(&ws.id, "user_123", MemberRole::Admin)
            .await
            .unwrap();
        let updated = repo.get_member(&ws.id, "user_123").await.unwrap().unwrap();
        assert_eq!(updated.role, MemberRole::Admin);

        // List workspaces for user
        let user_ws = repo.list_for_user("user_123").await.unwrap();
        assert_eq!(user_ws.len(), 1);
        assert_eq!(user_ws[0].id, ws.id);

        // Remove member
        repo.remove_member(&ws.id, "user_123").await.unwrap();
        let removed = repo.get_member(&ws.id, "user_123").await.unwrap();
        assert!(removed.is_none());
    }

    #[tokio::test]
    async fn test_board_crud() {
        let cp = ControlPlane::new_memory().await.unwrap();

        // Create workspace first (boards need workspace context)
        let ws = Workspace::new("Test Workspace", "test-workspace");
        cp.workspaces().create(&ws).await.unwrap();

        // Get workspace database
        // For in-memory tests, we use control_db as the workspace db
        // (in production, each workspace has its own DB)
        let ws_db = cp.control_db();

        // Initialize boards table in control_db for testing
        let conn = ws_db.connect().unwrap();
        conn.execute(
            r#"
            CREATE TABLE IF NOT EXISTS boards (
                id TEXT PRIMARY KEY,
                workspace_id TEXT NOT NULL,
                owner_id TEXT NOT NULL,
                title TEXT NOT NULL,
                description TEXT,
                settings TEXT DEFAULT '{}',
                is_pinned INTEGER DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            "#,
            (),
        )
        .await
        .unwrap();

        let repo = ControlPlane::boards(ws_db);

        // Create board
        let board = Board::new(&ws.id, "user_1", "My Dashboard");
        repo.create(&board).await.unwrap();

        // Get by ID
        let fetched = repo.get_by_id(&board.id).await.unwrap();
        assert!(fetched.is_some());
        let fetched = fetched.unwrap();
        assert_eq!(fetched.title, "My Dashboard");
        assert_eq!(fetched.owner_id, "user_1");
        assert!(!fetched.is_pinned);

        // Update
        let mut updated = fetched.clone();
        updated.title = "Updated Dashboard".to_string();
        repo.update(&updated).await.unwrap();

        let refetched = repo.get_by_id(&board.id).await.unwrap().unwrap();
        assert_eq!(refetched.title, "Updated Dashboard");

        // List for workspace
        let boards = repo.list_for_workspace(&ws.id).await.unwrap();
        assert_eq!(boards.len(), 1);

        // List for owner
        let owner_boards = repo.list_for_owner("user_1").await.unwrap();
        assert_eq!(owner_boards.len(), 1);

        // Pin/unpin
        repo.set_pinned(&board.id, true).await.unwrap();
        let pinned_boards = repo.list_pinned(&ws.id).await.unwrap();
        assert_eq!(pinned_boards.len(), 1);

        repo.set_pinned(&board.id, false).await.unwrap();
        let pinned_boards = repo.list_pinned(&ws.id).await.unwrap();
        assert_eq!(pinned_boards.len(), 0);

        // Delete
        repo.delete(&board.id).await.unwrap();
        let deleted = repo.get_by_id(&board.id).await.unwrap();
        assert!(deleted.is_none());
    }

    #[tokio::test]
    async fn test_sharing_crud() {
        let cp = ControlPlane::new_memory().await.unwrap();

        // Create workspace first
        let ws = Workspace::new("Test Workspace", "test-workspace");
        cp.workspaces().create(&ws).await.unwrap();

        let repo = cp.sharing();

        // Create shared link for a board
        let link = SharedLink::for_board("board_123", &ws.id, "user_1");
        let hash = link.hash.clone();
        repo.create(&link).await.unwrap();

        // Get by hash
        let fetched = repo.get_by_hash(&hash).await.unwrap();
        assert!(fetched.is_some());
        let fetched = fetched.unwrap();
        assert_eq!(fetched.resource_type, ResourceType::Board);
        assert_eq!(fetched.resource_id, "board_123");

        // Get for resource
        let by_resource = repo
            .get_for_resource(ResourceType::Board, "board_123")
            .await
            .unwrap();
        assert!(by_resource.is_some());
        assert_eq!(by_resource.unwrap().hash, hash);

        // List for workspace
        let ws_links = repo.list_for_workspace(&ws.id).await.unwrap();
        assert_eq!(ws_links.len(), 1);

        // List for user
        let user_links = repo.list_for_user("user_1").await.unwrap();
        assert_eq!(user_links.len(), 1);

        // Delete
        repo.delete(&hash).await.unwrap();
        let deleted = repo.get_by_hash(&hash).await.unwrap();
        assert!(deleted.is_none());
    }
}
