//! Authentication context types
//!
//! Unified types for auth + workspace + permission context.

use crate::roles::{Permission, Role};
use crate::user::UserInfo;

/// Complete authentication context for a request
///
/// Single source of truth containing user info and optional workspace access.
/// Use this when you need both user and workspace information.
#[derive(Debug, Clone)]
pub struct AuthContext {
    /// Authenticated user
    pub user: UserInfo,
    /// Workspace access (present if request is workspace-scoped)
    pub workspace: Option<WorkspaceAccess>,
}

impl AuthContext {
    /// Create a new auth context with just user info (no workspace)
    pub fn new(user: UserInfo) -> Self {
        Self {
            user,
            workspace: None,
        }
    }

    /// Create a new auth context with user and workspace
    pub fn with_workspace(user: UserInfo, workspace: WorkspaceAccess) -> Self {
        Self {
            user,
            workspace: Some(workspace),
        }
    }

    /// Get workspace access, returning error if not present
    pub fn require_workspace(&self) -> Result<&WorkspaceAccess, crate::AuthError> {
        self.workspace
            .as_ref()
            .ok_or(crate::AuthError::WorkspaceAccessDenied(
                "workspace context required".to_string(),
            ))
    }

    /// Check if user has a global permission (ignoring workspace context)
    pub fn has_permission(&self, permission: Permission) -> bool {
        self.user.has_permission(permission)
    }

    /// Check if user has permission in current workspace
    ///
    /// Returns false if no workspace context is present.
    pub fn has_workspace_permission(&self, permission: Permission) -> bool {
        self.workspace
            .as_ref()
            .map(|ws| ws.has_permission(permission))
            .unwrap_or(false)
    }

    /// Check if user is admin in current workspace
    pub fn is_workspace_admin(&self) -> bool {
        self.workspace
            .as_ref()
            .map(|ws| ws.is_admin())
            .unwrap_or(false)
    }
}

/// Validated workspace access context
///
/// Represents verified access to a workspace. When you have this struct,
/// it means the user has been validated as an active member of the workspace.
#[derive(Debug, Clone)]
pub struct WorkspaceAccess {
    /// Workspace ID
    pub id: String,
    /// User's role in this workspace
    pub role: Role,
}

impl WorkspaceAccess {
    /// Create a new workspace access context
    pub fn new(id: impl Into<String>, role: Role) -> Self {
        Self {
            id: id.into(),
            role,
        }
    }

    /// Check if user has a specific permission in this workspace
    pub fn has_permission(&self, permission: Permission) -> bool {
        self.role.has_permission(permission)
    }

    /// Check if user is admin (or higher) in this workspace
    pub fn is_admin(&self) -> bool {
        self.role >= Role::Admin
    }

    /// Check if user can create content in this workspace
    pub fn can_create(&self) -> bool {
        self.has_permission(Permission::Create)
    }

    /// Check if user can manage this workspace
    pub fn can_manage(&self) -> bool {
        self.has_permission(Permission::Admin)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auth_context_without_workspace() {
        let user = UserInfo::with_role("user-1", "test@example.com", Role::Editor);
        let ctx = AuthContext::new(user);

        assert!(ctx.workspace.is_none());
        assert!(ctx.has_permission(Permission::Create));
        assert!(!ctx.has_workspace_permission(Permission::Create));
        assert!(ctx.require_workspace().is_err());
    }

    #[test]
    fn test_auth_context_with_workspace() {
        let user = UserInfo::with_role("user-1", "test@example.com", Role::Viewer);
        let workspace = WorkspaceAccess::new("ws-1", Role::Admin);
        let ctx = AuthContext::with_workspace(user, workspace);

        assert!(ctx.workspace.is_some());
        // User is viewer globally, but admin in workspace
        assert!(!ctx.has_permission(Permission::Admin));
        assert!(ctx.has_workspace_permission(Permission::Admin));
        assert!(ctx.is_workspace_admin());
        assert!(ctx.require_workspace().is_ok());
    }

    #[test]
    fn test_workspace_access_permissions() {
        let viewer = WorkspaceAccess::new("ws-1", Role::Viewer);
        assert!(!viewer.can_create());
        assert!(!viewer.can_manage());
        assert!(!viewer.is_admin());

        let editor = WorkspaceAccess::new("ws-1", Role::Editor);
        assert!(editor.can_create());
        assert!(!editor.can_manage());
        assert!(!editor.is_admin());

        let admin = WorkspaceAccess::new("ws-1", Role::Admin);
        assert!(admin.can_create());
        assert!(admin.can_manage());
        assert!(admin.is_admin());
    }
}
