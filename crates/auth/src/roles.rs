//! Roles and permissions for access control
//!
//! Simple RBAC model with 4 roles and 3 explicit permissions.
//!
//! # Roles (hierarchy)
//!
//! - `Viewer` - View analytics and content
//! - `Editor` - Create/edit own content
//! - `Admin` - Manage workspace
//! - `Platform` - Cross-workspace ops (enterprise)
//!
//! # Permissions
//!
//! - `Create` - Create/edit/share own content
//! - `Admin` - Manage workspace (members, settings)
//! - `Platform` - Cross-workspace operations
//!
//! Note: View is implicit for all workspace members.

use std::fmt;

/// User role in the system (ordered hierarchy)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Role {
    /// View analytics and shared content
    Viewer = 0,
    /// Create/edit own content, run queries
    Editor = 1,
    /// Manage workspace, edit anyone's content
    Admin = 2,
    /// Cross-workspace operations (enterprise/self-hosted)
    Platform = 3,
}

impl Role {
    /// Parse role from string
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "viewer" | "view" | "readonly" | "read_only" => Some(Self::Viewer),
            "editor" | "analyst" | "user" => Some(Self::Editor),
            "admin" => Some(Self::Admin),
            "platform" | "platform_admin" => Some(Self::Platform),
            _ => None,
        }
    }

    /// Convert to string
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Viewer => "viewer",
            Self::Editor => "editor",
            Self::Admin => "admin",
            Self::Platform => "platform",
        }
    }

    /// Check if this role has a permission
    pub fn has_permission(&self, permission: Permission) -> bool {
        match permission {
            Permission::Create => *self >= Self::Editor,
            Permission::Admin => *self >= Self::Admin,
            Permission::Platform => *self >= Self::Platform,
        }
    }

    /// Check if this role can do everything another role can
    pub fn includes(&self, other: Role) -> bool {
        *self >= other
    }
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Permission for a specific capability
///
/// Note: View permission is implicit for all workspace members.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Permission {
    /// Create/edit/share own content (dashboards, metrics, saved queries)
    Create,
    /// Manage workspace (members, settings, integrations)
    Admin,
    /// Cross-workspace operations (enterprise/self-hosted)
    Platform,
}

impl Permission {
    /// Parse permission from string
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "create" | "edit" | "write" => Some(Self::Create),
            "admin" | "manage" => Some(Self::Admin),
            "platform" => Some(Self::Platform),
            _ => None,
        }
    }

    /// Convert to string
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::Admin => "admin",
            Self::Platform => "platform",
        }
    }

    /// Minimum role required for this permission
    pub fn min_role(&self) -> Role {
        match self {
            Self::Create => Role::Editor,
            Self::Admin => Role::Admin,
            Self::Platform => Role::Platform,
        }
    }
}

impl fmt::Display for Permission {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_role_parsing() {
        assert_eq!(Role::parse("viewer"), Some(Role::Viewer));
        assert_eq!(Role::parse("readonly"), Some(Role::Viewer));
        assert_eq!(Role::parse("editor"), Some(Role::Editor));
        assert_eq!(Role::parse("analyst"), Some(Role::Editor));
        assert_eq!(Role::parse("user"), Some(Role::Editor));
        assert_eq!(Role::parse("admin"), Some(Role::Admin));
        assert_eq!(Role::parse("platform"), Some(Role::Platform));
        assert_eq!(Role::parse("platform_admin"), Some(Role::Platform));
        assert_eq!(Role::parse("invalid"), None);
    }

    #[test]
    fn test_role_hierarchy() {
        assert!(Role::Platform > Role::Admin);
        assert!(Role::Admin > Role::Editor);
        assert!(Role::Editor > Role::Viewer);
    }

    #[test]
    fn test_role_includes() {
        assert!(Role::Admin.includes(Role::Editor));
        assert!(Role::Admin.includes(Role::Viewer));
        assert!(!Role::Editor.includes(Role::Admin));
    }

    #[test]
    fn test_permission_check() {
        assert!(!Role::Viewer.has_permission(Permission::Create));
        assert!(Role::Editor.has_permission(Permission::Create));
        assert!(Role::Admin.has_permission(Permission::Create));

        assert!(!Role::Editor.has_permission(Permission::Admin));
        assert!(Role::Admin.has_permission(Permission::Admin));

        assert!(!Role::Admin.has_permission(Permission::Platform));
        assert!(Role::Platform.has_permission(Permission::Platform));
    }

    #[test]
    fn test_permission_parsing() {
        assert_eq!(Permission::parse("create"), Some(Permission::Create));
        assert_eq!(Permission::parse("edit"), Some(Permission::Create));
        assert_eq!(Permission::parse("admin"), Some(Permission::Admin));
        assert_eq!(Permission::parse("platform"), Some(Permission::Platform));
        assert_eq!(Permission::parse("invalid"), None);
    }

    #[test]
    fn test_min_role() {
        assert_eq!(Permission::Create.min_role(), Role::Editor);
        assert_eq!(Permission::Admin.min_role(), Role::Admin);
        assert_eq!(Permission::Platform.min_role(), Role::Platform);
    }
}
