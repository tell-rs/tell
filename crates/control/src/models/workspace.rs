//! Workspace model
//!
//! Workspaces provide multi-tenant isolation. Each workspace has:
//! - Its own ClickHouse database for analytics
//! - Its own Turso database for boards/metrics
//! - Membership with roles (viewer, editor, admin)

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Workspace entity
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Workspace {
    /// Unique identifier (UUID)
    pub id: String,
    /// Display name
    pub name: String,
    /// URL-friendly slug (unique)
    pub slug: String,
    /// ClickHouse database name for analytics
    pub clickhouse_database: String,
    /// Workspace settings (JSON)
    #[serde(default)]
    pub settings: WorkspaceSettings,
    /// Status (active, suspended)
    #[serde(default)]
    pub status: WorkspaceStatus,
    /// Creation timestamp
    pub created_at: DateTime<Utc>,
    /// Last update timestamp
    pub updated_at: DateTime<Utc>,
}

impl Workspace {
    /// Create a new workspace with defaults
    pub fn new(name: impl Into<String>, slug: impl Into<String>) -> Self {
        let now = Utc::now();
        let slug = slug.into();

        Self {
            id: uuid::Uuid::new_v4().to_string(),
            name: name.into(),
            clickhouse_database: format!("ws_{}", slug.replace('-', "_")),
            slug,
            settings: WorkspaceSettings::default(),
            status: WorkspaceStatus::Active,
            created_at: now,
            updated_at: now,
        }
    }
}

/// Workspace settings stored as JSON
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WorkspaceSettings {
    /// Default date range for metrics (e.g., "7d", "30d")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_date_range: Option<String>,
    /// Default timezone
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timezone: Option<String>,
    /// Enabled feature flags
    #[serde(default)]
    pub features: Vec<String>,
}

/// Workspace status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WorkspaceStatus {
    #[default]
    Active,
    Suspended,
}

impl WorkspaceStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Suspended => "suspended",
        }
    }

    pub fn parse(s: &str) -> Self {
        match s {
            "suspended" => Self::Suspended,
            _ => Self::Active,
        }
    }
}

/// Workspace membership (user ↔ workspace)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceMembership {
    /// User ID (from tell-auth)
    pub user_id: String,
    /// Workspace ID
    pub workspace_id: String,
    /// Member role
    pub role: MemberRole,
    /// Membership status
    #[serde(default)]
    pub status: MemberStatus,
    /// When they joined
    pub created_at: DateTime<Utc>,
}

impl WorkspaceMembership {
    /// Create a new active membership
    pub fn new(
        user_id: impl Into<String>,
        workspace_id: impl Into<String>,
        role: MemberRole,
    ) -> Self {
        Self {
            user_id: user_id.into(),
            workspace_id: workspace_id.into(),
            role,
            status: MemberStatus::Active,
            created_at: Utc::now(),
        }
    }
}

/// Member role within a workspace
///
/// Maps to tell-auth roles but scoped to workspace:
/// - Viewer: View analytics and shared content
/// - Editor: Create/edit own content
/// - Admin: Manage workspace members and settings
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MemberRole {
    Viewer,
    Editor,
    Admin,
}

impl MemberRole {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Viewer => "viewer",
            Self::Editor => "editor",
            Self::Admin => "admin",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "viewer" => Some(Self::Viewer),
            "editor" => Some(Self::Editor),
            "admin" => Some(Self::Admin),
            _ => None,
        }
    }

    /// Check if this role can edit content
    pub fn can_edit(&self) -> bool {
        matches!(self, Self::Editor | Self::Admin)
    }

    /// Check if this role can manage workspace
    pub fn can_manage(&self) -> bool {
        matches!(self, Self::Admin)
    }
}

/// Membership status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MemberStatus {
    #[default]
    Active,
    Invited,
    Suspended,
}

impl MemberStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Invited => "invited",
            Self::Suspended => "suspended",
        }
    }

    pub fn parse(s: &str) -> Self {
        match s {
            "invited" => Self::Invited,
            "suspended" => Self::Suspended,
            _ => Self::Active,
        }
    }
}
