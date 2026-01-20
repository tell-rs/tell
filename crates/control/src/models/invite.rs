//! Workspace invite model
//!
//! Invitations for users to join workspaces.

use chrono::{DateTime, Duration, Utc};
use rand::Rng;
use serde::{Deserialize, Serialize};

use crate::MemberRole;

/// Workspace invitation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceInvite {
    /// Unique invite ID
    pub id: String,
    /// Email address of the invitee
    pub email: String,
    /// Workspace being invited to
    pub workspace_id: String,
    /// Role to grant on acceptance
    pub role: MemberRole,
    /// Unique invite token (for URL)
    pub token: String,
    /// Invite status
    pub status: InviteStatus,
    /// User who created the invite
    pub created_by: String,
    /// When the invite expires
    pub expires_at: DateTime<Utc>,
    /// When the invite was created
    pub created_at: DateTime<Utc>,
}

/// Invite status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum InviteStatus {
    /// Pending - waiting for acceptance
    Pending,
    /// Accepted - user joined the workspace
    Accepted,
    /// Expired - invite timed out
    Expired,
    /// Cancelled - admin revoked the invite
    Cancelled,
}

impl InviteStatus {
    /// Parse from string
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "pending" => Some(Self::Pending),
            "accepted" => Some(Self::Accepted),
            "expired" => Some(Self::Expired),
            "cancelled" => Some(Self::Cancelled),
            _ => None,
        }
    }

    /// Convert to string
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Accepted => "accepted",
            Self::Expired => "expired",
            Self::Cancelled => "cancelled",
        }
    }
}

impl WorkspaceInvite {
    /// Create a new workspace invite
    ///
    /// Generates a unique token and sets expiry to 7 days.
    pub fn new(
        email: &str,
        workspace_id: &str,
        role: MemberRole,
        created_by: &str,
    ) -> Self {
        Self {
            id: generate_id(),
            email: email.to_string(),
            workspace_id: workspace_id.to_string(),
            role,
            token: generate_token(),
            status: InviteStatus::Pending,
            created_by: created_by.to_string(),
            expires_at: Utc::now() + Duration::days(7),
            created_at: Utc::now(),
        }
    }

    /// Create an invite with custom expiry
    pub fn with_expiry(mut self, expires_at: DateTime<Utc>) -> Self {
        self.expires_at = expires_at;
        self
    }

    /// Check if the invite is expired
    pub fn is_expired(&self) -> bool {
        Utc::now() > self.expires_at
    }

    /// Check if the invite is still valid (pending and not expired)
    pub fn is_valid(&self) -> bool {
        self.status == InviteStatus::Pending && !self.is_expired()
    }

    /// Mark the invite as accepted
    pub fn accept(&mut self) {
        self.status = InviteStatus::Accepted;
    }

    /// Mark the invite as cancelled
    pub fn cancel(&mut self) {
        self.status = InviteStatus::Cancelled;
    }
}

/// Generate a unique invite ID
fn generate_id() -> String {
    format!("inv_{}", generate_random_string(16))
}

/// Generate a unique invite token (URL-safe)
fn generate_token() -> String {
    generate_random_string(32)
}

/// Generate a random alphanumeric string
fn generate_random_string(len: usize) -> String {
    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut rng = rand::rng();
    (0..len)
        .map(|_| {
            let idx = rng.random_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_invite() {
        let invite = WorkspaceInvite::new(
            "user@example.com",
            "ws_123",
            MemberRole::Editor,
            "admin_456",
        );

        assert!(invite.id.starts_with("inv_"));
        assert_eq!(invite.email, "user@example.com");
        assert_eq!(invite.workspace_id, "ws_123");
        assert_eq!(invite.role, MemberRole::Editor);
        assert_eq!(invite.status, InviteStatus::Pending);
        assert!(!invite.token.is_empty());
        assert!(invite.is_valid());
    }

    #[test]
    fn test_invite_expiry() {
        let mut invite = WorkspaceInvite::new(
            "user@example.com",
            "ws_123",
            MemberRole::Viewer,
            "admin_456",
        );

        // Not expired yet
        assert!(!invite.is_expired());
        assert!(invite.is_valid());

        // Set to past
        invite.expires_at = Utc::now() - Duration::hours(1);
        assert!(invite.is_expired());
        assert!(!invite.is_valid());
    }

    #[test]
    fn test_invite_status() {
        let mut invite = WorkspaceInvite::new(
            "user@example.com",
            "ws_123",
            MemberRole::Editor,
            "admin_456",
        );

        assert!(invite.is_valid());

        invite.accept();
        assert_eq!(invite.status, InviteStatus::Accepted);
        assert!(!invite.is_valid()); // No longer pending

        let mut invite2 = WorkspaceInvite::new(
            "user2@example.com",
            "ws_123",
            MemberRole::Viewer,
            "admin_456",
        );
        invite2.cancel();
        assert_eq!(invite2.status, InviteStatus::Cancelled);
        assert!(!invite2.is_valid());
    }

    #[test]
    fn test_status_parsing() {
        assert_eq!(InviteStatus::parse("pending"), Some(InviteStatus::Pending));
        assert_eq!(InviteStatus::parse("ACCEPTED"), Some(InviteStatus::Accepted));
        assert_eq!(InviteStatus::parse("Expired"), Some(InviteStatus::Expired));
        assert_eq!(InviteStatus::parse("cancelled"), Some(InviteStatus::Cancelled));
        assert_eq!(InviteStatus::parse("invalid"), None);
    }
}
