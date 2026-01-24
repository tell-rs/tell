//! Workspace membership provider trait
//!
//! Abstracts over different membership storage backends (LocalUserStore, ControlPlane, etc.)

use async_trait::async_trait;

use crate::error::Result;
use crate::roles::Role;

/// Membership status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MembershipStatus {
    /// User has been invited but hasn't accepted
    Invited,
    /// User is an active member
    Active,
    /// User has been removed
    Removed,
}

impl MembershipStatus {
    /// Check if this is an active membership
    pub fn is_active(&self) -> bool {
        *self == Self::Active
    }
}

/// Workspace membership record
#[derive(Debug, Clone)]
pub struct Membership {
    /// User ID
    pub user_id: String,
    /// Workspace ID
    pub workspace_id: String,
    /// User's role in this workspace
    pub role: Role,
    /// Membership status
    pub status: MembershipStatus,
}

impl Membership {
    /// Check if this is an active membership
    pub fn is_active(&self) -> bool {
        self.status.is_active()
    }
}

/// Trait for workspace membership validation
///
/// Implement this trait to provide workspace membership checking.
/// This abstracts over different storage backends like LocalUserStore or ControlPlane.
#[async_trait]
pub trait MembershipProvider: Send + Sync {
    /// Get a user's membership in a workspace
    ///
    /// Returns None if the user is not a member of the workspace.
    async fn get_membership(&self, user_id: &str, workspace_id: &str)
    -> Result<Option<Membership>>;

    /// Check if a user is an active member of a workspace
    async fn is_active_member(&self, user_id: &str, workspace_id: &str) -> Result<bool> {
        let membership = self.get_membership(user_id, workspace_id).await?;
        Ok(membership.map(|m| m.is_active()).unwrap_or(false))
    }
}

/// Membership provider that allows all access (for development/testing)
///
/// Always returns an active Admin membership for any user/workspace combination.
#[derive(Debug, Clone, Default)]
pub struct AllowAllMembership;

#[async_trait]
impl MembershipProvider for AllowAllMembership {
    async fn get_membership(
        &self,
        user_id: &str,
        workspace_id: &str,
    ) -> Result<Option<Membership>> {
        Ok(Some(Membership {
            user_id: user_id.to_string(),
            workspace_id: workspace_id.to_string(),
            role: Role::Admin,
            status: MembershipStatus::Active,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_membership_status() {
        assert!(MembershipStatus::Active.is_active());
        assert!(!MembershipStatus::Invited.is_active());
        assert!(!MembershipStatus::Removed.is_active());
    }

    #[test]
    fn test_membership_is_active() {
        let active = Membership {
            user_id: "user-1".to_string(),
            workspace_id: "ws-1".to_string(),
            role: Role::Editor,
            status: MembershipStatus::Active,
        };
        assert!(active.is_active());

        let invited = Membership {
            user_id: "user-1".to_string(),
            workspace_id: "ws-1".to_string(),
            role: Role::Editor,
            status: MembershipStatus::Invited,
        };
        assert!(!invited.is_active());
    }

    #[tokio::test]
    async fn test_allow_all_membership() {
        let provider = AllowAllMembership;

        let membership = provider
            .get_membership("any-user", "any-workspace")
            .await
            .unwrap()
            .unwrap();

        assert_eq!(membership.role, Role::Admin);
        assert!(membership.is_active());
    }
}
