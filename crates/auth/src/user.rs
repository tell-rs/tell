//! Authenticated user information
//!
//! UserInfo represents the authenticated user in a request context.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::claims::TokenClaims;
use crate::roles::{Permission, Role};

/// Authenticated user information
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UserInfo {
    /// User ID
    pub id: String,

    /// Email address
    pub email: String,

    /// First name
    #[serde(default)]
    pub first_name: String,

    /// Last name
    #[serde(default)]
    pub last_name: String,

    /// Username (optional)
    #[serde(default)]
    pub username: Option<String>,

    /// User's role (stored as string for serialization)
    #[serde(default = "default_role")]
    pub role: String,

    /// Additional metadata
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

fn default_role() -> String {
    "viewer".to_string()
}

impl UserInfo {
    /// Create a new user with minimal info (defaults to Viewer role)
    pub fn new(id: impl Into<String>, email: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            email: email.into(),
            first_name: String::new(),
            last_name: String::new(),
            username: None,
            role: "viewer".to_string(),
            metadata: HashMap::new(),
        }
    }

    /// Create a new user with a specific role
    pub fn with_role(id: impl Into<String>, email: impl Into<String>, role: Role) -> Self {
        Self {
            id: id.into(),
            email: email.into(),
            first_name: String::new(),
            last_name: String::new(),
            username: None,
            role: role.as_str().to_string(),
            metadata: HashMap::new(),
        }
    }

    /// Get the user's full name
    pub fn full_name(&self) -> String {
        match (self.first_name.as_str(), self.last_name.as_str()) {
            ("", "") => self.username.clone().unwrap_or_else(|| self.email.clone()),
            (first, "") => first.to_string(),
            ("", last) => last.to_string(),
            (first, last) => format!("{} {}", first, last),
        }
    }

    /// Get parsed role (defaults to Viewer if invalid)
    pub fn parsed_role(&self) -> Role {
        Role::parse(&self.role).unwrap_or(Role::Viewer)
    }

    /// Check if user has a specific permission
    pub fn has_permission(&self, permission: Permission) -> bool {
        self.parsed_role().has_permission(permission)
    }

    /// Check if user can create content
    pub fn can_create(&self) -> bool {
        self.has_permission(Permission::Create)
    }

    /// Check if user is admin (can manage workspace)
    pub fn is_admin(&self) -> bool {
        self.has_permission(Permission::Admin)
    }

    /// Check if user is platform admin
    pub fn is_platform(&self) -> bool {
        self.has_permission(Permission::Platform)
    }

    /// Get workspace scope from metadata (if set by API key)
    pub fn api_key_workspace_scope(&self) -> Option<&str> {
        self.metadata.get("api_key_workspace_scope").map(|s| s.as_str())
    }

    /// Get auth method from metadata
    pub fn auth_method(&self) -> Option<&str> {
        self.metadata.get("auth_method").map(|s| s.as_str())
    }

    /// Create UserInfo from JWT claims
    pub fn from_claims(claims: &TokenClaims) -> Self {
        let mut metadata = HashMap::new();
        metadata.insert("token_id".to_string(), claims.token_id.clone());
        metadata.insert("workspace_id".to_string(), claims.workspace_id.clone());
        metadata.insert("auth_method".to_string(), "jwt".to_string());

        Self {
            id: claims.user_id.clone(),
            email: claims.email.clone(),
            first_name: String::new(),
            last_name: String::new(),
            username: None,
            role: claims.role.clone(),
            metadata,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_user() {
        let user = UserInfo::new("user-1", "test@example.com");
        assert_eq!(user.id, "user-1");
        assert_eq!(user.email, "test@example.com");
        assert_eq!(user.parsed_role(), Role::Viewer);
    }

    #[test]
    fn test_with_role() {
        let user = UserInfo::with_role("user-1", "test@example.com", Role::Editor);
        assert_eq!(user.parsed_role(), Role::Editor);
        assert!(user.can_create());
        assert!(!user.is_admin());
    }

    #[test]
    fn test_full_name() {
        let mut user = UserInfo::new("1", "test@example.com");
        assert_eq!(user.full_name(), "test@example.com");

        user.first_name = "John".to_string();
        assert_eq!(user.full_name(), "John");

        user.last_name = "Doe".to_string();
        assert_eq!(user.full_name(), "John Doe");
    }

    #[test]
    fn test_permission_checks() {
        let viewer = UserInfo::new("1", "viewer@example.com");
        assert!(!viewer.can_create());
        assert!(!viewer.is_admin());

        let editor = UserInfo::with_role("2", "editor@example.com", Role::Editor);
        assert!(editor.can_create());
        assert!(!editor.is_admin());

        let admin = UserInfo::with_role("3", "admin@example.com", Role::Admin);
        assert!(admin.can_create());
        assert!(admin.is_admin());
        assert!(!admin.is_platform());

        let platform = UserInfo::with_role("4", "platform@example.com", Role::Platform);
        assert!(platform.can_create());
        assert!(platform.is_admin());
        assert!(platform.is_platform());
    }

    #[test]
    fn test_role_parsing_fallback() {
        let mut user = UserInfo::new("1", "test@example.com");
        user.role = "invalid_role".to_string();
        assert_eq!(user.parsed_role(), Role::Viewer); // Falls back to Viewer
    }
}
