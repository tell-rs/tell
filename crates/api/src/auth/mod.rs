//! Authentication and authorization module
//!
//! Simple RBAC with 4 roles and 3 permissions.
//!
//! # Roles
//!
//! - `Viewer` - View analytics and shared content
//! - `Editor` - Create/edit own content
//! - `Admin` - Manage workspace
//! - `Platform` - Cross-workspace ops
//!
//! # Usage
//!
//! ```ignore
//! use tell_api::auth::{Permission, RouterExt};
//!
//! Router::new()
//!     .route("/dashboard", post(create_dashboard))
//!     .with_permission(Permission::Create)
//! ```

pub mod extractors;
pub mod middleware;

// Re-export core types from tell-auth
pub use tell_auth::{
    extract_jwt, is_api_token_format, AuthProvider, LocalJwtProvider, Permission, Role,
    TokenClaims, UserInfo, TOKEN_PREFIX,
};

// HTTP-specific types
pub use extractors::{require_permission, RequirePermissionLayer, RouterExt};
pub use middleware::{
    is_public_path, AuthError, AuthUser, HasAuthProvider, OptionalAuthUser, WorkspaceId,
};
