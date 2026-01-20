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
    AuthProvider, LocalJwtProvider, Permission, Role, TOKEN_PREFIX, TokenClaims, UserInfo,
    extract_jwt, is_api_token_format,
};

// HTTP-specific types
pub use extractors::{RequirePermissionLayer, RouterExt, require_permission};
pub use middleware::{
    AuthError, AuthUser, HasAuthProvider, OptionalAuthUser, WorkspaceId, is_public_path,
};
