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
//! ## Type-safe extractors (recommended)
//!
//! ```ignore
//! use tell_api::auth::{Auth, Workspace, CanCreate, CanAdmin};
//!
//! // Any authenticated user
//! async fn get_profile(auth: Auth) -> impl IntoResponse { }
//!
//! // Must be Editor+ in workspace
//! async fn create_dashboard(ws: Workspace<CanCreate>) -> impl IntoResponse { }
//!
//! // Must be Admin+ in workspace
//! async fn manage_members(ws: Workspace<CanAdmin>) -> impl IntoResponse { }
//! ```
//!
//! ## Legacy permission layer
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
pub mod typed_extractors;

// Re-export core types from tell-auth
pub use tell_auth::{
    AuthContext, AuthProvider, LocalJwtProvider, MembershipProvider, Permission, Role,
    TOKEN_PREFIX, TokenClaims, UserInfo, WorkspaceAccess, extract_jwt, is_api_token_format,
};

// Type-safe extractors (recommended)
pub use typed_extractors::{
    AnyUser, Auth, CanAdmin, CanCreate, CanPlatform, HasMembershipProvider, PermissionLevel,
    TypedAuthError, Workspace,
};

// Legacy extractors (for backwards compatibility)
pub use extractors::{RequirePermissionLayer, RouterExt, require_permission};
pub use middleware::{
    AuthError, AuthUser, HasAuthProvider, HasUserStore, OptionalAuthUser,
    OptionalValidatedWorkspace, ValidatedWorkspace, WorkspaceId, extract_token, is_public_path,
};

// Re-export workspace/membership types
pub use tell_auth::{AllowAllMembership, Membership, MembershipStatus, WorkspaceMembership};
