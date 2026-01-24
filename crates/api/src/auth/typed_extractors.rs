//! Type-safe authentication extractors
//!
//! Provides `Auth<P>` and `Workspace<P>` extractors that enforce permissions
//! at extraction time, before handlers run.
//!
//! # Example
//!
//! ```ignore
//! use tell_api::auth::{Auth, Workspace, CanCreate, CanAdmin};
//!
//! // Any authenticated user
//! async fn get_profile(auth: Auth) -> impl IntoResponse { }
//!
//! // Must be Editor+ globally
//! async fn create_api_key(auth: Auth<CanCreate>) -> impl IntoResponse { }
//!
//! // Must be workspace member
//! async fn list_dashboards(ws: Workspace) -> impl IntoResponse { }
//!
//! // Must be Editor+ in workspace
//! async fn create_dashboard(ws: Workspace<CanCreate>) -> impl IntoResponse { }
//!
//! // Must be Admin+ in workspace
//! async fn invite_member(ws: Workspace<CanAdmin>) -> impl IntoResponse { }
//! ```

use std::marker::PhantomData;
use std::sync::Arc;

use axum::{
    Json,
    extract::FromRequestParts,
    http::{StatusCode, request::Parts},
    response::{IntoResponse, Response},
};
use serde::Deserialize;

use tell_auth::{
    AuthContext, AuthError as TellAuthError, MembershipProvider, Permission, Role, UserInfo,
    WorkspaceAccess,
};

use super::middleware::{HasAuthProvider, extract_token};

// ============================================================================
// Permission Level Markers
// ============================================================================

/// Trait for permission level markers
///
/// Implement this on marker types to specify what permission is required.
pub trait PermissionLevel: Send + Sync + 'static {
    /// The permission required, or None for any authenticated user
    const REQUIRED: Option<Permission>;
}

/// Any authenticated user (no specific permission required)
pub struct AnyUser;

/// Requires Create permission (Editor+)
pub struct CanCreate;

/// Requires Admin permission (Admin+)
pub struct CanAdmin;

/// Requires Platform permission (Platform only)
pub struct CanPlatform;

impl PermissionLevel for AnyUser {
    const REQUIRED: Option<Permission> = None;
}

impl PermissionLevel for CanCreate {
    const REQUIRED: Option<Permission> = Some(Permission::Create);
}

impl PermissionLevel for CanAdmin {
    const REQUIRED: Option<Permission> = Some(Permission::Admin);
}

impl PermissionLevel for CanPlatform {
    const REQUIRED: Option<Permission> = Some(Permission::Platform);
}

// ============================================================================
// Error Types
// ============================================================================

/// Error returned by typed auth extractors
#[derive(Debug)]
pub enum TypedAuthError {
    /// No token provided
    MissingToken,
    /// Token is invalid
    InvalidToken,
    /// Token has expired
    TokenExpired,
    /// Missing workspace ID header
    MissingWorkspace,
    /// User is not a member of the workspace
    NotWorkspaceMember,
    /// Membership is not active (invited/removed)
    MembershipInactive,
    /// Insufficient permission
    InsufficientPermission(Permission),
    /// Internal error
    Internal(String),
}

impl IntoResponse for TypedAuthError {
    fn into_response(self) -> Response {
        let (status, code, message) = match self {
            Self::MissingToken => (
                StatusCode::UNAUTHORIZED,
                "AUTH_REQUIRED",
                "Authentication required".to_string(),
            ),
            Self::InvalidToken => (
                StatusCode::UNAUTHORIZED,
                "INVALID_TOKEN",
                "Invalid authentication token".to_string(),
            ),
            Self::TokenExpired => (
                StatusCode::UNAUTHORIZED,
                "TOKEN_EXPIRED",
                "Authentication token has expired".to_string(),
            ),
            Self::MissingWorkspace => (
                StatusCode::BAD_REQUEST,
                "MISSING_WORKSPACE",
                "X-Workspace-ID header is required".to_string(),
            ),
            Self::NotWorkspaceMember => (
                StatusCode::FORBIDDEN,
                "NOT_MEMBER",
                "You are not a member of this workspace".to_string(),
            ),
            Self::MembershipInactive => (
                StatusCode::FORBIDDEN,
                "MEMBERSHIP_INACTIVE",
                "Your workspace membership is not active".to_string(),
            ),
            Self::InsufficientPermission(p) => (
                StatusCode::FORBIDDEN,
                "INSUFFICIENT_PERMISSION",
                format!("This action requires {} permission", p.as_str()),
            ),
            Self::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL_ERROR", msg),
        };

        let body = serde_json::json!({
            "error": code,
            "message": message,
        });

        (status, Json(body)).into_response()
    }
}

impl From<TellAuthError> for TypedAuthError {
    fn from(e: TellAuthError) -> Self {
        match e {
            TellAuthError::MissingToken => Self::MissingToken,
            TellAuthError::TokenExpired => Self::TokenExpired,
            TellAuthError::InvalidTokenFormat
            | TellAuthError::InvalidSignature
            | TellAuthError::InvalidClaims(_) => Self::InvalidToken,
            TellAuthError::WorkspaceAccessDenied(_) => Self::NotWorkspaceMember,
            _ => Self::Internal(e.to_string()),
        }
    }
}

// ============================================================================
// Traits for App State
// ============================================================================

/// Trait for app state that provides a membership provider
pub trait HasMembershipProvider: Send + Sync {
    /// Get the membership provider
    fn membership_provider(&self) -> Option<Arc<dyn MembershipProvider>>;
}

// ============================================================================
// Auth<P> Extractor
// ============================================================================

/// Type-safe authentication extractor
///
/// Extracts and validates the user from the request. The type parameter `P`
/// specifies the required permission level.
///
/// # Type Parameters
///
/// - `AnyUser` - Any authenticated user (default)
/// - `CanCreate` - Requires Editor+ role
/// - `CanAdmin` - Requires Admin+ role
/// - `CanPlatform` - Requires Platform role
///
/// # Example
///
/// ```ignore
/// // Any authenticated user
/// async fn handler(auth: Auth) -> impl IntoResponse { }
///
/// // Requires Editor+
/// async fn handler(auth: Auth<CanCreate>) -> impl IntoResponse { }
/// ```
#[derive(Debug, Clone)]
pub struct Auth<P: PermissionLevel = AnyUser> {
    /// The authentication context
    pub ctx: AuthContext,
    _level: PhantomData<P>,
}

impl<P: PermissionLevel> Auth<P> {
    /// Get the authenticated user
    pub fn user(&self) -> &UserInfo {
        &self.ctx.user
    }

    /// Get the user ID
    pub fn user_id(&self) -> &str {
        &self.ctx.user.id
    }

    /// Get the user's email
    pub fn email(&self) -> &str {
        &self.ctx.user.email
    }

    /// Get the user's role
    pub fn role(&self) -> Role {
        Role::parse(&self.ctx.user.role).unwrap_or(Role::Viewer)
    }

    /// Check if user has a permission
    pub fn has_permission(&self, permission: Permission) -> bool {
        self.ctx.has_permission(permission)
    }
}

impl<P: PermissionLevel> std::ops::Deref for Auth<P> {
    type Target = AuthContext;

    fn deref(&self) -> &Self::Target {
        &self.ctx
    }
}

impl<S, P> FromRequestParts<S> for Auth<P>
where
    S: HasAuthProvider + Send + Sync,
    P: PermissionLevel,
{
    type Rejection = TypedAuthError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        // Extract and validate token
        let token = extract_token(parts).ok_or(TypedAuthError::MissingToken)?;

        let provider = state.auth_provider();
        let user = provider.validate(&token).await.map_err(|e| match e {
            TellAuthError::TokenExpired => TypedAuthError::TokenExpired,
            TellAuthError::MissingToken => TypedAuthError::MissingToken,
            _ => TypedAuthError::InvalidToken,
        })?;

        // Check permission if required
        if let Some(required) = P::REQUIRED
            && !user.has_permission(required)
        {
            return Err(TypedAuthError::InsufficientPermission(required));
        }

        Ok(Auth {
            ctx: AuthContext::new(user),
            _level: PhantomData,
        })
    }
}

// ============================================================================
// Workspace<P> Extractor
// ============================================================================

/// Extract workspace ID from request headers or query params
fn extract_workspace_id(parts: &Parts) -> Option<String> {
    // Try X-Workspace-ID header first
    if let Some(header) = parts.headers.get("x-workspace-id")
        && header.len() <= 64
        && let Ok(s) = header.to_str()
    {
        return Some(s.to_string());
    }

    // Try query parameter
    if let Some(query) = parts.uri.query()
        && query.len() <= 1024
    {
        #[derive(Deserialize)]
        struct WsQuery {
            workspace_id: Option<String>,
        }
        if let Ok(params) = serde_urlencoded::from_str::<WsQuery>(query)
            && let Some(ws) = params.workspace_id
        {
            return Some(ws);
        }
    }

    None
}

/// Type-safe workspace extractor
///
/// Validates that the authenticated user has access to the requested workspace.
/// The type parameter `P` specifies the required permission level within the workspace.
///
/// # Guarantees
///
/// When you have a `Workspace<P>`, it means:
/// 1. User is authenticated
/// 2. Workspace ID was provided (X-Workspace-ID header)
/// 3. User is an active member of the workspace
/// 4. User has the required permission in the workspace
///
/// # Type Parameters
///
/// - `AnyUser` - Any workspace member (default)
/// - `CanCreate` - Requires Editor+ role in workspace
/// - `CanAdmin` - Requires Admin+ role in workspace
/// - `CanPlatform` - Requires Platform role
///
/// # Example
///
/// ```ignore
/// // Any workspace member
/// async fn list_dashboards(ws: Workspace) -> impl IntoResponse { }
///
/// // Requires Editor+ in workspace
/// async fn create_dashboard(ws: Workspace<CanCreate>) -> impl IntoResponse { }
///
/// // Requires Admin+ in workspace
/// async fn manage_members(ws: Workspace<CanAdmin>) -> impl IntoResponse { }
/// ```
#[derive(Debug, Clone)]
pub struct Workspace<P: PermissionLevel = AnyUser> {
    /// The authentication context (includes workspace)
    pub ctx: AuthContext,
    _level: PhantomData<P>,
}

impl<P: PermissionLevel> Workspace<P> {
    /// Get the workspace access (guaranteed to exist)
    pub fn access(&self) -> &WorkspaceAccess {
        // Safe: we validate workspace exists during extraction
        self.ctx.workspace.as_ref().unwrap()
    }

    /// Get the workspace ID
    pub fn id(&self) -> &str {
        &self.access().id
    }

    /// Get the user
    pub fn user(&self) -> &UserInfo {
        &self.ctx.user
    }

    /// Get the user ID
    pub fn user_id(&self) -> &str {
        &self.ctx.user.id
    }

    /// Get the user's role in this workspace
    pub fn role(&self) -> Role {
        self.access().role
    }

    /// Check if user has a permission in this workspace
    pub fn has_permission(&self, permission: Permission) -> bool {
        self.access().has_permission(permission)
    }

    /// Check if user is admin in this workspace
    pub fn is_admin(&self) -> bool {
        self.access().is_admin()
    }
}

impl<P: PermissionLevel> std::ops::Deref for Workspace<P> {
    type Target = AuthContext;

    fn deref(&self) -> &Self::Target {
        &self.ctx
    }
}

impl<S, P> FromRequestParts<S> for Workspace<P>
where
    S: HasAuthProvider + HasMembershipProvider + Send + Sync,
    P: PermissionLevel,
{
    type Rejection = TypedAuthError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        // 1. Authenticate user
        let token = extract_token(parts).ok_or(TypedAuthError::MissingToken)?;

        let provider = state.auth_provider();
        let user = provider.validate(&token).await.map_err(|e| match e {
            TellAuthError::TokenExpired => TypedAuthError::TokenExpired,
            TellAuthError::MissingToken => TypedAuthError::MissingToken,
            _ => TypedAuthError::InvalidToken,
        })?;

        // 2. Extract workspace ID
        let workspace_id = extract_workspace_id(parts).ok_or(TypedAuthError::MissingWorkspace)?;

        // 3. Get membership provider
        let membership_provider = state
            .membership_provider()
            .ok_or_else(|| TypedAuthError::Internal("No membership provider configured".into()))?;

        // 4. Validate membership
        let membership = membership_provider
            .get_membership(&user.id, &workspace_id)
            .await
            .map_err(|e| TypedAuthError::Internal(e.to_string()))?
            .ok_or(TypedAuthError::NotWorkspaceMember)?;

        if !membership.is_active() {
            return Err(TypedAuthError::MembershipInactive);
        }

        let workspace = WorkspaceAccess::new(workspace_id, membership.role);

        // 5. Check permission in workspace context
        if let Some(required) = P::REQUIRED
            && !workspace.has_permission(required)
        {
            return Err(TypedAuthError::InsufficientPermission(required));
        }

        Ok(Workspace {
            ctx: AuthContext::with_workspace(user, workspace),
            _level: PhantomData,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_permission_levels() {
        assert_eq!(AnyUser::REQUIRED, None);
        assert_eq!(CanCreate::REQUIRED, Some(Permission::Create));
        assert_eq!(CanAdmin::REQUIRED, Some(Permission::Admin));
        assert_eq!(CanPlatform::REQUIRED, Some(Permission::Platform));
    }

    #[test]
    fn test_typed_auth_error_response() {
        let error = TypedAuthError::InsufficientPermission(Permission::Admin);
        let response = error.into_response();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[test]
    fn test_typed_auth_error_from_tell_auth() {
        let expired = TypedAuthError::from(TellAuthError::TokenExpired);
        assert!(matches!(expired, TypedAuthError::TokenExpired));

        let missing = TypedAuthError::from(TellAuthError::MissingToken);
        assert!(matches!(missing, TypedAuthError::MissingToken));
    }
}
