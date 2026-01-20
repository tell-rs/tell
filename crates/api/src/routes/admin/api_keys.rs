//! API key management endpoints
//!
//! Endpoints for managing user API keys for programmatic access.
//!
//! # Routes
//!
//! ## User routes (manage own keys)
//! - `GET /api/v1/user/apikeys` - List own API keys
//! - `POST /api/v1/user/apikeys` - Create an API key
//! - `DELETE /api/v1/user/apikeys/{id}` - Revoke own API key
//!
//! ## Admin routes (manage all keys in workspace)
//! - `GET /api/v1/admin/apikeys` - List all workspace API keys
//! - `DELETE /api/v1/admin/apikeys/{id}` - Revoke any API key

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{delete, get, post},
    Json, Router,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use tell_auth::Permission;
use tell_control::UserApiKey;

use crate::auth::AuthUser;
use crate::error::ApiError;
use crate::state::AppState;

// =============================================================================
// Request/Response types
// =============================================================================

/// Create API key request
#[derive(Debug, Deserialize)]
pub struct CreateApiKeyRequest {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    /// Workspace to scope the key to (empty = user-level key)
    #[serde(default)]
    pub workspace_id: Option<String>,
    /// Permissions for the key (empty = inherit from user)
    #[serde(default)]
    pub permissions: Vec<String>,
    /// When the key expires (None = never)
    #[serde(default)]
    pub expires_at: Option<DateTime<Utc>>,
}

/// API key response (never includes the actual key)
#[derive(Debug, Serialize)]
pub struct ApiKeyResponse {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub user_id: String,
    pub workspace_id: String,
    /// Key prefix for identification (first 8 chars)
    pub key_prefix: String,
    pub permissions: Vec<String>,
    pub active: bool,
    pub last_used_at: Option<String>,
    pub expires_at: Option<String>,
    pub created_at: String,
}

impl From<UserApiKey> for ApiKeyResponse {
    fn from(key: UserApiKey) -> Self {
        Self {
            id: key.id,
            name: key.name,
            description: key.description,
            user_id: key.user_id,
            workspace_id: key.workspace_id,
            key_prefix: key.key_prefix,
            permissions: key.permissions,
            active: key.active,
            last_used_at: key.last_used_at.map(|dt| dt.to_rfc3339()),
            expires_at: key.expires_at.map(|dt| dt.to_rfc3339()),
            created_at: key.created_at.to_rfc3339(),
        }
    }
}

/// Create API key response (includes the key ONCE)
#[derive(Debug, Serialize)]
pub struct CreateApiKeyResponse {
    /// The actual API key (only shown once!)
    pub key: String,
    #[serde(flatten)]
    pub api_key: ApiKeyResponse,
}

/// List API keys response
#[derive(Debug, Serialize)]
pub struct ListApiKeysResponse {
    pub api_keys: Vec<ApiKeyResponse>,
    pub count: usize,
}

/// Query parameters for listing keys
#[derive(Debug, Deserialize)]
pub struct ListApiKeysQuery {
    #[serde(default)]
    pub workspace_id: Option<String>,
}

// =============================================================================
// User routes (manage own keys)
// =============================================================================

/// User API key routes
pub fn user_routes() -> Router<AppState> {
    Router::new()
        .route("/apikeys", get(list_user_api_keys))
        .route("/apikeys", post(create_user_api_key))
        .route("/apikeys/{id}", delete(revoke_user_api_key))
}

/// List API keys for the current user
///
/// GET /api/v1/user/apikeys?workspace_id={optional}
async fn list_user_api_keys(
    user: AuthUser,
    Query(query): Query<ListApiKeysQuery>,
    State(state): State<AppState>,
) -> Result<Json<ListApiKeysResponse>, ApiError> {
    let control = state
        .control
        .as_ref()
        .ok_or_else(|| ApiError::internal("Control plane not initialized"))?;

    let api_keys = if let Some(workspace_id) = query.workspace_id {
        // Check user has access to workspace
        check_workspace_membership(control, &workspace_id, &user.id).await?;
        control
            .api_keys()
            .list_for_user_in_workspace(&user.id, &workspace_id)
            .await
            .map_err(|e| ApiError::internal(format!("Failed to list API keys: {}", e)))?
    } else {
        control
            .api_keys()
            .list_for_user(&user.id)
            .await
            .map_err(|e| ApiError::internal(format!("Failed to list API keys: {}", e)))?
    };

    let count = api_keys.len();
    Ok(Json(ListApiKeysResponse {
        api_keys: api_keys.into_iter().map(ApiKeyResponse::from).collect(),
        count,
    }))
}

/// Create a new API key for the current user
///
/// POST /api/v1/user/apikeys
async fn create_user_api_key(
    user: AuthUser,
    State(state): State<AppState>,
    Json(req): Json<CreateApiKeyRequest>,
) -> Result<(StatusCode, Json<CreateApiKeyResponse>), ApiError> {
    let control = state
        .control
        .as_ref()
        .ok_or_else(|| ApiError::internal("Control plane not initialized"))?;

    // Validate name
    if req.name.is_empty() || req.name.len() > 100 {
        return Err(ApiError::validation("name", "must be 1-100 characters"));
    }

    // Validate description
    if let Some(ref desc) = req.description {
        if desc.len() > 500 {
            return Err(ApiError::validation(
                "description",
                "must be at most 500 characters",
            ));
        }
    }

    // If workspace_id provided, check user has access
    let workspace_id = if let Some(ref ws_id) = req.workspace_id {
        check_workspace_membership(control, ws_id, &user.id).await?;
        ws_id.clone()
    } else {
        String::new()
    };

    // Validate expiry (max 1 year)
    if let Some(expires) = req.expires_at {
        let max_expiry = Utc::now() + chrono::Duration::days(365);
        if expires > max_expiry {
            return Err(ApiError::validation(
                "expires_at",
                "must be within 1 year from now",
            ));
        }
        if expires < Utc::now() {
            return Err(ApiError::validation("expires_at", "must be in the future"));
        }
    }

    // Create the API key
    let (key, mut api_key) = UserApiKey::new(
        &user.id,
        &workspace_id,
        &req.name,
        req.permissions,
        req.expires_at,
    );

    if let Some(desc) = req.description {
        api_key = api_key.with_description(&desc);
    }

    control
        .api_keys()
        .create(&api_key)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to create API key: {}", e)))?;

    Ok((
        StatusCode::CREATED,
        Json(CreateApiKeyResponse {
            key,
            api_key: ApiKeyResponse::from(api_key),
        }),
    ))
}

/// Revoke an API key owned by the current user
///
/// DELETE /api/v1/user/apikeys/{id}
async fn revoke_user_api_key(
    user: AuthUser,
    Path(id): Path<String>,
    State(state): State<AppState>,
) -> Result<StatusCode, ApiError> {
    let control = state
        .control
        .as_ref()
        .ok_or_else(|| ApiError::internal("Control plane not initialized"))?;

    // Get the API key
    let api_key = control
        .api_keys()
        .get_by_id(&id)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to get API key: {}", e)))?
        .ok_or_else(|| ApiError::not_found("api_key", &id))?;

    // Check ownership
    if api_key.user_id != user.id {
        return Err(ApiError::forbidden("Not the owner of this API key"));
    }

    // Revoke the key
    control
        .api_keys()
        .revoke(&id)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to revoke API key: {}", e)))?;

    Ok(StatusCode::NO_CONTENT)
}

// =============================================================================
// Admin routes (manage all keys in workspace)
// =============================================================================

/// Admin API key routes
pub fn admin_routes() -> Router<AppState> {
    Router::new()
        .route("/apikeys", get(list_workspace_api_keys))
        .route("/apikeys/{id}", delete(revoke_workspace_api_key))
}

/// List all API keys in a workspace
///
/// GET /api/v1/admin/apikeys?workspace_id={required}
///
/// Requires Admin+ permission in the workspace.
async fn list_workspace_api_keys(
    user: AuthUser,
    Query(query): Query<ListApiKeysQuery>,
    State(state): State<AppState>,
) -> Result<Json<ListApiKeysResponse>, ApiError> {
    let workspace_id = query
        .workspace_id
        .ok_or_else(|| ApiError::validation("workspace_id", "is required"))?;

    let control = state
        .control
        .as_ref()
        .ok_or_else(|| ApiError::internal("Control plane not initialized"))?;

    // Check user has admin access to workspace
    check_workspace_admin(control, &workspace_id, &user).await?;

    let api_keys = control
        .api_keys()
        .list_for_workspace(&workspace_id)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to list API keys: {}", e)))?;

    let count = api_keys.len();
    Ok(Json(ListApiKeysResponse {
        api_keys: api_keys.into_iter().map(ApiKeyResponse::from).collect(),
        count,
    }))
}

/// Revoke any API key in a workspace (admin only)
///
/// DELETE /api/v1/admin/apikeys/{id}?workspace_id={required}
///
/// Requires Admin+ permission in the workspace.
async fn revoke_workspace_api_key(
    user: AuthUser,
    Path(id): Path<String>,
    Query(query): Query<ListApiKeysQuery>,
    State(state): State<AppState>,
) -> Result<StatusCode, ApiError> {
    let workspace_id = query
        .workspace_id
        .ok_or_else(|| ApiError::validation("workspace_id", "is required"))?;

    let control = state
        .control
        .as_ref()
        .ok_or_else(|| ApiError::internal("Control plane not initialized"))?;

    // Check user has admin access to workspace
    check_workspace_admin(control, &workspace_id, &user).await?;

    // Get the API key
    let api_key = control
        .api_keys()
        .get_by_id(&id)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to get API key: {}", e)))?
        .ok_or_else(|| ApiError::not_found("api_key", &id))?;

    // Verify the key belongs to this workspace
    if api_key.workspace_id != workspace_id {
        return Err(ApiError::not_found("api_key", &id));
    }

    // Revoke the key
    control
        .api_keys()
        .revoke(&id)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to revoke API key: {}", e)))?;

    Ok(StatusCode::NO_CONTENT)
}

// =============================================================================
// Helper functions
// =============================================================================

/// Check if user is a member of the workspace
async fn check_workspace_membership(
    control: &tell_control::ControlPlane,
    workspace_id: &str,
    user_id: &str,
) -> Result<(), ApiError> {
    let membership = control
        .workspaces()
        .get_member(workspace_id, user_id)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to check membership: {}", e)))?;

    if membership.is_none() {
        return Err(ApiError::forbidden("Not a workspace member"));
    }

    Ok(())
}

/// Check if user has admin access to the workspace
async fn check_workspace_admin(
    control: &tell_control::ControlPlane,
    workspace_id: &str,
    user: &AuthUser,
) -> Result<(), ApiError> {
    // Platform admins can access any workspace
    if user.has_permission(Permission::Platform) {
        return Ok(());
    }

    // Check workspace admin role
    let membership = control
        .workspaces()
        .get_member(workspace_id, &user.id)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to check membership: {}", e)))?;

    match membership {
        Some(m) if m.role.can_manage() => Ok(()),
        _ => Err(ApiError::forbidden("Admin access required")),
    }
}
