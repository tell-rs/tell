//! Workspace management endpoints
//!
//! Admin endpoints for workspace CRUD operations.
//!
//! # Auth Requirements
//!
//! | Endpoint | Auth | Permission |
//! |----------|------|------------|
//! | `GET /user/workspaces` | Required | Any member |
//! | `POST /user/workspaces` | Required | Any (if allowed) |
//! | `GET /admin/workspaces` | Required | Platform only |
//! | `GET /admin/workspaces/{id}` | Required | Admin+ |
//! | `PUT /admin/workspaces/{id}` | Required | Admin+ |
//! | `DELETE /admin/workspaces/{id}` | Required | Platform only |

use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::{delete, get, post, put},
};
use serde::{Deserialize, Serialize};

use tell_auth::Permission;
use tell_control::{MemberRole, Workspace, WorkspaceMembership};

use crate::auth::AuthUser;
use crate::error::ApiError;
use crate::state::AppState;

// =============================================================================
// Request/Response types
// =============================================================================

/// Create workspace request
#[derive(Debug, Deserialize)]
pub struct CreateWorkspaceRequest {
    pub name: String,
    pub slug: String,
}

/// Update workspace request
#[derive(Debug, Deserialize)]
pub struct UpdateWorkspaceRequest {
    pub name: Option<String>,
}

/// Workspace response
///
/// Note: clickhouse_database is intentionally omitted as it's internal infrastructure detail
#[derive(Debug, Serialize)]
pub struct WorkspaceResponse {
    pub id: String,
    pub name: String,
    pub slug: String,
    pub created_at: String,
    pub updated_at: String,
}

impl From<Workspace> for WorkspaceResponse {
    fn from(ws: Workspace) -> Self {
        Self {
            id: ws.id,
            name: ws.name,
            slug: ws.slug,
            created_at: ws.created_at.to_rfc3339(),
            updated_at: ws.updated_at.to_rfc3339(),
        }
    }
}

/// List workspaces response
#[derive(Debug, Serialize)]
pub struct ListWorkspacesResponse {
    pub workspaces: Vec<WorkspaceResponse>,
}

// =============================================================================
// User routes (any authenticated user)
// =============================================================================

/// User workspace routes
pub fn user_routes() -> Router<AppState> {
    Router::new()
        .route("/workspaces", get(list_user_workspaces))
        .route("/workspaces", post(create_workspace))
}

/// List workspaces the current user is a member of
///
/// GET /api/v1/user/workspaces
async fn list_user_workspaces(
    user: AuthUser,
    State(state): State<AppState>,
) -> Result<Json<ListWorkspacesResponse>, ApiError> {
    let control = state
        .control
        .as_ref()
        .ok_or_else(|| ApiError::internal("Control plane not initialized"))?;

    let workspaces = control
        .workspaces()
        .list_for_user(&user.id)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to list workspaces: {}", e)))?;

    Ok(Json(ListWorkspacesResponse {
        workspaces: workspaces
            .into_iter()
            .map(WorkspaceResponse::from)
            .collect(),
    }))
}

/// Create a new workspace
///
/// POST /api/v1/user/workspaces
///
/// The creating user becomes an Admin of the new workspace.
async fn create_workspace(
    user: AuthUser,
    State(state): State<AppState>,
    Json(req): Json<CreateWorkspaceRequest>,
) -> Result<(StatusCode, Json<WorkspaceResponse>), ApiError> {
    let control = state
        .control
        .as_ref()
        .ok_or_else(|| ApiError::internal("Control plane not initialized"))?;

    // Validate slug format
    if req.slug.is_empty() || req.slug.len() > 50 {
        return Err(ApiError::validation("slug", "must be 1-50 characters"));
    }
    if !req
        .slug
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-')
    {
        return Err(ApiError::validation(
            "slug",
            "must contain only alphanumeric characters and hyphens",
        ));
    }

    // Check if slug already exists
    let existing = control
        .workspaces()
        .get_by_slug(&req.slug)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to check slug: {}", e)))?;

    if existing.is_some() {
        return Err(ApiError::conflict("workspace", &req.slug));
    }

    // Create workspace
    let workspace = Workspace::new(&req.name, &req.slug);
    control
        .workspaces()
        .create(&workspace)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to create workspace: {}", e)))?;

    // Add creator as Admin
    let membership = WorkspaceMembership::new(&user.id, &workspace.id, MemberRole::Admin);
    control
        .workspaces()
        .add_member(&membership)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to add membership: {}", e)))?;

    Ok((
        StatusCode::CREATED,
        Json(WorkspaceResponse::from(workspace)),
    ))
}

// =============================================================================
// Admin routes (workspace admin or platform admin)
// =============================================================================

/// Admin workspace routes
pub fn admin_routes() -> Router<AppState> {
    Router::new()
        .route("/workspaces", get(list_all_workspaces))
        .route("/workspaces/{id}", get(get_workspace))
        .route("/workspaces/{id}", put(update_workspace))
        .route("/workspaces/{id}", delete(delete_workspace))
}

/// List all workspaces (Platform admin only)
///
/// GET /api/v1/admin/workspaces
async fn list_all_workspaces(
    user: AuthUser,
    State(state): State<AppState>,
) -> Result<Json<ListWorkspacesResponse>, ApiError> {
    // Require Platform permission
    if !user.has_permission(Permission::Platform) {
        return Err(ApiError::forbidden("Platform admin required"));
    }

    let control = state
        .control
        .as_ref()
        .ok_or_else(|| ApiError::internal("Control plane not initialized"))?;

    let workspaces = control
        .workspaces()
        .list_all()
        .await
        .map_err(|e| ApiError::internal(format!("Failed to list workspaces: {}", e)))?;

    Ok(Json(ListWorkspacesResponse {
        workspaces: workspaces
            .into_iter()
            .map(WorkspaceResponse::from)
            .collect(),
    }))
}

/// Get a workspace by ID
///
/// GET /api/v1/admin/workspaces/{id}
///
/// Requires Admin role in the workspace or Platform permission.
async fn get_workspace(
    user: AuthUser,
    Path(id): Path<String>,
    State(state): State<AppState>,
) -> Result<Json<WorkspaceResponse>, ApiError> {
    let control = state
        .control
        .as_ref()
        .ok_or_else(|| ApiError::internal("Control plane not initialized"))?;

    // Check if user has access
    let membership = control
        .workspaces()
        .get_member(&id, &user.id)
        .await
        .ok()
        .flatten();

    let has_access = user.has_permission(Permission::Platform)
        || membership.map(|m| m.role.can_manage()).unwrap_or(false);

    if !has_access {
        return Err(ApiError::forbidden("Admin access required"));
    }

    let workspace = control
        .workspaces()
        .get_by_id(&id)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to get workspace: {}", e)))?
        .ok_or_else(|| ApiError::not_found("workspace", &id))?;

    Ok(Json(WorkspaceResponse::from(workspace)))
}

/// Update a workspace
///
/// PUT /api/v1/admin/workspaces/{id}
///
/// Requires Admin role in the workspace or Platform permission.
async fn update_workspace(
    user: AuthUser,
    Path(id): Path<String>,
    State(state): State<AppState>,
    Json(req): Json<UpdateWorkspaceRequest>,
) -> Result<Json<WorkspaceResponse>, ApiError> {
    let control = state
        .control
        .as_ref()
        .ok_or_else(|| ApiError::internal("Control plane not initialized"))?;

    // Check if user has access
    let membership = control
        .workspaces()
        .get_member(&id, &user.id)
        .await
        .ok()
        .flatten();

    let has_access = user.has_permission(Permission::Platform)
        || membership.map(|m| m.role.can_manage()).unwrap_or(false);

    if !has_access {
        return Err(ApiError::forbidden("Admin access required"));
    }

    let mut workspace = control
        .workspaces()
        .get_by_id(&id)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to get workspace: {}", e)))?
        .ok_or_else(|| ApiError::not_found("workspace", &id))?;

    // Apply updates
    if let Some(name) = req.name {
        workspace.name = name;
    }

    control
        .workspaces()
        .update(&workspace)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to update workspace: {}", e)))?;

    Ok(Json(WorkspaceResponse::from(workspace)))
}

/// Delete a workspace
///
/// DELETE /api/v1/admin/workspaces/{id}
///
/// Requires Platform permission.
async fn delete_workspace(
    user: AuthUser,
    Path(id): Path<String>,
    State(state): State<AppState>,
) -> Result<StatusCode, ApiError> {
    // Require Platform permission for deletion
    if !user.has_permission(Permission::Platform) {
        return Err(ApiError::forbidden("Platform admin required"));
    }

    let control = state
        .control
        .as_ref()
        .ok_or_else(|| ApiError::internal("Control plane not initialized"))?;

    control
        .workspaces()
        .delete(&id)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to delete workspace: {}", e)))?;

    Ok(StatusCode::NO_CONTENT)
}
