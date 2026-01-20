//! User management endpoints
//!
//! Admin endpoints for managing users across the platform.
//!
//! # Routes
//!
//! - `GET /api/v1/admin/users` - List all users (Platform admin)
//! - `GET /api/v1/admin/users/{id}` - Get user by ID
//! - `POST /api/v1/admin/users` - Create a new user
//! - `PUT /api/v1/admin/users/{id}` - Update user
//! - `DELETE /api/v1/admin/users/{id}` - Delete user

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{delete, get, post, put},
};
use serde::{Deserialize, Serialize};

use tell_auth::Role;

use crate::auth::AuthUser;
use crate::error::ApiError;
use crate::state::AppState;

// =============================================================================
// Routes
// =============================================================================

/// Admin user management routes
pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/users", get(list_users))
        .route("/users", post(create_user))
        .route("/users/{id}", get(get_user))
        .route("/users/{id}", put(update_user))
        .route("/users/{id}", delete(delete_user))
}

// =============================================================================
// Request/Response types
// =============================================================================

/// User list query parameters
#[derive(Debug, Deserialize)]
pub struct ListUsersQuery {
    /// Filter by email (partial match)
    #[serde(default)]
    pub email: Option<String>,
    /// Filter by role
    #[serde(default)]
    pub role: Option<String>,
    /// Maximum results (default 100)
    #[serde(default = "default_limit")]
    pub limit: u32,
    /// Offset for pagination
    #[serde(default)]
    pub offset: u32,
}

fn default_limit() -> u32 {
    100
}

/// User response
#[derive(Debug, Serialize)]
pub struct UserResponse {
    pub id: String,
    pub email: String,
    pub role: String,
    pub created_at: String,
}

/// List users response
#[derive(Debug, Serialize)]
pub struct ListUsersResponse {
    pub users: Vec<UserResponse>,
    pub total: usize,
}

/// Create user request
#[derive(Debug, Deserialize)]
pub struct CreateUserRequest {
    pub email: String,
    pub password: String,
    #[serde(default = "default_role")]
    pub role: String,
}

fn default_role() -> String {
    "viewer".to_string()
}

/// Update user request
#[derive(Debug, Deserialize)]
pub struct UpdateUserRequest {
    #[serde(default)]
    pub email: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default)]
    pub role: Option<String>,
}

// =============================================================================
// Handlers
// =============================================================================

/// List all users (Platform admin only)
///
/// GET /api/v1/admin/users
async fn list_users(
    user: AuthUser,
    State(state): State<AppState>,
    Query(query): Query<ListUsersQuery>,
) -> Result<Json<ListUsersResponse>, ApiError> {
    // Require Platform permission
    if !user.is_platform() {
        return Err(ApiError::forbidden("Platform admin access required"));
    }

    let user_store = state
        .user_store
        .as_ref()
        .ok_or_else(|| ApiError::internal("User store not configured"))?;

    let all_users = user_store
        .list_all()
        .await
        .map_err(|e| ApiError::internal(format!("Failed to list users: {}", e)))?;

    // Apply filters
    let filtered: Vec<_> = all_users
        .into_iter()
        .filter(|u| {
            if let Some(ref email_filter) = query.email
                && !u.email.contains(email_filter)
            {
                return false;
            }
            if let Some(ref role_filter) = query.role
                && u.role.as_str() != role_filter
            {
                return false;
            }
            true
        })
        .collect();

    let total = filtered.len();

    // Apply pagination
    let users: Vec<UserResponse> = filtered
        .into_iter()
        .skip(query.offset as usize)
        .take(query.limit as usize)
        .map(|u| UserResponse {
            id: u.id,
            email: u.email,
            role: u.role.as_str().to_string(),
            created_at: u.created_at.to_rfc3339(),
        })
        .collect();

    Ok(Json(ListUsersResponse { users, total }))
}

/// Get a user by ID
///
/// GET /api/v1/admin/users/{id}
async fn get_user(
    user: AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<UserResponse>, ApiError> {
    // Require Platform permission
    if !user.is_platform() {
        return Err(ApiError::forbidden("Platform admin access required"));
    }

    let user_store = state
        .user_store
        .as_ref()
        .ok_or_else(|| ApiError::internal("User store not configured"))?;

    let found_user = user_store
        .get_by_id(&id)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to get user: {}", e)))?
        .ok_or_else(|| ApiError::not_found("user", &id))?;

    Ok(Json(UserResponse {
        id: found_user.id,
        email: found_user.email,
        role: found_user.role.as_str().to_string(),
        created_at: found_user.created_at.to_rfc3339(),
    }))
}

/// Create a new user (Platform admin only)
///
/// POST /api/v1/admin/users
async fn create_user(
    user: AuthUser,
    State(state): State<AppState>,
    Json(req): Json<CreateUserRequest>,
) -> Result<(StatusCode, Json<UserResponse>), ApiError> {
    // Require Platform permission
    if !user.is_platform() {
        return Err(ApiError::forbidden("Platform admin access required"));
    }

    let user_store = state
        .user_store
        .as_ref()
        .ok_or_else(|| ApiError::internal("User store not configured"))?;

    // Validate email
    if req.email.is_empty() || !req.email.contains('@') {
        return Err(ApiError::validation("email", "invalid email address"));
    }

    // Validate password
    if req.password.len() < 8 {
        return Err(ApiError::validation(
            "password",
            "must be at least 8 characters",
        ));
    }

    // Parse role
    let role = parse_role(&req.role)?;

    // Check if user already exists
    if user_store
        .get_by_email(&req.email)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to check email: {}", e)))?
        .is_some()
    {
        return Err(ApiError::conflict("user", "email already exists"));
    }

    // Create user
    let new_user = user_store
        .create_user(&req.email, &req.password, role)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to create user: {}", e)))?;

    Ok((
        StatusCode::CREATED,
        Json(UserResponse {
            id: new_user.id,
            email: new_user.email,
            role: new_user.role.as_str().to_string(),
            created_at: new_user.created_at.to_rfc3339(),
        }),
    ))
}

/// Update a user (Platform admin only)
///
/// PUT /api/v1/admin/users/{id}
async fn update_user(
    user: AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<UpdateUserRequest>,
) -> Result<Json<UserResponse>, ApiError> {
    // Require Platform permission
    if !user.is_platform() {
        return Err(ApiError::forbidden("Platform admin access required"));
    }

    let user_store = state
        .user_store
        .as_ref()
        .ok_or_else(|| ApiError::internal("User store not configured"))?;

    // Get existing user
    let mut existing = user_store
        .get_by_id(&id)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to get user: {}", e)))?
        .ok_or_else(|| ApiError::not_found("user", &id))?;

    // Update fields
    if let Some(email) = req.email {
        if email.is_empty() || !email.contains('@') {
            return Err(ApiError::validation("email", "invalid email address"));
        }
        // Check if new email is already taken by another user
        if let Some(other) = user_store
            .get_by_email(&email)
            .await
            .map_err(|e| ApiError::internal(format!("Failed to check email: {}", e)))?
            && other.id != id
        {
            return Err(ApiError::conflict("user", "email already exists"));
        }
        existing.email = email;
    }

    if let Some(role_str) = req.role {
        existing.role = parse_role(&role_str)?;
    }

    // Update password if provided
    if let Some(password) = req.password {
        if password.len() < 8 {
            return Err(ApiError::validation(
                "password",
                "must be at least 8 characters",
            ));
        }
        user_store
            .update_password(&id, &password)
            .await
            .map_err(|e| ApiError::internal(format!("Failed to update password: {}", e)))?;
    }

    // Update user
    user_store
        .update(&existing)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to update user: {}", e)))?;

    Ok(Json(UserResponse {
        id: existing.id,
        email: existing.email,
        role: existing.role.as_str().to_string(),
        created_at: existing.created_at.to_rfc3339(),
    }))
}

/// Delete a user (Platform admin only)
///
/// DELETE /api/v1/admin/users/{id}
async fn delete_user(
    user: AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    // Require Platform permission
    if !user.is_platform() {
        return Err(ApiError::forbidden("Platform admin access required"));
    }

    // Prevent self-deletion
    if id == user.id {
        return Err(ApiError::validation("id", "cannot delete your own account"));
    }

    let user_store = state
        .user_store
        .as_ref()
        .ok_or_else(|| ApiError::internal("User store not configured"))?;

    // Check user exists
    user_store
        .get_by_id(&id)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to get user: {}", e)))?
        .ok_or_else(|| ApiError::not_found("user", &id))?;

    // Delete user
    user_store
        .delete(&id)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to delete user: {}", e)))?;

    Ok(StatusCode::NO_CONTENT)
}

// =============================================================================
// Helpers
// =============================================================================

fn parse_role(role_str: &str) -> Result<Role, ApiError> {
    match role_str.to_lowercase().as_str() {
        "viewer" => Ok(Role::Viewer),
        "editor" => Ok(Role::Editor),
        "admin" => Ok(Role::Admin),
        "platform" => Ok(Role::Platform),
        _ => Err(ApiError::validation(
            "role",
            "must be one of: viewer, editor, admin, platform",
        )),
    }
}
