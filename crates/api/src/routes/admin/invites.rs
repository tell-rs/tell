//! Workspace invite endpoints
//!
//! Endpoints for managing workspace invitations.
//!
//! # Admin Routes (require workspace admin)
//!
//! - `POST /api/v1/admin/workspace/invites` - Create an invite
//! - `GET /api/v1/admin/workspace/invites` - List invites
//! - `DELETE /api/v1/admin/workspace/invites/{id}` - Cancel an invite
//!
//! # Public Routes (no auth required)
//!
//! - `GET /api/v1/invites/{token}` - Verify an invite
//! - `POST /api/v1/invites/{token}/accept` - Accept an invite

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{delete, get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};

use tell_auth::Permission;
use tell_control::{InviteStatus, MemberRole, WorkspaceInvite, WorkspaceMembership};

use crate::auth::{AuthUser, OptionalAuthUser};
use crate::error::ApiError;
use crate::state::AppState;

// =============================================================================
// Routes
// =============================================================================

/// Admin invite routes (workspace-scoped)
pub fn admin_routes() -> Router<AppState> {
    Router::new()
        .route("/workspace/invites", post(create_invite))
        .route("/workspace/invites", get(list_invites))
        .route("/workspace/invites/{id}", delete(cancel_invite))
}

/// Public invite routes (for accepting invites)
pub fn public_routes() -> Router<AppState> {
    Router::new()
        .route("/invites/{token}", get(verify_invite))
        .route("/invites/{token}/accept", post(accept_invite))
}

// =============================================================================
// Request/Response types
// =============================================================================

/// Create invite request
#[derive(Debug, Deserialize)]
pub struct CreateInviteRequest {
    /// Email address to invite
    pub email: String,
    /// Workspace ID
    pub workspace_id: String,
    /// Role to grant (default: viewer)
    #[serde(default = "default_role")]
    pub role: String,
}

fn default_role() -> String {
    "viewer".to_string()
}

/// Invite response
#[derive(Debug, Serialize)]
pub struct InviteResponse {
    pub id: String,
    pub email: String,
    pub workspace_id: String,
    pub role: String,
    pub status: String,
    pub expires_at: String,
    pub created_at: String,
    /// The invite URL (only on create)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub invite_url: Option<String>,
}

impl From<WorkspaceInvite> for InviteResponse {
    fn from(invite: WorkspaceInvite) -> Self {
        Self {
            id: invite.id,
            email: invite.email,
            workspace_id: invite.workspace_id,
            role: invite.role.as_str().to_string(),
            status: invite.status.as_str().to_string(),
            expires_at: invite.expires_at.to_rfc3339(),
            created_at: invite.created_at.to_rfc3339(),
            invite_url: None,
        }
    }
}

/// List invites query
#[derive(Debug, Deserialize)]
pub struct ListInvitesQuery {
    /// Workspace ID (required)
    pub workspace_id: String,
    /// Filter by status (optional)
    #[serde(default)]
    pub status: Option<String>,
}

/// List invites response
#[derive(Debug, Serialize)]
pub struct ListInvitesResponse {
    pub invites: Vec<InviteResponse>,
    pub count: usize,
}

/// Verify invite response
#[derive(Debug, Serialize)]
pub struct VerifyInviteResponse {
    pub valid: bool,
    pub email: String,
    pub workspace_name: String,
    pub role: String,
    pub expires_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// Accept invite request
#[derive(Debug, Deserialize)]
pub struct AcceptInviteRequest {
    /// User ID accepting the invite (for existing users)
    #[serde(default)]
    pub user_id: Option<String>,
}

// =============================================================================
// Admin Handlers
// =============================================================================

/// Create a workspace invite
///
/// POST /api/v1/admin/workspace/invites
async fn create_invite(
    user: AuthUser,
    State(state): State<AppState>,
    Json(req): Json<CreateInviteRequest>,
) -> Result<(StatusCode, Json<InviteResponse>), ApiError> {
    let control = state
        .control
        .as_ref()
        .ok_or_else(|| ApiError::internal("Control plane not initialized"))?;

    // Check user has admin access to workspace
    check_workspace_admin(control, &req.workspace_id, &user).await?;

    // Validate email
    if req.email.is_empty() || !req.email.contains('@') {
        return Err(ApiError::validation("email", "invalid email address"));
    }

    // Parse role
    let role = parse_role(&req.role)?;

    // Check if there's already a pending invite for this email
    if let Some(existing) = control
        .invites()
        .get_pending_for_email(&req.email, &req.workspace_id)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to check existing invite: {}", e)))?
    {
        // Return the existing invite
        let mut response = InviteResponse::from(existing.clone());
        response.invite_url = Some(format!("/invite/{}", existing.token));
        return Ok((StatusCode::OK, Json(response)));
    }

    // Check if user is already a member
    if control
        .workspaces()
        .get_member(&req.workspace_id, &req.email)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to check membership: {}", e)))?
        .is_some()
    {
        return Err(ApiError::conflict("invite", "user is already a member"));
    }

    // Create the invite
    let invite = WorkspaceInvite::new(&req.email, &req.workspace_id, role, &user.id);
    let token = invite.token.clone();

    control
        .invites()
        .create(&invite)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to create invite: {}", e)))?;

    let mut response = InviteResponse::from(invite);
    response.invite_url = Some(format!("/invite/{}", token));

    Ok((StatusCode::CREATED, Json(response)))
}

/// List invites for a workspace
///
/// GET /api/v1/admin/workspace/invites?workspace_id=...
async fn list_invites(
    user: AuthUser,
    State(state): State<AppState>,
    Query(query): Query<ListInvitesQuery>,
) -> Result<Json<ListInvitesResponse>, ApiError> {
    let control = state
        .control
        .as_ref()
        .ok_or_else(|| ApiError::internal("Control plane not initialized"))?;

    // Check user has admin access to workspace
    check_workspace_admin(control, &query.workspace_id, &user).await?;

    // Get invites
    let invites = if query.status.as_deref() == Some("pending") {
        control
            .invites()
            .list_pending_for_workspace(&query.workspace_id)
            .await
            .map_err(|e| ApiError::internal(format!("Failed to list invites: {}", e)))?
    } else {
        control
            .invites()
            .list_for_workspace(&query.workspace_id)
            .await
            .map_err(|e| ApiError::internal(format!("Failed to list invites: {}", e)))?
    };

    let count = invites.len();
    let invites: Vec<InviteResponse> = invites.into_iter().map(InviteResponse::from).collect();

    Ok(Json(ListInvitesResponse { invites, count }))
}

/// Cancel an invite
///
/// DELETE /api/v1/admin/workspace/invites/{id}?workspace_id=...
async fn cancel_invite(
    user: AuthUser,
    Path(id): Path<String>,
    Query(query): Query<ListInvitesQuery>,
    State(state): State<AppState>,
) -> Result<StatusCode, ApiError> {
    let control = state
        .control
        .as_ref()
        .ok_or_else(|| ApiError::internal("Control plane not initialized"))?;

    // Check user has admin access to workspace
    check_workspace_admin(control, &query.workspace_id, &user).await?;

    // Get the invite
    let invite = control
        .invites()
        .get_by_id(&id)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to get invite: {}", e)))?
        .ok_or_else(|| ApiError::not_found("invite", &id))?;

    // Verify it belongs to this workspace
    if invite.workspace_id != query.workspace_id {
        return Err(ApiError::not_found("invite", &id));
    }

    // Cancel the invite
    control
        .invites()
        .update_status(&id, InviteStatus::Cancelled)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to cancel invite: {}", e)))?;

    Ok(StatusCode::NO_CONTENT)
}

// =============================================================================
// Public Handlers
// =============================================================================

/// Verify an invite token
///
/// GET /api/v1/invites/{token}
async fn verify_invite(
    Path(token): Path<String>,
    State(state): State<AppState>,
) -> Result<Json<VerifyInviteResponse>, ApiError> {
    let control = state
        .control
        .as_ref()
        .ok_or_else(|| ApiError::internal("Control plane not initialized"))?;

    // Get the invite
    let invite = control
        .invites()
        .get_by_token(&token)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to get invite: {}", e)))?
        .ok_or_else(|| ApiError::not_found("invite", "token"))?;

    // Get workspace name
    let workspace = control
        .workspaces()
        .get_by_id(&invite.workspace_id)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to get workspace: {}", e)))?
        .ok_or_else(|| ApiError::not_found("workspace", &invite.workspace_id))?;

    // Check if valid
    let (valid, message) = if invite.status != InviteStatus::Pending {
        (false, Some(format!("Invite is {}", invite.status.as_str())))
    } else if invite.is_expired() {
        (false, Some("Invite has expired".to_string()))
    } else {
        (true, None)
    };

    Ok(Json(VerifyInviteResponse {
        valid,
        email: invite.email,
        workspace_name: workspace.name,
        role: invite.role.as_str().to_string(),
        expires_at: invite.expires_at.to_rfc3339(),
        message,
    }))
}

/// Accept an invite
///
/// POST /api/v1/invites/{token}/accept
async fn accept_invite(
    Path(token): Path<String>,
    State(state): State<AppState>,
    user: OptionalAuthUser,
    Json(req): Json<AcceptInviteRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let control = state
        .control
        .as_ref()
        .ok_or_else(|| ApiError::internal("Control plane not initialized"))?;

    // Get the invite
    let invite = control
        .invites()
        .get_by_token(&token)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to get invite: {}", e)))?
        .ok_or_else(|| ApiError::not_found("invite", "token"))?;

    // Check validity
    if invite.status != InviteStatus::Pending {
        return Err(ApiError::validation(
            "token",
            format!("Invite is {}", invite.status.as_str()),
        ));
    }

    if invite.is_expired() {
        return Err(ApiError::validation("token", "Invite has expired"));
    }

    // Determine user ID - require authentication for security
    // The user_id field in the request is only accepted if it matches the authenticated user
    let user_id = if let Some(ref auth_user) = user.0 {
        // Authenticated user - verify email matches invite
        if auth_user.email != invite.email {
            return Err(ApiError::forbidden(
                "Invite email does not match your account",
            ));
        }
        auth_user.id.clone()
    } else if req.user_id.is_some() {
        // No authenticated user - this requires additional validation
        // For security, we require the user to authenticate first
        // The user_id in the request is only for backwards compatibility
        // and MUST match a valid user with the invite email
        return Err(ApiError::validation(
            "authentication",
            "Please log in to accept this invite",
        ));
    } else {
        return Err(ApiError::validation(
            "authentication",
            "Authentication required to accept invite",
        ));
    };

    // Check if already a member
    if control
        .workspaces()
        .get_member(&invite.workspace_id, &user_id)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to check membership: {}", e)))?
        .is_some()
    {
        // Already a member, just mark invite as accepted
        control
            .invites()
            .update_status(&invite.id, InviteStatus::Accepted)
            .await
            .map_err(|e| ApiError::internal(format!("Failed to update invite: {}", e)))?;

        return Ok(Json(serde_json::json!({
            "message": "Already a workspace member",
            "workspace_id": invite.workspace_id
        })));
    }

    // Add user as member
    let membership = WorkspaceMembership::new(&user_id, &invite.workspace_id, invite.role);
    control
        .workspaces()
        .add_member(&membership)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to add member: {}", e)))?;

    // Mark invite as accepted
    control
        .invites()
        .update_status(&invite.id, InviteStatus::Accepted)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to update invite: {}", e)))?;

    Ok(Json(serde_json::json!({
        "message": "Successfully joined workspace",
        "workspace_id": invite.workspace_id,
        "role": invite.role.as_str()
    })))
}

// =============================================================================
// Helpers
// =============================================================================

fn parse_role(role_str: &str) -> Result<MemberRole, ApiError> {
    MemberRole::from_str(role_str).ok_or_else(|| {
        ApiError::validation("role", "must be one of: viewer, editor, admin")
    })
}

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
        _ => Err(ApiError::forbidden("Workspace admin access required")),
    }
}
