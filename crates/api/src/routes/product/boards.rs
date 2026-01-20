//! Board endpoints
//!
//! CRUD endpoints for boards (dashboards) within a workspace.
//!
//! # Auth Requirements
//!
//! | Endpoint | Auth | Permission | Notes |
//! |----------|------|------------|-------|
//! | `GET /boards` | Required | Viewer+ | List workspace boards |
//! | `GET /boards/{id}` | Required | Viewer+ | Get board by ID |
//! | `POST /boards` | Required | **Editor+** | Create board |
//! | `PUT /boards/{id}` | Required | Owner/Admin | Update board |
//! | `DELETE /boards/{id}` | Required | Owner/Admin | Delete board |
//! | `PUT /boards/{id}/pin` | Required | Editor+ | Pin/unpin board |

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{delete, get, post, put},
};
use serde::{Deserialize, Serialize};

use tell_auth::Permission;
use tell_control::{Board, BoardSettings, ControlPlane, ResourceType, SharedLink};

use crate::auth::AuthUser;
use crate::error::ApiError;
use crate::state::AppState;

// =============================================================================
// Request/Response types
// =============================================================================

/// Create board request
#[derive(Debug, Deserialize)]
pub struct CreateBoardRequest {
    pub title: String,
    pub description: Option<String>,
    pub workspace_id: String,
}

/// Update board request
#[derive(Debug, Deserialize)]
pub struct UpdateBoardRequest {
    pub title: Option<String>,
    pub description: Option<String>,
    pub settings: Option<BoardSettings>,
}

/// Pin/unpin request
#[derive(Debug, Deserialize)]
pub struct PinBoardRequest {
    pub pinned: bool,
}

/// List boards query parameters
#[derive(Debug, Deserialize)]
pub struct ListBoardsQuery {
    pub workspace_id: String,
    /// Filter to only pinned boards
    #[serde(default)]
    pub pinned_only: bool,
}

/// Board response
#[derive(Debug, Serialize)]
pub struct BoardResponse {
    pub id: String,
    pub workspace_id: String,
    pub owner_id: String,
    pub title: String,
    pub description: Option<String>,
    pub settings: BoardSettings,
    pub is_pinned: bool,
    pub created_at: String,
    pub updated_at: String,
}

impl From<Board> for BoardResponse {
    fn from(board: Board) -> Self {
        Self {
            id: board.id,
            workspace_id: board.workspace_id,
            owner_id: board.owner_id,
            title: board.title,
            description: board.description,
            settings: board.settings,
            is_pinned: board.is_pinned,
            created_at: board.created_at.to_rfc3339(),
            updated_at: board.updated_at.to_rfc3339(),
        }
    }
}

/// List boards response
#[derive(Debug, Serialize)]
pub struct ListBoardsResponse {
    pub boards: Vec<BoardResponse>,
}

/// Share board response
#[derive(Debug, Serialize)]
pub struct ShareBoardResponse {
    pub hash: String,
    pub url: String,
    pub expires_at: Option<String>,
    pub created_at: String,
}

impl From<SharedLink> for ShareBoardResponse {
    fn from(link: SharedLink) -> Self {
        Self {
            hash: link.hash.clone(),
            url: link.public_path(),
            expires_at: link.expires_at.map(|dt| dt.to_rfc3339()),
            created_at: link.created_at.to_rfc3339(),
        }
    }
}

// =============================================================================
// Routes
// =============================================================================

/// Board routes
pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/", get(list_boards))
        .route("/", post(create_board))
        .route("/{id}", get(get_board))
        .route("/{id}", put(update_board))
        .route("/{id}", delete(delete_board))
        .route("/{id}/pin", put(pin_board))
        .route("/{id}/share", post(share_board))
        .route("/{id}/share", get(get_share_link))
        .route("/{id}/share", delete(unshare_board))
}

// =============================================================================
// Handlers
// =============================================================================

/// List boards in a workspace
///
/// GET /api/v1/boards?workspace_id={workspace_id}&pinned_only={bool}
async fn list_boards(
    user: AuthUser,
    Query(query): Query<ListBoardsQuery>,
    State(state): State<AppState>,
) -> Result<Json<ListBoardsResponse>, ApiError> {
    let control = state
        .control
        .as_ref()
        .ok_or_else(|| ApiError::internal("Control plane not initialized"))?;

    // Check user has access to workspace
    check_workspace_membership(control, &query.workspace_id, &user.id).await?;

    // Get workspace database
    let ws_db = control
        .workspace_db(&query.workspace_id)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to get workspace database: {}", e)))?;

    let repo = ControlPlane::boards(&ws_db);

    let boards = if query.pinned_only {
        repo.list_pinned(&query.workspace_id)
            .await
            .map_err(|e| ApiError::internal(format!("Failed to list pinned boards: {}", e)))?
    } else {
        repo.list_for_workspace(&query.workspace_id)
            .await
            .map_err(|e| ApiError::internal(format!("Failed to list boards: {}", e)))?
    };

    Ok(Json(ListBoardsResponse {
        boards: boards.into_iter().map(BoardResponse::from).collect(),
    }))
}

/// Get a board by ID
///
/// GET /api/v1/boards/{id}
async fn get_board(
    user: AuthUser,
    Path(id): Path<String>,
    Query(query): Query<WorkspaceQuery>,
    State(state): State<AppState>,
) -> Result<Json<BoardResponse>, ApiError> {
    let control = state
        .control
        .as_ref()
        .ok_or_else(|| ApiError::internal("Control plane not initialized"))?;

    // Check user has access to workspace
    check_workspace_membership(control, &query.workspace_id, &user.id).await?;

    let ws_db = control
        .workspace_db(&query.workspace_id)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to get workspace database: {}", e)))?;

    let repo = ControlPlane::boards(&ws_db);

    let board = repo
        .get_by_id(&id)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to get board: {}", e)))?
        .ok_or_else(|| ApiError::not_found("board", &id))?;

    Ok(Json(BoardResponse::from(board)))
}

/// Create a new board
///
/// POST /api/v1/boards
///
/// Requires Editor+ permission.
async fn create_board(
    user: AuthUser,
    State(state): State<AppState>,
    Json(req): Json<CreateBoardRequest>,
) -> Result<(StatusCode, Json<BoardResponse>), ApiError> {
    // Require Editor+ permission
    if !user.has_permission(Permission::Create) {
        return Err(ApiError::forbidden(
            "Editor permission required to create boards",
        ));
    }

    let control = state
        .control
        .as_ref()
        .ok_or_else(|| ApiError::internal("Control plane not initialized"))?;

    // Check user has access to workspace
    check_workspace_membership(control, &req.workspace_id, &user.id).await?;

    // Validate title
    if req.title.is_empty() || req.title.len() > 200 {
        return Err(ApiError::validation("title", "must be 1-200 characters"));
    }

    let ws_db = control
        .workspace_db(&req.workspace_id)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to get workspace database: {}", e)))?;

    let repo = ControlPlane::boards(&ws_db);

    // Validate description length
    if let Some(ref desc) = req.description
        && desc.len() > 2000
    {
        return Err(ApiError::validation(
            "description",
            "must be at most 2000 characters",
        ));
    }

    // Create board with user as owner
    let mut board = Board::new(&req.workspace_id, &user.id, &req.title);
    if let Some(desc) = req.description {
        board = board.with_description(&desc);
    }

    repo.create(&board)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to create board: {}", e)))?;

    Ok((StatusCode::CREATED, Json(BoardResponse::from(board))))
}

/// Update a board
///
/// PUT /api/v1/boards/{id}
///
/// Requires ownership or Admin role.
async fn update_board(
    user: AuthUser,
    Path(id): Path<String>,
    Query(query): Query<WorkspaceQuery>,
    State(state): State<AppState>,
    Json(req): Json<UpdateBoardRequest>,
) -> Result<Json<BoardResponse>, ApiError> {
    let control = state
        .control
        .as_ref()
        .ok_or_else(|| ApiError::internal("Control plane not initialized"))?;

    let ws_db = control
        .workspace_db(&query.workspace_id)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to get workspace database: {}", e)))?;

    let repo = ControlPlane::boards(&ws_db);

    let mut board = repo
        .get_by_id(&id)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to get board: {}", e)))?
        .ok_or_else(|| ApiError::not_found("board", &id))?;

    // Check ownership or admin
    if !can_modify_board(&board, &user, control).await? {
        return Err(ApiError::forbidden("Not board owner or admin"));
    }

    // Apply updates
    if let Some(title) = req.title {
        if title.is_empty() || title.len() > 200 {
            return Err(ApiError::validation("title", "must be 1-200 characters"));
        }
        board.title = title;
    }
    if let Some(description) = req.description {
        if description.len() > 2000 {
            return Err(ApiError::validation(
                "description",
                "must be at most 2000 characters",
            ));
        }
        board.description = Some(description);
    }
    if let Some(settings) = req.settings {
        board.settings = settings;
    }

    repo.update(&board)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to update board: {}", e)))?;

    Ok(Json(BoardResponse::from(board)))
}

/// Delete a board
///
/// DELETE /api/v1/boards/{id}
///
/// Requires ownership or Admin role.
async fn delete_board(
    user: AuthUser,
    Path(id): Path<String>,
    Query(query): Query<WorkspaceQuery>,
    State(state): State<AppState>,
) -> Result<StatusCode, ApiError> {
    let control = state
        .control
        .as_ref()
        .ok_or_else(|| ApiError::internal("Control plane not initialized"))?;

    let ws_db = control
        .workspace_db(&query.workspace_id)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to get workspace database: {}", e)))?;

    let repo = ControlPlane::boards(&ws_db);

    let board = repo
        .get_by_id(&id)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to get board: {}", e)))?
        .ok_or_else(|| ApiError::not_found("board", &id))?;

    // Check ownership or admin
    if !can_modify_board(&board, &user, control).await? {
        return Err(ApiError::forbidden("Not board owner or admin"));
    }

    repo.delete(&id)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to delete board: {}", e)))?;

    Ok(StatusCode::NO_CONTENT)
}

/// Pin or unpin a board
///
/// PUT /api/v1/boards/{id}/pin
///
/// Requires ownership or Admin role.
async fn pin_board(
    user: AuthUser,
    Path(id): Path<String>,
    Query(query): Query<WorkspaceQuery>,
    State(state): State<AppState>,
    Json(req): Json<PinBoardRequest>,
) -> Result<Json<BoardResponse>, ApiError> {
    let control = state
        .control
        .as_ref()
        .ok_or_else(|| ApiError::internal("Control plane not initialized"))?;

    let ws_db = control
        .workspace_db(&query.workspace_id)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to get workspace database: {}", e)))?;

    let repo = ControlPlane::boards(&ws_db);

    // Get board and verify ownership
    let board = repo
        .get_by_id(&id)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to get board: {}", e)))?
        .ok_or_else(|| ApiError::not_found("board", &id))?;

    // Check ownership or admin (same as update/delete)
    if !can_modify_board(&board, &user, control).await? {
        return Err(ApiError::forbidden("Not board owner or admin"));
    }

    repo.set_pinned(&id, req.pinned)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to update pin status: {}", e)))?;

    // Get updated board
    let updated_board = repo
        .get_by_id(&id)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to get board: {}", e)))?
        .ok_or_else(|| ApiError::not_found("board", &id))?;

    Ok(Json(BoardResponse::from(updated_board)))
}

/// Create a share link for a board
///
/// POST /api/v1/boards/{id}/share
///
/// Requires ownership or Admin role.
async fn share_board(
    user: AuthUser,
    Path(id): Path<String>,
    Query(query): Query<WorkspaceQuery>,
    State(state): State<AppState>,
) -> Result<(StatusCode, Json<ShareBoardResponse>), ApiError> {
    let control = state
        .control
        .as_ref()
        .ok_or_else(|| ApiError::internal("Control plane not initialized"))?;

    let ws_db = control
        .workspace_db(&query.workspace_id)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to get workspace database: {}", e)))?;

    let board_repo = ControlPlane::boards(&ws_db);

    let board = board_repo
        .get_by_id(&id)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to get board: {}", e)))?
        .ok_or_else(|| ApiError::not_found("board", &id))?;

    // Check ownership or admin
    if !can_modify_board(&board, &user, control).await? {
        return Err(ApiError::forbidden("Not board owner or admin"));
    }

    // Check if already shared
    let existing = control
        .sharing()
        .get_for_resource(ResourceType::Board, &id)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to check existing share: {}", e)))?;

    if let Some(link) = existing {
        // Return existing share link
        return Ok((StatusCode::OK, Json(ShareBoardResponse::from(link))));
    }

    // Create new share link
    let link = SharedLink::for_board(&id, &query.workspace_id, &user.id);

    control
        .sharing()
        .create(&link)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to create share link: {}", e)))?;

    Ok((StatusCode::CREATED, Json(ShareBoardResponse::from(link))))
}

/// Get the share link for a board
///
/// GET /api/v1/boards/{id}/share
///
/// Requires ownership or Admin role. Returns the existing share link or 404 if not shared.
async fn get_share_link(
    user: AuthUser,
    Path(id): Path<String>,
    Query(query): Query<WorkspaceQuery>,
    State(state): State<AppState>,
) -> Result<Json<ShareBoardResponse>, ApiError> {
    let control = state
        .control
        .as_ref()
        .ok_or_else(|| ApiError::internal("Control plane not initialized"))?;

    let ws_db = control
        .workspace_db(&query.workspace_id)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to get workspace database: {}", e)))?;

    let board_repo = ControlPlane::boards(&ws_db);

    // Get board and verify ownership (share hash is sensitive)
    let board = board_repo
        .get_by_id(&id)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to get board: {}", e)))?
        .ok_or_else(|| ApiError::not_found("board", &id))?;

    // Check ownership or admin - share hash is sensitive info
    if !can_modify_board(&board, &user, control).await? {
        return Err(ApiError::forbidden("Not board owner or admin"));
    }

    let link = control
        .sharing()
        .get_for_resource(ResourceType::Board, &id)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to get share link: {}", e)))?
        .ok_or_else(|| ApiError::not_found("share_link", &id))?;

    Ok(Json(ShareBoardResponse::from(link)))
}

/// Revoke sharing for a board
///
/// DELETE /api/v1/boards/{id}/share
///
/// Requires ownership or Admin role.
async fn unshare_board(
    user: AuthUser,
    Path(id): Path<String>,
    Query(query): Query<WorkspaceQuery>,
    State(state): State<AppState>,
) -> Result<StatusCode, ApiError> {
    let control = state
        .control
        .as_ref()
        .ok_or_else(|| ApiError::internal("Control plane not initialized"))?;

    let ws_db = control
        .workspace_db(&query.workspace_id)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to get workspace database: {}", e)))?;

    let board_repo = ControlPlane::boards(&ws_db);

    let board = board_repo
        .get_by_id(&id)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to get board: {}", e)))?
        .ok_or_else(|| ApiError::not_found("board", &id))?;

    // Check ownership or admin
    if !can_modify_board(&board, &user, control).await? {
        return Err(ApiError::forbidden("Not board owner or admin"));
    }

    control
        .sharing()
        .delete_for_resource(ResourceType::Board, &id)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to delete share link: {}", e)))?;

    Ok(StatusCode::NO_CONTENT)
}

// =============================================================================
// Helper functions
// =============================================================================

/// Query param for workspace context
#[derive(Debug, Deserialize)]
pub struct WorkspaceQuery {
    pub workspace_id: String,
}

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

/// Check if user can modify a board (owner or admin)
async fn can_modify_board(
    board: &Board,
    user: &AuthUser,
    control: &tell_control::ControlPlane,
) -> Result<bool, ApiError> {
    // Owner can always modify
    if board.owner_id == user.id {
        return Ok(true);
    }

    // Platform admins can modify any board
    if user.has_permission(Permission::Platform) {
        return Ok(true);
    }

    // Workspace admins can modify any board in their workspace
    if user.has_permission(Permission::Admin) {
        let membership = control
            .workspaces()
            .get_member(&board.workspace_id, &user.id)
            .await
            .map_err(|e| ApiError::internal(format!("Failed to check membership: {}", e)))?;

        if let Some(m) = membership
            && m.role.can_manage()
        {
            return Ok(true);
        }
    }

    Ok(false)
}
