//! Public routes
//!
//! Routes that don't require authentication (shared links, etc.)

use axum::{
    extract::{Path, State},
    routing::get,
    Json, Router,
};
use serde::Serialize;

use tell_control::{BoardSettings, ControlPlane, ResourceType};

use crate::error::ApiError;
use crate::state::AppState;

// =============================================================================
// Response types
// =============================================================================

/// Public view of a shared board (read-only, no sensitive data)
#[derive(Debug, Serialize)]
pub struct SharedBoardView {
    pub title: String,
    pub description: Option<String>,
    pub settings: BoardSettings,
    pub is_pinned: bool,
}

// =============================================================================
// Routes
// =============================================================================

/// Public sharing routes (no auth required)
pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/b/{hash}", get(view_shared_board))
}

// =============================================================================
// Handlers
// =============================================================================

/// View a shared board
///
/// GET /s/b/{hash}
///
/// No authentication required - the hash validates access.
async fn view_shared_board(
    Path(hash): Path<String>,
    State(state): State<AppState>,
) -> Result<Json<SharedBoardView>, ApiError> {
    let control = state
        .control
        .as_ref()
        .ok_or_else(|| ApiError::internal("Control plane not initialized"))?;

    // Get the share link
    let link = control
        .sharing()
        .get_by_hash(&hash)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to get share link: {}", e)))?
        .ok_or_else(|| ApiError::not_found("share_link", &hash))?;

    // Check it's a board share
    if link.resource_type != ResourceType::Board {
        return Err(ApiError::not_found("share_link", &hash));
    }

    // Check expiration
    if link.is_expired() {
        return Err(ApiError::Forbidden("Share link has expired".to_string()));
    }

    // Get the board from workspace database
    let ws_db = control
        .workspace_db(&link.workspace_id)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to get workspace database: {}", e)))?;

    let board = ControlPlane::boards(&ws_db)
        .get_by_id(&link.resource_id)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to get board: {}", e)))?
        .ok_or_else(|| ApiError::not_found("board", &link.resource_id))?;

    // Return public view (no owner_id, workspace_id, etc.)
    Ok(Json(SharedBoardView {
        title: board.title,
        description: board.description,
        settings: board.settings,
        is_pinned: board.is_pinned,
    }))
}
