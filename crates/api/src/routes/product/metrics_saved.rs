//! Saved metrics endpoints
//!
//! CRUD and sharing endpoints for saved metric configurations.
//!
//! # Routes
//!
//! | Endpoint | Auth | Permission | Notes |
//! |----------|------|------------|-------|
//! | `GET /metrics/saved` | Required | Viewer+ | List workspace saved metrics |
//! | `GET /metrics/saved/{id}` | Required | Viewer+ | Get saved metric by ID |
//! | `POST /metrics/saved` | Required | **Editor+** | Create saved metric |
//! | `PUT /metrics/saved/{id}` | Required | Owner/Admin | Update saved metric |
//! | `DELETE /metrics/saved/{id}` | Required | Owner/Admin | Delete saved metric |
//! | `POST /metrics/saved/{id}/share` | Required | Owner/Admin | Create share link |
//! | `GET /metrics/saved/{id}/share` | Required | Owner/Admin | Get share link |
//! | `DELETE /metrics/saved/{id}/share` | Required | Owner/Admin | Revoke share link |

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{delete, get, post, put},
};
use serde::{Deserialize, Serialize};
use tracing::info;

use tell_auth::Permission;
use tell_control::{ControlPlane, MetricQueryConfig, ResourceType, SavedMetric, SharedLink};

use crate::audit::AuditAction;
use crate::auth::AuthUser;
use crate::error::ApiError;
use crate::state::AppState;

// =============================================================================
// Request/Response types
// =============================================================================

/// Create saved metric request
#[derive(Debug, Deserialize)]
pub struct CreateMetricRequest {
    pub title: String,
    pub description: Option<String>,
    pub workspace_id: String,
    pub query: MetricQueryConfig,
}

/// Update saved metric request
#[derive(Debug, Deserialize)]
pub struct UpdateMetricRequest {
    pub title: Option<String>,
    pub description: Option<String>,
    pub query: Option<MetricQueryConfig>,
}

/// List saved metrics query parameters
#[derive(Debug, Deserialize)]
pub struct ListMetricsQuery {
    pub workspace_id: String,
}

/// Saved metric response
#[derive(Debug, Serialize)]
pub struct MetricResponse {
    pub id: String,
    pub workspace_id: String,
    pub owner_id: String,
    pub title: String,
    pub description: Option<String>,
    pub query: MetricQueryConfig,
    pub created_at: String,
    pub updated_at: String,
}

impl From<SavedMetric> for MetricResponse {
    fn from(metric: SavedMetric) -> Self {
        Self {
            id: metric.id,
            workspace_id: metric.workspace_id,
            owner_id: metric.owner_id,
            title: metric.title,
            description: metric.description,
            query: metric.query,
            created_at: metric.created_at.to_rfc3339(),
            updated_at: metric.updated_at.to_rfc3339(),
        }
    }
}

/// List saved metrics response
#[derive(Debug, Serialize)]
pub struct ListMetricsResponse {
    pub metrics: Vec<MetricResponse>,
}

/// Share metric response
#[derive(Debug, Serialize)]
pub struct ShareMetricResponse {
    pub hash: String,
    pub url: String,
    pub expires_at: Option<String>,
    pub created_at: String,
}

impl From<SharedLink> for ShareMetricResponse {
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

/// Saved metrics routes
pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/", get(list_metrics))
        .route("/", post(create_metric))
        .route("/{id}", get(get_metric))
        .route("/{id}", put(update_metric))
        .route("/{id}", delete(delete_metric))
        .route("/{id}/share", post(share_metric))
        .route("/{id}/share", get(get_share_link))
        .route("/{id}/share", delete(unshare_metric))
}

// =============================================================================
// Handlers
// =============================================================================

/// List saved metrics in a workspace
///
/// GET /api/v1/metrics/saved?workspace_id={workspace_id}
async fn list_metrics(
    user: AuthUser,
    Query(query): Query<ListMetricsQuery>,
    State(state): State<AppState>,
) -> Result<Json<ListMetricsResponse>, ApiError> {
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

    let repo = ControlPlane::metrics(&ws_db);

    let metrics = repo
        .list_for_workspace(&query.workspace_id)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to list metrics: {}", e)))?;

    Ok(Json(ListMetricsResponse {
        metrics: metrics.into_iter().map(MetricResponse::from).collect(),
    }))
}

/// Get a saved metric by ID
///
/// GET /api/v1/metrics/saved/{id}
async fn get_metric(
    user: AuthUser,
    Path(id): Path<String>,
    Query(query): Query<WorkspaceQuery>,
    State(state): State<AppState>,
) -> Result<Json<MetricResponse>, ApiError> {
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

    let repo = ControlPlane::metrics(&ws_db);

    let metric = repo
        .get_by_id(&id)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to get metric: {}", e)))?
        .ok_or_else(|| ApiError::not_found("saved_metric", &id))?;

    Ok(Json(MetricResponse::from(metric)))
}

/// Create a new saved metric
///
/// POST /api/v1/metrics/saved
///
/// Requires Editor+ permission.
async fn create_metric(
    user: AuthUser,
    State(state): State<AppState>,
    Json(req): Json<CreateMetricRequest>,
) -> Result<(StatusCode, Json<MetricResponse>), ApiError> {
    // Require Editor+ permission
    if !user.has_permission(Permission::Create) {
        return Err(ApiError::forbidden(
            "Editor permission required to create metrics",
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

    // Validate query
    validate_metric_query(&req.query)?;

    let ws_db = control
        .workspace_db(&req.workspace_id)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to get workspace database: {}", e)))?;

    let repo = ControlPlane::metrics(&ws_db);

    // Create metric with user as owner
    let mut metric = SavedMetric::new(&req.workspace_id, &user.id, &req.title, req.query);
    if let Some(desc) = req.description {
        metric = metric.with_description(&desc);
    }

    repo.create(&metric)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to create metric: {}", e)))?;

    info!(
        target: "audit",
        action = AuditAction::Create.as_str(),
        resource_type = "saved_metric",
        resource_id = %metric.id,
        user_id = %user.id,
        workspace_id = %req.workspace_id
    );

    Ok((StatusCode::CREATED, Json(MetricResponse::from(metric))))
}

/// Update a saved metric
///
/// PUT /api/v1/metrics/saved/{id}
///
/// Requires ownership or Admin role.
async fn update_metric(
    user: AuthUser,
    Path(id): Path<String>,
    Query(query): Query<WorkspaceQuery>,
    State(state): State<AppState>,
    Json(req): Json<UpdateMetricRequest>,
) -> Result<Json<MetricResponse>, ApiError> {
    let control = state
        .control
        .as_ref()
        .ok_or_else(|| ApiError::internal("Control plane not initialized"))?;

    let ws_db = control
        .workspace_db(&query.workspace_id)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to get workspace database: {}", e)))?;

    let repo = ControlPlane::metrics(&ws_db);

    let mut metric = repo
        .get_by_id(&id)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to get metric: {}", e)))?
        .ok_or_else(|| ApiError::not_found("saved_metric", &id))?;

    // Check ownership or admin
    if !can_modify_metric(&metric, &user, control).await? {
        return Err(ApiError::forbidden("Not metric owner or admin"));
    }

    // Apply updates
    if let Some(title) = req.title {
        if title.is_empty() || title.len() > 200 {
            return Err(ApiError::validation("title", "must be 1-200 characters"));
        }
        metric.title = title;
    }
    if let Some(description) = req.description {
        metric.description = Some(description);
    }
    if let Some(query) = req.query {
        validate_metric_query(&query)?;
        metric.query = query;
    }

    repo.update(&metric)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to update metric: {}", e)))?;

    info!(
        target: "audit",
        action = AuditAction::Update.as_str(),
        resource_type = "saved_metric",
        resource_id = %metric.id,
        user_id = %user.id,
        workspace_id = %query.workspace_id
    );

    Ok(Json(MetricResponse::from(metric)))
}

/// Delete a saved metric
///
/// DELETE /api/v1/metrics/saved/{id}
///
/// Requires ownership or Admin role.
async fn delete_metric(
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

    let repo = ControlPlane::metrics(&ws_db);

    let metric = repo
        .get_by_id(&id)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to get metric: {}", e)))?
        .ok_or_else(|| ApiError::not_found("saved_metric", &id))?;

    // Check ownership or admin
    if !can_modify_metric(&metric, &user, control).await? {
        return Err(ApiError::forbidden("Not metric owner or admin"));
    }

    // Delete any share links first
    let _ = control
        .sharing()
        .delete_for_resource(ResourceType::Metric, &id)
        .await;

    repo.delete(&id)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to delete metric: {}", e)))?;

    info!(
        target: "audit",
        action = AuditAction::Delete.as_str(),
        resource_type = "saved_metric",
        resource_id = %id,
        user_id = %user.id,
        workspace_id = %query.workspace_id
    );

    Ok(StatusCode::NO_CONTENT)
}

/// Create a share link for a saved metric
///
/// POST /api/v1/metrics/saved/{id}/share
///
/// Requires ownership or Admin role.
async fn share_metric(
    user: AuthUser,
    Path(id): Path<String>,
    Query(query): Query<WorkspaceQuery>,
    State(state): State<AppState>,
) -> Result<(StatusCode, Json<ShareMetricResponse>), ApiError> {
    let control = state
        .control
        .as_ref()
        .ok_or_else(|| ApiError::internal("Control plane not initialized"))?;

    let ws_db = control
        .workspace_db(&query.workspace_id)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to get workspace database: {}", e)))?;

    let metric_repo = ControlPlane::metrics(&ws_db);

    let metric = metric_repo
        .get_by_id(&id)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to get metric: {}", e)))?
        .ok_or_else(|| ApiError::not_found("saved_metric", &id))?;

    // Check ownership or admin
    if !can_modify_metric(&metric, &user, control).await? {
        return Err(ApiError::forbidden("Not metric owner or admin"));
    }

    // Check if already shared
    let existing = control
        .sharing()
        .get_for_resource(ResourceType::Metric, &id)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to check existing share: {}", e)))?;

    if let Some(link) = existing {
        // Return existing share link
        return Ok((StatusCode::OK, Json(ShareMetricResponse::from(link))));
    }

    // Create new share link
    let link = SharedLink::for_metric(&id, &query.workspace_id, &user.id);

    control
        .sharing()
        .create(&link)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to create share link: {}", e)))?;

    info!(
        target: "audit",
        action = AuditAction::Share.as_str(),
        resource_type = "saved_metric",
        resource_id = %id,
        user_id = %user.id,
        workspace_id = %query.workspace_id
    );

    Ok((StatusCode::CREATED, Json(ShareMetricResponse::from(link))))
}

/// Get the share link for a saved metric
///
/// GET /api/v1/metrics/saved/{id}/share
///
/// Requires ownership or Admin role.
async fn get_share_link(
    user: AuthUser,
    Path(id): Path<String>,
    Query(query): Query<WorkspaceQuery>,
    State(state): State<AppState>,
) -> Result<Json<ShareMetricResponse>, ApiError> {
    let control = state
        .control
        .as_ref()
        .ok_or_else(|| ApiError::internal("Control plane not initialized"))?;

    let ws_db = control
        .workspace_db(&query.workspace_id)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to get workspace database: {}", e)))?;

    let metric_repo = ControlPlane::metrics(&ws_db);

    let metric = metric_repo
        .get_by_id(&id)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to get metric: {}", e)))?
        .ok_or_else(|| ApiError::not_found("saved_metric", &id))?;

    // Check ownership or admin - share hash is sensitive info
    if !can_modify_metric(&metric, &user, control).await? {
        return Err(ApiError::forbidden("Not metric owner or admin"));
    }

    let link = control
        .sharing()
        .get_for_resource(ResourceType::Metric, &id)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to get share link: {}", e)))?
        .ok_or_else(|| ApiError::not_found("share_link", &id))?;

    Ok(Json(ShareMetricResponse::from(link)))
}

/// Revoke sharing for a saved metric
///
/// DELETE /api/v1/metrics/saved/{id}/share
///
/// Requires ownership or Admin role.
async fn unshare_metric(
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

    let metric_repo = ControlPlane::metrics(&ws_db);

    let metric = metric_repo
        .get_by_id(&id)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to get metric: {}", e)))?
        .ok_or_else(|| ApiError::not_found("saved_metric", &id))?;

    // Check ownership or admin
    if !can_modify_metric(&metric, &user, control).await? {
        return Err(ApiError::forbidden("Not metric owner or admin"));
    }

    control
        .sharing()
        .delete_for_resource(ResourceType::Metric, &id)
        .await
        .map_err(|e| ApiError::internal(format!("Failed to delete share link: {}", e)))?;

    info!(
        target: "audit",
        action = AuditAction::Unshare.as_str(),
        resource_type = "saved_metric",
        resource_id = %id,
        user_id = %user.id,
        workspace_id = %query.workspace_id
    );

    Ok(StatusCode::NO_CONTENT)
}

// =============================================================================
// Validation
// =============================================================================

/// Valid metric types
const VALID_METRICS: &[&str] = &[
    "dau",
    "wau",
    "mau",
    "events",
    "top_events",
    "log_volume",
    "top_logs",
    "sessions",
    "stickiness",
];

/// Valid granularities
const VALID_GRANULARITIES: &[&str] = &[
    "minute",
    "hourly",
    "daily",
    "weekly",
    "monthly",
    "quarterly",
    "yearly",
];

/// Validate metric query configuration
fn validate_metric_query(query: &MetricQueryConfig) -> Result<(), ApiError> {
    // Validate metric type
    if query.metric.is_empty() {
        return Err(ApiError::validation("query.metric", "cannot be empty"));
    }
    if !VALID_METRICS.contains(&query.metric.as_str()) {
        return Err(ApiError::validation(
            "query.metric",
            format!(
                "invalid metric type '{}', valid types: {}",
                query.metric,
                VALID_METRICS.join(", ")
            ),
        ));
    }

    // Validate range
    if query.range.is_empty() {
        return Err(ApiError::validation("query.range", "cannot be empty"));
    }

    // Validate granularity if present
    if let Some(ref granularity) = query.granularity
        && !VALID_GRANULARITIES.contains(&granularity.as_str())
    {
        return Err(ApiError::validation(
            "query.granularity",
            format!(
                "invalid granularity '{}', valid values: {}",
                granularity,
                VALID_GRANULARITIES.join(", ")
            ),
        ));
    }

    // Validate compare mode if present
    if let Some(ref compare_mode) = query.compare_mode
        && compare_mode != "previous"
        && compare_mode != "previous_year"
    {
        return Err(ApiError::validation(
            "query.compare_mode",
            "must be 'previous' or 'previous_year'",
        ));
    }

    // Validate limit if present
    if let Some(limit) = query.limit
        && (limit == 0 || limit > 10000)
    {
        return Err(ApiError::validation(
            "query.limit",
            "must be between 1 and 10000",
        ));
    }

    Ok(())
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

/// Check if user can modify a saved metric (owner or admin)
async fn can_modify_metric(
    metric: &SavedMetric,
    user: &AuthUser,
    control: &tell_control::ControlPlane,
) -> Result<bool, ApiError> {
    // Owner can always modify
    if metric.owner_id == user.id {
        return Ok(true);
    }

    // Platform admins can modify any metric
    if user.has_permission(Permission::Platform) {
        return Ok(true);
    }

    // Workspace admins can modify any metric in their workspace
    if user.has_permission(Permission::Admin) {
        let membership = control
            .workspaces()
            .get_member(&metric.workspace_id, &user.id)
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
