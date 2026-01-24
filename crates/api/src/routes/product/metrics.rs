//! Metrics API routes
//!
//! Endpoints for analytics metrics:
//! - Active users (DAU, WAU, MAU)
//! - Events (counts, top events, properties, custom aggregations)
//! - Logs (volume, top logs)
//! - Sessions (volume, top, unique)
//! - Users (volume)
//! - Stickiness

use axum::extract::{Path, Query, State};
use axum::routing::get;
use axum::{Json, Router};

use tell_analytics::{
    SessionsMetric, StickinessMetric, TopEventsMetric, TopLogsMetric, TopSessionsMetric,
    UsersMetric,
};

use crate::auth::{AuthUser, WorkspaceId};
use crate::error::Result;
use crate::state::AppState;
use crate::types::{
    ApiResponse, CustomMetricParams, DrillDownParams, LogParams, MetricParams, TopParams,
};

/// Build the metrics router
pub fn routes() -> Router<AppState> {
    Router::new()
        // Active users
        .route("/dau", get(get_dau))
        .route("/wau", get(get_wau))
        .route("/mau", get(get_mau))
        // Active users raw data
        .route("/dau/raw", get(get_dau_raw))
        .route("/wau/raw", get(get_wau_raw))
        .route("/mau/raw", get(get_mau_raw))
        // Events
        .route("/events", get(get_events))
        .route("/events/top", get(get_top_events))
        .route("/events/top/{event_name}/raw", get(get_top_event_raw))
        .route("/events/properties", get(get_event_properties))
        .route("/events/custom", get(get_events_custom))
        .route("/events/raw", get(get_events_raw))
        // Logs
        .route("/logs", get(get_logs))
        .route("/logs/top", get(get_top_logs))
        .route("/logs/raw", get(get_logs_raw))
        // Sessions
        .route("/sessions", get(get_sessions))
        .route("/sessions/top", get(get_sessions_top))
        .route("/sessions/unique", get(get_sessions_unique))
        .route("/sessions/raw", get(get_sessions_raw))
        // Users
        .route("/users", get(get_users))
        .route("/users/raw", get(get_users_raw))
        // Stickiness
        .route("/stickiness/daily", get(get_daily_stickiness))
        .route("/stickiness/weekly", get(get_weekly_stickiness))
}

// ============================================================================
// Active Users
// ============================================================================

/// GET /api/v1/metrics/dau - Daily active users
async fn get_dau(
    State(state): State<AppState>,
    _user: AuthUser,
    workspace: WorkspaceId,
    Query(params): Query<MetricParams>,
) -> Result<Json<ApiResponse<tell_analytics::TimeSeriesData>>> {
    let filter = params.to_filter()?;
    let data = state
        .metrics
        .dau_with_comparison(&filter, workspace.0)
        .await?;
    Ok(Json(ApiResponse::new(data)))
}

/// GET /api/v1/metrics/wau - Weekly active users
async fn get_wau(
    State(state): State<AppState>,
    _user: AuthUser,
    workspace: WorkspaceId,
    Query(params): Query<MetricParams>,
) -> Result<Json<ApiResponse<tell_analytics::TimeSeriesData>>> {
    let filter = params.to_filter()?;
    let data = state
        .metrics
        .wau_with_comparison(&filter, workspace.0)
        .await?;
    Ok(Json(ApiResponse::new(data)))
}

/// GET /api/v1/metrics/mau - Monthly active users
async fn get_mau(
    State(state): State<AppState>,
    _user: AuthUser,
    workspace: WorkspaceId,
    Query(params): Query<MetricParams>,
) -> Result<Json<ApiResponse<tell_analytics::TimeSeriesData>>> {
    let filter = params.to_filter()?;
    let data = state
        .metrics
        .mau_with_comparison(&filter, workspace.0)
        .await?;
    Ok(Json(ApiResponse::new(data)))
}

/// GET /api/v1/metrics/dau/raw - Raw user data for DAU
async fn get_dau_raw(
    State(state): State<AppState>,
    _user: AuthUser,
    workspace: WorkspaceId,
    Query(params): Query<DrillDownParams>,
) -> Result<Json<ApiResponse<tell_query::QueryResult>>> {
    let filter = params.to_filter()?;
    let data = state
        .metrics
        .drill_down_users(&filter, workspace.0, params.limit)
        .await?;
    Ok(Json(ApiResponse::new(data)))
}

/// GET /api/v1/metrics/wau/raw - Raw user data for WAU
async fn get_wau_raw(
    State(state): State<AppState>,
    _user: AuthUser,
    workspace: WorkspaceId,
    Query(params): Query<DrillDownParams>,
) -> Result<Json<ApiResponse<tell_query::QueryResult>>> {
    // WAU uses same drill-down as DAU, just different time range interpretation
    let filter = params.to_filter()?;
    let data = state
        .metrics
        .drill_down_users(&filter, workspace.0, params.limit)
        .await?;
    Ok(Json(ApiResponse::new(data)))
}

/// GET /api/v1/metrics/mau/raw - Raw user data for MAU
async fn get_mau_raw(
    State(state): State<AppState>,
    _user: AuthUser,
    workspace: WorkspaceId,
    Query(params): Query<DrillDownParams>,
) -> Result<Json<ApiResponse<tell_query::QueryResult>>> {
    let filter = params.to_filter()?;
    let data = state
        .metrics
        .drill_down_users(&filter, workspace.0, params.limit)
        .await?;
    Ok(Json(ApiResponse::new(data)))
}

// ============================================================================
// Events
// ============================================================================

/// GET /api/v1/metrics/events - Event counts over time
async fn get_events(
    State(state): State<AppState>,
    _user: AuthUser,
    workspace: WorkspaceId,
    Query(params): Query<MetricParams>,
) -> Result<Json<ApiResponse<tell_analytics::TimeSeriesData>>> {
    let filter = params.to_filter()?;
    let data = state
        .metrics
        .event_count_with_comparison(&filter, workspace.0)
        .await?;
    Ok(Json(ApiResponse::new(data)))
}

/// GET /api/v1/metrics/events/top - Top events by count
async fn get_top_events(
    State(state): State<AppState>,
    _user: AuthUser,
    workspace: WorkspaceId,
    Query(params): Query<TopParams>,
) -> Result<Json<ApiResponse<tell_analytics::TimeSeriesData>>> {
    let filter = params.to_filter()?;
    let metric = TopEventsMetric::new(params.limit);
    let data = state.metrics.execute(&metric, &filter, workspace.0).await?;
    Ok(Json(ApiResponse::new(data)))
}

/// GET /api/v1/metrics/events/raw - Raw event data
async fn get_events_raw(
    State(state): State<AppState>,
    _user: AuthUser,
    workspace: WorkspaceId,
    Query(params): Query<DrillDownParams>,
) -> Result<Json<ApiResponse<tell_query::QueryResult>>> {
    let filter = params.to_filter()?;
    let data = state
        .metrics
        .drill_down_events(&filter, workspace.0, params.limit)
        .await?;
    Ok(Json(ApiResponse::new(data)))
}

/// GET /api/v1/metrics/events/top/{event_name}/raw - Raw data for a specific top event
async fn get_top_event_raw(
    State(state): State<AppState>,
    _user: AuthUser,
    workspace: WorkspaceId,
    Path(event_name): Path<String>,
    Query(params): Query<DrillDownParams>,
) -> Result<Json<ApiResponse<tell_query::QueryResult>>> {
    let mut filter = params.to_filter()?;
    filter = filter.with_condition(tell_analytics::Condition::eq("event_name", &event_name));
    let data = state
        .metrics
        .drill_down_events(&filter, workspace.0, params.limit)
        .await?;
    Ok(Json(ApiResponse::new(data)))
}

/// GET /api/v1/metrics/events/properties - Event property breakdown over time
async fn get_event_properties(
    State(state): State<AppState>,
    _user: AuthUser,
    workspace: WorkspaceId,
    Query(params): Query<TopParams>,
) -> Result<Json<ApiResponse<tell_analytics::TimeSeriesData>>> {
    // Require both event and property params
    let event = params.event.as_ref().ok_or_else(|| {
        crate::error::ApiError::InvalidFilter("event parameter is required".to_string())
    })?;
    let property = params.property.as_ref().ok_or_else(|| {
        crate::error::ApiError::InvalidFilter("property parameter is required".to_string())
    })?;

    let filter = params.to_filter()?;
    let data = state
        .metrics
        .event_property_breakdown(event, property, &filter, workspace.0)
        .await?;
    Ok(Json(ApiResponse::new(data)))
}

/// GET /api/v1/metrics/events/custom - Custom event metric aggregation
async fn get_events_custom(
    State(state): State<AppState>,
    _user: AuthUser,
    workspace: WorkspaceId,
    Query(params): Query<CustomMetricParams>,
) -> Result<Json<ApiResponse<tell_analytics::TimeSeriesData>>> {
    params.validate()?;
    let filter = params.to_filter()?;
    let data = state
        .metrics
        .custom_event_metric(
            &params.event,
            &params.property,
            &params.metric,
            &filter,
            workspace.0,
        )
        .await?;
    Ok(Json(ApiResponse::new(data)))
}

// ============================================================================
// Logs
// ============================================================================

/// GET /api/v1/metrics/logs - Log volume over time
async fn get_logs(
    State(state): State<AppState>,
    _user: AuthUser,
    workspace: WorkspaceId,
    Query(params): Query<LogParams>,
) -> Result<Json<ApiResponse<tell_analytics::TimeSeriesData>>> {
    let filter = params.to_filter()?;
    let data = state
        .metrics
        .log_volume_with_comparison(&filter, workspace.0)
        .await?;
    Ok(Json(ApiResponse::new(data)))
}

/// GET /api/v1/metrics/logs/top - Top logs by property
async fn get_top_logs(
    State(state): State<AppState>,
    _user: AuthUser,
    workspace: WorkspaceId,
    Query(params): Query<TopParams>,
) -> Result<Json<ApiResponse<tell_analytics::TimeSeriesData>>> {
    let filter = params.to_filter()?;
    // Use property param to determine grouping, default to level
    let property = params.property.as_deref().unwrap_or("level");
    let metric = TopLogsMetric::by_field(property, params.limit);
    let data = state.metrics.execute(&metric, &filter, workspace.0).await?;
    Ok(Json(ApiResponse::new(data)))
}

/// GET /api/v1/metrics/logs/raw - Raw log data
async fn get_logs_raw(
    State(state): State<AppState>,
    _user: AuthUser,
    workspace: WorkspaceId,
    Query(params): Query<DrillDownParams>,
) -> Result<Json<ApiResponse<tell_query::QueryResult>>> {
    let filter = params.to_filter()?;
    let data = state
        .metrics
        .drill_down_logs(&filter, workspace.0, params.limit)
        .await?;
    Ok(Json(ApiResponse::new(data)))
}

// ============================================================================
// Sessions
// ============================================================================

/// GET /api/v1/metrics/sessions - Session volume over time
async fn get_sessions(
    State(state): State<AppState>,
    _user: AuthUser,
    workspace: WorkspaceId,
    Query(params): Query<MetricParams>,
) -> Result<Json<ApiResponse<tell_analytics::TimeSeriesData>>> {
    let filter = params.to_filter()?;
    let metric = SessionsMetric::volume();
    let data = state
        .metrics
        .execute_with_comparison(&metric, &filter, workspace.0)
        .await?;
    Ok(Json(ApiResponse::new(data)))
}

/// GET /api/v1/metrics/sessions/top - Top sessions by property
async fn get_sessions_top(
    State(state): State<AppState>,
    _user: AuthUser,
    workspace: WorkspaceId,
    Query(params): Query<TopParams>,
) -> Result<Json<ApiResponse<tell_analytics::TimeSeriesData>>> {
    let filter = params.to_filter()?;
    // Use property param to determine grouping, default to device_type
    let by = params.property.as_deref().unwrap_or("device_type");
    let metric = TopSessionsMetric::new(by, params.limit);
    let data = state.metrics.execute(&metric, &filter, workspace.0).await?;
    Ok(Json(ApiResponse::new(data)))
}

/// GET /api/v1/metrics/sessions/unique - Unique sessions over time
async fn get_sessions_unique(
    State(state): State<AppState>,
    _user: AuthUser,
    workspace: WorkspaceId,
    Query(params): Query<MetricParams>,
) -> Result<Json<ApiResponse<tell_analytics::TimeSeriesData>>> {
    let filter = params.to_filter()?;
    let metric = SessionsMetric::unique();
    let data = state
        .metrics
        .execute_with_comparison(&metric, &filter, workspace.0)
        .await?;
    Ok(Json(ApiResponse::new(data)))
}

/// GET /api/v1/metrics/sessions/raw - Raw session data
async fn get_sessions_raw(
    State(state): State<AppState>,
    _user: AuthUser,
    workspace: WorkspaceId,
    Query(params): Query<DrillDownParams>,
) -> Result<Json<ApiResponse<tell_query::QueryResult>>> {
    let filter = params.to_filter()?;
    let data = state
        .metrics
        .drill_down_sessions(&filter, workspace.0, params.limit)
        .await?;
    Ok(Json(ApiResponse::new(data)))
}

// ============================================================================
// Stickiness
// ============================================================================

/// GET /api/v1/metrics/stickiness/daily - Daily stickiness (DAU/MAU)
async fn get_daily_stickiness(
    State(state): State<AppState>,
    _user: AuthUser,
    workspace: WorkspaceId,
    Query(params): Query<MetricParams>,
) -> Result<Json<ApiResponse<tell_analytics::TimeSeriesData>>> {
    let filter = params.to_filter()?;
    let metric = StickinessMetric::daily();
    let data = state
        .metrics
        .execute_with_comparison(&metric, &filter, workspace.0)
        .await?;
    Ok(Json(ApiResponse::new(data)))
}

/// GET /api/v1/metrics/stickiness/weekly - Weekly stickiness (WAU/MAU)
async fn get_weekly_stickiness(
    State(state): State<AppState>,
    _user: AuthUser,
    workspace: WorkspaceId,
    Query(params): Query<MetricParams>,
) -> Result<Json<ApiResponse<tell_analytics::TimeSeriesData>>> {
    let filter = params.to_filter()?;
    let metric = StickinessMetric::weekly();
    let data = state
        .metrics
        .execute_with_comparison(&metric, &filter, workspace.0)
        .await?;
    Ok(Json(ApiResponse::new(data)))
}

// ============================================================================
// Users
// ============================================================================

/// GET /api/v1/metrics/users - User count over time
async fn get_users(
    State(state): State<AppState>,
    _user: AuthUser,
    workspace: WorkspaceId,
    Query(params): Query<MetricParams>,
) -> Result<Json<ApiResponse<tell_analytics::TimeSeriesData>>> {
    let filter = params.to_filter()?;
    let metric = UsersMetric::count();
    let data = state
        .metrics
        .execute_with_comparison(&metric, &filter, workspace.0)
        .await?;
    Ok(Json(ApiResponse::new(data)))
}

/// GET /api/v1/metrics/users/raw - Raw user/device data
async fn get_users_raw(
    State(state): State<AppState>,
    _user: AuthUser,
    workspace: WorkspaceId,
    Query(params): Query<DrillDownParams>,
) -> Result<Json<ApiResponse<tell_query::QueryResult>>> {
    let filter = params.to_filter()?;
    let data = state
        .metrics
        .drill_down_users(&filter, workspace.0, params.limit)
        .await?;
    Ok(Json(ApiResponse::new(data)))
}
