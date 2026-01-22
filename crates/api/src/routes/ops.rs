//! Operations routes
//!
//! Health checks and server metrics endpoints for monitoring and observability.
//! These routes do not require authentication.

use std::sync::Arc;
use std::time::Instant;

use axum::{Json, Router, extract::State, routing::get};
use serde::Serialize;
use tell_metrics::{
    PipelineSnapshot, SinkMetricsProvider, SinkMetricsSnapshot, SourceMetricsProvider,
    SourceMetricsSnapshot,
};

use crate::state::AppState;

// =============================================================================
// Server Metrics State
// =============================================================================

/// Server metrics handles for the ingestion pipeline
///
/// This is populated when running `tell serve` to expose pipeline metrics.
/// When running `tell api` standalone, this will be None.
pub struct ServerMetrics {
    /// Server start time for uptime calculation
    pub start_time: Instant,
    /// Pipeline metrics snapshot function
    pub pipeline_snapshot: Option<Box<dyn Fn() -> PipelineSnapshot + Send + Sync>>,
    /// Source metrics providers
    pub sources: Vec<Arc<dyn SourceMetricsProvider>>,
    /// Sink metrics providers
    pub sinks: Vec<Arc<dyn SinkMetricsProvider>>,
}

impl ServerMetrics {
    /// Create new server metrics
    pub fn new(
        pipeline_snapshot: Option<Box<dyn Fn() -> PipelineSnapshot + Send + Sync>>,
        sources: Vec<Arc<dyn SourceMetricsProvider>>,
        sinks: Vec<Arc<dyn SinkMetricsProvider>>,
    ) -> Self {
        Self {
            start_time: Instant::now(),
            pipeline_snapshot,
            sources,
            sinks,
        }
    }

    /// Get uptime in seconds
    pub fn uptime_secs(&self) -> u64 {
        self.start_time.elapsed().as_secs()
    }
}

// =============================================================================
// Response Types
// =============================================================================

/// Health check response
#[derive(Debug, Serialize)]
pub struct HealthResponse {
    /// Server status
    pub status: &'static str,
    /// Uptime in seconds (only if server metrics available)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uptime_secs: Option<u64>,
}

/// Server metrics response
#[derive(Debug, Serialize)]
pub struct MetricsResponse {
    /// Server uptime in seconds
    pub uptime_secs: u64,
    /// Pipeline metrics
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pipeline: Option<PipelineSnapshot>,
    /// Per-source metrics
    pub sources: Vec<SourceSnapshot>,
    /// Per-sink metrics
    pub sinks: Vec<SinkSnapshot>,
}

/// Source metrics with metadata
#[derive(Debug, Serialize)]
pub struct SourceSnapshot {
    /// Source identifier
    pub id: String,
    /// Source type (tcp, http, syslog, etc.)
    #[serde(rename = "type")]
    pub source_type: String,
    /// Metrics
    #[serde(flatten)]
    pub metrics: SourceMetricsSnapshot,
}

/// Sink metrics with metadata
#[derive(Debug, Serialize)]
pub struct SinkSnapshot {
    /// Sink identifier
    pub id: String,
    /// Sink type (clickhouse, disk_binary, etc.)
    #[serde(rename = "type")]
    pub sink_type: String,
    /// Metrics
    #[serde(flatten)]
    pub metrics: SinkMetricsSnapshot,
}

// =============================================================================
// Routes
// =============================================================================

/// Operations routes (health, metrics)
pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/health", get(health_handler))
        .route("/metrics", get(metrics_handler))
}

// =============================================================================
// Handlers
// =============================================================================

/// Health check endpoint
///
/// GET /health
///
/// Returns server status. Always returns 200 OK if the API is running.
async fn health_handler(State(state): State<AppState>) -> Json<HealthResponse> {
    let uptime_secs = state.server_metrics.as_ref().map(|m| m.uptime_secs());

    Json(HealthResponse {
        status: "ok",
        uptime_secs,
    })
}

/// Server metrics endpoint
///
/// GET /metrics
///
/// Returns pipeline, source, and sink metrics. Returns 503 if server metrics
/// are not available (e.g., running `tell api` without `tell serve`).
async fn metrics_handler(
    State(state): State<AppState>,
) -> Result<Json<MetricsResponse>, (axum::http::StatusCode, Json<serde_json::Value>)> {
    let server = state.server_metrics.as_ref().ok_or_else(|| {
        (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "error": "SERVER_NOT_RUNNING",
                "message": "Server metrics not available. Start with `tell serve`."
            })),
        )
    })?;

    let pipeline = server.pipeline_snapshot.as_ref().map(|f| f());

    let sources: Vec<_> = server
        .sources
        .iter()
        .map(|s| SourceSnapshot {
            id: s.source_id().to_string(),
            source_type: s.source_type().to_string(),
            metrics: s.snapshot(),
        })
        .collect();

    let sinks: Vec<_> = server
        .sinks
        .iter()
        .map(|s| SinkSnapshot {
            id: s.sink_id().to_string(),
            sink_type: s.sink_type().to_string(),
            metrics: s.snapshot(),
        })
        .collect();

    Ok(Json(MetricsResponse {
        uptime_secs: server.uptime_secs(),
        pipeline,
        sources,
        sinks,
    }))
}
