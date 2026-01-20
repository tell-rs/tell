//! Tell API
//!
//! HTTP API for Tell analytics queries.
//!
//! # Overview
//!
//! This crate provides the REST API for querying analytics data. It's built on
//! Axum and integrates with the `tell-analytics` crate for query execution.
//!
//! # Usage
//!
//! ```ignore
//! use tell_api::{build_router, AppState};
//! use tell_analytics::MetricsEngine;
//!
//! // Create metrics engine with a query backend
//! let engine = MetricsEngine::new(backend);
//! let state = AppState::new(engine);
//!
//! // Build the router
//! let app = build_router(state);
//!
//! // Run with Axum
//! let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
//! axum::serve(listener, app).await?;
//! ```
//!
//! # Endpoints
//!
//! ## Active Users
//! - `GET /api/v1/metrics/dau` - Daily active users
//! - `GET /api/v1/metrics/wau` - Weekly active users
//! - `GET /api/v1/metrics/mau` - Monthly active users
//! - `GET /api/v1/metrics/{dau,wau,mau}/raw` - Raw user data
//!
//! ## Events
//! - `GET /api/v1/metrics/events` - Event counts
//! - `GET /api/v1/metrics/events/top` - Top events
//! - `GET /api/v1/metrics/events/raw` - Raw event data
//!
//! ## Logs
//! - `GET /api/v1/metrics/logs` - Log volume
//! - `GET /api/v1/metrics/logs/top` - Top logs by level
//! - `GET /api/v1/metrics/logs/raw` - Raw log data
//!
//! ## Sessions
//! - `GET /api/v1/metrics/sessions` - Session volume
//! - `GET /api/v1/metrics/sessions/unique` - Unique sessions
//! - `GET /api/v1/metrics/sessions/raw` - Raw session data
//!
//! ## Stickiness
//! - `GET /api/v1/metrics/stickiness/daily` - DAU/MAU ratio
//! - `GET /api/v1/metrics/stickiness/weekly` - WAU/MAU ratio
//!
//! # Query Parameters
//!
//! Most endpoints accept these query parameters:
//! - `range` - Time range (e.g., "7d", "30d", "2024-01-01,2024-01-31")
//! - `granularity` - Aggregation period (minute, hourly, daily, weekly, monthly)
//! - `breakdown` - Dimension to group by (e.g., "device_type", "country")
//! - `compare` - Comparison mode ("previous", "yoy")

pub mod audit;
pub mod auth;
pub mod error;
pub mod ratelimit;
pub mod routes;
pub mod state;
pub mod types;

// Re-exports
pub use audit::{AuditAction, audit_layer};
pub use auth::{
    AuthUser, OptionalAuthUser, Permission, Role, RouterExt, UserInfo, WorkspaceId,
    require_permission,
};
pub use error::{ApiError, Result};
pub use ratelimit::{RateLimitConfig, RateLimitLayer};
pub use routes::build_router;
pub use state::AppState;
pub use types::{ApiResponse, MetricParams};
