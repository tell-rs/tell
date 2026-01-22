//! API routes
//!
//! Domain-grouped HTTP route handlers.

pub mod admin;
pub mod auth;
pub mod ops;
pub mod product;
pub mod public;

use axum::{Router, middleware};

use crate::audit::audit_layer;
use crate::state::AppState;

/// Options for building the router
#[derive(Debug, Clone, Default)]
pub struct RouterOptions {
    /// Enable audit logging middleware
    pub audit_logging: bool,
}

/// Build the complete API router
pub fn build_router(state: AppState) -> Router {
    build_router_with_options(state, RouterOptions::default())
}

/// Build the complete API router with options
pub fn build_router_with_options(state: AppState, options: RouterOptions) -> Router {
    // Combine user routes (workspaces + api keys)
    let user_routes = admin::workspace_user_routes().merge(admin::apikey_user_routes());

    // Combine admin routes (workspaces + api keys + users + invites)
    let admin_routes = admin::workspace_admin_routes()
        .merge(admin::apikey_admin_routes())
        .merge(admin::user_admin_routes())
        .merge(admin::invite_admin_routes());

    let router = Router::new()
        // Operations routes (health, metrics - no auth)
        .merge(ops::routes())
        // Auth routes (login, setup)
        .nest("/api/v1/auth", auth::routes())
        // User routes (own workspaces, own API keys)
        .nest("/api/v1/user", user_routes)
        // Admin routes (workspace management, API key management)
        .nest("/api/v1/admin", admin_routes)
        // Product routes (metrics, boards, data)
        .nest("/api/v1/metrics", product::metrics::routes())
        .nest("/api/v1/boards", product::boards::routes())
        .nest("/api/v1/data", product::data::routes())
        // Public invite routes (verify/accept - no auth required)
        .nest("/api/v1", admin::invite_public_routes())
        // Public routes (shared links - no auth)
        .nest("/s", public::routes());

    // Conditionally add audit logging middleware
    let router = if options.audit_logging {
        router.layer(middleware::from_fn(audit_layer))
    } else {
        router
    };

    router.with_state(state)
}
