//! API routes
//!
//! Domain-grouped HTTP route handlers.

pub mod admin;
pub mod auth;
pub mod product;
pub mod public;

use axum::Router;

use crate::state::AppState;

/// Build the complete API router
pub fn build_router(state: AppState) -> Router {
    // Combine user routes (workspaces + api keys)
    let user_routes = admin::workspace_user_routes().merge(admin::apikey_user_routes());

    // Combine admin routes (workspaces + api keys + users + invites)
    let admin_routes = admin::workspace_admin_routes()
        .merge(admin::apikey_admin_routes())
        .merge(admin::user_admin_routes())
        .merge(admin::invite_admin_routes());

    Router::new()
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
        .nest("/s", public::routes())
        .with_state(state)
}
