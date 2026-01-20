//! Permission enforcement middleware
//!
//! Provides route-level permission checking with a clean API.
//!
//! # Example
//!
//! ```ignore
//! use tell_api::auth::{Permission, RouterExt};
//!
//! // Protect routes that create content
//! Router::new()
//!     .route("/dashboard", post(create_dashboard))
//!     .with_permission(Permission::Create)
//!
//! // Protect admin routes
//! Router::new()
//!     .route("/members", post(add_member))
//!     .with_permission(Permission::Admin)
//! ```

use std::task::{Context, Poll};

use axum::{
    extract::Request,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json, Router,
};
use futures_util::future::BoxFuture;
use tower::{Layer, Service};

use tell_auth::{Permission, UserInfo};

/// Layer that requires a specific permission
#[derive(Clone)]
pub struct RequirePermissionLayer {
    pub(crate) permission: Permission,
}

impl RequirePermissionLayer {
    pub fn new(permission: Permission) -> Self {
        Self { permission }
    }
}

impl<S> Layer<S> for RequirePermissionLayer {
    type Service = RequirePermissionService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RequirePermissionService {
            inner,
            permission: self.permission,
        }
    }
}

/// Service that checks permission before forwarding request
#[derive(Clone)]
pub struct RequirePermissionService<S> {
    inner: S,
    permission: Permission,
}

impl<S> Service<Request> for RequirePermissionService<S>
where
    S: Service<Request, Response = Response> + Clone + Send + 'static,
    S::Future: Send,
{
    type Response = Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request) -> Self::Future {
        let permission = self.permission;
        let mut inner = self.inner.clone();

        Box::pin(async move {
            // Get user from request extensions (set by AuthUser extractor)
            let user: Option<&UserInfo> = req.extensions().get();

            match user {
                Some(user) if user.has_permission(permission) => {
                    // User has permission, proceed
                    inner.call(req).await
                }
                Some(_) => {
                    // User authenticated but lacks permission
                    Ok(permission_denied_response(permission))
                }
                None => {
                    // Not authenticated
                    Ok(unauthorized_response())
                }
            }
        })
    }
}

fn permission_denied_response(permission: Permission) -> Response {
    let body = serde_json::json!({
        "error": "FORBIDDEN",
        "message": format!("Requires {} permission", permission.as_str()),
    });
    (StatusCode::FORBIDDEN, Json(body)).into_response()
}

fn unauthorized_response() -> Response {
    let body = serde_json::json!({
        "error": "UNAUTHORIZED",
        "message": "Authentication required",
    });
    (StatusCode::UNAUTHORIZED, Json(body)).into_response()
}

/// Create a permission layer
///
/// # Example
///
/// ```ignore
/// Router::new()
///     .route("/dashboard", post(create_dashboard))
///     .route_layer(require_permission(Permission::Create))
/// ```
pub fn require_permission(permission: Permission) -> RequirePermissionLayer {
    RequirePermissionLayer::new(permission)
}

/// Extension trait for Router with permission helpers
///
/// Provides a cleaner API for adding permission requirements.
pub trait RouterExt<S> {
    /// Require a permission for all routes in this router
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Routes that require content creation ability
    /// Router::new()
    ///     .route("/dashboard", post(create_dashboard))
    ///     .route("/query", post(save_query))
    ///     .with_permission(Permission::Create)
    ///
    /// // Admin-only routes
    /// Router::new()
    ///     .route("/members", get(list_members).post(add_member))
    ///     .route("/settings", get(get_settings).put(update_settings))
    ///     .with_permission(Permission::Admin)
    /// ```
    fn with_permission(self, permission: Permission) -> Self;
}

impl<S> RouterExt<S> for Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    fn with_permission(self, permission: Permission) -> Self {
        self.layer(RequirePermissionLayer::new(permission))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_require_permission_layer() {
        let layer = require_permission(Permission::Create);
        assert_eq!(layer.permission, Permission::Create);

        let admin_layer = require_permission(Permission::Admin);
        assert_eq!(admin_layer.permission, Permission::Admin);
    }
}
