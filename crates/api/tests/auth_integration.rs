//! Integration tests for auth endpoints
//!
//! Tests: setup status, protected routes, token validation

use axum::{
    Router,
    body::Body,
    http::{Method, Request, StatusCode, header},
};
use serde_json::{Value, json};
use std::sync::Arc;
use tower::ServiceExt;

use tell_api::{routes::build_router, state::AppState};
use tell_auth::{LocalJwtProvider, test_utils};
use tell_control::ControlPlane;

async fn test_app() -> Router {
    let control = Arc::new(ControlPlane::new_memory().await.unwrap());
    let auth = Arc::new(LocalJwtProvider::new(test_utils::TEST_SECRET));

    let state = AppState {
        metrics: Arc::new(create_mock_metrics_engine()),
        auth,
        auth_service: None,
        control: Some(control),
        user_store: None,
        local_user_store: None,
        jwt_secret: Some(test_utils::TEST_SECRET.to_vec()),
        jwt_expires_in: std::time::Duration::from_secs(3600),
        server_metrics: None,
    };

    build_router(state)
}

fn create_mock_metrics_engine() -> tell_analytics::MetricsEngine {
    use tell_query::ClickHouseBackendConfig;
    let config = ClickHouseBackendConfig::new("http://localhost:8123", "test");
    tell_analytics::MetricsEngine::new(Box::new(tell_query::ClickHouseBackend::new(&config)))
}

async fn response_json(response: axum::response::Response) -> Value {
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&body).unwrap_or(json!({}))
}

#[tokio::test]
async fn test_setup_status_no_user_store() {
    let app = test_app().await;

    let request = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/auth/setup/status")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    // Without user_store, setup endpoints may return different behavior
    // Just verify the endpoint exists and responds
    assert_ne!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_protected_route_requires_auth() {
    let app = test_app().await;

    let request = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/user/workspaces")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_protected_route_with_valid_token() {
    let app = test_app().await;

    let token = test_utils::admin_token("user-1", "admin@test.com");

    let request = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/user/workspaces")
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_protected_route_with_invalid_token() {
    let app = test_app().await;

    let request = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/user/workspaces")
        .header(header::AUTHORIZATION, "Bearer invalid_token_here")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_protected_route_with_expired_token_prefix() {
    let app = test_app().await;

    // Token without the tell_ prefix should fail
    let request = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/user/workspaces")
        .header(header::AUTHORIZATION, "Bearer eyJhbGciOiJIUzI1NiJ9.eyJ0b2tlbl9pZCI6InRlc3QiLCJ1c2VyX2lkIjoidXNlci0xIiwiZW1haWwiOiJ0ZXN0QGV4YW1wbGUuY29tIiwicm9sZSI6ImFkbWluIiwid29ya3NwYWNlX2lkIjoiMSIsInBlcm1pc3Npb25zIjpbXSwiZXhwIjoxLCJpYXQiOjF9.invalid")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_viewer_cannot_access_admin_routes() {
    let app = test_app().await;

    let token = test_utils::viewer_token("user-1", "viewer@test.com");

    // Viewers should not be able to create workspaces
    let request = Request::builder()
        .method(Method::POST)
        .uri("/api/v1/user/workspaces")
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            json!({"name": "Test", "slug": "test"}).to_string(),
        ))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    // Should be forbidden (403) since viewers can't create
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn test_editor_can_create_workspace() {
    let app = test_app().await;

    let token = test_utils::editor_token("user-1", "editor@test.com");

    let request = Request::builder()
        .method(Method::POST)
        .uri("/api/v1/user/workspaces")
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            json!({"name": "My Workspace", "slug": "my-ws"}).to_string(),
        ))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    let json = response_json(response).await;
    assert_eq!(json["name"], "My Workspace");
}

#[tokio::test]
async fn test_health_endpoint_no_auth() {
    let app = test_app().await;

    let request = Request::builder()
        .method(Method::GET)
        .uri("/health")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}
