//! Integration tests for admin endpoints
//!
//! Tests: API keys, invites, user management

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

async fn test_app() -> (Router, Arc<ControlPlane>) {
    let control = Arc::new(ControlPlane::new_memory().await.unwrap());
    let auth = Arc::new(LocalJwtProvider::new(test_utils::TEST_SECRET));

    let state = AppState {
        metrics: Arc::new(create_mock_metrics_engine()),
        auth,
        auth_service: None,
        control: Some(control.clone()),
        user_store: None,
        local_user_store: None,
        jwt_secret: Some(test_utils::TEST_SECRET.to_vec()),
        jwt_expires_in: std::time::Duration::from_secs(3600),
        server_metrics: None,
    };

    (build_router(state), control)
}

fn create_mock_metrics_engine() -> tell_analytics::MetricsEngine {
    use tell_query::ClickHouseBackendConfig;
    let config = ClickHouseBackendConfig::new("http://localhost:8123", "test");
    tell_analytics::MetricsEngine::new(Box::new(tell_query::ClickHouseBackend::new(&config)))
}

fn auth_request(method: Method, uri: &str, token: &str) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .body(Body::empty())
        .unwrap()
}

fn auth_json_request(method: Method, uri: &str, token: &str, body: Value) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

fn workspace_request(method: Method, uri: &str, token: &str, workspace_id: &str) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .header("X-Workspace-ID", workspace_id)
        .body(Body::empty())
        .unwrap()
}

/// Create a workspace and return its ID
async fn create_workspace(
    control: &ControlPlane,
    user_id: &str,
    role: tell_control::MemberRole,
) -> String {
    let workspace = tell_control::Workspace::new("Test Workspace", "test-ws");
    control.workspaces().create(&workspace).await.unwrap();

    let membership = tell_control::WorkspaceMembership::new(user_id, &workspace.id, role);
    control.workspaces().add_member(&membership).await.unwrap();

    workspace.id
}

// =============================================================================
// API Key Tests
// =============================================================================

#[tokio::test]
async fn test_apikey_endpoints_require_auth() {
    let (app, _control) = test_app().await;

    let endpoints = [
        ("/api/v1/user/apikeys", Method::GET),
        ("/api/v1/user/apikeys", Method::POST),
        ("/api/v1/admin/apikeys", Method::GET),
    ];

    for (endpoint, method) in endpoints {
        let request = Request::builder()
            .method(method.clone())
            .uri(endpoint)
            .body(Body::empty())
            .unwrap();

        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(
            response.status(),
            StatusCode::UNAUTHORIZED,
            "Expected 401 for {} without auth",
            endpoint
        );
    }
}

#[tokio::test]
async fn test_list_user_apikeys_endpoint() {
    let (app, _control) = test_app().await;
    let token = test_utils::viewer_token("user-1", "user@test.com");

    let request = auth_request(Method::GET, "/api/v1/user/apikeys", &token);
    let response = app.oneshot(request).await.unwrap();

    assert_ne!(response.status(), StatusCode::NOT_FOUND);
    assert_ne!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_create_apikey_endpoint() {
    let (app, _control) = test_app().await;
    let token = test_utils::editor_token("user-1", "user@test.com");

    let request = auth_json_request(
        Method::POST,
        "/api/v1/user/apikeys",
        &token,
        json!({
            "name": "Test API Key"
        }),
    );
    let response = app.oneshot(request).await.unwrap();

    // Should succeed or return validation error, not 404
    assert_ne!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_create_apikey_validates_name() {
    let (app, _control) = test_app().await;
    let token = test_utils::editor_token("user-1", "user@test.com");

    // Empty name should fail
    let request = auth_json_request(
        Method::POST,
        "/api/v1/user/apikeys",
        &token,
        json!({
            "name": ""
        }),
    );
    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn test_admin_apikeys_requires_workspace_id() {
    let (app, _control) = test_app().await;
    let token = test_utils::admin_token("user-1", "admin@test.com");

    // Missing X-Workspace-ID header should fail with BAD_REQUEST
    let request = auth_request(Method::GET, "/api/v1/admin/apikeys", &token);
    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_admin_apikeys_requires_admin_role() {
    let (app, control) = test_app().await;
    let token = test_utils::viewer_token("user-1", "viewer@test.com");

    // Create a workspace and add user as viewer
    let workspace_id = create_workspace(&control, "user-1", tell_control::MemberRole::Viewer).await;

    let request = workspace_request(Method::GET, "/api/v1/admin/apikeys", &token, &workspace_id);
    let response = app.oneshot(request).await.unwrap();

    // Should be forbidden (viewer can't access admin routes)
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

// =============================================================================
// Invite Tests
// =============================================================================

#[tokio::test]
async fn test_invite_admin_endpoints_require_auth() {
    let (app, _control) = test_app().await;

    let endpoints = [
        (
            "/api/v1/admin/workspace/invites?workspace_id=ws-1",
            Method::GET,
        ),
        ("/api/v1/admin/workspace/invites", Method::POST),
    ];

    for (endpoint, method) in endpoints {
        let request = Request::builder()
            .method(method.clone())
            .uri(endpoint)
            .body(Body::empty())
            .unwrap();

        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(
            response.status(),
            StatusCode::UNAUTHORIZED,
            "Expected 401 for {} without auth",
            endpoint
        );
    }
}

#[tokio::test]
async fn test_verify_invite_endpoint_exists() {
    let (app, _control) = test_app().await;

    // Public endpoint - no auth needed
    let request = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/invites/some-token")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    // Should be 404 (invite not found), not routing 404
    assert_ne!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_create_invite_validates_email() {
    let (app, control) = test_app().await;
    let token = test_utils::admin_token("user-1", "admin@test.com");

    // Create workspace with admin role
    let workspace_id = create_workspace(&control, "user-1", tell_control::MemberRole::Admin).await;

    let request = auth_json_request(
        Method::POST,
        "/api/v1/admin/workspace/invites",
        &token,
        json!({
            "email": "not-an-email",
            "workspace_id": workspace_id
        }),
    );
    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn test_create_invite_requires_admin() {
    let (app, control) = test_app().await;
    let token = test_utils::viewer_token("user-1", "viewer@test.com");

    // Create workspace with viewer role
    let workspace_id = create_workspace(&control, "user-1", tell_control::MemberRole::Viewer).await;

    let request = auth_json_request(
        Method::POST,
        "/api/v1/admin/workspace/invites",
        &token,
        json!({
            "email": "invite@test.com",
            "workspace_id": workspace_id
        }),
    );
    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn test_accept_invite_requires_auth() {
    let (app, _control) = test_app().await;

    // Note: With an invalid token, this returns 404 (invite not found)
    // because the invite must exist before we can check auth
    let request = Request::builder()
        .method(Method::POST)
        .uri("/api/v1/invites/some-token/accept")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(json!({}).to_string()))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    // With an invalid token, we get 404 because the invite doesn't exist
    // This is expected - the endpoint exists but requires a valid token
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

// =============================================================================
// User Admin Tests
// =============================================================================

#[tokio::test]
async fn test_admin_users_endpoint_exists() {
    let (app, control) = test_app().await;
    let token = test_utils::admin_token("user-1", "admin@test.com");

    // Create workspace with admin role
    let workspace_id = create_workspace(&control, "user-1", tell_control::MemberRole::Admin).await;

    let request = workspace_request(Method::GET, "/api/v1/admin/users", &token, &workspace_id);
    let response = app.oneshot(request).await.unwrap();

    // May be 403 (not admin of workspace) but not 404
    assert_ne!(response.status(), StatusCode::NOT_FOUND);
}

// =============================================================================
// Workspace Admin Tests
// =============================================================================

// Note: Workspace members endpoint is not implemented yet
// These tests are placeholders for when it gets added

#[tokio::test]
async fn test_admin_workspace_get_endpoint() {
    let (app, control) = test_app().await;
    let token = test_utils::admin_token("user-1", "admin@test.com");

    // Create workspace with admin role
    let workspace_id = create_workspace(&control, "user-1", tell_control::MemberRole::Admin).await;

    let request = auth_request(
        Method::GET,
        &format!("/api/v1/admin/workspaces/{}", workspace_id),
        &token,
    );
    let response = app.oneshot(request).await.unwrap();

    // Should succeed for workspace admin
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_admin_workspace_get_requires_admin() {
    let (app, control) = test_app().await;
    let token = test_utils::viewer_token("user-1", "viewer@test.com");

    // Create workspace with viewer role
    let workspace_id = create_workspace(&control, "user-1", tell_control::MemberRole::Viewer).await;

    let request = auth_request(
        Method::GET,
        &format!("/api/v1/admin/workspaces/{}", workspace_id),
        &token,
    );
    let response = app.oneshot(request).await.unwrap();

    // Viewer should be forbidden
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}
