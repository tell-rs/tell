//! Integration tests for data query endpoints
//!
//! Note: These test endpoint routing, auth, and validation, not actual ClickHouse queries.

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

/// Helper to make workspace-scoped requests
fn workspace_request(method: Method, uri: &str, token: &str, workspace_id: &str) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .header("X-Workspace-ID", workspace_id)
        .body(Body::empty())
        .unwrap()
}

/// Helper to make workspace-scoped JSON requests
fn workspace_json_request(
    method: Method,
    uri: &str,
    token: &str,
    workspace_id: &str,
    body: Value,
) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .header(header::CONTENT_TYPE, "application/json")
        .header("X-Workspace-ID", workspace_id)
        .body(Body::from(body.to_string()))
        .unwrap()
}

/// Create a workspace and return its ID
async fn create_workspace(control: &ControlPlane, user_id: &str) -> String {
    // Create workspace via control plane directly
    let workspace = tell_control::Workspace::new("Test Workspace", "test-ws");
    control.workspaces().create(&workspace).await.unwrap();

    // Add user as member
    let membership = tell_control::WorkspaceMembership::new(
        user_id,
        &workspace.id,
        tell_control::MemberRole::Editor,
    );
    control.workspaces().add_member(&membership).await.unwrap();

    workspace.id
}

#[tokio::test]
async fn test_data_endpoints_require_auth() {
    let (app, _control) = test_app().await;

    let endpoints = [
        ("/api/v1/data/sources", Method::GET),
        ("/api/v1/data/sources/events/fields", Method::GET),
        ("/api/v1/data/sources/events/values/event_name", Method::GET),
        ("/api/v1/data/query", Method::POST),
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
async fn test_list_sources_endpoint() {
    let (app, control) = test_app().await;
    let token = test_utils::viewer_token("user-1", "user@test.com");
    let workspace_id = create_workspace(&control, "user-1").await;

    let request = workspace_request(Method::GET, "/api/v1/data/sources", &token, &workspace_id);
    let response = app.oneshot(request).await.unwrap();

    // Should not be 404 or 401
    assert_ne!(response.status(), StatusCode::NOT_FOUND);
    assert_ne!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_source_fields_endpoint() {
    let (app, control) = test_app().await;
    let token = test_utils::viewer_token("user-1", "user@test.com");
    let workspace_id = create_workspace(&control, "user-1").await;

    let sources = ["events", "logs", "context", "user_traits"];

    for source in sources {
        let uri = format!("/api/v1/data/sources/{}/fields", source);
        let request = workspace_request(Method::GET, &uri, &token, &workspace_id);
        let response = app.clone().oneshot(request).await.unwrap();

        assert_ne!(
            response.status(),
            StatusCode::NOT_FOUND,
            "Expected {} fields endpoint to exist",
            source
        );
    }
}

#[tokio::test]
async fn test_source_fields_invalid_source() {
    let (app, control) = test_app().await;
    let token = test_utils::viewer_token("user-1", "user@test.com");
    let workspace_id = create_workspace(&control, "user-1").await;

    let request = workspace_request(
        Method::GET,
        "/api/v1/data/sources/invalid/fields",
        &token,
        &workspace_id,
    );
    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_field_values_endpoint() {
    let (app, control) = test_app().await;
    let token = test_utils::viewer_token("user-1", "user@test.com");
    let workspace_id = create_workspace(&control, "user-1").await;

    let request = workspace_request(
        Method::GET,
        "/api/v1/data/sources/events/values/event_name",
        &token,
        &workspace_id,
    );
    let response = app.oneshot(request).await.unwrap();

    // May fail if ClickHouse not running, but should not be 404
    assert_ne!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_field_values_invalid_field() {
    let (app, control) = test_app().await;
    let token = test_utils::viewer_token("user-1", "user@test.com");
    let workspace_id = create_workspace(&control, "user-1").await;

    let request = workspace_request(
        Method::GET,
        "/api/v1/data/sources/events/values/not_a_real_field",
        &token,
        &workspace_id,
    );
    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn test_query_endpoint_exists() {
    let (app, control) = test_app().await;
    let token = test_utils::viewer_token("user-1", "user@test.com");
    let workspace_id = create_workspace(&control, "user-1").await;

    let request = workspace_json_request(
        Method::POST,
        "/api/v1/data/query",
        &token,
        &workspace_id,
        json!({
            "query": "SELECT * FROM events_v1 LIMIT 10"
        }),
    );
    let response = app.oneshot(request).await.unwrap();

    // May fail if ClickHouse not running, but should not be 404
    assert_ne!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_query_rejects_empty() {
    let (app, control) = test_app().await;
    let token = test_utils::viewer_token("user-1", "user@test.com");
    let workspace_id = create_workspace(&control, "user-1").await;

    let request = workspace_json_request(
        Method::POST,
        "/api/v1/data/query",
        &token,
        &workspace_id,
        json!({
            "query": ""
        }),
    );
    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn test_query_rejects_non_select() {
    let (app, control) = test_app().await;
    let token = test_utils::viewer_token("user-1", "user@test.com");
    let workspace_id = create_workspace(&control, "user-1").await;

    let bad_queries = [
        "INSERT INTO events_v1 VALUES (1, 2, 3)",
        "DELETE FROM events_v1",
        "DROP TABLE events_v1",
        "UPDATE events_v1 SET x = 1",
        "ALTER TABLE events_v1 ADD COLUMN x INT",
    ];

    for query in bad_queries {
        let request = workspace_json_request(
            Method::POST,
            "/api/v1/data/query",
            &token,
            &workspace_id,
            json!({
                "query": query
            }),
        );
        let response = app.clone().oneshot(request).await.unwrap();

        assert_eq!(
            response.status(),
            StatusCode::UNPROCESSABLE_ENTITY,
            "Expected 422 for dangerous query: {}",
            query
        );
    }
}

#[tokio::test]
async fn test_query_blocks_injection_in_select() {
    let (app, control) = test_app().await;
    let token = test_utils::viewer_token("user-1", "user@test.com");
    let workspace_id = create_workspace(&control, "user-1").await;

    // Attempt SQL injection via SELECT
    let request = workspace_json_request(
        Method::POST,
        "/api/v1/data/query",
        &token,
        &workspace_id,
        json!({
            "query": "SELECT * FROM events_v1; DROP TABLE events_v1; --"
        }),
    );
    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn test_field_values_search_validation() {
    let (app, control) = test_app().await;
    let token = test_utils::viewer_token("user-1", "user@test.com");
    let workspace_id = create_workspace(&control, "user-1").await;

    // URL-encoded invalid characters in search (%27 = ', %20 = space, %3B = ;)
    let request = workspace_request(
        Method::GET,
        "/api/v1/data/sources/events/values/event_name?search=%27%3B%20DROP%20TABLE%20--",
        &token,
        &workspace_id,
    );
    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
}
