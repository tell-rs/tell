//! Integration tests for workspace endpoints
//!
//! Tests the full flow: auth + turso + endpoints

use std::sync::Arc;

use axum::{
    Router,
    body::Body,
    http::{Method, Request, StatusCode, header},
};
use serde_json::{Value, json};
use tower::ServiceExt;

use tell_api::{routes::build_router, state::AppState};
use tell_auth::{LocalJwtProvider, Role, test_utils};
use tell_control::ControlPlane;

/// Create a test app with in-memory control plane
async fn test_app() -> Router {
    // Create in-memory control plane
    let control = ControlPlane::new_memory().await.unwrap();

    // Create auth provider with test secret
    let auth = Arc::new(LocalJwtProvider::new(test_utils::TEST_SECRET));

    // Create app state
    let state = AppState {
        metrics: Arc::new(create_mock_metrics_engine()),
        auth,
        control: Some(Arc::new(control)),
        user_store: None,
        jwt_secret: Some(test_utils::TEST_SECRET.to_vec()),
        jwt_expires_in: std::time::Duration::from_secs(3600),
    };

    build_router(state)
}

/// Create a mock metrics engine (we don't need real analytics for these tests)
fn create_mock_metrics_engine() -> tell_analytics::MetricsEngine {
    // Create a mock backend using ClickHouse config
    // This won't actually connect since we're not making analytics calls in workspace tests
    use tell_query::ClickHouseBackendConfig;

    let config = ClickHouseBackendConfig::new("http://localhost:8123", "test");

    tell_analytics::MetricsEngine::new(Box::new(tell_query::ClickHouseBackend::new(&config)))
}

/// Helper to make authenticated requests
fn auth_request(method: Method, uri: &str, token: &str, body: Option<Value>) -> Request<Body> {
    let builder = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .header(header::CONTENT_TYPE, "application/json");

    if let Some(json_body) = body {
        builder.body(Body::from(json_body.to_string())).unwrap()
    } else {
        builder.body(Body::empty()).unwrap()
    }
}

/// Helper to extract JSON from response
async fn response_json(response: axum::response::Response) -> Value {
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&body).unwrap_or(json!({}))
}

// =============================================================================
// Tests
// =============================================================================

#[tokio::test]
async fn test_list_workspaces_requires_auth() {
    let app = test_app().await;

    // Request without auth token
    let request = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/user/workspaces")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_list_workspaces_empty() {
    let app = test_app().await;

    // Create a token for a test user
    let token = test_utils::editor_token("user_1", "user@example.com");

    let request = auth_request(Method::GET, "/api/v1/user/workspaces", &token, None);

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let json = response_json(response).await;
    assert!(json["workspaces"].is_array());
    assert_eq!(json["workspaces"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn test_create_workspace() {
    let app = test_app().await;

    let token = test_utils::editor_token("user_1", "user@example.com");

    let request = auth_request(
        Method::POST,
        "/api/v1/user/workspaces",
        &token,
        Some(json!({
            "name": "My Workspace",
            "slug": "my-workspace"
        })),
    );

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::CREATED);

    let json = response_json(response).await;
    assert_eq!(json["name"], "My Workspace");
    assert_eq!(json["slug"], "my-workspace");
    assert!(json["id"].is_string());
    // Note: clickhouse_database intentionally not exposed in API response (internal detail)
}

#[tokio::test]
async fn test_create_workspace_invalid_slug() {
    let app = test_app().await;

    let token = test_utils::editor_token("user_1", "user@example.com");

    // Slug with invalid characters
    let request = auth_request(
        Method::POST,
        "/api/v1/user/workspaces",
        &token,
        Some(json!({
            "name": "My Workspace",
            "slug": "my workspace!" // spaces and ! not allowed
        })),
    );

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn test_create_and_list_workspace() {
    let app = test_app().await;

    let token = test_utils::editor_token("user_1", "user@example.com");

    // Create workspace
    let create_request = auth_request(
        Method::POST,
        "/api/v1/user/workspaces",
        &token,
        Some(json!({
            "name": "Test Workspace",
            "slug": "test-workspace"
        })),
    );

    let create_response = app.clone().oneshot(create_request).await.unwrap();
    assert_eq!(create_response.status(), StatusCode::CREATED);

    // List workspaces - user should see the one they created
    let list_request = auth_request(Method::GET, "/api/v1/user/workspaces", &token, None);

    let list_response = app.oneshot(list_request).await.unwrap();
    assert_eq!(list_response.status(), StatusCode::OK);

    let json = response_json(list_response).await;
    let workspaces = json["workspaces"].as_array().unwrap();
    assert_eq!(workspaces.len(), 1);
    assert_eq!(workspaces[0]["name"], "Test Workspace");
}

#[tokio::test]
async fn test_admin_list_requires_platform_permission() {
    let app = test_app().await;

    // Regular editor token (not platform admin)
    let editor_token = test_utils::editor_token("user_1", "user@example.com");

    let request = auth_request(Method::GET, "/api/v1/admin/workspaces", &editor_token, None);

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn test_admin_list_with_platform_permission() {
    let app = test_app().await;

    // Platform admin token
    let platform_token = test_utils::create_test_token(
        "platform_user",
        "platform@example.com",
        Role::Platform,
        None,
    );

    let request = auth_request(
        Method::GET,
        "/api/v1/admin/workspaces",
        &platform_token,
        None,
    );

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let json = response_json(response).await;
    assert!(json["workspaces"].is_array());
}

#[tokio::test]
async fn test_get_workspace_by_id() {
    let app = test_app().await;

    let token = test_utils::admin_token("user_1", "user@example.com");

    // First create a workspace
    let create_request = auth_request(
        Method::POST,
        "/api/v1/user/workspaces",
        &token,
        Some(json!({
            "name": "Get Test",
            "slug": "get-test"
        })),
    );

    let create_response = app.clone().oneshot(create_request).await.unwrap();
    assert_eq!(create_response.status(), StatusCode::CREATED);

    let created = response_json(create_response).await;
    let workspace_id = created["id"].as_str().unwrap();

    // Now get it by ID
    let get_request = auth_request(
        Method::GET,
        &format!("/api/v1/admin/workspaces/{}", workspace_id),
        &token,
        None,
    );

    let get_response = app.oneshot(get_request).await.unwrap();
    assert_eq!(get_response.status(), StatusCode::OK);

    let json = response_json(get_response).await;
    assert_eq!(json["id"], workspace_id);
    assert_eq!(json["name"], "Get Test");
}

#[tokio::test]
async fn test_update_workspace() {
    let app = test_app().await;

    let token = test_utils::admin_token("user_1", "user@example.com");

    // Create workspace
    let create_request = auth_request(
        Method::POST,
        "/api/v1/user/workspaces",
        &token,
        Some(json!({
            "name": "Original Name",
            "slug": "update-test"
        })),
    );

    let create_response = app.clone().oneshot(create_request).await.unwrap();
    let created = response_json(create_response).await;
    let workspace_id = created["id"].as_str().unwrap();

    // Update it
    let update_request = auth_request(
        Method::PUT,
        &format!("/api/v1/admin/workspaces/{}", workspace_id),
        &token,
        Some(json!({
            "name": "Updated Name"
        })),
    );

    let update_response = app.clone().oneshot(update_request).await.unwrap();
    assert_eq!(update_response.status(), StatusCode::OK);

    let json = response_json(update_response).await;
    assert_eq!(json["name"], "Updated Name");
}

#[tokio::test]
async fn test_delete_workspace_requires_platform() {
    let app = test_app().await;

    // Admin token (not platform)
    let admin_token = test_utils::admin_token("user_1", "user@example.com");

    // Create workspace
    let create_request = auth_request(
        Method::POST,
        "/api/v1/user/workspaces",
        &admin_token,
        Some(json!({
            "name": "Delete Test",
            "slug": "delete-test"
        })),
    );

    let create_response = app.clone().oneshot(create_request).await.unwrap();
    let created = response_json(create_response).await;
    let workspace_id = created["id"].as_str().unwrap();

    // Try to delete with admin (should fail)
    let delete_request = auth_request(
        Method::DELETE,
        &format!("/api/v1/admin/workspaces/{}", workspace_id),
        &admin_token,
        None,
    );

    let delete_response = app.clone().oneshot(delete_request).await.unwrap();
    assert_eq!(delete_response.status(), StatusCode::FORBIDDEN);

    // Delete with platform token (should succeed)
    let platform_token = test_utils::create_test_token(
        "platform_user",
        "platform@example.com",
        Role::Platform,
        None,
    );

    let delete_request = auth_request(
        Method::DELETE,
        &format!("/api/v1/admin/workspaces/{}", workspace_id),
        &platform_token,
        None,
    );

    let delete_response = app.oneshot(delete_request).await.unwrap();
    assert_eq!(delete_response.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn test_workspace_isolation() {
    let app = test_app().await;

    // User 1 creates a workspace
    let user1_token = test_utils::editor_token("user_1", "user1@example.com");
    let create_request = auth_request(
        Method::POST,
        "/api/v1/user/workspaces",
        &user1_token,
        Some(json!({
            "name": "User1 Workspace",
            "slug": "user1-ws"
        })),
    );
    let _ = app.clone().oneshot(create_request).await.unwrap();

    // User 2 lists workspaces - should not see user1's workspace
    let user2_token = test_utils::editor_token("user_2", "user2@example.com");
    let list_request = auth_request(Method::GET, "/api/v1/user/workspaces", &user2_token, None);

    let list_response = app.oneshot(list_request).await.unwrap();
    assert_eq!(list_response.status(), StatusCode::OK);

    let json = response_json(list_response).await;
    let workspaces = json["workspaces"].as_array().unwrap();
    assert_eq!(workspaces.len(), 0); // User 2 sees no workspaces
}

#[tokio::test]
async fn test_duplicate_slug_rejected() {
    let app = test_app().await;

    let token = test_utils::editor_token("user_1", "user@example.com");

    // Create first workspace
    let create_request1 = auth_request(
        Method::POST,
        "/api/v1/user/workspaces",
        &token,
        Some(json!({
            "name": "First",
            "slug": "duplicate-slug"
        })),
    );
    let response1 = app.clone().oneshot(create_request1).await.unwrap();
    assert_eq!(response1.status(), StatusCode::CREATED);

    // Try to create another with same slug
    let create_request2 = auth_request(
        Method::POST,
        "/api/v1/user/workspaces",
        &token,
        Some(json!({
            "name": "Second",
            "slug": "duplicate-slug"
        })),
    );
    let response2 = app.oneshot(create_request2).await.unwrap();
    assert_eq!(response2.status(), StatusCode::CONFLICT);
}
