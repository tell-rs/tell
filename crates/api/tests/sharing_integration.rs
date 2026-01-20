//! Integration tests for sharing endpoints
//!
//! Tests the full flow: share board, view publicly, unshare

use std::sync::Arc;

use axum::{
    Router,
    body::Body,
    http::{Method, Request, StatusCode, header},
};
use serde_json::{Value, json};
use tower::ServiceExt;

use tell_api::{routes::build_router, state::AppState};
use tell_auth::{LocalJwtProvider, test_utils};
use tell_control::ControlPlane;

/// Create a test app with in-memory control plane
async fn test_app() -> (Router, Arc<ControlPlane>) {
    let control = Arc::new(ControlPlane::new_memory().await.unwrap());

    let auth = Arc::new(LocalJwtProvider::new(test_utils::TEST_SECRET));

    let state = AppState {
        metrics: Arc::new(create_mock_metrics_engine()),
        auth,
        control: Some(control.clone()),
        user_store: None,
        jwt_secret: Some(test_utils::TEST_SECRET.to_vec()),
        jwt_expires_in: std::time::Duration::from_secs(3600),
    };

    (build_router(state), control)
}

fn create_mock_metrics_engine() -> tell_analytics::MetricsEngine {
    use tell_query::ClickHouseBackendConfig;
    let config = ClickHouseBackendConfig::new("http://localhost:8123", "test");
    tell_analytics::MetricsEngine::new(Box::new(tell_query::ClickHouseBackend::new(&config)))
}

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

fn public_request(method: Method, uri: &str) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .body(Body::empty())
        .unwrap()
}

async fn response_json(response: axum::response::Response) -> Value {
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&body).unwrap_or(json!({}))
}

/// Create a workspace and return its ID
async fn create_workspace(app: &Router, token: &str, name: &str, slug: &str) -> String {
    let request = auth_request(
        Method::POST,
        "/api/v1/user/workspaces",
        token,
        Some(json!({
            "name": name,
            "slug": slug
        })),
    );

    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    let json = response_json(response).await;
    json["id"].as_str().unwrap().to_string()
}

/// Create a board and return its ID
async fn create_board(
    app: &Router,
    control: &ControlPlane,
    token: &str,
    workspace_id: &str,
    title: &str,
) -> String {
    // Initialize workspace DB
    let _ = control.workspace_db(workspace_id).await.unwrap();

    let request = auth_request(
        Method::POST,
        "/api/v1/boards",
        token,
        Some(json!({
            "title": title,
            "workspace_id": workspace_id
        })),
    );

    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    let json = response_json(response).await;
    json["id"].as_str().unwrap().to_string()
}

// =============================================================================
// Tests
// =============================================================================

#[tokio::test]
async fn test_share_board() {
    let (app, control) = test_app().await;

    // Create workspace and board
    let token = test_utils::editor_token("user_1", "user@example.com");
    let workspace_id = create_workspace(&app, &token, "Test Workspace", "test-ws").await;
    let board_id = create_board(&app, &control, &token, &workspace_id, "My Dashboard").await;

    // Share the board
    let share_request = auth_request(
        Method::POST,
        &format!(
            "/api/v1/boards/{}/share?workspace_id={}",
            board_id, workspace_id
        ),
        &token,
        None,
    );

    let share_response = app.clone().oneshot(share_request).await.unwrap();
    assert_eq!(share_response.status(), StatusCode::CREATED);

    let json = response_json(share_response).await;
    assert!(json["hash"].is_string());
    assert!(json["url"].as_str().unwrap().starts_with("/s/b/"));
    assert_eq!(json["hash"].as_str().unwrap().len(), 8);
}

#[tokio::test]
async fn test_share_board_idempotent() {
    let (app, control) = test_app().await;

    let token = test_utils::editor_token("user_1", "user@example.com");
    let workspace_id = create_workspace(&app, &token, "Test Workspace", "test-ws").await;
    let board_id = create_board(&app, &control, &token, &workspace_id, "My Dashboard").await;

    // Share the board
    let share_request1 = auth_request(
        Method::POST,
        &format!(
            "/api/v1/boards/{}/share?workspace_id={}",
            board_id, workspace_id
        ),
        &token,
        None,
    );

    let response1 = app.clone().oneshot(share_request1).await.unwrap();
    assert_eq!(response1.status(), StatusCode::CREATED);
    let json1 = response_json(response1).await;
    let hash1 = json1["hash"].as_str().unwrap();

    // Share again - should return existing link (OK, not CREATED)
    let share_request2 = auth_request(
        Method::POST,
        &format!(
            "/api/v1/boards/{}/share?workspace_id={}",
            board_id, workspace_id
        ),
        &token,
        None,
    );

    let response2 = app.clone().oneshot(share_request2).await.unwrap();
    assert_eq!(response2.status(), StatusCode::OK);
    let json2 = response_json(response2).await;
    let hash2 = json2["hash"].as_str().unwrap();

    // Same hash
    assert_eq!(hash1, hash2);
}

#[tokio::test]
async fn test_share_board_requires_ownership() {
    let (app, control) = test_app().await;

    // User 1 creates workspace and board
    let user1_token = test_utils::editor_token("user_1", "user1@example.com");
    let workspace_id = create_workspace(&app, &user1_token, "Test Workspace", "test-ws").await;
    let board_id = create_board(&app, &control, &user1_token, &workspace_id, "User1 Board").await;

    // User 2 (editor but not owner) tries to share
    let user2_token = test_utils::editor_token("user_2", "user2@example.com");

    // Add user2 to workspace
    let membership = tell_control::WorkspaceMembership::new(
        "user_2",
        &workspace_id,
        tell_control::MemberRole::Editor,
    );
    control.workspaces().add_member(&membership).await.unwrap();

    let share_request = auth_request(
        Method::POST,
        &format!(
            "/api/v1/boards/{}/share?workspace_id={}",
            board_id, workspace_id
        ),
        &user2_token,
        None,
    );

    let share_response = app.oneshot(share_request).await.unwrap();
    assert_eq!(share_response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn test_get_share_link() {
    let (app, control) = test_app().await;

    let token = test_utils::editor_token("user_1", "user@example.com");
    let workspace_id = create_workspace(&app, &token, "Test Workspace", "test-ws").await;
    let board_id = create_board(&app, &control, &token, &workspace_id, "My Dashboard").await;

    // Share the board
    let share_request = auth_request(
        Method::POST,
        &format!(
            "/api/v1/boards/{}/share?workspace_id={}",
            board_id, workspace_id
        ),
        &token,
        None,
    );
    let _ = app.clone().oneshot(share_request).await.unwrap();

    // Get the share link
    let get_request = auth_request(
        Method::GET,
        &format!(
            "/api/v1/boards/{}/share?workspace_id={}",
            board_id, workspace_id
        ),
        &token,
        None,
    );

    let get_response = app.oneshot(get_request).await.unwrap();
    assert_eq!(get_response.status(), StatusCode::OK);

    let json = response_json(get_response).await;
    assert!(json["hash"].is_string());
}

#[tokio::test]
async fn test_get_share_link_not_shared() {
    let (app, control) = test_app().await;

    let token = test_utils::editor_token("user_1", "user@example.com");
    let workspace_id = create_workspace(&app, &token, "Test Workspace", "test-ws").await;
    let board_id = create_board(&app, &control, &token, &workspace_id, "My Dashboard").await;

    // Try to get share link for unshared board
    let get_request = auth_request(
        Method::GET,
        &format!(
            "/api/v1/boards/{}/share?workspace_id={}",
            board_id, workspace_id
        ),
        &token,
        None,
    );

    let get_response = app.oneshot(get_request).await.unwrap();
    assert_eq!(get_response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_unshare_board() {
    let (app, control) = test_app().await;

    let token = test_utils::editor_token("user_1", "user@example.com");
    let workspace_id = create_workspace(&app, &token, "Test Workspace", "test-ws").await;
    let board_id = create_board(&app, &control, &token, &workspace_id, "My Dashboard").await;

    // Share the board
    let share_request = auth_request(
        Method::POST,
        &format!(
            "/api/v1/boards/{}/share?workspace_id={}",
            board_id, workspace_id
        ),
        &token,
        None,
    );
    let share_response = app.clone().oneshot(share_request).await.unwrap();
    let share_json = response_json(share_response).await;
    let hash = share_json["hash"].as_str().unwrap();

    // Unshare the board
    let unshare_request = auth_request(
        Method::DELETE,
        &format!(
            "/api/v1/boards/{}/share?workspace_id={}",
            board_id, workspace_id
        ),
        &token,
        None,
    );

    let unshare_response = app.clone().oneshot(unshare_request).await.unwrap();
    assert_eq!(unshare_response.status(), StatusCode::NO_CONTENT);

    // Verify public link no longer works
    let public_request = public_request(Method::GET, &format!("/s/b/{}", hash));
    let public_response = app.oneshot(public_request).await.unwrap();
    assert_eq!(public_response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_view_shared_board_public() {
    let (app, control) = test_app().await;

    let token = test_utils::editor_token("user_1", "user@example.com");
    let workspace_id = create_workspace(&app, &token, "Test Workspace", "test-ws").await;
    let board_id = create_board(&app, &control, &token, &workspace_id, "Public Dashboard").await;

    // Share the board
    let share_request = auth_request(
        Method::POST,
        &format!(
            "/api/v1/boards/{}/share?workspace_id={}",
            board_id, workspace_id
        ),
        &token,
        None,
    );
    let share_response = app.clone().oneshot(share_request).await.unwrap();
    let share_json = response_json(share_response).await;
    let hash = share_json["hash"].as_str().unwrap();

    // View publicly (no auth)
    let public_request = public_request(Method::GET, &format!("/s/b/{}", hash));
    let public_response = app.oneshot(public_request).await.unwrap();
    assert_eq!(public_response.status(), StatusCode::OK);

    let json = response_json(public_response).await;
    assert_eq!(json["title"], "Public Dashboard");
    // Should NOT include sensitive fields
    assert!(json.get("owner_id").is_none());
    assert!(json.get("workspace_id").is_none());
    assert!(json.get("id").is_none());
}

#[tokio::test]
async fn test_view_invalid_share_hash() {
    let (app, _) = test_app().await;

    let public_request = public_request(Method::GET, "/s/b/invalid123");
    let public_response = app.oneshot(public_request).await.unwrap();
    assert_eq!(public_response.status(), StatusCode::NOT_FOUND);
}
