//! Integration tests for board endpoints
//!
//! Tests the full flow: auth + workspace + board CRUD

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
    // Create in-memory control plane
    let control = Arc::new(ControlPlane::new_memory().await.unwrap());

    // Create auth provider with test secret
    let auth = Arc::new(LocalJwtProvider::new(test_utils::TEST_SECRET));

    // Create app state
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

/// Create a mock metrics engine
fn create_mock_metrics_engine() -> tell_analytics::MetricsEngine {
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

/// Initialize workspace database (triggers schema creation)
async fn init_workspace_db(control: &ControlPlane, workspace_id: &str) {
    // Get or create workspace database - this initializes the schema including boards table
    let _ = control.workspace_db(workspace_id).await.unwrap();
}

// =============================================================================
// Tests
// =============================================================================

#[tokio::test]
async fn test_create_board_requires_editor() {
    let (app, control) = test_app().await;

    // Create workspace with admin token
    let admin_token = test_utils::admin_token("user_1", "user@example.com");
    let workspace_id = create_workspace(&app, &admin_token, "Test Workspace", "test-ws").await;

    // Init boards table
    init_workspace_db(&control, &workspace_id).await;

    // Try to create board with viewer token (should fail)
    let viewer_token = test_utils::viewer_token("viewer_1", "viewer@example.com");

    // First add viewer to workspace
    let membership = tell_control::WorkspaceMembership::new(
        "viewer_1",
        &workspace_id,
        tell_control::MemberRole::Viewer,
    );
    control.workspaces().add_member(&membership).await.unwrap();

    let request = auth_request(
        Method::POST,
        "/api/v1/boards",
        &viewer_token,
        Some(json!({
            "title": "My Board",
            "workspace_id": workspace_id
        })),
    );

    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn test_create_board() {
    let (app, control) = test_app().await;

    // Create workspace
    let token = test_utils::editor_token("user_1", "user@example.com");
    let workspace_id = create_workspace(&app, &token, "Test Workspace", "test-ws").await;

    // Init boards table
    init_workspace_db(&control, &workspace_id).await;

    // Create board
    let request = auth_request(
        Method::POST,
        "/api/v1/boards",
        &token,
        Some(json!({
            "title": "My Dashboard",
            "description": "A test dashboard",
            "workspace_id": workspace_id
        })),
    );

    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    let json = response_json(response).await;
    assert_eq!(json["title"], "My Dashboard");
    assert_eq!(json["description"], "A test dashboard");
    assert_eq!(json["owner_id"], "user_1");
    assert!(json["id"].is_string());
}

#[tokio::test]
async fn test_list_boards() {
    let (app, control) = test_app().await;

    // Create workspace
    let token = test_utils::editor_token("user_1", "user@example.com");
    let workspace_id = create_workspace(&app, &token, "Test Workspace", "test-ws").await;

    // Init boards table
    init_workspace_db(&control, &workspace_id).await;

    // Create two boards
    for i in 1..=2 {
        let request = auth_request(
            Method::POST,
            "/api/v1/boards",
            &token,
            Some(json!({
                "title": format!("Board {}", i),
                "workspace_id": workspace_id
            })),
        );
        let _ = app.clone().oneshot(request).await.unwrap();
    }

    // List boards
    let request = auth_request(
        Method::GET,
        &format!("/api/v1/boards?workspace_id={}", workspace_id),
        &token,
        None,
    );

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let json = response_json(response).await;
    let boards = json["boards"].as_array().unwrap();
    assert_eq!(boards.len(), 2);
}

#[tokio::test]
async fn test_get_board() {
    let (app, control) = test_app().await;

    // Create workspace and board
    let token = test_utils::editor_token("user_1", "user@example.com");
    let workspace_id = create_workspace(&app, &token, "Test Workspace", "test-ws").await;
    init_workspace_db(&control, &workspace_id).await;

    let create_request = auth_request(
        Method::POST,
        "/api/v1/boards",
        &token,
        Some(json!({
            "title": "Get Test Board",
            "workspace_id": workspace_id
        })),
    );

    let create_response = app.clone().oneshot(create_request).await.unwrap();
    let created = response_json(create_response).await;
    let board_id = created["id"].as_str().unwrap();

    // Get board
    let get_request = auth_request(
        Method::GET,
        &format!("/api/v1/boards/{}?workspace_id={}", board_id, workspace_id),
        &token,
        None,
    );

    let get_response = app.oneshot(get_request).await.unwrap();
    assert_eq!(get_response.status(), StatusCode::OK);

    let json = response_json(get_response).await;
    assert_eq!(json["title"], "Get Test Board");
    assert_eq!(json["id"], board_id);
}

#[tokio::test]
async fn test_update_board_owner() {
    let (app, control) = test_app().await;

    // Create workspace and board
    let token = test_utils::editor_token("user_1", "user@example.com");
    let workspace_id = create_workspace(&app, &token, "Test Workspace", "test-ws").await;
    init_workspace_db(&control, &workspace_id).await;

    let create_request = auth_request(
        Method::POST,
        "/api/v1/boards",
        &token,
        Some(json!({
            "title": "Original Title",
            "workspace_id": workspace_id
        })),
    );

    let create_response = app.clone().oneshot(create_request).await.unwrap();
    let created = response_json(create_response).await;
    let board_id = created["id"].as_str().unwrap();

    // Update board as owner
    let update_request = auth_request(
        Method::PUT,
        &format!("/api/v1/boards/{}?workspace_id={}", board_id, workspace_id),
        &token,
        Some(json!({
            "title": "Updated Title"
        })),
    );

    let update_response = app.oneshot(update_request).await.unwrap();
    assert_eq!(update_response.status(), StatusCode::OK);

    let json = response_json(update_response).await;
    assert_eq!(json["title"], "Updated Title");
}

#[tokio::test]
async fn test_update_board_requires_ownership() {
    let (app, control) = test_app().await;

    // User 1 creates workspace and board
    let user1_token = test_utils::editor_token("user_1", "user1@example.com");
    let workspace_id = create_workspace(&app, &user1_token, "Test Workspace", "test-ws").await;
    init_workspace_db(&control, &workspace_id).await;

    let create_request = auth_request(
        Method::POST,
        "/api/v1/boards",
        &user1_token,
        Some(json!({
            "title": "User1's Board",
            "workspace_id": workspace_id
        })),
    );

    let create_response = app.clone().oneshot(create_request).await.unwrap();
    let created = response_json(create_response).await;
    let board_id = created["id"].as_str().unwrap();

    // User 2 (editor, but not owner) tries to update
    let user2_token = test_utils::editor_token("user_2", "user2@example.com");

    // Add user2 to workspace
    let membership = tell_control::WorkspaceMembership::new(
        "user_2",
        &workspace_id,
        tell_control::MemberRole::Editor,
    );
    control.workspaces().add_member(&membership).await.unwrap();

    let update_request = auth_request(
        Method::PUT,
        &format!("/api/v1/boards/{}?workspace_id={}", board_id, workspace_id),
        &user2_token,
        Some(json!({
            "title": "Hijacked Board"
        })),
    );

    let update_response = app.oneshot(update_request).await.unwrap();
    assert_eq!(update_response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn test_delete_board() {
    let (app, control) = test_app().await;

    // Create workspace and board
    let token = test_utils::editor_token("user_1", "user@example.com");
    let workspace_id = create_workspace(&app, &token, "Test Workspace", "test-ws").await;
    init_workspace_db(&control, &workspace_id).await;

    let create_request = auth_request(
        Method::POST,
        "/api/v1/boards",
        &token,
        Some(json!({
            "title": "To Delete",
            "workspace_id": workspace_id
        })),
    );

    let create_response = app.clone().oneshot(create_request).await.unwrap();
    let created = response_json(create_response).await;
    let board_id = created["id"].as_str().unwrap();

    // Delete board
    let delete_request = auth_request(
        Method::DELETE,
        &format!("/api/v1/boards/{}?workspace_id={}", board_id, workspace_id),
        &token,
        None,
    );

    let delete_response = app.clone().oneshot(delete_request).await.unwrap();
    assert_eq!(delete_response.status(), StatusCode::NO_CONTENT);

    // Verify it's gone
    let get_request = auth_request(
        Method::GET,
        &format!("/api/v1/boards/{}?workspace_id={}", board_id, workspace_id),
        &token,
        None,
    );

    let get_response = app.oneshot(get_request).await.unwrap();
    assert_eq!(get_response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_pin_board() {
    let (app, control) = test_app().await;

    // Create workspace and board
    let token = test_utils::editor_token("user_1", "user@example.com");
    let workspace_id = create_workspace(&app, &token, "Test Workspace", "test-ws").await;
    init_workspace_db(&control, &workspace_id).await;

    let create_request = auth_request(
        Method::POST,
        "/api/v1/boards",
        &token,
        Some(json!({
            "title": "Pinnable Board",
            "workspace_id": workspace_id
        })),
    );

    let create_response = app.clone().oneshot(create_request).await.unwrap();
    let created = response_json(create_response).await;
    let board_id = created["id"].as_str().unwrap();
    assert!(!created["is_pinned"].as_bool().unwrap());

    // Pin the board
    let pin_request = auth_request(
        Method::PUT,
        &format!(
            "/api/v1/boards/{}/pin?workspace_id={}",
            board_id, workspace_id
        ),
        &token,
        Some(json!({ "pinned": true })),
    );

    let pin_response = app.clone().oneshot(pin_request).await.unwrap();
    assert_eq!(pin_response.status(), StatusCode::OK);

    let json = response_json(pin_response).await;
    assert!(json["is_pinned"].as_bool().unwrap());

    // List pinned boards
    let list_request = auth_request(
        Method::GET,
        &format!(
            "/api/v1/boards?workspace_id={}&pinned_only=true",
            workspace_id
        ),
        &token,
        None,
    );

    let list_response = app.clone().oneshot(list_request).await.unwrap();
    let list_json = response_json(list_response).await;
    let boards = list_json["boards"].as_array().unwrap();
    assert_eq!(boards.len(), 1);
    assert!(boards[0]["is_pinned"].as_bool().unwrap());

    // Unpin
    let unpin_request = auth_request(
        Method::PUT,
        &format!(
            "/api/v1/boards/{}/pin?workspace_id={}",
            board_id, workspace_id
        ),
        &token,
        Some(json!({ "pinned": false })),
    );

    let unpin_response = app.oneshot(unpin_request).await.unwrap();
    assert_eq!(unpin_response.status(), StatusCode::OK);

    let unpin_json = response_json(unpin_response).await;
    assert!(!unpin_json["is_pinned"].as_bool().unwrap());
}

#[tokio::test]
async fn test_workspace_isolation_boards() {
    let (app, control) = test_app().await;

    // User 1 creates workspace and board
    let user1_token = test_utils::editor_token("user_1", "user1@example.com");
    let workspace1_id = create_workspace(&app, &user1_token, "Workspace 1", "ws-1").await;
    init_workspace_db(&control, &workspace1_id).await;

    let create_request = auth_request(
        Method::POST,
        "/api/v1/boards",
        &user1_token,
        Some(json!({
            "title": "User1 Board",
            "workspace_id": workspace1_id
        })),
    );
    let _ = app.clone().oneshot(create_request).await.unwrap();

    // User 2 creates their own workspace
    let user2_token = test_utils::editor_token("user_2", "user2@example.com");
    let workspace2_id = create_workspace(&app, &user2_token, "Workspace 2", "ws-2").await;
    init_workspace_db(&control, &workspace2_id).await;

    // User 2 lists boards in workspace1 (should fail - not a member)
    let list_request = auth_request(
        Method::GET,
        &format!("/api/v1/boards?workspace_id={}", workspace1_id),
        &user2_token,
        None,
    );

    let list_response = app.oneshot(list_request).await.unwrap();
    assert_eq!(list_response.status(), StatusCode::FORBIDDEN);
}
