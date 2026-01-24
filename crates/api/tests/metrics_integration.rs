//! Integration tests for metrics endpoints
//!
//! Note: These test endpoint routing and auth, not actual ClickHouse queries.
//! For query correctness, see unit tests in tell-analytics.

use axum::{
    Router,
    body::Body,
    http::{Method, Request, StatusCode, header},
};
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

fn auth_request(method: Method, uri: &str, token: &str) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .body(Body::empty())
        .unwrap()
}

#[tokio::test]
async fn test_metrics_requires_auth() {
    let app = test_app().await;

    let endpoints = [
        "/api/v1/metrics/dau?range=7d",
        "/api/v1/metrics/wau?range=7d",
        "/api/v1/metrics/mau?range=7d",
        "/api/v1/metrics/events?range=7d",
        "/api/v1/metrics/logs?range=7d",
    ];

    for endpoint in endpoints {
        let request = Request::builder()
            .method(Method::GET)
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
async fn test_metrics_dau_endpoint_exists() {
    let app = test_app().await;
    let token = test_utils::viewer_token("user-1", "user@test.com");

    let request = auth_request(Method::GET, "/api/v1/metrics/dau?range=7d", &token);
    let response = app.oneshot(request).await.unwrap();

    // May fail if ClickHouse not running, but should not be 404
    assert_ne!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_metrics_wau_endpoint_exists() {
    let app = test_app().await;
    let token = test_utils::viewer_token("user-1", "user@test.com");

    let request = auth_request(Method::GET, "/api/v1/metrics/wau?range=7d", &token);
    let response = app.oneshot(request).await.unwrap();

    assert_ne!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_metrics_mau_endpoint_exists() {
    let app = test_app().await;
    let token = test_utils::viewer_token("user-1", "user@test.com");

    let request = auth_request(Method::GET, "/api/v1/metrics/mau?range=7d", &token);
    let response = app.oneshot(request).await.unwrap();

    assert_ne!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_metrics_events_endpoint_exists() {
    let app = test_app().await;
    let token = test_utils::viewer_token("user-1", "user@test.com");

    let request = auth_request(Method::GET, "/api/v1/metrics/events?range=7d", &token);
    let response = app.oneshot(request).await.unwrap();

    assert_ne!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_metrics_events_top_endpoint_exists() {
    let app = test_app().await;
    let token = test_utils::viewer_token("user-1", "user@test.com");

    let request = auth_request(Method::GET, "/api/v1/metrics/events/top?range=7d", &token);
    let response = app.oneshot(request).await.unwrap();

    assert_ne!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_metrics_logs_endpoint_exists() {
    let app = test_app().await;
    let token = test_utils::viewer_token("user-1", "user@test.com");

    let request = auth_request(Method::GET, "/api/v1/metrics/logs?range=7d", &token);
    let response = app.oneshot(request).await.unwrap();

    assert_ne!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_metrics_stickiness_endpoints_exist() {
    let app = test_app().await;
    let token = test_utils::viewer_token("user-1", "user@test.com");

    let endpoints = [
        "/api/v1/metrics/stickiness/daily?range=30d",
        "/api/v1/metrics/stickiness/weekly?range=30d",
    ];

    for endpoint in endpoints {
        let request = auth_request(Method::GET, endpoint, &token);
        let response = app.clone().oneshot(request).await.unwrap();
        assert_ne!(
            response.status(),
            StatusCode::NOT_FOUND,
            "Expected {} to exist",
            endpoint
        );
    }
}

#[tokio::test]
async fn test_metrics_invalid_range() {
    let app = test_app().await;
    let token = test_utils::viewer_token("user-1", "user@test.com");

    let request = auth_request(Method::GET, "/api/v1/metrics/dau?range=invalid", &token);
    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_metrics_with_comparison() {
    let app = test_app().await;
    let token = test_utils::viewer_token("user-1", "user@test.com");

    let request = auth_request(
        Method::GET,
        "/api/v1/metrics/dau?range=7d&compare=previous",
        &token,
    );
    let response = app.oneshot(request).await.unwrap();

    // Route should exist - may fail validation or need ClickHouse
    // but should not be 404 (not found) or 401 (unauthorized)
    assert_ne!(response.status(), StatusCode::NOT_FOUND);
    assert_ne!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_metrics_with_breakdown() {
    let app = test_app().await;
    let token = test_utils::viewer_token("user-1", "user@test.com");

    let request = auth_request(
        Method::GET,
        "/api/v1/metrics/dau?range=7d&breakdown=device_type",
        &token,
    );
    let response = app.oneshot(request).await.unwrap();

    // Route should exist - may fail validation or need ClickHouse
    // but should not be 404 (not found) or 401 (unauthorized)
    assert_ne!(response.status(), StatusCode::NOT_FOUND);
    assert_ne!(response.status(), StatusCode::UNAUTHORIZED);
}
