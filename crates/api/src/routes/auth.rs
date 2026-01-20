//! Authentication routes
//!
//! Endpoints for login, setup, and token management.

use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use chrono::{Duration, Utc};
use jsonwebtoken::{encode, EncodingKey, Header};
use serde::{Deserialize, Serialize};

use tell_auth::{Role, TokenClaims, TOKEN_PREFIX};

use crate::ratelimit::RateLimitLayer;
use crate::state::AppState;

/// Auth routes
pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/login", post(login))
        .route("/setup", post(setup))
        .route("/setup/status", get(setup_status))
        // Apply stricter rate limiting on auth endpoints (10 req/min)
        .layer(RateLimitLayer::auth())
}

/// Login request
#[derive(Debug, Deserialize)]
pub struct LoginRequest {
    pub email: String,
    pub password: String,
}

/// Login response
#[derive(Debug, Serialize)]
pub struct LoginResponse {
    pub token: String,
    pub user: UserResponse,
    pub expires_at: i64,
}

/// User info in response
#[derive(Debug, Serialize)]
pub struct UserResponse {
    pub id: String,
    pub email: String,
    pub role: String,
}

/// Login endpoint
///
/// POST /api/v1/auth/login
async fn login(
    State(state): State<AppState>,
    Json(req): Json<LoginRequest>,
) -> Result<Json<LoginResponse>, AuthApiError> {
    // Get user store
    let user_store = state
        .user_store
        .as_ref()
        .ok_or(AuthApiError::LocalAuthNotConfigured)?;

    // Verify credentials
    let user = user_store
        .verify_credentials(&req.email, &req.password)
        .await
        .map_err(|e| AuthApiError::Internal(e.to_string()))?
        .ok_or(AuthApiError::InvalidCredentials)?;

    // Generate JWT
    let jwt_secret = state
        .jwt_secret
        .as_ref()
        .ok_or(AuthApiError::LocalAuthNotConfigured)?;

    let expires_in = state.jwt_expires_in;
    let now = Utc::now();
    let expires_at = now + Duration::from_std(expires_in).unwrap_or(Duration::hours(24));

    let claims = TokenClaims {
        token_id: format!("login-{}", user.id),
        user_id: user.id.clone(),
        email: user.email.clone(),
        role: user.role.as_str().to_string(),
        workspace_id: "1".to_string(), // Default workspace
        permissions: vec![],
        subject: Some(user.id.clone()),
        expires_at: expires_at.timestamp(),
        issued_at: now.timestamp(),
        not_before: None,
        issuer: Some("tell".to_string()),
        jwt_id: None,
    };

    let jwt = encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(jwt_secret),
    )
    .map_err(|e| AuthApiError::Internal(format!("failed to encode JWT: {}", e)))?;

    let token = format!("{}{}", TOKEN_PREFIX, jwt);

    Ok(Json(LoginResponse {
        token,
        user: UserResponse {
            id: user.id,
            email: user.email,
            role: user.role.as_str().to_string(),
        },
        expires_at: expires_at.timestamp(),
    }))
}

/// Setup request (first-run admin creation)
#[derive(Debug, Deserialize)]
pub struct SetupRequest {
    pub email: String,
    pub password: String,
}

/// Setup response
#[derive(Debug, Serialize)]
pub struct SetupResponse {
    pub message: String,
    pub user: UserResponse,
}

/// Setup status response
#[derive(Debug, Serialize)]
pub struct SetupStatusResponse {
    /// Is setup required (no users exist)?
    pub setup_required: bool,
    /// Is local auth configured?
    pub local_auth_enabled: bool,
}

/// First-run setup endpoint
///
/// POST /api/v1/auth/setup
///
/// Creates the initial admin user. Only works when no users exist.
async fn setup(
    State(state): State<AppState>,
    Json(req): Json<SetupRequest>,
) -> Result<Json<SetupResponse>, AuthApiError> {
    // Get user store
    let user_store = state
        .user_store
        .as_ref()
        .ok_or(AuthApiError::LocalAuthNotConfigured)?;

    // Check if setup is allowed (no existing users)
    let is_empty = user_store
        .is_empty()
        .await
        .map_err(|e| AuthApiError::Internal(e.to_string()))?;

    if !is_empty {
        return Err(AuthApiError::SetupAlreadyComplete);
    }

    // Validate email
    if req.email.is_empty() || !req.email.contains('@') {
        return Err(AuthApiError::InvalidEmail);
    }

    // Validate password
    if req.password.len() < 8 {
        return Err(AuthApiError::PasswordTooShort);
    }

    // Create admin user
    let user = user_store
        .create_user(&req.email, &req.password, Role::Admin)
        .await
        .map_err(|e| AuthApiError::Internal(e.to_string()))?;

    Ok(Json(SetupResponse {
        message: "Admin user created successfully".to_string(),
        user: UserResponse {
            id: user.id,
            email: user.email,
            role: user.role.as_str().to_string(),
        },
    }))
}

/// Check setup status
///
/// GET /api/v1/auth/setup/status
///
/// Returns whether first-run setup is required.
async fn setup_status(State(state): State<AppState>) -> Json<SetupStatusResponse> {
    let (setup_required, local_auth_enabled) = match &state.user_store {
        Some(store) => {
            let is_empty = store.is_empty().await.unwrap_or(true);
            (is_empty, true)
        }
        None => (false, false),
    };

    Json(SetupStatusResponse {
        setup_required,
        local_auth_enabled,
    })
}

/// Auth API errors
#[derive(Debug)]
pub enum AuthApiError {
    InvalidCredentials,
    LocalAuthNotConfigured,
    SetupAlreadyComplete,
    InvalidEmail,
    PasswordTooShort,
    Internal(String),
}

impl IntoResponse for AuthApiError {
    fn into_response(self) -> axum::response::Response {
        let (status, code, message) = match self {
            Self::InvalidCredentials => (
                StatusCode::UNAUTHORIZED,
                "INVALID_CREDENTIALS",
                "Invalid email or password".to_string(),
            ),
            Self::LocalAuthNotConfigured => (
                StatusCode::SERVICE_UNAVAILABLE,
                "LOCAL_AUTH_NOT_CONFIGURED",
                "Local authentication is not configured".to_string(),
            ),
            Self::SetupAlreadyComplete => (
                StatusCode::CONFLICT,
                "SETUP_ALREADY_COMPLETE",
                "Initial setup has already been completed".to_string(),
            ),
            Self::InvalidEmail => (
                StatusCode::BAD_REQUEST,
                "INVALID_EMAIL",
                "Invalid email address".to_string(),
            ),
            Self::PasswordTooShort => (
                StatusCode::BAD_REQUEST,
                "PASSWORD_TOO_SHORT",
                "Password must be at least 8 characters".to_string(),
            ),
            Self::Internal(msg) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "INTERNAL_ERROR",
                msg,
            ),
        };

        let body = serde_json::json!({
            "error": code,
            "message": message,
        });

        (status, Json(body)).into_response()
    }
}
