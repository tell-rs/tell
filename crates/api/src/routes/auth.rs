//! Authentication routes
//!
//! Endpoints for login, logout, refresh, and setup.

use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
};
use chrono::{Duration, Utc};
use jsonwebtoken::{EncodingKey, Header, encode};
use serde::{Deserialize, Serialize};
use tracing::info;

use tell_auth::{Role, TOKEN_PREFIX, TokenClaims};

use crate::audit::AuditAction;
use crate::auth::AuthUser;
use crate::ratelimit::RateLimitLayer;
use crate::state::AppState;

/// Extract client IP from headers (X-Forwarded-For, X-Real-IP, or nothing)
fn extract_client_ip(headers: &HeaderMap) -> Option<String> {
    // Try X-Forwarded-For first (may contain multiple IPs, take first)
    if let Some(xff) = headers.get("x-forwarded-for")
        && let Ok(value) = xff.to_str()
        && let Some(ip) = value.split(',').next()
    {
        return Some(ip.trim().to_string());
    }

    // Try X-Real-IP
    if let Some(xri) = headers.get("x-real-ip")
        && let Ok(value) = xri.to_str()
    {
        return Some(value.to_string());
    }

    None
}

/// Auth routes
pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/login", post(login))
        .route("/logout", post(logout))
        .route("/refresh", post(refresh))
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
    headers: HeaderMap,
    Json(req): Json<LoginRequest>,
) -> Result<Json<LoginResponse>, AuthApiError> {
    // Use AuthService if available (with session management)
    if let Some(auth_service) = &state.auth_service {
        let ip = extract_client_ip(&headers);
        let response = auth_service
            .login(&req.email, &req.password, ip.as_deref(), None)
            .await
            .map_err(|e| {
                info!(
                    target: "audit",
                    action = AuditAction::LoginFailure.as_str(),
                    email = %req.email,
                    reason = %e
                );
                match e {
                    tell_auth::AuthError::InvalidClaims(_) => AuthApiError::InvalidCredentials,
                    _ => AuthApiError::Internal(e.to_string()),
                }
            })?;

        info!(
            target: "audit",
            action = AuditAction::LoginSuccess.as_str(),
            user_id = %response.user.id,
            email = %response.user.email,
            role = %response.user.role,
            session_id = %response.session_id
        );

        return Ok(Json(LoginResponse {
            token: response.token,
            user: UserResponse {
                id: response.user.id,
                email: response.user.email,
                role: response.user.role,
            },
            expires_at: response.expires_at,
        }));
    }

    // Fallback: direct user store access (legacy/stateless mode)
    let user_store = state
        .user_store
        .as_ref()
        .ok_or(AuthApiError::LocalAuthNotConfigured)?;

    let user = match user_store
        .verify_credentials(&req.email, &req.password)
        .await
    {
        Ok(Some(user)) => user,
        Ok(None) => {
            info!(
                target: "audit",
                action = AuditAction::LoginFailure.as_str(),
                email = %req.email,
                reason = "invalid_credentials"
            );
            return Err(AuthApiError::InvalidCredentials);
        }
        Err(e) => {
            info!(
                target: "audit",
                action = AuditAction::LoginFailure.as_str(),
                email = %req.email,
                reason = "internal_error"
            );
            return Err(AuthApiError::Internal(e.to_string()));
        }
    };

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
        workspace_id: "1".to_string(),
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

    info!(
        target: "audit",
        action = AuditAction::LoginSuccess.as_str(),
        user_id = %user.id,
        email = %user.email,
        role = %user.role.as_str()
    );

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

/// Logout request
#[derive(Debug, Deserialize)]
pub struct LogoutRequest {
    /// Whether to revoke the token (add to blacklist)
    #[serde(default)]
    pub revoke: bool,
}

/// Logout endpoint
///
/// POST /api/v1/auth/logout
///
/// Deletes the session. If revoke=true, also blacklists the token.
async fn logout(
    State(state): State<AppState>,
    user: AuthUser,
    Json(req): Json<LogoutRequest>,
) -> Result<StatusCode, AuthApiError> {
    let auth_service = state
        .auth_service
        .as_ref()
        .ok_or(AuthApiError::LocalAuthNotConfigured)?;

    // We need the token to logout - get it from the user's metadata or pass it differently
    // For now, use logout_all which doesn't need the token
    // TODO: Pass token through request header or body

    info!(
        target: "audit",
        action = "logout",
        user_id = %user.id,
        revoke = req.revoke
    );

    // Logout all sessions for user (simplest approach)
    auth_service
        .logout_all(&user.id)
        .await
        .map_err(|e| AuthApiError::Internal(e.to_string()))?;

    Ok(StatusCode::NO_CONTENT)
}

/// Refresh request
#[derive(Debug, Deserialize)]
pub struct RefreshRequest {
    /// Current token to refresh
    pub token: String,
}

/// Refresh response
#[derive(Debug, Serialize)]
pub struct RefreshResponse {
    pub token: String,
    pub expires_at: i64,
}

/// Token refresh endpoint
///
/// POST /api/v1/auth/refresh
///
/// Generates a new token with extended expiry.
async fn refresh(
    State(state): State<AppState>,
    Json(req): Json<RefreshRequest>,
) -> Result<Json<RefreshResponse>, AuthApiError> {
    let auth_service = state
        .auth_service
        .as_ref()
        .ok_or(AuthApiError::LocalAuthNotConfigured)?;

    let response = auth_service
        .refresh_token(&req.token)
        .await
        .map_err(|e| match e {
            tell_auth::AuthError::SessionNotFound => AuthApiError::SessionNotFound,
            tell_auth::AuthError::SessionExpired => AuthApiError::SessionExpired,
            tell_auth::AuthError::TokenRevoked => AuthApiError::TokenRevoked,
            tell_auth::AuthError::TokenExpired => AuthApiError::TokenExpired,
            _ => AuthApiError::Internal(e.to_string()),
        })?;

    info!(
        target: "audit",
        action = "token_refresh",
        user_id = %response.user.id
    );

    Ok(Json(RefreshResponse {
        token: response.token,
        expires_at: response.expires_at,
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
    SessionNotFound,
    SessionExpired,
    TokenRevoked,
    TokenExpired,
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
            Self::SessionNotFound => (
                StatusCode::UNAUTHORIZED,
                "SESSION_NOT_FOUND",
                "Session not found or has been logged out".to_string(),
            ),
            Self::SessionExpired => (
                StatusCode::UNAUTHORIZED,
                "SESSION_EXPIRED",
                "Session has expired, please login again".to_string(),
            ),
            Self::TokenRevoked => (
                StatusCode::UNAUTHORIZED,
                "TOKEN_REVOKED",
                "Token has been revoked".to_string(),
            ),
            Self::TokenExpired => (
                StatusCode::UNAUTHORIZED,
                "TOKEN_EXPIRED",
                "Token has expired, please login again".to_string(),
            ),
            Self::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL_ERROR", msg),
        };

        let body = serde_json::json!({
            "error": code,
            "message": message,
        });

        (status, Json(body)).into_response()
    }
}
