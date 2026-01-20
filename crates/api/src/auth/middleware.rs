//! Authentication middleware for Axum
//!
//! Provides extractors and middleware for authentication and authorization.
//!
//! # Setup
//!
//! Your app state must implement `HasAuthProvider`:
//!
//! ```ignore
//! use std::sync::Arc;
//! use tell_auth::{AuthProvider, LocalJwtProvider};
//! use tell_api::auth::HasAuthProvider;
//!
//! struct AppState {
//!     auth: Arc<dyn AuthProvider>,
//! }
//!
//! impl HasAuthProvider for AppState {
//!     fn auth_provider(&self) -> &dyn AuthProvider {
//!         self.auth.as_ref()
//!     }
//! }
//!
//! // Create state with LocalJwtProvider
//! let state = AppState {
//!     auth: Arc::new(LocalJwtProvider::new(b"your-secret-key-here")),
//! };
//! ```

use std::sync::Arc;

use axum::{
    Json,
    extract::FromRequestParts,
    http::{StatusCode, header::AUTHORIZATION, request::Parts},
    response::{IntoResponse, Response},
};
use serde::Deserialize;

use tell_auth::{AuthError as TellAuthError, AuthProvider, Role, UserInfo};

/// Maximum token size (8KB) - prevents memory exhaustion attacks
const MAX_TOKEN_SIZE: usize = 8 * 1024;

/// Maximum cookie header size (16KB)
const MAX_COOKIE_SIZE: usize = 16 * 1024;

/// Trait for app state that provides an auth provider
///
/// Implement this trait on your app state to enable authentication extractors.
pub trait HasAuthProvider: Send + Sync {
    /// Get the auth provider
    fn auth_provider(&self) -> Arc<dyn AuthProvider>;
}

/// Error returned when authentication fails
#[derive(Debug)]
pub enum AuthError {
    /// No token provided
    MissingToken,
    /// Token format is invalid
    InvalidToken,
    /// Token is too large
    TokenTooLarge,
    /// Token has expired
    TokenExpired,
    /// Insufficient permissions (role required)
    InsufficientPermissions(Role),
}

impl IntoResponse for AuthError {
    fn into_response(self) -> Response {
        let (status, code, message) = match self {
            Self::MissingToken => (
                StatusCode::UNAUTHORIZED,
                "AUTH_REQUIRED",
                "Authentication required",
            ),
            Self::InvalidToken | Self::TokenTooLarge => (
                StatusCode::UNAUTHORIZED,
                "INVALID_TOKEN",
                "Invalid authentication token",
            ),
            Self::TokenExpired => (
                StatusCode::UNAUTHORIZED,
                "TOKEN_EXPIRED",
                "Authentication token has expired",
            ),
            Self::InsufficientPermissions(_) => (
                StatusCode::FORBIDDEN,
                "INSUFFICIENT_PERMISSIONS",
                "Insufficient permissions",
            ),
        };

        let body = serde_json::json!({
            "error": code,
            "message": message,
        });

        (status, Json(body)).into_response()
    }
}

/// Query parameters that may contain a token
#[derive(Debug, Deserialize)]
struct TokenQuery {
    token: Option<String>,
}

/// Extract token from request with size limits
///
/// Checks in order:
/// 1. Authorization header (Bearer or raw)
/// 2. Query parameter (?token=)
/// 3. Cookie (auth_token)
///
/// Returns None if token exceeds MAX_TOKEN_SIZE
fn extract_token(parts: &Parts) -> Option<String> {
    // Try Authorization header
    if let Some(token) = extract_from_auth_header(parts) {
        return if token.len() <= MAX_TOKEN_SIZE {
            Some(token)
        } else {
            None // Too large, reject silently (same as missing)
        };
    }

    // Try query parameter
    if let Some(token) = extract_from_query(parts) {
        return if token.len() <= MAX_TOKEN_SIZE {
            Some(token)
        } else {
            None
        };
    }

    // Try cookie
    if let Some(token) = extract_from_cookie(parts) {
        return if token.len() <= MAX_TOKEN_SIZE {
            Some(token)
        } else {
            None
        };
    }

    None
}

fn extract_from_auth_header(parts: &Parts) -> Option<String> {
    let auth_header = parts.headers.get(AUTHORIZATION)?;

    // Check header size before converting to string
    if auth_header.len() > MAX_TOKEN_SIZE + 7 {
        // "Bearer " = 7 chars
        return None;
    }

    let auth_str = auth_header.to_str().ok()?;
    let token = auth_str.strip_prefix("Bearer ").unwrap_or(auth_str);

    if token.is_empty() {
        None
    } else {
        Some(token.to_string())
    }
}

fn extract_from_query(parts: &Parts) -> Option<String> {
    let query = parts.uri.query()?;

    // Limit query string parsing
    if query.len() > MAX_TOKEN_SIZE * 2 {
        return None;
    }

    let params: TokenQuery = serde_urlencoded::from_str(query).ok()?;
    params
        .token
        .filter(|t| !t.is_empty() && t.len() <= MAX_TOKEN_SIZE)
}

fn extract_from_cookie(parts: &Parts) -> Option<String> {
    let cookie_header = parts.headers.get("cookie")?;

    // Check cookie header size
    if cookie_header.len() > MAX_COOKIE_SIZE {
        return None;
    }

    let cookies = cookie_header.to_str().ok()?;

    for cookie in cookies.split(';') {
        let cookie = cookie.trim();

        // Handle both quoted and unquoted values
        if let Some(value) = cookie.strip_prefix("auth_token=") {
            let value = value.trim();
            if value.is_empty() {
                continue;
            }

            // Handle quoted values: auth_token="value"
            let value = if value.starts_with('"') && value.ends_with('"') && value.len() >= 2 {
                &value[1..value.len() - 1]
            } else {
                value
            };

            // URL decode if needed (simple %XX decoding)
            let decoded = url_decode_simple(value);

            if !decoded.is_empty() && decoded.len() <= MAX_TOKEN_SIZE {
                return Some(decoded);
            }
        }
    }
    None
}

/// Simple URL decoding for cookie values
fn url_decode_simple(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '%' {
            // Try to decode %XX
            let hex: String = chars.by_ref().take(2).collect();
            if hex.len() == 2
                && let Ok(byte) = u8::from_str_radix(&hex, 16)
            {
                result.push(byte as char);
                continue;
            }
            // Invalid escape, keep as-is
            result.push('%');
            result.push_str(&hex);
        } else if c == '+' {
            result.push(' ');
        } else {
            result.push(c);
        }
    }

    result
}

/// Authenticated user extractor
///
/// Extracts and validates the user from the request.
/// Returns `AuthError` if authentication fails.
///
/// # Example
///
/// ```ignore
/// async fn handler(user: AuthUser) -> impl IntoResponse {
///     format!("Hello, {}!", user.email)
/// }
/// ```
#[derive(Debug, Clone)]
pub struct AuthUser(pub UserInfo);

impl std::ops::Deref for AuthUser {
    type Target = UserInfo;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<S> FromRequestParts<S> for AuthUser
where
    S: HasAuthProvider + Send + Sync,
{
    type Rejection = AuthError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        // Extract token
        let token = extract_token(parts).ok_or(AuthError::MissingToken)?;

        // Validate token using provider
        let provider = state.auth_provider();
        let user = provider.validate(&token).await.map_err(|e| match e {
            TellAuthError::TokenExpired => AuthError::TokenExpired,
            TellAuthError::MissingToken => AuthError::MissingToken,
            _ => AuthError::InvalidToken,
        })?;

        Ok(AuthUser(user))
    }
}

/// Optional authenticated user extractor
///
/// Like `AuthUser`, but returns `None` instead of error if not authenticated.
///
/// # Example
///
/// ```ignore
/// async fn handler(user: OptionalAuthUser) -> impl IntoResponse {
///     match user.0 {
///         Some(u) => format!("Hello, {}!", u.email),
///         None => "Hello, anonymous!".to_string(),
///     }
/// }
/// ```
#[derive(Debug, Clone)]
pub struct OptionalAuthUser(pub Option<UserInfo>);

impl<S> FromRequestParts<S> for OptionalAuthUser
where
    S: HasAuthProvider + Send + Sync,
{
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let user = if let Some(token) = extract_token(parts) {
            let provider = state.auth_provider();
            provider.validate(&token).await.ok()
        } else {
            None
        };
        Ok(OptionalAuthUser(user))
    }
}

/// Workspace ID extractor
///
/// Extracts workspace ID from:
/// 1. X-Workspace-ID header
/// 2. Query parameter ?workspace_id=
/// 3. API key scope (from user metadata)
#[derive(Debug, Clone, Copy)]
pub struct WorkspaceId(pub u64);

#[derive(Debug, Deserialize)]
struct WorkspaceQuery {
    workspace_id: Option<u64>,
}

impl<S> FromRequestParts<S> for WorkspaceId
where
    S: Send + Sync,
{
    type Rejection = (StatusCode, Json<serde_json::Value>);

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        // Try X-Workspace-ID header
        if let Some(id) = extract_workspace_from_header(parts) {
            return Ok(WorkspaceId(id));
        }

        // Try query parameter
        if let Some(id) = extract_workspace_from_query(parts) {
            return Ok(WorkspaceId(id));
        }

        // Default workspace for development
        Ok(WorkspaceId(1))
    }
}

fn extract_workspace_from_header(parts: &Parts) -> Option<u64> {
    let header = parts.headers.get("x-workspace-id")?;

    // Limit header size (workspace ID shouldn't be huge)
    if header.len() > 32 {
        return None;
    }

    let s = header.to_str().ok()?;
    s.parse().ok()
}

fn extract_workspace_from_query(parts: &Parts) -> Option<u64> {
    let query = parts.uri.query()?;

    // Don't parse huge query strings
    if query.len() > 1024 {
        return None;
    }

    let params: WorkspaceQuery = serde_urlencoded::from_str(query).ok()?;
    params.workspace_id
}

/// Public paths that don't require authentication
pub fn is_public_path(path: &str) -> bool {
    const PUBLIC_PATHS: [&str; 7] = [
        "/health",
        "/api/v1/auth/login",
        "/api/v1/auth/setup",
        "/api/v1/auth/refresh",
        "/s/", // Shared links
        "/api/v1/auth/setup/status",
        "/api/v1/invites/", // Invite verification and acceptance
    ];

    PUBLIC_PATHS.iter().any(|p| path.starts_with(p))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_public_path() {
        assert!(is_public_path("/health"));
        assert!(is_public_path("/api/v1/auth/login"));
        assert!(is_public_path("/api/v1/auth/setup"));
        assert!(is_public_path("/api/v1/auth/setup/status"));
        assert!(is_public_path("/s/m/abc123"));
        assert!(is_public_path("/api/v1/invites/abc123"));
        assert!(is_public_path("/api/v1/invites/abc123/accept"));
        assert!(!is_public_path("/api/v1/metrics/dau"));
        assert!(!is_public_path("/metrics"));
    }

    #[test]
    fn test_url_decode_simple() {
        assert_eq!(url_decode_simple("hello"), "hello");
        assert_eq!(url_decode_simple("hello%20world"), "hello world");
        assert_eq!(url_decode_simple("hello+world"), "hello world");
        assert_eq!(url_decode_simple("%41%42%43"), "ABC");
        assert_eq!(url_decode_simple("invalid%GG"), "invalid%GG");
        assert_eq!(url_decode_simple("trailing%"), "trailing%");
        assert_eq!(url_decode_simple("short%A"), "short%A");
    }

    #[test]
    fn test_max_token_size_constant() {
        // Ensure constant is reasonable
        assert_eq!(MAX_TOKEN_SIZE, 8192);
        assert_eq!(MAX_COOKIE_SIZE, 16384);
    }
}
