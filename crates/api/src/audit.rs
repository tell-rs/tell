//! Audit logging middleware
//!
//! Logs security-relevant operations for compliance and monitoring.
//!
//! # What gets logged
//!
//! - Authentication attempts (success/failure)
//! - Resource access (who accessed what)
//! - Modifications (create/update/delete)
//! - Permission denials
//!
//! # Output
//!
//! Uses `tracing` with structured fields. Configure subscriber to route to:
//! - File (JSON lines for SIEM ingestion)
//! - stdout (development)
//! - External service (Datadog, Splunk, etc.)
//!
//! # Example log entry
//!
//! ```json
//! {
//!   "timestamp": "2025-01-20T10:30:00Z",
//!   "level": "INFO",
//!   "target": "audit",
//!   "action": "board.create",
//!   "user_id": "user_123",
//!   "workspace_id": "ws_456",
//!   "resource_id": "board_789",
//!   "ip": "192.168.1.1",
//!   "status": "success"
//! }
//! ```

use axum::{body::Body, extract::Request, middleware::Next, response::Response};
use tracing::{Span, warn};

/// Audit event action types
#[derive(Debug, Clone, Copy)]
pub enum AuditAction {
    // Auth
    LoginSuccess,
    LoginFailure,
    TokenRefresh,

    // Resources
    Create,
    Read,
    Update,
    Delete,

    // Sharing
    Share,
    Unshare,
    PublicAccess,

    // Admin
    InviteCreate,
    InviteAccept,
    MemberAdd,
    MemberRemove,
    RoleChange,
}

impl AuditAction {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::LoginSuccess => "auth.login.success",
            Self::LoginFailure => "auth.login.failure",
            Self::TokenRefresh => "auth.token.refresh",
            Self::Create => "resource.create",
            Self::Read => "resource.read",
            Self::Update => "resource.update",
            Self::Delete => "resource.delete",
            Self::Share => "sharing.create",
            Self::Unshare => "sharing.revoke",
            Self::PublicAccess => "sharing.access",
            Self::InviteCreate => "invite.create",
            Self::InviteAccept => "invite.accept",
            Self::MemberAdd => "member.add",
            Self::MemberRemove => "member.remove",
            Self::RoleChange => "member.role_change",
        }
    }
}

/// Log an audit event (call from handlers for business-level events)
#[macro_export]
macro_rules! audit {
    ($action:expr, $($field:tt)*) => {
        tracing::info!(
            target: "audit",
            action = $action.as_str(),
            $($field)*
        )
    };
}

/// Log a failed audit event
#[macro_export]
macro_rules! audit_fail {
    ($action:expr, $reason:expr, $($field:tt)*) => {
        tracing::warn!(
            target: "audit",
            action = $action.as_str(),
            status = "failure",
            reason = $reason,
            $($field)*
        )
    };
}

/// Middleware that adds audit context to all requests
///
/// Adds a tracing span with:
/// - Request method and path
/// - Client IP address (from X-Forwarded-For or X-Real-IP headers)
///
/// Handlers can then use `audit!` macro to log specific events.
pub async fn audit_layer(request: Request<Body>, next: Next) -> Response {
    let method = request.method().clone();
    let path = request.uri().path().to_string();

    // Extract client IP from headers (for proxied requests)
    let client_ip = request
        .headers()
        .get("x-forwarded-for")
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.split(',').next())
        .map(|s| s.trim().to_string())
        .or_else(|| {
            request
                .headers()
                .get("x-real-ip")
                .and_then(|h| h.to_str().ok())
                .map(|s| s.to_string())
        })
        .unwrap_or_else(|| "unknown".to_string());

    // Create audit span
    let span = tracing::info_span!(
        target: "audit",
        "request",
        method = %method,
        path = %path,
        client_ip = %client_ip,
    );

    // Execute request within span
    let _guard = span.enter();
    let response = next.run(request).await;

    // Log response status for non-success
    let status = response.status();
    if status.is_client_error() || status.is_server_error() {
        warn!(
            target: "audit",
            status = %status.as_u16(),
            "request_completed"
        );
    }

    response
}

/// Get current span for adding audit fields
pub fn current_span() -> Span {
    Span::current()
}

// Re-export for convenience
pub use audit;
pub use audit_fail;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_action_strings() {
        assert_eq!(AuditAction::LoginSuccess.as_str(), "auth.login.success");
        assert_eq!(AuditAction::Create.as_str(), "resource.create");
        assert_eq!(AuditAction::Share.as_str(), "sharing.create");
    }
}
