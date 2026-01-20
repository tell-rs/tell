//! Test utilities for generating JWT tokens
//!
//! These helpers create valid signed JWTs for testing authentication flows.
//! Use these instead of mocking - they test the real validation code path.

use chrono::{Duration, Utc};
use jsonwebtoken::{EncodingKey, Header, encode};

use crate::Role;
use crate::claims::{TOKEN_PREFIX, TokenClaims};

/// Test secret for JWT signing (32 bytes for HS256)
pub const TEST_SECRET: &[u8] = b"test-secret-key-32-bytes-long!!!";

/// Create a test token with default settings
///
/// # Example
///
/// ```
/// use tell_auth::test_utils::{create_test_token, TEST_SECRET};
/// use tell_auth::{LocalJwtProvider, Role};
///
/// let token = create_test_token("user-1", "test@example.com", Role::Editor, None);
/// let provider = LocalJwtProvider::new(TEST_SECRET);
/// // Token is valid and can be verified
/// ```
pub fn create_test_token(
    user_id: &str,
    email: &str,
    role: Role,
    workspace_id: Option<&str>,
) -> String {
    create_test_token_with_options(
        user_id,
        email,
        role,
        workspace_id,
        None,
        TEST_SECRET,
        Duration::hours(1),
    )
}

/// Create a test token with full control over all options
pub fn create_test_token_with_options(
    user_id: &str,
    email: &str,
    role: Role,
    workspace_id: Option<&str>,
    issuer: Option<&str>,
    secret: &[u8],
    expires_in: Duration,
) -> String {
    let now = Utc::now();

    let claims = TokenClaims {
        token_id: format!("test-token-{}", user_id),
        user_id: user_id.to_string(),
        email: email.to_string(),
        role: role.as_str().to_string(),
        workspace_id: workspace_id.unwrap_or("1").to_string(),
        permissions: vec![],
        subject: Some(user_id.to_string()),
        expires_at: (now + expires_in).timestamp(),
        issued_at: now.timestamp(),
        not_before: None,
        issuer: issuer.map(String::from),
        jwt_id: None,
    };

    let jwt = encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(secret),
    )
    .expect("failed to encode test JWT");

    format!("{}{}", TOKEN_PREFIX, jwt)
}

/// Create a viewer token
pub fn viewer_token(user_id: &str, email: &str) -> String {
    create_test_token(user_id, email, Role::Viewer, None)
}

/// Create an editor token
pub fn editor_token(user_id: &str, email: &str) -> String {
    create_test_token(user_id, email, Role::Editor, None)
}

/// Create an admin token
pub fn admin_token(user_id: &str, email: &str) -> String {
    create_test_token(user_id, email, Role::Admin, None)
}

/// Create a platform token
pub fn platform_token(user_id: &str, email: &str) -> String {
    create_test_token(user_id, email, Role::Platform, None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LocalJwtProvider;
    use crate::provider::AuthProvider;

    #[tokio::test]
    async fn test_create_test_token() {
        let provider = LocalJwtProvider::new(TEST_SECRET);
        let token = create_test_token("user-1", "test@example.com", Role::Editor, None);

        let user = provider.validate(&token).await.unwrap();
        assert_eq!(user.id, "user-1");
        assert_eq!(user.email, "test@example.com");
        assert_eq!(user.parsed_role(), Role::Editor);
    }

    #[tokio::test]
    async fn test_convenience_functions() {
        let provider = LocalJwtProvider::new(TEST_SECRET);

        let viewer = provider
            .validate(&viewer_token("v1", "viewer@test.com"))
            .await
            .unwrap();
        assert_eq!(viewer.parsed_role(), Role::Viewer);
        assert!(!viewer.can_create());

        let editor = provider
            .validate(&editor_token("e1", "editor@test.com"))
            .await
            .unwrap();
        assert_eq!(editor.parsed_role(), Role::Editor);
        assert!(editor.can_create());
        assert!(!editor.is_admin());

        let admin = provider
            .validate(&admin_token("a1", "admin@test.com"))
            .await
            .unwrap();
        assert_eq!(admin.parsed_role(), Role::Admin);
        assert!(admin.is_admin());
        assert!(!admin.is_platform());

        let platform = provider
            .validate(&platform_token("p1", "platform@test.com"))
            .await
            .unwrap();
        assert_eq!(platform.parsed_role(), Role::Platform);
        assert!(platform.is_platform());
    }

    #[tokio::test]
    async fn test_workspace_id() {
        let provider = LocalJwtProvider::new(TEST_SECRET);
        let token = create_test_token(
            "user-1",
            "test@example.com",
            Role::Editor,
            Some("workspace-42"),
        );

        let user = provider.validate(&token).await.unwrap();
        assert_eq!(
            user.metadata.get("workspace_id"),
            Some(&"workspace-42".to_string())
        );
    }

    #[test]
    fn test_token_format() {
        let token = create_test_token("user-1", "test@example.com", Role::Viewer, None);
        assert!(token.starts_with("tell_"));
        assert!(token.len() > 100); // JWTs are typically longer
    }
}
