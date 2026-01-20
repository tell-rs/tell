//! Tests for GitHub connector

use crate::error::ConnectorError;
use crate::github::{parse_entity, GitHub, GitHubConfig};
use crate::traits::Connector;
use tell_protocol::BatchType;

// =============================================================================
// parse_entity tests
// =============================================================================

#[test]
fn test_parse_entity_valid() {
    let (owner, repo) = parse_entity("rust-lang/rust").unwrap();
    assert_eq!(owner, "rust-lang");
    assert_eq!(repo, "rust");
}

#[test]
fn test_parse_entity_with_dots() {
    let (owner, repo) = parse_entity("user.name/repo.name").unwrap();
    assert_eq!(owner, "user.name");
    assert_eq!(repo, "repo.name");
}

#[test]
fn test_parse_entity_with_hyphens() {
    let (owner, repo) = parse_entity("my-org/my-repo").unwrap();
    assert_eq!(owner, "my-org");
    assert_eq!(repo, "my-repo");
}

#[test]
fn test_parse_entity_empty_owner() {
    let result = parse_entity("/repo");
    assert!(matches!(result, Err(ConnectorError::InvalidEntity(_))));
}

#[test]
fn test_parse_entity_empty_repo() {
    let result = parse_entity("owner/");
    assert!(matches!(result, Err(ConnectorError::InvalidEntity(_))));
}

#[test]
fn test_parse_entity_no_slash() {
    let result = parse_entity("justrepo");
    assert!(matches!(result, Err(ConnectorError::InvalidEntity(_))));
}

#[test]
fn test_parse_entity_empty_string() {
    let result = parse_entity("");
    assert!(matches!(result, Err(ConnectorError::InvalidEntity(_))));
}

#[test]
fn test_parse_entity_multiple_slashes() {
    // Should only split on first slash
    let (owner, repo) = parse_entity("owner/repo/extra").unwrap();
    assert_eq!(owner, "owner");
    assert_eq!(repo, "repo/extra");
}

// =============================================================================
// GitHub connector construction tests
// =============================================================================

#[test]
fn test_github_new_default() {
    let github = GitHub::new(GitHubConfig::default()).expect("should create connector");
    assert_eq!(github.name(), "github");
}

#[test]
fn test_github_with_token() {
    let github = GitHub::with_token("ghp_test123").expect("should create connector");
    assert_eq!(github.name(), "github");
}

#[test]
fn test_github_config_default() {
    let config = GitHubConfig::default();
    assert!(config.token.is_none());
    assert_eq!(config.api_url, "https://api.github.com");
}

#[test]
fn test_github_config_with_token() {
    let config = GitHubConfig {
        token: Some("test_token".into()),
        ..Default::default()
    };
    assert_eq!(config.token.as_deref(), Some("test_token"));
}

#[test]
fn test_github_config_custom_api_url() {
    let config = GitHubConfig {
        api_url: "https://github.example.com/api/v3".into(),
        ..Default::default()
    };
    assert_eq!(config.api_url, "https://github.example.com/api/v3");
}

// =============================================================================
// Error type tests
// =============================================================================

#[test]
fn test_connector_error_invalid_entity_display() {
    let err = ConnectorError::InvalidEntity("bad format".into());
    assert!(err.to_string().contains("Invalid entity format"));
}

#[test]
fn test_connector_error_rate_limited_display() {
    let err = ConnectorError::RateLimited {
        retry_after_secs: 60,
    };
    assert!(err.to_string().contains("60 seconds"));
}

#[test]
fn test_connector_error_not_found_display() {
    let err = ConnectorError::NotFound("owner/repo".into());
    assert!(err.to_string().contains("not found"));
}

#[test]
fn test_connector_error_auth_failed_display() {
    let err = ConnectorError::AuthFailed("invalid token".into());
    assert!(err.to_string().contains("Authentication failed"));
}

// =============================================================================
// Integration tests (require network, run with --ignored)
// =============================================================================

#[tokio::test]
#[ignore = "requires network access"]
async fn test_github_pull_real_repo() {
    let github = GitHub::new(GitHubConfig::default()).expect("should create connector");
    let result = github.pull("rust-lang/rust").await;

    // Should succeed (public repo, no auth needed)
    let batch = result.expect("should fetch rust-lang/rust");
    assert_eq!(batch.batch_type(), BatchType::Snapshot);
    assert_eq!(batch.message_count(), 1);
    assert!(batch.count() > 0);
}

#[tokio::test]
#[ignore = "requires network access"]
async fn test_github_pull_nonexistent_repo() {
    let github = GitHub::new(GitHubConfig::default()).expect("should create connector");
    let result = github.pull("definitely-not-real-owner-12345/fake-repo-67890").await;

    assert!(matches!(result, Err(ConnectorError::NotFound(_))));
}

#[tokio::test]
#[ignore = "requires network access"]
async fn test_github_pull_invalid_entity() {
    let github = GitHub::new(GitHubConfig::default()).expect("should create connector");
    let result = github.pull("invalid-no-slash").await;

    assert!(matches!(result, Err(ConnectorError::InvalidEntity(_))));
}
