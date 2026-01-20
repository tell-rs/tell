//! Sharing models
//!
//! Shared links for public access to boards and metrics.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// A shared link for public access
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SharedLink {
    /// Unique hash for the link (URL-safe, short)
    pub hash: String,
    /// Type of resource being shared
    pub resource_type: ResourceType,
    /// ID of the shared resource
    pub resource_id: String,
    /// Workspace containing the resource
    pub workspace_id: String,
    /// User who created the share
    pub created_by: String,
    /// Optional expiration time
    pub expires_at: Option<DateTime<Utc>>,
    /// When the link was created
    pub created_at: DateTime<Utc>,
}

impl SharedLink {
    /// Create a new shared link for a board
    pub fn for_board(board_id: &str, workspace_id: &str, created_by: &str) -> Self {
        Self {
            hash: generate_share_hash(),
            resource_type: ResourceType::Board,
            resource_id: board_id.to_string(),
            workspace_id: workspace_id.to_string(),
            created_by: created_by.to_string(),
            expires_at: None,
            created_at: Utc::now(),
        }
    }

    /// Create a new shared link for a metric
    pub fn for_metric(metric_id: &str, workspace_id: &str, created_by: &str) -> Self {
        Self {
            hash: generate_share_hash(),
            resource_type: ResourceType::Metric,
            resource_id: metric_id.to_string(),
            workspace_id: workspace_id.to_string(),
            created_by: created_by.to_string(),
            expires_at: None,
            created_at: Utc::now(),
        }
    }

    /// Set an expiration time
    pub fn with_expiration(mut self, expires_at: DateTime<Utc>) -> Self {
        self.expires_at = Some(expires_at);
        self
    }

    /// Check if the link has expired
    pub fn is_expired(&self) -> bool {
        if let Some(expires) = self.expires_at {
            expires < Utc::now()
        } else {
            false
        }
    }

    /// Get the public URL path for this link
    pub fn public_path(&self) -> String {
        match self.resource_type {
            ResourceType::Board => format!("/s/b/{}", self.hash),
            ResourceType::Metric => format!("/s/m/{}", self.hash),
        }
    }
}

/// Type of shared resource
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ResourceType {
    Board,
    Metric,
}

impl ResourceType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Board => "board",
            Self::Metric => "metric",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "board" => Some(Self::Board),
            "metric" => Some(Self::Metric),
            _ => None,
        }
    }
}

/// Generate a URL-safe share hash (8 characters)
fn generate_share_hash() -> String {
    use rand::Rng;
    const CHARSET: &[u8] = b"abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789";
    let mut rng = rand::rng();
    (0..8)
        .map(|_| {
            let idx = rng.random_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_board_share() {
        let link = SharedLink::for_board("board_123", "ws_456", "user_789");
        assert_eq!(link.resource_type, ResourceType::Board);
        assert_eq!(link.resource_id, "board_123");
        assert_eq!(link.workspace_id, "ws_456");
        assert_eq!(link.created_by, "user_789");
        assert!(!link.is_expired());
        assert_eq!(link.hash.len(), 8);
    }

    #[test]
    fn test_share_with_expiration() {
        let future = Utc::now() + chrono::Duration::hours(24);
        let link = SharedLink::for_board("board_123", "ws_456", "user_789").with_expiration(future);
        assert!(!link.is_expired());

        let past = Utc::now() - chrono::Duration::hours(1);
        let expired_link =
            SharedLink::for_board("board_123", "ws_456", "user_789").with_expiration(past);
        assert!(expired_link.is_expired());
    }

    #[test]
    fn test_public_path() {
        let board_link = SharedLink::for_board("board_123", "ws_456", "user_789");
        assert!(board_link.public_path().starts_with("/s/b/"));

        let metric_link = SharedLink::for_metric("metric_123", "ws_456", "user_789");
        assert!(metric_link.public_path().starts_with("/s/m/"));
    }

    #[test]
    fn test_resource_type_serialization() {
        assert_eq!(ResourceType::Board.as_str(), "board");
        assert_eq!(ResourceType::Metric.as_str(), "metric");
        assert_eq!(ResourceType::from_str("board"), Some(ResourceType::Board));
        assert_eq!(ResourceType::from_str("invalid"), None);
    }
}
