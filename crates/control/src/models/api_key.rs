//! User API key model
//!
//! HTTP API keys for programmatic access to the Tell API.
//! These are different from streaming API keys used for data ingestion.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// A user API key for HTTP API access
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserApiKey {
    /// Unique key ID
    pub id: String,
    /// Owner user ID
    pub user_id: String,
    /// Workspace scope (empty string = user-level key)
    pub workspace_id: String,
    /// Human-readable name
    pub name: String,
    /// Optional description
    pub description: Option<String>,
    /// Argon2 hash of the key (we only store hash, not the actual key)
    #[serde(skip_serializing)]
    pub key_hash: String,
    /// Key prefix for identification (first 8 chars of the key)
    pub key_prefix: String,
    /// Permissions granted to this key (JSON array)
    pub permissions: Vec<String>,
    /// Whether the key is active
    pub active: bool,
    /// When the key was last used
    pub last_used_at: Option<DateTime<Utc>>,
    /// When the key expires (None = never)
    pub expires_at: Option<DateTime<Utc>>,
    /// When the key was created
    pub created_at: DateTime<Utc>,
    /// When the key was last updated
    pub updated_at: DateTime<Utc>,
}

impl UserApiKey {
    /// Create a new API key (returns key and model)
    ///
    /// The returned key should be shown to the user once and never stored.
    pub fn new(
        user_id: &str,
        workspace_id: &str,
        name: &str,
        permissions: Vec<String>,
        expires_at: Option<DateTime<Utc>>,
    ) -> (String, Self) {
        let id = uuid::Uuid::new_v4().to_string();
        let key = generate_api_key();
        let key_prefix = key[..8].to_string();
        let key_hash = hash_api_key(&key);
        let now = Utc::now();

        let api_key = Self {
            id,
            user_id: user_id.to_string(),
            workspace_id: workspace_id.to_string(),
            name: name.to_string(),
            description: None,
            key_hash,
            key_prefix,
            permissions,
            active: true,
            last_used_at: None,
            expires_at,
            created_at: now,
            updated_at: now,
        };

        (key, api_key)
    }

    /// Add a description
    pub fn with_description(mut self, description: &str) -> Self {
        self.description = Some(description.to_string());
        self
    }

    /// Check if the key is expired
    pub fn is_expired(&self) -> bool {
        if let Some(expires) = self.expires_at {
            expires < Utc::now()
        } else {
            false
        }
    }

    /// Check if the key is valid (active and not expired)
    pub fn is_valid(&self) -> bool {
        self.active && !self.is_expired()
    }

    /// Verify a key against the stored hash
    pub fn verify_key(&self, key: &str) -> bool {
        verify_api_key(key, &self.key_hash)
    }
}

/// Generate a new API key
///
/// Format: tell_<32 random hex chars>
/// Total length: 37 characters
fn generate_api_key() -> String {
    use rand::Rng;
    let mut rng = rand::rng();
    let random_bytes: [u8; 16] = rng.random();
    format!("tell_{}", hex::encode(random_bytes))
}

/// Hash an API key using SHA-256 (fast, non-reversible)
///
/// We use SHA-256 instead of Argon2 because:
/// 1. API keys are high-entropy (128 bits), so brute force is impractical
/// 2. Keys are checked on every API request, so speed matters
/// 3. We want O(1) lookup by key hash in the database
fn hash_api_key(key: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(key.as_bytes());
    hex::encode(hasher.finalize())
}

/// Verify an API key against a hash
fn verify_api_key(key: &str, hash: &str) -> bool {
    hash_api_key(key) == hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_api_key() {
        let (key, api_key) = UserApiKey::new(
            "user_123",
            "workspace_456",
            "Test Key",
            vec!["read".to_string()],
            None,
        );

        assert!(key.starts_with("tell_"));
        assert_eq!(key.len(), 37); // "tell_" + 32 hex chars
        assert_eq!(api_key.key_prefix, &key[..8]);
        assert!(api_key.active);
        assert!(!api_key.is_expired());
        assert!(api_key.verify_key(&key));
    }

    #[test]
    fn test_verify_wrong_key() {
        let (key, api_key) = UserApiKey::new("user_123", "workspace_456", "Test Key", vec![], None);

        assert!(api_key.verify_key(&key));
        assert!(!api_key.verify_key("tell_wrongkey12345678901234567890"));
    }

    #[test]
    fn test_expired_key() {
        let (_, mut api_key) = UserApiKey::new("user_123", "", "Test Key", vec![], None);

        // Set expiration to past
        api_key.expires_at = Some(Utc::now() - chrono::Duration::hours(1));
        assert!(api_key.is_expired());
        assert!(!api_key.is_valid());
    }

    #[test]
    fn test_inactive_key() {
        let (_, mut api_key) = UserApiKey::new("user_123", "", "Test Key", vec![], None);

        api_key.active = false;
        assert!(!api_key.is_valid());
    }
}
