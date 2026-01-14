//! Pseudonymization hasher
//!
//! HMAC-SHA256 based deterministic hashing for PII pseudonymization.

use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// Pseudonymization hasher using HMAC-SHA256
#[derive(Clone)]
pub struct PseudonymHasher {
    key: Vec<u8>,
}

impl PseudonymHasher {
    /// Create a new hasher with the given key
    pub fn new(key: impl AsRef<[u8]>) -> Self {
        Self {
            key: key.as_ref().to_vec(),
        }
    }

    /// Hash a value and return a prefixed pseudonym
    ///
    /// Format: `<prefix><base62(hmac[:16])>`
    /// Example: `usr_7Hx9KmPqR2sT4v`
    pub fn hash(&self, value: &str, prefix: &str) -> String {
        let mut mac = HmacSha256::new_from_slice(&self.key)
            .expect("HMAC can take key of any size");
        mac.update(value.as_bytes());
        let result = mac.finalize();
        let hash_bytes = result.into_bytes();

        // Take first 12 bytes (96 bits) for a good balance of uniqueness and length
        // Convert to u128 for base62 encoding (pad with zeros)
        let mut num_bytes = [0u8; 16];
        num_bytes[..12].copy_from_slice(&hash_bytes[..12]);
        let num = u128::from_le_bytes(num_bytes);

        // Encode as base62
        let encoded = base62::encode(num);

        format!("{}{}", prefix, encoded)
    }

    /// Hash a value with a default prefix
    pub fn hash_default(&self, value: &str) -> String {
        self.hash(value, "pii_")
    }
}

impl std::fmt::Debug for PseudonymHasher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PseudonymHasher")
            .field("key", &"[REDACTED]")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deterministic_hash() {
        let hasher = PseudonymHasher::new("secret-key");

        let hash1 = hasher.hash("user@example.com", "usr_");
        let hash2 = hasher.hash("user@example.com", "usr_");

        // Same input produces same output
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_different_inputs() {
        let hasher = PseudonymHasher::new("secret-key");

        let hash1 = hasher.hash("user1@example.com", "usr_");
        let hash2 = hasher.hash("user2@example.com", "usr_");

        // Different inputs produce different outputs
        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_different_keys() {
        let hasher1 = PseudonymHasher::new("key1");
        let hasher2 = PseudonymHasher::new("key2");

        let hash1 = hasher1.hash("user@example.com", "usr_");
        let hash2 = hasher2.hash("user@example.com", "usr_");

        // Same input with different keys produces different outputs
        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_prefix() {
        let hasher = PseudonymHasher::new("secret-key");

        let email_hash = hasher.hash("user@example.com", "usr_");
        let phone_hash = hasher.hash("+1234567890", "phn_");

        assert!(email_hash.starts_with("usr_"));
        assert!(phone_hash.starts_with("phn_"));
    }

    #[test]
    fn test_output_length() {
        let hasher = PseudonymHasher::new("secret-key");

        let hash = hasher.hash("user@example.com", "usr_");

        // Prefix (4) + base62 of 12 bytes (~16-17 chars)
        assert!(hash.len() > 10);
        assert!(hash.len() < 30);
    }

    #[test]
    fn test_default_prefix() {
        let hasher = PseudonymHasher::new("secret-key");

        let hash = hasher.hash_default("some-value");

        assert!(hash.starts_with("pii_"));
    }

    #[test]
    fn test_debug_redacts_key() {
        let hasher = PseudonymHasher::new("super-secret");
        let debug = format!("{:?}", hasher);

        assert!(!debug.contains("super-secret"));
        assert!(debug.contains("[REDACTED]"));
    }
}
