//! Embedded public key for license validation.
//!
//! The private key is kept secure on the license server (tell.rs).
//! Only the public key is embedded here for offline validation.

/// License key prefix
pub const KEY_PREFIX: &str = "tell_lic_1_";

/// Ed25519 public key for license validation (32 bytes).
///
/// This key is used to verify license signatures.
/// The corresponding private key is on the license server.
///
/// To generate a new keypair (for initial setup or rotation):
/// ```ignore
/// use ed25519_dalek::SigningKey;
/// use rand::rngs::OsRng;
///
/// let signing_key = SigningKey::generate(&mut OsRng);
/// let verifying_key = signing_key.verifying_key();
///
/// println!("Private: {}", hex::encode(signing_key.to_bytes()));
/// println!("Public: {}", hex::encode(verifying_key.to_bytes()));
/// ```
///
/// IMPORTANT: When rotating keys, update this constant and the server's private key.
/// Generated with: cd ../license-server && cargo run -- keygen
pub const LICENSE_PUBLIC_KEY: [u8; 32] = [
    // Public key: ca9fdb36a304b578a361630c33fb5dd0f2fe73a3faa2cbcf2c3fb91b3162be74
    0xca, 0x9f, 0xdb, 0x36, 0xa3, 0x04, 0xb5, 0x78, 0xa3, 0x61, 0x63, 0x0c, 0x33, 0xfb, 0x5d, 0xd0,
    0xf2, 0xfe, 0x73, 0xa3, 0xfa, 0xa2, 0xcb, 0xcf, 0x2c, 0x3f, 0xb9, 0x1b, 0x31, 0x62, 0xbe, 0x74,
];

/// Get the embedded public key.
pub fn public_key_bytes() -> &'static [u8; 32] {
    &LICENSE_PUBLIC_KEY
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_prefix() {
        assert!(KEY_PREFIX.starts_with("tell_"));
    }

    #[test]
    fn test_public_key_length() {
        assert_eq!(LICENSE_PUBLIC_KEY.len(), 32);
    }
}
