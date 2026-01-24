//! License key validation.
//!
//! Validates license keys using ed25519 signatures.
//! The public key is embedded in the binary for offline validation.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::{Signature, Verifier, VerifyingKey};

use crate::error::LicenseError;
use crate::keys::{KEY_PREFIX, public_key_bytes};
use crate::types::{License, LicensePayload};

/// Validate a license key and return the license if valid.
///
/// # Key Format
///
/// ```text
/// tell_lic_1_<base64url(payload)>.<base64url(signature)>
/// ```
///
/// # Validation Steps
///
/// 1. Check prefix
/// 2. Split payload and signature
/// 3. Decode base64
/// 4. Verify ed25519 signature
/// 5. Parse payload JSON
/// 6. Check expiry
///
/// # Returns
///
/// - `Ok(License)` if the key is valid and not expired
/// - `Err(LicenseError::Expired)` if valid but expired
/// - `Err(...)` for other validation failures
pub fn validate(key: &str) -> Result<License, LicenseError> {
    // Check prefix
    let key = key.trim();
    if !key.starts_with(KEY_PREFIX) {
        return Err(LicenseError::InvalidFormat("missing prefix".to_string()));
    }

    // Remove prefix
    let data = &key[KEY_PREFIX.len()..];

    // Split payload and signature
    let parts: Vec<&str> = data.split('.').collect();
    if parts.len() != 2 {
        return Err(LicenseError::InvalidFormat(
            "expected payload.signature".to_string(),
        ));
    }

    let payload_b64 = parts[0];
    let signature_b64 = parts[1];

    // Decode base64
    let payload_bytes = URL_SAFE_NO_PAD
        .decode(payload_b64)
        .map_err(|e| LicenseError::InvalidFormat(format!("payload decode: {}", e)))?;

    let signature_bytes = URL_SAFE_NO_PAD
        .decode(signature_b64)
        .map_err(|e| LicenseError::InvalidFormat(format!("signature decode: {}", e)))?;

    // Verify signature
    verify_signature(&payload_bytes, &signature_bytes)?;

    // Parse payload
    let payload: LicensePayload = serde_json::from_slice(&payload_bytes)
        .map_err(|e| LicenseError::InvalidFormat(format!("payload parse: {}", e)))?;

    // Build license
    let license = License {
        payload,
        raw_key: key.to_string(),
    };

    // Check expiry
    if license.is_expired() {
        return Err(LicenseError::Expired {
            customer: license.customer_name().to_string(),
            expired: license.payload.expires.clone(),
        });
    }

    Ok(license)
}

/// Verify the ed25519 signature.
fn verify_signature(payload: &[u8], signature: &[u8]) -> Result<(), LicenseError> {
    // Get embedded public key
    let public_key = VerifyingKey::from_bytes(public_key_bytes())
        .map_err(|e| LicenseError::CryptoError(format!("invalid public key: {}", e)))?;

    // Parse signature
    let signature: [u8; 64] = signature
        .try_into()
        .map_err(|_| LicenseError::InvalidSignature)?;

    let signature = Signature::from_bytes(&signature);

    // Verify
    public_key
        .verify(payload, &signature)
        .map_err(|_| LicenseError::InvalidSignature)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_missing_prefix() {
        let result = validate("invalid_key");
        assert!(matches!(result, Err(LicenseError::InvalidFormat(_))));
    }

    #[test]
    fn test_validate_missing_separator() {
        let result = validate("tell_lic_1_nodot");
        assert!(matches!(result, Err(LicenseError::InvalidFormat(_))));
    }

    #[test]
    fn test_validate_invalid_base64() {
        let result = validate("tell_lic_1_!!!.!!!");
        assert!(matches!(result, Err(LicenseError::InvalidFormat(_))));
    }

    #[test]
    fn test_validate_invalid_signature() {
        // Valid base64 but wrong signature
        let payload = URL_SAFE_NO_PAD.encode(r#"{"customer_id":"test","customer_name":"Test","tier":"pro","expires":"2099-01-01","issued":"2024-01-01"}"#);
        let fake_sig = URL_SAFE_NO_PAD.encode([0u8; 64]);
        let key = format!("tell_lic_1_{}.{}", payload, fake_sig);

        let result = validate(&key);
        assert!(matches!(result, Err(LicenseError::InvalidSignature)));
    }
}
