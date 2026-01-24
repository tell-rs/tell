//! Tell license validation.
//!
//! This crate handles license key validation for Tell Pro and Enterprise tiers.
//!
//! # License Model
//!
//! - **Free**: No license required, company revenue < $1M/year
//! - **Pro**: $299/mo, company revenue $1M-$10M/year
//! - **Enterprise**: $2000+/mo, company revenue > $10M/year
//!
//! # Key Format
//!
//! License keys are ed25519-signed tokens:
//!
//! ```text
//! tell_lic_1_<base64url(payload)>.<base64url(signature)>
//! ```
//!
//! The payload contains:
//! - Customer ID and name
//! - Tier (pro/enterprise)
//! - Issue and expiry dates
//!
//! # Validation
//!
//! Keys are validated offline using an embedded public key.
//! The private key is kept secure on the license server (tell.rs).
//!
//! # Usage
//!
//! ```rust,no_run
//! use tell_license::{load_license, LicenseState};
//!
//! // Load and validate license from default locations
//! let state = load_license(None);
//!
//! match &state {
//!     LicenseState::Free => println!("Running in free mode"),
//!     LicenseState::Licensed(lic) => println!("Licensed to: {}", lic.customer_name()),
//!     LicenseState::Expired(lic) => println!("License expired for: {}", lic.customer_name()),
//!     LicenseState::Invalid(err) => println!("Invalid license: {}", err),
//! }
//!
//! // Check if server can run
//! if !state.can_serve() {
//!     eprintln!("License expired. Please renew at https://tell.rs/renew");
//!     std::process::exit(1);
//! }
//! ```

mod error;
mod keys;
mod storage;
mod types;
mod validator;

pub use error::LicenseError;
pub use storage::{
    LICENSE_ENV_VAR, default_license_path, load_license_key, remove_license_key, save_license_key,
};
pub use types::{License, LicensePayload, LicenseState, Tier};
pub use validator::validate;

use std::path::Path;

/// Load and validate license from all sources.
///
/// Checks in order:
/// 1. `TELL_LICENSE_KEY` environment variable
/// 2. Explicit path (if provided)
/// 3. `~/.tell/license` file
///
/// Returns the license state (Free, Licensed, Expired, or Invalid).
pub fn load_license(explicit_path: Option<&Path>) -> LicenseState {
    // Try to load key from storage
    let Some(key) = load_license_key(explicit_path) else {
        return LicenseState::Free;
    };

    // Validate the key
    match validate(&key) {
        Ok(license) => LicenseState::Licensed(license),
        Err(LicenseError::Expired { customer, expired }) => {
            // Re-parse to get full license for display
            // This is a bit redundant but keeps the error handling clean
            LicenseState::Expired(License {
                payload: LicensePayload {
                    customer_id: customer.clone(),
                    customer_name: customer,
                    tier: Tier::Pro, // Assume Pro for expired
                    expires: expired,
                    issued: String::new(),
                },
                raw_key: key,
            })
        }
        Err(e) => LicenseState::Invalid(e.to_string()),
    }
}

/// Activate a license key.
///
/// Validates the key and saves it to the default location if valid.
pub fn activate_license(key: &str) -> Result<License, LicenseError> {
    // Validate first
    let license = validate(key)?;

    // Save to disk
    save_license_key(key)?;

    Ok(license)
}

/// Deactivate the current license.
///
/// Removes the saved license key.
pub fn deactivate_license() -> Result<(), LicenseError> {
    remove_license_key()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_license_no_key() {
        // Clear env var
        // SAFETY: Test runs single-threaded
        unsafe { std::env::remove_var(LICENSE_ENV_VAR) };

        // With no key, should be Free
        let state = load_license(None);
        assert!(matches!(state, LicenseState::Free));
        assert!(state.can_serve());
        assert_eq!(state.tier(), Tier::Free);
    }

    #[test]
    fn test_load_license_invalid_key() {
        // SAFETY: Test runs single-threaded
        unsafe { std::env::set_var(LICENSE_ENV_VAR, "invalid_key") };

        let state = load_license(None);
        assert!(matches!(state, LicenseState::Invalid(_)));
        assert!(state.can_serve()); // Invalid = treat as free

        // SAFETY: Test runs single-threaded
        unsafe { std::env::remove_var(LICENSE_ENV_VAR) };
    }

    #[test]
    fn test_validate_real_key() {
        // This key was generated with the matching private key
        let key = "tell_lic_1_eyJjdXN0b21lcl9pZCI6ImFjbWUtY29ycCIsImN1c3RvbWVyX25hbWUiOiJBY21lIENvcnBvcmF0aW9uIiwidGllciI6InBybyIsImV4cGlyZXMiOiIyMDI3LTAxLTE5IiwiaXNzdWVkIjoiMjAyNi0wMS0yNCJ9.6bUi4osG58g_PZhFmfVHUrO5iD_FfZ8UrDfudPiK_8CCi3_JSqXRR1G4HFJjkHhqfSOY5Vw-kLBs5J3vBhCTDQ";

        let result = validate(key);
        assert!(result.is_ok(), "Expected valid key: {:?}", result);

        let license = result.unwrap();
        assert_eq!(license.customer_name(), "Acme Corporation");
        assert_eq!(license.tier(), Tier::Pro);
        assert!(!license.is_expired());
    }
}
