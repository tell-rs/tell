//! License key storage.
//!
//! Handles loading and saving license keys from:
//! 1. `~/.tell/license` file
//! 2. Environment variable `TELL_LICENSE_KEY`
//! 3. Config file `[license]` section

use std::path::{Path, PathBuf};

use crate::error::LicenseError;

/// Environment variable for license key.
pub const LICENSE_ENV_VAR: &str = "TELL_LICENSE_KEY";

/// Get the default license file path.
pub fn default_license_path() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".tell")
        .join("license")
}

/// Load license key from all sources (in order of precedence).
///
/// Order:
/// 1. Environment variable `TELL_LICENSE_KEY`
/// 2. File at `~/.tell/license`
/// 3. Explicit path if provided
///
/// Returns `None` if no license key is found (free tier).
pub fn load_license_key(explicit_path: Option<&Path>) -> Option<String> {
    // 1. Check environment variable
    if let Ok(key) = std::env::var(LICENSE_ENV_VAR) {
        let key = key.trim().to_string();
        if !key.is_empty() {
            return Some(key);
        }
    }

    // 2. Check explicit path
    if let Some(path) = explicit_path
        && let Some(key) = read_key_file(path)
    {
        return Some(key);
    }

    // 3. Check default path
    let default_path = default_license_path();
    if let Some(key) = read_key_file(&default_path) {
        return Some(key);
    }

    None
}

/// Read license key from a file.
fn read_key_file(path: &Path) -> Option<String> {
    if !path.exists() {
        return None;
    }

    match std::fs::read_to_string(path) {
        Ok(content) => {
            let key = content.trim().to_string();
            if key.is_empty() { None } else { Some(key) }
        }
        Err(_) => None,
    }
}

/// Save license key to the default location.
pub fn save_license_key(key: &str) -> Result<PathBuf, LicenseError> {
    let path = default_license_path();

    // Create parent directory
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    // Write key
    std::fs::write(&path, key.trim())?;

    Ok(path)
}

/// Remove the saved license key.
pub fn remove_license_key() -> Result<(), LicenseError> {
    let path = default_license_path();

    if path.exists() {
        std::fs::remove_file(&path)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_default_license_path() {
        let path = default_license_path();
        assert!(path.to_string_lossy().contains(".tell"));
        assert!(path.to_string_lossy().contains("license"));
    }

    #[test]
    fn test_env_var_precedence() {
        let key = "tell_lic_1_test";
        // SAFETY: Test runs single-threaded
        unsafe { env::set_var(LICENSE_ENV_VAR, key) };

        let result = load_license_key(None);
        assert_eq!(result, Some(key.to_string()));

        // SAFETY: Test runs single-threaded
        unsafe { env::remove_var(LICENSE_ENV_VAR) };
    }

    #[test]
    fn test_empty_env_var() {
        // SAFETY: Test runs single-threaded
        unsafe { env::set_var(LICENSE_ENV_VAR, "") };

        let result = load_license_key(None);
        // Should not return empty string
        assert!(result.is_none() || !result.as_ref().unwrap().is_empty());

        // SAFETY: Test runs single-threaded
        unsafe { env::remove_var(LICENSE_ENV_VAR) };
    }
}
