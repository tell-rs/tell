//! Password hashing and verification
//!
//! Uses Argon2id for secure password hashing. Argon2 won the Password Hashing
//! Competition and is recommended for new applications.

use argon2::{
    Argon2,
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString, rand_core::OsRng},
};

use crate::error::{AuthError, Result};

/// Hash a password using Argon2id
///
/// Returns the hash in PHC format: `$argon2id$v=19$m=...,t=...,p=...$salt$hash`
///
/// # Example
///
/// ```
/// use tell_auth::password::hash_password;
///
/// let hash = hash_password("my_password").unwrap();
/// assert!(hash.starts_with("$argon2id$"));
/// ```
pub fn hash_password(password: &str) -> Result<String> {
    let salt = SaltString::generate(&mut OsRng);
    let argon2 = Argon2::default();

    let hash = argon2
        .hash_password(password.as_bytes(), &salt)
        .map_err(|e| AuthError::InvalidClaims(format!("password hash failed: {}", e)))?;

    Ok(hash.to_string())
}

/// Verify a password against a stored hash
///
/// # Example
///
/// ```
/// use tell_auth::password::{hash_password, verify_password};
///
/// let hash = hash_password("my_password").unwrap();
/// assert!(verify_password("my_password", &hash).unwrap());
/// assert!(!verify_password("wrong_password", &hash).unwrap());
/// ```
pub fn verify_password(password: &str, hash: &str) -> Result<bool> {
    let parsed_hash = PasswordHash::new(hash)
        .map_err(|e| AuthError::InvalidClaims(format!("invalid password hash: {}", e)))?;

    let argon2 = Argon2::default();

    match argon2.verify_password(password.as_bytes(), &parsed_hash) {
        Ok(()) => Ok(true),
        Err(argon2::password_hash::Error::Password) => Ok(false),
        Err(e) => Err(AuthError::InvalidClaims(format!(
            "password verification failed: {}",
            e
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_password() {
        let hash = hash_password("test_password").unwrap();

        // PHC format check
        assert!(hash.starts_with("$argon2id$"));

        // Hash should be different each time (different salt)
        let hash2 = hash_password("test_password").unwrap();
        assert_ne!(hash, hash2);
    }

    #[test]
    fn test_verify_correct_password() {
        let hash = hash_password("correct_password").unwrap();
        assert!(verify_password("correct_password", &hash).unwrap());
    }

    #[test]
    fn test_verify_wrong_password() {
        let hash = hash_password("correct_password").unwrap();
        assert!(!verify_password("wrong_password", &hash).unwrap());
    }

    #[test]
    fn test_verify_empty_password() {
        let hash = hash_password("").unwrap();
        assert!(verify_password("", &hash).unwrap());
        assert!(!verify_password("not_empty", &hash).unwrap());
    }

    #[test]
    fn test_verify_invalid_hash() {
        let result = verify_password("password", "not_a_valid_hash");
        assert!(result.is_err());
    }

    #[test]
    fn test_unicode_password() {
        let password = "пароль密码🔐";
        let hash = hash_password(password).unwrap();
        assert!(verify_password(password, &hash).unwrap());
    }

    #[test]
    fn test_long_password() {
        let password = "a".repeat(1000);
        let hash = hash_password(&password).unwrap();
        assert!(verify_password(&password, &hash).unwrap());
    }
}
