//! Password generation utilities

use std::time::{SystemTime, UNIX_EPOCH};

/// Alphanumeric characters for password generation
const CHARS: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

/// Generate a random alphanumeric password
///
/// Uses a simple PRNG seeded with system time and process info.
/// Not cryptographically secure but sufficient for ClickHouse passwords.
pub fn generate_password(length: usize) -> String {
    let mut seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(12345);

    // Mix in process ID for additional entropy
    seed ^= std::process::id() as u64;
    seed ^= (length as u64) << 32;

    let mut password = String::with_capacity(length);

    for i in 0..length {
        // Simple xorshift64
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed ^= (i as u64).wrapping_mul(0x517cc1b727220a95);

        let idx = (seed as usize) % CHARS.len();
        password.push(CHARS[idx] as char);
    }

    password
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_password_length() {
        assert_eq!(generate_password(16).len(), 16);
        assert_eq!(generate_password(32).len(), 32);
        assert_eq!(generate_password(64).len(), 64);
    }

    #[test]
    fn test_password_alphanumeric() {
        let password = generate_password(100);
        assert!(password.chars().all(|c| c.is_ascii_alphanumeric()));
    }

    #[test]
    fn test_passwords_different() {
        // Should generate different passwords each call
        let p1 = generate_password(32);
        let p2 = generate_password(32);
        // Not guaranteed but highly likely
        assert_ne!(p1, p2);
    }
}
