//! ClickHouse-specific helper functions
//!
//! Utility functions for ClickHouse data formatting and transformation.

use std::net::IpAddr;

use tell_protocol::Batch;
use uuid::Uuid;

/// Standard DNS namespace UUID (RFC 4122)
/// Same as Go: uuid.MustParse("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
const DNS_NAMESPACE: Uuid = Uuid::from_bytes([
    0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8,
]);

/// Extract source IP from batch as 16-byte array (IPv6 format)
///
/// ClickHouse stores IPs in FixedString(16) using IPv6 format.
/// IPv4 addresses are mapped to IPv6 (::ffff:x.x.x.x).
pub fn extract_source_ip(batch: &Batch) -> [u8; 16] {
    let mut ip_bytes = [0u8; 16];

    match batch.source_ip() {
        IpAddr::V4(v4) => {
            // IPv4-mapped IPv6: ::ffff:x.x.x.x
            ip_bytes[10] = 0xff;
            ip_bytes[11] = 0xff;
            ip_bytes[12..16].copy_from_slice(&v4.octets());
        }
        IpAddr::V6(v6) => {
            ip_bytes = v6.octets();
        }
    }

    ip_bytes
}

/// Generate deterministic user_id from email using UUID v5
///
/// Uses DNS namespace UUID and SHA-1 hashing, matching the Go implementation:
/// ```go
/// namespace := uuid.MustParse("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
/// userUUID := uuid.NewSHA1(namespace, []byte(normalizedEmail))
/// ```
///
/// Email is normalized (trimmed, lowercased) before hashing to ensure
/// "User@Example.com" and "user@example.com" produce the same UUID.
pub fn generate_user_id_from_email(email: &str) -> String {
    if email.is_empty() {
        return String::new();
    }

    // Normalize: trim whitespace and convert to lowercase
    // This matches Go: strings.ToLower(strings.TrimSpace(email))
    let normalized = email.trim().to_lowercase();

    if normalized.is_empty() {
        return String::new();
    }

    // Generate UUID v5 from normalized email using DNS namespace
    // This is equivalent to Go's uuid.NewSHA1(namespace, []byte(normalizedEmail))
    let user_uuid = Uuid::new_v5(&DNS_NAMESPACE, normalized.as_bytes());
    user_uuid.to_string()
}

/// Normalize locale to exactly 5 characters for FixedString(5)
pub fn normalize_locale(locale: &str) -> String {
    if locale.len() >= 5 {
        locale[..5].to_string()
    } else {
        format!("{:5}", locale)
    }
}
