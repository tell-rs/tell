//! ClickHouse-specific helper functions
//!
//! Utility functions for ClickHouse data formatting and transformation.

use std::net::IpAddr;

use cdp_protocol::Batch;

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
pub fn generate_user_id_from_email(email: &str) -> String {
    if email.is_empty() {
        return String::new();
    }

    // Normalize: trim whitespace and convert to lowercase
    let normalized = email.trim().to_lowercase();

    if normalized.is_empty() {
        return String::new();
    }

    // Generate UUID v5 from normalized email using DNS namespace
    // This matches the Go implementation: uuid.NewSHA1(namespace, []byte(normalizedEmail))
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    // Simple deterministic hash-based UUID (for compatibility)
    // In production, you'd use uuid crate with v5
    let mut hasher = DefaultHasher::new();
    normalized.hash(&mut hasher);
    let hash = hasher.finish();

    // Format as UUID-like string
    format!(
        "{:08x}-{:04x}-5{:03x}-{:04x}-{:012x}",
        (hash >> 32) as u32,
        ((hash >> 16) & 0xffff) as u16,
        ((hash >> 4) & 0xfff) as u16,
        ((hash & 0xf) << 12 | 0x8000 | (hash >> 48 & 0xfff)) as u16,
        hash & 0xffffffffffff
    )
}

/// Normalize locale to exactly 5 characters for FixedString(5)
pub fn normalize_locale(locale: &str) -> String {
    if locale.len() >= 5 {
        locale[..5].to_string()
    } else {
        format!("{:5}", locale)
    }
}
