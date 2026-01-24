//! Telemetry endpoint configuration.
//!
//! Centralized static configuration for the telemetry collection endpoint.
//! Update these values to change where telemetry is sent.

/// Telemetry collection endpoint host
pub const TELEMETRY_HOST: &str = "t.tell.rs";

/// TCP port for FBS protocol (primary)
pub const TELEMETRY_TCP_PORT: u16 = 50000;

/// HTTP port for FBS fallback
pub const TELEMETRY_HTTP_PORT: u16 = 443;

/// HTTP endpoint path for FBS fallback
pub const TELEMETRY_HTTP_PATH: &str = "/v1/ingest";

/// API key for telemetry ingestion (16 bytes)
///
/// This key is used to authenticate telemetry events to t.tell.rs.
/// It's intentionally public - rate limiting and validation happen server-side.
pub const TELEMETRY_API_KEY: [u8; 16] = [
    0x74, 0x65, 0x6c, 0x6c, // "tell"
    0x2d, 0x74, 0x65, 0x6c, // "-tel"
    0x65, 0x6d, 0x65, 0x74, // "emet"
    0x72, 0x79, 0x2d, 0x76, // "ry-v"
];

/// Get the TCP address for telemetry
#[inline]
pub fn tcp_addr() -> String {
    format!("{}:{}", TELEMETRY_HOST, TELEMETRY_TCP_PORT)
}

/// Get the HTTPS URL for telemetry fallback
#[inline]
pub fn https_url() -> String {
    format!(
        "https://{}:{}{}",
        TELEMETRY_HOST, TELEMETRY_HTTP_PORT, TELEMETRY_HTTP_PATH
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tcp_addr() {
        assert_eq!(tcp_addr(), "t.tell.rs:50000");
    }

    #[test]
    fn test_https_url() {
        assert_eq!(https_url(), "https://t.tell.rs:443/v1/ingest");
    }

    #[test]
    fn test_api_key_is_16_bytes() {
        assert_eq!(TELEMETRY_API_KEY.len(), 16);
    }
}
