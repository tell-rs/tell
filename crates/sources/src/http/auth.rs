//! HTTP authentication helpers
//!
//! API key extraction and validation from HTTP headers.

use std::net::IpAddr;

use axum::http::HeaderMap;
use tell_auth::{ApiKeyStore, WorkspaceId};

use super::error::HttpSourceError;

/// Extract API key from Authorization header (returns key only, for backward compat)
pub fn extract_api_key(
    headers: &HeaderMap,
    auth_store: &ApiKeyStore,
) -> Result<[u8; 16], HttpSourceError> {
    let (key, _workspace_id) = extract_api_key_with_workspace(headers, auth_store)?;
    Ok(key)
}

/// Extract API key and workspace ID from Authorization header
pub fn extract_api_key_with_workspace(
    headers: &HeaderMap,
    auth_store: &ApiKeyStore,
) -> Result<([u8; 16], WorkspaceId), HttpSourceError> {
    // Try Authorization: Bearer <key>
    let auth_header = headers
        .get("authorization")
        .or_else(|| headers.get("x-api-key"))
        .ok_or_else(|| HttpSourceError::AuthFailed("missing API key".into()))?;

    let auth_str = auth_header
        .to_str()
        .map_err(|_| HttpSourceError::AuthFailed("invalid header encoding".into()))?;

    // Parse Bearer token or raw key
    let key_str = if let Some(token) = auth_str.strip_prefix("Bearer ") {
        token.trim()
    } else {
        auth_str.trim()
    };

    // Validate key format and authenticate
    let key_bytes = parse_api_key(key_str)?;

    // Validate against auth store and get workspace ID
    let workspace_id = auth_store
        .validate_slice(&key_bytes)
        .ok_or_else(|| HttpSourceError::AuthFailed("invalid API key".into()))?;

    Ok((key_bytes, workspace_id))
}

/// Parse API key string to bytes
///
/// API keys must be exactly 32 hex characters (representing 16 bytes).
/// This strict validation prevents weak keys from padding/truncation.
pub fn parse_api_key(key: &str) -> Result<[u8; 16], HttpSourceError> {
    // Strict validation: must be exactly 32 hex characters
    if key.len() != 32 {
        return Err(HttpSourceError::AuthFailed(
            "API key must be 32 hex characters".into(),
        ));
    }

    if !key.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(HttpSourceError::AuthFailed(
            "API key must contain only hex characters".into(),
        ));
    }

    let bytes = hex::decode(key)
        .map_err(|_| HttpSourceError::AuthFailed("invalid API key format".into()))?;

    // This should always be 16 due to length check above, but verify anyway
    if bytes.len() != 16 {
        return Err(HttpSourceError::AuthFailed("API key must be 16 bytes".into()));
    }

    let mut result = [0u8; 16];
    result.copy_from_slice(&bytes);
    Ok(result)
}

/// Get source IP, checking X-Forwarded-For header for proxied requests
pub fn get_source_ip(headers: &HeaderMap, connection_ip: IpAddr) -> IpAddr {
    // Check X-Forwarded-For header (first IP is the original client)
    if let Some(xff) = headers.get("x-forwarded-for") {
        if let Ok(xff_str) = xff.to_str() {
            if let Some(first_ip) = xff_str.split(',').next() {
                if let Ok(ip) = first_ip.trim().parse::<IpAddr>() {
                    return ip;
                }
            }
        }
    }

    // Fall back to connection IP
    connection_ip
}

/// Check if content type indicates binary FlatBuffer
pub fn is_binary_content_type(content_type: &str) -> bool {
    let ct = content_type.to_lowercase();
    ct.starts_with("application/x-flatbuffers")
        || ct.starts_with("application/octet-stream")
        || ct.starts_with("application/x-tell")
}
