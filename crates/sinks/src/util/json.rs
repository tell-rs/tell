//! JSON extraction utilities
//!
//! Simple JSON field extraction without full parsing overhead.
//! Useful for extracting specific fields from JSON payloads.

use std::collections::HashMap;

/// Extract a string value from JSON payload
///
/// Performs simple pattern matching to extract string values without
/// full JSON parsing. Suitable for hot-path extraction of known fields.
///
/// # Example
/// ```ignore
/// let payload = br#"{"email": "user@example.com", "name": "Test"}"#;
/// let email = extract_json_string(payload, "email");
/// assert_eq!(email, "user@example.com");
/// ```
pub fn extract_json_string(payload: &[u8], key: &str) -> String {
    let json_str = match std::str::from_utf8(payload) {
        Ok(s) => s,
        Err(_) => return String::new(),
    };

    // Look for "key": "value" or "key":"value"
    let search = format!("\"{}\"", key);
    if let Some(start) = json_str.find(&search) {
        let after_key = &json_str[start + search.len()..];
        // Skip whitespace and colon
        let after_colon = after_key
            .trim_start()
            .strip_prefix(':')
            .unwrap_or(after_key);
        let after_colon = after_colon.trim_start();

        if after_colon.starts_with('"') {
            // String value
            let value_start = 1;
            if let Some(end) = after_colon[value_start..].find('"') {
                return after_colon[value_start..value_start + end].to_string();
            }
        }
    }

    String::new()
}

/// Extract a JSON object as key-value pairs
///
/// Extracts a nested object and returns its contents as string key-value pairs.
/// Handles simple flat objects; nested objects within are returned as-is.
///
/// # Example
/// ```ignore
/// let payload = br#"{"traits": {"plan": "premium", "tier": "gold"}}"#;
/// let traits = extract_json_object(payload, "traits");
/// assert_eq!(traits.get("plan"), Some(&"premium".to_string()));
/// ```
pub fn extract_json_object(payload: &[u8], key: &str) -> HashMap<String, String> {
    let mut result = HashMap::new();

    let json_str = match std::str::from_utf8(payload) {
        Ok(s) => s,
        Err(_) => return result,
    };

    // Find the key
    let search = format!("\"{}\"", key);
    if let Some(start) = json_str.find(&search) {
        let after_key = &json_str[start + search.len()..];
        let after_colon = after_key
            .trim_start()
            .strip_prefix(':')
            .unwrap_or(after_key);
        let after_colon = after_colon.trim_start();

        if after_colon.starts_with('{') {
            // Find matching closing brace
            let mut depth = 0;
            let mut obj_end = 0;
            for (i, ch) in after_colon.chars().enumerate() {
                match ch {
                    '{' => depth += 1,
                    '}' => {
                        depth -= 1;
                        if depth == 0 {
                            obj_end = i + 1;
                            break;
                        }
                    }
                    _ => {}
                }
            }

            if obj_end > 0 {
                let obj_str = &after_colon[1..obj_end - 1];
                // Parse simple key-value pairs
                for part in obj_str.split(',') {
                    let part = part.trim();
                    if let Some(colon_pos) = part.find(':') {
                        let k = part[..colon_pos].trim().trim_matches('"');
                        let v = part[colon_pos + 1..].trim().trim_matches('"');
                        if !k.is_empty() {
                            result.insert(k.to_string(), v.to_string());
                        }
                    }
                }
            }
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_json_string() {
        let payload = br#"{"email": "user@example.com", "name": "Test User"}"#;
        assert_eq!(extract_json_string(payload, "email"), "user@example.com");
        assert_eq!(extract_json_string(payload, "name"), "Test User");
    }

    #[test]
    fn test_extract_json_string_missing_key() {
        let payload = br#"{"email": "user@example.com"}"#;
        assert_eq!(extract_json_string(payload, "missing"), "");
    }

    #[test]
    fn test_extract_json_string_empty_payload() {
        assert_eq!(extract_json_string(b"", "key"), "");
        assert_eq!(extract_json_string(b"{}", "key"), "");
    }

    #[test]
    fn test_extract_json_string_invalid_utf8() {
        let payload = &[0xff, 0xfe, 0x00, 0x01];
        assert_eq!(extract_json_string(payload, "key"), "");
    }

    #[test]
    fn test_extract_json_object() {
        let payload = br#"{"traits": {"plan": "premium", "tier": "gold"}}"#;
        let result = extract_json_object(payload, "traits");
        assert_eq!(result.get("plan"), Some(&"premium".to_string()));
        assert_eq!(result.get("tier"), Some(&"gold".to_string()));
    }

    #[test]
    fn test_extract_json_object_missing() {
        let payload = br#"{"email": "test@test.com"}"#;
        let result = extract_json_object(payload, "traits");
        assert!(result.is_empty());
    }

    #[test]
    fn test_extract_json_object_empty() {
        let payload = br#"{"traits": {}}"#;
        let result = extract_json_object(payload, "traits");
        assert!(result.is_empty());
    }
}
