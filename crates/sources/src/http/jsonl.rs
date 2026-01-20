//! JSONL parsing with DoS protection
//!
//! Parses JSON Lines format with depth and line count limits.

use super::error::LineError;
use super::json_types::{JsonEvent, JsonLogEntry};

/// Maximum lines per JSONL request (DoS protection)
pub const MAX_LINES_PER_REQUEST: usize = 10_000;

/// Maximum JSON nesting depth (DoS protection)
pub const MAX_JSON_DEPTH: usize = 32;

/// Parse JSONL body into events with DoS protection
pub fn parse_jsonl_events(body: &[u8]) -> (Vec<JsonEvent>, Vec<LineError>) {
    let mut events = Vec::new();
    let mut errors = Vec::new();
    let mut line_count = 0;

    for (idx, line) in body.split(|&b| b == b'\n').enumerate() {
        let line_num = idx + 1;

        // Skip empty lines
        if line.is_empty() || line.iter().all(|&b| b.is_ascii_whitespace()) {
            continue;
        }

        line_count += 1;

        // DoS protection: limit number of lines
        if line_count > MAX_LINES_PER_REQUEST {
            errors.push(LineError {
                line: line_num,
                error: format!(
                    "exceeded maximum {} lines per request",
                    MAX_LINES_PER_REQUEST
                ),
            });
            break;
        }

        // Parse with depth limit
        match parse_json_with_depth_check::<JsonEvent>(line) {
            Ok(event) => events.push(event),
            Err(e) => errors.push(LineError {
                line: line_num,
                error: e,
            }),
        }
    }

    (events, errors)
}

/// Parse JSONL body into logs with DoS protection
pub fn parse_jsonl_logs(body: &[u8]) -> (Vec<JsonLogEntry>, Vec<LineError>) {
    let mut logs = Vec::new();
    let mut errors = Vec::new();
    let mut line_count = 0;

    for (idx, line) in body.split(|&b| b == b'\n').enumerate() {
        let line_num = idx + 1;

        if line.is_empty() || line.iter().all(|&b| b.is_ascii_whitespace()) {
            continue;
        }

        line_count += 1;

        // DoS protection: limit number of lines
        if line_count > MAX_LINES_PER_REQUEST {
            errors.push(LineError {
                line: line_num,
                error: format!(
                    "exceeded maximum {} lines per request",
                    MAX_LINES_PER_REQUEST
                ),
            });
            break;
        }

        // Parse with depth limit
        match parse_json_with_depth_check::<JsonLogEntry>(line) {
            Ok(log) => logs.push(log),
            Err(e) => errors.push(LineError {
                line: line_num,
                error: e,
            }),
        }
    }

    (logs, errors)
}

/// Parse JSON with depth checking for DoS protection
fn parse_json_with_depth_check<T: serde::de::DeserializeOwned>(data: &[u8]) -> Result<T, String> {
    // First, check the depth using a lightweight pass
    if exceeds_json_depth(data, MAX_JSON_DEPTH) {
        return Err(format!(
            "JSON nesting exceeds maximum depth of {}",
            MAX_JSON_DEPTH
        ));
    }

    // Then parse normally
    serde_json::from_slice(data).map_err(|e| e.to_string())
}

/// Check if JSON exceeds maximum nesting depth (lightweight check)
fn exceeds_json_depth(data: &[u8], max_depth: usize) -> bool {
    let mut depth: usize = 0;
    let mut in_string = false;
    let mut escape_next = false;

    for &byte in data {
        if escape_next {
            escape_next = false;
            continue;
        }

        match byte {
            b'\\' if in_string => {
                escape_next = true;
            }
            b'"' => {
                in_string = !in_string;
            }
            b'{' | b'[' if !in_string => {
                depth += 1;
                if depth > max_depth {
                    return true;
                }
            }
            b'}' | b']' if !in_string => {
                depth = depth.saturating_sub(1);
            }
            _ => {}
        }
    }

    false
}
