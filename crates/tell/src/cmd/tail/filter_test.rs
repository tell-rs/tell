//! Tests for content filters

use super::*;
use tell_protocol::{DecodedEvent, DecodedLogEntry, EventType, LogEventType, LogLevel};

// =============================================================================
// Pattern (Glob) Tests
// =============================================================================

#[test]
fn test_pattern_exact_match() {
    let p = Pattern::new("page_view");
    assert!(p.matches("page_view"));
    assert!(!p.matches("page_view_extra"));
    assert!(!p.matches("other"));
}

#[test]
fn test_pattern_wildcard_star() {
    let p = Pattern::new("page_*");
    assert!(p.matches("page_view"));
    assert!(p.matches("page_click"));
    assert!(p.matches("page_"));
    assert!(!p.matches("other"));
}

#[test]
fn test_pattern_wildcard_question() {
    let p = Pattern::new("page_?");
    assert!(p.matches("page_1"));
    assert!(p.matches("page_x"));
    assert!(!p.matches("page_view"));
    assert!(!p.matches("page_"));
}

#[test]
fn test_pattern_complex() {
    let p = Pattern::new("user_*_click");
    assert!(p.matches("user_123_click"));
    assert!(p.matches("user__click"));
    assert!(!p.matches("user_click"));
}

#[test]
fn test_pattern_star_at_end() {
    let p = Pattern::new("error*");
    assert!(p.matches("error"));
    assert!(p.matches("error_message"));
    assert!(p.matches("error: something went wrong"));
}

#[test]
fn test_pattern_star_at_start() {
    let p = Pattern::new("*_view");
    assert!(p.matches("page_view"));
    assert!(p.matches("_view"));
    assert!(!p.matches("page_click"));
}

#[test]
fn test_pattern_multiple_stars() {
    let p = Pattern::new("*user*click*");
    assert!(p.matches("user_click"));
    assert!(p.matches("the_user_did_click_here"));
    assert!(!p.matches("user"));
}

#[test]
fn test_glob_match_edge_cases() {
    // Empty pattern matches empty string
    assert!(glob_match("", ""));
    // Empty pattern doesn't match non-empty
    assert!(!glob_match("", "abc"));
    // Star matches empty
    assert!(glob_match("*", ""));
    assert!(glob_match("*", "anything"));
    // Question doesn't match empty
    assert!(!glob_match("?", ""));
    assert!(glob_match("?", "x"));
}

// =============================================================================
// ContentFilter Builder Tests
// =============================================================================

#[test]
fn test_filter_new_no_decode_needed() {
    let f = ContentFilter::new();
    assert!(!f.needs_decode());
}

#[test]
fn test_filter_with_event_names_needs_decode() {
    let f = ContentFilter::new().with_event_names(vec!["page_*"]);
    assert!(f.needs_decode());
}

#[test]
fn test_filter_with_log_levels_needs_decode() {
    let f = ContentFilter::new().with_log_levels(vec![LogLevel::Error]);
    assert!(f.needs_decode());
}

#[test]
fn test_filter_with_substring_needs_decode() {
    let f = ContentFilter::new().with_substring("error");
    assert!(f.needs_decode());
}

#[test]
fn test_filter_with_empty_event_names_no_decode() {
    let f = ContentFilter::new().with_event_names(vec![]);
    assert!(!f.needs_decode());
}

#[test]
fn test_filter_with_empty_levels_no_decode() {
    let f = ContentFilter::new().with_log_levels(vec![]);
    assert!(!f.needs_decode());
}

#[test]
fn test_filter_with_empty_substring_no_decode() {
    let f = ContentFilter::new().with_substring("");
    assert!(!f.needs_decode());
}

// =============================================================================
// Event Matching Tests
// =============================================================================

fn make_event<'a>(event_name: Option<&'a str>, payload: &'a [u8]) -> DecodedEvent<'a> {
    DecodedEvent {
        event_type: EventType::Track,
        timestamp: 1234567890,
        device_id: None,
        session_id: None,
        event_name,
        payload,
    }
}

#[test]
fn test_empty_filter_matches_all_events() {
    let f = ContentFilter::new();
    let event = make_event(Some("page_view"), b"{}");
    assert!(f.matches_event(&event));
}

#[test]
fn test_event_name_filter_matches() {
    let f = ContentFilter::new().with_event_names(vec!["page_*"]);

    assert!(f.matches_event(&make_event(Some("page_view"), b"{}")));
    assert!(f.matches_event(&make_event(Some("page_click"), b"{}")));
    assert!(!f.matches_event(&make_event(Some("button_click"), b"{}")));
}

#[test]
fn test_event_name_filter_multiple_patterns() {
    let f = ContentFilter::new().with_event_names(vec!["page_*", "button_*"]);

    assert!(f.matches_event(&make_event(Some("page_view"), b"{}")));
    assert!(f.matches_event(&make_event(Some("button_click"), b"{}")));
    assert!(!f.matches_event(&make_event(Some("form_submit"), b"{}")));
}

#[test]
fn test_event_name_filter_no_name() {
    let f = ContentFilter::new().with_event_names(vec!["page_*"]);

    // Event with no name should not match
    assert!(!f.matches_event(&make_event(None, b"{}")));
}

#[test]
fn test_event_substring_matches_name() {
    let f = ContentFilter::new().with_substring("view");

    assert!(f.matches_event(&make_event(Some("page_view"), b"{}")));
    assert!(!f.matches_event(&make_event(Some("page_click"), b"{}")));
}

#[test]
fn test_event_substring_matches_payload() {
    let f = ContentFilter::new().with_substring("error");

    let event = make_event(Some("api_call"), br#"{"status":"error"}"#);
    assert!(f.matches_event(&event));

    let event2 = make_event(Some("api_call"), br#"{"status":"success"}"#);
    assert!(!f.matches_event(&event2));
}

#[test]
fn test_event_combined_filters() {
    let f = ContentFilter::new()
        .with_event_names(vec!["api_*"])
        .with_substring("error");

    // Must match both: name pattern AND substring
    let event1 = make_event(Some("api_call"), br#"{"status":"error"}"#);
    assert!(f.matches_event(&event1));

    // Matches name but not substring
    let event2 = make_event(Some("api_call"), br#"{"status":"success"}"#);
    assert!(!f.matches_event(&event2));

    // Matches substring but not name
    let event3 = make_event(Some("page_view"), br#"{"status":"error"}"#);
    assert!(!f.matches_event(&event3));
}

// =============================================================================
// Log Matching Tests
// =============================================================================

fn make_log<'a>(
    level: LogLevel,
    source: Option<&'a str>,
    service: Option<&'a str>,
    payload: &'a [u8],
) -> DecodedLogEntry<'a> {
    DecodedLogEntry {
        event_type: LogEventType::Log,
        session_id: None,
        level,
        timestamp: 1234567890,
        source,
        service,
        payload,
    }
}

#[test]
fn test_empty_filter_matches_all_logs() {
    let f = ContentFilter::new();
    let log = make_log(LogLevel::Info, Some("host"), Some("svc"), b"{}");
    assert!(f.matches_log(&log));
}

#[test]
fn test_log_level_filter_single() {
    let f = ContentFilter::new().with_log_levels(vec![LogLevel::Error]);

    assert!(f.matches_log(&make_log(LogLevel::Error, None, None, b"{}")));
    assert!(!f.matches_log(&make_log(LogLevel::Info, None, None, b"{}")));
    assert!(!f.matches_log(&make_log(LogLevel::Warning, None, None, b"{}")));
}

#[test]
fn test_log_level_filter_multiple() {
    let f = ContentFilter::new().with_log_levels(vec![LogLevel::Error, LogLevel::Warning]);

    assert!(f.matches_log(&make_log(LogLevel::Error, None, None, b"{}")));
    assert!(f.matches_log(&make_log(LogLevel::Warning, None, None, b"{}")));
    assert!(!f.matches_log(&make_log(LogLevel::Info, None, None, b"{}")));
}

#[test]
fn test_log_substring_matches_source() {
    let f = ContentFilter::new().with_substring("prod");

    assert!(f.matches_log(&make_log(LogLevel::Info, Some("web-01.prod"), None, b"{}")));
    assert!(!f.matches_log(&make_log(LogLevel::Info, Some("web-01.dev"), None, b"{}")));
}

#[test]
fn test_log_substring_matches_service() {
    let f = ContentFilter::new().with_substring("gateway");

    assert!(f.matches_log(&make_log(
        LogLevel::Info,
        None,
        Some("api-gateway"),
        b"{}"
    )));
    assert!(!f.matches_log(&make_log(LogLevel::Info, None, Some("worker"), b"{}")));
}

#[test]
fn test_log_substring_matches_payload() {
    let f = ContentFilter::new().with_substring("timeout");

    let log = make_log(LogLevel::Error, None, None, br#"{"error":"connection timeout"}"#);
    assert!(f.matches_log(&log));

    let log2 = make_log(LogLevel::Error, None, None, br#"{"error":"connection refused"}"#);
    assert!(!f.matches_log(&log2));
}

#[test]
fn test_log_combined_filters() {
    let f = ContentFilter::new()
        .with_log_levels(vec![LogLevel::Error])
        .with_substring("timeout");

    // Must match both: level AND substring
    let log1 = make_log(LogLevel::Error, None, None, br#"{"error":"timeout"}"#);
    assert!(f.matches_log(&log1));

    // Matches level but not substring
    let log2 = make_log(LogLevel::Error, None, None, br#"{"error":"refused"}"#);
    assert!(!f.matches_log(&log2));

    // Matches substring but not level
    let log3 = make_log(LogLevel::Info, None, None, br#"{"msg":"timeout"}"#);
    assert!(!f.matches_log(&log3));
}

