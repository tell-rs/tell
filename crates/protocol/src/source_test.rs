//! Tests for SourceId type

use crate::source::SourceId;
use std::collections::{HashMap, HashSet};

// =============================================================================
// SourceId::new tests
// =============================================================================

#[test]
fn test_source_id_new_from_str() {
    let id = SourceId::new("tcp_main");
    assert_eq!(id.as_str(), "tcp_main");
}

#[test]
fn test_source_id_new_from_string() {
    let id = SourceId::new(String::from("syslog_tcp"));
    assert_eq!(id.as_str(), "syslog_tcp");
}

#[test]
fn test_source_id_new_empty() {
    let id = SourceId::new("");
    assert_eq!(id.as_str(), "");
}

#[test]
fn test_source_id_new_with_special_chars() {
    let id = SourceId::new("tcp-source_123.test");
    assert_eq!(id.as_str(), "tcp-source_123.test");
}

// =============================================================================
// SourceId::as_str tests
// =============================================================================

#[test]
fn test_source_id_as_str() {
    let id = SourceId::new("test");
    let s: &str = id.as_str();
    assert_eq!(s, "test");
}

// =============================================================================
// From trait tests
// =============================================================================

#[test]
fn test_source_id_from_str_ref() {
    let id: SourceId = "syslog_udp".into();
    assert_eq!(id.as_str(), "syslog_udp");
}

#[test]
fn test_source_id_from_string() {
    let s = String::from("tcp_debug");
    let id: SourceId = s.into();
    assert_eq!(id.as_str(), "tcp_debug");
}

// =============================================================================
// AsRef trait tests
// =============================================================================

#[test]
fn test_source_id_as_ref_str() {
    let id = SourceId::new("test_source");
    let s: &str = id.as_ref();
    assert_eq!(s, "test_source");
}

// =============================================================================
// Default trait tests
// =============================================================================

#[test]
fn test_source_id_default() {
    let id = SourceId::default();
    assert_eq!(id.as_str(), "unknown");
}

// =============================================================================
// Display trait tests
// =============================================================================

#[test]
fn test_source_id_display() {
    let id = SourceId::new("tcp_main");
    assert_eq!(format!("{}", id), "tcp_main");
}

#[test]
fn test_source_id_display_in_format_string() {
    let id = SourceId::new("source1");
    let msg = format!("Connected to {}", id);
    assert_eq!(msg, "Connected to source1");
}

// =============================================================================
// Clone trait tests
// =============================================================================

#[test]
fn test_source_id_clone() {
    let original = SourceId::new("tcp");
    let cloned = original.clone();
    assert_eq!(original, cloned);
    assert_eq!(cloned.as_str(), "tcp");
}

#[test]
fn test_source_id_clone_independence() {
    let original = SourceId::new("original");
    let cloned = original.clone();
    // Both should be equal and contain same data
    assert_eq!(original.as_str(), cloned.as_str());
}

// =============================================================================
// Debug trait tests
// =============================================================================

#[test]
fn test_source_id_debug() {
    let id = SourceId::new("debug_test");
    let debug_str = format!("{:?}", id);
    assert!(debug_str.contains("debug_test"));
}

// =============================================================================
// PartialEq / Eq trait tests
// =============================================================================

#[test]
fn test_source_id_eq_same() {
    let id1 = SourceId::new("tcp");
    let id2 = SourceId::new("tcp");
    assert_eq!(id1, id2);
}

#[test]
fn test_source_id_eq_different() {
    let id1 = SourceId::new("tcp");
    let id2 = SourceId::new("udp");
    assert_ne!(id1, id2);
}

#[test]
fn test_source_id_eq_case_sensitive() {
    let id1 = SourceId::new("TCP");
    let id2 = SourceId::new("tcp");
    assert_ne!(id1, id2);
}

#[test]
fn test_source_id_eq_empty() {
    let id1 = SourceId::new("");
    let id2 = SourceId::new("");
    assert_eq!(id1, id2);
}

#[test]
fn test_source_id_eq_reflexive() {
    let id = SourceId::new("test");
    assert_eq!(id, id);
}

#[test]
fn test_source_id_eq_symmetric() {
    let id1 = SourceId::new("test");
    let id2 = SourceId::new("test");
    assert_eq!(id1, id2);
    assert_eq!(id2, id1);
}

// =============================================================================
// Hash trait tests
// =============================================================================

#[test]
fn test_source_id_hash_set_insert() {
    let mut set = HashSet::new();
    set.insert(SourceId::new("tcp"));
    set.insert(SourceId::new("udp"));
    assert_eq!(set.len(), 2);
}

#[test]
fn test_source_id_hash_set_duplicate() {
    let mut set = HashSet::new();
    set.insert(SourceId::new("tcp"));
    set.insert(SourceId::new("tcp")); // duplicate
    assert_eq!(set.len(), 1);
}

#[test]
fn test_source_id_hash_set_contains() {
    let mut set = HashSet::new();
    set.insert(SourceId::new("tcp"));
    set.insert(SourceId::new("udp"));

    assert!(set.contains(&SourceId::new("tcp")));
    assert!(set.contains(&SourceId::new("udp")));
    assert!(!set.contains(&SourceId::new("syslog")));
}

#[test]
fn test_source_id_hash_map_key() {
    let mut map = HashMap::new();
    map.insert(SourceId::new("tcp_main"), 8080);
    map.insert(SourceId::new("tcp_debug"), 8081);

    assert_eq!(map.get(&SourceId::new("tcp_main")), Some(&8080));
    assert_eq!(map.get(&SourceId::new("tcp_debug")), Some(&8081));
    assert_eq!(map.get(&SourceId::new("unknown")), None);
}

// =============================================================================
// Real-world usage pattern tests
// =============================================================================

#[test]
fn test_source_id_typical_names() {
    let names = vec![
        "tcp_main",
        "tcp_debug",
        "tcp_forwarder",
        "syslog_tcp",
        "syslog_udp",
        "http_api",
    ];

    for name in names {
        let id = SourceId::new(name);
        assert_eq!(id.as_str(), name);
    }
}

#[test]
fn test_source_id_routing_lookup_pattern() {
    // Simulates the routing table lookup pattern
    let mut routes: HashMap<SourceId, Vec<&str>> = HashMap::new();
    routes.insert(SourceId::new("tcp_main"), vec!["clickhouse", "disk_binary"]);
    routes.insert(SourceId::new("tcp_debug"), vec!["stdout"]);

    let source = SourceId::new("tcp_main");
    let sinks = routes.get(&source);

    assert!(sinks.is_some());
    assert_eq!(sinks.unwrap(), &vec!["clickhouse", "disk_binary"]);
}
