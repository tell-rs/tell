//! Tests for reduce state management

use super::*;
use std::time::Duration;

#[test]
fn test_reduce_group_new() {
    let message = b"test message";
    let group = ReduceGroup::new(message, 12345);

    assert_eq!(group.first_message, message);
    assert_eq!(group.key_hash, 12345);
    assert_eq!(group.count, 1);
}

#[test]
fn test_reduce_group_add_event() {
    let mut group = ReduceGroup::new(b"test", 1);
    assert_eq!(group.count, 1);

    group.add_event();
    assert_eq!(group.count, 2);

    group.add_event();
    group.add_event();
    assert_eq!(group.count, 4);
}

#[test]
fn test_reduce_group_flush_count() {
    let mut group = ReduceGroup::new(b"test", 1);

    assert!(!group.should_flush_count(5));

    for _ in 0..4 {
        group.add_event();
    }
    assert!(group.should_flush_count(5)); // count is now 5
}

#[test]
fn test_reduce_state_new() {
    let state = ReduceState::new();
    assert!(state.is_empty());
    assert_eq!(state.group_count(), 0);
}

#[test]
fn test_reduce_state_get_or_create_new() {
    let mut state = ReduceState::new();
    let (group, is_new) = state.get_or_create(123, b"message1");

    assert!(is_new);
    assert_eq!(group.count, 1);
    assert_eq!(state.group_count(), 1);
}

#[test]
fn test_reduce_state_get_or_create_existing() {
    let mut state = ReduceState::new();

    // First call creates
    let (_, is_new) = state.get_or_create(123, b"message1");
    assert!(is_new);

    // Second call with same key increments
    let (group, is_new) = state.get_or_create(123, b"message2");
    assert!(!is_new);
    assert_eq!(group.count, 2);
    assert_eq!(state.group_count(), 1);
}

#[test]
fn test_reduce_state_multiple_groups() {
    let mut state = ReduceState::new();

    state.get_or_create(1, b"a");
    state.get_or_create(2, b"b");
    state.get_or_create(3, b"c");

    assert_eq!(state.group_count(), 3);
}

#[test]
fn test_reduce_state_remove() {
    let mut state = ReduceState::new();
    state.get_or_create(123, b"test");

    let removed = state.remove(123);
    assert!(removed.is_some());
    assert!(state.is_empty());

    let removed_again = state.remove(123);
    assert!(removed_again.is_none());
}

#[test]
fn test_reduce_state_flush_all() {
    let mut state = ReduceState::new();
    state.get_or_create(1, b"a");
    state.get_or_create(2, b"b");
    state.get_or_create(3, b"c");

    let flushed = state.flush_all();
    assert_eq!(flushed.len(), 3);
    assert!(state.is_empty());
}

#[test]
fn test_compute_group_key_empty_group_by() {
    // Empty group_by means hash entire message
    let key1 = compute_group_key(b"message1", &[]);
    let key2 = compute_group_key(b"message2", &[]);
    let key1_again = compute_group_key(b"message1", &[]);

    assert_ne!(key1, key2);
    assert_eq!(key1, key1_again);
}

#[test]
fn test_compute_group_key_json_field() {
    let group_by = vec!["error_code".to_string()];

    let msg1 = br#"{"error_code": "E001", "message": "first"}"#;
    let msg2 = br#"{"error_code": "E001", "message": "second"}"#;
    let msg3 = br#"{"error_code": "E002", "message": "third"}"#;

    let key1 = compute_group_key(msg1, &group_by);
    let key2 = compute_group_key(msg2, &group_by);
    let key3 = compute_group_key(msg3, &group_by);

    // Same error_code = same key
    assert_eq!(key1, key2);
    // Different error_code = different key
    assert_ne!(key1, key3);
}

#[test]
fn test_compute_group_key_multiple_fields() {
    let group_by = vec!["error_code".to_string(), "host".to_string()];

    let msg1 = br#"{"error_code": "E001", "host": "server1"}"#;
    let msg2 = br#"{"error_code": "E001", "host": "server1"}"#;
    let msg3 = br#"{"error_code": "E001", "host": "server2"}"#;

    let key1 = compute_group_key(msg1, &group_by);
    let key2 = compute_group_key(msg2, &group_by);
    let key3 = compute_group_key(msg3, &group_by);

    assert_eq!(key1, key2); // Same error_code AND host
    assert_ne!(key1, key3); // Same error_code but different host
}

#[test]
fn test_compute_group_key_nested_field() {
    let group_by = vec!["error.code".to_string()];

    let msg1 = br#"{"error": {"code": "E001", "msg": "a"}}"#;
    let msg2 = br#"{"error": {"code": "E001", "msg": "b"}}"#;
    let msg3 = br#"{"error": {"code": "E002", "msg": "c"}}"#;

    let key1 = compute_group_key(msg1, &group_by);
    let key2 = compute_group_key(msg2, &group_by);
    let key3 = compute_group_key(msg3, &group_by);

    assert_eq!(key1, key2);
    assert_ne!(key1, key3);
}

#[test]
fn test_compute_group_key_non_json() {
    // Non-JSON falls back to hashing entire message
    let group_by = vec!["error_code".to_string()];

    let msg1 = b"not json";
    let msg2 = b"not json";
    let msg3 = b"different";

    let key1 = compute_group_key(msg1, &group_by);
    let key2 = compute_group_key(msg2, &group_by);
    let key3 = compute_group_key(msg3, &group_by);

    assert_eq!(key1, key2);
    assert_ne!(key1, key3);
}

#[test]
fn test_compute_group_key_missing_field() {
    let group_by = vec!["nonexistent".to_string()];

    // Both messages missing the field - both hash to same value
    // because no field values are added to hasher
    let msg1 = br#"{"other": "value1"}"#;
    let msg2 = br#"{"other": "value2"}"#;

    let key1 = compute_group_key(msg1, &group_by);
    let key2 = compute_group_key(msg2, &group_by);

    // Both have missing field, so they get the same key
    assert_eq!(key1, key2);
}

#[test]
fn test_groups_to_flush_by_count() {
    let mut state = ReduceState::new();

    // Create group and add events up to max
    let (group, _) = state.get_or_create(1, b"test");
    for _ in 0..9 {
        group.add_event();
    }
    // Now count = 10

    let to_flush = state.groups_to_flush(Duration::from_secs(3600), 10);
    assert_eq!(to_flush.len(), 1);
    assert_eq!(to_flush[0], 1);
}
