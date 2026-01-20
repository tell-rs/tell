//! Tests for Drain algorithm

use super::*;

#[test]
fn test_drain_tree_creation() {
    let tree = DrainTree::new(0.5, 100);
    assert_eq!(tree.pattern_count(), 0);
}

#[test]
fn test_default_drain_tree() {
    let tree = DrainTree::default();
    assert_eq!(tree.similarity_threshold, 0.5);
    assert_eq!(tree.max_children, 100);
}

#[test]
fn test_single_message_creates_pattern() {
    let tree = DrainTree::new(0.5, 100);

    let id = tree.parse("User logged in successfully");
    assert!(id > 0);
    assert_eq!(tree.pattern_count(), 1);

    let pattern = tree.get_pattern(id).unwrap();
    assert_eq!(pattern.id, id);
    assert!(pattern.count >= 1);
}

#[test]
fn test_identical_messages_same_pattern() {
    let tree = DrainTree::new(0.5, 100);

    let id1 = tree.parse("User logged in successfully");
    let id2 = tree.parse("User logged in successfully");

    assert_eq!(id1, id2);
    assert_eq!(tree.pattern_count(), 1);

    let pattern = tree.get_pattern(id1).unwrap();
    assert_eq!(pattern.count, 2);
}

#[test]
fn test_similar_messages_with_variables() {
    let tree = DrainTree::new(0.5, 100);

    let id1 = tree.parse("User 12345 logged in");
    let id2 = tree.parse("User 67890 logged in");

    assert_eq!(id1, id2, "Numbers should be treated as variables");
    assert_eq!(tree.pattern_count(), 1);

    let pattern = tree.get_pattern(id1).unwrap();
    assert!(pattern.template.contains("<*>"));
}

#[test]
fn test_different_messages_different_patterns() {
    let tree = DrainTree::new(0.5, 100);

    let id1 = tree.parse("User logged in");
    let id2 = tree.parse("Connection closed");

    assert_ne!(id1, id2);
    assert_eq!(tree.pattern_count(), 2);
}

#[test]
fn test_variable_detection_numbers() {
    let tree = DrainTree::new(0.5, 100);

    assert!(tree.is_variable("12345"));
    assert!(tree.is_variable("-42"));
    assert!(tree.is_variable("3.14159"));
    assert!(tree.is_variable("0"));
}

#[test]
fn test_variable_detection_hex() {
    let tree = DrainTree::new(0.5, 100);

    assert!(tree.is_variable("0x1234"));
    assert!(tree.is_variable("0xDEADBEEF"));
    assert!(tree.is_variable("abcdef123456789")); // Long hex
}

#[test]
fn test_variable_detection_ip_addresses() {
    let tree = DrainTree::new(0.5, 100);

    assert!(tree.is_variable("192.168.1.1"));
    assert!(tree.is_variable("10.0.0.1"));
    assert!(tree.is_variable("255.255.255.0"));
}

#[test]
fn test_variable_detection_uuid() {
    let tree = DrainTree::new(0.5, 100);

    assert!(tree.is_variable("550e8400-e29b-41d4-a716-446655440000"));
    assert!(tree.is_variable("123e4567-e89b-12d3-a456-426614174000"));
}

#[test]
fn test_variable_detection_paths() {
    let tree = DrainTree::new(0.5, 100);

    assert!(tree.is_variable("/var/log/syslog"));
    assert!(tree.is_variable("C:\\Windows\\System32"));
    assert!(tree.is_variable("/home/user/file.txt"));
}

#[test]
fn test_variable_detection_urls() {
    let tree = DrainTree::new(0.5, 100);

    assert!(tree.is_variable("http://example.com"));
    assert!(tree.is_variable("https://api.example.com/v1"));
}

#[test]
fn test_variable_detection_email() {
    let tree = DrainTree::new(0.5, 100);

    assert!(tree.is_variable("user@example.com"));
    assert!(tree.is_variable("test.user@domain.org"));
}

#[test]
fn test_variable_detection_timestamps() {
    let tree = DrainTree::new(0.5, 100);

    assert!(tree.is_variable("12:34:56"));
    assert!(tree.is_variable("09:00"));
}

#[test]
fn test_non_variables() {
    let tree = DrainTree::new(0.5, 100);

    assert!(!tree.is_variable("User"));
    assert!(!tree.is_variable("logged"));
    assert!(!tree.is_variable("ERROR"));
    assert!(!tree.is_variable("successfully"));
}

#[test]
fn test_empty_message() {
    let tree = DrainTree::new(0.5, 100);

    let id = tree.parse("");
    assert_eq!(id, 0);
    assert_eq!(tree.pattern_count(), 0);
}

#[test]
fn test_whitespace_only_message() {
    let tree = DrainTree::new(0.5, 100);

    let id = tree.parse("   \t\n  ");
    assert_eq!(id, 0);
}

#[test]
fn test_pattern_template_format() {
    let tree = DrainTree::new(0.5, 100);

    let id = tree.parse("Request from 192.168.1.1 took 42ms");
    let pattern = tree.get_pattern(id).unwrap();

    assert!(pattern.template.contains("Request"));
    assert!(pattern.template.contains("from"));
    assert!(pattern.template.contains("<*>")); // IP and number
    assert!(pattern.template.contains("took"));
}

#[test]
fn test_high_similarity_threshold() {
    let tree = DrainTree::new(0.9, 100);

    let id1 = tree.parse("User logged in from office");
    let id2 = tree.parse("User logged in from home");

    // High threshold = slight differences create new patterns
    assert_ne!(id1, id2);
}

#[test]
fn test_low_similarity_threshold() {
    let tree = DrainTree::new(0.3, 100);

    let id1 = tree.parse("Error in module A");
    let id2 = tree.parse("Error in module B");

    // Low threshold = more lenient matching
    assert_eq!(id1, id2);
}

#[test]
fn test_all_patterns() {
    let tree = DrainTree::new(0.5, 100);

    tree.parse("Message type A");
    tree.parse("Message type B");
    tree.parse("Different message");

    let patterns = tree.all_patterns();
    assert!(patterns.len() >= 2); // At least 2 different patterns
}

#[test]
fn test_get_nonexistent_pattern() {
    let tree = DrainTree::new(0.5, 100);

    assert!(tree.get_pattern(999).is_none());
}

#[test]
fn test_concurrent_parsing() {
    use std::sync::Arc;
    use std::thread;

    let tree = Arc::new(DrainTree::new(0.5, 100));
    let mut handles = vec![];

    for i in 0..10 {
        let tree = Arc::clone(&tree);
        handles.push(thread::spawn(move || {
            for j in 0..100 {
                tree.parse(&format!("Request {} from user {}", i, j));
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Should have created patterns without panicking
    assert!(tree.pattern_count() > 0);
}

#[test]
fn test_log_patterns_realistic() {
    let tree = DrainTree::new(0.5, 100);

    // Typical log messages
    let messages = [
        "2024-01-15 10:23:45 INFO User admin logged in from 192.168.1.100",
        "2024-01-15 10:24:12 INFO User guest logged in from 10.0.0.50",
        "2024-01-15 10:25:00 ERROR Connection timeout after 30000ms",
        "2024-01-15 10:25:30 ERROR Connection timeout after 45000ms",
        "2024-01-15 10:26:00 INFO Request processed in 125ms",
        "2024-01-15 10:26:30 INFO Request processed in 89ms",
    ];

    let mut ids = vec![];
    for msg in &messages {
        ids.push(tree.parse(msg));
    }

    // Similar messages should cluster
    assert_eq!(ids[0], ids[1], "Login messages should match");
    assert_eq!(ids[2], ids[3], "Timeout messages should match");
    assert_eq!(ids[4], ids[5], "Request messages should match");

    // Different types should differ
    assert_ne!(ids[0], ids[2]);
    assert_ne!(ids[0], ids[4]);
    assert_ne!(ids[2], ids[4]);
}

#[test]
fn test_clear() {
    let tree = DrainTree::new(0.5, 100);

    tree.parse("Message one");
    tree.parse("Message two");
    assert!(tree.pattern_count() > 0);

    tree.clear();
    assert_eq!(tree.pattern_count(), 0);
}

#[test]
fn test_canonical_name_simple() {
    let tree = DrainTree::new(0.5, 100);

    let id = tree.parse("User logged in successfully");
    let pattern = tree.get_pattern(id).unwrap();

    assert_eq!(pattern.canonical_name, "User logged in successfully");
}

#[test]
fn test_canonical_name_with_variables() {
    let tree = DrainTree::new(0.5, 100);

    let id = tree.parse("User 12345 logged in from 192.168.1.1");
    let pattern = tree.get_pattern(id).unwrap();

    // Should skip the variable tokens and take first 5 non-wildcards
    assert_eq!(pattern.canonical_name, "User logged in from");
}

#[test]
fn test_canonical_name_truncated() {
    let tree = DrainTree::new(0.5, 100);

    let id = tree.parse("This is a very long message with many tokens");
    let pattern = tree.get_pattern(id).unwrap();

    // Should only have first 5 tokens
    assert_eq!(pattern.canonical_name, "This is a very long");
}

#[test]
fn test_canonical_name_all_variables() {
    // Create pattern with mostly variables
    let tokens: Vec<Option<String>> = vec![None, None, None];
    let canonical = generate_canonical_name(&tokens);
    assert_eq!(canonical, "Unknown Pattern");
}

#[test]
fn test_canonical_name_fewer_than_five_tokens() {
    let tree = DrainTree::new(0.5, 100);

    let id = tree.parse("Error found");
    let pattern = tree.get_pattern(id).unwrap();

    assert_eq!(pattern.canonical_name, "Error found");
}

#[test]
fn test_pattern_timestamps() {
    let tree = DrainTree::new(0.5, 100);

    let id = tree.parse("User logged in");
    let pattern = tree.get_pattern(id).unwrap();

    // first_seen and last_seen should be set
    assert!(pattern.first_seen > 0);
    assert!(pattern.last_seen > 0);
    assert!(pattern.last_seen >= pattern.first_seen);
}

#[test]
fn test_last_seen_updated_on_match() {
    use std::thread;
    use std::time::Duration;

    let tree = DrainTree::new(0.5, 100);

    // Create pattern
    let id = tree.parse("Request completed");
    let pattern1 = tree.get_pattern(id).unwrap();
    let first_last_seen = pattern1.last_seen;

    // Wait a tiny bit and match again
    thread::sleep(Duration::from_millis(10));
    tree.parse("Request completed");

    let pattern2 = tree.get_pattern(id).unwrap();

    // first_seen should be unchanged
    assert_eq!(pattern2.first_seen, pattern1.first_seen);

    // last_seen should be updated (or same if sub-second granularity)
    assert!(pattern2.last_seen >= first_last_seen);

    // count should be incremented
    assert_eq!(pattern2.count, 2);
}
