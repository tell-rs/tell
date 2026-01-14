//! Tests for pattern persistence

use super::*;
use tempfile::TempDir;

fn create_test_pattern(id: PatternId, template: &str) -> Pattern {
    Pattern {
        id,
        template: template.to_string(),
        tokens: template
            .split_whitespace()
            .map(|t| {
                if t == "<*>" {
                    None
                } else {
                    Some(t.to_string())
                }
            })
            .collect(),
        count: 1,
    }
}

#[test]
fn test_disabled_persistence() {
    let persistence = PatternPersistence::disabled();

    assert!(!persistence.is_enabled());

    // Operations are no-ops
    let pattern = create_test_pattern(1, "test");
    assert!(!persistence.add_pattern(&pattern));

    let result = persistence.flush();
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 0);

    let loaded = persistence.load();
    assert!(loaded.is_ok());
    assert!(loaded.unwrap().is_empty());
}

#[test]
fn test_add_pattern() {
    let persistence = PatternPersistence::new(Some("/tmp/test.json".into()), 10);

    let pattern = create_test_pattern(1, "User <*> logged in");
    let should_flush = persistence.add_pattern(&pattern);

    assert!(!should_flush);
    assert_eq!(persistence.pending_count(), 1);
}

#[test]
fn test_batch_threshold() {
    let persistence = PatternPersistence::new(Some("/tmp/test.json".into()), 3);

    let p1 = create_test_pattern(1, "Pattern 1");
    let p2 = create_test_pattern(2, "Pattern 2");
    let p3 = create_test_pattern(3, "Pattern 3");

    assert!(!persistence.add_pattern(&p1));
    assert!(!persistence.add_pattern(&p2));
    assert!(persistence.add_pattern(&p3)); // Threshold reached

    assert_eq!(persistence.pending_count(), 3);
}

#[test]
fn test_flush_and_load() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("patterns.json");

    let persistence = PatternPersistence::new(Some(file_path.clone()), 100);

    // Add patterns
    persistence.add_pattern(&create_test_pattern(1, "User <*> logged in"));
    persistence.add_pattern(&create_test_pattern(2, "Error in <*>"));

    // Flush
    let flushed = persistence.flush().unwrap();
    assert_eq!(flushed, 2);
    assert_eq!(persistence.pending_count(), 0);

    // Load
    let loaded = persistence.load().unwrap();
    assert_eq!(loaded.len(), 2);

    let ids: Vec<_> = loaded.iter().map(|p| p.id).collect();
    assert!(ids.contains(&1));
    assert!(ids.contains(&2));
}

#[test]
fn test_merge_on_flush() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("patterns.json");

    let persistence = PatternPersistence::new(Some(file_path), 100);

    // First batch
    persistence.add_pattern(&create_test_pattern(1, "Pattern 1"));
    persistence.flush().unwrap();

    // Second batch
    persistence.add_pattern(&create_test_pattern(2, "Pattern 2"));
    persistence.flush().unwrap();

    // Both should be present
    let loaded = persistence.load().unwrap();
    assert_eq!(loaded.len(), 2);
}

#[test]
fn test_update_existing_pattern() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("patterns.json");

    let persistence = PatternPersistence::new(Some(file_path), 100);

    // Initial pattern
    let mut p = create_test_pattern(1, "Pattern 1");
    p.count = 10;
    persistence.add_pattern(&p);
    persistence.flush().unwrap();

    // Update same pattern
    p.count = 20;
    persistence.add_pattern(&p);
    persistence.flush().unwrap();

    // Should have 1 pattern with updated count
    let loaded = persistence.load().unwrap();
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].count, 20);
}

#[test]
fn test_load_nonexistent_file() {
    let persistence = PatternPersistence::new(Some("/nonexistent/path.json".into()), 100);

    let loaded = persistence.load().unwrap();
    assert!(loaded.is_empty());
}

#[test]
fn test_save_all() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("patterns.json");

    let persistence = PatternPersistence::new(Some(file_path), 100);

    let patterns = vec![
        create_test_pattern(1, "Pattern 1"),
        create_test_pattern(2, "Pattern 2"),
        create_test_pattern(3, "Pattern 3"),
    ];

    persistence.save_all(&patterns).unwrap();

    let loaded = persistence.load().unwrap();
    assert_eq!(loaded.len(), 3);
}

#[test]
fn test_stored_pattern_from_pattern() {
    let pattern = Pattern {
        id: 42,
        template: "User <*> logged in".to_string(),
        tokens: vec![
            Some("User".to_string()),
            None,
            Some("logged".to_string()),
            Some("in".to_string()),
        ],
        count: 100,
    };

    let stored = StoredPattern::from(&pattern);

    assert_eq!(stored.id, 42);
    assert_eq!(stored.template, "User <*> logged in");
    assert_eq!(stored.count, 100);
    assert_eq!(stored.tokens.len(), 4);
}

#[test]
fn test_stored_pattern_serialization() {
    let stored = StoredPattern {
        id: 1,
        template: "Test <*>".to_string(),
        count: 5,
        tokens: vec![Some("Test".to_string()), None],
    };

    let json = serde_json::to_string(&stored).unwrap();
    assert!(json.contains("\"id\":1"));
    assert!(json.contains("\"template\":\"Test <*>\""));

    let deserialized: StoredPattern = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized.id, stored.id);
    assert_eq!(deserialized.template, stored.template);
}

#[test]
fn test_pattern_file_format() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("patterns.json");

    let persistence = PatternPersistence::new(Some(file_path.clone()), 100);

    persistence.add_pattern(&create_test_pattern(1, "Test pattern"));
    persistence.flush().unwrap();

    // Read raw file
    let content = std::fs::read_to_string(&file_path).unwrap();
    assert!(content.contains("\"version\":"));
    assert!(content.contains("\"patterns\":"));

    // Verify it's valid JSON
    let parsed: serde_json::Value = serde_json::from_str(&content).unwrap();
    assert_eq!(parsed["version"], FORMAT_VERSION);
}

#[test]
fn test_creates_parent_directories() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("nested/dir/patterns.json");

    let persistence = PatternPersistence::new(Some(file_path.clone()), 100);

    persistence.add_pattern(&create_test_pattern(1, "Test"));
    persistence.flush().unwrap();

    assert!(file_path.exists());
}

#[test]
fn test_empty_tokens_not_serialized() {
    let stored = StoredPattern {
        id: 1,
        template: "Test".to_string(),
        count: 1,
        tokens: vec![], // Empty
    };

    let json = serde_json::to_string(&stored).unwrap();

    // Empty tokens should be skipped
    assert!(!json.contains("\"tokens\""));
}
