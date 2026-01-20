//! Tests for pattern storage trait and implementations

use super::super::PatternPersistence;
use super::*;
use std::sync::Arc;
use tempfile::TempDir;

fn create_test_stored_pattern(id: u64, template: &str) -> StoredPattern {
    StoredPattern {
        id,
        template: template.to_string(),
        canonical_name: template
            .split_whitespace()
            .take(3)
            .collect::<Vec<_>>()
            .join(" "),
        count: 1,
        first_seen: 1700000000,
        last_seen: 1700000000,
        tokens: vec![],
    }
}

// =============================================================================
// NullPatternStorage Tests
// =============================================================================

#[tokio::test]
async fn test_null_storage_load_empty() {
    let storage = NullPatternStorage;

    let patterns = storage.load().await.unwrap();
    assert!(patterns.is_empty());
}

#[tokio::test]
async fn test_null_storage_save_noop() {
    let storage = NullPatternStorage;

    let patterns = vec![create_test_stored_pattern(1, "Test pattern")];
    storage.save(&patterns).await.unwrap();

    // Should still be empty after save (it's a no-op)
    let loaded = storage.load().await.unwrap();
    assert!(loaded.is_empty());
}

#[test]
fn test_null_storage_disabled() {
    let storage = NullPatternStorage;

    assert!(!storage.is_enabled());
    assert_eq!(storage.name(), "null");
}

// =============================================================================
// FilePatternStorage Tests
// =============================================================================

#[tokio::test]
async fn test_file_storage_roundtrip() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("patterns.json");

    let persistence = Arc::new(PatternPersistence::new(Some(file_path), 100));
    let storage = FilePatternStorage::new(persistence);

    // Should be empty initially
    let patterns = storage.load().await.unwrap();
    assert!(patterns.is_empty());

    // Save patterns
    let patterns_to_save = vec![
        create_test_stored_pattern(1, "User logged in"),
        create_test_stored_pattern(2, "Error occurred"),
    ];
    storage.save(&patterns_to_save).await.unwrap();

    // Load them back
    let loaded = storage.load().await.unwrap();
    assert_eq!(loaded.len(), 2);
}

#[test]
fn test_file_storage_enabled() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("patterns.json");

    let persistence = Arc::new(PatternPersistence::new(Some(file_path), 100));
    let storage = FilePatternStorage::new(persistence);

    assert!(storage.is_enabled());
    assert_eq!(storage.name(), "file");
}

#[test]
fn test_file_storage_disabled_persistence() {
    let persistence = Arc::new(PatternPersistence::disabled());
    let storage = FilePatternStorage::new(persistence);

    assert!(!storage.is_enabled());
}

// =============================================================================
// Storage Error Tests
// =============================================================================

#[test]
fn test_storage_error_display() {
    let conn_err = StorageError::Connection("timeout".to_string());
    assert!(conn_err.to_string().contains("connection error"));

    let query_err = StorageError::Query("syntax error".to_string());
    assert!(query_err.to_string().contains("query error"));

    let ser_err = StorageError::Serialization("invalid utf8".to_string());
    assert!(ser_err.to_string().contains("serialization error"));

    let other_err = StorageError::Other("something went wrong".to_string());
    assert!(other_err.to_string().contains("something went wrong"));
}

#[test]
fn test_storage_error_from_io() {
    let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
    let storage_err: StorageError = io_err.into();

    assert!(matches!(storage_err, StorageError::Io(_)));
    assert!(storage_err.to_string().contains("io error"));
}

// =============================================================================
// BoxedPatternStorage Tests
// =============================================================================

#[tokio::test]
async fn test_boxed_storage_dispatch() {
    // Test that both implementations can be used through Box<dyn PatternStorage>
    let null_storage: BoxedPatternStorage = Box::new(NullPatternStorage);
    assert!(!null_storage.is_enabled());

    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("patterns.json");
    let persistence = Arc::new(PatternPersistence::new(Some(file_path), 100));
    let file_storage: BoxedPatternStorage = Box::new(FilePatternStorage::new(persistence));
    assert!(file_storage.is_enabled());

    // Both should be usable through the trait
    let _ = null_storage.load().await;
    let _ = file_storage.load().await;
}
