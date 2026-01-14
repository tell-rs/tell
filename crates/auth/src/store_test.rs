//! Tests for ApiKeyStore
//!
//! Tests cover parsing, validation, hot reload, and edge cases.

use std::str::FromStr;

use crate::{API_KEY_LENGTH, ApiKeyStore, WorkspaceId};

use super::store::key_from_hex;

// =============================================================================
// Basic store tests
// =============================================================================

#[test]
fn test_new_store_is_empty() {
    let store = ApiKeyStore::new();
    assert!(store.is_empty());
    assert_eq!(store.len(), 0);
}

#[test]
fn test_with_capacity() {
    let store = ApiKeyStore::with_capacity(100);
    assert!(store.is_empty());
}

#[test]
fn test_default() {
    let store = ApiKeyStore::default();
    assert!(store.is_empty());
}

// =============================================================================
// Insert and validate tests
// =============================================================================

#[test]
fn test_insert_and_validate() {
    let store = ApiKeyStore::new();
    let key = [0x01u8; API_KEY_LENGTH];
    let workspace = WorkspaceId::new(1);

    store.insert(key, workspace);

    assert_eq!(store.len(), 1);
    assert!(store.contains(&key));

    let result = store.validate(&key);
    assert_eq!(result, Some(workspace));
}

#[test]
fn test_validate_unknown_key() {
    let store = ApiKeyStore::new();
    let key = [0x01u8; API_KEY_LENGTH];

    let result = store.validate(&key);
    assert!(result.is_none());
}

#[test]
fn test_insert_replaces() {
    let store = ApiKeyStore::new();
    let key = [0x01u8; API_KEY_LENGTH];

    store.insert(key, WorkspaceId::new(100));
    let old = store.insert(key, WorkspaceId::new(200));

    assert_eq!(old, Some(WorkspaceId::new(100)));
    assert_eq!(store.validate(&key), Some(WorkspaceId::new(200)));
    assert_eq!(store.len(), 1);
}

#[test]
fn test_remove() {
    let store = ApiKeyStore::new();
    let key = [0x01u8; API_KEY_LENGTH];

    store.insert(key, WorkspaceId::new(42));
    assert!(store.contains(&key));

    let removed = store.remove(&key);
    assert_eq!(removed, Some(WorkspaceId::new(42)));
    assert!(!store.contains(&key));
    assert!(store.is_empty());
}

#[test]
fn test_remove_nonexistent() {
    let store = ApiKeyStore::new();
    let key = [0x01u8; API_KEY_LENGTH];

    let removed = store.remove(&key);
    assert!(removed.is_none());
}

#[test]
fn test_clear() {
    let store = ApiKeyStore::new();

    store.insert([0x01u8; 16], WorkspaceId::new(1));
    store.insert([0x02u8; 16], WorkspaceId::new(2));
    assert_eq!(store.len(), 2);

    store.clear();
    assert!(store.is_empty());
}

// =============================================================================
// Validate slice tests
// =============================================================================

#[test]
fn test_validate_slice() {
    let store = ApiKeyStore::new();
    let key = [0x42u8; API_KEY_LENGTH];
    store.insert(key, WorkspaceId::new(99));

    let slice: &[u8] = &key;
    let result = store.validate_slice(slice);
    assert_eq!(result, Some(WorkspaceId::new(99)));
}

#[test]
fn test_validate_slice_wrong_length() {
    let store = ApiKeyStore::new();
    let key = [0x42u8; API_KEY_LENGTH];
    store.insert(key, WorkspaceId::new(99));

    // Too short
    let short: &[u8] = &[0x42u8; 10];
    assert!(store.validate_slice(short).is_none());

    // Too long
    let long: &[u8] = &[0x42u8; 20];
    assert!(store.validate_slice(long).is_none());

    // Empty
    let empty: &[u8] = &[];
    assert!(store.validate_slice(empty).is_none());
}

// =============================================================================
// Parse from string tests
// =============================================================================

#[test]
fn test_from_str_single_key() {
    let contents = "000102030405060708090a0b0c0d0e0f:1";
    let store = ApiKeyStore::from_str(contents).unwrap();

    assert_eq!(store.len(), 1);

    let key = key_from_hex("000102030405060708090a0b0c0d0e0f");
    assert_eq!(store.validate(&key), Some(WorkspaceId::new(1)));
}

#[test]
fn test_from_str_multiple_keys() {
    let contents = r#"
000102030405060708090a0b0c0d0e0f:1
0f0e0d0c0b0a09080706050403020100:2
deadbeefdeadbeefdeadbeefdeadbeef:3
"#;
    let store = ApiKeyStore::from_str(contents).unwrap();

    assert_eq!(store.len(), 3);

    assert_eq!(
        store.validate(&key_from_hex("000102030405060708090a0b0c0d0e0f")),
        Some(WorkspaceId::new(1))
    );
    assert_eq!(
        store.validate(&key_from_hex("0f0e0d0c0b0a09080706050403020100")),
        Some(WorkspaceId::new(2))
    );
    assert_eq!(
        store.validate(&key_from_hex("deadbeefdeadbeefdeadbeefdeadbeef")),
        Some(WorkspaceId::new(3))
    );
}

#[test]
fn test_from_str_with_comments() {
    let contents = r#"
# This is a comment
000102030405060708090a0b0c0d0e0f:1

# Another comment
deadbeefdeadbeefdeadbeefdeadbeef:2
"#;
    let store = ApiKeyStore::from_str(contents).unwrap();

    assert_eq!(store.len(), 2);
}

#[test]
fn test_from_str_empty() {
    let contents = "";
    let store = ApiKeyStore::from_str(contents).unwrap();
    assert!(store.is_empty());
}

#[test]
fn test_from_str_only_comments() {
    let contents = r#"
# Comment 1
# Comment 2

# Comment 3
"#;
    let store = ApiKeyStore::from_str(contents).unwrap();
    assert!(store.is_empty());
}

#[test]
fn test_from_str_whitespace_handling() {
    let contents = "  000102030405060708090a0b0c0d0e0f:1  \n";
    let store = ApiKeyStore::from_str(contents).unwrap();

    assert_eq!(store.len(), 1);
    assert_eq!(
        store.validate(&key_from_hex("000102030405060708090a0b0c0d0e0f")),
        Some(WorkspaceId::new(1))
    );
}

#[test]
fn test_from_str_numeric_workspace() {
    let contents = "000102030405060708090a0b0c0d0e0f:123";
    let store = ApiKeyStore::from_str(contents).unwrap();

    assert_eq!(
        store.validate(&key_from_hex("000102030405060708090a0b0c0d0e0f")),
        Some(WorkspaceId::new(123))
    );
}

#[test]
fn test_from_str_large_workspace_id() {
    // Test with large workspace IDs
    let contents = "000102030405060708090a0b0c0d0e0f:4294967295"; // u32::MAX
    let store = ApiKeyStore::from_str(contents).unwrap();

    assert_eq!(
        store.validate(&key_from_hex("000102030405060708090a0b0c0d0e0f")),
        Some(WorkspaceId::new(u32::MAX))
    );
}

#[test]
fn test_from_str_zero_workspace_id() {
    let contents = "000102030405060708090a0b0c0d0e0f:0";
    let store = ApiKeyStore::from_str(contents).unwrap();

    assert_eq!(
        store.validate(&key_from_hex("000102030405060708090a0b0c0d0e0f")),
        Some(WorkspaceId::new(0))
    );
}

#[test]
fn test_from_str_uppercase_hex() {
    let contents = "DEADBEEFDEADBEEFDEADBEEFDEADBEEF:42";
    let store = ApiKeyStore::from_str(contents).unwrap();

    assert_eq!(
        store.validate(&key_from_hex("deadbeefdeadbeefdeadbeefdeadbeef")),
        Some(WorkspaceId::new(42))
    );
}

#[test]
fn test_from_str_mixed_case_hex() {
    let contents = "DeAdBeEfDeAdBeEfDeAdBeEfDeAdBeEf:42";
    let store = ApiKeyStore::from_str(contents).unwrap();

    assert_eq!(
        store.validate(&key_from_hex("deadbeefdeadbeefdeadbeefdeadbeef")),
        Some(WorkspaceId::new(42))
    );
}

// =============================================================================
// Parse error tests
// =============================================================================

#[test]
fn test_from_str_missing_colon() {
    let contents = "000102030405060708090a0b0c0d0e0f 1";
    let result = ApiKeyStore::from_str(contents);

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("colon"));
}

#[test]
fn test_from_str_key_too_short() {
    let contents = "0001020304:1"; // Only 10 hex chars
    let result = ApiKeyStore::from_str(contents);

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("32 hex"));
}

#[test]
fn test_from_str_key_too_long() {
    let contents = "000102030405060708090a0b0c0d0e0f00:1"; // 34 hex chars
    let result = ApiKeyStore::from_str(contents);

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("32 hex"));
}

#[test]
fn test_from_str_invalid_hex() {
    let contents = "000102030405060708090a0b0c0d0eXX:1";
    let result = ApiKeyStore::from_str(contents);

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("invalid hex"));
}

#[test]
fn test_from_str_empty_workspace() {
    let contents = "000102030405060708090a0b0c0d0e0f:";
    let result = ApiKeyStore::from_str(contents);

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("empty workspace"));
}

#[test]
fn test_from_str_whitespace_only_workspace() {
    let contents = "000102030405060708090a0b0c0d0e0f:   ";
    let result = ApiKeyStore::from_str(contents);

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("empty workspace"));
}

#[test]
fn test_from_str_non_numeric_workspace() {
    let contents = "000102030405060708090a0b0c0d0e0f:workspace_name";
    let result = ApiKeyStore::from_str(contents);

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("must be numeric"));
}

#[test]
fn test_from_str_negative_workspace() {
    let contents = "000102030405060708090a0b0c0d0e0f:-1";
    let result = ApiKeyStore::from_str(contents);

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("must be numeric"));
}

#[test]
fn test_from_str_workspace_overflow() {
    // u32::MAX + 1 should fail
    let contents = "000102030405060708090a0b0c0d0e0f:4294967296";
    let result = ApiKeyStore::from_str(contents);

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("must be numeric"));
}

#[test]
fn test_from_str_duplicate_key() {
    let contents = r#"
000102030405060708090a0b0c0d0e0f:1
000102030405060708090a0b0c0d0e0f:2
"#;
    let result = ApiKeyStore::from_str(contents);

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("duplicate"));
}

#[test]
fn test_from_str_error_line_number() {
    let contents = r#"
# Line 1 comment
000102030405060708090a0b0c0d0e0f:1
bad_line_here
"#;
    let result = ApiKeyStore::from_str(contents);

    assert!(result.is_err());
    let err = result.unwrap_err();
    // Error should be on line 4
    assert!(err.to_string().contains("line 4"));
}

// =============================================================================
// Reload tests
// =============================================================================

#[test]
fn test_reload_from_str() {
    let store = ApiKeyStore::new();

    // Initial load
    let contents1 = "000102030405060708090a0b0c0d0e0f:1";
    store.reload_from_str(contents1).unwrap();
    assert_eq!(store.len(), 1);

    // Reload with different keys
    let contents2 = r#"
deadbeefdeadbeefdeadbeefdeadbeef:2
cafebabecafebabecafebabecafebabe:3
"#;
    let count = store.reload_from_str(contents2).unwrap();
    assert_eq!(count, 2);
    assert_eq!(store.len(), 2);

    // Old key should be gone
    assert!(
        store
            .validate(&key_from_hex("000102030405060708090a0b0c0d0e0f"))
            .is_none()
    );

    // New keys should work
    assert_eq!(
        store.validate(&key_from_hex("deadbeefdeadbeefdeadbeefdeadbeef")),
        Some(WorkspaceId::new(2))
    );
}

#[test]
fn test_reload_atomic_on_error() {
    let store = ApiKeyStore::new();

    // Initial load
    let contents1 = "000102030405060708090a0b0c0d0e0f:1";
    store.reload_from_str(contents1).unwrap();

    // Try to reload with invalid content
    let contents2 = "invalid";
    let result = store.reload_from_str(contents2);
    assert!(result.is_err());

    // Original keys should still be there
    assert_eq!(store.len(), 1);
    assert_eq!(
        store.validate(&key_from_hex("000102030405060708090a0b0c0d0e0f")),
        Some(WorkspaceId::new(1))
    );
}

#[test]
fn test_reload_empty() {
    let store = ApiKeyStore::new();

    // Initial load
    store
        .reload_from_str("000102030405060708090a0b0c0d0e0f:1")
        .unwrap();
    assert_eq!(store.len(), 1);

    // Reload with empty
    store.reload_from_str("").unwrap();
    assert!(store.is_empty());
}

// =============================================================================
// Workspaces helper test
// =============================================================================

#[test]
fn test_workspaces() {
    let store = ApiKeyStore::new();

    store.insert([0x01u8; 16], WorkspaceId::new(1));
    store.insert([0x02u8; 16], WorkspaceId::new(2));
    store.insert([0x03u8; 16], WorkspaceId::new(1)); // Duplicate workspace, different key

    let workspaces = store.workspaces();
    assert_eq!(workspaces.len(), 3);
    assert!(workspaces.contains(&WorkspaceId::new(1)));
    assert!(workspaces.contains(&WorkspaceId::new(2)));
}

// =============================================================================
// Concurrency tests
// =============================================================================

#[test]
fn test_concurrent_validate() {
    use std::sync::Arc;
    use std::thread;

    let store = Arc::new(ApiKeyStore::new());
    let key = [0xAAu8; API_KEY_LENGTH];
    store.insert(key, WorkspaceId::new(42));

    let mut handles = vec![];

    for _ in 0..10 {
        let store = Arc::clone(&store);
        handles.push(thread::spawn(move || {
            for _ in 0..1000 {
                let result = store.validate(&key);
                assert_eq!(result, Some(WorkspaceId::new(42)));
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }
}

#[test]
fn test_concurrent_insert_validate() {
    use std::sync::Arc;
    use std::thread;

    let store = Arc::new(ApiKeyStore::new());
    let mut handles = vec![];

    // Writer threads
    for i in 0..5 {
        let store = Arc::clone(&store);
        handles.push(thread::spawn(move || {
            for j in 0..100 {
                let mut key = [0u8; API_KEY_LENGTH];
                key[0] = i as u8;
                key[1] = j as u8;
                store.insert(key, WorkspaceId::new((i * 100 + j) as u32));
            }
        }));
    }

    // Reader threads
    for _ in 0..5 {
        let store = Arc::clone(&store);
        handles.push(thread::spawn(move || {
            for _ in 0..1000 {
                let _ = store.len();
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Should have 500 keys total
    assert_eq!(store.len(), 500);
}

#[test]
fn test_concurrent_reload() {
    use std::sync::Arc;
    use std::thread;

    let store = Arc::new(ApiKeyStore::new());
    let key = key_from_hex("000102030405060708090a0b0c0d0e0f");

    let mut handles = vec![];

    // Reload thread
    let store_reload = Arc::clone(&store);
    handles.push(thread::spawn(move || {
        for i in 0..100 {
            let contents = format!("000102030405060708090a0b0c0d0e0f:{}", i);
            let _ = store_reload.reload_from_str(&contents);
        }
    }));

    // Reader threads
    for _ in 0..5 {
        let store = Arc::clone(&store);
        handles.push(thread::spawn(move || {
            for _ in 0..1000 {
                // Should never panic, might return None during reload
                let _ = store.validate(&key);
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }
}

// =============================================================================
// Edge case tests
// =============================================================================

#[test]
fn test_all_zeros_key() {
    let store = ApiKeyStore::new();
    let key = [0x00u8; API_KEY_LENGTH];
    store.insert(key, WorkspaceId::new(1));

    assert_eq!(store.validate(&key), Some(WorkspaceId::new(1)));
}

#[test]
fn test_all_ones_key() {
    let store = ApiKeyStore::new();
    let key = [0xFFu8; API_KEY_LENGTH];
    store.insert(key, WorkspaceId::new(1));

    assert_eq!(store.validate(&key), Some(WorkspaceId::new(1)));
}

#[test]
fn test_many_keys_same_workspace() {
    let store = ApiKeyStore::new();

    for i in 0..100u8 {
        let mut key = [0u8; API_KEY_LENGTH];
        key[0] = i;
        store.insert(key, WorkspaceId::new(1)); // All same workspace
    }

    assert_eq!(store.len(), 100);

    // All should map to same workspace
    for i in 0..100u8 {
        let mut key = [0u8; API_KEY_LENGTH];
        key[0] = i;
        assert_eq!(store.validate(&key), Some(WorkspaceId::new(1)));
    }
}

#[test]
fn test_many_workspaces() {
    let store = ApiKeyStore::new();

    for i in 0..1000u32 {
        let mut key = [0u8; API_KEY_LENGTH];
        key[0] = (i >> 8) as u8;
        key[1] = (i & 0xFF) as u8;
        store.insert(key, WorkspaceId::new(i));
    }

    assert_eq!(store.len(), 1000);

    // Verify all mappings
    for i in 0..1000u32 {
        let mut key = [0u8; API_KEY_LENGTH];
        key[0] = (i >> 8) as u8;
        key[1] = (i & 0xFF) as u8;
        assert_eq!(store.validate(&key), Some(WorkspaceId::new(i)));
    }
}

// =============================================================================
// Copy semantics tests (WorkspaceId is now Copy)
// =============================================================================

#[test]
fn test_workspace_id_is_copy() {
    let ws1 = WorkspaceId::new(42);
    let ws2 = ws1; // Copy
    let ws3 = ws1; // Can copy again because ws1 wasn't moved

    assert_eq!(ws1, ws2);
    assert_eq!(ws1, ws3);
    assert_eq!(ws2, ws3);
}

#[test]
fn test_workspace_id_in_option() {
    let opt: Option<WorkspaceId> = Some(WorkspaceId::new(42));

    // Can unwrap multiple times conceptually via pattern matching
    if let Some(ws) = opt {
        assert_eq!(ws.as_u32(), 42);
    }
    // opt is still valid because WorkspaceId is Copy
    assert!(opt.is_some());
}
