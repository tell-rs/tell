//! Tests for user table row types

use super::users::{UserDeviceRow, UserRow, UserTraitRow};

#[test]
fn test_user_row_creation() {
    let row = UserRow {
        user_id: "abc-123".to_string(),
        email: "user@example.com".to_string(),
        name: "Test User".to_string(),
        updated_at: 1700000000000,
    };

    assert_eq!(row.user_id, "abc-123");
    assert_eq!(row.email, "user@example.com");
    assert_eq!(row.name, "Test User");
}

#[test]
fn test_user_device_row_creation() {
    let row = UserDeviceRow {
        user_id: "abc-123".to_string(),
        device_id: [0x03; 16],
        linked_at: 1700000000000,
    };

    assert_eq!(row.user_id, "abc-123");
    assert_eq!(row.device_id, [0x03; 16]);
}

#[test]
fn test_user_trait_row_creation() {
    let row = UserTraitRow {
        user_id: "abc-123".to_string(),
        trait_key: "plan".to_string(),
        trait_value: "premium".to_string(),
        updated_at: 1700000000000,
    };

    assert_eq!(row.user_id, "abc-123");
    assert_eq!(row.trait_key, "plan");
    assert_eq!(row.trait_value, "premium");
}
