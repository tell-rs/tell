//! Tests for connector snapshot table row types

use super::snapshots::SnapshotRow;

#[test]
fn test_snapshot_row_creation() {
    let row = SnapshotRow {
        timestamp: 1700000000000,
        connector: "github".to_string(),
        entity: "rust-lang/rust".to_string(),
        metrics: r#"{"stars": 85000, "forks": 11000}"#.to_string(),
    };

    assert_eq!(row.connector, "github");
    assert_eq!(row.entity, "rust-lang/rust");
    assert!(row.metrics.contains("stars"));
}

#[test]
fn test_snapshot_row_shopify() {
    let row = SnapshotRow {
        timestamp: 1700000000000,
        connector: "shopify".to_string(),
        entity: "mystore.myshopify.com".to_string(),
        metrics: r#"{"orders": 150, "revenue": 12500.50}"#.to_string(),
    };

    assert_eq!(row.connector, "shopify");
    assert_eq!(row.entity, "mystore.myshopify.com");
}

#[test]
fn test_snapshot_row_stripe() {
    let row = SnapshotRow {
        timestamp: 1700000000000,
        connector: "stripe".to_string(),
        entity: "acct_123456".to_string(),
        metrics: r#"{"mrr": 5000, "active_subscriptions": 42}"#.to_string(),
    };

    assert_eq!(row.connector, "stripe");
    assert_eq!(row.entity, "acct_123456");
}
