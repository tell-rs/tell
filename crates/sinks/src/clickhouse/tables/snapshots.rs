//! Connector snapshot table row types (snapshots_v1)

use clickhouse::Row;
use serde::Serialize;

/// Snapshot row for connector data (snapshots_v1 table)
///
/// Used by connectors (GitHub, Shopify, Stripe, etc.) to store periodic
/// metric snapshots from external APIs.
///
/// ```sql
/// CREATE TABLE snapshots_v1 (
///     timestamp DateTime64(3),
///     connector LowCardinality(String),
///     entity String,
///     metrics JSON
/// ) ENGINE = MergeTree()
/// PARTITION BY toYYYYMM(timestamp)
/// ORDER BY (timestamp, connector, entity)
/// TTL timestamp + INTERVAL 5 YEAR
/// COMMENT 'Connector snapshots - periodic metrics from external sources';
/// ```
#[derive(Debug, Clone, Row, Serialize)]
pub struct SnapshotRow {
    /// Snapshot timestamp in milliseconds
    pub timestamp: i64,

    /// Connector type (e.g., "github", "shopify", "stripe")
    pub connector: String,

    /// Entity identifier (e.g., "rust-lang/rust", "mystore.myshopify.com")
    pub entity: String,

    /// Metrics as JSON string (native JSON type in ClickHouse)
    pub metrics: String,
}
