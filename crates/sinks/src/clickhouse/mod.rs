//! ClickHouse Sink - Full Analytics Database
//!
//! High-performance sink for inserting events and logs into ClickHouse.
//! Implements the full Tell v1.1 schema with 7 tables and smart event routing.
//!
//! # Features
//!
//! - **Event-type routing**: Routes events to appropriate tables based on EventType
//! - **IDENTIFY → 3 tables**: Parallel inserts to users, user_devices, user_traits
//! - **CONTEXT extraction**: Extracts device/location fields from payload
//! - **Pattern ID support**: Optional pattern_id for logs from transformer
//! - **Connector snapshots**: Generic snapshots for external data sources
//! - **Per-table batching**: Separate batches for optimal performance
//! - **Concurrent flush**: All tables flushed in parallel
//! - **Retry logic**: Exponential backoff on transient failures
//!
//! # Tables
//!
//! | Table | Purpose |
//! |-------|---------|
//! | events_v1 | TRACK events |
//! | users_v1 | IDENTIFY core identity |
//! | user_devices | IDENTIFY device links |
//! | user_traits | IDENTIFY key-value traits |
//! | context_v1 | CONTEXT device/location |
//! | logs_v1 | Log entries |
//! | snapshots_v1 | Connector snapshots |
//!
//! # Implementations
//!
//! Two implementations are available:
//! - **Row-based** (`ClickHouseSink`): Uses clickhouse crate with row structs
//! - **Arrow-based** (`arrow::ArrowClickHouseSink`): HTTP Arrow format for better performance

pub mod arrow;
mod config;
mod error;
mod helpers;
mod metrics;
mod sink;
pub mod tables;

// Re-export public API
pub use config::{
    ClickHouseConfig, TableNames, DEFAULT_BATCH_SIZE, DEFAULT_CONNECTION_TIMEOUT,
    DEFAULT_FLUSH_INTERVAL, DEFAULT_RETRY_ATTEMPTS,
};
pub use error::ClickHouseSinkError;
pub use metrics::{ClickHouseMetrics, ClickHouseSinkMetricsHandle, MetricsSnapshot};
pub use sink::ClickHouseSink;
pub use tables::{
    ContextRow, EventRow, LogRow, SnapshotRow, UserDeviceRow, UserRow, UserTraitRow,
};

// Arrow-based sink (recommended for high throughput)
pub use arrow::ArrowClickHouseSink;

// Re-export for crate-internal testing
#[cfg(test)]
pub(crate) use sink::TableBatches;

// ClickHouse-specific helpers (IP conversion, locale normalization)
pub use helpers::{extract_source_ip, generate_user_id_from_email, normalize_locale};

#[cfg(test)]
#[path = "clickhouse_test.rs"]
mod clickhouse_test;

#[cfg(test)]
#[path = "helpers_test.rs"]
mod helpers_test;
