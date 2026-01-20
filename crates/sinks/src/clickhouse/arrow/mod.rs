//! Arrow-based ClickHouse sink
//!
//! High-performance sink that sends Arrow RecordBatches directly to ClickHouse
//! via HTTP `FORMAT Arrow`. This eliminates intermediate row structs and enables
//! zero-copy transfer of UUIDs from FlatBuffer bytes to ClickHouse.
//!
//! # Performance
//!
//! - UUIDs: FixedSizeBinary(16) maps directly to ClickHouse UUID
//! - Columnar: Arrow batches match ClickHouse's internal columnar storage
//! - HTTP streaming: Single request per batch, no row-by-row parsing
//!
//! # Usage
//!
//! ```ignore
//! use sinks::clickhouse::arrow::ArrowClickHouseSink;
//!
//! let sink = ArrowClickHouseSink::new(config, receiver);
//! sink.run().await?;
//! ```

mod builders;
mod schema;
mod sink;

pub use builders::{
    ContextBuilder, EventsBuilder, LogsBuilder, SnapshotsBuilder, UserDevicesBuilder,
    UserTraitsBuilder, UsersBuilder,
};
pub use schema::{
    context_schema, events_schema, logs_schema, snapshots_schema, user_devices_schema,
    user_traits_schema, users_schema,
};
pub use sink::ArrowClickHouseSink;
