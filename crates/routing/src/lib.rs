//! CDP Collector - Routing
//!
//! Pre-compiled routing tables for O(1) source→sinks lookup.
//! Zero-copy design: all allocations happen at compile time, not per-batch.
//!
//! # Design
//!
//! Routing decisions are made at config load time, not per-message.
//! The `RoutingTable` stores pre-computed mappings from source IDs to sink IDs,
//! enabling constant-time lookups in the hot path.
//!
//! # Zero-Copy Guarantees
//!
//! - `SinkId` is `Copy` - no heap allocation, fits in register
//! - `route()` returns `&[SinkId]` - slice into pre-allocated storage
//! - No allocations in the hot path
//! - No string operations during routing
//!
//! # Example
//!
//! ```
//! use cdp_routing::{RoutingTable, SinkId};
//! use cdp_protocol::SourceId;
//!
//! // At startup: compile routing table from config
//! let mut table = RoutingTable::new();
//! table.set_default(vec![SinkId::new(0)]);
//! table.add_route(SourceId::new("tcp"), vec![SinkId::new(1), SinkId::new(2)]);
//!
//! // Hot path: O(1) lookup, returns slice (zero-copy)
//! let sinks: &[SinkId] = table.route(&SourceId::new("tcp"));
//! assert_eq!(sinks.len(), 2);
//! ```

mod error;
mod sink_id;
mod table;

#[cfg(test)]
mod table_test;

pub use error::{Result, RoutingError};
pub use sink_id::SinkId;
pub use table::{RoutingTable, RoutingTableBuilder};

// Re-export SourceId for convenience
pub use cdp_protocol::SourceId;
