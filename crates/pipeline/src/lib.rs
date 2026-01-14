//! CDP Collector - Pipeline
//!
//! The async router that connects sources to sinks via channels.
//!
//! # Architecture
//!
//! ```text
//! [Sources]                    [Router]                    [Sinks]
//!    TCP ────┐                                          ┌──→ ClickHouse
//!    Syslog ─┼──→ mpsc::Receiver ──→ [Transform] ──→ RoutingTable ──→ Arc<Batch> ──→ Disk
//!    UDP ────┘                         Chain               O(1)       └──→ Parquet
//! ```
//!
//! # Key Design
//!
//! - **Channel-based**: Uses `tokio::sync::mpsc` for async communication
//! - **Arc fan-out**: Batches wrapped in `Arc` for zero-copy multi-sink delivery
//! - **Backpressure**: `try_send` for non-blocking sends with overflow handling
//! - **Pre-compiled routing**: `RoutingTable` lookup is O(1), no allocation
//! - **O(1) sink lookup**: Sinks stored in `Vec` indexed by `SinkId` (u16)
//! - **Optional transforms**: Transformer chain applied before routing
//!
//! # Example
//!
//! ```ignore
//! use pipeline::{Router, SinkHandle};
//! use cdp_routing::{RoutingTable, SinkId};
//! use tokio::sync::mpsc;
//!
//! // Build routing table
//! let mut routing_table = RoutingTable::new();
//! let clickhouse_id = routing_table.register_sink("clickhouse");
//! routing_table.set_default(vec![clickhouse_id]);
//!
//! // Create router
//! let mut router = Router::new(routing_table);
//!
//! // Register sink channels
//! let (sink_tx, sink_rx) = mpsc::channel(1000);
//! router.register_sink(SinkHandle::new(clickhouse_id, "clickhouse", sink_tx));
//!
//! // Create source channel
//! let (source_tx, source_rx) = mpsc::channel(1000);
//!
//! // Run router (typically spawned as a task)
//! tokio::spawn(router.run(source_rx));
//!
//! // Sources send batches to source_tx
//! // Sinks receive Arc<Batch> from sink_rx
//! ```

mod error;
mod metrics;
mod router;
mod sink_handle;

pub use error::{PipelineError, Result};
pub use metrics::{MetricsSnapshot, RouterMetrics};
pub use router::{Router, RouterMetricsHandle};
pub use sink_handle::SinkHandle;

// Re-export key types from dependencies for convenience
pub use cdp_protocol::Batch;
pub use cdp_routing::{RoutingTable, SinkId, SourceId};
pub use cdp_transform::Chain as TransformChain;

/// Default channel buffer size for sink channels
pub const DEFAULT_CHANNEL_SIZE: usize = 1000;

/// Default channel buffer size for source input
pub const DEFAULT_SOURCE_CHANNEL_SIZE: usize = 10000;

#[cfg(test)]
mod router_test;
