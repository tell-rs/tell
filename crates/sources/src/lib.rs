//! CDP Collector - Sources
//!
//! Network sources that receive data and produce `Batch` instances for the pipeline.
//!
//! # Available Sources
//!
//! - **TCP** - High-performance TCP source with FlatBuffers protocol (primary)
//! - **TCP Debug** - TCP source with hex dump and detailed tracing (debugging only)
//! - **Syslog TCP** - RFC 3164/5424 syslog over TCP
//! - **Syslog UDP** - RFC 3164/5424 syslog over UDP with multi-worker support
//!
//! # Design Principles
//!
//! - **Zero-copy reads**: Use `bytes::BytesMut` for buffer management
//! - **Async I/O**: Built on `tokio` for non-blocking operations
//! - **Batch building**: Accumulate messages into batches for efficient processing
//! - **Source identification**: Each source has a `SourceId` for routing decisions
//! - **Separate debug type**: `TcpDebugSource` is a separate type to ensure zero overhead in production
//! - **Sharded channels**: Distribute load across multiple router workers to reduce contention
//!
//! # Example
//!
//! ```ignore
//! use cdp_sources::tcp::{TcpSource, TcpSourceConfig};
//! use cdp_auth::ApiKeyStore;
//! use std::sync::Arc;
//! use tokio::sync::mpsc;
//!
//! let config = TcpSourceConfig {
//!     address: "0.0.0.0".into(),
//!     port: 50000,
//!     ..Default::default()
//! };
//!
//! let auth_store = Arc::new(ApiKeyStore::new());
//! let (batch_tx, batch_rx) = mpsc::channel(1000);
//!
//! let source = TcpSource::new(config, auth_store, batch_tx);
//! source.run().await?;
//! ```

pub mod syslog;
pub mod tcp;
pub mod tcp_debug;
mod sharded_sender;

// Common types for sources
mod common;

pub use common::{ConnectionInfo, MetricsSnapshot, SourceConfig, SourceMetrics};
pub use sharded_sender::ShardedSender;
pub use tcp::{
    TcpMetricsSnapshot, TcpSource, TcpSourceConfig, TcpSourceError, TcpSourceMetrics,
    TcpSourceMetricsHandle,
};

// TCP Debug source (separate type for zero production overhead)
pub use tcp_debug::{TcpDebugSource, TcpDebugSourceConfig, TcpDebugSourceMetrics};

// Syslog sources
pub use syslog::{
    SyslogTcpMetricsHandle, SyslogTcpMetricsSnapshot, SyslogTcpSource, SyslogTcpSourceConfig,
    SyslogTcpSourceError, SyslogTcpSourceMetrics, SyslogUdpMetricsHandle, SyslogUdpMetricsSnapshot,
    SyslogUdpSource, SyslogUdpSourceConfig, SyslogUdpSourceError, SyslogUdpSourceMetrics,
};

// Test modules
#[cfg(test)]
mod tcp_test;
