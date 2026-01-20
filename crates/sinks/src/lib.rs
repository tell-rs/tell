//! Tell - Sinks
//!
//! High-performance output sinks for Tell with 40M+ events/sec throughput.
//!
//! # Architecture
//!
//! Each sink receives `Arc<Batch>` instances via tokio channels and writes to
//! its destination. The atomic buffer chain pattern ensures zero data loss
//! during file rotation.
//!
//! ```text
//! [Router] --Arc<Batch>--> [Sink Channel] --> [Sink Task] --> [Destination]
//! ```
//!
//! # Available Sinks
//!
//! | Sink | Purpose | Uses Rotation |
//! |------|---------|---------------|
//! | `null` | Benchmarking (discard all) | No |
//! | `stdout` | Debug output | No |
//! | `disk_binary` | Binary storage with metadata | Yes |
//! | `disk_plaintext` | Human-readable logs | Yes |
//! | `clickhouse` | Analytics database | No |
//! | `parquet` | Columnar storage (cold data) | Yes |
//! | `arrow_ipc` | Fast columnar (hot data) | Yes |
//! | `forwarder` | Tell-to-Tell | No |
//!
//! # Example
//!
//! ```ignore
//! use sinks::null::NullSink;
//! use pipeline::{Router, SinkHandle, SinkId};
//! use tokio::sync::mpsc;
//!
//! // Create channel and sink
//! let (tx, rx) = mpsc::channel(1000);
//! let sink = NullSink::new(rx);
//!
//! // Register sender with router
//! router.register_sink(SinkHandle::new(SinkId::new(0), "null", tx));
//!
//! // Run sink (typically spawned as a task)
//! tokio::spawn(sink.run());
//! ```

// =============================================================================
// Sink implementations (each in its own submodule)
// =============================================================================

/// Null sink - discards all data (for benchmarking)
pub mod null;

/// Stdout sink - human-readable debug output
pub mod stdout;

/// Disk binary sink - high-performance binary storage with metadata headers
pub mod disk_binary;

/// Disk plaintext sink - human-readable text logs
pub mod disk_plaintext;

/// ClickHouse sink - real-time analytics database
pub mod clickhouse;

/// Parquet sink - columnar storage for data warehousing
pub mod parquet;

/// Arrow IPC sink - fast columnar storage for hot data
pub mod arrow_ipc;

/// Forwarder sink - Tell-to-Tell forwarding
pub mod forwarder;

// =============================================================================
// Shared utilities
// =============================================================================

/// Shared utilities for disk-based sinks (buffer pool, rotation, writers)
pub mod util;

/// Common types shared by all sinks (errors, metrics, config)
mod common;

// =============================================================================
// Public re-exports
// =============================================================================

pub use common::{MetricsSnapshot, SinkConfig, SinkError, SinkMetrics};

// Re-export main sink types for convenience
pub use arrow_ipc::{ArrowIpcSink, ArrowIpcSinkMetricsHandle};
pub use clickhouse::{ClickHouseSink, ClickHouseSinkMetricsHandle};
pub use disk_binary::{DiskBinaryConfig, DiskBinarySink, DiskBinarySinkMetricsHandle};
pub use disk_plaintext::{DiskPlaintextSink, DiskPlaintextSinkMetricsHandle};
pub use forwarder::{ForwarderSink, ForwarderSinkMetricsHandle};
pub use null::{NullSink, NullSinkConfig, NullSinkMetricsHandle};
pub use parquet::{ParquetSink, ParquetSinkMetricsHandle};
pub use stdout::{StdoutSink, StdoutSinkMetricsHandle};

// Tests are registered in their respective modules via #[cfg(test)]
// See: common.rs, null/mod.rs, stdout/mod.rs, util/buffer_pool.rs, etc.
