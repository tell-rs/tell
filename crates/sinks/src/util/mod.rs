//! Sink utilities for high-performance disk I/O
//!
//! This module provides reusable components for disk-based sinks:
//!
//! - **buffer_pool**: Pre-allocated buffer pool to reduce allocations
//! - **chain_writer**: Pluggable writers (plaintext, LZ4, binary)
//! - **atomic_rotation**: Lock-free file rotation with zero data loss
//! - **arrow_rows**: Shared Arrow schemas and row types for columnar sinks
//!
//! # Architecture
//!
//! The atomic rotation system ensures 40M+ events/sec throughput by:
//!
//! 1. **Lock-free hot path**: `ArcSwap` for atomic chain switching
//! 2. **Background draining**: Old chains drain via Arc refcount
//! 3. **Buffer pooling**: Reuse `BytesMut` buffers, no allocation per batch
//! 4. **Big buffers**: 32MB write buffers minimize syscalls
//!
//! ```text
//! [Batch] → [Buffer Pool] → [Serialize] → [Active Chain] → [Disk]
//!                                              ↓ (rotation)
//!                                         [New Chain]
//!                                              ↓
//!                                    [Old Chain drains via Arc]
//! ```

pub mod arrow_rows;
pub mod atomic_rotation;
pub mod buffer_pool;
pub mod chain_writer;
pub mod json;
pub mod rate_limited_logger;

pub use atomic_rotation::{
    AtomicRotationMetrics, AtomicRotationSink, BufferChain, ChainMetrics, FileContext,
    RotationConfig, RotationInterval, WriteRequest, DEFAULT_RETRY_DELAY, DEFAULT_WRITE_RETRIES,
};
pub use buffer_pool::{BufferPool, BufferPoolMetrics};
pub use chain_writer::{
    BinaryWriter, ChainWrite, ChainWriter, DEFAULT_BUFFER_SIZE, Lz4BlockSize, Lz4Config,
    Lz4Writer, PlainTextWriter,
};
pub use json::{extract_json_object, extract_json_string};
pub use rate_limited_logger::{
    RateLimitedLogger, DEFAULT_LOG_INTERVAL, MAX_DATA_LOG_LENGTH,
};
pub use arrow_rows::{
    EventRow, LogRow, SnapshotRow, event_schema, log_schema, snapshot_schema,
    events_to_record_batch, logs_to_record_batch, snapshots_to_record_batch,
};
