//! Sink utilities for high-performance disk I/O
//!
//! This module provides reusable components for disk-based sinks:
//!
//! - **buffer_pool**: Pre-allocated buffer pool to reduce allocations
//! - **chain_writer**: Pluggable writers (plaintext, LZ4, binary)
//! - **atomic_rotation**: Lock-free file rotation with zero data loss
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

pub mod atomic_rotation;
pub mod buffer_pool;
pub mod chain_writer;
pub mod json;

pub use atomic_rotation::{
    AtomicRotationMetrics, AtomicRotationSink, BufferChain, ChainMetrics, FileContext,
    RotationConfig, RotationInterval, WriteRequest,
};
pub use buffer_pool::{BufferPool, BufferPoolMetrics};
pub use chain_writer::{
    BinaryWriter, ChainWrite, ChainWriter, DEFAULT_BUFFER_SIZE, Lz4Writer, PlainTextWriter,
};
pub use json::{extract_json_object, extract_json_string};
