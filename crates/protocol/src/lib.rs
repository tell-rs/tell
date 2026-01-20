//! Tell Protocol - Zero-copy core types for Tell
//!
//! This crate provides the foundational types that flow through the pipeline:
//! - `Batch` - Zero-copy batch container using `bytes::Bytes`
//! - `SchemaType` - Event, Log, Metric, Trace, etc.
//! - `BatchType` - Internal routing classification
//! - `SourceId` - Source identification for routing decisions
//! - `FlatBatch` - Zero-copy FlatBuffer wire format parser
//!
//! # Design Principles
//!
//! - **Zero-copy**: Uses `bytes::Bytes` for reference-counted buffer sharing
//! - **No allocations in hot path**: Pre-allocated vectors, slice access
//! - **Arc-friendly**: Batches can be wrapped in Arc for multi-sink fan-out
//!
//! # FlatBuffers Integration
//!
//! This crate parses FlatBuffers wire format directly without code generation.
//! The wire format is documented in `crates/protocol/schema/*.fbs`.

mod batch;
mod decode;
mod encode;
mod error;
mod flatbuf;
mod schema;
mod source;

pub use batch::{Batch, BatchBuilder};
pub use decode::{
    DecodedBucket, DecodedData, DecodedEvent, DecodedHistogram, DecodedIntLabel, DecodedLabel,
    DecodedLogEntry, DecodedMetric, DecodedSnapshot, DecodedSpan, EventType, LogEventType,
    LogLevel, MetricType, SpanKind, SpanStatus, Temporality, decode_event_data, decode_log_data,
    decode_metric_data, decode_snapshot_data, decode_trace_data,
};
pub use encode::{
    BatchEncoder, EncodedEvent, EncodedLogEntry, EventEncoder, EventTypeValue, LogEncoder,
    LogEventTypeValue, LogLevelValue,
};
pub use error::ProtocolError;
pub use flatbuf::{FlatBatch, is_likely_flatbuffer};
pub use schema::{BatchType, SchemaType};
pub use source::SourceId;

// Re-export bytes for convenience
pub use bytes::{Bytes, BytesMut};

/// Result type for protocol operations
pub type Result<T> = std::result::Result<T, ProtocolError>;

/// Default batch size (number of items before flush)
pub const DEFAULT_BATCH_SIZE: usize = 500;

/// Default buffer capacity in bytes (256KB)
pub const DEFAULT_BUFFER_CAPACITY: usize = 256 * 1024;

/// API key length in bytes
pub const API_KEY_LENGTH: usize = 16;

/// IPv6 address length in bytes
pub const IPV6_LENGTH: usize = 16;

/// Maximum reasonable message size (100MB)
/// Prevents memory exhaustion attacks while allowing large batches
pub const MAX_REASONABLE_SIZE: usize = 100 * 1024 * 1024;

/// Minimum valid FlatBuffer Batch size
/// Root offset (4) + vtable offset (4) + vtable header (4) + min fields (4) = 16 bytes
pub const MIN_BATCH_SIZE: usize = 16;

// Test modules - only compiled during testing
#[cfg(test)]
mod batch_test;
#[cfg(test)]
mod error_test;
#[cfg(test)]
mod flatbuf_test;
#[cfg(test)]
mod schema_test;
#[cfg(test)]
mod source_test;
