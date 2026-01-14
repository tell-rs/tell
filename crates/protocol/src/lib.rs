//! CDP Protocol - Zero-copy core types for the CDP Collector
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
//! The wire format is documented in `../cdp-collector/pkg/schema/*.fbs`.

mod batch;
mod decode;
mod error;
mod flatbuf;
mod schema;
mod source;


pub use batch::{Batch, BatchBuilder};
pub use decode::{
    DecodedData, DecodedEvent, DecodedLogEntry, EventType, LogEventType, LogLevel,
    decode_event_data, decode_log_data,
};
pub use error::ProtocolError;
pub use flatbuf::FlatBatch;
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

// Test modules - only compiled during testing
#[cfg(test)]
mod batch_test;
#[cfg(test)]
mod decode_test;
#[cfg(test)]
mod error_test;
#[cfg(test)]
mod flatbuf_test;
#[cfg(test)]
mod schema_test;
#[cfg(test)]
mod source_test;
