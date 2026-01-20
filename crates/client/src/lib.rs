//! Tell Client Library
//!
//! This crate provides builders for constructing Tell FlatBuffer messages.
//! It is primarily used for:
//!
//! - **Testing**: Creating valid batches for pipeline benchmarks and integration tests
//! - **SDK development**: Reference implementation for client libraries
//!
//! # Architecture
//!
//! The library is organized into domain-specific modules:
//!
//! - [`batch`] - Outer FlatBuffer wrapper (Batch)
//! - [`event`] - Analytics events (Track, Identify, Group, etc.)
//! - [`log`] - Structured log entries (RFC 5424 levels)
//!
//! # Quick Start
//!
//! ## Building Events
//!
//! ```
//! use tell_client::{BatchBuilder, SchemaType};
//! use tell_client::event::{EventBuilder, EventDataBuilder, EventType};
//!
//! // Build an event
//! let event = EventBuilder::new()
//!     .track("page_view")
//!     .device_id([0x01; 16])
//!     .timestamp_now()
//!     .payload_json(r#"{"page": "/home"}"#)
//!     .build()
//!     .unwrap();
//!
//! // Batch multiple events
//! let event_data = EventDataBuilder::new()
//!     .add(event)
//!     .build()
//!     .unwrap();
//!
//! // Wrap in a Batch for sending
//! let batch = BatchBuilder::new()
//!     .api_key([0x01; 16])
//!     .event_data(event_data)
//!     .build()
//!     .unwrap();
//! ```
//!
//! ## Building Logs
//!
//! ```
//! use tell_client::{BatchBuilder, SchemaType};
//! use tell_client::log::{LogEntryBuilder, LogDataBuilder, LogLevel};
//!
//! // Build a log entry
//! let log = LogEntryBuilder::new()
//!     .error()
//!     .source("web-01.prod")
//!     .service("api-gateway")
//!     .timestamp_now()
//!     .payload_json(r#"{"error": "connection timeout"}"#)
//!     .build()
//!     .unwrap();
//!
//! // Batch multiple logs
//! let log_data = LogDataBuilder::new()
//!     .add(log)
//!     .build()
//!     .unwrap();
//!
//! // Wrap in a Batch for sending
//! let batch = BatchBuilder::new()
//!     .api_key([0x01; 16])
//!     .log_data(log_data)
//!     .build()
//!     .unwrap();
//! ```
//!
//! # Wire Format
//!
//! This crate builds valid FlatBuffer messages matching the schemas:
//!
//! - `common.fbs` - Batch wrapper
//! - `event.fbs` - Event and EventData
//! - `log.fbs` - LogEntry and LogData

mod error;

pub mod batch;
pub mod event;
pub mod log;
pub mod test;

// Re-export main types at crate root for convenience
pub use batch::{BatchBuilder, BuiltBatch};
pub use error::{BuilderError, Result};

// Re-export protocol types
pub use tell_protocol::SchemaType;
