//! FlatBuffer encoding for building wire-format messages
//!
//! This module provides functions to build FlatBuffer messages from structured data.
//! Used by the HTTP source to convert JSON payloads to FlatBuffer format.
//!
//! # Wire Format Layout
//!
//! FlatBuffers use a forward layout where:
//! 1. Root offset points to the main table
//! 2. VTables come before their tables
//! 3. Tables contain inline scalars and offsets to vectors
//! 4. Vectors come after the tables they belong to
//!
//! # Usage
//!
//! ```ignore
//! use tell_protocol::encode::{BatchEncoder, EventEncoder};
//!
//! let mut encoder = BatchEncoder::new();
//! encoder.set_api_key(&api_key);
//! encoder.set_schema_type(SchemaType::Event);
//!
//! let event_data = EventEncoder::encode_events(&events);
//! let batch_bytes = encoder.encode(&event_data);
//! ```

mod batch;
mod event;
mod log;

pub use batch::BatchEncoder;
pub use event::{EncodedEvent, EventEncoder, EventTypeValue};
pub use log::{EncodedLogEntry, LogEncoder, LogEventTypeValue, LogLevelValue};

/// Write a u16 in little-endian format
#[inline]
fn write_u16(buf: &mut Vec<u8>, value: u16) {
    buf.extend_from_slice(&value.to_le_bytes());
}

/// Write a u32 in little-endian format
#[inline]
fn write_u32(buf: &mut Vec<u8>, value: u32) {
    buf.extend_from_slice(&value.to_le_bytes());
}

/// Write a u64 in little-endian format
#[inline]
fn write_u64(buf: &mut Vec<u8>, value: u64) {
    buf.extend_from_slice(&value.to_le_bytes());
}

/// Write an i32 in little-endian format
#[inline]
fn write_i32(buf: &mut Vec<u8>, value: i32) {
    buf.extend_from_slice(&value.to_le_bytes());
}
