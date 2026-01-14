//! Batch module - outer FlatBuffer wrapper
//!
//! Builds `common.fbs::Batch` messages that wrap schema-specific payloads.
//! The `data` field contains the inner FlatBuffer (EventData, LogData, etc.).

mod builder;

#[cfg(test)]
mod builder_test;

pub use builder::{BatchBuilder, BuiltBatch};
