//! Decoder for nested FlatBuffer payloads
//!
//! This module provides zero-copy parsing of the `data` field from `FlatBatch`,
//! which contains either `EventData` (array of events) or `LogData` (array of logs).
//!
//! # Module Structure
//!
//! - `event` - Event type definitions and EventData parsing
//! - `log` - Log type definitions and LogData parsing
//! - `table` - Internal FlatBuffer table parsing helpers
//!
//! # Usage
//!
//! ```ignore
//! use tell_protocol::{decode_event_data, decode_log_data, FlatBatch};
//!
//! let flat_batch = FlatBatch::parse(msg)?;
//! let data = flat_batch.data()?;
//!
//! match flat_batch.schema_type() {
//!     SchemaType::Event => {
//!         let events = decode_event_data(data)?;
//!         for event in events {
//!             println!("{}: {}", event.event_name.unwrap_or("-"), event.timestamp);
//!         }
//!     }
//!     SchemaType::Log => {
//!         let logs = decode_log_data(data)?;
//!         for log in logs {
//!             println!("[{}] {}", log.level, log.service.unwrap_or("-"));
//!         }
//!     }
//!     _ => {}
//! }
//! ```

mod event;
mod log;
mod metric;
mod snapshot;
mod table;
mod trace;

// Re-export public types and functions
pub use event::{EventType, DecodedEvent, decode_event_data};
pub use log::{LogEventType, LogLevel, DecodedLogEntry, decode_log_data};
pub use metric::{
    MetricType, Temporality, DecodedLabel, DecodedIntLabel, DecodedBucket, DecodedHistogram,
    DecodedMetric, decode_metric_data,
};
pub use snapshot::{DecodedSnapshot, decode_snapshot_data};
pub use trace::{SpanKind, SpanStatus, DecodedSpan, decode_trace_data};

// =============================================================================
// Decoded Batch Result
// =============================================================================

/// Result of decoding a FlatBuffer data payload
#[derive(Debug)]
pub enum DecodedData<'a> {
    /// Contains decoded events
    Events(Vec<DecodedEvent<'a>>),
    /// Contains decoded log entries
    Logs(Vec<DecodedLogEntry<'a>>),
}

impl<'a> DecodedData<'a> {
    /// Get as events, if this is an Events variant
    pub fn as_events(&self) -> Option<&Vec<DecodedEvent<'a>>> {
        match self {
            DecodedData::Events(e) => Some(e),
            _ => None,
        }
    }

    /// Get as logs, if this is a Logs variant
    pub fn as_logs(&self) -> Option<&Vec<DecodedLogEntry<'a>>> {
        match self {
            DecodedData::Logs(l) => Some(l),
            _ => None,
        }
    }

    /// Get the number of items in the decoded data
    pub fn len(&self) -> usize {
        match self {
            DecodedData::Events(e) => e.len(),
            DecodedData::Logs(l) => l.len(),
        }
    }

    /// Check if the decoded data is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
#[path = "decode_test.rs"]
mod decode_test;
