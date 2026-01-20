//! Lazy Batch - Decode-on-demand batch wrapper
//!
//! `LazyBatch` wraps a `Batch` and provides lazy decoding of FlatBuffer
//! messages. This allows multiple transformers in a chain to share decoded
//! data without redundant parsing.
//!
//! # Design
//!
//! - **Decode once**: First transformer that needs decoded data pays the cost
//! - **Subsequent access**: O(1) lookup from cache
//! - **Zero overhead**: If no transformer decodes, no decoding happens
//!
//! # Example
//!
//! ```ignore
//! let lazy = LazyBatch::new(batch);
//!
//! // First access decodes
//! let messages = lazy.decoded_messages()?;
//!
//! // Subsequent access is cached
//! let messages_again = lazy.decoded_messages()?;
//! ```

use crate::TransformResult;
use std::sync::OnceLock;
use tell_protocol::{
    Batch, BatchType, FlatBatch, LogEventType, LogLevel, SchemaType, decode_log_data,
};

#[cfg(test)]
#[path = "lazy_batch_test.rs"]
mod tests;

/// Decoded message content from a batch
///
/// Contains the extracted text/content from FlatBuffer messages,
/// ready for pattern matching or other text-based transformations.
#[derive(Debug, Clone)]
pub struct DecodedMessages {
    /// Decoded message strings (UTF-8 or lossy converted)
    messages: Vec<String>,
}

impl DecodedMessages {
    /// Create empty decoded messages
    pub fn empty() -> Self {
        Self { messages: vec![] }
    }

    /// Create from a vector of decoded strings
    pub fn new(messages: Vec<String>) -> Self {
        Self { messages }
    }

    /// Get the number of decoded messages
    #[inline]
    pub fn len(&self) -> usize {
        self.messages.len()
    }

    /// Check if empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }

    /// Get a decoded message by index
    #[inline]
    pub fn get(&self, index: usize) -> Option<&str> {
        self.messages.get(index).map(|s| s.as_str())
    }

    /// Iterate over decoded messages
    pub fn iter(&self) -> impl Iterator<Item = &str> {
        self.messages.iter().map(|s| s.as_str())
    }
}

// =============================================================================
// Decoded Log Entries (Owned)
// =============================================================================

/// Owned version of a decoded log entry
///
/// Unlike `DecodedLogEntry` from the protocol crate which borrows from the
/// FlatBuffer, this version owns all data and can be cached across multiple
/// transformer accesses.
#[derive(Debug, Clone)]
pub struct OwnedLogEntry {
    /// Log event type
    pub event_type: LogEventType,
    /// Session ID (16 bytes UUID)
    pub session_id: Option<[u8; 16]>,
    /// Log severity level
    pub level: LogLevel,
    /// Timestamp in milliseconds since epoch
    pub timestamp: u64,
    /// Source hostname/instance
    pub source: Option<String>,
    /// Service/application name
    pub service: Option<String>,
    /// Payload bytes (usually JSON)
    pub payload: Vec<u8>,
}

impl OwnedLogEntry {
    /// Get payload as UTF-8 string (lossy)
    #[inline]
    pub fn payload_str(&self) -> std::borrow::Cow<'_, str> {
        String::from_utf8_lossy(&self.payload)
    }

    /// Try to get payload as UTF-8 string
    #[inline]
    pub fn payload_utf8(&self) -> Option<&str> {
        std::str::from_utf8(&self.payload).ok()
    }
}

/// Collection of decoded log entries from a batch
#[derive(Debug, Clone)]
pub struct DecodedLogs {
    /// Decoded log entries
    entries: Vec<OwnedLogEntry>,
}

impl DecodedLogs {
    /// Create empty decoded logs
    pub fn empty() -> Self {
        Self { entries: vec![] }
    }

    /// Create from a vector of owned log entries
    pub fn new(entries: Vec<OwnedLogEntry>) -> Self {
        Self { entries }
    }

    /// Get the number of decoded entries
    #[inline]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Get a decoded entry by index
    #[inline]
    pub fn get(&self, index: usize) -> Option<&OwnedLogEntry> {
        self.entries.get(index)
    }

    /// Iterate over decoded entries
    pub fn iter(&self) -> impl Iterator<Item = &OwnedLogEntry> {
        self.entries.iter()
    }
}

/// Lazy batch wrapper with on-demand decoding
///
/// Wraps a `Batch` and caches decoded message content.
/// Multiple transformers can share the decoded data.
///
/// # Caching Levels
///
/// - **Raw messages**: Basic UTF-8 decoding of message bytes
/// - **Structured logs**: Full FlatBuffer log entry decoding (service, level, etc.)
///
/// Each level is decoded on first access and cached for subsequent use.
pub struct LazyBatch {
    /// The underlying batch (owned)
    batch: Batch,

    /// Cached decoded messages (basic UTF-8, decoded on first access)
    decoded: OnceLock<DecodedMessages>,

    /// Cached decoded log entries (FlatBuffer decoded, on first access)
    decoded_logs: OnceLock<DecodedLogs>,
}

impl LazyBatch {
    /// Create a new lazy batch wrapper
    pub fn new(batch: Batch) -> Self {
        Self {
            batch,
            decoded: OnceLock::new(),
            decoded_logs: OnceLock::new(),
        }
    }

    /// Get the underlying batch reference
    #[inline]
    pub fn batch(&self) -> &Batch {
        &self.batch
    }

    /// Consume and return the underlying batch
    ///
    /// Use this when transformation is complete and you need the batch back.
    pub fn into_batch(self) -> Batch {
        self.batch
    }

    /// Get decoded messages, decoding on first access
    ///
    /// # Decoding Strategy
    ///
    /// Messages are decoded as UTF-8. Invalid UTF-8 sequences are replaced
    /// with the Unicode replacement character (U+FFFD).
    ///
    /// # Performance
    ///
    /// First call decodes all messages (~O(n) where n = total message bytes).
    /// Subsequent calls return cached result in O(1).
    pub fn decoded_messages(&self) -> TransformResult<&DecodedMessages> {
        // Use get_or_init since decoding cannot fail (we use lossy UTF-8)
        Ok(self.decoded.get_or_init(|| self.decode_all_messages()))
    }

    /// Check if messages have been decoded (basic UTF-8)
    #[inline]
    pub fn is_decoded(&self) -> bool {
        self.decoded.get().is_some()
    }

    /// Check if log entries have been decoded (FlatBuffer structured)
    #[inline]
    pub fn is_logs_decoded(&self) -> bool {
        self.decoded_logs.get().is_some()
    }

    /// Get decoded log entries, decoding on first access
    ///
    /// This provides structured access to log data including service name,
    /// log level, timestamp, and payload. Only works for Log batches.
    ///
    /// # Returns
    ///
    /// - `Ok(&DecodedLogs)` - Decoded log entries (may be empty for non-log batches)
    /// - Decoding errors are logged but return empty collection
    ///
    /// # Performance
    ///
    /// First call decodes all log entries from FlatBuffers.
    /// Subsequent calls return cached result in O(1).
    pub fn decoded_logs(&self) -> TransformResult<&DecodedLogs> {
        Ok(self.decoded_logs.get_or_init(|| self.decode_all_logs()))
    }

    /// Decode all log entries from FlatBuffer messages
    fn decode_all_logs(&self) -> DecodedLogs {
        // Only decode log batches
        if self.batch.batch_type() != BatchType::Log {
            return DecodedLogs::empty();
        }

        let mut entries = Vec::with_capacity(self.batch.message_count());

        for i in 0..self.batch.message_count() {
            if let Some(raw) = self.batch.get_message(i) {
                if let Some(entry) = self.decode_log_message(raw) {
                    entries.push(entry);
                }
            }
        }

        DecodedLogs::new(entries)
    }

    /// Decode a single log message from FlatBuffer
    fn decode_log_message(&self, raw: &[u8]) -> Option<OwnedLogEntry> {
        // Parse FlatBuffer wrapper
        let flat_batch = FlatBatch::parse(raw).ok()?;

        // Verify it's a log schema
        if flat_batch.schema_type() != SchemaType::Log {
            return None;
        }

        // Get the data payload
        let data = flat_batch.data().ok()?;

        // Decode log entries
        let logs = decode_log_data(data).ok()?;

        // Take first log entry (typically one per message)
        let log = logs.into_iter().next()?;

        Some(OwnedLogEntry {
            event_type: log.event_type,
            session_id: log.session_id.copied(),
            level: log.level,
            timestamp: log.timestamp,
            source: log.source.map(|s| s.to_string()),
            service: log.service.map(|s| s.to_string()),
            payload: log.payload.to_vec(),
        })
    }

    /// Decode all messages in the batch (basic UTF-8)
    fn decode_all_messages(&self) -> DecodedMessages {
        let mut messages = Vec::with_capacity(self.batch.message_count());

        for i in 0..self.batch.message_count() {
            let decoded = if let Some(raw) = self.batch.get_message(i) {
                // Decode as UTF-8, replacing invalid sequences
                String::from_utf8_lossy(raw).into_owned()
            } else {
                String::new() // Empty for missing messages
            };
            messages.push(decoded);
        }

        DecodedMessages::new(messages)
    }
}

/// Extension trait for converting Batch to LazyBatch
pub trait IntoLazyBatch {
    /// Convert into a lazy batch wrapper
    fn into_lazy(self) -> LazyBatch;
}

impl IntoLazyBatch for Batch {
    fn into_lazy(self) -> LazyBatch {
        LazyBatch::new(self)
    }
}
