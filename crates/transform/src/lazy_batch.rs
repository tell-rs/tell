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
use cdp_protocol::Batch;
use std::sync::OnceLock;

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

/// Lazy batch wrapper with on-demand decoding
///
/// Wraps a `Batch` and caches decoded message content.
/// Multiple transformers can share the decoded data.
pub struct LazyBatch {
    /// The underlying batch (owned)
    batch: Batch,

    /// Cached decoded messages (decoded on first access)
    decoded: OnceLock<DecodedMessages>,
}

impl LazyBatch {
    /// Create a new lazy batch wrapper
    pub fn new(batch: Batch) -> Self {
        Self {
            batch,
            decoded: OnceLock::new(),
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

    /// Check if messages have been decoded
    #[inline]
    pub fn is_decoded(&self) -> bool {
        self.decoded.get().is_some()
    }

    /// Decode all messages in the batch
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
