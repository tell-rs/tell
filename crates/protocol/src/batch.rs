//! Batch - Zero-copy batch container for Tell
//!
//! The `Batch` struct is the core data type that flows through the pipeline.
//! It uses `bytes::Bytes` for reference-counted buffer sharing, enabling
//! zero-copy fan-out to multiple sinks.

use bytes::Bytes;
use std::net::IpAddr;

use crate::schema::BatchType;
use crate::source::SourceId;
use crate::{DEFAULT_BATCH_SIZE, DEFAULT_BUFFER_CAPACITY};

/// Zero-copy batch container
///
/// # Design
///
/// - `buffer` uses `bytes::Bytes` which is reference-counted
/// - Cloning a `Batch` is O(1) - just increments the reference count
/// - Multiple sinks can hold references to the same underlying data
/// - `offsets` and `lengths` allow zero-copy access to individual messages
///
/// # Memory Layout
///
/// ```text
/// buffer: [msg0 bytes][msg1 bytes][msg2 bytes]...
/// offsets: [0, len0, len0+len1, ...]
/// lengths: [len0, len1, len2, ...]
/// ```
#[derive(Debug, Clone)]
pub struct Batch {
    /// Raw FlatBuffer messages - zero-copy via Bytes
    buffer: Bytes,

    /// Offsets into buffer for each message
    offsets: Vec<u32>,

    /// Lengths of each message
    lengths: Vec<u32>,

    /// Number of events/logs in this batch (may differ from message count)
    count: usize,

    /// Batch type for internal routing
    batch_type: BatchType,

    /// Workspace ID (extracted from API key validation)
    workspace_id: u32,

    /// Source IP for enrichment
    source_ip: IpAddr,

    /// Source identifier for routing decisions
    source_id: SourceId,

    /// Optional pattern IDs from transformer (parallel to messages)
    pattern_ids: Option<Vec<u64>>,
}

impl Batch {
    /// Get the raw buffer
    #[inline]
    pub fn buffer(&self) -> &Bytes {
        &self.buffer
    }

    /// Get a message slice by index - zero allocation
    ///
    /// Returns `None` if index is out of bounds.
    #[inline]
    pub fn get_message(&self, index: usize) -> Option<&[u8]> {
        if index >= self.offsets.len() {
            return None;
        }
        let start = self.offsets[index] as usize;
        let len = self.lengths[index] as usize;
        Some(&self.buffer[start..start + len])
    }

    /// Get message count (number of FlatBuffer messages, not event count)
    #[inline]
    pub fn message_count(&self) -> usize {
        self.offsets.len()
    }

    /// Get item count (number of events/logs in this batch)
    #[inline]
    pub fn count(&self) -> usize {
        self.count
    }

    /// Check if batch is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Get the batch type
    #[inline]
    pub fn batch_type(&self) -> BatchType {
        self.batch_type
    }

    /// Get the workspace ID
    #[inline]
    pub fn workspace_id(&self) -> u32 {
        self.workspace_id
    }

    /// Get the source IP
    #[inline]
    pub fn source_ip(&self) -> IpAddr {
        self.source_ip
    }

    /// Get the source ID
    #[inline]
    pub fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Get pattern IDs if set by transformer
    #[inline]
    pub fn pattern_ids(&self) -> Option<&[u64]> {
        self.pattern_ids.as_deref()
    }

    /// Set pattern IDs (called by transformer)
    #[inline]
    pub fn set_pattern_ids(&mut self, ids: Vec<u64>) {
        self.pattern_ids = Some(ids);
    }

    /// Get offsets slice for iteration
    #[inline]
    pub fn offsets(&self) -> &[u32] {
        &self.offsets
    }

    /// Get lengths slice for iteration
    #[inline]
    pub fn lengths(&self) -> &[u32] {
        &self.lengths
    }

    /// Get total bytes in buffer
    #[inline]
    pub fn total_bytes(&self) -> usize {
        self.buffer.len()
    }

    /// Iterate over all messages in the batch
    #[inline]
    pub fn messages(&self) -> impl Iterator<Item = &[u8]> {
        self.offsets
            .iter()
            .zip(self.lengths.iter())
            .map(move |(&offset, &len)| {
                let start = offset as usize;
                let end = start + len as usize;
                &self.buffer[start..end]
            })
    }
}

/// Builder for constructing batches incrementally
///
/// Used by sources to accumulate messages before sending to the pipeline.
pub struct BatchBuilder {
    /// Growing buffer for message data
    buffer: Vec<u8>,

    /// Offsets into buffer for each message
    offsets: Vec<u32>,

    /// Lengths of each message
    lengths: Vec<u32>,

    /// Total item count (events/logs)
    count: usize,

    /// Batch type for routing
    batch_type: BatchType,

    /// Workspace ID from API key validation
    workspace_id: u32,

    /// Source IP address
    source_ip: IpAddr,

    /// Source identifier
    source_id: SourceId,

    /// Maximum items before batch is full
    max_items: usize,
}

impl BatchBuilder {
    /// Create a new batch builder with default capacity
    pub fn new(batch_type: BatchType, source_id: SourceId) -> Self {
        Self::with_capacity(batch_type, source_id, DEFAULT_BUFFER_CAPACITY)
    }

    /// Create a new batch builder with specified capacity
    pub fn with_capacity(batch_type: BatchType, source_id: SourceId, capacity: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(capacity),
            offsets: Vec::with_capacity(DEFAULT_BATCH_SIZE),
            lengths: Vec::with_capacity(DEFAULT_BATCH_SIZE),
            count: 0,
            batch_type,
            workspace_id: 0,
            source_ip: IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED),
            source_id,
            max_items: DEFAULT_BATCH_SIZE,
        }
    }

    /// Set the maximum items before batch is considered full
    pub fn set_max_items(&mut self, max: usize) {
        self.max_items = max;
    }

    /// Set workspace ID for this batch
    pub fn set_workspace_id(&mut self, workspace_id: u32) {
        self.workspace_id = workspace_id;
    }

    /// Set source IP for this batch
    pub fn set_source_ip(&mut self, source_ip: IpAddr) {
        self.source_ip = source_ip;
    }

    /// Add a message to the batch
    ///
    /// `item_count` is the number of items (events/logs) in this message.
    /// Returns `true` if the batch is now full (>= max_items).
    pub fn add(&mut self, data: &[u8], item_count: usize) -> bool {
        let offset = self.buffer.len() as u32;
        self.buffer.extend_from_slice(data);
        self.offsets.push(offset);
        self.lengths.push(data.len() as u32);
        self.count += item_count;

        self.count >= self.max_items
    }

    /// Add a message without counting items (for raw forwarding)
    ///
    /// Returns `true` if the batch is now full.
    pub fn add_raw(&mut self, data: &[u8]) -> bool {
        self.add(data, 1)
    }

    /// Consume the builder and produce a finished Batch
    pub fn finish(self) -> Batch {
        Batch {
            buffer: Bytes::from(self.buffer),
            offsets: self.offsets,
            lengths: self.lengths,
            count: self.count,
            batch_type: self.batch_type,
            workspace_id: self.workspace_id,
            source_ip: self.source_ip,
            source_id: self.source_id,
            pattern_ids: None,
        }
    }

    /// Check if batch is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Get current item count
    #[inline]
    pub fn count(&self) -> usize {
        self.count
    }

    /// Get current message count
    #[inline]
    pub fn message_count(&self) -> usize {
        self.offsets.len()
    }

    /// Check if batch is full
    #[inline]
    pub fn is_full(&self) -> bool {
        self.count >= self.max_items
    }

    /// Get current buffer size in bytes
    #[inline]
    pub fn buffer_size(&self) -> usize {
        self.buffer.len()
    }

    /// Reset the builder for reuse (avoids reallocation)
    pub fn reset(&mut self) {
        self.buffer.clear();
        self.offsets.clear();
        self.lengths.clear();
        self.count = 0;
        self.workspace_id = 0;
        self.source_ip = IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED);
    }

    /// Reset with new batch type (for reusing builder across types)
    pub fn reset_with_type(&mut self, batch_type: BatchType) {
        self.reset();
        self.batch_type = batch_type;
    }
}
