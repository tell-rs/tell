//! FlatBuffer batch builder
//!
//! Constructs valid FlatBuffer wire format messages matching the schema
//! in `schema/common.fbs`. This is a hand-written builder that produces
//! bytes compatible with the `FlatBatch` parser in `tell-protocol`.
//!
//! # Wire Format
//!
//! FlatBuffers use a vtable-based format built from back to front:
//!
//! ```text
//! [4 bytes: root offset] -> points to table
//! [vtable]
//!   - vtable_size (u16)
//!   - table_size (u16)
//!   - field offsets (u16 each, 0 = not present)
//! [table]
//!   - soffset to vtable (i32, negative = vtable before table)
//!   - inline scalars and vector offsets
//! [vectors]
//!   - length (u32)
//!   - data bytes
//! ```

use bytes::Bytes;
use tell_protocol::{API_KEY_LENGTH, IPV6_LENGTH, SchemaType};

use crate::error::{BuilderError, Result};

/// Maximum data payload size (16 MB)
const MAX_DATA_SIZE: usize = 16 * 1024 * 1024;

/// Default protocol version (v1.0 = 100)
const DEFAULT_VERSION: u8 = 100;

/// Builder for constructing FlatBuffer Batch messages
///
/// Use the builder pattern to set fields, then call `build()` to produce
/// the wire format bytes.
///
/// # Required Fields
///
/// - `api_key` - 16-byte authentication key
/// - `data` - payload bytes (schema-specific content)
///
/// # Optional Fields
///
/// - `schema_type` - defaults to `Unknown`
/// - `version` - defaults to 100 (v1.0)
/// - `batch_id` - defaults to 0 (not set)
/// - `source_ip` - defaults to None (not set)
///
/// # Example
///
/// ```
/// use tell_client::{BatchBuilder, SchemaType};
///
/// let batch = BatchBuilder::new()
///     .api_key([0x01; 16])
///     .schema_type(SchemaType::Event)
///     .data(b"payload")
///     .build()
///     .unwrap();
/// ```
#[derive(Debug, Clone)]
pub struct BatchBuilder {
    api_key: Option<[u8; API_KEY_LENGTH]>,
    schema_type: SchemaType,
    version: u8,
    batch_id: u64,
    data: Option<Vec<u8>>,
    source_ip: Option<[u8; IPV6_LENGTH]>,
}

impl Default for BatchBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl BatchBuilder {
    /// Create a new batch builder with default values
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self {
            api_key: None,
            schema_type: SchemaType::Unknown,
            version: DEFAULT_VERSION,
            batch_id: 0,
            data: None,
            source_ip: None,
        }
    }

    /// Set the API key (required, 16 bytes)
    #[inline]
    #[must_use]
    pub fn api_key(mut self, key: [u8; API_KEY_LENGTH]) -> Self {
        self.api_key = Some(key);
        self
    }

    /// Set the API key from a slice (must be exactly 16 bytes)
    ///
    /// # Errors
    ///
    /// Returns error if slice length is not 16.
    pub fn api_key_slice(mut self, key: &[u8]) -> Result<Self> {
        if key.len() != API_KEY_LENGTH {
            return Err(BuilderError::InvalidApiKeyLength(key.len()));
        }
        let mut arr = [0u8; API_KEY_LENGTH];
        arr.copy_from_slice(key);
        self.api_key = Some(arr);
        Ok(self)
    }

    /// Set the schema type
    #[inline]
    #[must_use]
    pub fn schema_type(mut self, schema_type: SchemaType) -> Self {
        self.schema_type = schema_type;
        self
    }

    /// Set the protocol version
    #[inline]
    #[must_use]
    pub fn version(mut self, version: u8) -> Self {
        self.version = version;
        self
    }

    /// Set the batch ID (for deduplication)
    #[inline]
    #[must_use]
    pub fn batch_id(mut self, batch_id: u64) -> Self {
        self.batch_id = batch_id;
        self
    }

    /// Set the data payload (required)
    #[inline]
    #[must_use]
    pub fn data(mut self, data: &[u8]) -> Self {
        self.data = Some(data.to_vec());
        self
    }

    /// Set the data payload from owned bytes
    #[inline]
    #[must_use]
    pub fn data_owned(mut self, data: Vec<u8>) -> Self {
        self.data = Some(data);
        self
    }

    /// Set the data payload from a BuiltEventData
    #[inline]
    #[must_use]
    pub fn event_data(self, event_data: crate::event::BuiltEventData) -> Self {
        self.schema_type(SchemaType::Event)
            .data_owned(event_data.into_vec())
    }

    /// Set the data payload from a BuiltLogData
    #[inline]
    #[must_use]
    pub fn log_data(self, log_data: crate::log::BuiltLogData) -> Self {
        self.schema_type(SchemaType::Log)
            .data_owned(log_data.into_vec())
    }

    /// Set the source IP (optional, for forwarded batches)
    #[inline]
    #[must_use]
    pub fn source_ip(mut self, ip: [u8; IPV6_LENGTH]) -> Self {
        self.source_ip = Some(ip);
        self
    }

    /// Set the source IP from a slice (must be exactly 16 bytes)
    ///
    /// # Errors
    ///
    /// Returns error if slice length is not 16.
    pub fn source_ip_slice(mut self, ip: &[u8]) -> Result<Self> {
        if ip.len() != IPV6_LENGTH {
            return Err(BuilderError::InvalidSourceIpLength(ip.len()));
        }
        let mut arr = [0u8; IPV6_LENGTH];
        arr.copy_from_slice(ip);
        self.source_ip = Some(arr);
        Ok(self)
    }

    /// Clear the source IP
    #[inline]
    #[must_use]
    pub fn clear_source_ip(mut self) -> Self {
        self.source_ip = None;
        self
    }

    /// Build the FlatBuffer batch
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - `api_key` is not set
    /// - `data` is not set
    /// - `data` exceeds maximum size
    pub fn build(self) -> Result<BuiltBatch> {
        // Validate required fields
        let api_key = self.api_key.ok_or(BuilderError::MissingApiKey)?;
        let data = self.data.ok_or(BuilderError::MissingData)?;

        // Validate data size
        if data.len() > MAX_DATA_SIZE {
            return Err(BuilderError::data_too_large(data.len(), MAX_DATA_SIZE));
        }

        // Build the wire format
        let bytes = build_flatbuffer(
            &api_key,
            self.schema_type,
            self.version,
            self.batch_id,
            &data,
            self.source_ip.as_ref(),
        );

        Ok(BuiltBatch {
            bytes: Bytes::from(bytes),
        })
    }
}

/// A built FlatBuffer batch ready to send
#[derive(Debug, Clone)]
pub struct BuiltBatch {
    bytes: Bytes,
}

impl BuiltBatch {
    /// Get the raw bytes of the batch
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Convert to owned Bytes
    #[inline]
    pub fn into_bytes(self) -> Bytes {
        self.bytes
    }

    /// Get the length of the batch in bytes
    #[inline]
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Check if the batch is empty (it never should be)
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

impl AsRef<[u8]> for BuiltBatch {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

/// Build the FlatBuffer wire format
///
/// Layout strategy:
/// 1. Calculate sizes of all components
/// 2. Build vectors at the end (api_key, data, source_ip)
/// 3. Build table with offsets to vectors
/// 4. Build vtable
/// 5. Write root offset at start
fn build_flatbuffer(
    api_key: &[u8; API_KEY_LENGTH],
    schema_type: SchemaType,
    version: u8,
    batch_id: u64,
    data: &[u8],
    source_ip: Option<&[u8; IPV6_LENGTH]>,
) -> Vec<u8> {
    // Field presence
    let has_batch_id = batch_id != 0;
    let has_source_ip = source_ip.is_some();

    // VTable: size(u16) + table_size(u16) + 6 field slots (u16 each)
    let vtable_size: u16 = 4 + 6 * 2; // 16 bytes

    // Calculate buffer size
    let api_key_vec_size = 4 + API_KEY_LENGTH; // length + data
    let data_vec_size = 4 + data.len();
    let source_ip_vec_size = if has_source_ip { 4 + IPV6_LENGTH } else { 0 };

    // Fixed table layout (28 bytes after soffset = 32 total):
    // +4: api_key offset (u32)
    // +8: data offset (u32)
    // +12: source_ip offset (u32, 0 if not present)
    // +16: batch_id (u64, 0 if not present)
    // +24: schema_type (u8)
    // +25: version (u8)
    // +26-27: padding
    let table_size: u16 = 4 + 28; // soffset + inline data

    let estimated_size = 4 // root offset
        + vtable_size as usize
        + table_size as usize
        + api_key_vec_size
        + data_vec_size
        + source_ip_vec_size
        + 16; // padding buffer

    let mut buf = Vec::with_capacity(estimated_size);

    // === Root offset placeholder ===
    buf.extend_from_slice(&[0u8; 4]);

    // === VTable ===
    let vtable_start = buf.len();

    buf.extend_from_slice(&vtable_size.to_le_bytes());
    buf.extend_from_slice(&table_size.to_le_bytes());

    // Field offsets
    buf.extend_from_slice(&4u16.to_le_bytes()); // field 0: api_key
    buf.extend_from_slice(&24u16.to_le_bytes()); // field 1: schema_type
    buf.extend_from_slice(&25u16.to_le_bytes()); // field 2: version
    buf.extend_from_slice(&(if has_batch_id { 16u16 } else { 0u16 }).to_le_bytes()); // field 3: batch_id
    buf.extend_from_slice(&8u16.to_le_bytes()); // field 4: data
    buf.extend_from_slice(&(if has_source_ip { 12u16 } else { 0u16 }).to_le_bytes()); // field 5: source_ip

    // === Table ===
    let table_start = buf.len();

    // soffset: distance from table to vtable (standard FlatBuffers: vtable = table - soffset)
    let soffset: i32 = (table_start - vtable_start) as i32;
    buf.extend_from_slice(&soffset.to_le_bytes());

    // Placeholders for vector offsets
    let api_key_offset_pos = buf.len();
    buf.extend_from_slice(&[0u8; 4]); // api_key offset

    let data_offset_pos = buf.len();
    buf.extend_from_slice(&[0u8; 4]); // data offset

    let source_ip_offset_pos = buf.len();
    buf.extend_from_slice(&[0u8; 4]); // source_ip offset (or 0)

    // batch_id (u64)
    buf.extend_from_slice(&batch_id.to_le_bytes());

    // schema_type (u8)
    buf.push(schema_type.as_u8());

    // version (u8)
    buf.push(version);

    // padding (2 bytes)
    buf.extend_from_slice(&[0u8; 2]);

    // === Vectors ===

    // Align to 4 bytes before vectors
    while !buf.len().is_multiple_of(4) {
        buf.push(0);
    }

    // API key vector
    let api_key_vec_start = buf.len();
    buf.extend_from_slice(&(API_KEY_LENGTH as u32).to_le_bytes());
    buf.extend_from_slice(api_key);

    // Align
    while !buf.len().is_multiple_of(4) {
        buf.push(0);
    }

    // Data vector
    let data_vec_start = buf.len();
    buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
    buf.extend_from_slice(data);

    // Align
    while !buf.len().is_multiple_of(4) {
        buf.push(0);
    }

    // Source IP vector (optional)
    let source_ip_vec_start = if let Some(ip) = source_ip {
        let start = buf.len();
        buf.extend_from_slice(&(IPV6_LENGTH as u32).to_le_bytes());
        buf.extend_from_slice(ip);
        Some(start)
    } else {
        None
    };

    // === Fill in offsets ===

    // Root offset points to table
    let root_offset = table_start as u32;
    buf[0..4].copy_from_slice(&root_offset.to_le_bytes());

    // api_key offset (relative from field position to vector)
    let api_key_rel = (api_key_vec_start - api_key_offset_pos) as u32;
    buf[api_key_offset_pos..api_key_offset_pos + 4].copy_from_slice(&api_key_rel.to_le_bytes());

    // data offset
    let data_rel = (data_vec_start - data_offset_pos) as u32;
    buf[data_offset_pos..data_offset_pos + 4].copy_from_slice(&data_rel.to_le_bytes());

    // source_ip offset (if present)
    if let Some(start) = source_ip_vec_start {
        let source_ip_rel = (start - source_ip_offset_pos) as u32;
        buf[source_ip_offset_pos..source_ip_offset_pos + 4]
            .copy_from_slice(&source_ip_rel.to_le_bytes());
    }

    buf
}
