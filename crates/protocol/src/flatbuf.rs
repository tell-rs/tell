//! FlatBuffer wire format parsing
//!
//! Zero-copy parsing of FlatBuffer messages without code generation.
//! This module provides direct access to the wire format as defined in
//! `crates/protocol/schema/common.fbs`.
//!
//! # Wire Format
//!
//! FlatBuffers uses a vtable-based format:
//! ```text
//! [4 bytes: root offset] -> [table]
//!                              |
//!                              v
//!                           [vtable offset (i32, negative)]
//!                           [field data...]
//!                              |
//!                              v
//!                           [vtable]
//!                           [vtable size (u16)]
//!                           [table size (u16)]
//!                           [field offsets (u16 each)]
//! ```
//!
//! # Safety
//!
//! This module performs bounds checking on all accesses. Invalid messages
//! will return errors rather than panicking or reading out of bounds.

use crate::{API_KEY_LENGTH, IPV6_LENGTH, MAX_REASONABLE_SIZE, MIN_BATCH_SIZE, ProtocolError, Result, SchemaType};

/// Field IDs from common.fbs (these are vtable slot indices, not byte offsets)
/// Slot 0 and 1 are reserved for vtable_size and table_size
const FIELD_API_KEY: usize = 0;
const FIELD_SCHEMA_TYPE: usize = 1;
const FIELD_VERSION: usize = 2;
const FIELD_BATCH_ID: usize = 3;
const FIELD_DATA: usize = 4;
const FIELD_SOURCE_IP: usize = 5;

/// Zero-copy view into a FlatBuffer Batch message
///
/// This struct provides read-only access to fields in the wire format
/// without copying data. All field accessors return slices or values
/// directly from the underlying buffer.
///
/// # Example
///
/// ```ignore
/// let msg: &[u8] = receive_message();
/// let batch = FlatBatch::parse(msg)?;
///
/// let api_key = batch.api_key()?;
/// let schema_type = batch.schema_type();
/// let data = batch.data()?;
/// ```
#[derive(Debug, Clone, Copy)]
pub struct FlatBatch<'a> {
    /// Raw message bytes
    buf: &'a [u8],
    /// Offset to the root table
    table_offset: usize,
    /// Offset to the vtable
    vtable_offset: usize,
    /// Number of fields in vtable
    vtable_fields: usize,
}

impl<'a> FlatBatch<'a> {
    /// Parse a FlatBuffer Batch message
    ///
    /// This performs validation of the message structure but does not
    /// copy any data. Field access is deferred until accessor methods
    /// are called.
    ///
    /// # Validation Stages (matching Go SafeGetRootAsBatch)
    ///
    /// 1. Size bounds check (min 16 bytes, max 100MB)
    /// 2. Root offset sanity check
    /// 3. VTable sanity check
    /// 4. Bounds checking on all field accesses (Rust memory safety)
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Message is too short (< 16 bytes)
    /// - Message exceeds reasonable size (> 100MB)
    /// - Root offset is invalid
    /// - VTable structure is invalid
    pub fn parse(buf: &'a [u8]) -> Result<Self> {
        // Stage 1: Size bounds check
        if buf.len() < MIN_BATCH_SIZE {
            return Err(ProtocolError::too_short(MIN_BATCH_SIZE, buf.len()));
        }

        if buf.len() > MAX_REASONABLE_SIZE {
            return Err(ProtocolError::message_too_large(buf.len(), MAX_REASONABLE_SIZE));
        }

        // Read root offset (first 4 bytes, little-endian u32)
        let root_offset = read_u32(buf, 0)? as usize;

        // Validate root offset
        if root_offset >= buf.len() || root_offset + 4 > buf.len() {
            return Err(ProtocolError::invalid_flatbuffer(format!(
                "root offset {} exceeds buffer length {}",
                root_offset,
                buf.len()
            )));
        }

        // Table starts at root_offset
        let table_offset = root_offset;

        // Read vtable offset (i32 at table start, relative to table position)
        // FlatBuffers spec: vtable_location = table_location - soffset
        // The soffset is ALWAYS subtracted, regardless of sign
        let vtable_soffset = read_i32(buf, table_offset)?;
        let vtable_offset = if vtable_soffset >= 0 {
            table_offset
                .checked_sub(vtable_soffset as usize)
                .ok_or_else(|| {
                    ProtocolError::invalid_flatbuffer("vtable offset underflow".to_string())
                })?
        } else {
            // Negative soffset means vtable is after table (rare but valid)
            table_offset + ((-vtable_soffset) as usize)
        };

        // Validate vtable offset
        if vtable_offset + 4 > buf.len() {
            return Err(ProtocolError::invalid_flatbuffer(format!(
                "vtable offset {} exceeds buffer length {}",
                vtable_offset,
                buf.len()
            )));
        }

        // Read vtable size (first 2 bytes of vtable)
        let vtable_size = read_u16(buf, vtable_offset)? as usize;

        // Validate vtable size
        if vtable_size < 4 || vtable_offset + vtable_size > buf.len() {
            return Err(ProtocolError::invalid_flatbuffer(format!(
                "invalid vtable size {} at offset {}",
                vtable_size, vtable_offset
            )));
        }

        // Calculate number of field slots in vtable
        // vtable layout: [vtable_size:u16][table_size:u16][field_offsets:u16...]
        let vtable_fields = (vtable_size - 4) / 2;

        Ok(Self {
            buf,
            table_offset,
            vtable_offset,
            vtable_fields,
        })
    }

    /// Get the raw buffer
    #[inline]
    pub fn raw_bytes(&self) -> &'a [u8] {
        self.buf
    }

    /// Get field offset from vtable, or None if field not present
    fn field_offset(&self, field_index: usize) -> Option<usize> {
        if field_index >= self.vtable_fields {
            return None;
        }

        // Field offsets start at vtable + 4 (after vtable_size and table_size)
        let slot_offset = self.vtable_offset + 4 + (field_index * 2);

        if slot_offset + 2 > self.buf.len() {
            return None;
        }

        let field_offset = read_u16(self.buf, slot_offset).ok()? as usize;

        if field_offset == 0 {
            // Field not present
            None
        } else {
            // Field offset is relative to table start
            Some(self.table_offset + field_offset)
        }
    }

    /// Read a vector field (returns offset and length)
    fn read_vector(&self, field_index: usize) -> Result<Option<&'a [u8]>> {
        let field_offset = match self.field_offset(field_index) {
            Some(off) => off,
            None => return Ok(None),
        };

        // Vector fields contain an offset to the vector
        if field_offset + 4 > self.buf.len() {
            return Err(ProtocolError::invalid_flatbuffer(
                "vector offset out of bounds".to_string(),
            ));
        }

        let vector_offset_rel = read_u32(self.buf, field_offset)? as usize;
        let vector_offset = field_offset + vector_offset_rel;

        if vector_offset + 4 > self.buf.len() {
            return Err(ProtocolError::invalid_flatbuffer(
                "vector data offset out of bounds".to_string(),
            ));
        }

        // Vector starts with length (u32)
        let length = read_u32(self.buf, vector_offset)? as usize;
        let data_start = vector_offset + 4;

        if data_start + length > self.buf.len() {
            return Err(ProtocolError::invalid_flatbuffer(format!(
                "vector data extends past buffer: {} + {} > {}",
                data_start,
                length,
                self.buf.len()
            )));
        }

        Ok(Some(&self.buf[data_start..data_start + length]))
    }

    /// Read a scalar u8 field
    fn read_u8(&self, field_index: usize, default: u8) -> u8 {
        match self.field_offset(field_index) {
            Some(offset) => self.buf.get(offset).copied().unwrap_or(default),
            None => default,
        }
    }

    /// Read a scalar u64 field
    fn read_u64(&self, field_index: usize, default: u64) -> u64 {
        match self.field_offset(field_index) {
            Some(offset) => read_u64(self.buf, offset).unwrap_or(default),
            None => default,
        }
    }

    // =========================================================================
    // Public field accessors
    // =========================================================================

    /// Get the API key (required field, 16 bytes)
    ///
    /// # Errors
    ///
    /// Returns error if field is missing or has wrong length.
    pub fn api_key(&self) -> Result<&'a [u8; API_KEY_LENGTH]> {
        let data = self
            .read_vector(FIELD_API_KEY)?
            .ok_or(ProtocolError::missing_field("api_key"))?;

        if data.len() != API_KEY_LENGTH {
            return Err(ProtocolError::invalid_api_key_length(data.len()));
        }

        // Safe: we just verified the length
        Ok(data.try_into().unwrap())
    }

    /// Get the API key as a slice (for validation without array conversion)
    pub fn api_key_bytes(&self) -> Result<&'a [u8]> {
        self.read_vector(FIELD_API_KEY)?
            .ok_or(ProtocolError::missing_field("api_key"))
    }

    /// Get the schema type
    ///
    /// Returns `SchemaType::Unknown` if field is missing or invalid.
    pub fn schema_type(&self) -> SchemaType {
        SchemaType::from_u8(self.read_u8(FIELD_SCHEMA_TYPE, 0))
    }

    /// Get the protocol version
    ///
    /// Returns 0 if field is missing.
    pub fn version(&self) -> u8 {
        self.read_u8(FIELD_VERSION, 0)
    }

    /// Get the batch ID (for deduplication)
    ///
    /// Returns 0 if field is missing.
    pub fn batch_id(&self) -> u64 {
        self.read_u64(FIELD_BATCH_ID, 0)
    }

    /// Get the data payload (required field)
    ///
    /// This returns the raw bytes of the schema-specific payload
    /// (EventData, LogData, etc.) without copying.
    ///
    /// # Errors
    ///
    /// Returns error if field is missing.
    pub fn data(&self) -> Result<&'a [u8]> {
        self.read_vector(FIELD_DATA)?
            .ok_or(ProtocolError::missing_field("data"))
    }

    /// Get the source IP (optional, for forwarded batches)
    ///
    /// Returns None if field is missing, error if wrong length.
    pub fn source_ip(&self) -> Result<Option<&'a [u8; IPV6_LENGTH]>> {
        match self.read_vector(FIELD_SOURCE_IP)? {
            None => Ok(None),
            Some(data) => {
                if data.len() != IPV6_LENGTH {
                    Err(ProtocolError::invalid_source_ip_length(data.len()))
                } else {
                    // Safe: we just verified the length
                    Ok(Some(data.try_into().unwrap()))
                }
            }
        }
    }

    /// Get the source IP as raw bytes (for cases where length check is deferred)
    pub fn source_ip_bytes(&self) -> Result<Option<&'a [u8]>> {
        self.read_vector(FIELD_SOURCE_IP)
    }

    /// Check if this batch has a source IP field set
    pub fn has_source_ip(&self) -> bool {
        self.field_offset(FIELD_SOURCE_IP).is_some()
    }
}

// =============================================================================
// Helper functions for reading little-endian values
// =============================================================================

#[inline]
pub(crate) fn read_u16(buf: &[u8], offset: usize) -> Result<u16> {
    if offset + 2 > buf.len() {
        return Err(ProtocolError::too_short(offset + 2, buf.len()));
    }
    Ok(u16::from_le_bytes([buf[offset], buf[offset + 1]]))
}

#[inline]
pub(crate) fn read_u32(buf: &[u8], offset: usize) -> Result<u32> {
    if offset + 4 > buf.len() {
        return Err(ProtocolError::too_short(offset + 4, buf.len()));
    }
    Ok(u32::from_le_bytes([
        buf[offset],
        buf[offset + 1],
        buf[offset + 2],
        buf[offset + 3],
    ]))
}

#[inline]
pub(crate) fn read_i32(buf: &[u8], offset: usize) -> Result<i32> {
    if offset + 4 > buf.len() {
        return Err(ProtocolError::too_short(offset + 4, buf.len()));
    }
    Ok(i32::from_le_bytes([
        buf[offset],
        buf[offset + 1],
        buf[offset + 2],
        buf[offset + 3],
    ]))
}

#[inline]
pub(crate) fn read_u64(buf: &[u8], offset: usize) -> Result<u64> {
    if offset + 8 > buf.len() {
        return Err(ProtocolError::too_short(offset + 8, buf.len()));
    }
    Ok(u64::from_le_bytes([
        buf[offset],
        buf[offset + 1],
        buf[offset + 2],
        buf[offset + 3],
        buf[offset + 4],
        buf[offset + 5],
        buf[offset + 6],
        buf[offset + 7],
    ]))
}

// =============================================================================
// Quick heuristic for fast rejection
// =============================================================================

/// Check if data is likely a FlatBuffer (quick heuristic)
///
/// This performs fast checks to reject obviously invalid data (JSON, XML, etc.)
/// before attempting full parsing. This is a performance optimization for
/// high-throughput scenarios where garbage data may be received.
///
/// # Returns
///
/// - `true` if the data has FlatBuffer-like characteristics
/// - `false` if the data is obviously not a FlatBuffer
///
/// Note: Returning `true` does not guarantee the data is valid - use `FlatBatch::parse()`
/// for full validation.
#[inline]
pub fn is_likely_flatbuffer(buf: &[u8]) -> bool {
    // Too small to be valid
    if buf.len() < MIN_BATCH_SIZE {
        return false;
    }

    // Quick rejection of common non-FlatBuffer formats
    if let Some(&first) = buf.first() {
        // JSON starts with '{' or '['
        // XML starts with '<'
        // These are common garbage data patterns
        if first == b'{' || first == b'[' || first == b'<' {
            return false;
        }
    }

    // Check if root offset looks reasonable
    // FlatBuffers store root offset as little-endian u32 in first 4 bytes
    let root_offset = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;

    // Root offset should be between 4 and buffer length
    // A root offset of 0-3 is invalid, and beyond buffer length is invalid
    if root_offset < 4 || root_offset >= buf.len() {
        return false;
    }

    true
}
