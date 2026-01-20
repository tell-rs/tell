//! Batch FlatBuffer encoding
//!
//! Encodes the outer Batch container that wraps EventData/LogData.
//!
//! # Schema (batch.fbs)
//!
//! ```text
//! table Batch {
//!     api_key:[ubyte] (required, id: 0);
//!     schema_type:SchemaType (id: 1);
//!     version:uint8 (id: 2);
//!     batch_id:uint64 (id: 3);
//!     data:[ubyte] (required, id: 4);
//!     source_ip:[ubyte] (id: 5);
//! }
//! ```
//!
//! # Wire Format Layout
//!
//! FlatBuffer layout (forward format matching parser expectations):
//! ```text
//! [0..4]: root offset (points to table)
//! [4..vtable_end]: vtable
//! [vtable_end..table_end]: table with inline fields and offset placeholders
//! [table_end..]: vectors (api_key, data, source_ip)
//! ```

use std::net::IpAddr;

use super::{write_i32, write_u32};
use crate::SchemaType;

/// Current protocol version (v1.0 = 100)
const PROTOCOL_VERSION: u8 = 100;

/// Encoder for Batch FlatBuffer (outer container)
pub struct BatchEncoder {
    api_key: [u8; 16],
    schema_type: SchemaType,
    batch_id: u64,
    source_ip: Option<[u8; 16]>,
}

impl BatchEncoder {
    /// Create a new batch encoder
    pub fn new() -> Self {
        Self {
            api_key: [0u8; 16],
            schema_type: SchemaType::Unknown,
            batch_id: 0,
            source_ip: None,
        }
    }

    /// Set the API key (16 bytes)
    pub fn set_api_key(&mut self, key: &[u8; 16]) {
        self.api_key = *key;
    }

    /// Set the schema type
    pub fn set_schema_type(&mut self, schema_type: SchemaType) {
        self.schema_type = schema_type;
    }

    /// Set the batch ID (for deduplication)
    pub fn set_batch_id(&mut self, id: u64) {
        self.batch_id = id;
    }

    /// Set the source IP (for forwarding)
    pub fn set_source_ip(&mut self, ip: IpAddr) {
        self.source_ip = Some(ip_to_bytes(ip));
    }

    /// Encode a complete Batch with the given data payload
    ///
    /// The data should be an already-encoded EventData or LogData FlatBuffer.
    ///
    /// Layout follows flatbuf_test.rs create_test_batch format:
    /// - Root offset at start
    /// - VTable after root offset
    /// - Table after vtable
    /// - Vectors (api_key, data, source_ip) after table
    pub fn encode(&mut self, data: &[u8]) -> Vec<u8> {
        // Estimate capacity: root(4) + vtable(16) + table(~28) + vectors
        let capacity =
            64 + self.api_key.len() + data.len() + self.source_ip.as_ref().map_or(0, |_| 20);
        let mut buf = Vec::with_capacity(capacity);

        // === Build VTable ===
        // VTable: vtable_size(u16), table_size(u16), field_offsets(u16 each for 6 fields)
        let vtable_size: u16 = 4 + 6 * 2; // 16 bytes

        // Table inline layout (after soffset):
        // +4: api_key offset (u32)
        // +8: data offset (u32)
        // +12: schema_type (u8)
        // +13: version (u8)
        // +14: padding (2 bytes)
        // Optional +16: source_ip offset (u32)
        let table_inline_size: u16 = if self.source_ip.is_some() {
            4 + 4 + 4 + 1 + 1 + 2 + 4 // 20 bytes with source_ip
        } else {
            4 + 4 + 4 + 1 + 1 + 2 // 16 bytes without source_ip
        };

        // Reserve space for root offset (filled in later)
        buf.extend_from_slice(&[0u8; 4]);

        // VTable starts at offset 4
        let vtable_start = buf.len();
        buf.extend_from_slice(&vtable_size.to_le_bytes());
        buf.extend_from_slice(&table_inline_size.to_le_bytes());

        // Field offsets (from table start, after soffset):
        // Field 0 (api_key): offset to u32 that points to vector
        // Field 1 (schema_type): offset to inline u8
        // Field 2 (version): offset to inline u8
        // Field 3 (batch_id): 0 (not present - we don't use it currently)
        // Field 4 (data): offset to u32 that points to vector
        // Field 5 (source_ip): offset to u32 or 0 if not present
        let field_api_key_offset: u16 = 4;
        let field_schema_type_offset: u16 = 12;
        let field_version_offset: u16 = 13;
        let field_batch_id_offset: u16 = 0; // not present
        let field_data_offset: u16 = 8;
        let field_source_ip_offset: u16 = if self.source_ip.is_some() { 16 } else { 0 };

        buf.extend_from_slice(&field_api_key_offset.to_le_bytes());
        buf.extend_from_slice(&field_schema_type_offset.to_le_bytes());
        buf.extend_from_slice(&field_version_offset.to_le_bytes());
        buf.extend_from_slice(&field_batch_id_offset.to_le_bytes());
        buf.extend_from_slice(&field_data_offset.to_le_bytes());
        buf.extend_from_slice(&field_source_ip_offset.to_le_bytes());

        // === Build Table ===
        let table_start = buf.len();

        // soffset: signed distance from table to vtable
        let soffset: i32 = (table_start - vtable_start) as i32;
        write_i32(&mut buf, soffset);

        // Placeholder for api_key vector offset (filled in later)
        let api_key_offset_pos = buf.len();
        buf.extend_from_slice(&[0u8; 4]);

        // Placeholder for data vector offset (filled in later)
        let data_offset_pos = buf.len();
        buf.extend_from_slice(&[0u8; 4]);

        // schema_type (u8)
        buf.push(self.schema_type.as_u8());

        // version (u8)
        buf.push(PROTOCOL_VERSION);

        // Padding (2 bytes)
        buf.extend_from_slice(&[0u8; 2]);

        // Placeholder for source_ip offset if present
        let source_ip_offset_pos = if self.source_ip.is_some() {
            let pos = buf.len();
            buf.extend_from_slice(&[0u8; 4]);
            Some(pos)
        } else {
            None
        };

        // Align to 4 bytes before vectors
        while buf.len() % 4 != 0 {
            buf.push(0);
        }

        // === Build Vectors ===

        // API key vector
        let api_key_vec_start = buf.len();
        write_u32(&mut buf, self.api_key.len() as u32);
        buf.extend_from_slice(&self.api_key);
        // Align to 4 bytes
        while buf.len() % 4 != 0 {
            buf.push(0);
        }

        // Data vector
        let data_vec_start = buf.len();
        write_u32(&mut buf, data.len() as u32);
        buf.extend_from_slice(data);
        // Align to 4 bytes
        while buf.len() % 4 != 0 {
            buf.push(0);
        }

        // Source IP vector (if present)
        let source_ip_vec_start = self.source_ip.map(|ip| {
            let start = buf.len();
            write_u32(&mut buf, ip.len() as u32);
            buf.extend_from_slice(&ip);
            while buf.len() % 4 != 0 {
                buf.push(0);
            }
            start
        });

        // === Fill in offsets ===

        // api_key offset: relative from field position to vector
        let api_key_rel = (api_key_vec_start - api_key_offset_pos) as u32;
        buf[api_key_offset_pos..api_key_offset_pos + 4].copy_from_slice(&api_key_rel.to_le_bytes());

        // data offset: relative from field position to vector
        let data_rel = (data_vec_start - data_offset_pos) as u32;
        buf[data_offset_pos..data_offset_pos + 4].copy_from_slice(&data_rel.to_le_bytes());

        // source_ip offset (if present)
        if let (Some(pos), Some(vec_start)) = (source_ip_offset_pos, source_ip_vec_start) {
            let ip_rel = (vec_start - pos) as u32;
            buf[pos..pos + 4].copy_from_slice(&ip_rel.to_le_bytes());
        }

        // Root offset: points to table start
        let root_offset = table_start as u32;
        buf[0..4].copy_from_slice(&root_offset.to_le_bytes());

        buf
    }
}

impl Default for BatchEncoder {
    fn default() -> Self {
        Self::new()
    }
}

/// Convert IpAddr to 16-byte IPv6 format (IPv4 uses mapped format)
fn ip_to_bytes(ip: IpAddr) -> [u8; 16] {
    match ip {
        IpAddr::V4(v4) => {
            let octets = v4.octets();
            [
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, octets[0], octets[1], octets[2],
                octets[3],
            ]
        }
        IpAddr::V6(v6) => v6.octets(),
    }
}
