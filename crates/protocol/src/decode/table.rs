//! FlatBuffer table parsing helpers
//!
//! Generic utilities for parsing FlatBuffer tables without code generation.

use crate::{ProtocolError, Result};

/// Helper for parsing FlatBuffer tables
#[derive(Debug, Clone, Copy)]
pub(crate) struct FlatTable<'a> {
    buf: &'a [u8],
    table_offset: usize,
    vtable_offset: usize,
    vtable_fields: usize,
}

impl<'a> FlatTable<'a> {
    /// Parse a table at the given offset
    pub fn parse(buf: &'a [u8], table_offset: usize) -> Result<Self> {
        if table_offset + 4 > buf.len() {
            return Err(ProtocolError::invalid_flatbuffer(
                "table offset out of bounds",
            ));
        }

        // Read vtable soffset (standard FlatBuffers: vtable = table - soffset)
        let vtable_soffset = read_i32(buf, table_offset)?;
        let vtable_offset = if vtable_soffset >= 0 {
            table_offset
                .checked_sub(vtable_soffset as usize)
                .ok_or_else(|| ProtocolError::invalid_flatbuffer("vtable offset underflow"))?
        } else {
            // Negative soffset means vtable is after table (rare but valid)
            table_offset + ((-vtable_soffset) as usize)
        };

        if vtable_offset + 4 > buf.len() {
            return Err(ProtocolError::invalid_flatbuffer("vtable out of bounds"));
        }

        let vtable_size = read_u16(buf, vtable_offset)? as usize;
        if vtable_size < 4 || vtable_offset + vtable_size > buf.len() {
            return Err(ProtocolError::invalid_flatbuffer("invalid vtable size"));
        }

        let vtable_fields = (vtable_size - 4) / 2;

        Ok(Self {
            buf,
            table_offset,
            vtable_offset,
            vtable_fields,
        })
    }

    /// Get field offset from vtable
    fn field_offset(&self, field_index: usize) -> Option<usize> {
        if field_index >= self.vtable_fields {
            return None;
        }

        let slot_offset = self.vtable_offset + 4 + (field_index * 2);
        if slot_offset + 2 > self.buf.len() {
            return None;
        }

        let field_offset = read_u16(self.buf, slot_offset).ok()? as usize;
        if field_offset == 0 {
            None
        } else {
            Some(self.table_offset + field_offset)
        }
    }

    /// Read u8 field with default
    pub fn read_u8(&self, field_index: usize, default: u8) -> u8 {
        self.field_offset(field_index)
            .and_then(|off| self.buf.get(off).copied())
            .unwrap_or(default)
    }

    /// Read u64 field with default
    pub fn read_u64(&self, field_index: usize, default: u64) -> u64 {
        self.field_offset(field_index)
            .and_then(|off| read_u64(self.buf, off).ok())
            .unwrap_or(default)
    }

    /// Read i64 field with default
    pub fn read_i64(&self, field_index: usize, default: i64) -> i64 {
        self.field_offset(field_index)
            .and_then(|off| read_i64(self.buf, off).ok())
            .unwrap_or(default)
    }

    /// Read f64 (double) field with default
    pub fn read_f64(&self, field_index: usize, default: f64) -> f64 {
        self.field_offset(field_index)
            .and_then(|off| read_f64(self.buf, off).ok())
            .unwrap_or(default)
    }

    /// Read nested table field
    pub fn read_table(&self, field_index: usize) -> Result<Option<FlatTable<'a>>> {
        let Some(field_offset) = self.field_offset(field_index) else {
            return Ok(None);
        };

        if field_offset + 4 > self.buf.len() {
            return Err(ProtocolError::invalid_flatbuffer("table offset out of bounds"));
        }

        // Read relative offset to nested table
        let table_rel = read_u32(self.buf, field_offset)? as usize;
        let table_offset = field_offset + table_rel;

        let table = FlatTable::parse(self.buf, table_offset)?;
        Ok(Some(table))
    }

    /// Read bytes vector
    pub fn read_bytes(&self, field_index: usize) -> Result<Option<&'a [u8]>> {
        let Some(field_offset) = self.field_offset(field_index) else {
            return Ok(None);
        };

        read_vector(self.buf, field_offset)
    }

    /// Read fixed-size bytes (e.g., UUID)
    pub fn read_fixed_bytes<const N: usize>(&self, field_index: usize) -> Result<Option<&'a [u8; N]>> {
        let Some(bytes) = self.read_bytes(field_index)? else {
            return Ok(None);
        };

        if bytes.len() != N {
            return Ok(None); // Wrong length, treat as not present
        }

        // Safe: we just checked the length
        Ok(Some(bytes.try_into().unwrap()))
    }

    /// Read string field
    pub fn read_string(&self, field_index: usize) -> Result<Option<&'a str>> {
        let Some(bytes) = self.read_bytes(field_index)? else {
            return Ok(None);
        };

        std::str::from_utf8(bytes)
            .map(Some)
            .map_err(|_| ProtocolError::invalid_flatbuffer("invalid UTF-8 string"))
    }

    /// Read vector of tables
    pub fn read_vector_of_tables(&self, field_index: usize) -> Result<Option<Vec<FlatTable<'a>>>> {
        let Some(field_offset) = self.field_offset(field_index) else {
            return Ok(None);
        };

        if field_offset + 4 > self.buf.len() {
            return Err(ProtocolError::invalid_flatbuffer(
                "vector offset out of bounds",
            ));
        }

        // Read relative offset to vector
        let vector_rel = read_u32(self.buf, field_offset)? as usize;
        let vector_offset = field_offset + vector_rel;

        if vector_offset + 4 > self.buf.len() {
            return Err(ProtocolError::invalid_flatbuffer(
                "vector data out of bounds",
            ));
        }

        // Read vector length
        let length = read_u32(self.buf, vector_offset)? as usize;
        let data_start = vector_offset + 4;

        // Each element is a 4-byte offset to a table
        if data_start + length * 4 > self.buf.len() {
            return Err(ProtocolError::invalid_flatbuffer(
                "vector elements out of bounds",
            ));
        }

        let mut tables = Vec::with_capacity(length);

        for i in 0..length {
            let elem_offset_pos = data_start + i * 4;
            let elem_rel = read_u32(self.buf, elem_offset_pos)? as usize;
            let elem_offset = elem_offset_pos + elem_rel;

            let table = FlatTable::parse(self.buf, elem_offset)?;
            tables.push(table);
        }

        Ok(Some(tables))
    }
}

// =============================================================================
// Read Helpers
// =============================================================================

/// Read vector from buffer
fn read_vector(buf: &[u8], field_offset: usize) -> Result<Option<&[u8]>> {
    if field_offset + 4 > buf.len() {
        return Err(ProtocolError::invalid_flatbuffer(
            "vector offset out of bounds",
        ));
    }

    let vector_rel = read_u32(buf, field_offset)? as usize;
    let vector_offset = field_offset + vector_rel;

    if vector_offset + 4 > buf.len() {
        return Err(ProtocolError::invalid_flatbuffer(
            "vector data out of bounds",
        ));
    }

    let length = read_u32(buf, vector_offset)? as usize;
    let data_start = vector_offset + 4;

    if data_start + length > buf.len() {
        return Err(ProtocolError::invalid_flatbuffer(
            "vector data extends past buffer",
        ));
    }

    Ok(Some(&buf[data_start..data_start + length]))
}

#[inline]
fn read_u16(buf: &[u8], offset: usize) -> Result<u16> {
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
fn read_i32(buf: &[u8], offset: usize) -> Result<i32> {
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
fn read_u64(buf: &[u8], offset: usize) -> Result<u64> {
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

#[inline]
fn read_i64(buf: &[u8], offset: usize) -> Result<i64> {
    if offset + 8 > buf.len() {
        return Err(ProtocolError::too_short(offset + 8, buf.len()));
    }
    Ok(i64::from_le_bytes([
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

#[inline]
fn read_f64(buf: &[u8], offset: usize) -> Result<f64> {
    if offset + 8 > buf.len() {
        return Err(ProtocolError::too_short(offset + 8, buf.len()));
    }
    Ok(f64::from_le_bytes([
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
