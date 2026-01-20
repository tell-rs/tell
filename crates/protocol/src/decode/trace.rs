//! Trace decoding from FlatBuffer payloads
//!
//! Parses TraceData FlatBuffer containing distributed tracing spans.
//!
//! # Schema Reference (trace.fbs)
//!
//! ```text
//! table Span {
//!     trace_id:[ubyte] (id: 0);
//!     span_id:[ubyte] (id: 1);
//!     parent_span_id:[ubyte] (id: 2);
//!     timestamp:uint64 (id: 3);
//!     duration:uint64 (id: 4);
//!     name:string (id: 5);
//!     kind:SpanKind (id: 6);
//!     status:SpanStatus (id: 7);
//!     source:string (id: 8);
//!     service:string (id: 9);
//!     session_id:[ubyte] (id: 10);
//!     payload:[ubyte] (id: 11);
//! }
//! table TraceData { spans:[Span] (required); }
//! ```

use super::table::{FlatTable, read_u32};
use crate::{ProtocolError, Result};

// =============================================================================
// Span Types
// =============================================================================

/// Span kind - the role of this span in the trace
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum SpanKind {
    Unspecified = 0,
    Internal = 1,
    Server = 2,
    Client = 3,
    Producer = 4,
    Consumer = 5,
}

impl SpanKind {
    #[inline]
    pub const fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Internal,
            2 => Self::Server,
            3 => Self::Client,
            4 => Self::Producer,
            5 => Self::Consumer,
            _ => Self::Unspecified,
        }
    }

    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unspecified => "unspecified",
            Self::Internal => "internal",
            Self::Server => "server",
            Self::Client => "client",
            Self::Producer => "producer",
            Self::Consumer => "consumer",
        }
    }
}

impl std::fmt::Display for SpanKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Span status - success or failure
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum SpanStatus {
    Unset = 0,
    Ok = 1,
    Error = 2,
}

impl SpanStatus {
    #[inline]
    pub const fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Ok,
            2 => Self::Error,
            _ => Self::Unset,
        }
    }

    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unset => "unset",
            Self::Ok => "ok",
            Self::Error => "error",
        }
    }
}

impl std::fmt::Display for SpanStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

// =============================================================================
// Decoded Span
// =============================================================================

/// A decoded span from TraceData
#[derive(Debug, Clone)]
pub struct DecodedSpan<'a> {
    /// 16-byte trace ID - links all spans in trace
    pub trace_id: Option<&'a [u8; 16]>,
    /// 8-byte span ID - unique per span
    pub span_id: Option<&'a [u8; 8]>,
    /// 8-byte parent span ID (None = root span)
    pub parent_span_id: Option<&'a [u8; 8]>,
    /// Start time in nanoseconds since epoch
    pub timestamp: u64,
    /// Duration in nanoseconds
    pub duration: u64,
    /// Operation name
    pub name: Option<&'a str>,
    /// Role in trace
    pub kind: SpanKind,
    /// Status (OK/ERROR)
    pub status: SpanStatus,
    /// Source hostname/instance
    pub source: Option<&'a str>,
    /// Service name
    pub service: Option<&'a str>,
    /// 16-byte session UUID for correlation
    pub session_id: Option<&'a [u8; 16]>,
    /// JSON payload bytes
    pub payload: &'a [u8],
}

impl<'a> DecodedSpan<'a> {
    /// Check if this is a root span (no parent)
    pub fn is_root(&self) -> bool {
        self.parent_span_id.is_none()
    }
}

// =============================================================================
// TraceData Parser
// =============================================================================

/// Parse TraceData FlatBuffer from bytes
pub fn decode_trace_data(buf: &[u8]) -> Result<Vec<DecodedSpan<'_>>> {
    if buf.len() < 8 {
        return Err(ProtocolError::too_short(8, buf.len()));
    }

    let root_offset = read_u32(buf, 0)? as usize;
    if root_offset >= buf.len() {
        return Err(ProtocolError::invalid_flatbuffer(
            "root offset out of bounds",
        ));
    }

    let table = FlatTable::parse(buf, root_offset)?;

    let spans_vec = table
        .read_vector_of_tables(0)?
        .ok_or_else(|| ProtocolError::missing_field("spans"))?;

    let mut spans = Vec::with_capacity(spans_vec.len());
    for span_table in spans_vec {
        let span = parse_span(&span_table)?;
        spans.push(span);
    }

    Ok(spans)
}

fn parse_span<'a>(table: &FlatTable<'a>) -> Result<DecodedSpan<'a>> {
    // Field 0: trace_id (16 bytes)
    let trace_id = table.read_fixed_bytes::<16>(0)?;

    // Field 1: span_id (8 bytes)
    let span_id = table.read_fixed_bytes::<8>(1)?;

    // Field 2: parent_span_id (8 bytes)
    let parent_span_id = table.read_fixed_bytes::<8>(2)?;

    // Field 3: timestamp (u64)
    let timestamp = table.read_u64(3, 0);

    // Field 4: duration (u64)
    let duration = table.read_u64(4, 0);

    // Field 5: name (string)
    let name = table.read_string(5)?;

    // Field 6: kind (u8)
    let kind = SpanKind::from_u8(table.read_u8(6, 0));

    // Field 7: status (u8)
    let status = SpanStatus::from_u8(table.read_u8(7, 0));

    // Field 8: source (string)
    let source = table.read_string(8)?;

    // Field 9: service (string)
    let service = table.read_string(9)?;

    // Field 10: session_id (16 bytes)
    let session_id = table.read_fixed_bytes::<16>(10)?;

    // Field 11: payload (bytes)
    let payload = table.read_bytes(11)?.unwrap_or(&[]);

    Ok(DecodedSpan {
        trace_id,
        span_id,
        parent_span_id,
        timestamp,
        duration,
        name,
        kind,
        status,
        source,
        service,
        session_id,
        payload,
    })
}
