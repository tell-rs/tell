//! Wire protocol for tap communication
//!
//! Defines the message types exchanged between TapServer and CLI clients.
//! Uses simple length-prefixed binary format for future client compatibility
//! (not Rust-specific like bincode).
//!
//! # Wire Format
//!
//! All messages are length-prefixed:
//! ```text
//! ┌──────────────┬─────────────────────────────────────┐
//! │ 4 bytes      │ N bytes                             │
//! │ length (BE)  │ payload                             │
//! └──────────────┴─────────────────────────────────────┘
//! ```
//!
//! # Message Types
//!
//! - `Subscribe` (0x01): Client → Server, request subscription with filters
//! - `Batch` (0x02): Server → Client, batch data envelope
//! - `Heartbeat` (0x03): Server → Client, keep-alive
//! - `Error` (0x04): Server → Client, error message

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use crate::error::{Result, TapError};

/// Message type discriminants
const MSG_SUBSCRIBE: u8 = 0x01;
const MSG_BATCH: u8 = 0x02;
const MSG_HEARTBEAT: u8 = 0x03;
const MSG_ERROR: u8 = 0x04;

/// Messages exchanged between tap server and clients
#[derive(Debug, Clone, PartialEq)]
pub enum TapMessage {
    /// Client → Server: Subscribe with filters
    Subscribe(SubscribeRequest),
    /// Server → Client: Batch data
    Batch(TapEnvelope),
    /// Server → Client: Keep-alive ping
    Heartbeat,
    /// Server → Client: Error message
    Error(String),
}

/// Subscription request from client
///
/// Contains metadata filters (server-side) and rate limiting options.
/// All filters are optional - None means "match all".
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SubscribeRequest {
    /// Filter by workspace IDs (None = all workspaces)
    pub workspace_ids: Option<Vec<u32>>,
    /// Filter by source IDs (None = all sources)
    pub source_ids: Option<Vec<String>>,
    /// Filter by batch types (None = all types)
    /// Values: 0=Unknown, 1=Event, 2=Log, 3=Metric, 4=Trace
    pub batch_types: Option<Vec<u8>>,
    /// Request last N batches from replay buffer before live stream
    pub last_n: u32,
    /// Sampling rate (0.0-1.0, e.g., 0.01 = 1% of batches)
    pub sample_rate: Option<f32>,
    /// Maximum batches per second (rate limiting)
    pub max_batches_per_sec: Option<u32>,
}

/// Batch envelope sent to clients
///
/// Contains batch metadata (already decoded from Batch struct) plus
/// raw FlatBuffer payload. CLI decodes the payload.
#[derive(Debug, Clone, PartialEq)]
pub struct TapEnvelope {
    /// Workspace ID
    pub workspace_id: u32,
    /// Source identifier
    pub source_id: String,
    /// Batch type (0=Unknown, 1=Event, 2=Log, 3=Metric, 4=Trace)
    pub batch_type: u8,
    /// Source IP address
    pub source_ip: IpAddr,
    /// Number of items in this batch
    pub count: u32,
    /// Message offsets into payload
    pub offsets: Vec<u32>,
    /// Message lengths
    pub lengths: Vec<u32>,
    /// Raw FlatBuffer payload (untouched)
    pub payload: Bytes,
}

impl TapMessage {
    /// Encode message to bytes with length prefix
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(256);

        // Reserve space for length prefix (filled in at end)
        buf.put_u32(0);

        match self {
            TapMessage::Subscribe(req) => {
                buf.put_u8(MSG_SUBSCRIBE);
                req.encode(&mut buf);
            }
            TapMessage::Batch(envelope) => {
                buf.put_u8(MSG_BATCH);
                envelope.encode(&mut buf);
            }
            TapMessage::Heartbeat => {
                buf.put_u8(MSG_HEARTBEAT);
            }
            TapMessage::Error(msg) => {
                buf.put_u8(MSG_ERROR);
                encode_string(msg, &mut buf);
            }
        }

        // Write length prefix (excluding the 4-byte length field itself)
        let len = (buf.len() - 4) as u32;
        buf[0..4].copy_from_slice(&len.to_be_bytes());

        buf.freeze()
    }

    /// Decode message from bytes (without length prefix)
    ///
    /// Expects the payload after the length prefix has been read.
    pub fn decode(mut buf: Bytes) -> Result<Self> {
        if buf.is_empty() {
            return Err(TapError::Protocol("empty message".into()));
        }

        let msg_type = buf.get_u8();

        match msg_type {
            MSG_SUBSCRIBE => {
                let req = SubscribeRequest::decode(&mut buf)?;
                Ok(TapMessage::Subscribe(req))
            }
            MSG_BATCH => {
                let envelope = TapEnvelope::decode(&mut buf)?;
                Ok(TapMessage::Batch(envelope))
            }
            MSG_HEARTBEAT => Ok(TapMessage::Heartbeat),
            MSG_ERROR => {
                let msg = decode_string(&mut buf)?;
                Ok(TapMessage::Error(msg))
            }
            _ => Err(TapError::Protocol(format!(
                "unknown message type: {msg_type}"
            ))),
        }
    }
}

impl SubscribeRequest {
    /// Create a new subscribe request with no filters (match all)
    pub fn new() -> Self {
        Self::default()
    }

    /// Set workspace filter
    pub fn with_workspaces(mut self, ids: Vec<u32>) -> Self {
        self.workspace_ids = Some(ids);
        self
    }

    /// Set source filter
    pub fn with_sources(mut self, ids: Vec<String>) -> Self {
        self.source_ids = Some(ids);
        self
    }

    /// Set batch type filter
    pub fn with_types(mut self, types: Vec<u8>) -> Self {
        self.batch_types = Some(types);
        self
    }

    /// Set replay buffer request
    pub fn with_last_n(mut self, n: u32) -> Self {
        self.last_n = n;
        self
    }

    /// Set sampling rate
    pub fn with_sample_rate(mut self, rate: f32) -> Self {
        self.sample_rate = Some(rate.clamp(0.0, 1.0));
        self
    }

    /// Set rate limit
    pub fn with_rate_limit(mut self, batches_per_sec: u32) -> Self {
        self.max_batches_per_sec = Some(batches_per_sec);
        self
    }

    fn encode(&self, buf: &mut BytesMut) {
        // workspace_ids: Option<Vec<u32>>
        encode_option_vec_u32(&self.workspace_ids, buf);

        // source_ids: Option<Vec<String>>
        encode_option_vec_string(&self.source_ids, buf);

        // batch_types: Option<Vec<u8>>
        encode_option_vec_u8(&self.batch_types, buf);

        // last_n: u32
        buf.put_u32(self.last_n);

        // sample_rate: Option<f32>
        match self.sample_rate {
            Some(rate) => {
                buf.put_u8(1);
                buf.put_f32(rate);
            }
            None => buf.put_u8(0),
        }

        // max_batches_per_sec: Option<u32>
        match self.max_batches_per_sec {
            Some(rate) => {
                buf.put_u8(1);
                buf.put_u32(rate);
            }
            None => buf.put_u8(0),
        }
    }

    fn decode(buf: &mut Bytes) -> Result<Self> {
        let workspace_ids = decode_option_vec_u32(buf)?;
        let source_ids = decode_option_vec_string(buf)?;
        let batch_types = decode_option_vec_u8(buf)?;

        if buf.remaining() < 4 {
            return Err(TapError::Protocol("truncated subscribe request".into()));
        }
        let last_n = buf.get_u32();

        let sample_rate = decode_option_f32(buf)?;
        let max_batches_per_sec = decode_option_u32(buf)?;

        Ok(Self {
            workspace_ids,
            source_ids,
            batch_types,
            last_n,
            sample_rate,
            max_batches_per_sec,
        })
    }
}

impl TapEnvelope {
    fn encode(&self, buf: &mut BytesMut) {
        // workspace_id: u32
        buf.put_u32(self.workspace_id);

        // source_id: String
        encode_string(&self.source_id, buf);

        // batch_type: u8
        buf.put_u8(self.batch_type);

        // source_ip: IpAddr (1 byte tag + 4 or 16 bytes)
        encode_ip_addr(&self.source_ip, buf);

        // count: u32
        buf.put_u32(self.count);

        // offsets: Vec<u32>
        buf.put_u32(self.offsets.len() as u32);
        for offset in &self.offsets {
            buf.put_u32(*offset);
        }

        // lengths: Vec<u32>
        buf.put_u32(self.lengths.len() as u32);
        for len in &self.lengths {
            buf.put_u32(*len);
        }

        // payload: Bytes
        buf.put_u32(self.payload.len() as u32);
        buf.put_slice(&self.payload);
    }

    fn decode(buf: &mut Bytes) -> Result<Self> {
        if buf.remaining() < 4 {
            return Err(TapError::Protocol("truncated envelope".into()));
        }
        let workspace_id = buf.get_u32();

        let source_id = decode_string(buf)?;

        if buf.remaining() < 1 {
            return Err(TapError::Protocol("truncated envelope".into()));
        }
        let batch_type = buf.get_u8();

        let source_ip = decode_ip_addr(buf)?;

        if buf.remaining() < 4 {
            return Err(TapError::Protocol("truncated envelope".into()));
        }
        let count = buf.get_u32();

        let offsets = decode_vec_u32(buf)?;
        let lengths = decode_vec_u32(buf)?;

        if buf.remaining() < 4 {
            return Err(TapError::Protocol("truncated envelope".into()));
        }
        let payload_len = buf.get_u32() as usize;
        if buf.remaining() < payload_len {
            return Err(TapError::Protocol("truncated payload".into()));
        }
        let payload = buf.split_to(payload_len);

        Ok(Self {
            workspace_id,
            source_id,
            batch_type,
            source_ip,
            count,
            offsets,
            lengths,
            payload,
        })
    }
}

// ============================================================================
// Encoding helpers
// ============================================================================

fn encode_string(s: &str, buf: &mut BytesMut) {
    let bytes = s.as_bytes();
    buf.put_u32(bytes.len() as u32);
    buf.put_slice(bytes);
}

fn decode_string(buf: &mut Bytes) -> Result<String> {
    if buf.remaining() < 4 {
        return Err(TapError::Protocol("truncated string length".into()));
    }
    let len = buf.get_u32() as usize;
    if buf.remaining() < len {
        return Err(TapError::Protocol("truncated string".into()));
    }
    let bytes = buf.split_to(len);
    String::from_utf8(bytes.to_vec()).map_err(|e| TapError::Protocol(format!("invalid UTF-8: {e}")))
}

fn encode_ip_addr(ip: &IpAddr, buf: &mut BytesMut) {
    match ip {
        IpAddr::V4(v4) => {
            buf.put_u8(4);
            buf.put_slice(&v4.octets());
        }
        IpAddr::V6(v6) => {
            buf.put_u8(6);
            buf.put_slice(&v6.octets());
        }
    }
}

fn decode_ip_addr(buf: &mut Bytes) -> Result<IpAddr> {
    if buf.remaining() < 1 {
        return Err(TapError::Protocol("truncated IP address".into()));
    }
    let version = buf.get_u8();
    match version {
        4 => {
            if buf.remaining() < 4 {
                return Err(TapError::Protocol("truncated IPv4".into()));
            }
            let mut octets = [0u8; 4];
            buf.copy_to_slice(&mut octets);
            Ok(IpAddr::V4(Ipv4Addr::from(octets)))
        }
        6 => {
            if buf.remaining() < 16 {
                return Err(TapError::Protocol("truncated IPv6".into()));
            }
            let mut octets = [0u8; 16];
            buf.copy_to_slice(&mut octets);
            Ok(IpAddr::V6(Ipv6Addr::from(octets)))
        }
        _ => Err(TapError::Protocol(format!("invalid IP version: {version}"))),
    }
}

fn encode_option_vec_u32(opt: &Option<Vec<u32>>, buf: &mut BytesMut) {
    match opt {
        Some(vec) => {
            buf.put_u8(1);
            buf.put_u32(vec.len() as u32);
            for v in vec {
                buf.put_u32(*v);
            }
        }
        None => buf.put_u8(0),
    }
}

fn decode_option_vec_u32(buf: &mut Bytes) -> Result<Option<Vec<u32>>> {
    if buf.remaining() < 1 {
        return Err(TapError::Protocol("truncated option".into()));
    }
    if buf.get_u8() == 0 {
        return Ok(None);
    }
    Ok(Some(decode_vec_u32(buf)?))
}

fn decode_vec_u32(buf: &mut Bytes) -> Result<Vec<u32>> {
    if buf.remaining() < 4 {
        return Err(TapError::Protocol("truncated vec length".into()));
    }
    let len = buf.get_u32() as usize;
    if buf.remaining() < len * 4 {
        return Err(TapError::Protocol("truncated vec".into()));
    }
    let mut vec = Vec::with_capacity(len);
    for _ in 0..len {
        vec.push(buf.get_u32());
    }
    Ok(vec)
}

fn encode_option_vec_u8(opt: &Option<Vec<u8>>, buf: &mut BytesMut) {
    match opt {
        Some(vec) => {
            buf.put_u8(1);
            buf.put_u32(vec.len() as u32);
            buf.put_slice(vec);
        }
        None => buf.put_u8(0),
    }
}

fn decode_option_vec_u8(buf: &mut Bytes) -> Result<Option<Vec<u8>>> {
    if buf.remaining() < 1 {
        return Err(TapError::Protocol("truncated option".into()));
    }
    if buf.get_u8() == 0 {
        return Ok(None);
    }
    if buf.remaining() < 4 {
        return Err(TapError::Protocol("truncated vec length".into()));
    }
    let len = buf.get_u32() as usize;
    if buf.remaining() < len {
        return Err(TapError::Protocol("truncated vec".into()));
    }
    let bytes = buf.split_to(len);
    Ok(Some(bytes.to_vec()))
}

fn encode_option_vec_string(opt: &Option<Vec<String>>, buf: &mut BytesMut) {
    match opt {
        Some(vec) => {
            buf.put_u8(1);
            buf.put_u32(vec.len() as u32);
            for s in vec {
                encode_string(s, buf);
            }
        }
        None => buf.put_u8(0),
    }
}

fn decode_option_vec_string(buf: &mut Bytes) -> Result<Option<Vec<String>>> {
    if buf.remaining() < 1 {
        return Err(TapError::Protocol("truncated option".into()));
    }
    if buf.get_u8() == 0 {
        return Ok(None);
    }
    if buf.remaining() < 4 {
        return Err(TapError::Protocol("truncated vec length".into()));
    }
    let len = buf.get_u32() as usize;
    let mut vec = Vec::with_capacity(len);
    for _ in 0..len {
        vec.push(decode_string(buf)?);
    }
    Ok(Some(vec))
}

fn decode_option_f32(buf: &mut Bytes) -> Result<Option<f32>> {
    if buf.remaining() < 1 {
        return Err(TapError::Protocol("truncated option".into()));
    }
    if buf.get_u8() == 0 {
        return Ok(None);
    }
    if buf.remaining() < 4 {
        return Err(TapError::Protocol("truncated f32".into()));
    }
    Ok(Some(buf.get_f32()))
}

fn decode_option_u32(buf: &mut Bytes) -> Result<Option<u32>> {
    if buf.remaining() < 1 {
        return Err(TapError::Protocol("truncated option".into()));
    }
    if buf.get_u8() == 0 {
        return Ok(None);
    }
    if buf.remaining() < 4 {
        return Err(TapError::Protocol("truncated u32".into()));
    }
    Ok(Some(buf.get_u32()))
}

/// Read exactly 4 bytes for length prefix
pub fn read_length_prefix(buf: &[u8]) -> Option<u32> {
    if buf.len() < 4 {
        return None;
    }
    Some(u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]))
}

#[cfg(test)]
#[path = "protocol_test.rs"]
mod tests;
