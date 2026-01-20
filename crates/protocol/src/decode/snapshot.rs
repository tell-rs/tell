//! Snapshot decoding from FlatBuffer payloads
//!
//! Parses SnapshotData FlatBuffer containing connector snapshots.
//!
//! # Schema Reference (snapshot.fbs)
//!
//! ```text
//! table Snapshot {
//!     timestamp:uint64 (id: 0);
//!     source:string (id: 1);
//!     entity:string (id: 2);
//!     payload:[ubyte] (id: 3);
//! }
//! table SnapshotData { snapshots:[Snapshot] (required); }
//! ```

use crate::{ProtocolError, Result};
use super::table::{FlatTable, read_u32};

// =============================================================================
// Decoded Snapshot
// =============================================================================

/// A decoded snapshot from SnapshotData
#[derive(Debug, Clone)]
pub struct DecodedSnapshot<'a> {
    /// Timestamp in milliseconds since epoch
    pub timestamp: u64,
    /// Connector source name (e.g., "github", "stripe", "linear")
    pub source: Option<&'a str>,
    /// Entity identifier (e.g., "user/repo", "acct_123")
    pub entity: Option<&'a str>,
    /// JSON payload bytes
    pub payload: &'a [u8],
}

// =============================================================================
// SnapshotData Parser
// =============================================================================

/// Parse SnapshotData FlatBuffer from bytes
///
/// # Layout
///
/// SnapshotData table:
/// - Field 0: snapshots (vector of Snapshot tables)
pub fn decode_snapshot_data(buf: &[u8]) -> Result<Vec<DecodedSnapshot<'_>>> {
    if buf.len() < 8 {
        return Err(ProtocolError::too_short(8, buf.len()));
    }

    // Read root offset
    let root_offset = read_u32(buf, 0)? as usize;
    if root_offset >= buf.len() {
        return Err(ProtocolError::invalid_flatbuffer(
            "root offset out of bounds",
        ));
    }

    // Parse SnapshotData table
    let table = FlatTable::parse(buf, root_offset)?;

    // Field 0: snapshots vector
    let snapshots_vec = table
        .read_vector_of_tables(0)?
        .ok_or_else(|| ProtocolError::missing_field("snapshots"))?;

    let mut snapshots = Vec::with_capacity(snapshots_vec.len());

    for snapshot_table in snapshots_vec {
        let snapshot = parse_snapshot(&snapshot_table)?;
        snapshots.push(snapshot);
    }

    Ok(snapshots)
}

/// Parse a single Snapshot table
fn parse_snapshot<'a>(table: &FlatTable<'a>) -> Result<DecodedSnapshot<'a>> {
    // Field 0: timestamp (u64)
    let timestamp = table.read_u64(0, 0);

    // Field 1: source (string)
    let source = table.read_string(1)?;

    // Field 2: entity (string)
    let entity = table.read_string(2)?;

    // Field 3: payload (vector of ubyte)
    let payload = table.read_bytes(3)?.unwrap_or(&[]);

    Ok(DecodedSnapshot {
        timestamp,
        source,
        entity,
        payload,
    })
}
