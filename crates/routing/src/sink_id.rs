//! Sink identifier type
//!
//! `SinkId` is a lightweight, Copy identifier for sinks.
//! Designed for zero-copy routing in the hot path.

use std::fmt;

/// Sink identifier for routing
///
/// A lightweight handle that identifies a sink in the routing table.
/// Designed to be `Copy` and fit in a register for maximum performance.
///
/// # Zero-Copy Design
///
/// - `Copy` trait: no heap allocation, passed by value
/// - Small size (u16): fits in register, cache-friendly
/// - No string operations in hot path
///
/// # Example
///
/// ```
/// use cdp_routing::SinkId;
///
/// let sink = SinkId::new(0);
/// let copy = sink;  // Copy, not move
/// assert_eq!(sink, copy);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SinkId(u16);

impl SinkId {
    /// Maximum number of sinks supported
    pub const MAX: u16 = u16::MAX;

    /// Create a new sink ID from a numeric index
    ///
    /// Sink IDs are assigned sequentially during routing table compilation.
    #[inline]
    #[must_use]
    pub const fn new(index: u16) -> Self {
        Self(index)
    }

    /// Get the numeric index of this sink
    #[inline]
    #[must_use]
    pub const fn index(self) -> u16 {
        self.0
    }

    /// Get the index as usize (for array indexing)
    #[inline]
    #[must_use]
    pub const fn as_usize(self) -> usize {
        self.0 as usize
    }
}

impl fmt::Display for SinkId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "sink:{}", self.0)
    }
}

impl From<u16> for SinkId {
    #[inline]
    fn from(index: u16) -> Self {
        Self::new(index)
    }
}

impl From<SinkId> for u16 {
    #[inline]
    fn from(id: SinkId) -> Self {
        id.0
    }
}

impl From<SinkId> for usize {
    #[inline]
    fn from(id: SinkId) -> Self {
        id.0 as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let id = SinkId::new(42);
        assert_eq!(id.index(), 42);
        assert_eq!(id.as_usize(), 42);
    }

    #[test]
    fn test_copy() {
        let id1 = SinkId::new(1);
        let id2 = id1; // Copy
        assert_eq!(id1, id2);
    }

    #[test]
    fn test_equality() {
        let id1 = SinkId::new(5);
        let id2 = SinkId::new(5);
        let id3 = SinkId::new(10);

        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
    }

    #[test]
    fn test_ordering() {
        let id1 = SinkId::new(1);
        let id2 = SinkId::new(2);
        let id3 = SinkId::new(3);

        assert!(id1 < id2);
        assert!(id2 < id3);
        assert!(id1 < id3);
    }

    #[test]
    fn test_hash() {
        use std::collections::HashSet;

        let mut set = HashSet::new();
        set.insert(SinkId::new(1));
        set.insert(SinkId::new(2));
        set.insert(SinkId::new(1)); // Duplicate

        assert_eq!(set.len(), 2);
        assert!(set.contains(&SinkId::new(1)));
        assert!(set.contains(&SinkId::new(2)));
    }

    #[test]
    fn test_display() {
        let id = SinkId::new(123);
        assert_eq!(id.to_string(), "sink:123");
    }

    #[test]
    fn test_from_u16() {
        let id: SinkId = 99u16.into();
        assert_eq!(id.index(), 99);
    }

    #[test]
    fn test_into_u16() {
        let id = SinkId::new(77);
        let value: u16 = id.into();
        assert_eq!(value, 77);
    }

    #[test]
    fn test_into_usize() {
        let id = SinkId::new(55);
        let value: usize = id.into();
        assert_eq!(value, 55);
    }

    #[test]
    fn test_max() {
        let id = SinkId::new(SinkId::MAX);
        assert_eq!(id.index(), u16::MAX);
    }

    #[test]
    fn test_const_new() {
        // Verify const fn works at compile time
        const ID: SinkId = SinkId::new(10);
        assert_eq!(ID.index(), 10);
    }

    #[test]
    fn test_size() {
        // Verify SinkId is small (2 bytes)
        assert_eq!(std::mem::size_of::<SinkId>(), 2);
    }

    #[test]
    fn test_array_indexing() {
        let sinks = ["stdout", "clickhouse", "disk"];
        let id = SinkId::new(1);
        assert_eq!(sinks[id.as_usize()], "clickhouse");
    }
}
