//! Workspace identifier type
//!
//! `WorkspaceId` identifies a tenant/workspace in the system.
//! API keys map to workspace IDs for multi-tenant isolation.
//!
//! Uses `u32` to match Go's `type WorkspaceID uint32` for maximum performance.
//! This eliminates atomic operations and provides zero-cost cloning via `Copy`.

use std::fmt;

/// Workspace identifier
///
/// A lightweight handle that identifies a workspace/tenant.
/// Uses `u32` internally for zero-cost cloning and optimal performance.
///
/// # Performance
///
/// - `Copy` trait - clone is a simple register copy (zero cost)
/// - Integer hash - trivial hash computation
/// - 4 bytes - fits in a register, excellent cache behavior
/// - No atomic operations - unlike `Arc<str>`
///
/// # Example
///
/// ```
/// use cdp_auth::WorkspaceId;
///
/// let ws = WorkspaceId::new(1);
/// assert_eq!(ws.as_u32(), 1);
///
/// // Zero-cost clone (Copy)
/// let ws2 = ws;
/// assert_eq!(ws, ws2);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WorkspaceId(u32);

impl WorkspaceId {
    /// Create a new workspace ID
    #[inline]
    pub const fn new(id: u32) -> Self {
        Self(id)
    }

    /// Get the workspace ID as a u32
    #[inline]
    pub const fn as_u32(&self) -> u32 {
        self.0
    }
}

impl fmt::Display for WorkspaceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u32> for WorkspaceId {
    #[inline]
    fn from(id: u32) -> Self {
        Self::new(id)
    }
}

impl From<WorkspaceId> for u32 {
    #[inline]
    fn from(ws: WorkspaceId) -> Self {
        ws.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let ws = WorkspaceId::new(42);
        assert_eq!(ws.as_u32(), 42);
    }

    #[test]
    fn test_from_u32() {
        let ws: WorkspaceId = 123.into();
        assert_eq!(ws.as_u32(), 123);
    }

    #[test]
    fn test_into_u32() {
        let ws = WorkspaceId::new(456);
        let val: u32 = ws.into();
        assert_eq!(val, 456);
    }

    #[test]
    fn test_display() {
        let ws = WorkspaceId::new(789);
        assert_eq!(ws.to_string(), "789");
    }

    #[test]
    fn test_copy() {
        let ws1 = WorkspaceId::new(1);
        let ws2 = ws1; // Copy, not move
        let ws3 = ws1; // Can copy again
        assert_eq!(ws1, ws2);
        assert_eq!(ws1, ws3);
    }

    #[test]
    fn test_equality() {
        let ws1 = WorkspaceId::new(100);
        let ws2 = WorkspaceId::new(100);
        let ws3 = WorkspaceId::new(200);

        assert_eq!(ws1, ws2);
        assert_ne!(ws1, ws3);
    }

    #[test]
    fn test_hash() {
        use std::collections::HashSet;

        let mut set = HashSet::new();
        set.insert(WorkspaceId::new(1));
        set.insert(WorkspaceId::new(2));
        set.insert(WorkspaceId::new(1)); // Duplicate

        assert_eq!(set.len(), 2);
        assert!(set.contains(&WorkspaceId::new(1)));
        assert!(set.contains(&WorkspaceId::new(2)));
    }

    #[test]
    fn test_zero() {
        let ws = WorkspaceId::new(0);
        assert_eq!(ws.as_u32(), 0);
        assert_eq!(ws.to_string(), "0");
    }

    #[test]
    fn test_max() {
        let ws = WorkspaceId::new(u32::MAX);
        assert_eq!(ws.as_u32(), u32::MAX);
        assert_eq!(ws.to_string(), "4294967295");
    }

    #[test]
    fn test_size() {
        // WorkspaceId should be exactly 4 bytes (u32)
        assert_eq!(std::mem::size_of::<WorkspaceId>(), 4);
    }

    #[test]
    fn test_const_new() {
        // Can be used in const context
        const WS: WorkspaceId = WorkspaceId::new(42);
        assert_eq!(WS.as_u32(), 42);
    }
}
