//! Source identification types
//!
//! `SourceId` identifies the origin of a batch for routing decisions.

use std::fmt;

/// Source identifier for routing decisions
///
/// Each source (TCP, Syslog, etc.) has a unique identifier that the routing
/// system uses to determine which sinks should receive its batches.
///
/// # Example
///
/// ```
/// use tell_protocol::SourceId;
///
/// let source = SourceId::new("tcp_main");
/// assert_eq!(source.as_str(), "tcp_main");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SourceId(String);

impl SourceId {
    /// Create a new source ID
    #[inline]
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Get the source ID as a string slice
    #[inline]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SourceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&str> for SourceId {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

impl From<String> for SourceId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl AsRef<str> for SourceId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Default for SourceId {
    fn default() -> Self {
        Self::new("unknown")
    }
}
