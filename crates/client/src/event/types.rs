//! Event types for product analytics
//!
//! Matches the EventType enum in `event.fbs`.

/// Event types for different processing paths
///
/// Determines downstream processing and storage routing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum EventType {
    /// Default value (should not be used in production)
    #[default]
    Unknown = 0,
    /// User action tracking (page views, clicks, etc.) → events table
    Track = 1,
    /// User identification/traits updates → users table
    Identify = 2,
    /// Group membership/traits updates → users table
    Group = 3,
    /// Identity resolution/user merging → users table
    Alias = 4,
    /// Generic entity enrichment → metadata
    Enrich = 5,
    /// Session/device context updates → context table
    Context = 6,
}

impl EventType {
    /// Convert to u8 for FlatBuffer encoding
    #[inline]
    pub const fn as_u8(self) -> u8 {
        self as u8
    }

    /// Create from u8 (returns Unknown for invalid values)
    #[inline]
    pub const fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Track,
            2 => Self::Identify,
            3 => Self::Group,
            4 => Self::Alias,
            5 => Self::Enrich,
            6 => Self::Context,
            _ => Self::Unknown,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_type_values() {
        assert_eq!(EventType::Unknown.as_u8(), 0);
        assert_eq!(EventType::Track.as_u8(), 1);
        assert_eq!(EventType::Identify.as_u8(), 2);
        assert_eq!(EventType::Group.as_u8(), 3);
        assert_eq!(EventType::Alias.as_u8(), 4);
        assert_eq!(EventType::Enrich.as_u8(), 5);
        assert_eq!(EventType::Context.as_u8(), 6);
    }

    #[test]
    fn test_event_type_from_u8() {
        assert_eq!(EventType::from_u8(0), EventType::Unknown);
        assert_eq!(EventType::from_u8(1), EventType::Track);
        assert_eq!(EventType::from_u8(2), EventType::Identify);
        assert_eq!(EventType::from_u8(3), EventType::Group);
        assert_eq!(EventType::from_u8(4), EventType::Alias);
        assert_eq!(EventType::from_u8(5), EventType::Enrich);
        assert_eq!(EventType::from_u8(6), EventType::Context);
        assert_eq!(EventType::from_u8(255), EventType::Unknown);
    }

    #[test]
    fn test_event_type_default() {
        assert_eq!(EventType::default(), EventType::Unknown);
    }
}
