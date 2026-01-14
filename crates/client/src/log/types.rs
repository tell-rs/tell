//! Log types for structured logging
//!
//! Matches the LogLevel and LogEventType enums in `log.fbs`.

/// Log severity levels (RFC 5424 syslog standard + TRACE)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum LogLevel {
    /// System is unusable
    Emergency = 0,
    /// Action must be taken immediately
    Alert = 1,
    /// Critical conditions
    Critical = 2,
    /// Error conditions
    Error = 3,
    /// Warning conditions
    Warning = 4,
    /// Normal but significant condition
    Notice = 5,
    /// Informational messages
    #[default]
    Info = 6,
    /// Debug-level messages
    Debug = 7,
    /// Detailed tracing information
    Trace = 8,
}

impl LogLevel {
    /// Convert to u8 for FlatBuffer encoding
    #[inline]
    pub const fn as_u8(self) -> u8 {
        self as u8
    }

    /// Create from u8 (returns Info for invalid values)
    #[inline]
    pub const fn from_u8(value: u8) -> Self {
        match value {
            0 => Self::Emergency,
            1 => Self::Alert,
            2 => Self::Critical,
            3 => Self::Error,
            4 => Self::Warning,
            5 => Self::Notice,
            6 => Self::Info,
            7 => Self::Debug,
            8 => Self::Trace,
            _ => Self::Info,
        }
    }
}

/// Log-specific event types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum LogEventType {
    /// Default/unspecified log type
    Unknown = 0,
    /// Standard log collection
    #[default]
    Log = 1,
    /// Log enrichment/annotation
    Enrich = 2,
}

impl LogEventType {
    /// Convert to u8 for FlatBuffer encoding
    #[inline]
    pub const fn as_u8(self) -> u8 {
        self as u8
    }

    /// Create from u8 (returns Log for invalid values)
    #[inline]
    pub const fn from_u8(value: u8) -> Self {
        match value {
            0 => Self::Unknown,
            1 => Self::Log,
            2 => Self::Enrich,
            _ => Self::Log,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_level_values() {
        assert_eq!(LogLevel::Emergency.as_u8(), 0);
        assert_eq!(LogLevel::Alert.as_u8(), 1);
        assert_eq!(LogLevel::Critical.as_u8(), 2);
        assert_eq!(LogLevel::Error.as_u8(), 3);
        assert_eq!(LogLevel::Warning.as_u8(), 4);
        assert_eq!(LogLevel::Notice.as_u8(), 5);
        assert_eq!(LogLevel::Info.as_u8(), 6);
        assert_eq!(LogLevel::Debug.as_u8(), 7);
        assert_eq!(LogLevel::Trace.as_u8(), 8);
    }

    #[test]
    fn test_log_level_from_u8() {
        assert_eq!(LogLevel::from_u8(0), LogLevel::Emergency);
        assert_eq!(LogLevel::from_u8(1), LogLevel::Alert);
        assert_eq!(LogLevel::from_u8(2), LogLevel::Critical);
        assert_eq!(LogLevel::from_u8(3), LogLevel::Error);
        assert_eq!(LogLevel::from_u8(4), LogLevel::Warning);
        assert_eq!(LogLevel::from_u8(5), LogLevel::Notice);
        assert_eq!(LogLevel::from_u8(6), LogLevel::Info);
        assert_eq!(LogLevel::from_u8(7), LogLevel::Debug);
        assert_eq!(LogLevel::from_u8(8), LogLevel::Trace);
        assert_eq!(LogLevel::from_u8(255), LogLevel::Info); // invalid -> default
    }

    #[test]
    fn test_log_level_default() {
        assert_eq!(LogLevel::default(), LogLevel::Info);
    }

    #[test]
    fn test_log_event_type_values() {
        assert_eq!(LogEventType::Unknown.as_u8(), 0);
        assert_eq!(LogEventType::Log.as_u8(), 1);
        assert_eq!(LogEventType::Enrich.as_u8(), 2);
    }

    #[test]
    fn test_log_event_type_from_u8() {
        assert_eq!(LogEventType::from_u8(0), LogEventType::Unknown);
        assert_eq!(LogEventType::from_u8(1), LogEventType::Log);
        assert_eq!(LogEventType::from_u8(2), LogEventType::Enrich);
        assert_eq!(LogEventType::from_u8(255), LogEventType::Log); // invalid -> default
    }

    #[test]
    fn test_log_event_type_default() {
        assert_eq!(LogEventType::default(), LogEventType::Log);
    }
}
