//! Schema types for Tell protocol
//!
//! These types mirror the FlatBuffers schema definitions and are used
//! for routing decisions within the pipeline.

/// Schema type for routing and streaming (matches FlatBuffers SchemaType enum)
///
/// Used by Tell to determine how to process incoming data.
/// This must stay in sync with `schema/batch.fbs`.
///
/// NOTE: These values are used on the wire and must match BatchType::to_u8()
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum SchemaType {
    /// Default value (should not be used in practice)
    Unknown = 0,
    /// Product analytics events
    Event = 1,
    /// Structured logs (includes syslog routed as Log)
    Log = 2,
    /// Performance metrics (gauges, counters, histograms)
    Metric = 3,
    /// Distributed tracing spans
    Trace = 4,
    /// External data source snapshots (connectors)
    Snapshot = 5,
}

impl SchemaType {
    /// Parse schema type from raw byte value
    #[inline]
    pub const fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Event,
            2 => Self::Log,
            3 => Self::Metric,
            4 => Self::Trace,
            5 => Self::Snapshot,
            _ => Self::Unknown,
        }
    }

    /// Convert to raw byte value
    #[inline]
    pub const fn as_u8(self) -> u8 {
        self as u8
    }

    /// Check if this schema type is supported for processing
    #[inline]
    pub const fn is_supported(self) -> bool {
        matches!(self, Self::Event | Self::Log | Self::Snapshot)
    }

    /// Get the string name of this schema type
    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Event => "event",
            Self::Log => "log",
            Self::Metric => "metric",
            Self::Trace => "trace",
            Self::Snapshot => "snapshot",
        }
    }
}

impl std::fmt::Display for SchemaType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Internal batch type for pipeline routing
///
/// This is the internal classification used by the pipeline,
/// separate from the wire protocol `SchemaType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BatchType {
    /// Product analytics events
    Event,
    /// Structured logs (from FlatBuffers protocol)
    Log,
    /// Raw syslog protocol (converted to Log internally)
    Syslog,
    /// Performance metrics (experimental)
    Metric,
    /// Distributed traces (experimental)
    Trace,
    /// External data source snapshots (connectors)
    Snapshot,
}

impl BatchType {
    /// Convert from wire protocol SchemaType to internal BatchType
    #[inline]
    pub const fn from_schema_type(schema_type: SchemaType) -> Option<Self> {
        match schema_type {
            SchemaType::Event => Some(Self::Event),
            SchemaType::Log => Some(Self::Log),
            SchemaType::Metric => Some(Self::Metric),
            SchemaType::Trace => Some(Self::Trace),
            SchemaType::Snapshot => Some(Self::Snapshot),
            SchemaType::Unknown => None,
        }
    }

    /// Check if this batch type contains events
    #[inline]
    pub const fn is_event(self) -> bool {
        matches!(self, Self::Event)
    }

    /// Check if this batch type contains logs
    #[inline]
    pub const fn is_log(self) -> bool {
        matches!(self, Self::Log | Self::Syslog)
    }

    /// Check if this batch type contains snapshots
    #[inline]
    pub const fn is_snapshot(self) -> bool {
        matches!(self, Self::Snapshot)
    }

    /// Get the string name of this batch type
    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Event => "event",
            Self::Log => "log",
            Self::Syslog => "syslog",
            Self::Metric => "metric",
            Self::Trace => "trace",
            Self::Snapshot => "snapshot",
        }
    }
}

impl std::fmt::Display for BatchType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl BatchType {
    /// Convert to wire protocol representation
    ///
    /// NOTE: These values must match SchemaType for 1:1 wire mapping.
    /// Syslog maps to Log (2) on the wire since it's an internal routing variant.
    #[inline]
    pub const fn to_u8(self) -> u8 {
        match self {
            Self::Event => 1,
            Self::Log => 2,
            Self::Syslog => 2, // Syslog routes as Log on wire
            Self::Metric => 3,
            Self::Trace => 4,
            Self::Snapshot => 5,
        }
    }
}

impl TryFrom<u8> for BatchType {
    type Error = ();

    /// Convert from wire protocol value to BatchType
    ///
    /// Note: Value 2 maps to Log (not Syslog, which is internal-only)
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Event),
            2 => Ok(Self::Log),
            3 => Ok(Self::Metric),
            4 => Ok(Self::Trace),
            5 => Ok(Self::Snapshot),
            _ => Err(()),
        }
    }
}
