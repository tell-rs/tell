//! Routing error types

use thiserror::Error;

/// Result type for routing operations
pub type Result<T> = std::result::Result<T, RoutingError>;

/// Errors that can occur during routing table compilation
#[derive(Debug, Error)]
pub enum RoutingError {
    /// Sink name not found in sink registry
    #[error("unknown sink '{name}' in routing configuration")]
    UnknownSink {
        /// Name of the missing sink
        name: String,
    },

    /// Source name not found in source registry
    #[error("unknown source '{name}' in routing configuration")]
    UnknownSource {
        /// Name of the missing source
        name: String,
    },

    /// Duplicate route definition
    #[error("duplicate route for source '{source_name}'")]
    DuplicateRoute {
        /// Source that has duplicate routes
        source_name: String,
    },

    /// Empty sinks list in route
    #[error("route for source '{source_name}' has no sinks")]
    EmptySinks {
        /// Source with empty sinks
        source_name: String,
    },
}

impl RoutingError {
    /// Create an UnknownSink error
    #[inline]
    pub fn unknown_sink(name: impl Into<String>) -> Self {
        Self::UnknownSink { name: name.into() }
    }

    /// Create an UnknownSource error
    #[inline]
    pub fn unknown_source(name: impl Into<String>) -> Self {
        Self::UnknownSource { name: name.into() }
    }

    /// Create a DuplicateRoute error
    #[inline]
    pub fn duplicate_route(source_name: impl Into<String>) -> Self {
        Self::DuplicateRoute {
            source_name: source_name.into(),
        }
    }

    /// Create an EmptySinks error
    #[inline]
    pub fn empty_sinks(source_name: impl Into<String>) -> Self {
        Self::EmptySinks {
            source_name: source_name.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unknown_sink_error() {
        let err = RoutingError::unknown_sink("clickhouse");
        assert!(err.to_string().contains("clickhouse"));
        assert!(err.to_string().contains("unknown sink"));
    }

    #[test]
    fn test_unknown_source_error() {
        let err = RoutingError::unknown_source("tcp_main");
        assert!(err.to_string().contains("tcp_main"));
        assert!(err.to_string().contains("unknown source"));
    }

    #[test]
    fn test_duplicate_route_error() {
        let err = RoutingError::duplicate_route("tcp");
        assert!(err.to_string().contains("tcp"));
        assert!(err.to_string().contains("duplicate"));
    }

    #[test]
    fn test_empty_sinks_error() {
        let err = RoutingError::empty_sinks("syslog");
        assert!(err.to_string().contains("syslog"));
        assert!(err.to_string().contains("no sinks"));
    }
}
