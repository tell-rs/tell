//! Configuration error types

use std::io;
use thiserror::Error;

/// Result type for configuration operations
pub type Result<T> = std::result::Result<T, ConfigError>;

/// Errors that can occur when loading or validating configuration
#[derive(Debug, Error)]
pub enum ConfigError {
    /// Failed to read configuration file
    #[error("failed to read config file '{path}': {source}")]
    IoError {
        /// Path to the file
        path: String,
        /// Underlying IO error
        #[source]
        source: io::Error,
    },

    /// Failed to parse TOML
    #[error("failed to parse config: {0}")]
    ParseError(#[from] toml::de::Error),

    /// Validation error - sink referenced in routing doesn't exist
    #[error("routing references unknown sink '{sink}'")]
    UnknownSink {
        /// Name of the missing sink
        sink: String,
    },

    /// Validation error - source referenced in routing doesn't exist
    #[error("routing references unknown source '{source_name}'")]
    UnknownSource {
        /// Name of the missing source
        source_name: String,
    },

    /// Validation error - duplicate port
    #[error("port {port} is used by multiple sources: {sources}")]
    DuplicatePort {
        /// The conflicting port
        port: u16,
        /// Sources using this port
        sources: String,
    },

    /// Validation error - required field missing
    #[error("{component} '{name}' is missing required field '{field}'")]
    MissingField {
        /// Component type (e.g., "sink", "source")
        component: &'static str,
        /// Name of the component
        name: String,
        /// Missing field name
        field: &'static str,
    },

    /// Validation error - invalid value
    #[error("{component} '{name}' has invalid {field}: {message}")]
    InvalidValue {
        /// Component type
        component: &'static str,
        /// Name of the component
        name: String,
        /// Field name
        field: &'static str,
        /// Error message
        message: String,
    },

    /// No sources enabled
    #[error("no sources are enabled - at least one source must be enabled")]
    NoSourcesEnabled,

    /// No sinks enabled
    #[error("no sinks are enabled - at least one sink must be enabled")]
    NoSinksEnabled,
}

impl ConfigError {
    /// Create an UnknownSink error
    pub fn unknown_sink(sink: impl Into<String>) -> Self {
        Self::UnknownSink { sink: sink.into() }
    }

    /// Create an UnknownSource error
    pub fn unknown_source(source_name: impl Into<String>) -> Self {
        Self::UnknownSource {
            source_name: source_name.into(),
        }
    }

    /// Create a DuplicatePort error
    pub fn duplicate_port(port: u16, sources: impl Into<String>) -> Self {
        Self::DuplicatePort {
            port,
            sources: sources.into(),
        }
    }

    /// Create a MissingField error
    pub fn missing_field(
        component: &'static str,
        name: impl Into<String>,
        field: &'static str,
    ) -> Self {
        Self::MissingField {
            component,
            name: name.into(),
            field,
        }
    }

    /// Create an InvalidValue error
    pub fn invalid_value(
        component: &'static str,
        name: impl Into<String>,
        field: &'static str,
        message: impl Into<String>,
    ) -> Self {
        Self::InvalidValue {
            component,
            name: name.into(),
            field,
            message: message.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unknown_sink_error() {
        let err = ConfigError::unknown_sink("clickhouse_main");
        assert!(err.to_string().contains("clickhouse_main"));
        assert!(err.to_string().contains("unknown sink"));
    }

    #[test]
    fn test_unknown_source_error() {
        let err = ConfigError::unknown_source("tcp_main");
        assert!(err.to_string().contains("tcp_main"));
        assert!(err.to_string().contains("unknown source"));
    }

    #[test]
    fn test_duplicate_port_error() {
        let err = ConfigError::duplicate_port(50000, "tcp, tcp_debug");
        assert!(err.to_string().contains("50000"));
        assert!(err.to_string().contains("tcp, tcp_debug"));
    }

    #[test]
    fn test_missing_field_error() {
        let err = ConfigError::missing_field("sink", "clickhouse_main", "host");
        assert!(err.to_string().contains("sink"));
        assert!(err.to_string().contains("clickhouse_main"));
        assert!(err.to_string().contains("host"));
    }

    #[test]
    fn test_invalid_value_error() {
        let err = ConfigError::invalid_value(
            "sink",
            "disk_plaintext",
            "rotation",
            "must be 'hourly' or 'daily'",
        );
        assert!(err.to_string().contains("disk_plaintext"));
        assert!(err.to_string().contains("rotation"));
    }

    #[test]
    fn test_no_sources_enabled() {
        let err = ConfigError::NoSourcesEnabled;
        assert!(err.to_string().contains("no sources"));
    }

    #[test]
    fn test_no_sinks_enabled() {
        let err = ConfigError::NoSinksEnabled;
        assert!(err.to_string().contains("no sinks"));
    }
}
