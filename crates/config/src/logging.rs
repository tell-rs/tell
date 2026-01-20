//! Logging configuration
//!
//! Controls the internal logging behavior of Tell.

use serde::Deserialize;

/// Log level
#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    /// Trace level - very verbose
    Trace,
    /// Debug level - debugging information
    Debug,
    /// Info level - normal operation (default)
    #[default]
    Info,
    /// Warn level - warnings only
    Warn,
    /// Error level - errors only
    Error,
}

impl LogLevel {
    /// Convert to tracing level filter string
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Trace => "trace",
            Self::Debug => "debug",
            Self::Info => "info",
            Self::Warn => "warn",
            Self::Error => "error",
        }
    }
}

/// Log output format
#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    /// Human-readable console output (default)
    #[default]
    Console,
    /// JSON structured logging
    Json,
}

/// Log output destination
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum LogOutput {
    /// Write to stdout (default)
    #[default]
    Stdout,
    /// Write to stderr
    Stderr,
    /// Write to a file
    #[serde(untagged)]
    File(String),
}

/// Logging configuration
///
/// # Example
///
/// ```toml
/// [log]
/// level = "info"
/// format = "console"
/// output = "stdout"
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct LogConfig {
    /// Log level (trace, debug, info, warn, error)
    /// Default: info
    pub level: LogLevel,

    /// Output format (console, json)
    /// Default: console
    pub format: LogFormat,

    /// Output destination (stdout, stderr, or file path)
    /// Default: stdout
    pub output: LogOutput,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: LogLevel::Info,
            format: LogFormat::Console,
            output: LogOutput::Stdout,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = LogConfig::default();
        assert_eq!(config.level, LogLevel::Info);
        assert_eq!(config.format, LogFormat::Console);
        assert_eq!(config.output, LogOutput::Stdout);
    }

    #[test]
    fn test_deserialize_empty() {
        let config: LogConfig = toml::from_str("").unwrap();
        assert_eq!(config.level, LogLevel::Info);
        assert_eq!(config.format, LogFormat::Console);
    }

    #[test]
    fn test_deserialize_full() {
        let toml = r#"
level = "debug"
format = "json"
output = "stderr"
"#;
        let config: LogConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.level, LogLevel::Debug);
        assert_eq!(config.format, LogFormat::Json);
        assert_eq!(config.output, LogOutput::Stderr);
    }

    #[test]
    fn test_deserialize_all_levels() {
        for (s, expected) in [
            ("trace", LogLevel::Trace),
            ("debug", LogLevel::Debug),
            ("info", LogLevel::Info),
            ("warn", LogLevel::Warn),
            ("error", LogLevel::Error),
        ] {
            let toml = format!("level = \"{}\"", s);
            let config: LogConfig = toml::from_str(&toml).unwrap();
            assert_eq!(config.level, expected);
        }
    }

    #[test]
    fn test_level_as_str() {
        assert_eq!(LogLevel::Trace.as_str(), "trace");
        assert_eq!(LogLevel::Debug.as_str(), "debug");
        assert_eq!(LogLevel::Info.as_str(), "info");
        assert_eq!(LogLevel::Warn.as_str(), "warn");
        assert_eq!(LogLevel::Error.as_str(), "error");
    }
}
