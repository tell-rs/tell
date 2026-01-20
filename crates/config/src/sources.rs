//! Source configuration types
//!
//! Configuration for all data sources (TCP, Syslog, etc.)
//! Sources produce data that flows into the pipeline.

use serde::Deserialize;
use std::time::Duration;

/// Container for all source configurations
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct SourcesConfig {
    /// TCP sources (primary, high-performance)
    /// Can have multiple instances on different ports
    #[serde(default)]
    pub tcp: Vec<TcpSourceConfig>,

    /// TCP debug source (hex dump output for debugging)
    /// Separate from main TCP to avoid performance impact
    #[serde(default)]
    pub tcp_debug: Option<TcpDebugSourceConfig>,

    /// HTTP REST API source (for JSONL ingestion)
    #[serde(default)]
    pub http: Option<HttpSourceConfig>,

    /// Syslog TCP source
    #[serde(default)]
    pub syslog_tcp: Option<SyslogTcpSourceConfig>,

    /// Syslog UDP source
    #[serde(default)]
    pub syslog_udp: Option<SyslogUdpSourceConfig>,
}

/// TCP source configuration
///
/// High-performance FlatBuffer protocol source.
///
/// # Example
///
/// ```toml
/// [[sources.tcp]]
/// port = 50000
///
/// [[sources.tcp]]
/// port = 8081
/// forwarding_mode = true
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct TcpSourceConfig {
    /// Whether this source is enabled
    /// Default: true (enabled when config is present)
    pub enabled: bool,

    /// Bind address
    /// Default: "0.0.0.0"
    pub address: String,

    /// Listen port (required - no sensible default)
    pub port: u16,

    /// Buffer size for reads (bytes)
    /// Default: uses global.buffer_size
    pub buffer_size: Option<usize>,

    /// Channel queue size
    /// Default: uses global.queue_size
    pub queue_size: Option<usize>,

    /// Enable TCP_NODELAY (disable Nagle's algorithm)
    /// Default: true
    pub no_delay: bool,

    /// Batch flush interval
    /// Default: 100ms
    #[serde(with = "humantime_serde")]
    pub flush_interval: Duration,

    /// Enable forwarding mode (trust source_ip field from upstream Tell instances)
    /// Default: false (secure default - never trust client-provided IPs)
    pub forwarding_mode: bool,
}

impl Default for TcpSourceConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            address: "0.0.0.0".into(),
            port: 50000,
            buffer_size: None,
            queue_size: None,
            no_delay: true,
            flush_interval: Duration::from_millis(100),
            forwarding_mode: false,
        }
    }
}

/// TCP debug source configuration
///
/// Same as TCP but outputs hex dump for debugging.
/// Kept separate to avoid conditional logic in hot path.
///
/// # Example
///
/// ```toml
/// [sources.tcp_debug]
/// port = 50001
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct TcpDebugSourceConfig {
    /// Whether this source is enabled
    /// Default: true
    pub enabled: bool,

    /// Bind address
    /// Default: "0.0.0.0"
    pub address: String,

    /// Listen port
    pub port: u16,

    /// Buffer size for reads (bytes)
    pub buffer_size: Option<usize>,

    /// Channel queue size
    pub queue_size: Option<usize>,

    /// Enable TCP_NODELAY
    /// Default: true
    pub no_delay: bool,

    /// Batch flush interval
    /// Default: 100ms
    #[serde(with = "humantime_serde")]
    pub flush_interval: Duration,
}

impl Default for TcpDebugSourceConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            address: "0.0.0.0".into(),
            port: 50001,
            buffer_size: None,
            queue_size: None,
            no_delay: true,
            flush_interval: Duration::from_millis(100),
        }
    }
}

/// Syslog TCP source configuration
///
/// Receives syslog messages over TCP (RFC 3164 / RFC 5424).
///
/// # Example
///
/// ```toml
/// [sources.syslog_tcp]
/// port = 514
/// workspace_id = "syslog"
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct SyslogTcpSourceConfig {
    /// Whether this source is enabled
    /// Default: true
    pub enabled: bool,

    /// Bind address
    /// Default: "0.0.0.0"
    pub address: String,

    /// Listen port
    /// Default: 514
    pub port: u16,

    /// Buffer size for reads (bytes)
    pub buffer_size: Option<usize>,

    /// Channel queue size
    pub queue_size: Option<usize>,

    /// Enable TCP_NODELAY
    /// Default: true
    pub no_delay: bool,

    /// Batch flush interval
    /// Default: 100ms
    #[serde(with = "humantime_serde")]
    pub flush_interval: Duration,

    /// Workspace ID (syslog doesn't have API keys, so we assign a workspace)
    pub workspace_id: Option<String>,

    /// Maximum syslog message size
    /// Default: 8192 (8KB)
    pub max_message_size: usize,

    /// Connection timeout
    /// Default: 30s
    #[serde(with = "humantime_serde")]
    pub connection_timeout: Duration,
}

impl Default for SyslogTcpSourceConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            address: "0.0.0.0".into(),
            port: 514,
            buffer_size: None,
            queue_size: None,
            no_delay: true,
            flush_interval: Duration::from_millis(100),
            workspace_id: None,
            max_message_size: 8192,
            connection_timeout: Duration::from_secs(30),
        }
    }
}

/// Syslog UDP source configuration
///
/// Receives syslog messages over UDP (RFC 3164 / RFC 5424).
///
/// # Example
///
/// ```toml
/// [sources.syslog_udp]
/// port = 514
/// workspace_id = "syslog"
/// num_workers = 4
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct SyslogUdpSourceConfig {
    /// Whether this source is enabled
    /// Default: true
    pub enabled: bool,

    /// Bind address
    /// Default: "0.0.0.0"
    pub address: String,

    /// Listen port
    /// Default: 514
    pub port: u16,

    /// Socket buffer size (bytes)
    pub buffer_size: Option<usize>,

    /// Number of worker threads for UDP processing
    /// Default: 4
    pub num_workers: usize,

    /// Workspace ID (syslog doesn't have API keys, so we assign a workspace)
    pub workspace_id: Option<String>,

    /// Maximum syslog message size
    /// Default: 8192 (8KB)
    pub max_message_size: usize,
}

impl Default for SyslogUdpSourceConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            address: "0.0.0.0".into(),
            port: 514,
            buffer_size: None,
            num_workers: 4,
            workspace_id: None,
            max_message_size: 8192,
        }
    }
}

/// HTTP REST API source configuration
///
/// Receives events and logs via HTTP POST in JSONL format.
///
/// # Example
///
/// ```toml
/// [sources.http]
/// port = 8080
/// max_payload_size = 10485760
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct HttpSourceConfig {
    /// Whether this source is enabled
    /// Default: true
    pub enabled: bool,

    /// Bind address
    /// Default: "0.0.0.0"
    pub address: String,

    /// Listen port
    /// Default: 8080
    pub port: u16,

    /// Maximum request payload size in bytes
    /// Default: 10MB
    pub max_payload_size: usize,

    /// Maximum items per batch before flush
    /// Default: 500
    pub batch_size: usize,

    /// Enable CORS (for browser clients)
    /// Default: false
    pub cors_enabled: bool,
}

impl Default for HttpSourceConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            address: "0.0.0.0".into(),
            port: 8080,
            max_payload_size: 10 * 1024 * 1024, // 10MB
            batch_size: 500,
            cors_enabled: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tcp_defaults() {
        let config = TcpSourceConfig::default();
        assert!(config.enabled);
        assert_eq!(config.address, "0.0.0.0");
        assert_eq!(config.port, 50000);
        assert!(config.no_delay);
        assert!(!config.forwarding_mode);
        assert_eq!(config.flush_interval, Duration::from_millis(100));
    }

    #[test]
    fn test_tcp_debug_defaults() {
        let config = TcpDebugSourceConfig::default();
        assert!(config.enabled);
        assert_eq!(config.port, 50001);
    }

    #[test]
    fn test_syslog_tcp_defaults() {
        let config = SyslogTcpSourceConfig::default();
        assert!(config.enabled);
        assert_eq!(config.port, 514);
        assert_eq!(config.max_message_size, 8192);
        assert_eq!(config.connection_timeout, Duration::from_secs(30));
    }

    #[test]
    fn test_syslog_udp_defaults() {
        let config = SyslogUdpSourceConfig::default();
        assert!(config.enabled);
        assert_eq!(config.port, 514);
        assert_eq!(config.num_workers, 4);
    }

    #[test]
    fn test_deserialize_single_tcp() {
        let toml = r#"
[[tcp]]
port = 50000
"#;
        let config: SourcesConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.tcp.len(), 1);
        assert_eq!(config.tcp[0].port, 50000);
        assert!(config.tcp[0].enabled);
    }

    #[test]
    fn test_deserialize_multiple_tcp() {
        let toml = r#"
[[tcp]]
port = 50000
forwarding_mode = false

[[tcp]]
port = 8081
forwarding_mode = true
"#;
        let config: SourcesConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.tcp.len(), 2);
        assert_eq!(config.tcp[0].port, 50000);
        assert!(!config.tcp[0].forwarding_mode);
        assert_eq!(config.tcp[1].port, 8081);
        assert!(config.tcp[1].forwarding_mode);
    }

    #[test]
    fn test_deserialize_tcp_debug() {
        let toml = r#"
[tcp_debug]
port = 50001
"#;
        let config: SourcesConfig = toml::from_str(toml).unwrap();
        assert!(config.tcp_debug.is_some());
        assert_eq!(config.tcp_debug.unwrap().port, 50001);
    }

    #[test]
    fn test_deserialize_syslog() {
        let toml = r#"
[syslog_tcp]
port = 514
workspace_id = "syslog_workspace"

[syslog_udp]
port = 514
num_workers = 8
"#;
        let config: SourcesConfig = toml::from_str(toml).unwrap();

        let tcp = config.syslog_tcp.unwrap();
        assert_eq!(tcp.port, 514);
        assert_eq!(tcp.workspace_id, Some("syslog_workspace".into()));

        let udp = config.syslog_udp.unwrap();
        assert_eq!(udp.port, 514);
        assert_eq!(udp.num_workers, 8);
    }

    #[test]
    fn test_deserialize_with_durations() {
        let toml = r#"
[[tcp]]
port = 50000
flush_interval = "200ms"

[syslog_tcp]
port = 514
connection_timeout = "60s"
flush_interval = "1s"
"#;
        let config: SourcesConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.tcp[0].flush_interval, Duration::from_millis(200));

        let syslog = config.syslog_tcp.unwrap();
        assert_eq!(syslog.connection_timeout, Duration::from_secs(60));
        assert_eq!(syslog.flush_interval, Duration::from_secs(1));
    }

    #[test]
    fn test_deserialize_empty_sources() {
        let config: SourcesConfig = toml::from_str("").unwrap();
        assert!(config.tcp.is_empty());
        assert!(config.tcp_debug.is_none());
        assert!(config.syslog_tcp.is_none());
        assert!(config.syslog_udp.is_none());
    }

    #[test]
    fn test_deserialize_disabled() {
        let toml = r#"
[[tcp]]
port = 50000
enabled = false
"#;
        let config: SourcesConfig = toml::from_str(toml).unwrap();
        assert!(!config.tcp[0].enabled);
    }
}
