//! Sink configuration types
//!
//! Configuration for all data sinks (ClickHouse, disk, etc.)
//! Sinks consume data from the pipeline and write to destinations.
//!
//! Sinks are named instances, allowing multiple sinks of the same type
//! (e.g., multiple ClickHouse clusters, debug vs production disk paths).

use serde::Deserialize;
use std::collections::HashMap;
use std::time::Duration;

/// Container for all sink configurations
///
/// Sinks are stored as a map of name -> config.
///
/// # Example
///
/// ```toml
/// [sinks.stdout]
/// # uses all defaults
///
/// [sinks.disk_plaintext]
/// path = "logs/"
///
/// [sinks.clickhouse_prod]
/// type = "clickhouse"
/// host = "prod-cluster:9000"
///
/// [sinks.clickhouse_staging]
/// type = "clickhouse"
/// host = "staging:9000"
/// ```
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct SinksConfig {
    /// Named sink instances
    #[serde(flatten)]
    sinks: HashMap<String, SinkConfig>,
}

impl SinksConfig {
    /// Get a sink by name
    pub fn get(&self, name: &str) -> Option<&SinkConfig> {
        self.sinks.get(name)
    }

    /// Check if a sink exists
    pub fn contains(&self, name: &str) -> bool {
        self.sinks.contains_key(name)
    }

    /// Iterate over all sinks
    pub fn iter(&self) -> impl Iterator<Item = (&String, &SinkConfig)> {
        self.sinks.iter()
    }

    /// Get the number of configured sinks
    pub fn len(&self) -> usize {
        self.sinks.len()
    }

    /// Check if no sinks are configured
    pub fn is_empty(&self) -> bool {
        self.sinks.is_empty()
    }

    /// Get all sink names
    pub fn names(&self) -> impl Iterator<Item = &String> {
        self.sinks.keys()
    }
}

/// Configuration for a single sink instance
///
/// The sink type is inferred from the config key name if not specified:
/// - `stdout` / `stdout_*` -> Stdout
/// - `null` / `null_*` -> Null
/// - `disk_plaintext` / `disk_plaintext_*` -> DiskPlaintext
/// - `disk_binary` / `disk_binary_*` -> DiskBinary
/// - `clickhouse` / `clickhouse_*` -> ClickHouse
/// - `parquet` / `parquet_*` -> Parquet
/// - `forwarder` / `forwarder_*` -> Forwarder
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SinkConfig {
    /// Null sink - discards all data (for benchmarking)
    Null(NullSinkConfig),

    /// Stdout sink - human-readable debug output
    Stdout(StdoutSinkConfig),

    /// Binary disk sink - high-performance binary storage
    DiskBinary(DiskBinarySinkConfig),

    /// Plaintext disk sink - human-readable logs
    DiskPlaintext(DiskPlaintextSinkConfig),

    /// ClickHouse sink - real-time analytics database
    Clickhouse(ClickHouseSinkConfig),

    /// Parquet sink - columnar storage for data warehousing
    Parquet(ParquetSinkConfig),

    /// Forwarder sink - collector-to-collector forwarding
    Forwarder(ForwarderSinkConfig),
}

/// Per-sink metrics configuration
#[derive(Debug, Clone, Copy)]
pub struct SinkMetricsConfig {
    /// Whether per-sink metrics are enabled
    pub enabled: bool,
    /// Reporting interval
    pub interval: Duration,
}

impl SinkConfig {
    /// Check if the sink is enabled
    pub fn is_enabled(&self) -> bool {
        match self {
            Self::Null(c) => c.enabled,
            Self::Stdout(c) => c.enabled,
            Self::DiskBinary(c) => c.enabled,
            Self::DiskPlaintext(c) => c.enabled,
            Self::Clickhouse(c) => c.enabled,
            Self::Parquet(c) => c.enabled,
            Self::Forwarder(c) => c.enabled,
        }
    }

    /// Get the sink type name
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::Null(_) => "null",
            Self::Stdout(_) => "stdout",
            Self::DiskBinary(_) => "disk_binary",
            Self::DiskPlaintext(_) => "disk_plaintext",
            Self::Clickhouse(_) => "clickhouse",
            Self::Parquet(_) => "parquet",
            Self::Forwarder(_) => "forwarder",
        }
    }

    /// Get per-sink metrics configuration
    pub fn metrics_config(&self) -> SinkMetricsConfig {
        match self {
            Self::Null(c) => SinkMetricsConfig {
                enabled: c.metrics_enabled,
                interval: c.metrics_interval,
            },
            Self::Stdout(c) => SinkMetricsConfig {
                enabled: c.metrics_enabled,
                interval: c.metrics_interval,
            },
            Self::DiskBinary(c) => SinkMetricsConfig {
                enabled: c.metrics_enabled,
                interval: c.metrics_interval,
            },
            Self::DiskPlaintext(c) => SinkMetricsConfig {
                enabled: c.metrics_enabled,
                interval: c.metrics_interval,
            },
            Self::Clickhouse(c) => SinkMetricsConfig {
                enabled: c.metrics_enabled,
                interval: c.metrics_interval,
            },
            Self::Parquet(c) => SinkMetricsConfig {
                enabled: c.metrics_enabled,
                interval: c.metrics_interval,
            },
            Self::Forwarder(c) => SinkMetricsConfig {
                enabled: c.metrics_enabled,
                interval: c.metrics_interval,
            },
        }
    }
}

/// Null sink configuration - discards all data
///
/// Useful for benchmarking pipeline throughput.
///
/// # Example
///
/// ```toml
/// [sinks.null_benchmark]
/// type = "null"
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct NullSinkConfig {
    /// Whether this sink is enabled
    /// Default: true
    pub enabled: bool,

    /// Enable per-sink metrics
    /// Default: true
    pub metrics_enabled: bool,

    /// Metrics reporting interval
    /// Default: 10s
    #[serde(with = "humantime_serde")]
    pub metrics_interval: Duration,
}

impl Default for NullSinkConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            metrics_enabled: true,
            metrics_interval: Duration::from_secs(10),
        }
    }
}

/// Stdout sink configuration - human-readable output
///
/// # Example
///
/// ```toml
/// [sinks.stdout]
/// # all defaults
///
/// [sinks.stdout_verbose]
/// type = "stdout"
/// metrics_enabled = true
/// metrics_interval = "5s"
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct StdoutSinkConfig {
    /// Whether this sink is enabled
    /// Default: true
    pub enabled: bool,

    /// Enable per-sink metrics
    /// Default: true
    pub metrics_enabled: bool,

    /// Metrics reporting interval
    /// Default: 10s
    #[serde(with = "humantime_serde")]
    pub metrics_interval: Duration,
}

impl Default for StdoutSinkConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            metrics_enabled: true,
            metrics_interval: Duration::from_secs(10),
        }
    }
}

/// Rotation interval for disk-based sinks
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum RotationInterval {
    /// Rotate every hour
    Hourly,
    /// Rotate every day (default)
    #[default]
    Daily,
}

/// Compression type for disk-based sinks
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Compression {
    /// No compression (default)
    #[default]
    None,
    /// LZ4 compression (fast, moderate ratio)
    Lz4,
}

/// Binary disk sink configuration
///
/// High-performance binary storage with atomic rotation.
///
/// # Example
///
/// ```toml
/// [sinks.disk_binary]
/// path = "archive/"
/// rotation = "hourly"
/// compression = "lz4"
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct DiskBinarySinkConfig {
    /// Whether this sink is enabled
    /// Default: true
    pub enabled: bool,

    /// Output directory path
    /// Required when enabled
    pub path: String,

    /// Buffer size in bytes
    /// Default: 32MB
    pub buffer_size: usize,

    /// File rotation interval
    /// Default: daily
    pub rotation: RotationInterval,

    /// Compression type
    /// Default: none
    pub compression: Compression,

    /// Enable per-sink metrics
    /// Default: true
    pub metrics_enabled: bool,

    /// Metrics reporting interval
    /// Default: 10s
    #[serde(with = "humantime_serde")]
    pub metrics_interval: Duration,
}

impl Default for DiskBinarySinkConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            path: String::new(),
            buffer_size: 32 * 1024 * 1024, // 32MB
            rotation: RotationInterval::Daily,
            compression: Compression::None,
            metrics_enabled: true,
            metrics_interval: Duration::from_secs(10),
        }
    }
}

/// Plaintext disk sink configuration
///
/// Human-readable log files with optional compression.
///
/// # Example
///
/// ```toml
/// [sinks.disk_plaintext]
/// path = "logs/"
/// rotation = "daily"
/// compression = "lz4"
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct DiskPlaintextSinkConfig {
    /// Whether this sink is enabled
    /// Default: true
    pub enabled: bool,

    /// Output directory path
    /// Required when enabled
    pub path: String,

    /// Buffer size in bytes
    /// Default: 8MB
    pub buffer_size: usize,

    /// Write queue size
    /// Default: 10000
    pub write_queue_size: usize,

    /// File rotation interval
    /// Default: daily
    pub rotation: RotationInterval,

    /// Compression type
    /// Default: none
    pub compression: Compression,

    /// Flush interval
    /// Default: 100ms
    #[serde(with = "humantime_serde")]
    pub flush_interval: Duration,

    /// Enable per-sink metrics
    /// Default: true
    pub metrics_enabled: bool,

    /// Metrics reporting interval
    /// Default: 10s
    #[serde(with = "humantime_serde")]
    pub metrics_interval: Duration,
}

impl Default for DiskPlaintextSinkConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            path: String::new(),
            buffer_size: 8 * 1024 * 1024, // 8MB
            write_queue_size: 10000,
            rotation: RotationInterval::Daily,
            compression: Compression::None,
            flush_interval: Duration::from_millis(100),
            metrics_enabled: true,
            metrics_interval: Duration::from_secs(10),
        }
    }
}

/// ClickHouse sink configuration
///
/// Real-time analytics database sink.
///
/// # Example
///
/// ```toml
/// [sinks.clickhouse]
/// host = "localhost:9000"
/// database = "cdp"
/// username = "default"
/// batch_size = 50000
/// flush_interval = "5s"
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ClickHouseSinkConfig {
    /// Whether this sink is enabled
    /// Default: true
    pub enabled: bool,

    /// ClickHouse host (host:port)
    /// Required when enabled
    pub host: String,

    /// Database name
    /// Default: "default"
    pub database: String,

    /// Username for authentication
    /// Default: "default"
    pub username: String,

    /// Password for authentication
    /// Default: ""
    pub password: String,

    /// Table prefix for workspace tables
    pub hostname_prefix: Option<String>,

    /// Batch size (rows per insert)
    /// Default: 50000
    pub batch_size: usize,

    /// Flush interval
    /// Default: 5s
    #[serde(with = "humantime_serde")]
    pub flush_interval: Duration,

    /// Enable per-sink metrics
    /// Default: true
    pub metrics_enabled: bool,

    /// Metrics reporting interval
    /// Default: 10s
    #[serde(with = "humantime_serde")]
    pub metrics_interval: Duration,
}

impl Default for ClickHouseSinkConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            host: String::new(),
            database: "default".into(),
            username: "default".into(),
            password: String::new(),
            hostname_prefix: None,
            batch_size: 50000,
            flush_interval: Duration::from_secs(5),
            metrics_enabled: true,
            metrics_interval: Duration::from_secs(10),
        }
    }
}

/// Parquet data format
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ParquetDataFormat {
    /// Binary FlatBuffer bytes
    Binary,
    /// JSON strings (easier for BI tools)
    #[default]
    Json,
}

/// Parquet compression type
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ParquetCompression {
    /// Snappy compression (default, good balance)
    #[default]
    Snappy,
    /// Gzip compression (better ratio, slower)
    Gzip,
    /// LZ4 compression (faster, lower ratio)
    Lz4,
    /// No compression
    Uncompressed,
}

/// Parquet sink configuration
///
/// Columnar storage for data warehousing.
///
/// # Example
///
/// ```toml
/// [sinks.parquet]
/// path = "parquet/"
/// rotation = "daily"
/// data_format = "json"
/// compression = "snappy"
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ParquetSinkConfig {
    /// Whether this sink is enabled
    /// Default: true
    pub enabled: bool,

    /// Output directory path
    /// Required when enabled
    pub path: String,

    /// File rotation interval
    /// Default: daily
    pub rotation: RotationInterval,

    /// Data format
    /// Default: json
    pub data_format: ParquetDataFormat,

    /// Compression type
    /// Default: snappy
    pub compression: ParquetCompression,

    /// Enable per-sink metrics
    /// Default: true
    pub metrics_enabled: bool,

    /// Metrics reporting interval
    /// Default: 10s
    #[serde(with = "humantime_serde")]
    pub metrics_interval: Duration,
}

impl Default for ParquetSinkConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            path: String::new(),
            rotation: RotationInterval::Daily,
            data_format: ParquetDataFormat::Json,
            compression: ParquetCompression::Snappy,
            metrics_enabled: true,
            metrics_interval: Duration::from_secs(10),
        }
    }
}

/// Forwarder sink configuration
///
/// Collector-to-collector forwarding for edge deployments.
///
/// # Example
///
/// ```toml
/// [sinks.forwarder]
/// target = "collector.example.com:8081"
/// api_key = "0123456789abcdef0123456789abcdef"
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ForwarderSinkConfig {
    /// Whether this sink is enabled
    /// Default: true
    pub enabled: bool,

    /// Target collector address (host:port)
    /// Required when enabled
    pub target: String,

    /// Hex-encoded API key (32 hex chars = 16 bytes)
    /// Required when enabled
    pub api_key: String,

    /// TCP buffer size
    /// Default: 256KB
    pub buffer_size: usize,

    /// Connection timeout
    /// Default: 10s
    #[serde(with = "humantime_serde")]
    pub connection_timeout: Duration,

    /// Write timeout per batch
    /// Default: 5s
    #[serde(with = "humantime_serde")]
    pub write_timeout: Duration,

    /// Number of retry attempts on failure
    /// Default: 3
    pub retry_attempts: usize,

    /// Wait time between retries
    /// Default: 1s
    #[serde(with = "humantime_serde")]
    pub retry_interval: Duration,

    /// Wait time before reconnecting after disconnect
    /// Default: 5s
    #[serde(with = "humantime_serde")]
    pub reconnect_interval: Duration,

    /// Enable per-sink metrics
    /// Default: true
    pub metrics_enabled: bool,

    /// Metrics reporting interval
    /// Default: 10s
    #[serde(with = "humantime_serde")]
    pub metrics_interval: Duration,
}

impl Default for ForwarderSinkConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            target: String::new(),
            api_key: String::new(),
            buffer_size: 256 * 1024,
            connection_timeout: Duration::from_secs(10),
            write_timeout: Duration::from_secs(5),
            retry_attempts: 3,
            retry_interval: Duration::from_secs(1),
            reconnect_interval: Duration::from_secs(5),
            metrics_enabled: true,
            metrics_interval: Duration::from_secs(10),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_null_sink_defaults() {
        let config = NullSinkConfig::default();
        assert!(config.enabled);
        assert!(config.metrics_enabled);
        assert_eq!(config.metrics_interval, Duration::from_secs(10));
    }

    #[test]
    fn test_stdout_sink_defaults() {
        let config = StdoutSinkConfig::default();
        assert!(config.enabled);
        assert!(config.metrics_enabled);
        assert_eq!(config.metrics_interval, Duration::from_secs(10));
    }

    #[test]
    fn test_disk_binary_sink_defaults() {
        let config = DiskBinarySinkConfig::default();
        assert!(config.enabled);
        assert_eq!(config.buffer_size, 32 * 1024 * 1024);
        assert_eq!(config.rotation, RotationInterval::Daily);
        assert_eq!(config.compression, Compression::None);
    }

    #[test]
    fn test_disk_plaintext_sink_defaults() {
        let config = DiskPlaintextSinkConfig::default();
        assert!(config.enabled);
        assert_eq!(config.buffer_size, 8 * 1024 * 1024);
        assert_eq!(config.write_queue_size, 10000);
        assert_eq!(config.flush_interval, Duration::from_millis(100));
    }

    #[test]
    fn test_clickhouse_sink_defaults() {
        let config = ClickHouseSinkConfig::default();
        assert!(config.enabled);
        assert_eq!(config.database, "default");
        assert_eq!(config.username, "default");
        assert_eq!(config.batch_size, 50000);
        assert_eq!(config.flush_interval, Duration::from_secs(5));
    }

    #[test]
    fn test_parquet_sink_defaults() {
        let config = ParquetSinkConfig::default();
        assert!(config.enabled);
        assert_eq!(config.rotation, RotationInterval::Daily);
        assert_eq!(config.data_format, ParquetDataFormat::Json);
        assert_eq!(config.compression, ParquetCompression::Snappy);
    }

    #[test]
    fn test_forwarder_sink_defaults() {
        let config = ForwarderSinkConfig::default();
        assert!(config.enabled);
        assert_eq!(config.buffer_size, 256 * 1024);
        assert_eq!(config.connection_timeout, Duration::from_secs(10));
        assert_eq!(config.retry_attempts, 3);
        assert!(config.metrics_enabled);
        assert_eq!(config.metrics_interval, Duration::from_secs(10));
    }

    #[test]
    fn test_deserialize_stdout() {
        let toml = r#"
[stdout]
type = "stdout"
"#;
        let config: SinksConfig = toml::from_str(toml).unwrap();
        assert!(config.contains("stdout"));
        let sink = config.get("stdout").unwrap();
        assert!(sink.is_enabled());
        assert_eq!(sink.type_name(), "stdout");
    }

    #[test]
    fn test_deserialize_multiple_sinks() {
        let toml = r#"
[stdout]
type = "stdout"

[disk_plaintext]
type = "disk_plaintext"
path = "logs/"
rotation = "daily"

[clickhouse_prod]
type = "clickhouse"
host = "prod:9000"
batch_size = 100000
"#;
        let config: SinksConfig = toml::from_str(toml).unwrap();

        assert_eq!(config.len(), 3);
        assert!(config.contains("stdout"));
        assert!(config.contains("disk_plaintext"));
        assert!(config.contains("clickhouse_prod"));

        if let Some(SinkConfig::DiskPlaintext(disk)) = config.get("disk_plaintext") {
            assert_eq!(disk.path, "logs/");
            assert_eq!(disk.rotation, RotationInterval::Daily);
        } else {
            panic!("Expected disk_plaintext config");
        }

        if let Some(SinkConfig::Clickhouse(ch)) = config.get("clickhouse_prod") {
            assert_eq!(ch.host, "prod:9000");
            assert_eq!(ch.batch_size, 100000);
        } else {
            panic!("Expected clickhouse config");
        }
    }

    #[test]
    fn test_deserialize_disk_binary_with_compression() {
        let toml = r#"
[archive]
type = "disk_binary"
path = "archive/"
compression = "lz4"
rotation = "hourly"
"#;
        let config: SinksConfig = toml::from_str(toml).unwrap();

        if let Some(SinkConfig::DiskBinary(disk)) = config.get("archive") {
            assert_eq!(disk.path, "archive/");
            assert_eq!(disk.compression, Compression::Lz4);
            assert_eq!(disk.rotation, RotationInterval::Hourly);
        } else {
            panic!("Expected disk_binary config");
        }
    }

    #[test]
    fn test_deserialize_parquet() {
        let toml = r#"
[warehouse]
type = "parquet"
path = "data/"
data_format = "binary"
compression = "gzip"
"#;
        let config: SinksConfig = toml::from_str(toml).unwrap();

        if let Some(SinkConfig::Parquet(pq)) = config.get("warehouse") {
            assert_eq!(pq.path, "data/");
            assert_eq!(pq.data_format, ParquetDataFormat::Binary);
            assert_eq!(pq.compression, ParquetCompression::Gzip);
        } else {
            panic!("Expected parquet config");
        }
    }

    #[test]
    fn test_deserialize_forwarder() {
        let toml = r#"
[forwarder]
type = "forwarder"
target = "collector.example.com:8081"
api_key = "0123456789abcdef0123456789abcdef"
connection_timeout = "30s"
retry_attempts = 5
"#;
        let config: SinksConfig = toml::from_str(toml).unwrap();

        if let Some(SinkConfig::Forwarder(fwd)) = config.get("forwarder") {
            assert_eq!(fwd.target, "collector.example.com:8081");
            assert_eq!(fwd.api_key, "0123456789abcdef0123456789abcdef");
            assert_eq!(fwd.connection_timeout, Duration::from_secs(30));
            assert_eq!(fwd.retry_attempts, 5);
        } else {
            panic!("Expected forwarder config");
        }
    }

    #[test]
    fn test_deserialize_disabled_sink() {
        let toml = r#"
[debug_sink]
type = "null"
enabled = false
"#;
        let config: SinksConfig = toml::from_str(toml).unwrap();

        let sink = config.get("debug_sink").unwrap();
        assert!(!sink.is_enabled());
    }

    #[test]
    fn test_empty_sinks() {
        let config: SinksConfig = toml::from_str("").unwrap();
        assert!(config.is_empty());
        assert_eq!(config.len(), 0);
    }

    #[test]
    fn test_sink_names() {
        let toml = r#"
[a]
type = "null"

[b]
type = "stdout"

[c]
type = "null"
"#;
        let config: SinksConfig = toml::from_str(toml).unwrap();

        let names: Vec<_> = config.names().collect();
        assert_eq!(names.len(), 3);
        assert!(names.contains(&&"a".to_string()));
        assert!(names.contains(&&"b".to_string()));
        assert!(names.contains(&&"c".to_string()));
    }

    #[test]
    fn test_iter_sinks() {
        let toml = r#"
[sink1]
type = "null"

[sink2]
type = "stdout"
enabled = false
"#;
        let config: SinksConfig = toml::from_str(toml).unwrap();

        let enabled_count = config.iter().filter(|(_, s)| s.is_enabled()).count();
        assert_eq!(enabled_count, 1);
    }
}
