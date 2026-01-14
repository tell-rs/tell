//! Global configuration settings
//!
//! These settings apply across all components and provide sensible defaults.

use serde::Deserialize;

/// Global configuration that applies to all components
///
/// All fields have sensible defaults - you only need to specify what you want to change.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct GlobalConfig {
    /// Number of worker threads for async runtime
    /// Default: number of CPU cores
    pub num_processors: usize,

    /// Default buffer size for network I/O (bytes)
    /// Default: 262144 (256KB)
    pub buffer_size: usize,

    /// Default batch size (messages per batch)
    /// Default: 500
    pub batch_size: usize,

    /// Default channel queue size
    /// Default: 1000
    pub queue_size: usize,

    /// Path to API keys configuration file
    /// Default: "configs/apikeys.conf"
    pub api_keys_file: String,

    /// Number of router worker threads (shards)
    /// Default: None (auto = number of CPU cores)
    /// Set to 1 for single-threaded router (simpler debugging)
    pub router_workers: Option<usize>,
}

impl Default for GlobalConfig {
    fn default() -> Self {
        Self {
            num_processors: num_cpus(),
            buffer_size: 256 * 1024, // 256KB
            batch_size: 500,
            queue_size: 1000,
            api_keys_file: "configs/apikeys.conf".into(),
            router_workers: None, // Auto = num_cpus
        }
    }
}

impl GlobalConfig {
    /// Get the effective number of router workers
    ///
    /// Returns the configured value, or num_cpus if not set (auto mode).
    pub fn effective_router_workers(&self) -> usize {
        self.router_workers.unwrap_or_else(num_cpus).max(1)
    }
}

/// Get the number of available CPUs, defaulting to 4 if detection fails
fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = GlobalConfig::default();
        assert!(config.num_processors > 0);
        assert_eq!(config.buffer_size, 256 * 1024);
        assert_eq!(config.batch_size, 500);
        assert_eq!(config.queue_size, 1000);
        assert_eq!(config.api_keys_file, "configs/apikeys.conf");
    }

    #[test]
    fn test_deserialize_empty() {
        let config: GlobalConfig = toml::from_str("").unwrap();
        assert_eq!(config.buffer_size, 256 * 1024);
    }

    #[test]
    fn test_deserialize_partial() {
        let toml = r#"
buffer_size = 524288
batch_size = 1000
"#;
        let config: GlobalConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.buffer_size, 524288);
        assert_eq!(config.batch_size, 1000);
        // Defaults still apply
        assert_eq!(config.queue_size, 1000);
        assert_eq!(config.api_keys_file, "configs/apikeys.conf");
    }

    #[test]
    fn test_deserialize_full() {
        let toml = r#"
num_processors = 16
buffer_size = 1048576
batch_size = 2000
queue_size = 5000
api_keys_file = "/etc/cdp/keys.conf"
"#;
        let config: GlobalConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.num_processors, 16);
        assert_eq!(config.buffer_size, 1048576);
        assert_eq!(config.batch_size, 2000);
        assert_eq!(config.queue_size, 5000);
        assert_eq!(config.api_keys_file, "/etc/cdp/keys.conf");
    }
}
