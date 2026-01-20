//! Pattern matcher configuration
//!
//! Configuration types for the pattern transformer.

use std::path::PathBuf;
use std::time::Duration;
use tell_config::TransformerInstanceConfig;

#[cfg(test)]
#[path = "config_test.rs"]
mod tests;

/// Configuration for the pattern matcher transformer
#[derive(Debug, Clone)]
pub struct PatternConfig {
    /// Whether the transformer is enabled
    pub enabled: bool,

    /// Similarity threshold for Drain algorithm (0.0 - 1.0)
    ///
    /// Higher values = more strict matching, fewer patterns
    /// Lower values = more lenient matching, more patterns
    /// Default: 0.5
    pub similarity_threshold: f64,

    /// Maximum child nodes per Drain tree node
    ///
    /// Limits memory usage of the pattern tree.
    /// Default: 100
    pub max_child_nodes: usize,

    /// Maximum cached message hashes
    ///
    /// Level 1 cache: exact message hash → pattern ID
    /// Default: 100_000
    pub cache_size: usize,

    /// Pattern persistence configuration
    pub persistence: PersistenceConfig,

    /// Hot reload configuration
    pub reload: ReloadConfig,
}

impl Default for PatternConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            similarity_threshold: 0.5,
            max_child_nodes: 100,
            cache_size: 100_000,
            persistence: PersistenceConfig::default(),
            reload: ReloadConfig::default(),
        }
    }
}

impl PatternConfig {
    /// Create a new config with defaults
    pub fn new() -> Self {
        Self::default()
    }

    /// Set similarity threshold (clamped to 0.0-1.0)
    pub fn with_similarity_threshold(mut self, threshold: f64) -> Self {
        self.similarity_threshold = threshold.clamp(0.0, 1.0);
        self
    }

    /// Set max child nodes
    pub fn with_max_child_nodes(mut self, max: usize) -> Self {
        self.max_child_nodes = max.max(1);
        self
    }

    /// Set cache size
    pub fn with_cache_size(mut self, size: usize) -> Self {
        self.cache_size = size;
        self
    }

    /// Disable the transformer
    pub fn disabled(mut self) -> Self {
        self.enabled = false;
        self
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.similarity_threshold < 0.0 || self.similarity_threshold > 1.0 {
            return Err(format!(
                "similarity_threshold must be between 0.0 and 1.0, got {}",
                self.similarity_threshold
            ));
        }

        if self.max_child_nodes == 0 {
            return Err("max_child_nodes must be at least 1".to_string());
        }

        self.persistence.validate()?;
        self.reload.validate()?;

        Ok(())
    }
}

/// Pattern persistence configuration
#[derive(Debug, Clone)]
pub struct PersistenceConfig {
    /// Enable persistence to disk
    pub enabled: bool,

    /// Path to pattern storage file
    ///
    /// Patterns are stored as JSON for human readability.
    pub file_path: Option<PathBuf>,

    /// Flush interval for writing new patterns
    pub flush_interval: Duration,

    /// Maximum patterns to batch before flush
    pub batch_size: usize,

    /// Use background worker for non-blocking persistence
    ///
    /// When enabled, new patterns are sent to a background task
    /// via a channel, avoiding blocking the hot path.
    pub use_background_worker: bool,

    /// Channel capacity for background worker
    ///
    /// Only used when `use_background_worker` is true.
    pub channel_capacity: usize,
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            file_path: None,
            flush_interval: Duration::from_secs(5),
            batch_size: 100,
            use_background_worker: true,
            channel_capacity: 10_000,
        }
    }
}

impl PersistenceConfig {
    /// Enable file-based persistence
    pub fn with_file(mut self, path: PathBuf) -> Self {
        self.enabled = true;
        self.file_path = Some(path);
        self
    }

    /// Set flush interval
    pub fn with_flush_interval(mut self, interval: Duration) -> Self {
        self.flush_interval = interval;
        self
    }

    /// Set batch size
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size.max(1);
        self
    }

    /// Enable/disable background worker
    pub fn with_background_worker(mut self, enabled: bool) -> Self {
        self.use_background_worker = enabled;
        self
    }

    /// Set channel capacity for background worker
    pub fn with_channel_capacity(mut self, capacity: usize) -> Self {
        self.channel_capacity = capacity.max(1);
        self
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.enabled && self.file_path.is_none() {
            return Err("persistence enabled but no file_path specified".to_string());
        }

        if self.flush_interval.is_zero() {
            return Err("flush_interval must be greater than zero".to_string());
        }

        if self.batch_size == 0 {
            return Err("batch_size must be at least 1".to_string());
        }

        Ok(())
    }
}

/// Hot reload configuration
#[derive(Debug, Clone)]
pub struct ReloadConfig {
    /// Enable hot reload of patterns
    pub enabled: bool,

    /// Interval between reload checks
    pub interval: Duration,
}

impl Default for ReloadConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            interval: Duration::from_secs(60),
        }
    }
}

impl ReloadConfig {
    /// Enable hot reload with specified interval
    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.enabled = true;
        self.interval = interval;
        self
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.enabled && self.interval.is_zero() {
            return Err("reload interval must be greater than zero".to_string());
        }
        Ok(())
    }
}

/// ClickHouse configuration for pattern storage
///
/// Future: Used for distributed pattern storage and hot reload.
#[derive(Debug, Clone)]
pub struct ClickHouseConfig {
    /// ClickHouse connection URL
    pub url: String,

    /// Database name
    pub database: String,

    /// Table name for patterns
    pub table: String,

    /// Connection pool size
    pub pool_size: usize,
}

impl Default for ClickHouseConfig {
    fn default() -> Self {
        Self {
            url: "http://localhost:8123".to_string(),
            database: "tell".to_string(),
            table: "log_patterns".to_string(),
            pool_size: 5,
        }
    }
}

impl TryFrom<&TransformerInstanceConfig> for PatternConfig {
    type Error = String;

    fn try_from(config: &TransformerInstanceConfig) -> Result<Self, Self::Error> {
        let mut pattern_config = PatternConfig::default();

        if !config.enabled {
            pattern_config.enabled = false;
        }

        if let Some(threshold) = config.get_float("similarity_threshold") {
            pattern_config.similarity_threshold = threshold.clamp(0.0, 1.0);
        }

        if let Some(cache_size) = config.get_int("cache_size") {
            pattern_config.cache_size = cache_size as usize;
        }

        if let Some(max_nodes) = config.get_int("max_child_nodes") {
            pattern_config.max_child_nodes = (max_nodes).max(1) as usize;
        }

        // Persistence config
        if config.get_bool("persistence_enabled").unwrap_or(false)
            && let Some(path) = config.get_path("persistence_file")
        {
            pattern_config.persistence = pattern_config.persistence.with_file(path);
        }

        pattern_config.validate()?;
        Ok(pattern_config)
    }
}
