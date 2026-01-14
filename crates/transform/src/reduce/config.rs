//! Reduce transformer configuration

use cdp_config::TransformerInstanceConfig;
use std::time::Duration;

#[cfg(test)]
#[path = "config_test.rs"]
mod tests;

/// Configuration for the reduce transformer
#[derive(Debug, Clone)]
pub struct ReduceConfig {
    /// Whether the transformer is enabled
    pub enabled: bool,

    /// Fields to group by (empty = hash entire message)
    ///
    /// Messages with the same values for these fields are grouped together.
    /// For JSON messages, these are dot-notation paths like "error_code" or "user.id".
    pub group_by: Vec<String>,

    /// Time window before flushing a group (milliseconds)
    ///
    /// A group is flushed when `window_ms` has elapsed since the first event.
    /// Default: 5000ms (5 seconds)
    pub window_ms: u64,

    /// Maximum events per group before forced flush
    ///
    /// A group is flushed immediately when it reaches this count.
    /// Default: 1000
    pub max_events: usize,

    /// Minimum events required to apply reduction
    ///
    /// Groups with fewer events at flush time pass through unchanged,
    /// preserving zero-copy for unique events.
    /// Default: 2
    pub min_events: usize,
}

impl Default for ReduceConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            group_by: Vec::new(),
            window_ms: 5000,
            max_events: 1000,
            min_events: 2,
        }
    }
}

impl ReduceConfig {
    /// Create a new config with defaults
    pub fn new() -> Self {
        Self::default()
    }

    /// Set fields to group by
    pub fn with_group_by(mut self, fields: Vec<String>) -> Self {
        self.group_by = fields;
        self
    }

    /// Set time window in milliseconds
    pub fn with_window_ms(mut self, ms: u64) -> Self {
        self.window_ms = ms;
        self
    }

    /// Set maximum events per group
    pub fn with_max_events(mut self, max: usize) -> Self {
        self.max_events = max.max(1);
        self
    }

    /// Set minimum events for reduction
    pub fn with_min_events(mut self, min: usize) -> Self {
        self.min_events = min.max(1);
        self
    }

    /// Disable the transformer
    pub fn disabled(mut self) -> Self {
        self.enabled = false;
        self
    }

    /// Get window duration
    #[inline]
    pub fn window_duration(&self) -> Duration {
        Duration::from_millis(self.window_ms)
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.window_ms == 0 {
            return Err("window_ms must be greater than 0".to_string());
        }

        if self.max_events == 0 {
            return Err("max_events must be at least 1".to_string());
        }

        if self.min_events == 0 {
            return Err("min_events must be at least 1".to_string());
        }

        if self.min_events > self.max_events {
            return Err(format!(
                "min_events ({}) cannot exceed max_events ({})",
                self.min_events, self.max_events
            ));
        }

        Ok(())
    }
}

impl TryFrom<&TransformerInstanceConfig> for ReduceConfig {
    type Error = String;

    fn try_from(config: &TransformerInstanceConfig) -> Result<Self, Self::Error> {
        let mut reduce_config = ReduceConfig::default();

        if !config.enabled {
            reduce_config.enabled = false;
        }

        if let Some(group_by) = config.get_string_array("group_by") {
            reduce_config.group_by = group_by;
        }

        if let Some(window_ms) = config.get_int("window_ms") {
            reduce_config.window_ms = window_ms as u64;
        }

        if let Some(max_events) = config.get_int("max_events") {
            reduce_config.max_events = (max_events).max(1) as usize;
        }

        if let Some(min_events) = config.get_int("min_events") {
            reduce_config.min_events = (min_events).max(1) as usize;
        }

        reduce_config.validate()?;
        Ok(reduce_config)
    }
}
