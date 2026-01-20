//! Filter Transformer - Drop unwanted events
//!
//! Filters events based on conditions to reduce noise and cost.
//!
//! # Job To Be Done
//!
//! Drop unwanted events at the edge to reduce noise and cost.
//! Use when dropping >10% of events, otherwise skip for zero-copy.
//!
//! # Configuration
//!
//! | Option | Type | Default | Description |
//! |--------|------|---------|-------------|
//! | `action` | string | `"drop"` | Action when conditions match: `drop` or `keep` |
//! | `match_mode` | string | `"all"` | How to combine conditions: `all` (AND) or `any` (OR) |
//! | `conditions` | array | required | List of conditions to evaluate |
//!
//! ## Condition Options
//!
//! | Option | Type | Description |
//! |--------|------|-------------|
//! | `field` | string | JSON field path (dot notation: `user.email`) |
//! | `operator` | string | Comparison operator (see below) |
//! | `value` | string | Value to compare against (optional for `exists`) |
//!
//! ## Operators
//!
//! | Operator | Description |
//! |----------|-------------|
//! | `eq` | Equal to value |
//! | `ne` | Not equal to value |
//! | `contains` | String contains value |
//! | `starts_with` | String starts with value |
//! | `ends_with` | String ends with value |
//! | `regex` | Matches regex pattern |
//! | `exists` | Field exists (value ignored) |
//! | `gt` | Greater than (numeric) |
//! | `lt` | Less than (numeric) |
//! | `gte` | Greater than or equal (numeric) |
//! | `lte` | Less than or equal (numeric) |
//!
//! # TOML Examples
//!
//! ```toml
//! # Drop debug logs
//! [[routing.rules.transformers]]
//! type = "filter"
//! action = "drop"
//!
//! [[routing.rules.transformers.conditions]]
//! field = "level"
//! operator = "eq"
//! value = "debug"
//! ```
//!
//! ```toml
//! # Keep only errors from production
//! [[routing.rules.transformers]]
//! type = "filter"
//! action = "keep"
//! match_mode = "all"
//!
//! [[routing.rules.transformers.conditions]]
//! field = "level"
//! operator = "eq"
//! value = "error"
//!
//! [[routing.rules.transformers.conditions]]
//! field = "env"
//! operator = "eq"
//! value = "production"
//! ```
//!
//! ```toml
//! # Drop health checks and metrics
//! [[routing.rules.transformers]]
//! type = "filter"
//! action = "drop"
//! match_mode = "any"
//!
//! [[routing.rules.transformers.conditions]]
//! field = "path"
//! operator = "eq"
//! value = "/health"
//!
//! [[routing.rules.transformers.conditions]]
//! field = "path"
//! operator = "starts_with"
//! value = "/metrics"
//! ```
//!
//! # Rust Example
//!
//! ```ignore
//! let config = FilterConfig::new()
//!     .with_action(FilterAction::Drop)
//!     .with_condition(Condition::eq("level", "debug"));
//!
//! let transformer = FilterTransformer::new(config)?;
//! let batch = transformer.transform(batch).await?;
//! ```

mod config;

pub use config::{Condition, FilterAction, FilterConfig, MatchMode, Operator};

use crate::registry::{TransformerConfig, TransformerFactory};
use crate::{TransformError, TransformResult, Transformer};
use tell_protocol::{Batch, BatchBuilder};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(test)]
#[path = "mod_test.rs"]
mod tests;

/// Metrics for the filter transformer
#[derive(Debug, Default)]
pub struct FilterMetrics {
    /// Batches processed
    pub batches_processed: AtomicU64,
    /// Messages received
    pub messages_received: AtomicU64,
    /// Messages passed (not filtered)
    pub messages_passed: AtomicU64,
    /// Messages dropped
    pub messages_dropped: AtomicU64,
}

impl FilterMetrics {
    /// Get drop rate (0.0 - 1.0)
    pub fn drop_rate(&self) -> f64 {
        let received = self.messages_received.load(Ordering::Relaxed);
        let dropped = self.messages_dropped.load(Ordering::Relaxed);
        if received == 0 {
            0.0
        } else {
            dropped as f64 / received as f64
        }
    }
}

/// Filter transformer
///
/// Filters events based on conditions. Stateless.
pub struct FilterTransformer {
    config: FilterConfig,
    metrics: FilterMetrics,
}

impl FilterTransformer {
    /// Create a new filter transformer
    pub fn new(config: FilterConfig) -> TransformResult<Self> {
        config.validate().map_err(TransformError::config)?;

        Ok(Self {
            config,
            metrics: FilterMetrics::default(),
        })
    }

    /// Get transformer metrics
    pub fn metrics(&self) -> &FilterMetrics {
        &self.metrics
    }

    /// Check if a message matches the conditions
    fn matches(&self, message: &[u8]) -> bool {
        // Try to parse as JSON
        let json: serde_json::Value = match serde_json::from_slice(message) {
            Ok(v) => v,
            Err(_) => {
                // Non-JSON: can only match exists/not-exists on the whole message
                return false;
            }
        };

        match self.config.match_mode {
            MatchMode::All => self.config.conditions.iter().all(|c| self.eval_condition(c, &json)),
            MatchMode::Any => self.config.conditions.iter().any(|c| self.eval_condition(c, &json)),
        }
    }

    /// Evaluate a single condition against a JSON value
    fn eval_condition(&self, condition: &Condition, json: &serde_json::Value) -> bool {
        let field_value = get_json_field(json, &condition.field);

        match &condition.operator {
            Operator::Exists => field_value.is_some(),

            Operator::Eq => {
                if let (Some(field), Some(expected)) = (field_value, &condition.value) {
                    json_value_equals(field, expected)
                } else {
                    false
                }
            }

            Operator::Ne => {
                if let (Some(field), Some(expected)) = (field_value, &condition.value) {
                    !json_value_equals(field, expected)
                } else {
                    // Field doesn't exist - not equal to anything
                    true
                }
            }

            Operator::Contains => {
                if let (Some(field), Some(substr)) = (field_value, &condition.value) {
                    json_value_as_string(field)
                        .map(|s| s.contains(substr.as_str()))
                        .unwrap_or(false)
                } else {
                    false
                }
            }

            Operator::StartsWith => {
                if let (Some(field), Some(prefix)) = (field_value, &condition.value) {
                    json_value_as_string(field)
                        .map(|s| s.starts_with(prefix.as_str()))
                        .unwrap_or(false)
                } else {
                    false
                }
            }

            Operator::EndsWith => {
                if let (Some(field), Some(suffix)) = (field_value, &condition.value) {
                    json_value_as_string(field)
                        .map(|s| s.ends_with(suffix.as_str()))
                        .unwrap_or(false)
                } else {
                    false
                }
            }

            Operator::Regex(re) => {
                if let Some(field) = field_value {
                    json_value_as_string(field)
                        .map(|s| re.is_match(&s))
                        .unwrap_or(false)
                } else {
                    false
                }
            }

            Operator::Gt => numeric_compare(field_value, &condition.value, |a, b| a > b),
            Operator::Lt => numeric_compare(field_value, &condition.value, |a, b| a < b),
            Operator::Gte => numeric_compare(field_value, &condition.value, |a, b| a >= b),
            Operator::Lte => numeric_compare(field_value, &condition.value, |a, b| a <= b),
        }
    }

    /// Process a batch, filtering events
    fn process_batch(&self, batch: Batch) -> Batch {
        self.metrics.batches_processed.fetch_add(1, Ordering::Relaxed);

        let msg_count = batch.message_count();
        self.metrics.messages_received.fetch_add(msg_count as u64, Ordering::Relaxed);

        let mut builder = BatchBuilder::new(batch.batch_type(), batch.source_id().clone());
        let mut passed = 0u64;
        let mut dropped = 0u64;

        for i in 0..msg_count {
            let message = match batch.get_message(i) {
                Some(m) => m,
                None => continue,
            };

            let matches = self.matches(message);

            // Determine if we should keep or drop
            let keep = match self.config.action {
                FilterAction::Drop => !matches,  // Drop matches, keep non-matches
                FilterAction::Keep => matches,   // Keep matches, drop non-matches
            };

            if keep {
                builder.add_raw(message);
                passed += 1;
            } else {
                dropped += 1;
            }
        }

        self.metrics.messages_passed.fetch_add(passed, Ordering::Relaxed);
        self.metrics.messages_dropped.fetch_add(dropped, Ordering::Relaxed);

        builder.finish()
    }
}

impl Transformer for FilterTransformer {
    fn transform<'a>(
        &'a self,
        batch: Batch,
    ) -> Pin<Box<dyn Future<Output = TransformResult<Batch>> + Send + 'a>> {
        Box::pin(async move { Ok(self.process_batch(batch)) })
    }

    fn name(&self) -> &'static str {
        "filter"
    }

    fn enabled(&self) -> bool {
        self.config.enabled
    }
}

impl std::fmt::Debug for FilterTransformer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilterTransformer")
            .field("config", &self.config)
            .field("metrics", &self.metrics)
            .finish()
    }
}

/// Factory for creating filter transformers
#[derive(Debug, Clone, Copy)]
pub struct FilterFactory;

impl TransformerFactory for FilterFactory {
    fn create(&self, config: &TransformerConfig) -> TransformResult<Box<dyn Transformer>> {
        let filter_config = parse_filter_config(config)?;
        Ok(Box::new(FilterTransformer::new(filter_config)?))
    }

    fn name(&self) -> &'static str {
        "filter"
    }

    fn default_config(&self) -> Option<TransformerConfig> {
        let mut config = HashMap::new();
        config.insert("action".to_string(), toml::Value::String("drop".to_string()));
        config.insert("match".to_string(), toml::Value::String("all".to_string()));
        Some(config)
    }
}

/// Parse FilterConfig from TransformerConfig map
fn parse_filter_config(config: &TransformerConfig) -> TransformResult<FilterConfig> {
    // Create a minimal TransformerInstanceConfig to reuse TryFrom
    let mut instance = tell_config::TransformerInstanceConfig::noop();
    instance.transformer_type = "filter".to_string();

    // Copy over options
    for (k, v) in config {
        instance.options.insert(k.clone(), v.clone());
    }

    // Check enabled
    if let Some(toml::Value::Boolean(false)) = config.get("enabled") {
        instance.enabled = false;
    }

    FilterConfig::try_from(&instance).map_err(TransformError::config)
}

/// Get a field from JSON using dot notation
fn get_json_field<'a>(json: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
    let mut current = json;

    for part in path.split('.') {
        match current {
            serde_json::Value::Object(map) => {
                current = map.get(part)?;
            }
            _ => return None,
        }
    }

    Some(current)
}

/// Compare JSON value to string for equality
fn json_value_equals(value: &serde_json::Value, expected: &str) -> bool {
    match value {
        serde_json::Value::String(s) => s == expected,
        serde_json::Value::Number(n) => n.to_string() == expected,
        serde_json::Value::Bool(b) => {
            (expected == "true" && *b) || (expected == "false" && !*b)
        }
        serde_json::Value::Null => expected == "null",
        _ => false,
    }
}

/// Get JSON value as string for string operations
fn json_value_as_string(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        serde_json::Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// Compare numeric values
fn numeric_compare<F>(field: Option<&serde_json::Value>, expected: &Option<String>, cmp: F) -> bool
where
    F: Fn(f64, f64) -> bool,
{
    let field_num = field.and_then(|v| {
        match v {
            serde_json::Value::Number(n) => n.as_f64(),
            serde_json::Value::String(s) => s.parse::<f64>().ok(),
            _ => None,
        }
    });

    let expected_num = expected.as_ref().and_then(|s| s.parse::<f64>().ok());

    match (field_num, expected_num) {
        (Some(a), Some(b)) => cmp(a, b),
        _ => false,
    }
}
