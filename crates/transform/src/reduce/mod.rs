//! Reduce Transformer - Consolidate redundant events
//!
//! Groups similar events and flushes them as single events with count metadata.
//! Use for cost optimization when handling high-volume redundant data.
//!
//! # Job To Be Done
//!
//! Consolidate redundant events at the edge to reduce storage costs.
//! Only use when expecting >90% reduction, otherwise keep zero-copy.
//!
//! # Example
//!
//! ```ignore
//! let config = ReduceConfig::new()
//!     .with_group_by(vec!["error_code".to_string()])
//!     .with_window_ms(5000)
//!     .with_max_events(1000);
//!
//! let transformer = ReduceTransformer::new(config)?;
//! let batch = transformer.transform(batch).await?;
//! ```

mod config;
mod state;

pub use config::ReduceConfig;
pub use state::{ReduceGroup, ReduceState, compute_group_key};

use crate::registry::{TransformerConfig, TransformerFactory};
use crate::{TransformError, TransformResult, Transformer};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use tell_protocol::{Batch, BatchBuilder};
use tokio::sync::Mutex;

#[cfg(test)]
#[path = "mod_test.rs"]
mod tests;

/// Metrics for the reduce transformer
#[derive(Debug, Default)]
pub struct ReduceMetrics {
    /// Batches processed
    pub batches_processed: AtomicU64,
    /// Messages received
    pub messages_received: AtomicU64,
    /// Messages output (after reduction)
    pub messages_output: AtomicU64,
    /// Groups created
    pub groups_created: AtomicU64,
    /// Groups flushed
    pub groups_flushed: AtomicU64,
    /// Events reduced (input - output)
    pub events_reduced: AtomicU64,
}

impl ReduceMetrics {
    /// Get reduction ratio (0.0 - 1.0)
    pub fn reduction_ratio(&self) -> f64 {
        let received = self.messages_received.load(Ordering::Relaxed);
        let output = self.messages_output.load(Ordering::Relaxed);
        if received == 0 {
            0.0
        } else {
            1.0 - (output as f64 / received as f64)
        }
    }
}

/// Reduce transformer
///
/// Consolidates similar events into single events with count metadata.
/// Stateful: maintains groups across batch boundaries.
pub struct ReduceTransformer {
    config: ReduceConfig,
    state: Mutex<ReduceState>,
    metrics: ReduceMetrics,
}

impl ReduceTransformer {
    /// Create a new reduce transformer
    pub fn new(config: ReduceConfig) -> TransformResult<Self> {
        config.validate().map_err(TransformError::config)?;

        Ok(Self {
            config,
            state: Mutex::new(ReduceState::new()),
            metrics: ReduceMetrics::default(),
        })
    }

    /// Get transformer metrics
    pub fn metrics(&self) -> &ReduceMetrics {
        &self.metrics
    }

    /// Get current group count
    pub async fn group_count(&self) -> usize {
        self.state.lock().await.group_count()
    }

    /// Force flush all groups
    pub async fn flush_all(&self) -> Vec<ReduceGroup> {
        let mut state = self.state.lock().await;
        let groups = state.flush_all();
        self.metrics
            .groups_flushed
            .fetch_add(groups.len() as u64, Ordering::Relaxed);
        groups
    }

    /// Process a batch, grouping events and flushing as needed
    async fn process_batch(&self, batch: Batch) -> TransformResult<Batch> {
        let mut state = self.state.lock().await;

        self.metrics
            .batches_processed
            .fetch_add(1, Ordering::Relaxed);
        self.metrics
            .messages_received
            .fetch_add(batch.message_count() as u64, Ordering::Relaxed);

        // Track messages to output
        let mut output_messages: Vec<OutputMessage> = Vec::new();

        // First, check for groups that need flushing due to time window
        let window = self.config.window_duration();
        let to_flush = state.groups_to_flush(window, self.config.max_events);

        for key_hash in to_flush {
            if let Some(group) = state.remove(key_hash) {
                output_messages.push(self.create_output_message(&group));
                self.metrics.groups_flushed.fetch_add(1, Ordering::Relaxed);
            }
        }

        // Process incoming messages
        for i in 0..batch.message_count() {
            let message = match batch.get_message(i) {
                Some(m) => m,
                None => continue,
            };

            let key_hash = compute_group_key(message, &self.config.group_by);
            let (group, is_new) = state.get_or_create(key_hash, message);

            if is_new {
                self.metrics.groups_created.fetch_add(1, Ordering::Relaxed);
            }

            // Check if this addition triggers a count-based flush
            if group.should_flush_count(self.config.max_events) {
                // Remove and output
                if let Some(group) = state.remove(key_hash) {
                    output_messages.push(self.create_output_message(&group));
                    self.metrics.groups_flushed.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        // Build output batch
        let output_batch = self.build_output_batch(&batch, output_messages);

        self.metrics
            .messages_output
            .fetch_add(output_batch.message_count() as u64, Ordering::Relaxed);

        let reduced = batch
            .message_count()
            .saturating_sub(output_batch.message_count());
        self.metrics
            .events_reduced
            .fetch_add(reduced as u64, Ordering::Relaxed);

        Ok(output_batch)
    }

    /// Create output message from a group
    fn create_output_message(&self, group: &ReduceGroup) -> OutputMessage {
        if group.count < self.config.min_events as u64 {
            // Below threshold - pass through unchanged
            OutputMessage::Passthrough(group.first_message.clone())
        } else {
            // Add reduction metadata
            OutputMessage::Reduced {
                original: group.first_message.clone(),
                count: group.count,
                first_ts: group.created_at,
                last_ts: group.last_event_at,
            }
        }
    }

    /// Build the output batch from output messages
    fn build_output_batch(&self, original: &Batch, messages: Vec<OutputMessage>) -> Batch {
        if messages.is_empty() {
            // Return empty batch with same metadata
            let builder = BatchBuilder::new(original.batch_type(), original.source_id().clone());
            return builder.finish();
        }

        let mut builder = BatchBuilder::new(original.batch_type(), original.source_id().clone());

        for msg in messages {
            match msg {
                OutputMessage::Passthrough(data) => {
                    builder.add_raw(&data);
                }
                OutputMessage::Reduced {
                    original,
                    count,
                    first_ts,
                    last_ts,
                } => {
                    // Inject metadata into JSON message
                    let enriched = self.enrich_message(&original, count, first_ts, last_ts);
                    builder.add_raw(&enriched);
                }
            }
        }

        builder.finish()
    }

    /// Enrich message with reduction metadata
    fn enrich_message(
        &self,
        original: &[u8],
        count: u64,
        first_ts: std::time::Instant,
        last_ts: std::time::Instant,
    ) -> Vec<u8> {
        // Try to parse as JSON and add metadata fields
        if let Ok(mut json) = serde_json::from_slice::<serde_json::Value>(original)
            && let Some(obj) = json.as_object_mut()
        {
            obj.insert("_reduced_count".to_string(), serde_json::json!(count));
            // Store duration in milliseconds (Instant doesn't give wall time)
            let duration_ms = last_ts.duration_since(first_ts).as_millis() as u64;
            obj.insert(
                "_reduced_span_ms".to_string(),
                serde_json::json!(duration_ms),
            );

            if let Ok(serialized) = serde_json::to_vec(&json) {
                return serialized;
            }
        }

        // Fallback: return original with metadata as suffix
        let suffix = format!(" [_reduced_count={}]", count);
        let mut result = original.to_vec();
        result.extend_from_slice(suffix.as_bytes());
        result
    }
}

/// Internal enum for output message types
enum OutputMessage {
    /// Pass through unchanged (below min_events)
    Passthrough(Vec<u8>),
    /// Reduced message with metadata
    Reduced {
        original: Vec<u8>,
        count: u64,
        first_ts: std::time::Instant,
        last_ts: std::time::Instant,
    },
}

impl Transformer for ReduceTransformer {
    fn transform<'a>(
        &'a self,
        batch: Batch,
    ) -> Pin<Box<dyn Future<Output = TransformResult<Batch>> + Send + 'a>> {
        Box::pin(async move { self.process_batch(batch).await })
    }

    fn name(&self) -> &'static str {
        "reduce"
    }

    fn enabled(&self) -> bool {
        self.config.enabled
    }
}

impl std::fmt::Debug for ReduceTransformer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReduceTransformer")
            .field("config", &self.config)
            .field("metrics", &self.metrics)
            .finish()
    }
}

/// Factory for creating reduce transformers
#[derive(Debug, Clone, Copy)]
pub struct ReduceFactory;

impl TransformerFactory for ReduceFactory {
    fn create(&self, config: &TransformerConfig) -> TransformResult<Box<dyn Transformer>> {
        let reduce_config = parse_reduce_config(config)?;
        Ok(Box::new(ReduceTransformer::new(reduce_config)?))
    }

    fn name(&self) -> &'static str {
        "reduce"
    }

    fn default_config(&self) -> Option<TransformerConfig> {
        let mut config = HashMap::new();
        config.insert("window_ms".to_string(), toml::Value::Integer(5000));
        config.insert("max_events".to_string(), toml::Value::Integer(1000));
        config.insert("min_events".to_string(), toml::Value::Integer(2));
        Some(config)
    }
}

/// Parse ReduceConfig from TransformerConfig map
fn parse_reduce_config(config: &TransformerConfig) -> TransformResult<ReduceConfig> {
    let mut reduce_config = ReduceConfig::default();

    // Parse enabled
    if let Some(toml::Value::Boolean(enabled)) = config.get("enabled") {
        reduce_config.enabled = *enabled;
    }

    // Parse group_by
    if let Some(toml::Value::Array(arr)) = config.get("group_by") {
        reduce_config.group_by = arr
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect();
    }

    // Parse window_ms
    if let Some(toml::Value::Integer(ms)) = config.get("window_ms") {
        reduce_config.window_ms = *ms as u64;
    }

    // Parse max_events
    if let Some(toml::Value::Integer(max)) = config.get("max_events") {
        reduce_config.max_events = (*max).max(1) as usize;
    }

    // Parse min_events
    if let Some(toml::Value::Integer(min)) = config.get("min_events") {
        reduce_config.min_events = (*min).max(1) as usize;
    }

    Ok(reduce_config)
}
