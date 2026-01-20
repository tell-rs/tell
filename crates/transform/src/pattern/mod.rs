//! Pattern Transformer - Log pattern extraction
//!
//! Extracts patterns from log messages using the Drain algorithm.
//! Patterns are assigned unique IDs and cached for efficient lookup.
//!
//! # Features
//!
//! - **Drain algorithm**: Industry-standard log pattern extraction
//! - **3-level caching**: Minimizes pattern matching overhead
//! - **Persistence**: Optional file-based pattern storage
//! - **Thread-safe**: Lock-free reads, minimal contention
//!
//! # Usage
//!
//! ```ignore
//! let config = PatternConfig::default();
//! let transformer = PatternTransformer::new(config)?;
//!
//! let batch = transformer.transform(batch).await?;
//! // batch.pattern_ids() now contains pattern IDs for each message
//! ```

mod cache;
mod config;
mod drain;
mod persistence;
mod reload;
mod storage;
mod worker;

pub use cache::{CacheStats, PatternCache};
pub use config::{ClickHouseConfig, PatternConfig, PersistenceConfig, ReloadConfig};
pub use drain::{DrainTree, Pattern, PatternId, generate_canonical_name};
pub use persistence::{PatternPersistence, StoredPattern};
pub use reload::{ReloadWorker, ReloadWorkerConfig, ReloadWorkerHandle, spawn_reload_worker};
pub use storage::{
    BoxedPatternStorage, FilePatternStorage, NullPatternStorage, PatternStorage, StorageError,
    StorageResult,
};
pub use worker::{PatternWorker, PatternWorkerHandle, WorkerConfig, spawn_pattern_worker};

use crate::{
    TransformError, TransformResult, Transformer,
    registry::{TransformerConfig, TransformerFactory},
};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tell_protocol::{Batch, BatchType, FlatBatch, SchemaType, decode_log_data};
use tokio_util::sync::CancellationToken;

#[cfg(test)]
#[path = "mod_test.rs"]
mod tests;

/// Pattern transformer metrics
#[derive(Debug, Default)]
pub struct PatternMetrics {
    /// Batches processed
    pub batches_processed: AtomicU64,

    /// Messages processed
    pub messages_processed: AtomicU64,

    /// New patterns created
    pub patterns_created: AtomicU64,

    /// Cache hits (L1 + L2)
    pub cache_hits: AtomicU64,

    /// Cache misses (required Drain lookup)
    pub cache_misses: AtomicU64,
}

impl PatternMetrics {
    /// Get cache hit rate
    pub fn cache_hit_rate(&self) -> f64 {
        let hits = self.cache_hits.load(Ordering::Relaxed);
        let misses = self.cache_misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }
}

/// Pattern transformer for log pattern extraction
///
/// Implements the `Transformer` trait to extract patterns from log messages.
/// Only processes `BatchType::Log` batches; other types pass through unchanged.
pub struct PatternTransformer {
    /// Configuration
    config: PatternConfig,

    /// Drain tree for pattern extraction
    drain: DrainTree,

    /// Multi-level pattern cache
    cache: PatternCache,

    /// Pattern persistence (sync fallback)
    persistence: Arc<PatternPersistence>,

    /// Background worker handle (optional, for async persistence)
    worker_handle: Option<PatternWorkerHandle>,

    /// Metrics
    metrics: PatternMetrics,
}

impl PatternTransformer {
    /// Create a new pattern transformer (sync persistence only)
    ///
    /// For async persistence with background worker, use `with_cancellation`.
    pub fn new(config: PatternConfig) -> TransformResult<Self> {
        Self::with_cancellation(config, None)
    }

    /// Create a new pattern transformer with optional background worker
    ///
    /// If `cancel` is provided and `config.persistence.use_background_worker` is true,
    /// patterns will be persisted asynchronously via a background task.
    pub fn with_cancellation(
        config: PatternConfig,
        cancel: Option<CancellationToken>,
    ) -> TransformResult<Self> {
        config.validate().map_err(TransformError::config)?;

        let drain = DrainTree::new(config.similarity_threshold, config.max_child_nodes);

        let cache = PatternCache::new(config.cache_size, config.cache_size / 10);

        let persistence = Arc::new(if config.persistence.enabled {
            PatternPersistence::new(
                config.persistence.file_path.clone(),
                config.persistence.batch_size,
            )
        } else {
            PatternPersistence::disabled()
        });

        // Spawn background worker if configured
        let worker_handle = if config.persistence.enabled
            && config.persistence.use_background_worker
            && cancel.is_some()
        {
            let worker_config = WorkerConfig::default()
                .with_channel_capacity(config.persistence.channel_capacity)
                .with_flush_interval(config.persistence.flush_interval)
                .with_batch_size(config.persistence.batch_size);

            Some(spawn_pattern_worker(
                Arc::clone(&persistence),
                worker_config,
                cancel.unwrap(),
            ))
        } else {
            None
        };

        Ok(Self {
            config,
            drain,
            cache,
            persistence,
            worker_handle,
            metrics: PatternMetrics::default(),
        })
    }

    /// Load existing patterns from persistence
    pub fn load_patterns(&self) -> TransformResult<usize> {
        let stored = self.persistence.load()?;
        let count = stored.len();

        for pattern in stored {
            // Pre-populate cache with known patterns
            self.cache
                .put_l2(hash_template(&pattern.template), pattern.id);
        }

        tracing::info!("Loaded {} patterns from persistence", count);
        Ok(count)
    }

    /// Save all patterns to persistence
    pub fn save_patterns(&self) -> TransformResult<()> {
        let patterns = self.drain.all_patterns();
        self.persistence.save_all(&patterns)?;
        tracing::info!("Saved {} patterns to persistence", patterns.len());
        Ok(())
    }

    /// Get transformer metrics
    pub fn metrics(&self) -> &PatternMetrics {
        &self.metrics
    }

    /// Get cache statistics
    pub fn cache_stats(&self) -> &CacheStats {
        self.cache.stats()
    }

    /// Get pattern count
    pub fn pattern_count(&self) -> usize {
        self.drain.pattern_count()
    }

    /// Get a pattern by ID
    pub fn get_pattern(&self, id: PatternId) -> Option<Pattern> {
        self.drain.get_pattern(id)
    }

    /// Process a batch and extract pattern IDs
    fn process_batch(&self, mut batch: Batch) -> Batch {
        // Only process log batches
        if batch.batch_type() != BatchType::Log {
            return batch;
        }

        self.metrics
            .batches_processed
            .fetch_add(1, Ordering::Relaxed);

        let message_count = batch.message_count();
        let mut pattern_ids = Vec::with_capacity(message_count);

        // Process each message in the batch
        for i in 0..message_count {
            let pattern_id = if let Some(raw) = batch.get_message(i) {
                // Try to decode the FlatBuffer to get service and message
                self.extract_pattern_from_flatbuffer(raw)
            } else {
                0 // Invalid message
            };
            pattern_ids.push(pattern_id);
        }

        self.metrics
            .messages_processed
            .fetch_add(message_count as u64, Ordering::Relaxed);

        batch.set_pattern_ids(pattern_ids);
        batch
    }

    /// Extract pattern ID from a FlatBuffer message
    ///
    /// Decodes the FlatBuffer to get service and message content,
    /// then uses service-aware hashing for pattern isolation.
    fn extract_pattern_from_flatbuffer(&self, raw: &[u8]) -> PatternId {
        // Try to parse as FlatBuffer batch
        let flat_batch = match FlatBatch::parse(raw) {
            Ok(fb) => fb,
            Err(_) => {
                // Not a valid FlatBuffer, use raw as message with empty service
                let message = String::from_utf8_lossy(raw);
                return self.extract_pattern("", &message);
            }
        };

        // Only process log schema
        if flat_batch.schema_type() != SchemaType::Log {
            return 0;
        }

        // Get the data payload
        let data = match flat_batch.data() {
            Ok(d) => d,
            Err(_) => return 0,
        };

        // Decode log entries
        let logs = match decode_log_data(data) {
            Ok(l) => l,
            Err(_) => return 0,
        };

        // Process first log entry (batch typically has one message per FlatBuffer)
        if let Some(log) = logs.first() {
            let service = log.service.unwrap_or("");
            let message = extract_message_from_payload(log.payload);
            self.extract_pattern(service, &message)
        } else {
            0
        }
    }

    /// Extract pattern ID for a single message with service isolation
    fn extract_pattern(&self, service: &str, message: &str) -> PatternId {
        // Try cache first (service-aware)
        if let Some(id) = self.cache.get(service, message) {
            self.metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
            return id;
        }

        self.metrics.cache_misses.fetch_add(1, Ordering::Relaxed);

        // Drain tree lookup (message only - service handled by cache)
        let pattern_count_before = self.drain.pattern_count();
        let id = self.drain.parse(message);
        let pattern_count_after = self.drain.pattern_count();

        // Track new patterns
        if pattern_count_after > pattern_count_before {
            self.metrics
                .patterns_created
                .fetch_add(1, Ordering::Relaxed);

            // Persist new pattern
            if let Some(pattern) = self.drain.get_pattern(id) {
                self.persist_pattern(pattern);
            }
        }

        // Update cache (service-aware)
        self.cache.put(service, message, id);

        id
    }

    /// Persist a pattern (async via worker or sync fallback)
    fn persist_pattern(&self, pattern: Pattern) {
        // Try background worker first (non-blocking)
        if let Some(ref handle) = self.worker_handle {
            if handle.send(pattern.clone()) {
                return; // Successfully queued
            }
            // Channel full - fall through to sync persistence
            tracing::debug!("Pattern worker channel full, using sync persistence");
        }

        // Sync fallback: add to pending queue
        if self.persistence.add_pattern(&pattern) {
            // Batch threshold reached, flush synchronously
            if let Err(e) = self.persistence.flush() {
                tracing::warn!(error = %e, "Failed to flush patterns");
            }
        }
    }
}

/// Extract message text from log payload
///
/// Looks for common message fields in JSON payload.
/// Falls back to raw payload string if not JSON.
fn extract_message_from_payload(payload: &[u8]) -> String {
    if payload.is_empty() {
        return String::new();
    }

    // Try to parse as JSON
    if let Ok(json) = serde_json::from_slice::<serde_json::Value>(payload) {
        // Look for common message fields
        const MESSAGE_FIELDS: &[&str] = &["message", "msg", "text", "log", "event"];

        for field in MESSAGE_FIELDS {
            if let Some(serde_json::Value::String(s)) = json.get(field) {
                return s.clone();
            }
        }

        // No message field found, return entire JSON as string
        return String::from_utf8_lossy(payload).into_owned();
    }

    // Not JSON, return raw string
    String::from_utf8_lossy(payload).into_owned()
}

impl Transformer for PatternTransformer {
    fn transform<'a>(
        &'a self,
        batch: Batch,
    ) -> Pin<Box<dyn Future<Output = TransformResult<Batch>> + Send + 'a>> {
        Box::pin(async move {
            if !self.config.enabled {
                return Ok(batch);
            }

            Ok(self.process_batch(batch))
        })
    }

    fn name(&self) -> &'static str {
        "pattern_matcher"
    }

    fn enabled(&self) -> bool {
        self.config.enabled
    }

    fn close(&self) -> TransformResult<()> {
        // Save patterns to persistence if enabled
        if self.config.persistence.enabled {
            self.save_patterns()?;
        }
        Ok(())
    }
}

/// Factory for creating PatternTransformer instances
pub struct PatternFactory;

impl TransformerFactory for PatternFactory {
    fn create(&self, config: &TransformerConfig) -> TransformResult<Box<dyn Transformer>> {
        let pattern_config = parse_pattern_config(config)?;
        let transformer = PatternTransformer::new(pattern_config)?;
        Ok(Box::new(transformer))
    }

    fn name(&self) -> &'static str {
        "pattern_matcher"
    }

    fn default_config(&self) -> Option<TransformerConfig> {
        let mut config = TransformerConfig::new();
        config.insert("similarity_threshold".to_string(), toml::Value::Float(0.5));
        config.insert("max_child_nodes".to_string(), toml::Value::Integer(100));
        config.insert("cache_size".to_string(), toml::Value::Integer(100_000));
        Some(config)
    }
}

/// Parse pattern config from TOML values
fn parse_pattern_config(config: &TransformerConfig) -> TransformResult<PatternConfig> {
    let mut pattern_config = PatternConfig::default();

    if let Some(toml::Value::Float(v)) = config.get("similarity_threshold") {
        pattern_config.similarity_threshold = *v;
    }

    if let Some(toml::Value::Integer(v)) = config.get("max_child_nodes") {
        pattern_config.max_child_nodes = *v as usize;
    }

    if let Some(toml::Value::Integer(v)) = config.get("cache_size") {
        pattern_config.cache_size = *v as usize;
    }

    if let Some(toml::Value::Boolean(v)) = config.get("enabled") {
        pattern_config.enabled = *v;
    }

    if let Some(toml::Value::String(v)) = config.get("persistence_file") {
        pattern_config.persistence =
            PersistenceConfig::default().with_file(std::path::PathBuf::from(v));
    }

    pattern_config.validate().map_err(TransformError::config)?;

    Ok(pattern_config)
}

/// Hash a template for cache lookup (re-export for persistence)
fn hash_template(template: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    template.hash(&mut hasher);
    hasher.finish()
}
