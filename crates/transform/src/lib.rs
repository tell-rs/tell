//! Tell - Transform
//!
//! Transformer chain for batch modification in-flight.
//!
//! # Overview
//!
//! Transformers modify batches as they flow through the pipeline. They can:
//! - Enrich data (add fields, resolve IDs)
//! - Extract patterns (log pattern matching)
//! - Filter messages
//! - Normalize formats
//!
//! # Design Principles
//!
//! - **Fast**: Transformers should add microseconds, not milliseconds
//! - **Non-blocking**: Never block on I/O or external services
//! - **Thread-safe**: Transformers may be called concurrently
//! - **Zero-cost when disabled**: Empty chain is a no-op
//!
//! # Architecture
//!
//! ```text
//! [Batch] → [Transformer 1] → [Transformer 2] → ... → [Batch']
//! ```
//!
//! Transformers are chained together and applied in order. The `Chain`
//! struct handles sequencing and error propagation.
//!
//! # Adding a New Transformer
//!
//! Each transformer owns its config. Follow this pattern:
//!
//! 1. **Create config struct** with builder pattern and validation:
//!
//! ```ignore
//! // my_transformer/config.rs
//! pub struct MyConfig {
//!     pub enabled: bool,
//!     pub threshold: f64,
//! }
//!
//! impl MyConfig {
//!     pub fn with_threshold(mut self, t: f64) -> Self {
//!         self.threshold = t;
//!         self
//!     }
//!
//!     pub fn validate(&self) -> Result<(), String> {
//!         if self.threshold < 0.0 { return Err("threshold must be >= 0".into()); }
//!         Ok(())
//!     }
//! }
//! ```
//!
//! 2. **Implement `TryFrom<&TransformerInstanceConfig>`** for TOML parsing:
//!
//! ```ignore
//! impl TryFrom<&TransformerInstanceConfig> for MyConfig {
//!     type Error = String;
//!
//!     fn try_from(config: &TransformerInstanceConfig) -> Result<Self, Self::Error> {
//!         let mut my_config = MyConfig::default();
//!
//!         if let Some(t) = config.get_float("threshold") {
//!             my_config.threshold = t;
//!         }
//!
//!         my_config.validate()?;
//!         Ok(my_config)
//!     }
//! }
//! ```
//!
//! 3. **Implement `Transformer` trait** on your transformer struct.
//!
//! 4. **Register in `create_default_registry()`** and add to `KNOWN_TRANSFORMER_TYPES`.
//!
//! # Modules
//!
//! - `chain` - Sequential transformer execution
//! - `registry` - Dynamic transformer creation from config
//! - `lazy_batch` - Decode-on-demand batch wrapper
//! - `noop` - Pass-through transformer for testing
//! - `pattern` - Log pattern extraction using Drain algorithm
//! - `reduce` - Event consolidation for cost reduction
//! - `filter` - Drop unwanted events based on conditions
//! - `redact` - Remove or pseudonymize PII
//!
//! # Example
//!
//! ```ignore
//! use tell_transform::{Chain, PatternTransformer, PatternConfig};
//!
//! // Create transformers
//! let pattern = PatternTransformer::new(PatternConfig::default())?;
//!
//! // Build chain
//! let chain = Chain::new(vec![Box::new(pattern)]);
//!
//! // Transform batches
//! let transformed = chain.transform(batch).await?;
//! ```

mod chain;
mod error;
mod lazy_batch;
pub mod filter;
pub mod noop;
pub mod pattern;
pub mod redact;
pub mod reduce;
pub mod registry;

pub use chain::Chain;
pub use error::TransformError;
pub use filter::{
    Condition, FilterAction, FilterConfig, FilterFactory, FilterMetrics, FilterTransformer,
    MatchMode, Operator,
};
pub use lazy_batch::{DecodedLogs, DecodedMessages, IntoLazyBatch, LazyBatch, OwnedLogEntry};
pub use noop::NoopTransformer;
pub use pattern::{
    BoxedPatternStorage, CacheStats, ClickHouseConfig, DrainTree, FilePatternStorage,
    NullPatternStorage, Pattern, PatternCache, PatternConfig, PatternFactory, PatternId,
    PatternMetrics, PatternPersistence, PatternStorage, PatternTransformer, PatternWorker,
    PatternWorkerHandle, PersistenceConfig, ReloadConfig, ReloadWorker, ReloadWorkerConfig,
    ReloadWorkerHandle, StorageError, StorageResult, StoredPattern, WorkerConfig,
    spawn_pattern_worker, spawn_reload_worker,
};
pub use redact::{
    CustomPattern, PatternType, PseudonymHasher, RedactConfig, RedactFactory, RedactMetrics,
    RedactStrategy, RedactTransformer, TargetedField,
};
pub use reduce::{ReduceConfig, ReduceFactory, ReduceGroup, ReduceMetrics, ReduceState, ReduceTransformer};
pub use registry::{default_registry, NoopFactory, TransformerConfig, TransformerFactory, TransformerRegistry};

use tell_protocol::Batch;
use std::future::Future;
use std::pin::Pin;

/// Result type for transformer operations
pub type TransformResult<T> = Result<T, TransformError>;

/// Trait for batch transformers
///
/// Implementors must be `Send + Sync` to allow concurrent use across tasks.
///
/// # Design
///
/// Transformers should be:
/// - **Fast**: Complete in microseconds, not milliseconds
/// - **Non-blocking**: Never block on I/O in the transform path
/// - **Thread-safe**: May be called from multiple tasks concurrently
///
/// # Example
///
/// ```ignore
/// struct MyTransformer;
///
/// impl Transformer for MyTransformer {
///     fn transform<'a>(
///         &'a self,
///         batch: Batch,
///     ) -> Pin<Box<dyn Future<Output = TransformResult<Batch>> + Send + 'a>> {
///         Box::pin(async move {
///             // Transform the batch
///             Ok(batch)
///         })
///     }
///
///     fn name(&self) -> &'static str {
///         "my_transformer"
///     }
/// }
/// ```
pub trait Transformer: Send + Sync {
    /// Transform a batch, returning the modified batch
    ///
    /// The transformer can modify the batch in-place or return a new batch.
    /// Returning an error will cause the batch to be dropped.
    fn transform<'a>(
        &'a self,
        batch: Batch,
    ) -> Pin<Box<dyn Future<Output = TransformResult<Batch>> + Send + 'a>>;

    /// Name of this transformer for logging and metrics
    fn name(&self) -> &'static str;

    /// Whether this transformer is currently enabled
    ///
    /// Disabled transformers are filtered out of chains at construction time.
    fn enabled(&self) -> bool {
        true
    }

    /// Clean up resources held by this transformer
    ///
    /// Called during graceful shutdown. Implementations should:
    /// - Flush any pending data
    /// - Close file handles
    /// - Save state if persistence is enabled
    ///
    /// Default implementation is a no-op.
    fn close(&self) -> TransformResult<()> {
        Ok(())
    }
}

/// Create a default transformer registry with all built-in transformers
///
/// Includes:
/// - `noop` - Pass-through transformer
/// - `pattern_matcher` - Log pattern extraction
/// - `reduce` - Event consolidation for cost reduction
/// - `filter` - Drop unwanted events based on conditions
/// - `redact` - Remove or pseudonymize PII
pub fn create_default_registry() -> TransformerRegistry {
    let mut registry = TransformerRegistry::new();
    registry.register("noop", NoopFactory);
    registry.register("pattern_matcher", PatternFactory);
    registry.register("reduce", ReduceFactory);
    registry.register("filter", FilterFactory);
    registry.register("redact", RedactFactory);
    registry
}
