//! Tell - Metrics
//!
//! Internal metrics collection and reporting for observability.
//!
//! # Overview
//!
//! This crate provides:
//! - Atomic metric counters for pipeline, sources, sinks, and transformers
//! - Provider traits for components to expose metrics
//! - Unified reporter with configurable output formats (human, JSON)
//! - Per-sink individual reporting
//!
//! # Design Principles
//!
//! - **Lock-free**: All metrics use atomic operations
//! - **Low overhead**: No allocations during metric updates
//! - **Configurable**: Reporting intervals and formats from config
//! - **Trait-based**: Components implement provider traits for collection
//!
//! # Metrics Handle Pattern
//!
//! Components use `Arc<Metrics>` internally and provide a `metrics_handle()` method
//! that returns a lightweight handle implementing the appropriate provider trait.
//! The handle remains valid after `run()` consumes the component.
//!
//! ```text
//! Component (owns Arc<Metrics>)
//!     │
//!     ├──► metrics_handle() → Handle (clones Arc, implements Provider trait)
//!     │
//!     └──► run() [consumes self, Arc keeps metrics alive]
//!
//! Tell wiring:
//!     1. Create component
//!     2. Call metrics_handle() → store in Vec<Arc<dyn Provider>>
//!     3. Spawn component.run()
//!     4. Build UnifiedReporter with collected handles
//! ```
//!
//! # Metric Categories
//!
//! - **Pipeline**: batches_routed, messages_processed, dropped, backpressure
//! - **Sources**: connections_active, messages_received, bytes_received, errors
//! - **Sinks**: batches_written, messages_written, bytes_written, write_errors
//! - **Transformers**: batches_processed, processing_time, errors
//!
//! # Example
//!
//! ```ignore
//! use metrics::{SinkMetricsProvider, UnifiedReporter};
//! use std::sync::Arc;
//!
//! // Component provides metrics handle before running
//! let sink = MySink::new(config, receiver);
//! let handle: Arc<dyn SinkMetricsProvider> = Arc::new(sink.metrics_handle());
//! tokio::spawn(sink.run());
//!
//! // Reporter collects from handles
//! let reporter = UnifiedReporter::builder()
//!     .sinks(vec![handle])
//!     .build();
//! ```

mod collected;
pub mod format;
mod reporter;
mod sink_reporter;
mod traits;

pub use collected::{
    CollectedMetrics, CollectedSink, CollectedSource, CollectedTransformer, MetricsRates,
    PipelineRates, PipelineSnapshot, SinkRates, SourceRates, TransformerRates,
};
pub use format::{HumanFormatter, JsonFormatter, MetricsFormatter};
pub use reporter::{PipelineMetricsProvider, UnifiedReporter, UnifiedReporterBuilder};
pub use sink_reporter::{SinkReporter, spawn_sink_reporters};
pub use traits::{
    SinkMetrics, SinkMetricsConfig, SinkMetricsProvider, SinkMetricsSnapshot, SourceMetrics,
    SourceMetricsProvider, SourceMetricsSnapshot, TransformerMetrics, TransformerMetricsProvider,
    TransformerMetricsSnapshot,
};

use std::sync::atomic::{AtomicU64, Ordering};

/// Atomic counter wrapper for convenient metric operations
#[derive(Debug, Default)]
pub struct Counter(AtomicU64);

impl Counter {
    /// Create a new counter initialized to 0
    #[inline]
    pub const fn new() -> Self {
        Self(AtomicU64::new(0))
    }

    /// Increment the counter by `val` (relaxed ordering for performance)
    #[inline]
    pub fn add(&self, val: u64) {
        self.0.fetch_add(val, Ordering::Relaxed);
    }

    /// Increment the counter by 1
    #[inline]
    pub fn inc(&self) {
        self.add(1);
    }

    /// Get the current value (relaxed ordering)
    #[inline]
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }

    /// Reset the counter to 0 and return the previous value
    #[inline]
    pub fn take(&self) -> u64 {
        self.0.swap(0, Ordering::Relaxed)
    }
}
