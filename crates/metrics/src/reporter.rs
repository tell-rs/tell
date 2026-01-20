//! Unified metrics reporter
//!
//! Collects metrics from all components and reports them periodically.
//!
//! # Overview
//!
//! The `UnifiedReporter` aggregates metrics from:
//! - Pipeline router (batches, messages, backpressure)
//! - Sources (connections, messages received)
//! - Sinks (messages written, errors)
//! - Transformers (batches processed, timing)
//!
//! It runs as an async task, collecting snapshots at the configured interval
//! and outputting formatted metrics via tracing.

use crate::{
    format::MetricsFormatter, CollectedMetrics, CollectedSink, CollectedSource,
    CollectedTransformer, HumanFormatter, JsonFormatter, PipelineSnapshot, SinkMetricsProvider,
    SourceMetricsProvider, TransformerMetricsProvider,
};
use tell_config::{MetricsConfig, MetricsFormat};
use std::sync::Arc;
use std::time::Instant;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::info;

/// Trait for pipeline metrics provider
///
/// The pipeline has its own metrics in `pipeline::RouterMetrics`.
/// This trait bridges to our `PipelineSnapshot`.
pub trait PipelineMetricsProvider: Send + Sync {
    /// Get a snapshot of pipeline metrics
    fn pipeline_snapshot(&self) -> PipelineSnapshot;
}

/// Builder for constructing a UnifiedReporter
#[derive(Default)]
pub struct UnifiedReporterBuilder {
    config: Option<MetricsConfig>,
    pipeline: Option<Arc<dyn PipelineMetricsProvider>>,
    sources: Vec<Arc<dyn SourceMetricsProvider>>,
    sinks: Vec<Arc<dyn SinkMetricsProvider>>,
    transformers: Vec<Arc<dyn TransformerMetricsProvider>>,
}

impl UnifiedReporterBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the metrics configuration
    pub fn config(mut self, config: MetricsConfig) -> Self {
        self.config = Some(config);
        self
    }

    /// Set the pipeline metrics provider
    pub fn pipeline(mut self, provider: Arc<dyn PipelineMetricsProvider>) -> Self {
        self.pipeline = Some(provider);
        self
    }

    /// Register a source metrics provider
    pub fn source(mut self, provider: Arc<dyn SourceMetricsProvider>) -> Self {
        self.sources.push(provider);
        self
    }

    /// Register multiple source metrics providers
    pub fn sources(mut self, providers: Vec<Arc<dyn SourceMetricsProvider>>) -> Self {
        self.sources.extend(providers);
        self
    }

    /// Register a sink metrics provider
    pub fn sink(mut self, provider: Arc<dyn SinkMetricsProvider>) -> Self {
        self.sinks.push(provider);
        self
    }

    /// Register multiple sink metrics providers
    pub fn sinks(mut self, providers: Vec<Arc<dyn SinkMetricsProvider>>) -> Self {
        self.sinks.extend(providers);
        self
    }

    /// Register a transformer metrics provider
    pub fn transformer(mut self, provider: Arc<dyn TransformerMetricsProvider>) -> Self {
        self.transformers.push(provider);
        self
    }

    /// Register multiple transformer metrics providers
    pub fn transformers(mut self, providers: Vec<Arc<dyn TransformerMetricsProvider>>) -> Self {
        self.transformers.extend(providers);
        self
    }

    /// Build the UnifiedReporter
    pub fn build(self) -> UnifiedReporter {
        let config = self.config.unwrap_or_default();
        let formatter: Box<dyn MetricsFormatter> = match config.format {
            MetricsFormat::Human => Box::new(HumanFormatter::new()),
            MetricsFormat::Json => Box::new(JsonFormatter::new()),
        };

        UnifiedReporter {
            config,
            formatter,
            pipeline: self.pipeline,
            sources: self.sources,
            sinks: self.sinks,
            transformers: self.transformers,
            previous: None,
        }
    }
}

/// Unified metrics reporter
///
/// Collects and reports metrics from all components at a configured interval.
pub struct UnifiedReporter {
    config: MetricsConfig,
    formatter: Box<dyn MetricsFormatter>,
    pipeline: Option<Arc<dyn PipelineMetricsProvider>>,
    sources: Vec<Arc<dyn SourceMetricsProvider>>,
    sinks: Vec<Arc<dyn SinkMetricsProvider>>,
    transformers: Vec<Arc<dyn TransformerMetricsProvider>>,
    previous: Option<CollectedMetrics>,
}

impl UnifiedReporter {
    /// Create a new builder
    pub fn builder() -> UnifiedReporterBuilder {
        UnifiedReporterBuilder::new()
    }

    /// Run the reporter until cancellation
    ///
    /// This is the main entry point - spawn this as a tokio task.
    pub async fn run(mut self, cancel: CancellationToken) {
        if !self.config.enabled {
            info!("metrics reporting disabled");
            return;
        }

        let mut ticker = interval(self.config.interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        info!(
            interval_secs = self.config.interval.as_secs(),
            format = ?self.config.format,
            "metrics reporter started"
        );

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    info!("metrics reporter shutting down");
                    break;
                }
                _ = ticker.tick() => {
                    self.report();
                }
            }
        }
    }

    /// Collect and report metrics once
    fn report(&mut self) {
        let metrics = self.collect();
        let rates = self.previous.as_ref().and_then(|prev| metrics.rates(prev));

        let output = self.formatter.format_unified(&metrics, rates.as_ref());

        // Log each line separately for human format (multiple lines)
        for line in output.lines() {
            info!("{}", line);
        }

        self.previous = Some(metrics);
    }

    /// Collect metrics from all registered providers
    fn collect(&self) -> CollectedMetrics {
        let mut metrics = CollectedMetrics {
            timestamp: Some(Instant::now()),
            ..Default::default()
        };

        // Pipeline metrics
        if self.config.include_pipeline
            && let Some(ref provider) = self.pipeline
        {
            metrics.pipeline = Some(provider.pipeline_snapshot());
        }

        // Source metrics
        if self.config.include_sources {
            metrics.sources = self
                .sources
                .iter()
                .map(|s| CollectedSource {
                    id: s.source_id().to_string(),
                    source_type: s.source_type().to_string(),
                    snapshot: s.snapshot(),
                })
                .collect();
        }

        // Sink metrics
        if self.config.include_sinks {
            metrics.sinks = self
                .sinks
                .iter()
                .map(|s| CollectedSink {
                    id: s.sink_id().to_string(),
                    sink_type: s.sink_type().to_string(),
                    snapshot: s.snapshot(),
                })
                .collect();
        }

        // Transformer metrics
        if self.config.include_transforms {
            metrics.transformers = self
                .transformers
                .iter()
                .map(|t| CollectedTransformer {
                    id: t.transformer_id().to_string(),
                    transformer_type: t.transformer_type().to_string(),
                    snapshot: t.snapshot(),
                })
                .collect();
        }

        metrics
    }

    /// Add a source provider dynamically
    pub fn add_source(&mut self, provider: Arc<dyn SourceMetricsProvider>) {
        self.sources.push(provider);
    }

    /// Add a sink provider dynamically
    pub fn add_sink(&mut self, provider: Arc<dyn SinkMetricsProvider>) {
        self.sinks.push(provider);
    }

    /// Add a transformer provider dynamically
    pub fn add_transformer(&mut self, provider: Arc<dyn TransformerMetricsProvider>) {
        self.transformers.push(provider);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SinkMetrics, SourceMetrics, TransformerMetrics};
    use std::time::Duration;

    // Test implementations
    struct TestPipeline {
        snapshot: PipelineSnapshot,
    }

    impl PipelineMetricsProvider for TestPipeline {
        fn pipeline_snapshot(&self) -> PipelineSnapshot {
            self.snapshot
        }
    }

    struct TestSource {
        id: String,
        metrics: SourceMetrics,
    }

    impl SourceMetricsProvider for TestSource {
        fn source_id(&self) -> &str {
            &self.id
        }
        fn source_type(&self) -> &str {
            "test"
        }
        fn snapshot(&self) -> crate::SourceMetricsSnapshot {
            self.metrics.snapshot()
        }
    }

    struct TestSink {
        id: String,
        metrics: SinkMetrics,
    }

    impl SinkMetricsProvider for TestSink {
        fn sink_id(&self) -> &str {
            &self.id
        }
        fn sink_type(&self) -> &str {
            "test"
        }
        fn metrics_config(&self) -> crate::SinkMetricsConfig {
            crate::SinkMetricsConfig::default()
        }
        fn snapshot(&self) -> crate::SinkMetricsSnapshot {
            self.metrics.snapshot()
        }
    }

    struct TestTransformer {
        id: String,
        metrics: TransformerMetrics,
    }

    impl TransformerMetricsProvider for TestTransformer {
        fn transformer_id(&self) -> &str {
            &self.id
        }
        fn transformer_type(&self) -> &str {
            "test"
        }
        fn snapshot(&self) -> crate::TransformerMetricsSnapshot {
            self.metrics.snapshot()
        }
    }

    #[test]
    fn test_builder_default() {
        let reporter = UnifiedReporter::builder().build();

        assert!(reporter.config.enabled);
        assert!(reporter.pipeline.is_none());
        assert!(reporter.sources.is_empty());
        assert!(reporter.sinks.is_empty());
        assert!(reporter.transformers.is_empty());
    }

    #[test]
    fn test_builder_with_config() {
        let config = MetricsConfig {
            enabled: false,
            interval: Duration::from_secs(30),
            format: MetricsFormat::Json,
            ..Default::default()
        };

        let reporter = UnifiedReporter::builder().config(config.clone()).build();

        assert!(!reporter.config.enabled);
        assert_eq!(reporter.config.interval, Duration::from_secs(30));
    }

    #[test]
    fn test_builder_with_providers() {
        let pipeline = Arc::new(TestPipeline {
            snapshot: PipelineSnapshot::default(),
        }) as Arc<dyn PipelineMetricsProvider>;

        let source = Arc::new(TestSource {
            id: "tcp".into(),
            metrics: SourceMetrics::new(),
        }) as Arc<dyn SourceMetricsProvider>;

        let sink = Arc::new(TestSink {
            id: "clickhouse".into(),
            metrics: SinkMetrics::new(),
        }) as Arc<dyn SinkMetricsProvider>;

        let transformer = Arc::new(TestTransformer {
            id: "pattern".into(),
            metrics: TransformerMetrics::new(),
        }) as Arc<dyn TransformerMetricsProvider>;

        let reporter = UnifiedReporter::builder()
            .pipeline(pipeline)
            .source(source)
            .sink(sink)
            .transformer(transformer)
            .build();

        assert!(reporter.pipeline.is_some());
        assert_eq!(reporter.sources.len(), 1);
        assert_eq!(reporter.sinks.len(), 1);
        assert_eq!(reporter.transformers.len(), 1);
    }

    #[test]
    fn test_collect_empty() {
        let reporter = UnifiedReporter::builder().build();
        let metrics = reporter.collect();

        assert!(metrics.timestamp.is_some());
        assert!(metrics.pipeline.is_none());
        assert!(metrics.sources.is_empty());
        assert!(metrics.sinks.is_empty());
        assert!(metrics.transformers.is_empty());
    }

    #[test]
    fn test_collect_with_providers() {
        let pipeline = Arc::new(TestPipeline {
            snapshot: PipelineSnapshot {
                messages_processed: 1000,
                bytes_processed: 50000,
                ..Default::default()
            },
        }) as Arc<dyn PipelineMetricsProvider>;

        let source = Arc::new(TestSource {
            id: "tcp".into(),
            metrics: SourceMetrics::new(),
        }) as Arc<dyn SourceMetricsProvider>;

        let reporter = UnifiedReporter::builder()
            .pipeline(pipeline)
            .source(source)
            .build();

        let metrics = reporter.collect();

        assert!(metrics.pipeline.is_some());
        assert_eq!(metrics.pipeline.unwrap().messages_processed, 1000);
        assert_eq!(metrics.sources.len(), 1);
        assert_eq!(metrics.sources[0].id, "tcp");
    }

    #[test]
    fn test_collect_respects_include_flags() {
        let pipeline = Arc::new(TestPipeline {
            snapshot: PipelineSnapshot::default(),
        }) as Arc<dyn PipelineMetricsProvider>;

        let source = Arc::new(TestSource {
            id: "tcp".into(),
            metrics: SourceMetrics::new(),
        }) as Arc<dyn SourceMetricsProvider>;

        let config = MetricsConfig {
            include_pipeline: false,
            include_sources: false,
            ..Default::default()
        };

        let reporter = UnifiedReporter::builder()
            .config(config)
            .pipeline(pipeline)
            .source(source)
            .build();

        let metrics = reporter.collect();

        assert!(metrics.pipeline.is_none()); // Excluded by flag
        assert!(metrics.sources.is_empty()); // Excluded by flag
    }

    #[tokio::test]
    async fn test_run_disabled() {
        let config = MetricsConfig {
            enabled: false,
            ..Default::default()
        };

        let reporter = UnifiedReporter::builder().config(config).build();
        let cancel = CancellationToken::new();

        // Should return immediately when disabled
        reporter.run(cancel).await;
    }

    #[tokio::test]
    async fn test_run_cancellation() {
        let config = MetricsConfig {
            enabled: true,
            interval: Duration::from_millis(100),
            ..Default::default()
        };

        let reporter = UnifiedReporter::builder().config(config).build();
        let cancel = CancellationToken::new();

        let cancel_clone = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            cancel_clone.cancel();
        });

        // Should exit when cancelled
        reporter.run(cancel).await;
    }
}
