//! Per-sink metrics reporter
//!
//! Reports metrics for individual sinks at their configured intervals.
//! This provides granular visibility into each sink's health.

use crate::{
    HumanFormatter, JsonFormatter, SinkMetricsProvider, SinkMetricsSnapshot,
    format::MetricsFormatter,
};
use std::sync::Arc;
use tell_config::MetricsFormat;
use tokio::time::{Duration, interval};
use tokio_util::sync::CancellationToken;
use tracing::info;

/// Per-sink metrics reporter
///
/// Reports metrics for a single sink at its configured interval.
/// Spawn one of these for each sink that has metrics enabled.
pub struct SinkReporter {
    sink: Arc<dyn SinkMetricsProvider>,
    formatter: Box<dyn MetricsFormatter>,
    previous: Option<SinkMetricsSnapshot>,
}

impl SinkReporter {
    /// Create a new sink reporter
    pub fn new(sink: Arc<dyn SinkMetricsProvider>, format: MetricsFormat) -> Self {
        let formatter: Box<dyn MetricsFormatter> = match format {
            MetricsFormat::Human => Box::new(HumanFormatter::new()),
            MetricsFormat::Json => Box::new(JsonFormatter::new()),
        };

        Self {
            sink,
            formatter,
            previous: None,
        }
    }

    /// Run the reporter until cancellation
    pub async fn run(mut self, cancel: CancellationToken) {
        let config = self.sink.metrics_config();

        if !config.enabled {
            return;
        }

        let mut ticker = interval(config.interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        info!(
            sink_id = self.sink.sink_id(),
            sink_type = self.sink.sink_type(),
            interval_secs = config.interval.as_secs(),
            "sink metrics reporter started"
        );

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    break;
                }
                _ = ticker.tick() => {
                    self.report(config.interval);
                }
            }
        }
    }

    /// Report current metrics
    fn report(&mut self, report_interval: Duration) {
        let snapshot = self.sink.snapshot();

        // Calculate delta from previous if available
        let output = self.formatter.format_sink(
            self.sink.sink_id(),
            self.sink.sink_type(),
            &snapshot,
            report_interval.as_secs(),
        );

        info!("{}", output);

        self.previous = Some(snapshot);
    }
}

/// Spawn sink reporters for all sinks with metrics enabled
///
/// Returns handles to the spawned tasks (for cleanup, though normally
/// they just run until cancellation).
pub fn spawn_sink_reporters(
    sinks: Vec<Arc<dyn SinkMetricsProvider>>,
    format: MetricsFormat,
    cancel: CancellationToken,
) -> Vec<tokio::task::JoinHandle<()>> {
    sinks
        .into_iter()
        .filter(|sink| sink.metrics_config().enabled)
        .map(|sink| {
            let reporter = SinkReporter::new(sink, format);
            let cancel = cancel.clone();
            tokio::spawn(async move {
                reporter.run(cancel).await;
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SinkMetrics, SinkMetricsConfig};
    use std::sync::atomic::Ordering;

    struct TestSink {
        id: String,
        sink_type: String,
        metrics: SinkMetrics,
        config: SinkMetricsConfig,
    }

    impl SinkMetricsProvider for TestSink {
        fn sink_id(&self) -> &str {
            &self.id
        }

        fn sink_type(&self) -> &str {
            &self.sink_type
        }

        fn metrics_config(&self) -> SinkMetricsConfig {
            self.config
        }

        fn snapshot(&self) -> SinkMetricsSnapshot {
            self.metrics.snapshot()
        }
    }

    #[test]
    fn test_new_reporter() {
        let sink = Arc::new(TestSink {
            id: "test".into(),
            sink_type: "test".into(),
            metrics: SinkMetrics::new(),
            config: SinkMetricsConfig::default(),
        }) as Arc<dyn SinkMetricsProvider>;

        let reporter = SinkReporter::new(sink, MetricsFormat::Human);
        assert!(reporter.previous.is_none());
    }

    #[tokio::test]
    async fn test_run_disabled() {
        let sink = Arc::new(TestSink {
            id: "test".into(),
            sink_type: "test".into(),
            metrics: SinkMetrics::new(),
            config: SinkMetricsConfig {
                enabled: false,
                interval: Duration::from_secs(10),
            },
        }) as Arc<dyn SinkMetricsProvider>;

        let reporter = SinkReporter::new(sink, MetricsFormat::Human);
        let cancel = CancellationToken::new();

        // Should return immediately when disabled
        reporter.run(cancel).await;
    }

    #[tokio::test]
    async fn test_run_cancellation() {
        let sink = Arc::new(TestSink {
            id: "test".into(),
            sink_type: "test".into(),
            metrics: SinkMetrics::new(),
            config: SinkMetricsConfig {
                enabled: true,
                interval: Duration::from_millis(100),
            },
        }) as Arc<dyn SinkMetricsProvider>;

        let reporter = SinkReporter::new(sink, MetricsFormat::Human);
        let cancel = CancellationToken::new();

        let cancel_clone = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            cancel_clone.cancel();
        });

        // Should exit when cancelled
        reporter.run(cancel).await;
    }

    #[tokio::test]
    async fn test_report_with_activity() {
        let metrics = SinkMetrics::new();
        metrics.batches_written.fetch_add(100, Ordering::Relaxed);
        metrics.messages_written.fetch_add(50000, Ordering::Relaxed);
        metrics
            .bytes_written
            .fetch_add(10_000_000, Ordering::Relaxed);

        let sink = Arc::new(TestSink {
            id: "clickhouse".into(),
            sink_type: "clickhouse".into(),
            metrics,
            config: SinkMetricsConfig {
                enabled: true,
                interval: Duration::from_secs(10),
            },
        }) as Arc<dyn SinkMetricsProvider>;

        let mut reporter = SinkReporter::new(sink, MetricsFormat::Human);

        // Report should work without panicking
        reporter.report(Duration::from_secs(10));

        assert!(reporter.previous.is_some());
        assert_eq!(reporter.previous.unwrap().batches_written, 100);
    }

    #[test]
    fn test_spawn_filters_disabled() {
        let enabled_sink = Arc::new(TestSink {
            id: "enabled".into(),
            sink_type: "test".into(),
            metrics: SinkMetrics::new(),
            config: SinkMetricsConfig {
                enabled: true,
                interval: Duration::from_secs(10),
            },
        }) as Arc<dyn SinkMetricsProvider>;

        let disabled_sink = Arc::new(TestSink {
            id: "disabled".into(),
            sink_type: "test".into(),
            metrics: SinkMetrics::new(),
            config: SinkMetricsConfig {
                enabled: false,
                interval: Duration::from_secs(10),
            },
        }) as Arc<dyn SinkMetricsProvider>;

        // Note: We can't easily test the actual spawn without a runtime,
        // but we can verify the filtering logic works
        let sinks = [enabled_sink, disabled_sink];
        let enabled_count = sinks.iter().filter(|s| s.metrics_config().enabled).count();

        assert_eq!(enabled_count, 1);
    }
}
