//! Collected metrics snapshot and rate calculations
//!
//! This module contains the unified metrics snapshot that holds
//! all component metrics at a point in time, plus rate calculation
//! utilities for computing per-second rates.

use crate::{SinkMetricsSnapshot, SourceMetricsSnapshot, TransformerMetricsSnapshot};
use std::time::{Duration, Instant};

/// Pipeline router metrics snapshot
///
/// Matches the fields from `pipeline::RouterMetrics`.
#[derive(Debug, Clone, Copy, Default, serde::Serialize)]
pub struct PipelineSnapshot {
    /// Total batches received from sources
    pub batches_received: u64,
    /// Batches successfully routed
    pub batches_routed: u64,
    /// Batches dropped (no route or all sinks failed)
    pub batches_dropped: u64,
    /// Successful sink sends
    pub sink_sends_success: u64,
    /// Failed sink sends
    pub sink_sends_failed: u64,
    /// Backpressure events
    pub backpressure_events: u64,
    /// Total messages processed
    pub messages_processed: u64,
    /// Total bytes processed
    pub bytes_processed: u64,
}

/// Collected source snapshot with metadata
#[derive(Debug, Clone)]
pub struct CollectedSource {
    /// Source identifier
    pub id: String,
    /// Source type (tcp, syslog_tcp, etc.)
    pub source_type: String,
    /// Metrics snapshot
    pub snapshot: SourceMetricsSnapshot,
}

/// Collected sink snapshot with metadata
#[derive(Debug, Clone)]
pub struct CollectedSink {
    /// Sink identifier
    pub id: String,
    /// Sink type (clickhouse, disk_binary, etc.)
    pub sink_type: String,
    /// Metrics snapshot
    pub snapshot: SinkMetricsSnapshot,
}

/// Collected transformer snapshot with metadata
#[derive(Debug, Clone)]
pub struct CollectedTransformer {
    /// Transformer identifier
    pub id: String,
    /// Transformer type (pattern, redact, filter, etc.)
    pub transformer_type: String,
    /// Metrics snapshot
    pub snapshot: TransformerMetricsSnapshot,
}

/// Complete metrics collection at a point in time
#[derive(Debug, Clone, Default)]
pub struct CollectedMetrics {
    /// When this collection was taken
    pub timestamp: Option<Instant>,

    /// Pipeline router metrics
    pub pipeline: Option<PipelineSnapshot>,

    /// All source metrics
    pub sources: Vec<CollectedSource>,

    /// All sink metrics
    pub sinks: Vec<CollectedSink>,

    /// All transformer metrics
    pub transformers: Vec<CollectedTransformer>,
}

impl CollectedMetrics {
    /// Create a new empty collection
    pub fn new() -> Self {
        Self {
            timestamp: Some(Instant::now()),
            ..Default::default()
        }
    }

    /// Calculate rates by comparing with a previous snapshot
    ///
    /// Returns None if there's no previous snapshot or timestamps are missing.
    pub fn rates(&self, previous: &CollectedMetrics) -> Option<MetricsRates> {
        let current_ts = self.timestamp?;
        let previous_ts = previous.timestamp?;

        let elapsed = current_ts.duration_since(previous_ts);
        if elapsed.is_zero() {
            return None;
        }

        let elapsed_secs = elapsed.as_secs_f64();

        // Pipeline rates
        let pipeline_rates = match (&self.pipeline, &previous.pipeline) {
            (Some(current), Some(prev)) => Some(PipelineRates {
                messages_per_sec: rate(
                    current.messages_processed,
                    prev.messages_processed,
                    elapsed_secs,
                ),
                bytes_per_sec: rate(current.bytes_processed, prev.bytes_processed, elapsed_secs),
                batches_per_sec: rate(
                    current.batches_received,
                    prev.batches_received,
                    elapsed_secs,
                ),
            }),
            _ => None,
        };

        // Source rates (match by id)
        let source_rates: Vec<_> = self
            .sources
            .iter()
            .filter_map(|current| {
                let prev = previous.sources.iter().find(|s| s.id == current.id)?;
                Some(SourceRates {
                    id: current.id.clone(),
                    source_type: current.source_type.clone(),
                    messages_per_sec: rate(
                        current.snapshot.messages_received,
                        prev.snapshot.messages_received,
                        elapsed_secs,
                    ),
                    bytes_per_sec: rate(
                        current.snapshot.bytes_received,
                        prev.snapshot.bytes_received,
                        elapsed_secs,
                    ),
                    connections_active: current.snapshot.connections_active,
                    errors: current.snapshot.errors.saturating_sub(prev.snapshot.errors),
                })
            })
            .collect();

        // Sink rates (match by id)
        let sink_rates: Vec<_> = self
            .sinks
            .iter()
            .filter_map(|current| {
                let prev = previous.sinks.iter().find(|s| s.id == current.id)?;
                Some(SinkRates {
                    id: current.id.clone(),
                    sink_type: current.sink_type.clone(),
                    messages_per_sec: rate(
                        current.snapshot.messages_written,
                        prev.snapshot.messages_written,
                        elapsed_secs,
                    ),
                    bytes_per_sec: rate(
                        current.snapshot.bytes_written,
                        prev.snapshot.bytes_written,
                        elapsed_secs,
                    ),
                    errors: current
                        .snapshot
                        .write_errors
                        .saturating_sub(prev.snapshot.write_errors),
                })
            })
            .collect();

        // Transformer rates (match by id)
        let transformer_rates: Vec<_> = self
            .transformers
            .iter()
            .filter_map(|current| {
                let prev = previous.transformers.iter().find(|t| t.id == current.id)?;
                let batches_delta = current
                    .snapshot
                    .batches_processed
                    .saturating_sub(prev.snapshot.batches_processed);
                let time_delta = current
                    .snapshot
                    .processing_time_ns
                    .saturating_sub(prev.snapshot.processing_time_ns);

                Some(TransformerRates {
                    id: current.id.clone(),
                    transformer_type: current.transformer_type.clone(),
                    batches_per_sec: rate(
                        current.snapshot.batches_processed,
                        prev.snapshot.batches_processed,
                        elapsed_secs,
                    ),
                    avg_time: if batches_delta > 0 {
                        Duration::from_nanos(time_delta / batches_delta)
                    } else {
                        Duration::ZERO
                    },
                    errors: current.snapshot.errors.saturating_sub(prev.snapshot.errors),
                })
            })
            .collect();

        Some(MetricsRates {
            elapsed,
            pipeline: pipeline_rates,
            sources: source_rates,
            sinks: sink_rates,
            transformers: transformer_rates,
        })
    }
}

/// Calculate rate per second
#[inline]
fn rate(current: u64, previous: u64, elapsed_secs: f64) -> f64 {
    let delta = current.saturating_sub(previous);
    delta as f64 / elapsed_secs
}

/// Calculated rates between two snapshots
#[derive(Debug, Clone)]
pub struct MetricsRates {
    /// Time elapsed between snapshots
    pub elapsed: Duration,

    /// Pipeline rates
    pub pipeline: Option<PipelineRates>,

    /// Per-source rates
    pub sources: Vec<SourceRates>,

    /// Per-sink rates
    pub sinks: Vec<SinkRates>,

    /// Per-transformer rates
    pub transformers: Vec<TransformerRates>,
}

/// Pipeline rates
#[derive(Debug, Clone, Copy)]
pub struct PipelineRates {
    /// Messages processed per second
    pub messages_per_sec: f64,
    /// Bytes processed per second
    pub bytes_per_sec: f64,
    /// Batches received per second
    pub batches_per_sec: f64,
}

/// Source rates
#[derive(Debug, Clone)]
pub struct SourceRates {
    /// Source identifier
    pub id: String,
    /// Source type
    pub source_type: String,
    /// Messages received per second
    pub messages_per_sec: f64,
    /// Bytes received per second
    pub bytes_per_sec: f64,
    /// Current active connections
    pub connections_active: u64,
    /// Errors in this period
    pub errors: u64,
}

/// Sink rates
#[derive(Debug, Clone)]
pub struct SinkRates {
    /// Sink identifier
    pub id: String,
    /// Sink type
    pub sink_type: String,
    /// Messages written per second
    pub messages_per_sec: f64,
    /// Bytes written per second
    pub bytes_per_sec: f64,
    /// Errors in this period
    pub errors: u64,
}

/// Transformer rates
#[derive(Debug, Clone)]
pub struct TransformerRates {
    /// Transformer identifier
    pub id: String,
    /// Transformer type
    pub transformer_type: String,
    /// Batches processed per second
    pub batches_per_sec: f64,
    /// Average processing time per batch
    pub avg_time: Duration,
    /// Errors in this period
    pub errors: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_collected_metrics_new() {
        let metrics = CollectedMetrics::new();
        assert!(metrics.timestamp.is_some());
        assert!(metrics.pipeline.is_none());
        assert!(metrics.sources.is_empty());
        assert!(metrics.sinks.is_empty());
        assert!(metrics.transformers.is_empty());
    }

    #[test]
    fn test_rate_calculation() {
        assert_eq!(rate(1000, 0, 1.0), 1000.0);
        assert_eq!(rate(1000, 500, 2.0), 250.0);
        assert_eq!(rate(100, 100, 1.0), 0.0);
        // Saturating sub handles underflow
        assert_eq!(rate(50, 100, 1.0), 0.0);
    }

    #[test]
    fn test_pipeline_rates() {
        let start = Instant::now();
        let prev = CollectedMetrics {
            timestamp: Some(start),
            pipeline: Some(PipelineSnapshot {
                messages_processed: 0,
                bytes_processed: 0,
                batches_received: 0,
                ..Default::default()
            }),
            ..Default::default()
        };

        // Simulate 10 seconds later
        let current = CollectedMetrics {
            timestamp: Some(start + Duration::from_secs(10)),
            pipeline: Some(PipelineSnapshot {
                messages_processed: 10_000_000,
                bytes_processed: 500_000_000,
                batches_received: 20_000,
                ..Default::default()
            }),
            ..Default::default()
        };

        let rates = current.rates(&prev).unwrap();
        let pipeline = rates.pipeline.unwrap();

        assert_eq!(pipeline.messages_per_sec, 1_000_000.0);
        assert_eq!(pipeline.bytes_per_sec, 50_000_000.0);
        assert_eq!(pipeline.batches_per_sec, 2_000.0);
    }

    #[test]
    fn test_source_rates() {
        let start = Instant::now();

        let prev = CollectedMetrics {
            timestamp: Some(start),
            sources: vec![CollectedSource {
                id: "tcp".into(),
                source_type: "tcp".into(),
                snapshot: SourceMetricsSnapshot {
                    messages_received: 0,
                    bytes_received: 0,
                    connections_active: 5,
                    errors: 0,
                    ..Default::default()
                },
            }],
            ..Default::default()
        };

        let current = CollectedMetrics {
            timestamp: Some(start + Duration::from_secs(10)),
            sources: vec![CollectedSource {
                id: "tcp".into(),
                source_type: "tcp".into(),
                snapshot: SourceMetricsSnapshot {
                    messages_received: 100_000,
                    bytes_received: 5_000_000,
                    connections_active: 7,
                    errors: 2,
                    ..Default::default()
                },
            }],
            ..Default::default()
        };

        let rates = current.rates(&prev).unwrap();
        assert_eq!(rates.sources.len(), 1);

        let source = &rates.sources[0];
        assert_eq!(source.id, "tcp");
        assert_eq!(source.messages_per_sec, 10_000.0);
        assert_eq!(source.bytes_per_sec, 500_000.0);
        assert_eq!(source.connections_active, 7);
        assert_eq!(source.errors, 2);
    }

    #[test]
    fn test_rates_no_previous() {
        let current = CollectedMetrics::new();
        let prev = CollectedMetrics::default(); // No timestamp

        assert!(current.rates(&prev).is_none());
    }

    #[test]
    fn test_transformer_avg_time() {
        let start = Instant::now();

        let prev = CollectedMetrics {
            timestamp: Some(start),
            transformers: vec![CollectedTransformer {
                id: "pattern_0".into(),
                transformer_type: "pattern".into(),
                snapshot: TransformerMetricsSnapshot {
                    batches_processed: 0,
                    processing_time_ns: 0,
                    ..Default::default()
                },
            }],
            ..Default::default()
        };

        let current = CollectedMetrics {
            timestamp: Some(start + Duration::from_secs(10)),
            transformers: vec![CollectedTransformer {
                id: "pattern_0".into(),
                transformer_type: "pattern".into(),
                snapshot: TransformerMetricsSnapshot {
                    batches_processed: 1000,
                    processing_time_ns: 1_000_000_000, // 1 second total
                    ..Default::default()
                },
            }],
            ..Default::default()
        };

        let rates = current.rates(&prev).unwrap();
        let transformer = &rates.transformers[0];

        assert_eq!(transformer.avg_time, Duration::from_millis(1)); // 1ms per batch
    }
}
