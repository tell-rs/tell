//! Human-readable metrics formatter
//!
//! Formats metrics in a compact, readable format for operators.
//!
//! # Example Output
//!
//! ```text
//! [metrics] pipeline: 1.2M/s | 45.3 MB/s | batches: 2,400 | drop: 0 | bp: 0
//! [metrics] sources: tcp (5 conn, 1.2M/s) | syslog_tcp (12 conn, 50K/s)
//! [metrics] sinks: clickhouse (1.1M/s, 0 err) | disk_binary (100K/s, 0 err)
//! [metrics] transforms: pattern (2.4K batches, 1.2ms avg)
//! ```

use super::{format_bytes, format_bytes_per_sec, format_count, format_rate, MetricsFormatter};
use crate::{CollectedMetrics, MetricsRates, SinkMetricsSnapshot};
use std::fmt::Write;
use std::time::Duration;

/// Human-readable metrics formatter
#[derive(Debug, Clone, Default)]
pub struct HumanFormatter;

impl HumanFormatter {
    /// Create a new human formatter
    pub fn new() -> Self {
        Self
    }

    fn format_pipeline(&self, rates: &MetricsRates) -> Option<String> {
        let pipeline = rates.pipeline.as_ref()?;

        Some(format!(
            "[metrics] pipeline: {} | {} | batches: {:.0}/s",
            format_rate(pipeline.messages_per_sec),
            format_bytes_per_sec(pipeline.bytes_per_sec),
            pipeline.batches_per_sec,
        ))
    }

    fn format_sources(&self, rates: &MetricsRates) -> Option<String> {
        if rates.sources.is_empty() {
            return None;
        }

        let mut output = String::from("[metrics] sources:");

        for (i, source) in rates.sources.iter().enumerate() {
            if i > 0 {
                output.push_str(" |");
            }

            let _ = write!(
                output,
                " {} ({} conn, {}",
                source.id,
                source.connections_active,
                format_rate(source.messages_per_sec),
            );

            if source.errors > 0 {
                let _ = write!(output, ", {} err", source.errors);
            }

            output.push(')');
        }

        Some(output)
    }

    fn format_sinks(&self, rates: &MetricsRates) -> Option<String> {
        if rates.sinks.is_empty() {
            return None;
        }

        let mut output = String::from("[metrics] sinks:");

        for (i, sink) in rates.sinks.iter().enumerate() {
            if i > 0 {
                output.push_str(" |");
            }

            let _ = write!(output, " {} ({}", sink.id, format_rate(sink.messages_per_sec),);

            if sink.errors > 0 {
                let _ = write!(output, ", {} err", sink.errors);
            } else {
                output.push_str(", ok");
            }

            output.push(')');
        }

        Some(output)
    }

    fn format_transformers(&self, rates: &MetricsRates) -> Option<String> {
        if rates.transformers.is_empty() {
            return None;
        }

        let mut output = String::from("[metrics] transforms:");

        for (i, transformer) in rates.transformers.iter().enumerate() {
            if i > 0 {
                output.push_str(" |");
            }

            let avg_ms = transformer.avg_time.as_secs_f64() * 1000.0;

            let _ = write!(
                output,
                " {} ({:.0} batches/s, {:.2}ms",
                transformer.id, transformer.batches_per_sec, avg_ms,
            );

            if transformer.errors > 0 {
                let _ = write!(output, ", {} err", transformer.errors);
            }

            output.push(')');
        }

        Some(output)
    }
}

impl MetricsFormatter for HumanFormatter {
    fn format_unified(&self, _metrics: &CollectedMetrics, rates: Option<&MetricsRates>) -> String {
        let Some(rates) = rates else {
            return "[metrics] collecting baseline...".to_string();
        };

        let mut lines = Vec::with_capacity(4);

        if let Some(line) = self.format_pipeline(rates) {
            lines.push(line);
        }

        if let Some(line) = self.format_sources(rates) {
            lines.push(line);
        }

        if let Some(line) = self.format_sinks(rates) {
            lines.push(line);
        }

        if let Some(line) = self.format_transformers(rates) {
            lines.push(line);
        }

        if lines.is_empty() {
            "[metrics] no activity".to_string()
        } else {
            lines.join("\n")
        }
    }

    fn format_sink(
        &self,
        sink_id: &str,
        _sink_type: &str,
        snapshot: &SinkMetricsSnapshot,
        interval_secs: u64,
    ) -> String {
        format!(
            "[sink:{}] period: {}s | batches: {} | messages: {} | bytes: {} | errors: {}",
            sink_id,
            interval_secs,
            format_count(snapshot.batches_written),
            format_count(snapshot.messages_written),
            format_bytes(snapshot.bytes_written),
            snapshot.write_errors,
        )
    }
}

/// Format a duration for display
#[allow(dead_code)] // May be used for uptime display
pub fn format_duration(d: Duration) -> String {
    let secs = d.as_secs();
    if secs >= 3600 {
        format!("{}h{}m", secs / 3600, (secs % 3600) / 60)
    } else if secs >= 60 {
        format!("{}m{}s", secs / 60, secs % 60)
    } else {
        format!("{}s", secs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        CollectedMetrics, MetricsRates, PipelineRates, SinkMetricsSnapshot, SinkRates,
        SourceRates, TransformerRates,
    };

    fn make_test_rates() -> MetricsRates {
        MetricsRates {
            elapsed: Duration::from_secs(10),
            pipeline: Some(PipelineRates {
                messages_per_sec: 1_200_000.0,
                bytes_per_sec: 50_000_000.0,
                batches_per_sec: 2400.0,
            }),
            sources: vec![
                SourceRates {
                    id: "tcp".into(),
                    source_type: "tcp".into(),
                    messages_per_sec: 1_000_000.0,
                    bytes_per_sec: 40_000_000.0,
                    connections_active: 5,
                    errors: 0,
                },
                SourceRates {
                    id: "syslog_tcp".into(),
                    source_type: "syslog_tcp".into(),
                    messages_per_sec: 200_000.0,
                    bytes_per_sec: 10_000_000.0,
                    connections_active: 12,
                    errors: 2,
                },
            ],
            sinks: vec![
                SinkRates {
                    id: "clickhouse".into(),
                    sink_type: "clickhouse".into(),
                    messages_per_sec: 1_100_000.0,
                    bytes_per_sec: 45_000_000.0,
                    errors: 0,
                },
                SinkRates {
                    id: "disk_binary".into(),
                    sink_type: "disk_binary".into(),
                    messages_per_sec: 100_000.0,
                    bytes_per_sec: 5_000_000.0,
                    errors: 3,
                },
            ],
            transformers: vec![TransformerRates {
                id: "pattern_0".into(),
                transformer_type: "pattern".into(),
                batches_per_sec: 2400.0,
                avg_time: Duration::from_micros(1200),
                errors: 0,
            }],
        }
    }

    #[test]
    fn test_format_unified_with_rates() {
        let formatter = HumanFormatter::new();
        let metrics = CollectedMetrics::default();
        let rates = make_test_rates();

        let output = formatter.format_unified(&metrics, Some(&rates));

        assert!(output.contains("[metrics] pipeline:"));
        assert!(output.contains("1.2M/s"));
        assert!(output.contains("[metrics] sources:"));
        assert!(output.contains("tcp (5 conn"));
        assert!(output.contains("[metrics] sinks:"));
        assert!(output.contains("clickhouse"));
        assert!(output.contains("[metrics] transforms:"));
        assert!(output.contains("pattern_0"));
    }

    #[test]
    fn test_format_unified_no_rates() {
        let formatter = HumanFormatter::new();
        let metrics = CollectedMetrics::default();

        let output = formatter.format_unified(&metrics, None);
        assert!(output.contains("collecting baseline"));
    }

    #[test]
    fn test_format_sink() {
        let formatter = HumanFormatter::new();
        let snapshot = SinkMetricsSnapshot {
            batches_written: 240,
            messages_written: 120_000,
            bytes_written: 45_000_000,
            write_errors: 0,
            ..Default::default()
        };

        let output = formatter.format_sink("clickhouse", "clickhouse", &snapshot, 10);

        assert!(output.contains("[sink:clickhouse]"));
        assert!(output.contains("period: 10s"));
        assert!(output.contains("batches: 240"));
        assert!(output.contains("messages: 120.0K"));
        assert!(output.contains("bytes: 42.9 MB"));
        assert!(output.contains("errors: 0"));
    }

    #[test]
    fn test_format_sink_with_errors() {
        let formatter = HumanFormatter::new();
        let snapshot = SinkMetricsSnapshot {
            batches_written: 100,
            messages_written: 50_000,
            bytes_written: 10_000_000,
            write_errors: 5,
            ..Default::default()
        };

        let output = formatter.format_sink("disk", "disk_binary", &snapshot, 5);
        assert!(output.contains("errors: 5"));
    }

    #[test]
    fn test_format_duration() {
        assert_eq!(format_duration(Duration::from_secs(30)), "30s");
        assert_eq!(format_duration(Duration::from_secs(90)), "1m30s");
        assert_eq!(format_duration(Duration::from_secs(3661)), "1h1m");
    }
}
