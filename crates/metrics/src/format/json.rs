//! JSON metrics formatter
//!
//! Formats metrics as structured JSON for machine parsing.
//!
//! # Example Output
//!
//! ```json
//! {
//!   "type": "unified",
//!   "pipeline": {
//!     "messages_per_sec": 1200000,
//!     "bytes_per_sec": 45300000
//!   },
//!   "sources": [...],
//!   "sinks": [...],
//!   "transformers": [...]
//! }
//! ```

use super::MetricsFormatter;
use crate::{CollectedMetrics, MetricsRates, SinkMetricsSnapshot};
use serde::Serialize;

/// JSON metrics formatter
#[derive(Debug, Clone, Default)]
pub struct JsonFormatter;

impl JsonFormatter {
    /// Create a new JSON formatter
    pub fn new() -> Self {
        Self
    }
}

/// JSON structure for unified metrics output
#[derive(Serialize)]
struct UnifiedJson<'a> {
    #[serde(rename = "type")]
    report_type: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pipeline: Option<PipelineJson>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    sources: Vec<SourceJson<'a>>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    sinks: Vec<SinkJson<'a>>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    transformers: Vec<TransformerJson<'a>>,
}

#[derive(Serialize)]
struct PipelineJson {
    messages_per_sec: u64,
    bytes_per_sec: u64,
    batches_per_sec: u64,
}

#[derive(Serialize)]
struct SourceJson<'a> {
    id: &'a str,
    #[serde(rename = "type")]
    source_type: &'a str,
    connections_active: u64,
    messages_per_sec: u64,
    bytes_per_sec: u64,
    errors: u64,
}

#[derive(Serialize)]
struct SinkJson<'a> {
    id: &'a str,
    #[serde(rename = "type")]
    sink_type: &'a str,
    messages_per_sec: u64,
    bytes_per_sec: u64,
    errors: u64,
}

#[derive(Serialize)]
struct TransformerJson<'a> {
    id: &'a str,
    #[serde(rename = "type")]
    transformer_type: &'a str,
    batches_per_sec: u64,
    avg_time_us: u64,
    errors: u64,
}

/// JSON structure for per-sink metrics output
#[derive(Serialize)]
struct SinkReportJson<'a> {
    #[serde(rename = "type")]
    report_type: &'static str,
    sink_id: &'a str,
    sink_type: &'a str,
    period_secs: u64,
    batches_written: u64,
    messages_written: u64,
    bytes_written: u64,
    write_errors: u64,
}

impl MetricsFormatter for JsonFormatter {
    fn format_unified(&self, _metrics: &CollectedMetrics, rates: Option<&MetricsRates>) -> String {
        let Some(rates) = rates else {
            return r#"{"type":"unified","status":"collecting_baseline"}"#.to_string();
        };

        let json = UnifiedJson {
            report_type: "unified",
            pipeline: rates.pipeline.as_ref().map(|p| PipelineJson {
                messages_per_sec: p.messages_per_sec as u64,
                bytes_per_sec: p.bytes_per_sec as u64,
                batches_per_sec: p.batches_per_sec as u64,
            }),
            sources: rates
                .sources
                .iter()
                .map(|s| SourceJson {
                    id: &s.id,
                    source_type: &s.source_type,
                    connections_active: s.connections_active,
                    messages_per_sec: s.messages_per_sec as u64,
                    bytes_per_sec: s.bytes_per_sec as u64,
                    errors: s.errors,
                })
                .collect(),
            sinks: rates
                .sinks
                .iter()
                .map(|s| SinkJson {
                    id: &s.id,
                    sink_type: &s.sink_type,
                    messages_per_sec: s.messages_per_sec as u64,
                    bytes_per_sec: s.bytes_per_sec as u64,
                    errors: s.errors,
                })
                .collect(),
            transformers: rates
                .transformers
                .iter()
                .map(|t| TransformerJson {
                    id: &t.id,
                    transformer_type: &t.transformer_type,
                    batches_per_sec: t.batches_per_sec as u64,
                    avg_time_us: t.avg_time.as_micros() as u64,
                    errors: t.errors,
                })
                .collect(),
        };

        // Use compact JSON (no pretty printing for log lines)
        serde_json::to_string(&json).unwrap_or_else(|_| "{}".to_string())
    }

    fn format_sink(
        &self,
        sink_id: &str,
        sink_type: &str,
        snapshot: &SinkMetricsSnapshot,
        interval_secs: u64,
    ) -> String {
        let json = SinkReportJson {
            report_type: "sink",
            sink_id,
            sink_type,
            period_secs: interval_secs,
            batches_written: snapshot.batches_written,
            messages_written: snapshot.messages_written,
            bytes_written: snapshot.bytes_written,
            write_errors: snapshot.write_errors,
        };

        serde_json::to_string(&json).unwrap_or_else(|_| "{}".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MetricsRates, PipelineRates, SinkRates, SourceRates, TransformerRates};
    use std::time::Duration;

    fn make_test_rates() -> MetricsRates {
        MetricsRates {
            elapsed: Duration::from_secs(10),
            pipeline: Some(PipelineRates {
                messages_per_sec: 1_200_000.0,
                bytes_per_sec: 50_000_000.0,
                batches_per_sec: 2400.0,
            }),
            sources: vec![SourceRates {
                id: "tcp".into(),
                source_type: "tcp".into(),
                messages_per_sec: 1_000_000.0,
                bytes_per_sec: 40_000_000.0,
                connections_active: 5,
                errors: 0,
            }],
            sinks: vec![SinkRates {
                id: "clickhouse".into(),
                sink_type: "clickhouse".into(),
                messages_per_sec: 1_100_000.0,
                bytes_per_sec: 45_000_000.0,
                errors: 0,
            }],
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
        let formatter = JsonFormatter::new();
        let metrics = CollectedMetrics::default();
        let rates = make_test_rates();

        let output = formatter.format_unified(&metrics, Some(&rates));

        // Parse and verify structure
        let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();

        assert_eq!(parsed["type"], "unified");
        assert_eq!(parsed["pipeline"]["messages_per_sec"], 1_200_000);
        assert_eq!(parsed["sources"][0]["id"], "tcp");
        assert_eq!(parsed["sinks"][0]["id"], "clickhouse");
        assert_eq!(parsed["transformers"][0]["id"], "pattern_0");
    }

    #[test]
    fn test_format_unified_no_rates() {
        let formatter = JsonFormatter::new();
        let metrics = CollectedMetrics::default();

        let output = formatter.format_unified(&metrics, None);
        let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();

        assert_eq!(parsed["status"], "collecting_baseline");
    }

    #[test]
    fn test_format_sink() {
        let formatter = JsonFormatter::new();
        let snapshot = SinkMetricsSnapshot {
            batches_written: 240,
            messages_written: 120_000,
            bytes_written: 45_000_000,
            write_errors: 2,
            ..Default::default()
        };

        let output = formatter.format_sink("clickhouse", "clickhouse", &snapshot, 10);
        let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();

        assert_eq!(parsed["type"], "sink");
        assert_eq!(parsed["sink_id"], "clickhouse");
        assert_eq!(parsed["period_secs"], 10);
        assert_eq!(parsed["batches_written"], 240);
        assert_eq!(parsed["messages_written"], 120_000);
        assert_eq!(parsed["write_errors"], 2);
    }

    #[test]
    fn test_empty_collections_omitted() {
        let formatter = JsonFormatter::new();
        let metrics = CollectedMetrics::default();
        let rates = MetricsRates {
            elapsed: Duration::from_secs(10),
            pipeline: Some(PipelineRates {
                messages_per_sec: 1000.0,
                bytes_per_sec: 10000.0,
                batches_per_sec: 100.0,
            }),
            sources: vec![],
            sinks: vec![],
            transformers: vec![],
        };

        let output = formatter.format_unified(&metrics, Some(&rates));
        let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();

        // Empty arrays should be omitted
        assert!(parsed.get("sources").is_none());
        assert!(parsed.get("sinks").is_none());
        assert!(parsed.get("transformers").is_none());
    }
}
