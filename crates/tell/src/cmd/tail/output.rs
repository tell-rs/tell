//! Output formatting for tap messages
//!
//! Decodes FlatBuffer payloads using tell_protocol and formats as JSON.

use owo_colors::{OwoColorize, Style};
use serde::Serialize;
use tell_protocol::{
    DecodedEvent, DecodedLogEntry, DecodedMetric, DecodedSnapshot, DecodedSpan, FlatBatch,
    LogLevel, MetricType, SpanStatus, decode_event_data, decode_log_data, decode_metric_data,
    decode_snapshot_data, decode_trace_data,
};
use tell_tap::TapEnvelope;

/// Batch type constants (aligned with SchemaType wire values)
/// These must match BatchType::to_u8() and SchemaType values
const BATCH_TYPE_EVENT: u8 = 1;
const BATCH_TYPE_LOG: u8 = 2; // Also used for Syslog on wire
const BATCH_TYPE_METRIC: u8 = 3;
const BATCH_TYPE_TRACE: u8 = 4;
const BATCH_TYPE_SNAPSHOT: u8 = 5;

/// Output format
#[derive(Debug, Clone, Copy)]
pub enum Format {
    /// Human-readable text (default for TTY)
    Text,
    /// Full JSON with decoded payload
    Json,
    /// Compact single-line JSON (default for pipes)
    Compact,
    /// Raw metadata only
    Raw,
}

impl Format {
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "text" | "t" => Format::Text,
            "json" | "j" => Format::Json,
            "compact" | "c" => Format::Compact,
            "raw" | "r" => Format::Raw,
            _ => Format::Text, // Text is default for human readability
        }
    }
}

/// Output formatter
pub struct Formatter {
    format: Format,
    metadata_only: bool,
    use_color: bool,
}

/// Color styles for terminal output
struct ColorStyles {
    timestamp: Style,
    label: Style,
    event_type: Style,
    device_id: Style,
    payload: Style,
    error: Style,
    ok: Style,
}

impl ColorStyles {
    fn new(enabled: bool) -> Self {
        if enabled {
            Self {
                timestamp: Style::new().dimmed(),
                label: Style::new().dimmed(),
                event_type: Style::new(),
                device_id: Style::new().dimmed(),
                payload: Style::new().dimmed(),
                error: Style::new().red(),
                ok: Style::new().green(),
            }
        } else {
            Self {
                timestamp: Style::new(),
                label: Style::new(),
                event_type: Style::new(),
                device_id: Style::new(),
                payload: Style::new(),
                error: Style::new(),
                ok: Style::new(),
            }
        }
    }
}

/// Get style for log level - only errors/warnings stand out
fn log_level_style(level: LogLevel, enabled: bool) -> Style {
    if !enabled {
        return Style::new();
    }
    match level {
        LogLevel::Fatal | LogLevel::Error => Style::new().red(),
        LogLevel::Warning => Style::new().yellow(),
        LogLevel::Info | LogLevel::Debug => Style::new(),
        LogLevel::Trace => Style::new().dimmed(),
    }
}

impl Formatter {
    /// Create a new formatter
    pub fn new(format: &str, metadata_only: bool) -> Self {
        Self {
            format: Format::from_str(format),
            metadata_only,
            use_color: true, // Default on, caller sets based on TTY
        }
    }

    /// Enable or disable color output
    pub fn with_color(mut self, use_color: bool) -> Self {
        self.use_color = use_color;
        self
    }

    /// Print a tap envelope to stdout
    pub fn print(&self, envelope: &TapEnvelope) {
        match self.format {
            Format::Text => self.print_text(envelope),
            Format::Json => self.print_json(envelope),
            Format::Compact => self.print_compact(envelope),
            Format::Raw => self.print_raw(envelope),
        }
    }

    fn print_json(&self, envelope: &TapEnvelope) {
        let output = if self.metadata_only {
            BatchOutput::metadata_only(envelope)
        } else {
            BatchOutput::full(envelope)
        };

        match serde_json::to_string_pretty(&output) {
            Ok(json) => println!("{json}"),
            Err(e) => tracing::error!(error = %e, "failed to serialize batch"),
        }
    }

    fn print_compact(&self, envelope: &TapEnvelope) {
        let output = if self.metadata_only {
            BatchOutput::metadata_only(envelope)
        } else {
            BatchOutput::full(envelope)
        };

        match serde_json::to_string(&output) {
            Ok(json) => println!("{json}"),
            Err(e) => tracing::error!(error = %e, "failed to serialize batch"),
        }
    }

    fn print_raw(&self, envelope: &TapEnvelope) {
        println!(
            "workspace={} source={} type={} count={} bytes={}",
            envelope.workspace_id,
            envelope.source_id,
            batch_type_name(envelope.batch_type),
            envelope.count,
            envelope.payload.len()
        );
    }

    fn print_text(&self, envelope: &TapEnvelope) {
        if self.metadata_only {
            println!(
                "[ws:{}] {} src={} count={} bytes={}",
                envelope.workspace_id,
                batch_type_name(envelope.batch_type).to_uppercase(),
                envelope.source_id,
                envelope.count,
                envelope.payload.len()
            );
            return;
        }

        match envelope.batch_type {
            BATCH_TYPE_EVENT => self.print_text_events(envelope),
            BATCH_TYPE_LOG => self.print_text_logs(envelope),
            BATCH_TYPE_SNAPSHOT => self.print_text_snapshots(envelope),
            BATCH_TYPE_METRIC => self.print_text_metrics(envelope),
            BATCH_TYPE_TRACE => self.print_text_traces(envelope),
            _ => {
                println!(
                    "[ws:{}] {} (unsupported type {})",
                    envelope.workspace_id,
                    batch_type_name(envelope.batch_type).to_uppercase(),
                    envelope.batch_type
                );
            }
        }
    }

    fn print_text_events(&self, envelope: &TapEnvelope) {
        let styles = ColorStyles::new(self.use_color);

        // Batch metadata
        let ws = format!("ws:{}", envelope.workspace_id);
        let src = &envelope.source_id;
        let ip = envelope.source_ip;

        // Iterate through each message in the envelope
        for i in 0..envelope.offsets.len() {
            let msg = get_message(envelope, i);
            let Some(msg) = msg else { continue };

            // Parse outer FlatBatch wrapper
            let flat_batch = match FlatBatch::parse(msg) {
                Ok(fb) => fb,
                Err(e) => {
                    println!(
                        "{} {} {} {} parse error: {}",
                        ws.style(styles.label),
                        src.style(styles.label),
                        ip.style(styles.label),
                        "event".style(styles.error),
                        e.style(styles.error)
                    );
                    continue;
                }
            };

            // Extract inner data and decode as EventData
            let data = match flat_batch.data() {
                Ok(d) => d,
                Err(e) => {
                    println!(
                        "{} {} {} {} data error: {}",
                        ws.style(styles.label),
                        src.style(styles.label),
                        ip.style(styles.label),
                        "event".style(styles.error),
                        e.style(styles.error)
                    );
                    continue;
                }
            };

            let events = match decode_event_data(data) {
                Ok(e) => e,
                Err(e) => {
                    println!(
                        "{} {} {} {} decode error: {}",
                        ws.style(styles.label),
                        src.style(styles.label),
                        ip.style(styles.label),
                        "event".style(styles.error),
                        e.style(styles.error)
                    );
                    continue;
                }
            };

            for event in events {
                let ts = format_timestamp(event.timestamp);
                let name = event.event_name.unwrap_or("-");
                let device = event
                    .device_id
                    .map(|d| format!("dev={}", format_uuid_short(d)))
                    .unwrap_or_default();
                let payload = format_payload_inline(event.payload);

                println!(
                    "{} {} {} {} {} {} {} {} {}",
                    ts.style(styles.timestamp),
                    ws.style(styles.label),
                    src.style(styles.label),
                    ip.style(styles.label),
                    "event".style(styles.label),
                    event.event_type.as_str().style(styles.event_type),
                    name,
                    device.style(styles.device_id),
                    payload.style(styles.payload)
                );
            }
        }
    }

    fn print_text_logs(&self, envelope: &TapEnvelope) {
        let styles = ColorStyles::new(self.use_color);

        // Batch metadata
        let ws = format!("ws:{}", envelope.workspace_id);
        let src = &envelope.source_id;
        let ip = envelope.source_ip;

        // Iterate through each message in the envelope
        for i in 0..envelope.offsets.len() {
            let msg = get_message(envelope, i);
            let Some(msg) = msg else { continue };

            // Parse outer FlatBatch wrapper
            let flat_batch = match FlatBatch::parse(msg) {
                Ok(fb) => fb,
                Err(e) => {
                    println!(
                        "{} {} {} {} parse error: {}",
                        ws.style(styles.label),
                        src.style(styles.label),
                        ip.style(styles.label),
                        "log".style(styles.error),
                        e.style(styles.error)
                    );
                    continue;
                }
            };

            // Extract inner data and decode as LogData
            let data = match flat_batch.data() {
                Ok(d) => d,
                Err(e) => {
                    println!(
                        "{} {} {} {} data error: {}",
                        ws.style(styles.label),
                        src.style(styles.label),
                        ip.style(styles.label),
                        "log".style(styles.error),
                        e.style(styles.error)
                    );
                    continue;
                }
            };

            let logs = match decode_log_data(data) {
                Ok(l) => l,
                Err(e) => {
                    println!(
                        "{} {} {} {} decode error: {}",
                        ws.style(styles.label),
                        src.style(styles.label),
                        ip.style(styles.label),
                        "log".style(styles.error),
                        e.style(styles.error)
                    );
                    continue;
                }
            };

            for log in logs {
                let ts = format_timestamp(log.timestamp);
                let level_str = format!("{:7}", log.level.as_str());
                let level_style = log_level_style(log.level, self.use_color);
                let svc = log.service.unwrap_or("-");
                let src_host = log.source.unwrap_or("");
                let location = if src_host.is_empty() {
                    svc.to_string()
                } else {
                    format!("{}@{}", svc, src_host)
                };
                let payload = format_payload_inline(log.payload);

                println!(
                    "{} {} {} {} {} {} {} {}",
                    ts.style(styles.timestamp),
                    ws.style(styles.label),
                    src.style(styles.label),
                    ip.style(styles.label),
                    "log".style(styles.label),
                    level_str.style(level_style),
                    location,
                    payload.style(styles.payload)
                );
            }
        }
    }

    fn print_text_snapshots(&self, envelope: &TapEnvelope) {
        let styles = ColorStyles::new(self.use_color);

        let ws = format!("ws:{}", envelope.workspace_id);
        let src = &envelope.source_id;
        let ip = envelope.source_ip;

        for i in 0..envelope.offsets.len() {
            let msg = get_message(envelope, i);
            let Some(msg) = msg else { continue };

            let flat_batch = match FlatBatch::parse(msg) {
                Ok(fb) => fb,
                Err(e) => {
                    println!(
                        "{} {} {} {} parse error: {}",
                        ws.style(styles.label),
                        src.style(styles.label),
                        ip.style(styles.label),
                        "snapshot".style(styles.error),
                        e.style(styles.error)
                    );
                    continue;
                }
            };

            let data = match flat_batch.data() {
                Ok(d) => d,
                Err(_) => continue,
            };

            let snapshots = match decode_snapshot_data(data) {
                Ok(s) => s,
                Err(_) => continue,
            };

            for snapshot in snapshots {
                let ts = format_timestamp(snapshot.timestamp);
                let source = snapshot.source.unwrap_or("-");
                let entity = snapshot.entity.unwrap_or("-");
                let payload = format_payload_inline(snapshot.payload);

                println!(
                    "{} {} {} {} {} {} {} {}",
                    ts.style(styles.timestamp),
                    ws.style(styles.label),
                    src.style(styles.label),
                    ip.style(styles.label),
                    "snapshot".style(styles.label),
                    source,
                    entity.style(styles.label),
                    payload.style(styles.payload)
                );
            }
        }
    }

    fn print_text_metrics(&self, envelope: &TapEnvelope) {
        let styles = ColorStyles::new(self.use_color);

        let ws = format!("ws:{}", envelope.workspace_id);
        let src = &envelope.source_id;
        let ip = envelope.source_ip;

        for i in 0..envelope.offsets.len() {
            let msg = get_message(envelope, i);
            let Some(msg) = msg else { continue };

            let flat_batch = match FlatBatch::parse(msg) {
                Ok(fb) => fb,
                Err(e) => {
                    println!(
                        "{} {} {} {} parse error: {}",
                        ws.style(styles.label),
                        src.style(styles.label),
                        ip.style(styles.label),
                        "metric".style(styles.error),
                        e.style(styles.error)
                    );
                    continue;
                }
            };

            let data = match flat_batch.data() {
                Ok(d) => d,
                Err(_) => continue,
            };

            let metrics = match decode_metric_data(data) {
                Ok(m) => m,
                Err(_) => continue,
            };

            for metric in metrics {
                let ts = format_timestamp_ns(metric.timestamp);
                let metric_type = metric.metric_type.as_str();
                let name = metric.name;
                let service = metric.service.unwrap_or("-");

                // Format value or histogram
                let value_str = match metric.metric_type {
                    MetricType::Histogram => {
                        if let Some(h) = &metric.histogram {
                            format!(
                                "count={} sum={:.3} min={:.3} max={:.3}",
                                h.count, h.sum, h.min, h.max
                            )
                        } else {
                            String::from("-")
                        }
                    }
                    _ => format!("{:.3}", metric.value),
                };

                // Format labels
                let labels = if metric.labels.is_empty() && metric.int_labels.is_empty() {
                    String::new()
                } else {
                    let mut parts: Vec<String> = metric
                        .labels
                        .iter()
                        .map(|l| format!("{}={}", l.key, l.value))
                        .collect();
                    parts.extend(
                        metric
                            .int_labels
                            .iter()
                            .map(|l| format!("{}={}", l.key, l.value)),
                    );
                    format!("{{{}}}", parts.join(","))
                };

                println!(
                    "{} {} {} {} {} {} {}@{} {} {}",
                    ts.style(styles.timestamp),
                    ws.style(styles.label),
                    src.style(styles.label),
                    ip.style(styles.label),
                    "metric".style(styles.label),
                    metric_type,
                    name,
                    service.style(styles.label),
                    value_str,
                    labels.style(styles.payload)
                );
            }
        }
    }

    fn print_text_traces(&self, envelope: &TapEnvelope) {
        let styles = ColorStyles::new(self.use_color);

        let ws = format!("ws:{}", envelope.workspace_id);
        let src = &envelope.source_id;
        let ip = envelope.source_ip;

        for i in 0..envelope.offsets.len() {
            let msg = get_message(envelope, i);
            let Some(msg) = msg else { continue };

            let flat_batch = match FlatBatch::parse(msg) {
                Ok(fb) => fb,
                Err(e) => {
                    println!(
                        "{} {} {} {} parse error: {}",
                        ws.style(styles.label),
                        src.style(styles.label),
                        ip.style(styles.label),
                        "trace".style(styles.error),
                        e.style(styles.error)
                    );
                    continue;
                }
            };

            let data = match flat_batch.data() {
                Ok(d) => d,
                Err(_) => continue,
            };

            let spans = match decode_trace_data(data) {
                Ok(s) => s,
                Err(_) => continue,
            };

            for span in spans {
                let ts = format_timestamp_ns(span.timestamp);
                let duration_ms = span.duration as f64 / 1_000_000.0;
                let name = span.name.unwrap_or("-");
                let kind = span.kind.as_str();
                let status = match span.status {
                    SpanStatus::Ok => "OK".style(styles.ok),
                    SpanStatus::Error => "ERR".style(styles.error),
                    SpanStatus::Unset => "-".style(styles.label),
                };
                let service = span.service.unwrap_or("-");

                let trace_id = span
                    .trace_id
                    .map(|t| format!("trace={}", format_uuid_short(t)))
                    .unwrap_or_default();

                let parent = if span.is_root() {
                    "root".to_string()
                } else {
                    span.parent_span_id
                        .map(|p| format!("parent={:02x}{:02x}{:02x}{:02x}", p[0], p[1], p[2], p[3]))
                        .unwrap_or_default()
                };

                println!(
                    "{} {} {} {} {} {} {} {}@{} {:.2}ms {} {}",
                    ts.style(styles.timestamp),
                    ws.style(styles.label),
                    src.style(styles.label),
                    ip.style(styles.label),
                    "trace".style(styles.label),
                    kind,
                    status,
                    name,
                    service.style(styles.label),
                    duration_ms,
                    trace_id.style(styles.label),
                    parent.style(styles.label)
                );
            }
        }
    }
}

/// Extract a message from the envelope by index
fn get_message(envelope: &TapEnvelope, index: usize) -> Option<&[u8]> {
    if index >= envelope.offsets.len() {
        return None;
    }
    let start = envelope.offsets[index] as usize;
    let len = envelope.lengths[index] as usize;
    if start + len > envelope.payload.len() {
        return None;
    }
    Some(&envelope.payload[start..start + len])
}

/// Serializable batch output
#[derive(Serialize)]
struct BatchOutput {
    workspace_id: u32,
    source_id: String,
    batch_type: String,
    source_ip: String,
    count: u32,
    payload_bytes: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    events: Option<Vec<EventOutput>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    logs: Option<Vec<LogOutput>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    snapshots: Option<Vec<SnapshotOutput>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    metrics: Option<Vec<MetricOutput>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    traces: Option<Vec<TraceOutput>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

impl BatchOutput {
    fn metadata_only(envelope: &TapEnvelope) -> Self {
        Self {
            workspace_id: envelope.workspace_id,
            source_id: envelope.source_id.clone(),
            batch_type: batch_type_name(envelope.batch_type).to_string(),
            source_ip: envelope.source_ip.to_string(),
            count: envelope.count,
            payload_bytes: envelope.payload.len(),
            events: None,
            logs: None,
            snapshots: None,
            metrics: None,
            traces: None,
            error: None,
        }
    }

    fn full(envelope: &TapEnvelope) -> Self {
        let mut output = Self::metadata_only(envelope);

        match envelope.batch_type {
            BATCH_TYPE_EVENT => match decode_events(envelope) {
                Ok(events) => output.events = Some(events),
                Err(e) => output.error = Some(format!("decode error: {e}")),
            },
            BATCH_TYPE_LOG => match decode_logs(envelope) {
                Ok(logs) => output.logs = Some(logs),
                Err(e) => output.error = Some(format!("decode error: {e}")),
            },
            BATCH_TYPE_SNAPSHOT => match decode_snapshots(envelope) {
                Ok(snapshots) => output.snapshots = Some(snapshots),
                Err(e) => output.error = Some(format!("decode error: {e}")),
            },
            BATCH_TYPE_METRIC => match decode_metrics(envelope) {
                Ok(metrics) => output.metrics = Some(metrics),
                Err(e) => output.error = Some(format!("decode error: {e}")),
            },
            BATCH_TYPE_TRACE => match decode_traces(envelope) {
                Ok(traces) => output.traces = Some(traces),
                Err(e) => output.error = Some(format!("decode error: {e}")),
            },
            _ => {
                output.error = Some(format!(
                    "unsupported batch type: {} ({})",
                    envelope.batch_type,
                    batch_type_name(envelope.batch_type)
                ));
            }
        }

        output
    }
}

/// Decoded event output
#[derive(Serialize)]
struct EventOutput {
    event_type: String,
    timestamp: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    device_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    event_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    payload: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    payload_raw: Option<String>,
}

impl EventOutput {
    fn from_decoded(event: &DecodedEvent<'_>) -> Self {
        let (payload, payload_raw) = parse_payload(event.payload);

        Self {
            event_type: event.event_type.as_str().to_string(),
            timestamp: event.timestamp,
            device_id: event.device_id.map(format_uuid),
            session_id: event.session_id.map(format_uuid),
            event_name: event.event_name.map(|s| s.to_string()),
            payload,
            payload_raw,
        }
    }
}

/// Decoded log output
#[derive(Serialize)]
struct LogOutput {
    event_type: String,
    level: String,
    timestamp: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    service: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    payload: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    payload_raw: Option<String>,
}

impl LogOutput {
    fn from_decoded(log: &DecodedLogEntry<'_>) -> Self {
        let (payload, payload_raw) = parse_payload(log.payload);

        Self {
            event_type: log.event_type.as_str().to_string(),
            level: log.level.as_str().to_string(),
            timestamp: log.timestamp,
            session_id: log.session_id.map(format_uuid),
            source: log.source.map(|s| s.to_string()),
            service: log.service.map(|s| s.to_string()),
            payload,
            payload_raw,
        }
    }
}

/// Decoded snapshot output
#[derive(Serialize)]
struct SnapshotOutput {
    timestamp: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    entity: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    payload: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    payload_raw: Option<String>,
}

impl SnapshotOutput {
    fn from_decoded(snapshot: &DecodedSnapshot<'_>) -> Self {
        let (payload, payload_raw) = parse_payload(snapshot.payload);

        Self {
            timestamp: snapshot.timestamp,
            source: snapshot.source.map(|s| s.to_string()),
            entity: snapshot.entity.map(|s| s.to_string()),
            payload,
            payload_raw,
        }
    }
}

/// Decoded metric output
#[derive(Serialize)]
struct MetricOutput {
    metric_type: String,
    timestamp: u64,
    name: String,
    value: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    service: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    session_id: Option<String>,
    temporality: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    labels: Vec<LabelOutput>,
    #[serde(skip_serializing_if = "Option::is_none")]
    histogram: Option<HistogramOutput>,
}

#[derive(Serialize)]
struct LabelOutput {
    key: String,
    value: String,
}

#[derive(Serialize)]
struct HistogramOutput {
    count: u64,
    sum: f64,
    min: f64,
    max: f64,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    buckets: Vec<BucketOutput>,
}

#[derive(Serialize)]
struct BucketOutput {
    upper_bound: f64,
    count: u64,
}

impl MetricOutput {
    fn from_decoded(metric: &DecodedMetric<'_>) -> Self {
        let mut labels: Vec<LabelOutput> = metric
            .labels
            .iter()
            .map(|l| LabelOutput {
                key: l.key.to_string(),
                value: l.value.to_string(),
            })
            .collect();

        // Add int labels as string values
        labels.extend(metric.int_labels.iter().map(|l| LabelOutput {
            key: l.key.to_string(),
            value: l.value.to_string(),
        }));

        let histogram = metric.histogram.as_ref().map(|h| HistogramOutput {
            count: h.count,
            sum: h.sum,
            min: h.min,
            max: h.max,
            buckets: h
                .buckets
                .iter()
                .map(|b| BucketOutput {
                    upper_bound: b.upper_bound,
                    count: b.count,
                })
                .collect(),
        });

        Self {
            metric_type: metric.metric_type.as_str().to_string(),
            timestamp: metric.timestamp,
            name: metric.name.to_string(),
            value: metric.value,
            source: metric.source.map(|s| s.to_string()),
            service: metric.service.map(|s| s.to_string()),
            session_id: metric.session_id.map(format_uuid),
            temporality: metric.temporality.as_str().to_string(),
            labels,
            histogram,
        }
    }
}

/// Decoded trace span output
#[derive(Serialize)]
struct TraceOutput {
    #[serde(skip_serializing_if = "Option::is_none")]
    trace_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    span_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    parent_span_id: Option<String>,
    timestamp: u64,
    duration: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    kind: String,
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    service: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    payload: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    payload_raw: Option<String>,
}

impl TraceOutput {
    fn from_decoded(span: &DecodedSpan<'_>) -> Self {
        let (payload, payload_raw) = parse_payload(span.payload);

        Self {
            trace_id: span.trace_id.map(format_uuid),
            span_id: span.span_id.map(|id| format_hex_bytes(id)),
            parent_span_id: span.parent_span_id.map(|id| format_hex_bytes(id)),
            timestamp: span.timestamp,
            duration: span.duration,
            name: span.name.map(|s| s.to_string()),
            kind: span.kind.as_str().to_string(),
            status: span.status.as_str().to_string(),
            source: span.source.map(|s| s.to_string()),
            service: span.service.map(|s| s.to_string()),
            session_id: span.session_id.map(format_uuid),
            payload,
            payload_raw,
        }
    }
}

/// Decode events from envelope payload
fn decode_events(envelope: &TapEnvelope) -> Result<Vec<EventOutput>, String> {
    let mut all_events = Vec::new();

    for i in 0..envelope.offsets.len() {
        let Some(msg) = get_message(envelope, i) else {
            continue;
        };

        let flat_batch = FlatBatch::parse(msg).map_err(|e| e.to_string())?;
        let data = flat_batch.data().map_err(|e| e.to_string())?;
        let events = decode_event_data(data).map_err(|e| e.to_string())?;

        all_events.extend(events.iter().map(EventOutput::from_decoded));
    }

    Ok(all_events)
}

/// Decode logs from envelope payload
fn decode_logs(envelope: &TapEnvelope) -> Result<Vec<LogOutput>, String> {
    let mut all_logs = Vec::new();

    for i in 0..envelope.offsets.len() {
        let Some(msg) = get_message(envelope, i) else {
            continue;
        };

        let flat_batch = FlatBatch::parse(msg).map_err(|e| e.to_string())?;
        let data = flat_batch.data().map_err(|e| e.to_string())?;
        let logs = decode_log_data(data).map_err(|e| e.to_string())?;

        all_logs.extend(logs.iter().map(LogOutput::from_decoded));
    }

    Ok(all_logs)
}

/// Decode snapshots from envelope payload
fn decode_snapshots(envelope: &TapEnvelope) -> Result<Vec<SnapshotOutput>, String> {
    let mut all_snapshots = Vec::new();

    for i in 0..envelope.offsets.len() {
        let Some(msg) = get_message(envelope, i) else {
            continue;
        };

        let flat_batch = FlatBatch::parse(msg).map_err(|e| e.to_string())?;
        let data = flat_batch.data().map_err(|e| e.to_string())?;
        let snapshots = decode_snapshot_data(data).map_err(|e| e.to_string())?;

        all_snapshots.extend(snapshots.iter().map(SnapshotOutput::from_decoded));
    }

    Ok(all_snapshots)
}

/// Decode metrics from envelope payload
fn decode_metrics(envelope: &TapEnvelope) -> Result<Vec<MetricOutput>, String> {
    let mut all_metrics = Vec::new();

    for i in 0..envelope.offsets.len() {
        let Some(msg) = get_message(envelope, i) else {
            continue;
        };

        let flat_batch = FlatBatch::parse(msg).map_err(|e| e.to_string())?;
        let data = flat_batch.data().map_err(|e| e.to_string())?;
        let metrics = decode_metric_data(data).map_err(|e| e.to_string())?;

        all_metrics.extend(metrics.iter().map(MetricOutput::from_decoded));
    }

    Ok(all_metrics)
}

/// Decode traces from envelope payload
fn decode_traces(envelope: &TapEnvelope) -> Result<Vec<TraceOutput>, String> {
    let mut all_traces = Vec::new();

    for i in 0..envelope.offsets.len() {
        let Some(msg) = get_message(envelope, i) else {
            continue;
        };

        let flat_batch = FlatBatch::parse(msg).map_err(|e| e.to_string())?;
        let data = flat_batch.data().map_err(|e| e.to_string())?;
        let spans = decode_trace_data(data).map_err(|e| e.to_string())?;

        all_traces.extend(spans.iter().map(TraceOutput::from_decoded));
    }

    Ok(all_traces)
}

/// Parse payload bytes as JSON, falling back to raw string
fn parse_payload(bytes: &[u8]) -> (Option<serde_json::Value>, Option<String>) {
    if bytes.is_empty() {
        return (None, None);
    }

    // Try parsing as JSON first
    if let Ok(json) = serde_json::from_slice::<serde_json::Value>(bytes) {
        return (Some(json), None);
    }

    // Fall back to UTF-8 string
    if let Ok(s) = std::str::from_utf8(bytes) {
        return (None, Some(s.to_string()));
    }

    // Last resort: hex dump
    (None, Some(hex_preview(bytes)))
}

/// Format UUID bytes as hyphenated string
fn format_uuid(bytes: &[u8; 16]) -> String {
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15]
    )
}

/// Create a hex preview string
fn hex_preview(bytes: &[u8]) -> String {
    let preview_bytes = &bytes[..bytes.len().min(64)];
    let hex: Vec<String> = preview_bytes.iter().map(|b| format!("{b:02x}")).collect();
    let truncated = if bytes.len() > 64 { "..." } else { "" };
    format!("{}{truncated}", hex.join(" "))
}

/// Get batch type name (aligned with SchemaType values)
fn batch_type_name(t: u8) -> &'static str {
    match t {
        0 => "unknown",
        1 => "event",
        2 => "log",
        3 => "metric",
        4 => "trace",
        5 => "snapshot",
        _ => "unknown",
    }
}

/// Format timestamp as HH:MM:SS.mmm (from milliseconds)
fn format_timestamp(ts_millis: u64) -> String {
    use std::time::{Duration, UNIX_EPOCH};

    let d = UNIX_EPOCH + Duration::from_millis(ts_millis);
    let datetime: chrono::DateTime<chrono::Utc> = d.into();
    datetime.format("%H:%M:%S%.3f").to_string()
}

/// Format timestamp as HH:MM:SS.mmm (from nanoseconds)
fn format_timestamp_ns(ts_nanos: u64) -> String {
    use chrono::{TimeZone, Utc};
    let secs = (ts_nanos / 1_000_000_000) as i64;
    let nanos = (ts_nanos % 1_000_000_000) as u32;
    Utc.timestamp_opt(secs, nanos)
        .single()
        .map(|dt| dt.format("%H:%M:%S%.3f").to_string())
        .unwrap_or_else(|| format!("{}", ts_nanos))
}

/// Format bytes as hex string (for span IDs)
fn format_hex_bytes(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Format UUID as short form (first 8 chars)
fn format_uuid_short(bytes: &[u8; 16]) -> String {
    format!(
        "{:02x}{:02x}{:02x}{:02x}",
        bytes[0], bytes[1], bytes[2], bytes[3]
    )
}

/// Format payload as inline compact JSON or truncated string
fn format_payload_inline(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        return String::new();
    }

    // Try JSON first - compact it
    if let Ok(json) = serde_json::from_slice::<serde_json::Value>(bytes) {
        let compact = serde_json::to_string(&json).unwrap_or_default();
        if compact.len() > 80 {
            format!("{}...", &compact[..77])
        } else {
            compact
        }
    } else if let Ok(s) = std::str::from_utf8(bytes) {
        // UTF-8 string
        if s.len() > 80 {
            format!("{}...", &s[..77])
        } else {
            s.to_string()
        }
    } else {
        // Binary - show length
        format!("<{} bytes>", bytes.len())
    }
}
