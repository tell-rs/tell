//! Stdout Sink - Human-readable debug output
//!
//! Outputs decoded log/event messages to stdout in a format matching `tell tail`.
//! Not intended for production use at high throughput.
//!
//! # Example Output
//!
//! ```text
//! 07:34:59.161 ws:1 tcp_debug 127.0.0.1 log info    my-app@Kong.local {"message":"started"}
//! 07:34:59.162 ws:1 tcp_debug 127.0.0.1 log error   my-app@Kong.local {"message":"failed"}
//! 07:35:00.100 ws:1 tcp_debug 127.0.0.1 event track page_view {"url":"/home"}
//! ```

use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tell_protocol::{
    decode_event_data, decode_log_data, decode_metric_data, decode_snapshot_data, decode_trace_data,
    Batch, BatchType, FlatBatch, LogLevel, MetricType, SchemaType, SpanStatus,
};
use chrono::{TimeZone, Utc};
use tell_metrics::{SinkMetricsConfig, SinkMetricsProvider, SinkMetricsSnapshot};
use owo_colors::{OwoColorize, Style};
use tokio::sync::mpsc;

/// Stdout sink for debug output
pub struct StdoutSink {
    /// Channel receiver for batches
    receiver: mpsc::Receiver<Arc<Batch>>,

    /// Configuration
    config: StdoutConfig,

    /// Sink name for logging
    name: String,

    /// Metrics (Arc for sharing with metrics handle)
    metrics: Arc<StdoutSinkMetrics>,
}

/// Configuration for stdout sink
#[derive(Debug, Clone)]
pub struct StdoutConfig {
    /// Enable colored output
    pub color: bool,

    /// Show batch summary headers (default: false for cleaner output)
    pub show_batch_headers: bool,

    /// Maximum messages to show per batch (0 = all)
    pub max_messages: usize,
}

impl Default for StdoutConfig {
    fn default() -> Self {
        Self {
            color: true,
            show_batch_headers: false,
            max_messages: 0, // Show all
        }
    }
}

impl StdoutConfig {
    /// Create config with colors disabled (for piped output)
    pub fn no_color() -> Self {
        Self {
            color: false,
            ..Self::default()
        }
    }

    /// Create config with batch headers enabled
    pub fn with_headers() -> Self {
        Self {
            show_batch_headers: true,
            ..Self::default()
        }
    }
}

// =============================================================================
// Color Styles
// =============================================================================

/// Color styles for terminal output
struct Styles {
    timestamp: Style,
    label: Style,
    payload: Style,
    error: Style,
    ok: Style,
}

impl Styles {
    fn new(enabled: bool) -> Self {
        if enabled {
            Self {
                timestamp: Style::new().dimmed(),
                label: Style::new().dimmed(),
                payload: Style::new().dimmed(),
                error: Style::new().red(),
                ok: Style::new().green(),
            }
        } else {
            Self {
                timestamp: Style::new(),
                label: Style::new(),
                payload: Style::new(),
                error: Style::new(),
                ok: Style::new(),
            }
        }
    }
}

/// Get style for log level
fn level_style(level: LogLevel, enabled: bool) -> Style {
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

// =============================================================================
// Metrics
// =============================================================================

/// Metrics for stdout sink
#[derive(Debug, Default)]
pub struct StdoutSinkMetrics {
    batches_received: AtomicU64,
    messages_received: AtomicU64,
    bytes_received: AtomicU64,
}

impl StdoutSinkMetrics {
    #[inline]
    pub const fn new() -> Self {
        Self {
            batches_received: AtomicU64::new(0),
            messages_received: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
        }
    }

    #[inline]
    pub fn record_batch(&self, message_count: u64, byte_count: u64) {
        self.batches_received.fetch_add(1, Ordering::Relaxed);
        self.messages_received
            .fetch_add(message_count, Ordering::Relaxed);
        self.bytes_received.fetch_add(byte_count, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            batches_received: self.batches_received.load(Ordering::Relaxed),
            messages_received: self.messages_received.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
        }
    }
}

/// Point-in-time snapshot of stdout sink metrics
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub batches_received: u64,
    pub messages_received: u64,
    pub bytes_received: u64,
}

/// Handle for accessing stdout sink metrics
#[derive(Clone)]
pub struct StdoutSinkMetricsHandle {
    id: String,
    metrics: Arc<StdoutSinkMetrics>,
}

impl SinkMetricsProvider for StdoutSinkMetricsHandle {
    fn sink_id(&self) -> &str {
        &self.id
    }

    fn sink_type(&self) -> &str {
        "stdout"
    }

    fn metrics_config(&self) -> SinkMetricsConfig {
        SinkMetricsConfig {
            enabled: true,
            interval: Duration::from_secs(10),
        }
    }

    fn snapshot(&self) -> SinkMetricsSnapshot {
        let s = self.metrics.snapshot();
        SinkMetricsSnapshot {
            batches_received: s.batches_received,
            batches_written: s.batches_received,
            messages_written: s.messages_received,
            bytes_written: s.bytes_received,
            write_errors: 0,
            flush_count: 0,
        }
    }
}

// =============================================================================
// StdoutSink Implementation
// =============================================================================

impl StdoutSink {
    /// Create a new stdout sink with default config
    pub fn new(receiver: mpsc::Receiver<Arc<Batch>>) -> Self {
        Self::with_config(receiver, StdoutConfig::default())
    }

    /// Create a new stdout sink with custom config
    pub fn with_config(receiver: mpsc::Receiver<Arc<Batch>>, config: StdoutConfig) -> Self {
        Self::with_name_and_config(receiver, "stdout", config)
    }

    /// Create a new stdout sink with custom name and config
    pub fn with_name_and_config(
        receiver: mpsc::Receiver<Arc<Batch>>,
        name: impl Into<String>,
        config: StdoutConfig,
    ) -> Self {
        Self {
            receiver,
            config,
            name: name.into(),
            metrics: Arc::new(StdoutSinkMetrics::new()),
        }
    }

    /// Get reference to metrics
    #[inline]
    pub fn metrics(&self) -> &StdoutSinkMetrics {
        &self.metrics
    }

    /// Get a metrics handle for reporting
    pub fn metrics_handle(&self) -> StdoutSinkMetricsHandle {
        StdoutSinkMetricsHandle {
            id: self.name.clone(),
            metrics: Arc::clone(&self.metrics),
        }
    }

    /// Get the sink name
    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Run the sink, processing batches until the channel closes
    pub async fn run(mut self) -> MetricsSnapshot {
        tracing::info!(sink = %self.name, "stdout sink starting");

        while let Some(batch) = self.receiver.recv().await {
            self.process_batch(&batch);
        }

        let snapshot = self.metrics.snapshot();
        tracing::info!(
            sink = %self.name,
            batches = snapshot.batches_received,
            messages = snapshot.messages_received,
            bytes = snapshot.bytes_received,
            "stdout sink shutting down"
        );

        snapshot
    }

    /// Process a single batch
    fn process_batch(&self, batch: &Batch) {
        self.metrics
            .record_batch(batch.count() as u64, batch.total_bytes() as u64);

        let styles = Styles::new(self.config.color);

        // Optional batch header
        if self.config.show_batch_headers {
            println!(
                "[BATCH] type={:?} workspace={} source={} count={} bytes={}",
                batch.batch_type(),
                batch.workspace_id(),
                batch.source_id(),
                batch.count(),
                batch.total_bytes()
            );
        }

        // Common batch info
        let ws = format!("ws:{}", batch.workspace_id());
        let src = batch.source_id();
        let ip = format_ip(batch.source_ip());

        match batch.batch_type() {
            BatchType::Log | BatchType::Syslog => {
                self.print_logs(batch, &ws, src.as_str(), &ip, &styles);
            }
            BatchType::Event => {
                self.print_events(batch, &ws, src.as_str(), &ip, &styles);
            }
            BatchType::Snapshot => {
                self.print_snapshots(batch, &ws, src.as_str(), &ip, &styles);
            }
            BatchType::Metric => {
                self.print_metrics(batch, &ws, src.as_str(), &ip, &styles);
            }
            BatchType::Trace => {
                self.print_traces(batch, &ws, src.as_str(), &ip, &styles);
            }
        }
    }

    /// Print decoded log messages
    fn print_logs(&self, batch: &Batch, ws: &str, src: &str, ip: &str, styles: &Styles) {
        let max = if self.config.max_messages == 0 {
            batch.message_count()
        } else {
            self.config.max_messages.min(batch.message_count())
        };

        for i in 0..max {
            let Some(msg) = batch.get_message(i) else {
                continue;
            };

            // Parse FlatBatch envelope
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

            // Get inner data
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

            // Decode LogData
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

            // Print each log entry
            for log in logs {
                let ts = format_timestamp(log.timestamp);
                let level_str = format!("{:7}", log.level.as_str());
                let level_style = level_style(log.level, self.config.color);

                let svc = log.service.unwrap_or("-");
                let host = log.source.unwrap_or("");
                let location = if host.is_empty() {
                    svc.to_string()
                } else {
                    format!("{}@{}", svc, host)
                };

                let payload = format_payload(log.payload);

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

    /// Print decoded event messages
    fn print_events(&self, batch: &Batch, ws: &str, src: &str, ip: &str, styles: &Styles) {
        let max = if self.config.max_messages == 0 {
            batch.message_count()
        } else {
            self.config.max_messages.min(batch.message_count())
        };

        for i in 0..max {
            let Some(msg) = batch.get_message(i) else {
                continue;
            };

            // Parse FlatBatch envelope
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

            // Get inner data
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

            // Decode EventData
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

            // Print each event
            for event in events {
                let ts = format_timestamp(event.timestamp);
                let event_type = event.event_type.as_str();
                let name = event.event_name.unwrap_or("-");

                let device = event
                    .device_id
                    .map(|d| format!("dev={}", format_uuid_short(d)))
                    .unwrap_or_default();

                let payload = format_payload(event.payload);

                println!(
                    "{} {} {} {} {} {} {} {} {}",
                    ts.style(styles.timestamp),
                    ws.style(styles.label),
                    src.style(styles.label),
                    ip.style(styles.label),
                    "event".style(styles.label),
                    event_type,
                    name,
                    device.style(styles.label),
                    payload.style(styles.payload)
                );
            }
        }
    }

    /// Print decoded snapshot messages
    fn print_snapshots(&self, batch: &Batch, ws: &str, src: &str, ip: &str, styles: &Styles) {
        let max = if self.config.max_messages == 0 {
            batch.message_count()
        } else {
            self.config.max_messages.min(batch.message_count())
        };

        for i in 0..max {
            let Some(msg) = batch.get_message(i) else {
                continue;
            };

            // Parse FlatBatch envelope
            let flat_batch = match FlatBatch::parse(msg) {
                Ok(fb) => fb,
                Err(_) => {
                    // Fallback: might be raw JSON (legacy format)
                    println!("{}", String::from_utf8_lossy(msg));
                    continue;
                }
            };

            if flat_batch.schema_type() != SchemaType::Snapshot {
                continue;
            }

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
                let payload = format_payload(snapshot.payload);

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

    /// Print decoded metric messages
    fn print_metrics(&self, batch: &Batch, ws: &str, src: &str, ip: &str, styles: &Styles) {
        let max = if self.config.max_messages == 0 {
            batch.message_count()
        } else {
            self.config.max_messages.min(batch.message_count())
        };

        for i in 0..max {
            let Some(msg) = batch.get_message(i) else {
                continue;
            };

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
                            format!("count={} sum={:.3} min={:.3} max={:.3}", h.count, h.sum, h.min, h.max)
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
                    parts.extend(metric.int_labels.iter().map(|l| format!("{}={}", l.key, l.value)));
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

    /// Print decoded trace spans
    fn print_traces(&self, batch: &Batch, ws: &str, src: &str, ip: &str, styles: &Styles) {
        let max = if self.config.max_messages == 0 {
            batch.message_count()
        } else {
            self.config.max_messages.min(batch.message_count())
        };

        for i in 0..max {
            let Some(msg) = batch.get_message(i) else {
                continue;
            };

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

// =============================================================================
// Formatting Helpers
// =============================================================================

/// Format timestamp as HH:MM:SS.mmm (from milliseconds)
fn format_timestamp(ts_millis: u64) -> String {
    Utc.timestamp_millis_opt(ts_millis as i64)
        .single()
        .map(|dt| dt.format("%H:%M:%S%.3f").to_string())
        .unwrap_or_else(|| format!("{}", ts_millis))
}

/// Format timestamp as HH:MM:SS.mmm (from nanoseconds)
fn format_timestamp_ns(ts_nanos: u64) -> String {
    let secs = (ts_nanos / 1_000_000_000) as i64;
    let nanos = (ts_nanos % 1_000_000_000) as u32;
    Utc.timestamp_opt(secs, nanos)
        .single()
        .map(|dt| dt.format("%H:%M:%S%.3f").to_string())
        .unwrap_or_else(|| format!("{}", ts_nanos))
}

/// Format IP address
fn format_ip(ip: IpAddr) -> String {
    ip.to_string()
}

/// Format UUID as short form (first 8 hex chars)
fn format_uuid_short(bytes: &[u8; 16]) -> String {
    format!(
        "{:02x}{:02x}{:02x}{:02x}",
        bytes[0], bytes[1], bytes[2], bytes[3]
    )
}

/// Format payload as compact JSON or truncated string
fn format_payload(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        return String::new();
    }

    // Try JSON first
    if let Ok(json) = serde_json::from_slice::<serde_json::Value>(bytes) {
        let compact = serde_json::to_string(&json).unwrap_or_default();
        if compact.len() > 120 {
            format!("{}...", &compact[..117])
        } else {
            compact
        }
    } else if let Ok(s) = std::str::from_utf8(bytes) {
        // UTF-8 string
        if s.len() > 120 {
            format!("{}...", &s[..117])
        } else {
            s.to_string()
        }
    } else {
        // Binary
        format!("<{} bytes>", bytes.len())
    }
}

#[cfg(test)]
#[path = "stdout_test.rs"]
mod stdout_test;
