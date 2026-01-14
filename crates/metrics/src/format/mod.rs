//! Metrics output formatters
//!
//! Formats collected metrics for human-readable or JSON output.

mod human;
mod json;

pub use human::HumanFormatter;
pub use json::JsonFormatter;

use crate::{CollectedMetrics, MetricsRates, SinkMetricsSnapshot};

/// Trait for metrics formatters
pub trait MetricsFormatter: Send + Sync {
    /// Format unified metrics report
    fn format_unified(&self, metrics: &CollectedMetrics, rates: Option<&MetricsRates>) -> String;

    /// Format per-sink metrics report
    fn format_sink(
        &self,
        sink_id: &str,
        sink_type: &str,
        snapshot: &SinkMetricsSnapshot,
        interval_secs: u64,
    ) -> String;
}

/// Format bytes in human-readable form (KB, MB, GB)
pub fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * 1024;
    const GB: u64 = 1024 * 1024 * 1024;

    if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

/// Format bytes per second in human-readable form
pub fn format_bytes_per_sec(bytes_per_sec: f64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = 1024.0 * 1024.0;
    const GB: f64 = 1024.0 * 1024.0 * 1024.0;

    if bytes_per_sec >= GB {
        format!("{:.1} GB/s", bytes_per_sec / GB)
    } else if bytes_per_sec >= MB {
        format!("{:.1} MB/s", bytes_per_sec / MB)
    } else if bytes_per_sec >= KB {
        format!("{:.1} KB/s", bytes_per_sec / KB)
    } else {
        format!("{:.0} B/s", bytes_per_sec)
    }
}

/// Format count with K/M suffix for readability
pub fn format_count(count: u64) -> String {
    const K: u64 = 1000;
    const M: u64 = 1_000_000;

    if count >= M {
        format!("{:.1}M", count as f64 / M as f64)
    } else if count >= K {
        format!("{:.1}K", count as f64 / K as f64)
    } else {
        count.to_string()
    }
}

/// Format rate per second with K/M suffix
pub fn format_rate(rate: f64) -> String {
    const K: f64 = 1000.0;
    const M: f64 = 1_000_000.0;

    if rate >= M {
        format!("{:.1}M/s", rate / M)
    } else if rate >= K {
        format!("{:.1}K/s", rate / K)
    } else {
        format!("{:.0}/s", rate)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(500), "500 B");
        assert_eq!(format_bytes(1024), "1.0 KB");
        assert_eq!(format_bytes(1536), "1.5 KB");
        assert_eq!(format_bytes(1024 * 1024), "1.0 MB");
        assert_eq!(format_bytes(1024 * 1024 * 1024), "1.0 GB");
        assert_eq!(format_bytes(1536 * 1024 * 1024), "1.5 GB");
    }

    #[test]
    fn test_format_bytes_per_sec() {
        assert_eq!(format_bytes_per_sec(500.0), "500 B/s");
        assert_eq!(format_bytes_per_sec(1024.0), "1.0 KB/s");
        assert_eq!(format_bytes_per_sec(50.0 * 1024.0 * 1024.0), "50.0 MB/s");
    }

    #[test]
    fn test_format_count() {
        assert_eq!(format_count(500), "500");
        assert_eq!(format_count(1000), "1.0K");
        assert_eq!(format_count(1500), "1.5K");
        assert_eq!(format_count(1_000_000), "1.0M");
        assert_eq!(format_count(1_500_000), "1.5M");
    }

    #[test]
    fn test_format_rate() {
        assert_eq!(format_rate(500.0), "500/s");
        assert_eq!(format_rate(1000.0), "1.0K/s");
        assert_eq!(format_rate(1_200_000.0), "1.2M/s");
    }
}
