//! Common utilities for tell-bench

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use sysinfo::System;

// =============================================================================
// System Info
// =============================================================================

/// Get system info as a formatted string: "Apple M4 Pro (aarch64) | 12 cores | 24.0 GB"
pub fn system_info_string() -> String {
    let mut sys = System::new_all();
    sys.refresh_all();

    let cpu_name = sys
        .cpus()
        .first()
        .map(|cpu| cpu.brand().trim().to_string())
        .unwrap_or_else(|| "Unknown".to_string());

    format!(
        "{} ({}) | {} cores | {:.1} GB",
        cpu_name,
        std::env::consts::ARCH,
        sys.cpus().len(),
        sys.total_memory() as f64 / 1_000_000_000.0
    )
}

// =============================================================================
// Formatting
// =============================================================================

/// Format a number with human-readable suffixes (K, M, B)
pub fn format_number(n: u64) -> String {
    if n >= 1_000_000_000 {
        format!("{:.2}B", n as f64 / 1_000_000_000.0)
    } else if n >= 1_000_000 {
        format!("{:.2}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.2}K", n as f64 / 1_000.0)
    } else {
        n.to_string()
    }
}

/// Format bytes as human-readable (always MB for consistency in benchmarks)
pub fn format_bytes_mb(bytes: u64) -> String {
    format!("{:.1} MB", bytes as f64 / 1_000_000.0)
}

/// Format bytes as human-readable (auto-scale)
pub fn format_bytes(bytes: u64) -> String {
    if bytes >= 1_000_000_000 {
        format!("{:.1} GB", bytes as f64 / 1_000_000_000.0)
    } else if bytes >= 1_000_000 {
        format!("{:.1} MB", bytes as f64 / 1_000_000.0)
    } else if bytes >= 1_000 {
        format!("{:.1} KB", bytes as f64 / 1_000.0)
    } else {
        format!("{} B", bytes)
    }
}

/// Format rate as MB/s
pub fn format_rate_mb(bytes_per_sec: f64) -> String {
    format!("{:.1} MB/s", bytes_per_sec / 1_000_000.0)
}

// =============================================================================
// Header Printing
// =============================================================================

const LINE_WIDTH: usize = 72;

/// Print a benchmark header in the compact box style
pub fn print_header(test_name: &str, details: &str) {
    let line = "─".repeat(LINE_WIDTH);
    println!("{}", line);
    println!("{} | {}", test_name, details);
    println!("System    | {}", system_info_string());
    println!("{}", line);
}

// =============================================================================
// Progress Reporting
// =============================================================================

/// Timed progress reporter - prints lines like `[   1.0s]    64.57M events/s |  12924.1 MB/s |   64639000 events`
pub struct TimedProgressReporter {
    running: Arc<AtomicBool>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl TimedProgressReporter {
    /// Start a timed progress reporter (tokio)
    pub fn start(
        events: Arc<AtomicU64>,
        bytes: Arc<AtomicU64>,
        total: Option<u64>,
    ) -> Self {
        let running = Arc::new(AtomicBool::new(true));
        let running_clone = running.clone();

        let handle = tokio::spawn(async move {
            let start = Instant::now();
            let mut last_events = 0u64;
            let mut last_bytes = 0u64;
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            interval.tick().await; // Skip first immediate tick

            loop {
                interval.tick().await;
                if !running_clone.load(Ordering::Relaxed) {
                    break;
                }

                let elapsed = start.elapsed().as_secs_f64();
                let current = events.load(Ordering::Relaxed);
                let current_bytes = bytes.load(Ordering::Relaxed);
                let rate = current - last_events;
                let byte_rate = (current_bytes - last_bytes) as f64;

                let progress_str = if let Some(t) = total {
                    format!(" ({:5.1}%)", (current as f64 / t as f64) * 100.0)
                } else {
                    String::new()
                };

                println!(
                    "[{:6.1}s] {:>10} events/s | {:>10} | {:>12} events{}",
                    elapsed,
                    format_number(rate),
                    format_rate_mb(byte_rate),
                    format_number(current),
                    progress_str
                );

                last_events = current;
                last_bytes = current_bytes;

                if let Some(t) = total {
                    if current >= t {
                        break;
                    }
                }
            }
        });

        Self {
            running,
            handle: Some(handle),
        }
    }

    /// Start a progress reporter with a target total (backward compatible)
    pub fn start_with_target(
        events: Arc<AtomicU64>,
        bytes: Arc<AtomicU64>,
        total: u64,
    ) -> Self {
        Self::start(events, bytes, Some(total))
    }

    /// Stop the progress reporter
    pub fn stop(mut self) {
        self.running.store(false, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

/// Thread-based timed progress reporter (for sync contexts like HTTP)
pub struct ThreadTimedProgressReporter {
    running: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl ThreadTimedProgressReporter {
    /// Start a thread-based timed progress reporter
    pub fn start(
        events: Arc<AtomicU64>,
        bytes: Arc<AtomicU64>,
        total: Option<u64>,
    ) -> Self {
        let running = Arc::new(AtomicBool::new(true));
        let running_clone = running.clone();

        let handle = std::thread::spawn(move || {
            let start = Instant::now();
            let mut last_events = 0u64;
            let mut last_bytes = 0u64;

            while running_clone.load(Ordering::Relaxed) {
                std::thread::sleep(Duration::from_secs(1));
                if !running_clone.load(Ordering::Relaxed) {
                    break;
                }

                let elapsed = start.elapsed().as_secs_f64();
                let current = events.load(Ordering::Relaxed);
                let current_bytes = bytes.load(Ordering::Relaxed);
                let rate = current - last_events;
                let byte_rate = (current_bytes - last_bytes) as f64;

                let progress_str = if let Some(t) = total {
                    format!(" ({:5.1}%)", (current as f64 / t as f64) * 100.0)
                } else {
                    String::new()
                };

                println!(
                    "[{:6.1}s] {:>10} events/s | {:>10} | {:>12} events{}",
                    elapsed,
                    format_number(rate),
                    format_rate_mb(byte_rate),
                    format_number(current),
                    progress_str
                );

                last_events = current;
                last_bytes = current_bytes;

                if let Some(t) = total {
                    if current >= t {
                        break;
                    }
                }
            }
        });

        Self {
            running,
            handle: Some(handle),
        }
    }

    /// Stop the progress reporter
    pub fn stop(mut self) {
        self.running.store(false, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

/// Print final summary line
pub fn print_summary(total_events: u64, total_bytes: u64, elapsed: Duration) {
    let secs = elapsed.as_secs_f64();
    let events_per_sec = total_events as f64 / secs;
    let bytes_per_sec = total_bytes as f64 / secs;
    println!(
        "───────── {} events | {} | {:.2}s | {} events/s | {}",
        format_number(total_events),
        format_bytes_mb(total_bytes),
        secs,
        format_number(events_per_sec as u64),
        format_rate_mb(bytes_per_sec)
    );
}

/// Alias for backward compatibility with syslog benchmarks
pub type TokioProgressReporter = TimedProgressReporter;

// =============================================================================
// Syslog Utilities
// =============================================================================

/// Syslog format options
#[derive(clap::ValueEnum, Clone, Copy, Debug)]
pub enum SyslogFormat {
    /// RFC 3164 (BSD syslog)
    Rfc3164,
    /// RFC 5424 (IETF syslog)
    Rfc5424,
}

/// Generate a syslog message
pub fn generate_syslog_message(format: SyslogFormat, client_id: usize, seq: u64) -> String {
    match format {
        SyslogFormat::Rfc3164 => {
            format!(
                "<134>Jan 15 12:00:00 host{} app[{}]: Message sequence {}",
                client_id,
                std::process::id(),
                seq
            )
        }
        SyslogFormat::Rfc5424 => {
            format!(
                "<134>1 2024-01-15T12:00:00.000Z host{} app {} ID{} - Message sequence {}",
                client_id,
                std::process::id(),
                seq,
                seq
            )
        }
    }
}

// =============================================================================
// HTTP Utilities
// =============================================================================

/// Parse HTTP URL into host, port, and path prefix
pub fn parse_http_url(url: &str) -> Result<(String, u16, String), Box<dyn std::error::Error>> {
    let url = url.trim_end_matches('/');
    let without_scheme = url
        .strip_prefix("http://")
        .or_else(|| url.strip_prefix("https://"))
        .unwrap_or(url);

    let (host_port, path) = if let Some(idx) = without_scheme.find('/') {
        (&without_scheme[..idx], &without_scheme[idx..])
    } else {
        (without_scheme, "")
    };

    let (host, port) = if let Some(idx) = host_port.rfind(':') {
        let host = &host_port[..idx];
        let port: u16 = host_port[idx + 1..].parse()?;
        (host.to_string(), port)
    } else {
        (host_port.to_string(), 8080)
    };

    Ok((host, port, path.to_string()))
}

/// Generate JSONL events for HTTP JSON benchmarks
pub fn generate_events_jsonl(count: usize, client_id: usize) -> Vec<u8> {
    let mut lines = Vec::with_capacity(count);
    for i in 0..count {
        lines.push(format!(
            r#"{{"type":"track","event":"bench_event_{}","device_id":"550e8400-e29b-41d4-a716-446655440000","timestamp":{},"properties":{{"client":{},"index":{}}}}}"#,
            i,
            chrono::Utc::now().timestamp_millis(),
            client_id,
            i
        ));
    }
    lines.join("\n").into_bytes()
}
