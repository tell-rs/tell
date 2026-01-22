//! Status command - Check server health and get metrics
//!
//! Connects to the API server to get server status and metrics.
//!
//! # Usage
//!
//! ```bash
//! # Check if server is running
//! tell status
//!
//! # Get detailed metrics
//! tell status --metrics
//!
//! # Watch mode (refresh every second)
//! tell status --watch
//!
//! # JSON output
//! tell status --json
//! ```

use std::time::Duration;

use anyhow::{Context, Result};
use clap::Args;
use serde::{Deserialize, Serialize};

/// Status command arguments
#[derive(Args, Debug)]
pub struct StatusArgs {
    /// API server endpoint URL
    #[arg(long, default_value = "http://127.0.0.1:3000")]
    pub endpoint: String,

    /// Show detailed metrics
    #[arg(short, long)]
    pub metrics: bool,

    /// Watch mode - refresh every second
    #[arg(short, long)]
    pub watch: bool,

    /// Output as JSON
    #[arg(long)]
    pub json: bool,
}

/// Health response from control endpoint
#[derive(Debug, Deserialize, Serialize)]
struct HealthResponse {
    status: String,
    uptime_secs: u64,
}

/// Metrics response from control endpoint
#[derive(Debug, Deserialize, Serialize)]
struct MetricsResponse {
    uptime_secs: u64,
    pipeline: Option<PipelineMetrics>,
    sources: Vec<SourceMetrics>,
    sinks: Vec<SinkMetrics>,
}

#[derive(Debug, Deserialize, Serialize)]
struct PipelineMetrics {
    messages_processed: u64,
    bytes_processed: u64,
    batches_received: u64,
    batches_routed: u64,
    batches_dropped: u64,
}

#[derive(Debug, Deserialize, Serialize)]
struct SourceMetrics {
    id: String,
    #[serde(rename = "type")]
    source_type: String,
    messages_received: u64,
    bytes_received: u64,
    connections_active: u64,
    errors: u64,
}

#[derive(Debug, Deserialize, Serialize)]
struct SinkMetrics {
    id: String,
    #[serde(rename = "type")]
    sink_type: String,
    messages_written: u64,
    bytes_written: u64,
    write_errors: u64,
}

/// Run the status command
pub async fn run(args: StatusArgs) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .context("failed to create HTTP client")?;

    if args.watch {
        run_watch_mode(&client, &args).await
    } else {
        run_once(&client, &args).await
    }
}

async fn run_once(client: &reqwest::Client, args: &StatusArgs) -> Result<()> {
    if args.metrics {
        let metrics = fetch_metrics(client, &args.endpoint).await?;
        if args.json {
            println!("{}", serde_json::to_string_pretty(&metrics)?);
        } else {
            print_metrics(&metrics);
        }
    } else {
        let health = fetch_health(client, &args.endpoint).await?;
        if args.json {
            println!("{}", serde_json::to_string_pretty(&health)?);
        } else {
            print_health(&health);
        }
    }
    Ok(())
}

async fn run_watch_mode(client: &reqwest::Client, args: &StatusArgs) -> Result<()> {
    use std::io::{Write, stdout};

    // Store previous metrics for rate calculation
    let mut prev_metrics: Option<MetricsResponse> = None;
    let mut prev_time = std::time::Instant::now();

    loop {
        // Clear screen and move cursor to top
        print!("\x1B[2J\x1B[H");
        stdout().flush()?;

        match fetch_metrics(client, &args.endpoint).await {
            Ok(metrics) => {
                let now = std::time::Instant::now();
                let elapsed = now.duration_since(prev_time).as_secs_f64();

                print_metrics_with_rates(&metrics, prev_metrics.as_ref(), elapsed);

                prev_metrics = Some(metrics);
                prev_time = now;
            }
            Err(e) => {
                println!("Error: {}", e);
                println!("\nServer may not be running. Start with: tell serve");
            }
        }

        println!("\n[Press Ctrl+C to exit]");
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn fetch_health(client: &reqwest::Client, endpoint: &str) -> Result<HealthResponse> {
    let url = format!("{}/health", endpoint);
    let resp = client
        .get(&url)
        .send()
        .await
        .context("failed to connect to control endpoint - is the server running?")?;

    if !resp.status().is_success() {
        anyhow::bail!("health check failed: {}", resp.status());
    }

    resp.json().await.context("failed to parse health response")
}

async fn fetch_metrics(client: &reqwest::Client, endpoint: &str) -> Result<MetricsResponse> {
    let url = format!("{}/metrics", endpoint);
    let resp = client
        .get(&url)
        .send()
        .await
        .context("failed to connect to control endpoint - is the server running?")?;

    if !resp.status().is_success() {
        anyhow::bail!("metrics fetch failed: {}", resp.status());
    }

    resp.json()
        .await
        .context("failed to parse metrics response")
}

fn print_health(health: &HealthResponse) {
    let uptime = format_duration(health.uptime_secs);
    println!("Status: {}", health.status);
    println!("Uptime: {}", uptime);
}

fn print_metrics(metrics: &MetricsResponse) {
    print_metrics_with_rates(metrics, None, 0.0);
}

fn print_metrics_with_rates(
    metrics: &MetricsResponse,
    prev: Option<&MetricsResponse>,
    elapsed: f64,
) {
    let uptime = format_duration(metrics.uptime_secs);
    println!("Tell Server Status");
    println!("==================");
    println!("Uptime: {}", uptime);
    println!();

    // Pipeline metrics
    if let Some(ref p) = metrics.pipeline {
        println!("Pipeline:");
        println!("  Messages: {}", format_number(p.messages_processed));
        println!("  Bytes:    {}", format_bytes(p.bytes_processed));
        println!(
            "  Batches:  {} received, {} routed, {} dropped",
            format_number(p.batches_received),
            format_number(p.batches_routed),
            p.batches_dropped
        );

        // Calculate rates if we have previous data
        if let Some(prev_p) = prev.and_then(|p| p.pipeline.as_ref())
            && elapsed > 0.0
        {
            let msg_rate = (p.messages_processed - prev_p.messages_processed) as f64 / elapsed;
            let byte_rate = (p.bytes_processed - prev_p.bytes_processed) as f64 / elapsed;
            println!(
                "  Rate:     {}/s, {}/s",
                format_rate(msg_rate),
                format_bytes_rate(byte_rate)
            );
        }
        println!();
    }

    // Sources
    if !metrics.sources.is_empty() {
        println!("Sources:");
        for source in &metrics.sources {
            println!("  {} ({}):", source.id, source.source_type);
            println!(
                "    Messages: {}, Bytes: {}, Connections: {}, Errors: {}",
                format_number(source.messages_received),
                format_bytes(source.bytes_received),
                source.connections_active,
                source.errors
            );
        }
        println!();
    }

    // Sinks
    if !metrics.sinks.is_empty() {
        println!("Sinks:");
        for sink in &metrics.sinks {
            println!("  {} ({}):", sink.id, sink.sink_type);
            println!(
                "    Messages: {}, Bytes: {}, Errors: {}",
                format_number(sink.messages_written),
                format_bytes(sink.bytes_written),
                sink.write_errors
            );
        }
    }
}

fn format_duration(secs: u64) -> String {
    if secs < 60 {
        format!("{}s", secs)
    } else if secs < 3600 {
        format!("{}m {}s", secs / 60, secs % 60)
    } else if secs < 86400 {
        format!("{}h {}m", secs / 3600, (secs % 3600) / 60)
    } else {
        format!("{}d {}h", secs / 86400, (secs % 86400) / 3600)
    }
}

fn format_number(n: u64) -> String {
    if n >= 1_000_000_000 {
        format!("{:.1}B", n as f64 / 1_000_000_000.0)
    } else if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.1}K", n as f64 / 1_000.0)
    } else {
        n.to_string()
    }
}

fn format_bytes(bytes: u64) -> String {
    if bytes >= 1_000_000_000 {
        format!("{:.1}GB", bytes as f64 / 1_000_000_000.0)
    } else if bytes >= 1_000_000 {
        format!("{:.1}MB", bytes as f64 / 1_000_000.0)
    } else if bytes >= 1_000 {
        format!("{:.1}KB", bytes as f64 / 1_000.0)
    } else {
        format!("{}B", bytes)
    }
}

fn format_rate(rate: f64) -> String {
    if rate >= 1_000_000.0 {
        format!("{:.1}M", rate / 1_000_000.0)
    } else if rate >= 1_000.0 {
        format!("{:.1}K", rate / 1_000.0)
    } else {
        format!("{:.0}", rate)
    }
}

fn format_bytes_rate(rate: f64) -> String {
    if rate >= 1_000_000_000.0 {
        format!("{:.1}GB", rate / 1_000_000_000.0)
    } else if rate >= 1_000_000.0 {
        format!("{:.1}MB", rate / 1_000_000.0)
    } else if rate >= 1_000.0 {
        format!("{:.1}KB", rate / 1_000.0)
    } else {
        format!("{:.0}B", rate)
    }
}
