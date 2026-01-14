//! Load test binary - mirrors Go benchmark methodology
//!
//! Run against a live collector to measure true throughput with concurrent clients.
//!
//! Usage:
//!   cargo run --release -p cdp-bench --bin loadtest
//!   cargo run --release -p cdp-bench --bin loadtest -- --clients 100 --events 100000000
//!
//! Default: 100 clients, 500 events/batch, 100M total events

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use cdp_client::test::TcpTestClient;
use cdp_client::{BatchBuilder, SchemaType};
use chrono::Local;
use clap::Parser;
use sysinfo::System;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

/// Load test configuration
#[derive(Parser, Debug)]
#[command(name = "loadtest", about = "CDP Collector load test")]
struct Args {
    /// Server address
    #[arg(short, long, default_value = "127.0.0.1:50000")]
    server: String,

    /// Number of concurrent clients
    #[arg(short, long, default_value = "100")]
    clients: usize,

    /// Total events to send
    #[arg(short, long, default_value = "100000000")]
    events: u64,

    /// Events per batch
    #[arg(short, long, default_value = "500")]
    batch_size: usize,

    /// API key (hex)
    #[arg(short = 'k', long, default_value = "000102030405060708090a0b0c0d0e0f")]
    api_key: String,

    /// Report interval in seconds
    #[arg(short, long, default_value = "1")]
    report_interval: u64,

    /// Use preset configuration (overrides clients/events/batch_size)
    /// Options: vector-fair (32 clients, matches Vector's benchmark setup)
    #[arg(short, long)]
    preset: Option<String>,
}

impl Args {
    /// Apply preset configuration, overriding individual flags
    fn apply_preset(mut self) -> Result<Self, String> {
        if let Some(preset) = &self.preset {
            match preset.as_str() {
                "vector-fair" => {
                    self.clients = 32;
                    self.events = 50_000_000;
                    self.batch_size = 500;
                }
                _ => return Err(format!("Unknown preset: '{}'. Available: vector-fair", preset)),
            }
        }
        Ok(self)
    }
}

/// System information for benchmark context
struct SystemInfo {
    cpu_name: String,
    cpu_cores: usize,
    memory_gb: f64,
    os: String,
    arch: String,
}

impl SystemInfo {
    fn collect() -> Self {
        let sys = System::new_all();

        let cpu_name = sys.cpus().first()
            .map(|cpu| cpu.brand().to_string())
            .unwrap_or_else(|| "Unknown".to_string());

        let cpu_cores = sys.cpus().len();
        let memory_gb = sys.total_memory() as f64 / 1_073_741_824.0; // bytes to GB

        let os = format!("{} {}",
            System::name().unwrap_or_else(|| "Unknown".to_string()),
            System::os_version().unwrap_or_default()
        );

        let arch = std::env::consts::ARCH.to_string();

        Self { cpu_name, cpu_cores, memory_gb, os, arch }
    }

    fn one_line(&self) -> String {
        format!("{} ({}) | {} cores | {:.1} GB | {}",
            self.cpu_name, self.arch, self.cpu_cores, self.memory_gb, self.os)
    }
}

/// Get current timestamp in ISO format (matches collector log format)
fn timestamp() -> String {
    Local::now().format("%Y-%m-%dT%H:%M:%S%.6f").to_string()
}

/// Shared metrics
struct Metrics {
    events_sent: AtomicU64,
    batches_sent: AtomicU64,
    bytes_sent: AtomicU64,
    errors: AtomicU64,
    clients_connected: AtomicU64,
    clients_done: AtomicU64,
}

impl Metrics {
    fn new() -> Self {
        Self {
            events_sent: AtomicU64::new(0),
            batches_sent: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            clients_connected: AtomicU64::new(0),
            clients_done: AtomicU64::new(0),
        }
    }
}

/// Client configuration for load testing
#[derive(Clone)]
struct ClientConfig {
    server: String,
    api_key: [u8; 16],
    batches_to_send: u64,
    batch_size: usize,
    event_size: usize,
}

/// Run a single client
async fn run_client(
    client_id: usize,
    config: ClientConfig,
    metrics: Arc<Metrics>,
    start_signal: Arc<Semaphore>,
) {
    let ClientConfig {
        server,
        api_key,
        batches_to_send,
        batch_size,
        event_size,
    } = config;
    // Connect
    let mut client = match TcpTestClient::connect(&server).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Client {} failed to connect: {}", client_id, e);
            metrics.errors.fetch_add(1, Ordering::Relaxed);
            return;
        }
    };

    metrics.clients_connected.fetch_add(1, Ordering::Relaxed);

    // Wait for start signal (all clients connected)
    let _ = start_signal.acquire().await;

    // Pre-build a batch template
    let event_data = vec![0x42u8; event_size];
    let mut payload = Vec::with_capacity(batch_size * event_size);
    for _ in 0..batch_size {
        payload.extend_from_slice(&event_data);
    }

    let batch = match BatchBuilder::new()
        .api_key(api_key)
        .schema_type(SchemaType::Event)
        .data(&payload)
        .build()
    {
        Ok(b) => b,
        Err(e) => {
            eprintln!("Client {} failed to build batch: {}", client_id, e);
            metrics.errors.fetch_add(1, Ordering::Relaxed);
            return;
        }
    };

    let batch_bytes = batch.len() as u64;

    // Send batches
    for _ in 0..batches_to_send {
        if let Err(e) = client.send(&batch).await {
            eprintln!("Client {} send error: {}", client_id, e);
            metrics.errors.fetch_add(1, Ordering::Relaxed);
            break;
        }

        metrics.events_sent.fetch_add(batch_size as u64, Ordering::Relaxed);
        metrics.batches_sent.fetch_add(1, Ordering::Relaxed);
        metrics.bytes_sent.fetch_add(batch_bytes, Ordering::Relaxed);
    }

    // Flush and close
    let _ = client.flush().await;
    let _ = client.close().await;

    metrics.clients_done.fetch_add(1, Ordering::Relaxed);
}

/// Reporter task - prints metrics every interval
async fn reporter(
    metrics: Arc<Metrics>,
    total_events: u64,
    num_clients: usize,
    report_interval: Duration,
    start_time: Instant,
) {
    let mut last_events = 0u64;
    let mut last_bytes = 0u64;
    let mut last_time = start_time;

    // Track sustained throughput (intervals with all clients active)
    let mut sustained_rates: Vec<f64> = Vec::new();

    loop {
        tokio::time::sleep(report_interval).await;

        let now = Instant::now();
        let current_events = metrics.events_sent.load(Ordering::Relaxed);
        let current_bytes = metrics.bytes_sent.load(Ordering::Relaxed);
        let current_batches = metrics.batches_sent.load(Ordering::Relaxed);
        let errors = metrics.errors.load(Ordering::Relaxed);
        let connected = metrics.clients_connected.load(Ordering::Relaxed);
        let done = metrics.clients_done.load(Ordering::Relaxed);

        // Interval stats
        let interval_events = current_events - last_events;
        let interval_bytes = current_bytes - last_bytes;
        let interval_secs = now.duration_since(last_time).as_secs_f64();

        let events_per_sec = interval_events as f64 / interval_secs;
        let mb_per_sec = (interval_bytes as f64 / 1_000_000.0) / interval_secs;

        // Track sustained rate (only when all clients still active)
        let active_clients = connected - done;
        if active_clients == num_clients as u64 {
            sustained_rates.push(events_per_sec);
        }

        // Overall stats
        let total_secs = now.duration_since(start_time).as_secs_f64();
        let progress = (current_events as f64 / total_events as f64) * 100.0;

        println!(
            "[{:>6.1}s] {:>10.2}M events/s | {:>8.1} MB/s | {:>10} events ({:>5.1}%) | batches: {} | clients: {}/{} | errors: {}",
            total_secs,
            events_per_sec / 1_000_000.0,
            mb_per_sec,
            current_events,
            progress,
            current_batches,
            active_clients,
            num_clients,
            errors,
        );

        last_events = current_events;
        last_bytes = current_bytes;
        last_time = now;

        // Stop if all done
        if done >= num_clients as u64 || current_events >= total_events {
            let overall_rate = current_events as f64 / total_secs;

            // Calculate sustained rate (average of full-capacity intervals)
            let sustained_rate = if sustained_rates.is_empty() {
                overall_rate
            } else {
                sustained_rates.iter().sum::<f64>() / sustained_rates.len() as f64
            };

            // Final summary - compact format
            println!();
            println!("Finished  | {}", timestamp());
            println!("Results   | Sustained: {:.2}M/s | Overall: {:.2}M/s | {:.2}s | {} events | {} batches | {} MB | {} errors",
                sustained_rate / 1_000_000.0,
                overall_rate / 1_000_000.0,
                total_secs,
                current_events,
                current_batches,
                current_bytes / 1_000_000,
                errors);
            break;
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse().apply_preset()?;

    // Parse API key
    let api_key_bytes = hex::decode(&args.api_key)?;
    if api_key_bytes.len() != 16 {
        return Err("API key must be 32 hex characters (16 bytes)".into());
    }
    let mut api_key = [0u8; 16];
    api_key.copy_from_slice(&api_key_bytes);

    // Calculate distribution
    let batches_total = args.events / args.batch_size as u64;
    let batches_per_client = batches_total / args.clients as u64;
    let actual_events = batches_per_client * args.clients as u64 * args.batch_size as u64;

    let sysinfo = SystemInfo::collect();

    // Compact header
    println!("Load Test | {} | {} clients | {}M events | batch {}",
        args.server, args.clients, actual_events / 1_000_000, args.batch_size);
    println!("System    | {}", sysinfo.one_line());
    if let Some(preset) = &args.preset {
        println!("Preset    | {}", preset);
    }

    let metrics = Arc::new(Metrics::new());

    // Semaphore to synchronize start (initially no permits)
    let start_signal = Arc::new(Semaphore::new(0));

    // Spawn clients
    let mut clients = JoinSet::new();

    print!("Connecting {} clients... ", args.clients);

    let client_config = ClientConfig {
        server: args.server.clone(),
        api_key,
        batches_to_send: batches_per_client,
        batch_size: args.batch_size,
        event_size: 200, // ~200 bytes like Go benchmark
    };

    for i in 0..args.clients {
        let config = client_config.clone();
        let metrics = Arc::clone(&metrics);
        let start_signal = Arc::clone(&start_signal);

        clients.spawn(run_client(i, config, metrics, start_signal));
    }

    // Wait for all clients to connect
    tokio::time::sleep(Duration::from_millis(500)).await;

    let connected = metrics.clients_connected.load(Ordering::Relaxed);
    println!("{}/{} connected", connected, args.clients);

    if connected == 0 {
        println!("No clients connected. Is the collector running?");
        return Ok(());
    }

    println!();
    println!("Started   | {}", timestamp());

    let start_time = Instant::now();

    // Release all clients to start sending
    start_signal.add_permits(args.clients);

    // Start reporter
    let reporter_metrics = Arc::clone(&metrics);
    let reporter_handle = tokio::spawn(reporter(
        reporter_metrics,
        actual_events,
        args.clients,
        Duration::from_secs(args.report_interval),
        start_time,
    ));

    // Wait for all clients to finish
    while let Some(result) = clients.join_next().await {
        if let Err(e) = result {
            eprintln!("Client task error: {}", e);
        }
    }

    // Wait for reporter to print final stats
    let _ = reporter_handle.await;

    Ok(())
}
