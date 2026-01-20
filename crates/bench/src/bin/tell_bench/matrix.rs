//! Matrix benchmark - runs all sources with multiple batch sizes
//!
//! Outputs a markdown table for easy copy-paste into RESULTS.md

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use tell_client::event::{EventBuilder, EventDataBuilder};
use tell_client::test::TcpTestClient;
use tell_client::{BatchBuilder, SchemaType};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UdpSocket};
use tokio::task::JoinSet;

use crate::common::{format_number, generate_events_jsonl, generate_syslog_message, system_info_string, SyslogFormat};

/// Matrix benchmark configuration
#[derive(clap::Args)]
pub struct MatrixArgs {
    /// Events per test (default 50M for accurate results)
    #[arg(short, long, default_value = "50000000")]
    pub events: u64,

    /// Number of clients
    #[arg(short, long, default_value = "5")]
    pub clients: usize,

    /// TCP server address
    #[arg(long, default_value = "127.0.0.1:50000")]
    pub tcp_server: String,

    /// HTTP server URL
    #[arg(long, default_value = "http://127.0.0.1:8080")]
    pub http_url: String,

    /// Syslog TCP server address
    #[arg(long, default_value = "127.0.0.1:50514")]
    pub syslog_tcp_server: String,

    /// Syslog UDP server address
    #[arg(long, default_value = "127.0.0.1:50514")]
    pub syslog_udp_server: String,

    /// API key (32 hex characters)
    #[arg(short = 'k', long, default_value = "000102030405060708090a0b0c0d0e0f")]
    pub api_key: String,

    /// Skip TCP benchmarks
    #[arg(long)]
    pub skip_tcp: bool,

    /// Skip HTTP benchmarks
    #[arg(long)]
    pub skip_http: bool,

    /// Skip Syslog benchmarks
    #[arg(long)]
    pub skip_syslog: bool,
}

impl Default for MatrixArgs {
    fn default() -> Self {
        Self {
            events: 50_000_000,
            clients: 5,
            tcp_server: "127.0.0.1:50000".to_string(),
            http_url: "http://127.0.0.1:8080".to_string(),
            syslog_tcp_server: "127.0.0.1:50514".to_string(),
            syslog_udp_server: "127.0.0.1:50514".to_string(),
            api_key: "000102030405060708090a0b0c0d0e0f".to_string(),
            skip_tcp: false,
            skip_http: false,
            skip_syslog: false,
        }
    }
}

/// Benchmark result for a single test
struct BenchResult {
    events_per_sec: u64,
}

impl BenchResult {
    fn format(&self) -> String {
        format_number(self.events_per_sec)
    }
}

/// Run all benchmarks and output matrix
pub async fn run(args: MatrixArgs) -> Result<(), Box<dyn std::error::Error>> {
    let batch_sizes = [500, 100, 10, 3];

    eprintln!("Running benchmarks: {} events per test, {} clients\n", format_number(args.events), args.clients);

    // Collect all results first
    let mut results: Vec<(&str, Vec<String>)> = Vec::new();

    // TCP Binary
    if !args.skip_tcp {
        let mut row = Vec::new();
        for &batch_size in &batch_sizes {
            let result = run_tcp_bench(&args.tcp_server, &args.api_key, args.clients, args.events, batch_size).await;
            match result {
                Ok(r) => row.push(r.format()),
                Err(e) => {
                    eprintln!("TCP error: {}", e);
                    row.push("ERR".to_string());
                }
            }
        }
        results.push(("TCP Binary", row));
    }

    // HTTP FBS
    if !args.skip_http {
        let mut row = Vec::new();
        for &batch_size in &batch_sizes {
            let result = run_http_fbs_bench(&args.http_url, &args.api_key, args.clients, args.events, batch_size).await;
            match result {
                Ok(r) => row.push(r.format()),
                Err(e) => {
                    eprintln!("HTTP FBS error: {}", e);
                    row.push("ERR".to_string());
                }
            }
        }
        results.push(("HTTP FBS", row));

        // HTTP JSON
        let mut row = Vec::new();
        for &batch_size in &batch_sizes {
            let result = run_http_json_bench(&args.http_url, &args.api_key, args.clients, args.events, batch_size).await;
            match result {
                Ok(r) => row.push(r.format()),
                Err(e) => {
                    eprintln!("HTTP JSON error: {}", e);
                    row.push("ERR".to_string());
                }
            }
        }
        results.push(("HTTP JSON", row));
    }

    // Syslog TCP
    if !args.skip_syslog {
        let mut row = Vec::new();
        for &batch_size in &batch_sizes {
            let result = run_syslog_tcp_bench(&args.syslog_tcp_server, args.clients, args.events, batch_size).await;
            match result {
                Ok(r) => row.push(r.format()),
                Err(e) => {
                    eprintln!("Syslog TCP error: {}", e);
                    row.push("ERR".to_string());
                }
            }
        }
        results.push(("Syslog TCP", row));

        // Syslog UDP
        let mut row = Vec::new();
        for &batch_size in &batch_sizes {
            let result = run_syslog_udp_bench(&args.syslog_udp_server, args.clients, args.events, batch_size).await;
            match result {
                Ok(r) => row.push(r.format()),
                Err(e) => {
                    eprintln!("Syslog UDP error: {}", e);
                    row.push("ERR".to_string());
                }
            }
        }
        results.push(("Syslog UDP", row));
    }

    // Print the complete table
    eprintln!();
    println!("## Benchmark Matrix\n");
    println!("{} | {} clients | {} events/test | null sink\n", system_info_string(), args.clients, format_number(args.events));

    // Header
    print!("| Source |");
    for b in &batch_sizes {
        print!(" b{} |", b);
    }
    println!();

    // Separator
    print!("|--------|");
    for _ in &batch_sizes {
        print!("------|");
    }
    println!();

    // Data rows
    for (name, row) in &results {
        print!("| {} |", name);
        for val in row {
            print!(" {} |", val);
        }
        println!();
    }

    Ok(())
}

// =============================================================================
// TCP Binary Benchmark
// =============================================================================

async fn run_tcp_bench(
    server: &str,
    api_key: &str,
    clients: usize,
    total_events: u64,
    batch_size: usize,
) -> Result<BenchResult, Box<dyn std::error::Error>> {
    eprintln!("  Testing TCP b{}...", batch_size);

    let api_key_bytes = hex::decode(api_key)?;
    let mut api_key_arr = [0u8; 16];
    api_key_arr.copy_from_slice(&api_key_bytes);

    let events_sent = Arc::new(AtomicU64::new(0));
    let bytes_sent = Arc::new(AtomicU64::new(0));
    let events_per_client = total_events / clients as u64;
    let batches_per_client = events_per_client / batch_size as u64;

    let start = Instant::now();
    let mut tasks = JoinSet::new();

    for _ in 0..clients {
        let server = server.to_string();
        let events_sent = Arc::clone(&events_sent);
        let bytes_sent = Arc::clone(&bytes_sent);

        tasks.spawn(async move {
            let mut client = TcpTestClient::connect(&server).await?;

            // Build batch once and reuse
            let event_size = 200;
            let event_data = vec![0x42u8; event_size];
            let mut payload = Vec::with_capacity(batch_size * event_size);
            for _ in 0..batch_size {
                payload.extend_from_slice(&event_data);
            }

            let batch = BatchBuilder::new()
                .api_key(api_key_arr)
                .schema_type(SchemaType::Event)
                .data(&payload)
                .build()?;

            let batch_bytes = batch.len() as u64;

            for _ in 0..batches_per_client {
                client.send(&batch).await?;
                events_sent.fetch_add(batch_size as u64, Ordering::Relaxed);
                bytes_sent.fetch_add(batch_bytes, Ordering::Relaxed);
            }

            let _ = client.flush().await;
            let _ = client.close().await;

            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        });
    }

    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(e.to_string().into()),
            Err(e) => return Err(format!("task panicked: {}", e).into()),
        }
    }

    let elapsed = start.elapsed();
    let total_sent = events_sent.load(Ordering::Relaxed);
    let _total_bytes = bytes_sent.load(Ordering::Relaxed);

    Ok(BenchResult {
        events_per_sec: (total_sent as f64 / elapsed.as_secs_f64()) as u64,
    })
}

// =============================================================================
// HTTP FBS Benchmark
// =============================================================================

async fn run_http_fbs_bench(
    url: &str,
    api_key: &str,
    clients: usize,
    total_events: u64,
    batch_size: usize,
) -> Result<BenchResult, Box<dyn std::error::Error>> {
    eprintln!("  Testing HTTP-FBS b{}...", batch_size);

    let api_key_bytes = hex::decode(api_key)?;
    let api_key_arr: [u8; 16] = api_key_bytes.try_into().map_err(|_| "invalid api key")?;

    let (host, port, path_prefix) = parse_http_url(url)?;
    let addr: SocketAddr = format!("{}:{}", host, port).parse()?;

    let events_sent = Arc::new(AtomicU64::new(0));
    let bytes_sent = Arc::new(AtomicU64::new(0));
    let events_per_client = total_events / clients as u64;

    let start = Instant::now();
    let mut tasks = JoinSet::new();

    for _ in 0..clients {
        let events_sent = Arc::clone(&events_sent);
        let bytes_sent = Arc::clone(&bytes_sent);
        let api_key_str = api_key.to_string();
        let path_prefix = path_prefix.clone();

        tasks.spawn(async move {
            let mut stream = TcpStream::connect(addr).await?;
            let _ = stream.set_nodelay(true);

            let mut local_events = 0u64;
            let device_id: [u8; 16] = [0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4,
                                       0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00];

            while local_events < events_per_client {
                let batch_count = std::cmp::min(batch_size, (events_per_client - local_events) as usize);

                // Build events
                let mut event_data_builder = EventDataBuilder::new();
                for i in 0..batch_count {
                    let event = EventBuilder::new()
                        .track(&format!("bench_{}", i))
                        .device_id(device_id)
                        .timestamp_now()
                        .build()?;
                    event_data_builder = event_data_builder.add(event);
                }
                let event_data = event_data_builder.build()?;

                let batch = BatchBuilder::new()
                    .api_key(api_key_arr)
                    .event_data(event_data)
                    .build()?;
                let fbs_data = batch.as_bytes();

                let request = format!(
                    "POST {}/v1/ingest HTTP/1.1\r\n\
                     Host: localhost\r\n\
                     Content-Type: application/x-flatbuffers\r\n\
                     X-API-Key: {}\r\n\
                     Content-Length: {}\r\n\
                     Connection: keep-alive\r\n\
                     \r\n",
                    path_prefix, api_key_str, fbs_data.len()
                );

                stream.write_all(request.as_bytes()).await?;
                stream.write_all(fbs_data).await?;
                read_http_response(&mut stream).await?;

                local_events += batch_count as u64;
                events_sent.fetch_add(batch_count as u64, Ordering::Relaxed);
                bytes_sent.fetch_add((request.len() + fbs_data.len()) as u64, Ordering::Relaxed);
            }

            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        });
    }

    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(e.to_string().into()),
            Err(e) => return Err(format!("task panicked: {}", e).into()),
        }
    }

    let elapsed = start.elapsed();
    let total_sent = events_sent.load(Ordering::Relaxed);
    let _total_bytes = bytes_sent.load(Ordering::Relaxed);

    Ok(BenchResult {
        events_per_sec: (total_sent as f64 / elapsed.as_secs_f64()) as u64,
    })
}

// =============================================================================
// HTTP JSON Benchmark
// =============================================================================

async fn run_http_json_bench(
    url: &str,
    api_key: &str,
    clients: usize,
    total_events: u64,
    batch_size: usize,
) -> Result<BenchResult, Box<dyn std::error::Error>> {
    eprintln!("  Testing HTTP-JSON b{}...", batch_size);

    let (host, port, path_prefix) = parse_http_url(url)?;
    let addr: SocketAddr = format!("{}:{}", host, port).parse()?;

    let events_sent = Arc::new(AtomicU64::new(0));
    let bytes_sent = Arc::new(AtomicU64::new(0));
    let events_per_client = total_events / clients as u64;

    let start = Instant::now();
    let mut tasks = JoinSet::new();

    for client_id in 0..clients {
        let events_sent = Arc::clone(&events_sent);
        let bytes_sent = Arc::clone(&bytes_sent);
        let api_key = api_key.to_string();
        let path_prefix = path_prefix.clone();

        tasks.spawn(async move {
            let mut stream = TcpStream::connect(addr).await?;
            let _ = stream.set_nodelay(true);

            let mut local_events = 0u64;

            while local_events < events_per_client {
                let batch_count = std::cmp::min(batch_size, (events_per_client - local_events) as usize);
                let jsonl = generate_events_jsonl(batch_count, client_id);

                let request = format!(
                    "POST {}/v1/events HTTP/1.1\r\n\
                     Host: localhost\r\n\
                     Content-Type: application/json\r\n\
                     X-API-Key: {}\r\n\
                     Content-Length: {}\r\n\
                     Connection: keep-alive\r\n\
                     \r\n",
                    path_prefix, api_key, jsonl.len()
                );

                stream.write_all(request.as_bytes()).await?;
                stream.write_all(&jsonl).await?;
                read_http_response(&mut stream).await?;

                local_events += batch_count as u64;
                events_sent.fetch_add(batch_count as u64, Ordering::Relaxed);
                bytes_sent.fetch_add((request.len() + jsonl.len()) as u64, Ordering::Relaxed);
            }

            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        });
    }

    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(e.to_string().into()),
            Err(e) => return Err(format!("task panicked: {}", e).into()),
        }
    }

    let elapsed = start.elapsed();
    let total_sent = events_sent.load(Ordering::Relaxed);
    let _total_bytes = bytes_sent.load(Ordering::Relaxed);

    Ok(BenchResult {
        events_per_sec: (total_sent as f64 / elapsed.as_secs_f64()) as u64,
    })
}

// =============================================================================
// Syslog TCP Benchmark
// =============================================================================

async fn run_syslog_tcp_bench(
    server: &str,
    clients: usize,
    total_events: u64,
    batch_size: usize,
) -> Result<BenchResult, Box<dyn std::error::Error>> {
    eprintln!("  Testing Syslog-TCP b{}...", batch_size);

    let addr: SocketAddr = server.parse()?;

    let events_sent = Arc::new(AtomicU64::new(0));
    let bytes_sent = Arc::new(AtomicU64::new(0));
    let events_per_client = total_events / clients as u64;

    let start = Instant::now();
    let mut tasks = JoinSet::new();

    for client_id in 0..clients {
        let events_sent = Arc::clone(&events_sent);
        let bytes_sent = Arc::clone(&bytes_sent);

        tasks.spawn(async move {
            let mut stream = TcpStream::connect(addr).await?;
            let _ = stream.set_nodelay(true);

            let mut local_events = 0u64;
            let mut seq = 0u64;

            while local_events < events_per_client {
                // Build batch of messages
                let mut batch_data = Vec::new();
                let batch_count = std::cmp::min(batch_size, (events_per_client - local_events) as usize);

                for _ in 0..batch_count {
                    let msg = generate_syslog_message(SyslogFormat::Rfc3164, client_id, seq);
                    batch_data.extend_from_slice(msg.as_bytes());
                    batch_data.push(b'\n');
                    seq += 1;
                }

                stream.write_all(&batch_data).await?;

                local_events += batch_count as u64;
                events_sent.fetch_add(batch_count as u64, Ordering::Relaxed);
                bytes_sent.fetch_add(batch_data.len() as u64, Ordering::Relaxed);
            }

            let _ = stream.flush().await;

            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        });
    }

    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(e.to_string().into()),
            Err(e) => return Err(format!("task panicked: {}", e).into()),
        }
    }

    let elapsed = start.elapsed();
    let total_sent = events_sent.load(Ordering::Relaxed);
    let _total_bytes = bytes_sent.load(Ordering::Relaxed);

    Ok(BenchResult {
        events_per_sec: (total_sent as f64 / elapsed.as_secs_f64()) as u64,
    })
}

// =============================================================================
// Syslog UDP Benchmark
// =============================================================================

async fn run_syslog_udp_bench(
    server: &str,
    clients: usize,
    total_events: u64,
    batch_size: usize,
) -> Result<BenchResult, Box<dyn std::error::Error>> {
    eprintln!("  Testing Syslog-UDP b{}...", batch_size);

    let server_addr: SocketAddr = server.parse()?;

    let events_sent = Arc::new(AtomicU64::new(0));
    let bytes_sent = Arc::new(AtomicU64::new(0));
    let events_per_client = total_events / clients as u64;

    let start = Instant::now();
    let mut tasks = JoinSet::new();

    for client_id in 0..clients {
        let events_sent = Arc::clone(&events_sent);
        let bytes_sent = Arc::clone(&bytes_sent);

        tasks.spawn(async move {
            let socket = UdpSocket::bind("0.0.0.0:0").await?;
            socket.connect(server_addr).await?;

            let mut local_events = 0u64;
            let mut seq = 0u64;

            while local_events < events_per_client {
                let batch_count = std::cmp::min(batch_size, (events_per_client - local_events) as usize);

                // UDP sends each message as separate datagram
                for _ in 0..batch_count {
                    let msg = generate_syslog_message(SyslogFormat::Rfc3164, client_id, seq);
                    let msg_bytes = msg.as_bytes();
                    socket.send(msg_bytes).await?;
                    bytes_sent.fetch_add(msg_bytes.len() as u64, Ordering::Relaxed);
                    seq += 1;
                }

                local_events += batch_count as u64;
                events_sent.fetch_add(batch_count as u64, Ordering::Relaxed);
            }

            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        });
    }

    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(e.to_string().into()),
            Err(e) => return Err(format!("task panicked: {}", e).into()),
        }
    }

    let elapsed = start.elapsed();
    let total_sent = events_sent.load(Ordering::Relaxed);
    let _total_bytes = bytes_sent.load(Ordering::Relaxed);

    Ok(BenchResult {
        events_per_sec: (total_sent as f64 / elapsed.as_secs_f64()) as u64,
    })
}

// =============================================================================
// Helpers
// =============================================================================

/// Read HTTP response until end of headers
async fn read_http_response(stream: &mut TcpStream) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut buf = [0u8; 1024];
    let mut total = 0;

    loop {
        let n = stream.read(&mut buf[total..]).await?;
        if n == 0 {
            return Err("connection closed".into());
        }
        total += n;

        if total >= 4 {
            for i in 0..total - 3 {
                if &buf[i..i + 4] == b"\r\n\r\n" {
                    return Ok(());
                }
            }
        }

        if total >= buf.len() {
            return Ok(());
        }
    }
}

/// Parse HTTP URL into (host, port, path)
fn parse_http_url(url: &str) -> Result<(String, u16, String), Box<dyn std::error::Error>> {
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
