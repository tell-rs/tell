//! HTTP source benchmarks (JSON and FlatBuffer)

use std::io::{self, Write};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use tell_client::BatchBuilder;
use tell_client::event::{EventBuilder, EventDataBuilder};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::task::JoinSet;

use crate::common::{
    ThreadTimedProgressReporter, format_number, generate_events_jsonl, parse_http_url,
    print_header, print_summary,
};

/// Read HTTP response until we see end of headers (\r\n\r\n)
/// Returns Ok(()) on success, Err on connection issues
async fn read_http_response(
    stream: &mut TcpStream,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut buf = [0u8; 1024];
    let mut total = 0;

    loop {
        let n = stream.read(&mut buf[total..]).await?;
        if n == 0 {
            // Connection closed
            return Err("connection closed".into());
        }
        total += n;

        // Check for end of HTTP headers (\r\n\r\n)
        // Our responses are small (202 Accepted with JSON body), so headers + body fit in 1024 bytes
        if total >= 4 {
            for i in 0..total - 3 {
                if &buf[i..i + 4] == b"\r\n\r\n" {
                    // Found end of headers - for our simple responses, body is included
                    return Ok(());
                }
            }
        }

        // Buffer full but no end of headers found - response too large (shouldn't happen)
        if total >= buf.len() {
            return Ok(()); // Assume we got enough
        }
    }
}

/// HTTP benchmark mode (shared by JSON and FlatBuffer)
#[derive(clap::Subcommand)]
pub enum HttpMode {
    /// Load test - measure throughput
    Load {
        /// Number of concurrent clients
        #[arg(short, long, default_value = "5")]
        clients: usize,

        /// Total events to send
        #[arg(short, long, default_value = "1000000")]
        events: u64,

        /// Events per request
        #[arg(short, long, default_value = "500")]
        batch_size: usize,

        /// Server URL
        #[arg(short = 'u', long, default_value = "http://127.0.0.1:8080")]
        url: String,

        /// API key (32 hex characters = 16 bytes)
        #[arg(short = 'k', long, default_value = "000102030405060708090a0b0c0d0e0f")]
        api_key: String,
    },
    /// Benchmark - test different request sizes
    Benchmark {
        /// Number of concurrent clients
        #[arg(short, long, default_value = "5")]
        clients: usize,

        /// Requests per size
        #[arg(long, default_value = "1000")]
        requests_per_size: usize,

        /// Server URL
        #[arg(short = 'u', long, default_value = "http://127.0.0.1:8080")]
        url: String,

        /// API key (32 hex characters = 16 bytes)
        #[arg(short = 'k', long, default_value = "000102030405060708090a0b0c0d0e0f")]
        api_key: String,
    },
}

/// Run HTTP JSON benchmark
pub async fn run_json(mode: HttpMode) -> Result<(), Box<dyn std::error::Error>> {
    match mode {
        HttpMode::Load {
            clients,
            events,
            batch_size,
            url,
            api_key,
        } => json_load_test(&url, &api_key, clients, events, batch_size).await,
        HttpMode::Benchmark {
            clients,
            requests_per_size,
            url,
            api_key,
        } => json_benchmark(&url, &api_key, clients, requests_per_size).await,
    }
}

/// Run HTTP FlatBuffer benchmark
pub async fn run_fbs(mode: HttpMode) -> Result<(), Box<dyn std::error::Error>> {
    match mode {
        HttpMode::Load {
            clients,
            events,
            batch_size,
            url,
            api_key,
        } => fbs_load_test(&url, &api_key, clients, events, batch_size).await,
        HttpMode::Benchmark {
            clients,
            requests_per_size,
            url,
            api_key,
        } => fbs_benchmark(&url, &api_key, clients, requests_per_size).await,
    }
}

// =============================================================================
// HTTP JSON
// =============================================================================

async fn json_load_test(
    url: &str,
    api_key: &str,
    clients: usize,
    total_events: u64,
    batch_size: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    print_header(
        "HTTP JSON",
        &format!(
            "{}/v1/events | {} clients | {} events | batch {}",
            url,
            clients,
            format_number(total_events),
            batch_size
        ),
    );

    let events_sent = Arc::new(AtomicU64::new(0));
    let bytes_sent = Arc::new(AtomicU64::new(0));
    let requests_sent = Arc::new(AtomicU64::new(0));

    let (host, port, path_prefix) = parse_http_url(url)?;
    let addr: SocketAddr = format!("{}:{}", host, port).parse()?;

    // Start progress thread
    let progress = ThreadTimedProgressReporter::start(
        Arc::clone(&events_sent),
        Arc::clone(&bytes_sent),
        Some(total_events),
    );

    let start = Instant::now();
    let events_per_client = total_events / clients as u64;
    let mut handles = JoinSet::new();

    for client_id in 0..clients {
        let events_sent = Arc::clone(&events_sent);
        let bytes_sent = Arc::clone(&bytes_sent);
        let requests_sent = Arc::clone(&requests_sent);
        let api_key = api_key.to_string();
        let path_prefix = path_prefix.clone();

        handles.spawn(async move {
            let mut stream = TcpStream::connect(addr).await?;
            let _ = stream.set_nodelay(true);

            let mut local_events = 0u64;

            while local_events < events_per_client {
                let batch_count =
                    std::cmp::min(batch_size, (events_per_client - local_events) as usize);
                let jsonl = generate_events_jsonl(batch_count, client_id);

                let request = format!(
                    "POST {}/v1/events HTTP/1.1\r\n\
                     Host: localhost\r\n\
                     Content-Type: application/json\r\n\
                     X-API-Key: {}\r\n\
                     Content-Length: {}\r\n\
                     Connection: keep-alive\r\n\
                     \r\n",
                    path_prefix,
                    api_key,
                    jsonl.len()
                );

                stream.write_all(request.as_bytes()).await?;
                stream.write_all(&jsonl).await?;

                // Read HTTP response properly (wait for server to respond)
                read_http_response(&mut stream).await?;

                local_events += batch_count as u64;
                events_sent.fetch_add(batch_count as u64, Ordering::Relaxed);
                bytes_sent.fetch_add((request.len() + jsonl.len()) as u64, Ordering::Relaxed);
                requests_sent.fetch_add(1, Ordering::Relaxed);
            }

            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        });
    }

    while let Some(result) = handles.join_next().await {
        if let Err(e) = result {
            eprintln!("\nClient error: {:?}", e);
        }
    }

    progress.stop();

    let elapsed = start.elapsed();
    let total_events = events_sent.load(Ordering::Relaxed);
    let total_bytes = bytes_sent.load(Ordering::Relaxed);

    print_summary(total_events, total_bytes, elapsed);

    Ok(())
}

async fn json_benchmark(
    url: &str,
    api_key: &str,
    clients: usize,
    requests_per_size: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("HTTP JSON Benchmark");
    println!("===================");
    println!("URL:              {}/v1/events", url);
    println!("Clients:          {}", clients);
    println!("Requests/Size:    {}", requests_per_size);
    println!();

    let batch_sizes = [10, 50, 100, 500, 1000];

    for batch_size in batch_sizes {
        let total_events = (requests_per_size * batch_size * clients) as u64;
        print!("  {} events/request: ", batch_size);
        let _ = io::stdout().flush();

        let events_sent = Arc::new(AtomicU64::new(0));
        let (host, port, path_prefix) = parse_http_url(url)?;
        let addr: SocketAddr = format!("{}:{}", host, port).parse()?;

        let start = Instant::now();
        let mut handles = JoinSet::new();

        for client_id in 0..clients {
            let events_sent = Arc::clone(&events_sent);
            let api_key = api_key.to_string();
            let path_prefix = path_prefix.clone();

            handles.spawn(async move {
                let mut stream = TcpStream::connect(addr).await?;
                let _ = stream.set_nodelay(true);

                for _ in 0..requests_per_size {
                    let jsonl = generate_events_jsonl(batch_size, client_id);
                    let request = format!(
                        "POST {}/v1/events HTTP/1.1\r\n\
                         Host: localhost\r\n\
                         Content-Type: application/json\r\n\
                         X-API-Key: {}\r\n\
                         Content-Length: {}\r\n\
                         Connection: keep-alive\r\n\
                         \r\n",
                        path_prefix,
                        api_key,
                        jsonl.len()
                    );

                    stream.write_all(request.as_bytes()).await?;
                    stream.write_all(&jsonl).await?;

                    // Read HTTP response properly
                    read_http_response(&mut stream).await?;

                    events_sent.fetch_add(batch_size as u64, Ordering::Relaxed);
                }

                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
            });
        }

        while handles.join_next().await.is_some() {}

        let elapsed = start.elapsed();
        let events_per_sec = total_events as f64 / elapsed.as_secs_f64();
        println!(
            "{:.0} events/sec ({:.2}s)",
            events_per_sec,
            elapsed.as_secs_f64()
        );
    }

    Ok(())
}

// =============================================================================
// HTTP FlatBuffer
// =============================================================================

async fn fbs_load_test(
    url: &str,
    api_key: &str,
    clients: usize,
    total_events: u64,
    batch_size: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    print_header(
        "HTTP FBS",
        &format!(
            "{}/v1/ingest | {} clients | {} events | batch {}",
            url,
            clients,
            format_number(total_events),
            batch_size
        ),
    );

    let events_sent = Arc::new(AtomicU64::new(0));
    let bytes_sent = Arc::new(AtomicU64::new(0));
    let requests_sent = Arc::new(AtomicU64::new(0));

    let (host, port, path_prefix) = parse_http_url(url)?;
    let addr: SocketAddr = format!("{}:{}", host, port).parse()?;

    let api_key_bytes = hex::decode(api_key)?;
    let api_key_arr: [u8; 16] = api_key_bytes
        .try_into()
        .map_err(|_| "invalid api key length")?;

    // Start progress thread
    let progress = ThreadTimedProgressReporter::start(
        Arc::clone(&events_sent),
        Arc::clone(&bytes_sent),
        Some(total_events),
    );

    let start = Instant::now();
    let events_per_client = total_events / clients as u64;
    let mut handles = JoinSet::new();

    for _client_id in 0..clients {
        let events_sent = Arc::clone(&events_sent);
        let bytes_sent = Arc::clone(&bytes_sent);
        let requests_sent = Arc::clone(&requests_sent);
        let api_key_str = api_key.to_string();
        let path_prefix = path_prefix.clone();

        handles.spawn(async move {
            let mut stream = TcpStream::connect(addr).await?;
            let _ = stream.set_nodelay(true);

            let mut local_events = 0u64;
            let device_id: [u8; 16] = [
                0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44,
                0x00, 0x00,
            ];

            while local_events < events_per_client {
                let batch_count =
                    std::cmp::min(batch_size, (events_per_client - local_events) as usize);

                // Build events using tell-client
                let mut event_data_builder = EventDataBuilder::new();
                for i in 0..batch_count {
                    let event = EventBuilder::new()
                        .track(&format!("bench_event_{}", i))
                        .device_id(device_id)
                        .timestamp_now()
                        .payload_json(r#"{"key":"value"}"#)
                        .build()
                        .expect("valid event");
                    event_data_builder = event_data_builder.add(event);
                }
                let event_data = event_data_builder.build().expect("valid event data");

                // Wrap in batch
                let batch = BatchBuilder::new()
                    .api_key(api_key_arr)
                    .event_data(event_data)
                    .build()
                    .expect("valid batch");
                let fbs_data = batch.as_bytes();

                let request = format!(
                    "POST {}/v1/ingest HTTP/1.1\r\n\
                     Host: localhost\r\n\
                     Content-Type: application/x-flatbuffers\r\n\
                     X-API-Key: {}\r\n\
                     Content-Length: {}\r\n\
                     Connection: keep-alive\r\n\
                     \r\n",
                    path_prefix,
                    api_key_str,
                    fbs_data.len()
                );

                stream.write_all(request.as_bytes()).await?;
                stream.write_all(fbs_data).await?;

                // Read HTTP response properly (wait for server to respond)
                read_http_response(&mut stream).await?;

                local_events += batch_count as u64;
                events_sent.fetch_add(batch_count as u64, Ordering::Relaxed);
                bytes_sent.fetch_add((request.len() + fbs_data.len()) as u64, Ordering::Relaxed);
                requests_sent.fetch_add(1, Ordering::Relaxed);
            }

            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        });
    }

    while let Some(result) = handles.join_next().await {
        if let Err(e) = result {
            eprintln!("\nClient error: {:?}", e);
        }
    }

    progress.stop();

    let elapsed = start.elapsed();
    let total_events = events_sent.load(Ordering::Relaxed);
    let total_bytes = bytes_sent.load(Ordering::Relaxed);

    print_summary(total_events, total_bytes, elapsed);

    Ok(())
}

async fn fbs_benchmark(
    url: &str,
    api_key: &str,
    clients: usize,
    requests_per_size: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("HTTP FlatBuffer Benchmark");
    println!("=========================");
    println!("URL:              {}/v1/ingest", url);
    println!("Clients:          {}", clients);
    println!("Requests/Size:    {}", requests_per_size);
    println!();

    let batch_sizes = [10, 50, 100, 500, 1000];
    let api_key_bytes = hex::decode(api_key)?;
    let api_key_arr: [u8; 16] = api_key_bytes
        .try_into()
        .map_err(|_| "invalid api key length")?;

    for batch_size in batch_sizes {
        let total_events = (requests_per_size * batch_size * clients) as u64;
        print!("  {} events/request: ", batch_size);
        let _ = io::stdout().flush();

        let events_sent = Arc::new(AtomicU64::new(0));
        let (host, port, path_prefix) = parse_http_url(url)?;
        let addr: SocketAddr = format!("{}:{}", host, port).parse()?;

        let start = Instant::now();
        let mut handles = JoinSet::new();

        for _client_id in 0..clients {
            let events_sent = Arc::clone(&events_sent);
            let api_key_str = api_key.to_string();
            let path_prefix = path_prefix.clone();

            handles.spawn(async move {
                let mut stream = TcpStream::connect(addr).await?;
                let _ = stream.set_nodelay(true);
                let device_id: [u8; 16] = [
                    0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55,
                    0x44, 0x00, 0x00,
                ];

                for _ in 0..requests_per_size {
                    // Build events
                    let mut event_data_builder = EventDataBuilder::new();
                    for i in 0..batch_size {
                        let event = EventBuilder::new()
                            .track(&format!("bench_event_{}", i))
                            .device_id(device_id)
                            .timestamp_now()
                            .payload_json(r#"{"key":"value"}"#)
                            .build()
                            .expect("valid event");
                        event_data_builder = event_data_builder.add(event);
                    }
                    let event_data = event_data_builder.build().expect("valid event data");

                    // Wrap in batch
                    let batch = BatchBuilder::new()
                        .api_key(api_key_arr)
                        .event_data(event_data)
                        .build()
                        .expect("valid batch");
                    let fbs_data = batch.as_bytes();

                    let request = format!(
                        "POST {}/v1/ingest HTTP/1.1\r\n\
                         Host: localhost\r\n\
                         Content-Type: application/x-flatbuffers\r\n\
                         X-API-Key: {}\r\n\
                         Content-Length: {}\r\n\
                         Connection: keep-alive\r\n\
                         \r\n",
                        path_prefix,
                        api_key_str,
                        fbs_data.len()
                    );

                    stream.write_all(request.as_bytes()).await?;
                    stream.write_all(fbs_data).await?;

                    // Read HTTP response properly
                    read_http_response(&mut stream).await?;

                    events_sent.fetch_add(batch_size as u64, Ordering::Relaxed);
                }

                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
            });
        }

        while handles.join_next().await.is_some() {}

        let elapsed = start.elapsed();
        let events_per_sec = total_events as f64 / elapsed.as_secs_f64();
        println!(
            "{:.0} events/sec ({:.2}s)",
            events_per_sec,
            elapsed.as_secs_f64()
        );
    }

    Ok(())
}
