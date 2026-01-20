//! TCP FlatBuffer source benchmarks

use std::io::{self, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tell_client::test::TcpTestClient;
use tell_client::{BatchBuilder, SchemaType};
use tokio::io::AsyncWriteExt;
use tokio::task::JoinSet;

use crate::common::{format_number, print_header, print_summary, TimedProgressReporter};

/// TCP benchmark mode
#[derive(clap::Subcommand)]
pub enum TcpMode {
    /// Load test - measure throughput with FlatBuffer batches
    Load {
        /// Number of concurrent clients
        #[arg(short, long, default_value = "5")]
        clients: usize,

        /// Total events to send
        #[arg(short, long, default_value = "10000000")]
        events: u64,

        /// Messages per batch
        #[arg(short, long, default_value = "500")]
        batch_size: usize,

        /// Server address
        #[arg(short, long, default_value = "127.0.0.1:50000")]
        server: String,

        /// API key (32 hex characters = 16 bytes)
        #[arg(short = 'k', long, default_value = "000102030405060708090a0b0c0d0e0f")]
        api_key: String,
    },
    /// Benchmark - test different message sizes
    Benchmark {
        /// Number of concurrent clients
        #[arg(short, long, default_value = "5")]
        clients: usize,

        /// Batches per size
        #[arg(long, default_value = "10000")]
        batches_per_size: usize,

        /// Messages per batch
        #[arg(long, default_value = "100")]
        batch_size: usize,

        /// Server address
        #[arg(short, long, default_value = "127.0.0.1:50000")]
        server: String,

        /// API key (32 hex characters = 16 bytes)
        #[arg(short = 'k', long, default_value = "000102030405060708090a0b0c0d0e0f")]
        api_key: String,
    },
    /// Crash test - send malformed/malicious FlatBuffer data
    Crash {
        /// Server address
        #[arg(short, long, default_value = "127.0.0.1:50000")]
        server: String,
    },
}

/// Run TCP benchmark based on mode
pub async fn run(mode: TcpMode) -> Result<(), Box<dyn std::error::Error>> {
    match mode {
        TcpMode::Load {
            clients,
            events,
            batch_size,
            server,
            api_key,
        } => load_test(&server, &api_key, clients, events, batch_size).await,
        TcpMode::Benchmark {
            clients,
            batches_per_size,
            batch_size,
            server,
            api_key,
        } => benchmark(&server, &api_key, clients, batches_per_size, batch_size).await,
        TcpMode::Crash { server } => crash_test(&server).await,
    }
}

async fn load_test(
    server: &str,
    api_key: &str,
    clients: usize,
    total_events: u64,
    batch_size: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    print_header(
        "Load Test",
        &format!(
            "{} | {} clients | {} events | batch {}",
            server,
            clients,
            format_number(total_events),
            batch_size
        ),
    );

    let events_sent = Arc::new(AtomicU64::new(0));
    let bytes_sent = Arc::new(AtomicU64::new(0));
    let events_per_client = total_events / clients as u64;
    let batches_per_client = events_per_client / batch_size as u64;

    let start = Instant::now();
    let mut tasks = JoinSet::new();

    // Spawn reporter
    let reporter = TimedProgressReporter::start(
        Arc::clone(&events_sent),
        Arc::clone(&bytes_sent),
        Some(total_events),
    );

    // Spawn clients
    for client_id in 0..clients {
        let server = server.to_string();
        let api_key = api_key.to_string();
        let events_sent = Arc::clone(&events_sent);
        let bytes_sent = Arc::clone(&bytes_sent);

        tasks.spawn(async move {
            if let Err(e) = flatbuffer_client(
                &server,
                &api_key,
                client_id,
                batches_per_client,
                batch_size,
                &events_sent,
                &bytes_sent,
            )
            .await
            {
                eprintln!("Client {} error: {}", client_id, e);
            }
        });
    }

    while let Some(result) = tasks.join_next().await {
        if let Err(e) = result {
            eprintln!("Task error: {}", e);
        }
    }

    reporter.stop();

    let elapsed = start.elapsed();
    let total_sent = events_sent.load(Ordering::Relaxed);
    let total_bytes = bytes_sent.load(Ordering::Relaxed);

    print_summary(total_sent, total_bytes, elapsed);

    Ok(())
}

async fn flatbuffer_client(
    server: &str,
    api_key: &str,
    _client_id: usize,
    batches: u64,
    batch_size: usize,
    events_sent: &AtomicU64,
    bytes_sent: &AtomicU64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let api_key_bytes =
        hex::decode(api_key).map_err(|e| format!("Invalid API key hex: {}", e))?;
    if api_key_bytes.len() != 16 {
        return Err("API key must be 32 hex characters".into());
    }
    let mut api_key_arr = [0u8; 16];
    api_key_arr.copy_from_slice(&api_key_bytes);

    let mut client = TcpTestClient::connect(server).await?;

    // Pre-build event data (~200 bytes per event like Go benchmark)
    let event_size = 200;
    let event_data = vec![0x42u8; event_size];
    let mut payload = Vec::with_capacity(batch_size * event_size);
    for _ in 0..batch_size {
        payload.extend_from_slice(&event_data);
    }

    // Build batch once and reuse
    let batch = BatchBuilder::new()
        .api_key(api_key_arr)
        .schema_type(SchemaType::Event)
        .data(&payload)
        .build()
        .map_err(|e| format!("Failed to build batch: {}", e))?;

    let batch_bytes = batch.len() as u64;

    for _ in 0..batches {
        client.send(&batch).await?;
        events_sent.fetch_add(batch_size as u64, Ordering::Relaxed);
        bytes_sent.fetch_add(batch_bytes, Ordering::Relaxed);
    }

    let _ = client.flush().await;
    let _ = client.close().await;

    Ok(())
}

async fn benchmark(
    server: &str,
    api_key: &str,
    clients: usize,
    batches_per_size: usize,
    batch_size: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    print_header(
        "Benchmark",
        &format!(
            "{} | {} clients | {} batches/size | batch {}",
            server, clients, batches_per_size, batch_size
        ),
    );
    println!();

    let sizes = [100, 256, 512, 1024, 4096, 16384, 65536, 262144, 1048576];

    println!(
        "{:>10} | {:>12} | {:>10} | {:>12}",
        "Msg Size", "Msg/s", "MB/s", "Total"
    );
    println!("{}", "-".repeat(55));

    for msg_size in sizes {
        let events_sent = Arc::new(AtomicU64::new(0));
        let bytes_sent = Arc::new(AtomicU64::new(0));

        let start = Instant::now();
        let mut tasks = JoinSet::new();
        let batches_per_client = batches_per_size / clients;

        for client_id in 0..clients {
            let server = server.to_string();
            let api_key = api_key.to_string();
            let events_sent = Arc::clone(&events_sent);
            let bytes_sent = Arc::clone(&bytes_sent);

            tasks.spawn(async move {
                let _ = sized_client(
                    &server,
                    &api_key,
                    client_id,
                    batches_per_client as u64,
                    batch_size,
                    msg_size,
                    &events_sent,
                    &bytes_sent,
                )
                .await;
            });
        }

        while tasks.join_next().await.is_some() {}

        let elapsed = start.elapsed();
        let total = events_sent.load(Ordering::Relaxed);
        let total_bytes = bytes_sent.load(Ordering::Relaxed);
        let msg_rate = total as f64 / elapsed.as_secs_f64();
        let mb_rate = (total_bytes as f64 / elapsed.as_secs_f64()) / 1_000_000.0;

        println!(
            "{:>9}B | {:>12} | {:>9.1} | {:>12}",
            format_number(msg_size as u64),
            format_number(msg_rate as u64),
            mb_rate,
            format_number(total)
        );
    }

    Ok(())
}

async fn sized_client(
    server: &str,
    api_key: &str,
    _client_id: usize,
    batches: u64,
    batch_size: usize,
    msg_size: usize,
    events_sent: &AtomicU64,
    bytes_sent: &AtomicU64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let api_key_bytes =
        hex::decode(api_key).map_err(|e| format!("Invalid API key hex: {}", e))?;
    if api_key_bytes.len() != 16 {
        return Err("API key must be 32 hex characters".into());
    }
    let mut api_key_arr = [0u8; 16];
    api_key_arr.copy_from_slice(&api_key_bytes);

    let mut client = TcpTestClient::connect(server).await?;

    // Generate payload of exact size
    let event_data: Vec<u8> = (0..msg_size).map(|i| (i % 26) as u8 + b'a').collect();
    let mut payload = Vec::with_capacity(batch_size * msg_size);
    for _ in 0..batch_size {
        payload.extend_from_slice(&event_data);
    }

    // Build batch once and reuse
    let batch = BatchBuilder::new()
        .api_key(api_key_arr)
        .schema_type(SchemaType::Event)
        .data(&payload)
        .build()
        .map_err(|e| format!("Failed to build batch: {}", e))?;

    let batch_bytes = batch.len() as u64;

    for _ in 0..batches {
        client.send(&batch).await?;
        events_sent.fetch_add(batch_size as u64, Ordering::Relaxed);
        bytes_sent.fetch_add(batch_bytes, Ordering::Relaxed);
    }

    let _ = client.flush().await;
    let _ = client.close().await;

    Ok(())
}

async fn crash_test(server: &str) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== TCP FlatBuffer Crash Test ===");
    println!("Server: {}", server);
    println!();

    let tests: Vec<(&str, Vec<u8>)> = vec![
        (
            "JSON garbage",
            br#"{"event":"test","data":"this is not flatbuffer"}"#.to_vec(),
        ),
        ("Zero size header", vec![0, 0, 0, 0]),
        ("Negative size (0xFFFFFFFF)", vec![0xFF, 0xFF, 0xFF, 0xFF]),
        ("Huge size (1GB)", vec![0x00, 0x00, 0x00, 0x40]),
        ("Size larger than data", {
            let mut v = vec![0x00, 0x01, 0x00, 0x00];
            v.extend_from_slice(b"but only this much");
            v
        }),
        ("Invalid vtable offset", {
            let mut v = vec![0x10, 0x00, 0x00, 0x00];
            v.extend_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF]);
            v.extend_from_slice(&[0x00; 8]);
            v
        }),
        ("Random binary garbage", {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut hasher = DefaultHasher::new();
            std::time::SystemTime::now().hash(&mut hasher);
            let seed = hasher.finish();
            (0..1000)
                .map(|i| ((seed.wrapping_mul(i as u64)) & 0xFF) as u8)
                .collect()
        }),
        ("Incomplete size header (2 bytes)", vec![0x10, 0x00]),
        ("Incomplete size header (3 bytes)", vec![0x10, 0x00, 0x00]),
        ("All null bytes (100)", vec![0x00; 100]),
        ("UTF-8 BOM + garbage", {
            let mut v = vec![0xEF, 0xBB, 0xBF];
            v.extend_from_slice(b"not flatbuffer");
            v
        }),
    ];

    for (name, data) in &tests {
        print!("  {:35} ... ", name);
        io::stdout().flush()?;

        match tokio::net::TcpStream::connect(server).await {
            Ok(mut stream) => match stream.write_all(data).await {
                Ok(()) => {
                    let _ = stream.flush().await;
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    println!("\x1b[32mOK\x1b[0m (sent {} bytes)", data.len());
                }
                Err(e) => println!("\x1b[33mWRITE ERR\x1b[0m: {}", e),
            },
            Err(e) => println!("\x1b[31mCONNECT ERR\x1b[0m: {}", e),
        }
    }

    // High volume test
    println!();
    print!("  {:35} ... ", "High volume (1000 rapid connections)");
    io::stdout().flush()?;

    let mut success = 0;
    let mut fail = 0;
    for _ in 0..1000 {
        match tokio::net::TcpStream::connect(server).await {
            Ok(mut stream) => {
                let data = vec![0xDE, 0xAD, 0xBE, 0xEF];
                if stream.write_all(&data).await.is_ok() {
                    success += 1;
                } else {
                    fail += 1;
                }
            }
            Err(_) => fail += 1,
        }
    }
    println!("\x1b[32mOK\x1b[0m ({} success, {} fail)", success, fail);

    println!();
    println!("Crash test complete. Server should still be running.");

    Ok(())
}
