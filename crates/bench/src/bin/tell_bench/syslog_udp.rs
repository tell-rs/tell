//! Syslog UDP source benchmarks

use std::io::{self, Write};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::net::UdpSocket;
use tokio::task::JoinSet;

use crate::common::{
    format_bytes, format_number, generate_syslog_message, SyslogFormat, TokioProgressReporter,
};

/// Syslog UDP benchmark mode
#[derive(clap::Subcommand)]
pub enum SyslogUdpMode {
    /// Load test - measure throughput
    Load {
        /// Number of clients
        #[arg(short, long, default_value = "5")]
        clients: usize,

        /// Total messages to send
        #[arg(short, long, default_value = "10000000")]
        events: u64,

        /// Server address:port
        #[arg(short, long, default_value = "127.0.0.1:50515")]
        server: String,

        /// Syslog format
        #[arg(short, long, default_value = "rfc3164")]
        format: SyslogFormat,
    },
    /// Benchmark - test different message sizes
    Benchmark {
        /// Number of clients
        #[arg(short, long, default_value = "5")]
        clients: usize,

        /// Batches per size
        #[arg(long, default_value = "10000")]
        batches_per_size: usize,

        /// Messages per batch
        #[arg(long, default_value = "500")]
        batch_size: usize,

        /// Server address:port
        #[arg(short, long, default_value = "127.0.0.1:50515")]
        server: String,
    },
    /// Crash test - send malicious/malformed data
    Crash {
        /// Server address:port
        #[arg(short, long, default_value = "127.0.0.1:50515")]
        server: String,
    },
    /// Oversized packet test - verify server handles large packets without DoS
    Oversized {
        /// Number of clients
        #[arg(short, long, default_value = "5")]
        clients: usize,

        /// Test duration in seconds
        #[arg(short, long, default_value = "30")]
        duration: u64,

        /// Packet size in bytes (should exceed server's max_message_size)
        #[arg(short, long, default_value = "65536")]
        message_size: usize,

        /// Ratio of oversized packets (0.0-1.0, default 1.0 = all oversized)
        #[arg(long, default_value = "1.0")]
        oversized_ratio: f64,

        /// Server address:port
        #[arg(short, long, default_value = "127.0.0.1:50515")]
        server: String,
    },
}

/// Run Syslog UDP benchmark
pub async fn run(mode: SyslogUdpMode) -> Result<(), Box<dyn std::error::Error>> {
    match mode {
        SyslogUdpMode::Load {
            clients,
            events,
            server,
            format,
        } => load_test(&server, clients, events, format).await,
        SyslogUdpMode::Benchmark {
            clients,
            batches_per_size,
            batch_size,
            server,
        } => benchmark(&server, clients, batches_per_size, batch_size).await,
        SyslogUdpMode::Crash { server } => crash_test(&server).await,
        SyslogUdpMode::Oversized {
            clients,
            duration,
            message_size,
            oversized_ratio,
            server,
        } => oversized_test(&server, clients, duration, message_size, oversized_ratio).await,
    }
}

async fn load_test(
    server: &str,
    clients: usize,
    total_events: u64,
    format: SyslogFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Syslog UDP Load Test ===");
    println!("Server: {}", server);
    println!("Clients: {}", clients);
    println!("Events: {}", format_number(total_events));
    println!("Format: {:?}", format);
    println!();

    let events_sent = Arc::new(AtomicU64::new(0));
    let bytes_sent = Arc::new(AtomicU64::new(0));
    let events_per_client = total_events / clients as u64;

    let server_addr: SocketAddr = server.parse()?;

    let start = Instant::now();
    let mut tasks = JoinSet::new();

    let reporter = TokioProgressReporter::start_with_target(
        Arc::clone(&events_sent),
        Arc::clone(&bytes_sent),
        total_events,
    );

    for client_id in 0..clients {
        let events_sent = Arc::clone(&events_sent);
        let bytes_sent = Arc::clone(&bytes_sent);

        tasks.spawn(async move {
            if let Err(e) =
                udp_client(server_addr, client_id, events_per_client, format, &events_sent, &bytes_sent)
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
    let rate = total_sent as f64 / elapsed.as_secs_f64();

    println!();
    println!("=== Results ===");
    println!("Total packets:  {}", format_number(total_sent));
    println!("Total bytes:    {}", format_bytes(total_bytes));
    println!("Duration:       {:.2}s", elapsed.as_secs_f64());
    println!("Throughput:     {} pkt/s", format_number(rate as u64));
    println!(
        "Bandwidth:      {}/s",
        format_bytes((total_bytes as f64 / elapsed.as_secs_f64()) as u64)
    );

    Ok(())
}

async fn udp_client(
    server: SocketAddr,
    client_id: usize,
    events: u64,
    format: SyslogFormat,
    events_sent: &AtomicU64,
    bytes_sent: &AtomicU64,
) -> io::Result<()> {
    let socket = UdpSocket::bind("0.0.0.0:0").await?;
    socket.connect(server).await?;

    for i in 0..events {
        let msg = generate_syslog_message(format, client_id, i);
        let bytes = msg.as_bytes();
        socket.send(bytes).await?;
        events_sent.fetch_add(1, Ordering::Relaxed);
        bytes_sent.fetch_add(bytes.len() as u64, Ordering::Relaxed);
    }

    Ok(())
}

async fn benchmark(
    server: &str,
    clients: usize,
    batches_per_size: usize,
    batch_size: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Syslog UDP Benchmark ===");
    println!("Server: {}", server);
    println!("Clients: {}", clients);
    println!("Batches/size: {}", batches_per_size);
    println!("Batch size: {}", batch_size);
    println!();

    let sizes = [64, 128, 256, 512, 1024, 1400];

    println!(
        "{:>8} | {:>12} | {:>10} | {:>12}",
        "Size", "Pkt/s", "MB/s", "Total"
    );
    println!("{}", "-".repeat(50));

    let server_addr: SocketAddr = server.parse()?;

    for size in sizes {
        let total_events = (batches_per_size * batch_size) as u64;
        let events_sent = Arc::new(AtomicU64::new(0));
        let bytes_sent = Arc::new(AtomicU64::new(0));

        let start = Instant::now();
        let mut tasks = JoinSet::new();
        let events_per_client = total_events / clients as u64;

        for _client_id in 0..clients {
            let events_sent = Arc::clone(&events_sent);
            let bytes_sent = Arc::clone(&bytes_sent);

            tasks.spawn(async move {
                let _ = sized_client(server_addr, events_per_client, size, &events_sent, &bytes_sent)
                    .await;
            });
        }

        while tasks.join_next().await.is_some() {}

        let elapsed = start.elapsed();
        let total = events_sent.load(Ordering::Relaxed);
        let total_bytes = bytes_sent.load(Ordering::Relaxed);
        let pkt_rate = total as f64 / elapsed.as_secs_f64();
        let mb_rate = (total_bytes as f64 / elapsed.as_secs_f64()) / 1_000_000.0;

        println!(
            "{:>7}B | {:>12} | {:>9.1} | {:>12}",
            size,
            format_number(pkt_rate as u64),
            mb_rate,
            format_number(total)
        );
    }

    Ok(())
}

async fn sized_client(
    server: SocketAddr,
    events: u64,
    msg_size: usize,
    events_sent: &AtomicU64,
    bytes_sent: &AtomicU64,
) -> io::Result<()> {
    let socket = UdpSocket::bind("0.0.0.0:0").await?;
    socket.connect(server).await?;

    let header = "<134>Jan 15 12:00:00 host app: ";
    let payload_size = msg_size.saturating_sub(header.len());
    let payload: String = (0..payload_size)
        .map(|i| ((i % 26) as u8 + b'a') as char)
        .collect();
    let msg = format!("{}{}", header, payload);

    for _ in 0..events {
        socket.send(msg.as_bytes()).await?;
        events_sent.fetch_add(1, Ordering::Relaxed);
        bytes_sent.fetch_add(msg.len() as u64, Ordering::Relaxed);
    }

    Ok(())
}

async fn crash_test(server: &str) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Syslog UDP Crash Test ===");
    println!("Server: {}", server);
    println!();

    let server_addr: SocketAddr = server.parse()?;
    let socket = UdpSocket::bind("0.0.0.0:0").await?;

    let tests: Vec<(&str, Vec<u8>)> = vec![
        ("Empty packet", vec![]),
        ("Single byte", vec![b'x']),
        ("Only whitespace", b"   \t\t  ".to_vec()),
        ("No priority", b"This is not valid syslog".to_vec()),
        ("Invalid priority", b"<999>Invalid priority".to_vec()),
        ("Unclosed priority", b"<134 missing close".to_vec()),
        ("Binary garbage", vec![0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0xFF]),
        ("Null bytes", b"<134>Jan 1 00:00:00 host \x00\x00\x00".to_vec()),
        ("Max UDP size (64KB)", {
            let mut v = b"<134>Jan 1 00:00:00 host app: ".to_vec();
            v.extend(std::iter::repeat(b'X').take(65000));
            v
        }),
        (
            "UTF-8 valid",
            "<134>Jan 1 00:00:00 host app: 你好世界 ".as_bytes().to_vec(),
        ),
        ("Invalid UTF-8", vec![b'<', b'1', b'3', b'4', b'>', 0xFF, 0xFE]),
        ("Trailing newline", b"<134>Jan 1 00:00:00 host app: msg\n".to_vec()),
        ("CRLF", b"<134>Jan 1 00:00:00 host app: msg\r\n".to_vec()),
    ];

    for (name, data) in &tests {
        print!("  {:30} ... ", name);
        io::stdout().flush()?;

        match socket.send_to(data, server_addr).await {
            Ok(sent) => {
                tokio::time::sleep(Duration::from_millis(5)).await;
                println!("\x1b[32mOK\x1b[0m (sent {} bytes)", sent);
            }
            Err(e) => println!("\x1b[31mERR\x1b[0m: {}", e),
        }
    }

    print!("  {:30} ... ", "Rapid fire (10000 pkts)");
    io::stdout().flush()?;
    let msg = b"<134>Jan 1 00:00:00 host app: rapid";
    for _ in 0..10000 {
        let _ = socket.send_to(msg, server_addr).await;
    }
    println!("\x1b[32mOK\x1b[0m");

    println!();
    println!("Crash test complete. Server should still be running.");

    Ok(())
}

async fn oversized_test(
    server: &str,
    clients: usize,
    duration_secs: u64,
    message_size: usize,
    oversized_ratio: f64,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Syslog UDP Oversized Packet Test ===");
    println!("Server: {}", server);
    println!("Clients: {}", clients);
    println!("Duration: {}s", duration_secs);
    println!("Packet size: {} bytes", format_number(message_size as u64));
    println!("Oversized ratio: {:.0}%", oversized_ratio * 100.0);
    println!();
    println!("Note: UDP packets > 64KB may be fragmented or dropped by the OS.");
    println!();

    let packets_sent = Arc::new(AtomicU64::new(0));
    let oversized_sent = Arc::new(AtomicU64::new(0));
    let bytes_sent = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let running = Arc::new(AtomicBool::new(true));

    let server_addr: SocketAddr = server.parse()?;

    let start = Instant::now();
    let duration = Duration::from_secs(duration_secs);
    let mut tasks = JoinSet::new();

    // Spawn reporter
    let packets_clone = Arc::clone(&packets_sent);
    let oversized_clone = Arc::clone(&oversized_sent);
    let bytes_clone = Arc::clone(&bytes_sent);
    let errors_clone = Arc::clone(&errors);
    let running_clone = Arc::clone(&running);
    tokio::spawn(async move {
        let mut last_packets = 0u64;
        let mut last_oversized = 0u64;
        let mut last_bytes = 0u64;
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.tick().await;

        while running_clone.load(Ordering::Relaxed) {
            interval.tick().await;
            let current = packets_clone.load(Ordering::Relaxed);
            let current_oversized = oversized_clone.load(Ordering::Relaxed);
            let current_bytes = bytes_clone.load(Ordering::Relaxed);
            let current_errors = errors_clone.load(Ordering::Relaxed);

            println!(
                "[progress] {:>8} pkt/s | {:>8} oversized/s | {:>8}/s | errors: {}",
                format_number(current - last_packets),
                format_number(current_oversized - last_oversized),
                format_bytes(current_bytes - last_bytes),
                current_errors
            );

            last_packets = current;
            last_oversized = current_oversized;
            last_bytes = current_bytes;
        }
    });

    for _client_id in 0..clients {
        let packets_sent = Arc::clone(&packets_sent);
        let oversized_sent = Arc::clone(&oversized_sent);
        let bytes_sent = Arc::clone(&bytes_sent);
        let errors = Arc::clone(&errors);
        let running = Arc::clone(&running);

        tasks.spawn(async move {
            let result = oversized_client(
                server_addr,
                duration,
                message_size,
                oversized_ratio,
                &packets_sent,
                &oversized_sent,
                &bytes_sent,
                &running,
            )
            .await;
            if result.is_err() {
                errors.fetch_add(1, Ordering::Relaxed);
            }
        });
    }

    tokio::time::sleep(duration).await;
    running.store(false, Ordering::Relaxed);

    while let Some(result) = tasks.join_next().await {
        if let Err(e) = result {
            eprintln!("Task error: {}", e);
        }
    }

    let elapsed = start.elapsed();
    let total_packets = packets_sent.load(Ordering::Relaxed);
    let total_oversized = oversized_sent.load(Ordering::Relaxed);
    let total_bytes = bytes_sent.load(Ordering::Relaxed);
    let total_errors = errors.load(Ordering::Relaxed);

    println!();
    println!("=== Results ===");
    println!("Total packets:    {}", format_number(total_packets));
    println!(
        "Total oversized:  {} (should trigger messages_malformed)",
        format_number(total_oversized)
    );
    println!("Total bytes:      {}", format_bytes(total_bytes));
    println!("Duration:         {:.2}s", elapsed.as_secs_f64());
    println!(
        "Throughput:       {} pkt/s",
        format_number((total_packets as f64 / elapsed.as_secs_f64()) as u64)
    );
    println!("Send errors:      {}", total_errors);

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn oversized_client(
    server: SocketAddr,
    duration: Duration,
    message_size: usize,
    oversized_ratio: f64,
    packets_sent: &AtomicU64,
    oversized_sent: &AtomicU64,
    bytes_sent: &AtomicU64,
    running: &AtomicBool,
) -> io::Result<()> {
    let socket = UdpSocket::bind("0.0.0.0:0").await?;
    socket.connect(server).await?;

    let header = "<134>Jan 15 12:00:00 host app: ";
    let oversized_payload: String = (0..message_size)
        .map(|i| ((i % 26) as u8 + b'a') as char)
        .collect();
    let oversized_msg = format!("{}{}", header, oversized_payload);
    let normal_payload: String = (0..64).map(|i| ((i % 26) as u8 + b'a') as char).collect();
    let normal_msg = format!("{}{}", header, normal_payload);

    let start = Instant::now();
    let mut rng_state: u64 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;

    while running.load(Ordering::Relaxed) && start.elapsed() < duration {
        rng_state = rng_state.wrapping_mul(6364136223846793005).wrapping_add(1);
        let rand_val = (rng_state >> 33) as f64 / (u32::MAX as f64);

        let (msg, is_oversized) = if rand_val < oversized_ratio {
            (&oversized_msg, true)
        } else {
            (&normal_msg, false)
        };

        // UDP send may fail for large packets
        if socket.send(msg.as_bytes()).await.is_ok() {
            packets_sent.fetch_add(1, Ordering::Relaxed);
            bytes_sent.fetch_add(msg.len() as u64, Ordering::Relaxed);
            if is_oversized {
                oversized_sent.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    Ok(())
}
