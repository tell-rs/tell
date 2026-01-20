//! Syslog TCP source benchmarks

use std::io::{self, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::task::JoinSet;

use crate::common::{
    SyslogFormat, TokioProgressReporter, format_bytes, format_number, generate_syslog_message,
};

/// Type alias for crash test case generators
type CrashTestCase = (&'static str, Box<dyn Fn() -> Vec<u8> + Send + Sync>);

/// Syslog TCP benchmark mode
#[derive(clap::Subcommand)]
pub enum SyslogTcpMode {
    /// Load test - measure throughput
    Load {
        /// Number of clients
        #[arg(short, long, default_value = "5")]
        clients: usize,

        /// Total messages to send
        #[arg(short, long, default_value = "10000000")]
        events: u64,

        /// Server address:port
        #[arg(short, long, default_value = "127.0.0.1:50514")]
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
        #[arg(short, long, default_value = "127.0.0.1:50514")]
        server: String,
    },
    /// Crash test - send malicious/malformed data
    Crash {
        /// Server address:port
        #[arg(short, long, default_value = "127.0.0.1:50514")]
        server: String,
    },
    /// Oversized message test - verify server handles large messages without DoS
    Oversized {
        /// Number of clients
        #[arg(short, long, default_value = "5")]
        clients: usize,

        /// Test duration in seconds
        #[arg(short, long, default_value = "30")]
        duration: u64,

        /// Message size in bytes (should exceed server's max_message_size)
        #[arg(short, long, default_value = "65536")]
        message_size: usize,

        /// Ratio of oversized messages (0.0-1.0, default 1.0 = all oversized)
        #[arg(long, default_value = "1.0")]
        oversized_ratio: f64,

        /// Server address:port
        #[arg(short, long, default_value = "127.0.0.1:50514")]
        server: String,
    },
}

/// Run Syslog TCP benchmark
pub async fn run(mode: SyslogTcpMode) -> Result<(), Box<dyn std::error::Error>> {
    match mode {
        SyslogTcpMode::Load {
            clients,
            events,
            server,
            format,
        } => load_test(&server, clients, events, format).await,
        SyslogTcpMode::Benchmark {
            clients,
            batches_per_size,
            batch_size,
            server,
        } => benchmark(&server, clients, batches_per_size, batch_size).await,
        SyslogTcpMode::Crash { server } => crash_test(&server).await,
        SyslogTcpMode::Oversized {
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
    println!("=== Syslog TCP Load Test ===");
    println!("Server: {}", server);
    println!("Clients: {}", clients);
    println!("Events: {}", format_number(total_events));
    println!("Format: {:?}", format);
    println!();

    let events_sent = Arc::new(AtomicU64::new(0));
    let bytes_sent = Arc::new(AtomicU64::new(0));
    let events_per_client = total_events / clients as u64;

    let start = Instant::now();
    let mut tasks = JoinSet::new();

    let reporter = TokioProgressReporter::start_with_target(
        Arc::clone(&events_sent),
        Arc::clone(&bytes_sent),
        total_events,
    );

    for client_id in 0..clients {
        let server = server.to_string();
        let events_sent = Arc::clone(&events_sent);
        let bytes_sent = Arc::clone(&bytes_sent);

        tasks.spawn(async move {
            if let Err(e) = tcp_client(
                &server,
                client_id,
                events_per_client,
                format,
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
    let rate = total_sent as f64 / elapsed.as_secs_f64();

    println!();
    println!("=== Results ===");
    println!("Total messages: {}", format_number(total_sent));
    println!("Total bytes:    {}", format_bytes(total_bytes));
    println!("Duration:       {:.2}s", elapsed.as_secs_f64());
    println!("Throughput:     {} msg/s", format_number(rate as u64));
    println!(
        "Bandwidth:      {}/s",
        format_bytes((total_bytes as f64 / elapsed.as_secs_f64()) as u64)
    );

    Ok(())
}

async fn tcp_client(
    server: &str,
    client_id: usize,
    events: u64,
    format: SyslogFormat,
    events_sent: &AtomicU64,
    bytes_sent: &AtomicU64,
) -> io::Result<()> {
    let mut stream = TcpStream::connect(server).await?;
    stream.set_nodelay(true)?;

    let batch_size = 500;
    let mut buffer = Vec::with_capacity(batch_size * 256);

    for batch_num in 0..(events / batch_size as u64) {
        buffer.clear();

        for i in 0..batch_size {
            let msg = generate_syslog_message(
                format,
                client_id,
                batch_num * batch_size as u64 + i as u64,
            );
            buffer.extend_from_slice(msg.as_bytes());
            buffer.push(b'\n');
        }

        stream.write_all(&buffer).await?;
        events_sent.fetch_add(batch_size as u64, Ordering::Relaxed);
        bytes_sent.fetch_add(buffer.len() as u64, Ordering::Relaxed);
    }

    stream.flush().await?;
    Ok(())
}

async fn benchmark(
    server: &str,
    clients: usize,
    batches_per_size: usize,
    batch_size: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Syslog TCP Benchmark ===");
    println!("Server: {}", server);
    println!("Clients: {}", clients);
    println!("Batches/size: {}", batches_per_size);
    println!("Batch size: {}", batch_size);
    println!();

    let sizes = [64, 128, 256, 512, 1024, 2048, 4096, 8192];

    println!(
        "{:>8} | {:>12} | {:>10} | {:>12}",
        "Size", "Msg/s", "MB/s", "Total"
    );
    println!("{}", "-".repeat(50));

    for size in sizes {
        let total_events = (batches_per_size * batch_size) as u64;
        let events_sent = Arc::new(AtomicU64::new(0));
        let bytes_sent = Arc::new(AtomicU64::new(0));

        let start = Instant::now();
        let mut tasks = JoinSet::new();
        let events_per_client = total_events / clients as u64;

        for client_id in 0..clients {
            let server = server.to_string();
            let events_sent = Arc::clone(&events_sent);
            let bytes_sent = Arc::clone(&bytes_sent);

            tasks.spawn(async move {
                let _ = sized_client(
                    &server,
                    client_id,
                    events_per_client,
                    size,
                    batch_size,
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
            "{:>7}B | {:>12} | {:>9.1} | {:>12}",
            size,
            format_number(msg_rate as u64),
            mb_rate,
            format_number(total)
        );
    }

    Ok(())
}

async fn sized_client(
    server: &str,
    _client_id: usize,
    events: u64,
    msg_size: usize,
    batch_size: usize,
    events_sent: &AtomicU64,
    bytes_sent: &AtomicU64,
) -> io::Result<()> {
    let mut stream = TcpStream::connect(server).await?;
    stream.set_nodelay(true)?;

    let payload: String = (0..msg_size)
        .map(|i| ((i % 26) as u8 + b'a') as char)
        .collect();
    let msg = format!("<134>Jan 15 12:00:00 host app: {}\n", payload);

    let mut buffer = Vec::with_capacity(batch_size * msg.len());

    for _ in 0..(events / batch_size as u64) {
        buffer.clear();
        for _ in 0..batch_size {
            buffer.extend_from_slice(msg.as_bytes());
        }
        stream.write_all(&buffer).await?;
        events_sent.fetch_add(batch_size as u64, Ordering::Relaxed);
        bytes_sent.fetch_add(buffer.len() as u64, Ordering::Relaxed);
    }

    Ok(())
}

async fn crash_test(server: &str) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Syslog TCP Crash Test ===");
    println!("Server: {}", server);
    println!();

    let tests: Vec<CrashTestCase> = vec![
        ("Empty line", Box::new(|| b"\n".to_vec())),
        ("Only whitespace", Box::new(|| b"   \t\t  \n".to_vec())),
        (
            "No priority",
            Box::new(|| b"This is not valid syslog\n".to_vec()),
        ),
        (
            "Invalid priority",
            Box::new(|| b"<999>Invalid priority\n".to_vec()),
        ),
        (
            "Unclosed priority",
            Box::new(|| b"<134 missing close\n".to_vec()),
        ),
        (
            "Binary garbage",
            Box::new(|| vec![0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0xFF, b'\n']),
        ),
        (
            "Null bytes",
            Box::new(|| b"<134>Jan 1 00:00:00 host \x00\x00\x00\n".to_vec()),
        ),
        (
            "Very long line (16KB)",
            Box::new(|| {
                let mut v = b"<134>Jan 1 00:00:00 host app: ".to_vec();
                v.extend(std::iter::repeat_n(b'X', 16 * 1024));
                v.push(b'\n');
                v
            }),
        ),
        (
            "Very long line (64KB)",
            Box::new(|| {
                let mut v = b"<134>Jan 1 00:00:00 host app: ".to_vec();
                v.extend(std::iter::repeat_n(b'Y', 64 * 1024));
                v.push(b'\n');
                v
            }),
        ),
        (
            "UTF-8 valid",
            Box::new(|| {
                "<134>Jan 1 00:00:00 host app: 你好世界 \n"
                    .as_bytes()
                    .to_vec()
            }),
        ),
        (
            "Invalid UTF-8",
            Box::new(|| vec![b'<', b'1', b'3', b'4', b'>', 0xFF, 0xFE, b'\n']),
        ),
        (
            "CRLF endings",
            Box::new(|| b"<134>Jan 1 00:00:00 host app: CRLF\r\n".to_vec()),
        ),
        (
            "Mixed line endings",
            Box::new(|| b"<134>line1\n<134>line2\r\n<134>line3\n".to_vec()),
        ),
        (
            "Rapid small messages",
            Box::new(|| {
                let mut v = Vec::new();
                for _ in 0..1000 {
                    v.extend_from_slice(b"<134>x\n");
                }
                v
            }),
        ),
    ];

    for (name, generator) in &tests {
        print!("  {:30} ... ", name);
        io::stdout().flush()?;

        match TcpStream::connect(server).await {
            Ok(mut stream) => {
                let data = generator();
                match stream.write_all(&data).await {
                    Ok(()) => {
                        let _ = stream.flush().await;
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        println!("\x1b[32mOK\x1b[0m (sent {} bytes)", data.len());
                    }
                    Err(e) => println!("\x1b[33mWRITE ERR\x1b[0m: {}", e),
                }
            }
            Err(e) => println!("\x1b[31mCONNECT ERR\x1b[0m: {}", e),
        }
    }

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
    println!("=== Syslog TCP Oversized Message Test ===");
    println!("Server: {}", server);
    println!("Clients: {}", clients);
    println!("Duration: {}s", duration_secs);
    println!("Message size: {} bytes", format_number(message_size as u64));
    println!("Oversized ratio: {:.0}%", oversized_ratio * 100.0);
    println!();

    let messages_sent = Arc::new(AtomicU64::new(0));
    let oversized_sent = Arc::new(AtomicU64::new(0));
    let bytes_sent = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let running = Arc::new(AtomicBool::new(true));

    let start = Instant::now();
    let duration = Duration::from_secs(duration_secs);
    let mut tasks = JoinSet::new();

    // Spawn reporter
    let messages_clone = Arc::clone(&messages_sent);
    let oversized_clone = Arc::clone(&oversized_sent);
    let bytes_clone = Arc::clone(&bytes_sent);
    let errors_clone = Arc::clone(&errors);
    let running_clone = Arc::clone(&running);
    tokio::spawn(async move {
        let mut last_messages = 0u64;
        let mut last_oversized = 0u64;
        let mut last_bytes = 0u64;
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.tick().await;

        while running_clone.load(Ordering::Relaxed) {
            interval.tick().await;
            let current = messages_clone.load(Ordering::Relaxed);
            let current_oversized = oversized_clone.load(Ordering::Relaxed);
            let current_bytes = bytes_clone.load(Ordering::Relaxed);
            let current_errors = errors_clone.load(Ordering::Relaxed);

            println!(
                "[progress] {:>8} msg/s | {:>8} oversized/s | {:>8}/s | errors: {}",
                format_number(current - last_messages),
                format_number(current_oversized - last_oversized),
                format_bytes(current_bytes - last_bytes),
                current_errors
            );

            last_messages = current;
            last_oversized = current_oversized;
            last_bytes = current_bytes;
        }
    });

    for client_id in 0..clients {
        let server = server.to_string();
        let messages_sent = Arc::clone(&messages_sent);
        let oversized_sent = Arc::clone(&oversized_sent);
        let bytes_sent = Arc::clone(&bytes_sent);
        let errors = Arc::clone(&errors);
        let running = Arc::clone(&running);

        tasks.spawn(async move {
            let result = oversized_client(
                &server,
                client_id,
                duration,
                message_size,
                oversized_ratio,
                &messages_sent,
                &oversized_sent,
                &bytes_sent,
                &running,
            )
            .await;
            if let Err(e) = result {
                errors.fetch_add(1, Ordering::Relaxed);
                eprintln!("Client {} error: {}", client_id, e);
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
    let total_messages = messages_sent.load(Ordering::Relaxed);
    let total_oversized = oversized_sent.load(Ordering::Relaxed);
    let total_bytes = bytes_sent.load(Ordering::Relaxed);
    let total_errors = errors.load(Ordering::Relaxed);

    println!();
    println!("=== Results ===");
    println!("Total messages:   {}", format_number(total_messages));
    println!(
        "Total oversized:  {} (should trigger messages_malformed)",
        format_number(total_oversized)
    );
    println!("Total bytes:      {}", format_bytes(total_bytes));
    println!("Duration:         {:.2}s", elapsed.as_secs_f64());
    println!(
        "Throughput:       {} msg/s",
        format_number((total_messages as f64 / elapsed.as_secs_f64()) as u64)
    );
    println!("Client errors:    {}", total_errors);

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn oversized_client(
    server: &str,
    _client_id: usize,
    duration: Duration,
    message_size: usize,
    oversized_ratio: f64,
    messages_sent: &AtomicU64,
    oversized_sent: &AtomicU64,
    bytes_sent: &AtomicU64,
    running: &AtomicBool,
) -> io::Result<()> {
    let mut stream = TcpStream::connect(server).await?;
    stream.set_nodelay(true)?;

    let header = "<134>Jan 15 12:00:00 host app: ";
    let oversized_payload: String = (0..message_size)
        .map(|i| ((i % 26) as u8 + b'a') as char)
        .collect();
    let oversized_msg = format!("{}{}\n", header, oversized_payload);
    let normal_payload: String = (0..64).map(|i| ((i % 26) as u8 + b'a') as char).collect();
    let normal_msg = format!("{}{}\n", header, normal_payload);

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

        match stream.write_all(msg.as_bytes()).await {
            Ok(()) => {
                messages_sent.fetch_add(1, Ordering::Relaxed);
                bytes_sent.fetch_add(msg.len() as u64, Ordering::Relaxed);
                if is_oversized {
                    oversized_sent.fetch_add(1, Ordering::Relaxed);
                }
            }
            Err(e) => {
                if e.kind() == io::ErrorKind::BrokenPipe
                    || e.kind() == io::ErrorKind::ConnectionReset
                {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    stream = TcpStream::connect(server).await?;
                    stream.set_nodelay(true)?;
                } else {
                    return Err(e);
                }
            }
        }
    }

    let _ = stream.flush().await;
    Ok(())
}
