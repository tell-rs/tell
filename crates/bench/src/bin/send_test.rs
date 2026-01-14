//! Simple test sender for cdp-tail testing
//!
//! Sends a few real events and logs to verify cdp-tail output.
//!
//! Usage:
//!   cargo run -p cdp-bench --bin send_test
//!   cargo run -p cdp-bench --bin send_test -- --server 127.0.0.1:50000

use std::time::Duration;

use cdp_client::event::{EventBuilder, EventDataBuilder};
use cdp_client::log::{LogDataBuilder, LogEntryBuilder};
use cdp_client::test::TcpTestClient;
use cdp_client::BatchBuilder;
use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "send_test", about = "Send test events/logs to collector")]
struct Args {
    /// Server address
    #[arg(short, long, default_value = "127.0.0.1:50000")]
    server: String,

    /// API key (hex, 32 chars)
    #[arg(short = 'k', long, default_value = "000102030405060708090a0b0c0d0e0f")]
    api_key: String,

    /// Number of batches to send
    #[arg(short, long, default_value = "3")]
    batches: usize,

    /// Delay between batches (ms)
    #[arg(short, long, default_value = "500")]
    delay: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Parse API key
    let api_key_bytes = hex::decode(&args.api_key)?;
    if api_key_bytes.len() != 16 {
        return Err("API key must be 32 hex characters (16 bytes)".into());
    }
    let mut api_key = [0u8; 16];
    api_key.copy_from_slice(&api_key_bytes);

    println!("Connecting to {}...", args.server);

    let mut client = TcpTestClient::connect(&args.server).await?;
    println!("Connected. Sending {} batches...\n", args.batches);

    for i in 0..args.batches {
        // Send events batch
        send_events(&mut client, api_key, i).await?;
        println!("  Sent event batch {}", i + 1);

        tokio::time::sleep(Duration::from_millis(args.delay / 2)).await;

        // Send logs batch
        send_logs(&mut client, api_key, i).await?;
        println!("  Sent log batch {}", i + 1);

        if i < args.batches - 1 {
            tokio::time::sleep(Duration::from_millis(args.delay)).await;
        }
    }

    client.flush().await?;
    client.close().await?;

    println!("\nDone. Check cdp-tail output.");
    Ok(())
}

async fn send_events(
    client: &mut TcpTestClient,
    api_key: [u8; 16],
    batch_num: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let device_id = [0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4,
                     0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, batch_num as u8];

    // Build a few events
    let event1 = EventBuilder::new()
        .track("page_view")
        .device_id(device_id)
        .timestamp_now()
        .payload_json(&format!(r#"{{"page":"/home","batch":{}}}"#, batch_num))
        .build()?;

    let event2 = EventBuilder::new()
        .track("button_click")
        .device_id(device_id)
        .timestamp_now()
        .payload_json(r#"{"button":"signup","position":"header"}"#)
        .build()?;

    let event3 = EventBuilder::new()
        .identify()
        .device_id(device_id)
        .timestamp_now()
        .payload_json(r#"{"user_id":"user_12345","plan":"pro"}"#)
        .build()?;

    let event_data = EventDataBuilder::new()
        .add(event1)
        .add(event2)
        .add(event3)
        .build()?;

    let batch = BatchBuilder::new()
        .api_key(api_key)
        .event_data(event_data)
        .build()?;

    client.send(&batch).await?;
    Ok(())
}

async fn send_logs(
    client: &mut TcpTestClient,
    api_key: [u8; 16],
    batch_num: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let log1 = LogEntryBuilder::new()
        .info()
        .source("web-01.prod")
        .service("api-gateway")
        .timestamp_now()
        .payload_json(&format!(r#"{{"msg":"request processed","batch":{}}}"#, batch_num))
        .build()?;

    let log2 = LogEntryBuilder::new()
        .warning()
        .source("web-02.prod")
        .service("auth-service")
        .timestamp_now()
        .payload_json(r#"{"msg":"rate limit approaching","current":950,"max":1000}"#)
        .build()?;

    let log3 = LogEntryBuilder::new()
        .error()
        .source("db-01.prod")
        .service("postgres")
        .timestamp_now()
        .payload_json(r#"{"error":"connection timeout","retry":3}"#)
        .build()?;

    let log4 = LogEntryBuilder::new()
        .debug()
        .source("cache-01")
        .service("redis")
        .timestamp_now()
        .payload_json(r#"{"op":"get","key":"session:abc","hit":true}"#)
        .build()?;

    let log_data = LogDataBuilder::new()
        .add(log1)
        .add(log2)
        .add(log3)
        .add(log4)
        .build()?;

    let batch = BatchBuilder::new()
        .api_key(api_key)
        .log_data(log_data)
        .build()?;

    client.send(&batch).await?;
    Ok(())
}
