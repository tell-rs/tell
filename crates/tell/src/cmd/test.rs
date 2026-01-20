//! Test command - send sample events and logs to verify pipeline
//!
//! Simple tool to send a few test messages to verify the server is working.
//!
//! # Usage
//!
//! ```bash
//! # Defaults: localhost:50000, 3 events, 3 logs
//! tell test
//!
//! # Custom server
//! tell test --server 192.168.1.100:50000
//!
//! # More events
//! tell test --events 10 --logs 5
//! ```

use anyhow::{Context, Result};
use clap::Args;
use std::time::Duration;

use tell_client::batch::BatchBuilder;
use tell_client::event::{EventBuilder, EventDataBuilder};
use tell_client::log::{LogDataBuilder, LogEntryBuilder};
use tell_client::test::TcpTestClient;

/// Default server address
const DEFAULT_SERVER: &str = "127.0.0.1:50000";

/// Default API key (matches typical test configs)
const DEFAULT_API_KEY: [u8; 16] = [
    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
];

#[derive(Args, Debug)]
pub struct TestArgs {
    /// Server address
    #[arg(short, long, default_value = DEFAULT_SERVER)]
    server: String,

    /// Number of events to send
    #[arg(short, long, default_value = "3")]
    events: usize,

    /// Number of logs to send
    #[arg(short, long, default_value = "3")]
    logs: usize,

    /// API key in hex (32 chars)
    #[arg(short = 'k', long)]
    api_key: Option<String>,

    /// Quiet mode - don't print details
    #[arg(short, long)]
    quiet: bool,
}

pub async fn run(args: TestArgs) -> Result<()> {
    let api_key = parse_api_key(args.api_key.as_deref())?;

    if !args.quiet {
        let key_hex: String = api_key.iter().map(|b| format!("{:02x}", b)).collect();
        println!("Connecting to {} (api_key: {})...", args.server, key_hex);
    }

    let mut client = TcpTestClient::connect(&args.server)
        .await
        .with_context(|| format!("failed to connect to {}", args.server))?;

    if !args.quiet {
        println!(
            "Connected. Sending {} events and {} logs...\n",
            args.events, args.logs
        );
    }

    // Send events
    for i in 0..args.events {
        let batch = build_test_event(i, &api_key)?;
        client.send(&batch).await.context("failed to send event")?;

        if !args.quiet {
            println!("  [EVENT {}] track: test_event_{}", i + 1, i);
        }
    }

    // Send logs
    for i in 0..args.logs {
        let batch = build_test_log(i, &api_key)?;
        client.send(&batch).await.context("failed to send log")?;

        if !args.quiet {
            let level = match i % 3 {
                0 => "INFO",
                1 => "WARNING",
                _ => "ERROR",
            };
            println!("  [LOG {}] level: {}, service: tell-test", i + 1, level);
        }
    }

    client.flush().await.context("failed to flush")?;

    // Small delay to ensure server processes before we close
    tokio::time::sleep(Duration::from_millis(100)).await;

    client.close().await.context("failed to close connection")?;

    if !args.quiet {
        println!(
            "\nDone. Sent {} events and {} logs.",
            args.events, args.logs
        );
        println!("Check your sinks (stdout, disk, etc.) or use 'tell tail' to verify.");
    }

    Ok(())
}

fn build_test_event(index: usize, api_key: &[u8; 16]) -> Result<tell_client::batch::BuiltBatch> {
    let device_id: [u8; 16] = {
        let mut id = [0u8; 16];
        id[15] = index as u8;
        id
    };

    let event = EventBuilder::new()
        .track(&format!("test_event_{}", index))
        .device_id(device_id)
        .timestamp_now()
        .payload_json(&format!(
            r#"{{"index": {}, "test": true, "source": "tell test"}}"#,
            index
        ))
        .build()
        .context("failed to build event")?;

    let event_data = EventDataBuilder::new()
        .add(event)
        .build()
        .context("failed to build event data")?;

    BatchBuilder::new()
        .api_key(*api_key)
        .event_data(event_data)
        .build()
        .context("failed to build batch")
}

fn build_test_log(index: usize, api_key: &[u8; 16]) -> Result<tell_client::batch::BuiltBatch> {
    let log = match index % 3 {
        0 => LogEntryBuilder::new().info(),
        1 => LogEntryBuilder::new().warning(),
        _ => LogEntryBuilder::new().error(),
    }
    .source("localhost")
    .service("tell-test")
    .timestamp_now()
    .payload_json(&format!(
        r#"{{"message": "Test log message {}", "index": {}, "source": "tell test"}}"#,
        index, index
    ))
    .build()
    .context("failed to build log")?;

    let log_data = LogDataBuilder::new()
        .add(log)
        .build()
        .context("failed to build log data")?;

    BatchBuilder::new()
        .api_key(*api_key)
        .log_data(log_data)
        .build()
        .context("failed to build batch")
}

fn parse_api_key(hex: Option<&str>) -> Result<[u8; 16]> {
    match hex {
        Some(s) => {
            let s = s.trim_start_matches("0x");
            if s.len() != 32 {
                anyhow::bail!("API key must be 32 hex characters (16 bytes)");
            }
            let mut key = [0u8; 16];
            for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
                let hex_str = std::str::from_utf8(chunk)?;
                key[i] = u8::from_str_radix(hex_str, 16)
                    .with_context(|| format!("invalid hex at position {}", i * 2))?;
            }
            Ok(key)
        }
        None => Ok(DEFAULT_API_KEY),
    }
}
