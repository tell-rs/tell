//! Tail command - Live streaming CLI for Tell
//!
//! Connect to a running Tell server and stream batches in real-time.

mod client;
mod filter;
mod output;

use std::path::PathBuf;

use anyhow::Result;
use clap::Args;
use tracing_subscriber::EnvFilter;

use tell_protocol::{FlatBatch, LogLevel, decode_event_data, decode_log_data};
use tell_tap::{SubscribeRequest, TapEnvelope, TapMessage};

use filter::ContentFilter;

/// Tail command arguments
#[derive(Args, Debug)]
pub struct TailArgs {
    /// Socket path to connect to
    #[arg(short, long, default_value = "/tmp/tell-tap.sock")]
    socket: PathBuf,

    /// Filter by workspace ID (can be repeated)
    #[arg(short, long = "workspace", value_name = "ID")]
    workspaces: Vec<u32>,

    /// Filter by source ID (can be repeated)
    #[arg(long = "source", value_name = "NAME")]
    sources: Vec<String>,

    /// Filter by batch type: event, log, syslog, metric, trace
    #[arg(short = 't', long = "type", value_name = "TYPE")]
    types: Vec<String>,

    /// Sample rate (0.0 - 1.0, e.g., 0.01 = 1%)
    #[arg(long, value_name = "RATE")]
    sample: Option<f32>,

    /// Max batches per second
    #[arg(long = "rate-limit", value_name = "N")]
    rate_limit: Option<u32>,

    /// Replay last N batches on connect
    #[arg(long = "last", value_name = "N", default_value = "0")]
    last_n: u32,

    // Content filters (client-side, decoded)
    /// Filter by event name (glob pattern, can be repeated)
    #[arg(short = 'e', long = "event", value_name = "PATTERN")]
    event_names: Vec<String>,

    /// Filter by log level: error, warning, info, debug, trace (can be repeated)
    #[arg(short = 'l', long = "level", value_name = "LEVEL")]
    log_levels: Vec<String>,

    /// Filter by regex pattern (matches any string field)
    #[arg(short = 'm', long = "match", value_name = "REGEX")]
    pattern: Option<String>,

    /// Output format: text (default), json, compact, raw
    #[arg(short = 'o', long = "output", default_value = "text")]
    format: String,

    /// Show metadata only (no payload decode)
    #[arg(long)]
    metadata_only: bool,

    /// Disable colored output
    #[arg(long)]
    no_color: bool,

    /// Verbose output (show debug info)
    #[arg(short, long)]
    verbose: bool,

    /// Quiet mode (suppress connection messages)
    #[arg(short, long)]
    quiet: bool,

    /// Print demo output to test colors (doesn't connect)
    #[arg(long)]
    demo: bool,
}

/// Run the tail command
pub async fn run(args: TailArgs) -> Result<()> {
    // Set up logging for tail command
    let filter = if args.verbose {
        EnvFilter::new("debug")
    } else if args.quiet {
        EnvFilter::new("error")
    } else {
        EnvFilter::new("info")
    };

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_ansi(atty::is(atty::Stream::Stderr))
        .init();

    // Build subscribe request
    let mut request = SubscribeRequest::new().with_last_n(args.last_n);

    if !args.workspaces.is_empty() {
        request = request.with_workspaces(args.workspaces.clone());
    }

    if !args.sources.is_empty() {
        request = request.with_sources(args.sources.clone());
    }

    if !args.types.is_empty() {
        let type_ids: Vec<u8> = args
            .types
            .iter()
            .filter_map(|t| parse_batch_type(t))
            .collect();
        if !type_ids.is_empty() {
            request = request.with_types(type_ids);
        }
    }

    if let Some(rate) = args.sample {
        request = request.with_sample_rate(rate);
    }

    if let Some(limit) = args.rate_limit {
        request = request.with_rate_limit(limit);
    }

    // Create output formatter
    // Enable color only if: stdout is TTY AND --no-color not set
    let use_color = atty::is(atty::Stream::Stdout) && !args.no_color;
    let formatter = output::Formatter::new(&args.format, args.metadata_only).with_color(use_color);

    // Demo mode - print sample output and exit
    if args.demo {
        print_demo(use_color);
        return Ok(());
    }

    // Build content filter
    let content_filter = build_content_filter(&args);

    // Connect and stream
    if !args.quiet {
        tracing::info!(
            socket = %args.socket.display(),
            "connecting to server"
        );
    }

    let mut client = client::TapClient::connect(&args.socket).await?;

    if !args.quiet {
        tracing::info!("connected, subscribing...");
    }

    client.subscribe(&request).await?;

    if !args.quiet {
        tracing::info!("streaming batches (Ctrl+C to stop)");
    }

    // Main loop with signal handling
    loop {
        tokio::select! {
            result = client.recv() => {
                match result {
                    Ok(Some(msg)) => {
                        match msg {
                            TapMessage::Batch(envelope) => {
                                // Apply content filter if needed
                                if content_filter.needs_decode()
                                    && !filter_batch(&envelope, &content_filter)
                                {
                                    continue; // Skip batch, nothing matched
                                }
                                formatter.print(&envelope);
                            }
                            TapMessage::Heartbeat => {
                                if args.verbose {
                                    tracing::debug!("heartbeat");
                                }
                            }
                            TapMessage::Error(e) => {
                                tracing::error!(error = %e, "server error");
                            }
                            TapMessage::Subscribe(_) => {
                                // Server shouldn't send this
                            }
                        }
                    }
                    Ok(None) => {
                        if !args.quiet {
                            tracing::info!("connection closed");
                        }
                        break;
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "receive error");
                        break;
                    }
                }
            }
            _ = tokio::signal::ctrl_c() => {
                if !args.quiet {
                    tracing::info!("interrupted, shutting down");
                }
                break;
            }
        }
    }

    Ok(())
}

/// Build content filter from CLI arguments
fn build_content_filter(args: &TailArgs) -> ContentFilter {
    let mut filter = ContentFilter::new();

    if !args.event_names.is_empty() {
        let patterns: Vec<&str> = args.event_names.iter().map(|s| s.as_str()).collect();
        filter = filter.with_event_names(patterns);
    }

    if !args.log_levels.is_empty() {
        let levels: Vec<LogLevel> = args
            .log_levels
            .iter()
            .filter_map(|s| parse_log_level(s))
            .collect();
        filter = filter.with_log_levels(levels);
    }

    if let Some(ref text) = args.pattern {
        filter = filter.with_substring(text);
    }

    filter
}

/// Check if any message in the batch matches the content filter
fn filter_batch(envelope: &TapEnvelope, filter: &ContentFilter) -> bool {
    match envelope.batch_type {
        0 => {
            // Event batch - check each message
            for i in 0..envelope.offsets.len() {
                if let Some(data) = extract_inner_data(envelope, i)
                    && let Ok(events) = decode_event_data(data)
                    && events.iter().any(|e| filter.matches_event(e))
                {
                    return true;
                }
            }
            false
        }
        1 => {
            // Log batch - check each message
            for i in 0..envelope.offsets.len() {
                if let Some(data) = extract_inner_data(envelope, i)
                    && let Ok(logs) = decode_log_data(data)
                    && logs.iter().any(|l| filter.matches_log(l))
                {
                    return true;
                }
            }
            false
        }
        _ => true, // Unknown types pass through
    }
}

/// Extract inner data from a message in the envelope
fn extract_inner_data(envelope: &TapEnvelope, index: usize) -> Option<&[u8]> {
    if index >= envelope.offsets.len() {
        return None;
    }
    let start = envelope.offsets[index] as usize;
    let len = envelope.lengths[index] as usize;
    if start + len > envelope.payload.len() {
        return None;
    }
    let msg = &envelope.payload[start..start + len];

    // Parse outer FlatBatch and extract inner data
    let flat_batch = FlatBatch::parse(msg).ok()?;
    flat_batch.data().ok()
}

/// Parse batch type string to u8
fn parse_batch_type(s: &str) -> Option<u8> {
    match s.to_lowercase().as_str() {
        "event" | "events" | "e" => Some(0),
        "log" | "logs" | "l" => Some(1),
        "syslog" | "s" => Some(2),
        "metric" | "metrics" | "m" => Some(3),
        "trace" | "traces" | "t" => Some(4),
        _ => {
            tracing::warn!(type_name = %s, "unknown batch type, ignoring");
            None
        }
    }
}

/// Parse log level string to LogLevel
fn parse_log_level(s: &str) -> Option<LogLevel> {
    match s.to_lowercase().as_str() {
        "fatal" | "critical" | "crit" | "emergency" | "emerg" | "alert" => Some(LogLevel::Fatal),
        "error" | "err" => Some(LogLevel::Error),
        "warning" | "warn" => Some(LogLevel::Warning),
        "info" | "notice" => Some(LogLevel::Info),
        "debug" => Some(LogLevel::Debug),
        "trace" => Some(LogLevel::Trace),
        _ => {
            tracing::warn!(level = %s, "unknown log level, ignoring");
            None
        }
    }
}

/// Print demo output to test colors
fn print_demo(use_color: bool) {
    use owo_colors::{OwoColorize, Style};

    let (dim, normal, yellow, red) = if use_color {
        (
            Style::new().dimmed(),
            Style::new(),
            Style::new().yellow(),
            Style::new().red(),
        )
    } else {
        (Style::new(), Style::new(), Style::new(), Style::new())
    };

    println!(
        "demo output (color={}){}",
        use_color,
        if use_color {
            ""
        } else {
            " - use TTY or remove --no-color"
        }
    );
    println!();

    // Sample events - now with ws, source, ip
    println!(
        "{} {} {} {} {} {} page_view {} {}",
        "14:23:01.234".style(dim),
        "ws:1".style(dim),
        "tcp_main".style(dim),
        "192.168.1.100".style(dim),
        "event".style(dim),
        "track".style(normal),
        "dev=550e8400".style(dim),
        r#"{"page":"/home"}"#.style(dim)
    );
    println!(
        "{} {} {} {} {} {} user_signup {} {}",
        "14:23:01.567".style(dim),
        "ws:1".style(dim),
        "tcp_main".style(dim),
        "192.168.1.100".style(dim),
        "event".style(dim),
        "identify".style(normal),
        "dev=a1b2c3d4".style(dim),
        r#"{"user_id":"12345"}"#.style(dim)
    );
    println!();

    // Sample logs - with ws, source, ip
    println!(
        "{} {} {} {} {} {} api-gateway@web-01 {}",
        "14:23:02.001".style(dim),
        "ws:1".style(dim),
        "tcp_main".style(dim),
        "10.0.0.50".style(dim),
        "log".style(dim),
        "error  ".style(red),
        r#"{"error":"timeout"}"#.style(dim)
    );
    println!(
        "{} {} {} {} {} {} auth-svc@web-02 {}",
        "14:23:02.123".style(dim),
        "ws:1".style(dim),
        "tcp_main".style(dim),
        "10.0.0.51".style(dim),
        "log".style(dim),
        "warning".style(yellow),
        r#"{"msg":"rate limit"}"#.style(dim)
    );
    println!(
        "{} {} {} {} {} {} worker@runner {}",
        "14:23:02.456".style(dim),
        "ws:2".style(dim),
        "tcp_logs".style(dim),
        "10.0.0.52".style(dim),
        "log".style(dim),
        "info   ".style(normal),
        r#"{"job":"batch"}"#.style(dim)
    );
}
