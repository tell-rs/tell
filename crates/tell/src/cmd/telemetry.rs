//! Telemetry command - Show and manage product telemetry
//!
//! Displays exactly what telemetry data would be sent, for transparency.
//!
//! # Usage
//!
//! ```bash
//! # Show what telemetry data would be sent
//! tell telemetry show
//!
//! # JSON output
//! tell telemetry show --json
//! ```

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Args, Subcommand};
use tell_config::Config;
use tell_telemetry::{Collector, ConfigShape, PipelineMetrics, TelemetryPayload};

/// Telemetry command arguments
#[derive(Args, Debug)]
pub struct TelemetryArgs {
    #[command(subcommand)]
    pub command: TelemetryCommand,
}

#[derive(Subcommand, Debug)]
pub enum TelemetryCommand {
    /// Show what telemetry data would be sent
    Show(ShowArgs),
}

#[derive(Args, Debug)]
pub struct ShowArgs {
    /// Path to config file
    #[arg(short, long)]
    pub config: Option<PathBuf>,

    /// Output as JSON
    #[arg(long)]
    pub json: bool,
}

/// Run the telemetry command
pub async fn run(args: TelemetryArgs) -> Result<()> {
    match args.command {
        TelemetryCommand::Show(show_args) => run_show(show_args).await,
    }
}

async fn run_show(args: ShowArgs) -> Result<()> {
    // Load config to determine what sources/sinks are enabled
    let config = load_config(args.config.as_ref())?;

    // Load or generate install ID
    let install_id_path = tell_telemetry::default_install_id_path();
    let install_id = match Collector::load_or_create_install_id(&install_id_path) {
        Ok(id) => id,
        Err(_) => Collector::generate_install_id(),
    };

    // Create collector
    let collector = Collector::new(install_id);

    // Build config shape
    let config_shape = build_config_shape(&config);

    // Collect payload (with zeroed runtime metrics since server isn't running)
    let payload = collector.collect(config_shape, PipelineMetrics::default());

    if args.json {
        println!("{}", serde_json::to_string_pretty(&payload)?);
    } else {
        print_payload(&payload, &config);
    }

    Ok(())
}

fn load_config(path: Option<&PathBuf>) -> Result<Config> {
    match path {
        Some(p) => Config::from_file(p).context("failed to load config"),
        None => {
            // Try default paths
            let default_paths = [
                PathBuf::from("configs/config.toml"),
                PathBuf::from("config.toml"),
            ];

            for p in &default_paths {
                if p.exists() {
                    return Config::from_file(p).context("failed to load config");
                }
            }

            Ok(Config::default())
        }
    }
}

fn build_config_shape(config: &Config) -> ConfigShape {
    let mut sources = Vec::new();
    if !config.sources.tcp.is_empty() {
        sources.push("tcp".to_string());
    }
    if config.sources.tcp_debug.is_some() {
        sources.push("tcp_debug".to_string());
    }
    if config.sources.syslog_tcp.is_some() {
        sources.push("syslog_tcp".to_string());
    }
    if config.sources.syslog_udp.is_some() {
        sources.push("syslog_udp".to_string());
    }
    if config.sources.http.is_some() {
        sources.push("http".to_string());
    }

    let sinks: Vec<String> = config
        .sinks
        .iter()
        .map(|(_, sink)| sink.type_name().to_string())
        .collect();

    let connectors: Vec<String> = config
        .connectors
        .iter()
        .filter(|(_, c)| c.is_enabled())
        .map(|(_, c)| c.connector_type.clone())
        .collect();

    let mut transformers = Vec::new();
    for rule in &config.routing.rules {
        for t in &rule.transformers {
            if !transformers.contains(&t.transformer_type) {
                transformers.push(t.transformer_type.clone());
            }
        }
    }

    ConfigShape {
        sources,
        sinks,
        connectors,
        transformers,
        routing_rules: config.routing.rules.len() as u32,
    }
}

fn print_payload(payload: &TelemetryPayload, config: &Config) {
    println!("Tell Telemetry");
    println!("==============");
    println!();

    // Status
    if config.telemetry.enabled {
        println!(
            "Status:   ENABLED (TCP: {}, HTTP fallback: {})",
            tell_telemetry::endpoint::tcp_addr(),
            tell_telemetry::endpoint::https_url()
        );
        println!("Interval: {}d", config.telemetry.interval.as_secs() / 86400);
    } else {
        println!("Status:   DISABLED (no data will be sent)");
    }
    println!();

    // Deployment
    println!("Deployment:");
    println!("  Install ID: {}", payload.deployment.install_id);
    println!("  Version:    {}", payload.deployment.version);
    println!(
        "  Platform:   {}-{}",
        payload.deployment.os, payload.deployment.arch
    );
    println!("  CPU cores:  {}", payload.deployment.cpu_cores);
    println!(
        "  Memory:     {:.1} GB",
        payload.deployment.memory_bytes as f64 / 1_000_000_000.0
    );
    println!();

    // Configuration
    println!("Configuration:");
    println!(
        "  Sources:      {}",
        if payload.config.sources.is_empty() {
            "(none)".to_string()
        } else {
            payload.config.sources.join(", ")
        }
    );
    println!(
        "  Sinks:        {}",
        if payload.config.sinks.is_empty() {
            "(none)".to_string()
        } else {
            payload.config.sinks.join(", ")
        }
    );
    println!(
        "  Connectors:   {}",
        if payload.config.connectors.is_empty() {
            "(none)".to_string()
        } else {
            payload.config.connectors.join(", ")
        }
    );
    println!(
        "  Transformers: {}",
        if payload.config.transformers.is_empty() {
            "(none)".to_string()
        } else {
            payload.config.transformers.join(", ")
        }
    );
    println!("  Routing rules: {}", payload.config.routing_rules);
    println!();

    // Runtime metrics (will be zeros when not running)
    println!("Runtime Metrics (current session):");
    println!("  Messages:    {}", payload.metrics.messages_total);
    println!("  Bytes:       {}", payload.metrics.bytes_total);
    println!("  Uptime:      {}s", payload.metrics.uptime_secs);
    println!("  Errors:      {}", payload.metrics.errors_total);
    println!("  Connections: {}", payload.metrics.connections_active);
    println!();

    // Features
    println!("Feature Usage:");
    println!("  Tail used:     {}", payload.features.tail_used);
    println!("  Query used:    {}", payload.features.query_used);
    println!("  TUI used:      {}", payload.features.tui_used);
    println!("  Boards:        {}", payload.features.boards_count);
    println!("  Metrics:       {}", payload.features.metrics_count);
    println!("  API keys:      {}", payload.features.api_keys_count);
    println!("  Workspaces:    {}", payload.features.workspaces_count);
    println!();

    // Footer
    println!("To disable telemetry, add to your config:");
    println!("  [telemetry]");
    println!("  enabled = false");
}
