//! Collector - High-performance data streaming engine
//!
//! # Usage
//!
//! ```bash
//! # Run the server (default)
//! collector
//! collector --config configs/config.toml
//!
//! # Stream live data from a running collector
//! collector tail
//! collector tail --workspace 1 --type event
//! ```

mod cmd;
mod transformer_builder;

use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

/// Collector - High-performance data streaming engine
#[derive(Parser, Debug)]
#[command(name = "collector")]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,

    // Global args that apply to serve when no subcommand given
    /// Path to configuration file
    #[arg(short, long, default_value = "configs/config.toml", global = true)]
    config: std::path::PathBuf,

    /// Log level (trace, debug, info, warn, error)
    #[arg(short, long, default_value = "info", global = true)]
    log_level: String,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Run the collector server
    Serve(cmd::serve::ServeArgs),

    /// Stream live data from a running collector
    Tail(cmd::tail::TailArgs),

    /// Pull snapshot from external connector (GitHub, etc.)
    Pull(cmd::pull::PullArgs),
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        // Explicit subcommand
        Some(Command::Serve(args)) => {
            init_logging(&args.log_level)?;
            cmd::serve::run(args).await
        }
        Some(Command::Tail(args)) => {
            // Tail initializes its own logging
            cmd::tail::run(args).await
        }
        Some(Command::Pull(args)) => {
            init_logging(&cli.log_level)?;
            cmd::pull::run(args).await
        }
        // No subcommand = run server (default behavior)
        None => {
            init_logging(&cli.log_level)?;
            let args = cmd::serve::ServeArgs {
                config: cli.config,
                log_level: cli.log_level,
            };
            cmd::serve::run(args).await
        }
    }
}

/// Initialize the tracing subscriber for logging
fn init_logging(level: &str) -> Result<()> {
    let filter = EnvFilter::try_new(level)
        .or_else(|_| EnvFilter::try_new("info"))
        .map_err(|e| anyhow::anyhow!("invalid log level: {}", e))?;

    tracing_subscriber::registry()
        .with(fmt::layer().with_target(true).with_thread_ids(false))
        .with(filter)
        .init();

    Ok(())
}
