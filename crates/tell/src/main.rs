//! Tell - Analytics that tell the whole story
//!
//! # Usage
//!
//! ```bash
//! # Open TUI (default)
//! tell
//! tell --config configs/config.toml
//!
//! # Run the server (daemon mode)
//! tell run
//! tell run --config configs/config.toml
//!
//! # Stream live data from a running server
//! tell tail
//! tell tail --workspace 1 --type event
//! ```

mod cmd;
mod transformer_builder;
mod tui;

use anyhow::Result;
use clap::{Parser, Subcommand};
use tell_config::Config;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

/// Analytics that tell the whole story
#[derive(Parser, Debug)]
#[command(name = "tell")]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,

    // Global args that apply to serve when no subcommand given
    /// Path to configuration file (error if specified but not found)
    #[arg(short, long, global = true)]
    config: Option<std::path::PathBuf>,

    /// Log level (trace, debug, info, warn, error). Overrides config file.
    #[arg(short, long, global = true)]
    log_level: Option<String>,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Run the server (daemon mode)
    Run(cmd::serve::ServeArgs),

    /// Stream live data from a running server (Unix only)
    #[cfg(unix)]
    Tail(cmd::tail::TailArgs),

    /// Pull snapshot from external connector (GitHub, etc.)
    Pull(cmd::pull::PullArgs),

    /// Execute SQL queries against analytics data
    Query(cmd::query::QueryArgs),

    /// Query analytics metrics (DAU, WAU, MAU, events, logs)
    Metrics(cmd::metrics::MetricsArgs),

    /// Read disk binary files
    Read(cmd::read::ReadArgs),

    /// Send test events and logs to verify pipeline
    Test(cmd::test::TestArgs),

    /// Check server status and metrics
    Status(cmd::status::StatusArgs),

    /// ClickHouse schema management
    Clickhouse(cmd::clickhouse::ClickHouseArgs),

    /// Authenticate with the Tell API server
    Auth(cmd::auth::AuthArgs),
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        // Run server in daemon mode
        Some(Command::Run(mut args)) => {
            // CLI global --config overrides subcommand config if both specified
            if args.config.is_none() && cli.config.is_some() {
                args.config = cli.config;
            }
            let log_level = resolve_log_level(cli.log_level.as_deref(), args.config.as_deref());
            init_logging(&log_level)?;
            cmd::serve::run(args).await
        }
        #[cfg(unix)]
        Some(Command::Tail(args)) => {
            // Tail initializes its own logging
            cmd::tail::run(args).await
        }
        Some(Command::Pull(args)) => {
            let log_level = resolve_log_level(cli.log_level.as_deref(), cli.config.as_deref());
            init_logging(&log_level)?;
            cmd::pull::run(args).await
        }
        Some(Command::Query(args)) => {
            // Query doesn't need logging - just outputs to stdout
            cmd::query::run(args).await
        }
        Some(Command::Metrics(args)) => {
            // Metrics doesn't need logging - just outputs to stdout
            cmd::metrics::run(args).await
        }
        Some(Command::Read(args)) => {
            // Read doesn't need logging - just outputs to stdout
            cmd::read::run(args).await
        }
        Some(Command::Test(args)) => {
            // Test doesn't need logging - just outputs to stdout
            cmd::test::run(args).await
        }
        Some(Command::Status(args)) => {
            // Status doesn't need logging - just outputs to stdout
            cmd::status::run(args).await
        }
        Some(Command::Clickhouse(args)) => {
            // ClickHouse commands don't need logging - just outputs to stdout
            cmd::clickhouse::run(args).await
        }
        Some(Command::Auth(args)) => {
            // Auth commands don't need logging - just outputs to stdout
            cmd::auth::run(args).await
        }
        // No subcommand = open TUI (default behavior)
        None => {
            // TUI doesn't need logging (uses alternate screen)
            let args = cmd::interactive::InteractiveArgs { config: cli.config };
            cmd::interactive::run(args).await
        }
    }
}

/// Resolve log level: CLI flag > config file > default "info"
fn resolve_log_level(cli_level: Option<&str>, config_path: Option<&std::path::Path>) -> String {
    // CLI flag takes precedence
    if let Some(level) = cli_level {
        return level.to_string();
    }

    // Try to load from config file if specified
    if let Some(path) = config_path
        && path.exists()
        && let Ok(config) = Config::from_file(path)
    {
        return config.log.level.as_str().to_string();
    }

    // Default
    "info".to_string()
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
