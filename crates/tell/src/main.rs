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

const HELP_TEMPLATE: &str = "\
{before-help}{name} {version}
{about}

{usage-heading} {usage}

{all-args}{after-help}";

/// Analytics that tell the whole story
#[derive(Parser, Debug)]
#[command(name = "tell")]
#[command(version, about, long_about = None)]
#[command(help_template = HELP_TEMPLATE)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,

    /// Path to config file
    #[arg(short, long, global = true)]
    config: Option<std::path::PathBuf>,

    /// Log level (trace/debug/info/warn/error)
    #[arg(short, long, global = true)]
    log_level: Option<String>,

    /// Print help for all commands (including advanced)
    #[arg(long)]
    help_all: bool,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Start server
    Run(cmd::serve::ServeArgs),

    /// Server health and metrics
    Status(cmd::status::StatusArgs),

    /// DAU, MAU, retention, top events (--json for piping)
    Metrics(cmd::metrics::MetricsArgs),

    /// Stream live data (Unix only)
    #[cfg(unix)]
    Tail(cmd::tail::TailArgs),

    /// Authenticate
    Login,

    /// Sign out
    Logout,

    /// Manage TCP API keys for SDK ingestion
    #[command(name = "apikeys")]
    ApiKeys(cmd::apikeys::ApiKeysArgs),

    /// Manage license key
    License(cmd::license::LicenseArgs),

    /// Execute raw SQL queries
    #[command(hide = true)]
    Query(cmd::query::QueryArgs),

    /// Pull from external connector (GitHub, etc.)
    #[command(hide = true)]
    Pull(cmd::pull::PullArgs),

    /// Read disk binary files
    #[command(hide = true)]
    Read(cmd::read::ReadArgs),

    /// Send test events to verify pipeline
    #[command(hide = true)]
    Test(cmd::test::TestArgs),

    /// ClickHouse workspace setup (init, check, destroy)
    #[command(name = "setup-db", hide = true)]
    SetupDb(cmd::clickhouse::ClickHouseArgs),

    /// Show and manage telemetry settings
    #[command(hide = true)]
    Telemetry(cmd::telemetry::TelemetryArgs),
}

#[tokio::main]
async fn main() -> Result<()> {
    // Intercept top-level help before clap parses
    let args: Vec<String> = std::env::args().collect();
    if args.len() == 2 && (args[1] == "--help" || args[1] == "-h") {
        print_help(false);
        return Ok(());
    }
    if args.len() == 2 && args[1] == "--help-all" {
        print_help(true);
        return Ok(());
    }

    let cli = Cli::parse();

    if cli.help_all {
        print_help(true);
        return Ok(());
    }

    match cli.command {
        // === Server ===
        Some(Command::Run(mut args)) => {
            if args.config.is_none() && cli.config.is_some() {
                args.config = cli.config;
            }
            let log_level = resolve_log_level(cli.log_level.as_deref(), args.config.as_deref());
            init_logging(&log_level)?;
            cmd::serve::run(args).await
        }
        Some(Command::Status(args)) => cmd::status::run(args).await,

        // === Insights ===
        Some(Command::Metrics(args)) => cmd::metrics::run(args).await,
        #[cfg(unix)]
        Some(Command::Tail(args)) => cmd::tail::run(args).await,

        // === Setup ===
        Some(Command::Login) => cmd::auth::run_login(cli.config.as_ref()).await,
        Some(Command::Logout) => cmd::auth::run_logout(),
        Some(Command::ApiKeys(args)) => cmd::apikeys::run(args).await,
        Some(Command::License(args)) => cmd::license::run(args).await,

        // === Advanced ===
        Some(Command::Query(args)) => cmd::query::run(args).await,
        Some(Command::Pull(args)) => {
            let log_level = resolve_log_level(cli.log_level.as_deref(), cli.config.as_deref());
            init_logging(&log_level)?;
            cmd::pull::run(args).await
        }
        Some(Command::Read(args)) => cmd::read::run(args).await,
        Some(Command::Test(args)) => cmd::test::run(args).await,
        Some(Command::SetupDb(args)) => cmd::clickhouse::run(args).await,
        Some(Command::Telemetry(args)) => cmd::telemetry::run(args).await,

        // No subcommand = open TUI
        None => {
            let args = cmd::interactive::InteractiveArgs { config: cli.config };
            cmd::interactive::run(args).await
        }
    }
}

/// Print help with grouped commands
fn print_help(show_advanced: bool) {
    let version = env!("CARGO_PKG_VERSION");
    println!("tell {version}");
    println!("Analytics that tell the whole story\n");
    println!("Usage: tell [OPTIONS] [COMMAND]\n");
    println!("Server:");
    println!("  run         Start server");
    println!("  status      Server health and metrics\n");
    println!("Data:");
    println!("  metrics     DAU, MAU, retention, top events (--json for piping)");
    #[cfg(unix)]
    println!("  tail        Stream live data\n");
    #[cfg(not(unix))]
    println!();
    println!("Auth:");
    println!("  login       Authenticate");
    println!("  logout      Sign out");
    println!("  apikeys     Manage TCP API keys for SDK ingestion");
    println!("  license     Manage license key\n");
    if show_advanced {
        println!("Advanced:");
        println!("  query       Execute raw SQL queries");
        println!("  pull        Pull from external connector (GitHub, etc.)");
        println!("  read        Read disk binary files");
        println!("  test        Send test events to verify pipeline");
        println!("  setup-db    ClickHouse workspace setup (init, check, destroy)");
        println!("  telemetry   Show and manage telemetry settings\n");
    }
    println!("Options:");
    println!("  -c, --config <CONFIG>  Path to config file");
    println!("  -l, --log-level <LOG_LEVEL>  Log level (trace/debug/info/warn/error)");
    println!("  -h, --help             Print help");
    println!("      --help-all         Print help for all commands (including advanced)");
    println!("  -V, --version          Print version\n");
    println!("Run 'tell' for interactive mode");
    if !show_advanced {
        println!("Run 'tell --help-all' for all commands");
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
