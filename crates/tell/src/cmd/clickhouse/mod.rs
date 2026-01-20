//! ClickHouse management commands
//!
//! Commands for setting up and managing ClickHouse schema for Tell workspaces.
//!
//! # Quick Start
//!
//! ```bash
//! tell clickhouse init acme                      # create workspace, writes configs/acme.toml
//! tell --config configs/acme.toml                # run Tell with the new config
//! ```
//!
//! # Commands
//!
//! ```bash
//! # Initialize a new workspace (creates database, tables, users, config)
//! tell clickhouse init acme
//! tell clickhouse init acme --dry-run              # preview SQL without executing
//! tell clickhouse init acme --url http://ch:8123   # custom ClickHouse URL
//!
//! # Check existing workspace setup
//! tell clickhouse check acme
//!
//! # Remove a workspace (requires confirmation)
//! tell clickhouse destroy acme
//! tell clickhouse destroy acme --yes               # skip confirmation
//! ```
//!
//! # What `init` Creates
//!
//! - **Database**: `<workspace>` (e.g., `acme`)
//! - **Tables**: events_v1, context_v1, users_v1, user_devices, user_traits,
//!   logs_v1, pattern_registry, pattern_groups, snapshots_v1
//! - **Users**: `<workspace>_collector` (INSERT), `<workspace>_dashboard` (SELECT)
//! - **Config**: `configs/<workspace>.toml` with generated credentials
//!
//! Passwords are randomly generated unless provided via `--collector-password`
//! and `--dashboard-password`.

mod check;
mod client;
mod config;
mod destroy;
mod init;
mod password;
mod schema;

use anyhow::Result;
use clap::{Args, Subcommand};

pub use check::CheckArgs;
pub use destroy::DestroyArgs;
pub use init::InitArgs;

/// ClickHouse management commands
#[derive(Args, Debug)]
pub struct ClickHouseArgs {
    #[command(subcommand)]
    pub command: ClickHouseCommand,
}

#[derive(Subcommand, Debug)]
pub enum ClickHouseCommand {
    /// Initialize a new workspace (database, tables, users)
    Init(InitArgs),

    /// Check if workspace is properly set up
    Check(CheckArgs),

    /// Destroy a workspace (removes all data!)
    Destroy(DestroyArgs),
}

pub async fn run(args: ClickHouseArgs) -> Result<()> {
    match args.command {
        ClickHouseCommand::Init(args) => init::run(args).await,
        ClickHouseCommand::Check(args) => check::run(args).await,
        ClickHouseCommand::Destroy(args) => destroy::run(args).await,
    }
}
