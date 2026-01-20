//! ClickHouse check command
//!
//! Validates that a workspace schema is properly set up.
//!
//! # Usage
//!
//! ```bash
//! tell clickhouse check acme
//! tell clickhouse check acme --url http://clickhouse:8123
//! ```

use anyhow::Result;
use clap::Args;
use owo_colors::OwoColorize;

use super::client::{DEFAULT_URL, SchemaClient};
use super::schema;

#[derive(Args, Debug)]
pub struct CheckArgs {
    /// Workspace name to check
    pub workspace: String,

    /// ClickHouse HTTP URL
    #[arg(long, default_value = DEFAULT_URL)]
    pub url: String,
}

pub async fn run(args: CheckArgs) -> Result<()> {
    let workspace = &args.workspace;

    println!();
    println!("{}", "Tell ClickHouse Check".bold());
    println!("{}", "─".repeat(50));
    println!("Workspace     {}", workspace.cyan());
    println!("URL           {}", args.url.dimmed());
    println!("{}", "─".repeat(50));
    println!();

    // Connect to ClickHouse
    print!("Connecting to ClickHouse... ");
    let client = match SchemaClient::connect(&args.url).await {
        Ok(c) => {
            println!("{}", "✓".green());
            c
        }
        Err(e) => {
            println!("{}", "✗".red());
            println!("  {}", e.to_string().red());
            return Err(e);
        }
    };

    // Get version
    if let Ok(version) = client.version().await {
        println!("  Version: {}", version.dimmed());
    }

    let mut all_ok = true;

    // Check database
    print!("Database '{}'... ", workspace);
    if client.database_exists(workspace).await? {
        println!("{}", "✓".green());
    } else {
        println!("{}", "✗ not found".red());
        all_ok = false;
    }

    // Check tables
    println!("Tables:");
    for table in schema::TABLE_NAMES {
        print!("  {}... ", table);
        if client.table_exists(workspace, table).await? {
            println!("{}", "✓".green());
        } else {
            println!("{}", "✗ not found".red());
            all_ok = false;
        }
    }

    // Check users
    println!("Users:");
    let collector_user = format!("{}_collector", workspace);
    let dashboard_user = format!("{}_dashboard", workspace);

    print!("  {}... ", collector_user);
    if client.user_exists(&collector_user).await? {
        println!("{}", "✓".green());
    } else {
        println!("{}", "✗ not found".red());
        all_ok = false;
    }

    print!("  {}... ", dashboard_user);
    if client.user_exists(&dashboard_user).await? {
        println!("{}", "✓".green());
    } else {
        println!("{}", "✗ not found".red());
        all_ok = false;
    }

    println!();

    if all_ok {
        println!("{}", "All checks passed!".green().bold());
    } else {
        println!(
            "{}",
            "Some checks failed. Run 'tell clickhouse init' to set up.".yellow()
        );
    }

    println!();

    Ok(())
}
