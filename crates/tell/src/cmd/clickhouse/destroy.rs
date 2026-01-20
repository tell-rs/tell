//! ClickHouse destroy command
//!
//! Removes database, tables, and users for a workspace.
//!
//! # Usage
//!
//! ```bash
//! tell clickhouse destroy acme
//! tell clickhouse destroy acme --yes  # skip confirmation
//! ```

use anyhow::{bail, Result};
use clap::Args;
use owo_colors::OwoColorize;
use std::io::{self, Write};

use super::client::{SchemaClient, DEFAULT_URL};
use super::schema;

#[derive(Args, Debug)]
pub struct DestroyArgs {
    /// Workspace name to destroy
    pub workspace: String,

    /// ClickHouse HTTP URL
    #[arg(long, default_value = DEFAULT_URL)]
    pub url: String,

    /// Skip confirmation prompt
    #[arg(long)]
    pub yes: bool,

    /// Show what would be destroyed without executing
    #[arg(long)]
    pub dry_run: bool,
}

pub async fn run(args: DestroyArgs) -> Result<()> {
    let workspace = &args.workspace;

    println!();
    println!("{}", "Tell ClickHouse Destroy".bold().red());
    println!("{}", "─".repeat(50));
    println!("Workspace     {}", workspace.cyan());
    println!("URL           {}", args.url.dimmed());
    println!("{}", "─".repeat(50));
    println!();

    println!(
        "{}",
        "WARNING: This will permanently delete:".yellow().bold()
    );
    println!("  - Database: {}", workspace);
    println!("  - All tables and data in {}", workspace);
    println!("  - User: {}_collector", workspace);
    println!("  - User: {}_dashboard", workspace);
    println!();

    if args.dry_run {
        println!("{}", "[DRY RUN] Would execute:".yellow().bold());
        println!();
        for sql in schema::drop_users(workspace) {
            println!("{};", sql);
        }
        println!("{};", schema::drop_database(workspace));
        println!();
        return Ok(());
    }

    // Confirmation
    if !args.yes {
        print!(
            "Type '{}' to confirm: ",
            workspace.red().bold()
        );
        io::stdout().flush()?;

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;

        if input.trim() != workspace {
            println!();
            println!("{}", "Aborted.".yellow());
            return Ok(());
        }
    }

    println!();

    // Connect to ClickHouse
    print!("Connecting to ClickHouse... ");
    let client = SchemaClient::connect(&args.url).await?;
    println!("{}", "✓".green());

    // Check if database exists
    if !client.database_exists(workspace).await? {
        println!(
            "{}",
            format!("Database '{workspace}' does not exist.").yellow()
        );
        bail!("database does not exist");
    }

    // Drop users first (before database, as they might have grants)
    print!("Dropping users... ");
    client.execute_all(&schema::drop_users(workspace)).await?;
    println!("{}", "✓".green());

    // Drop database (cascades to all tables)
    print!("Dropping database '{}'... ", workspace);
    client.execute(&schema::drop_database(workspace)).await?;
    println!("{}", "✓".green());

    println!();
    println!(
        "{}",
        format!("Workspace '{}' destroyed.", workspace)
            .green()
            .bold()
    );
    println!();

    Ok(())
}
