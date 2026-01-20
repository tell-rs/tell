//! ClickHouse init command
//!
//! Creates database, tables, users, and permissions for a workspace.
//!
//! # Usage
//!
//! ```bash
//! tell clickhouse init acme
//! tell clickhouse init acme --url http://clickhouse:8123
//! tell clickhouse init acme --dry-run
//! ```

use anyhow::{bail, Context, Result};
use clap::Args;
use owo_colors::OwoColorize;
use std::path::PathBuf;

use super::client::{SchemaClient, DEFAULT_URL};
use super::config::generate_config;
use super::password::generate_password;
use super::schema;

#[derive(Args, Debug)]
pub struct InitArgs {
    /// Workspace name (used as database name and user prefix)
    pub workspace: String,

    /// ClickHouse HTTP URL
    #[arg(long, default_value = DEFAULT_URL)]
    pub url: String,

    /// Collector user password (generated if not provided)
    #[arg(long)]
    pub collector_password: Option<String>,

    /// Dashboard user password (generated if not provided)
    #[arg(long)]
    pub dashboard_password: Option<String>,

    /// Output path for generated config file
    #[arg(long)]
    pub config_output: Option<PathBuf>,

    /// Show what would be created without executing
    #[arg(long)]
    pub dry_run: bool,

    /// Skip confirmation prompt
    #[arg(long)]
    pub yes: bool,
}

pub async fn run(args: InitArgs) -> Result<()> {
    let workspace = &args.workspace;

    // Validate workspace name (alphanumeric + underscore only)
    if !is_valid_workspace_name(workspace) {
        bail!(
            "Invalid workspace name '{}'. Use only letters, numbers, and underscores.",
            workspace
        );
    }

    // Generate passwords if not provided
    let collector_password = args
        .collector_password
        .unwrap_or_else(|| generate_password(32));
    let dashboard_password = args
        .dashboard_password
        .unwrap_or_else(|| generate_password(32));

    // Config output path
    let config_path = args
        .config_output
        .unwrap_or_else(|| PathBuf::from(format!("configs/{workspace}.toml")));

    // Print header
    println!();
    println!(
        "{} {}",
        "Tell ClickHouse Init".bold(),
        format!("(schema v{})", schema::SCHEMA_VERSION).dimmed()
    );
    println!("{}", "─".repeat(50));
    println!("Workspace     {}", workspace.cyan());
    println!("URL           {}", args.url.dimmed());
    println!("Database      {}", workspace.cyan());
    println!(
        "Users         {}_collector, {}_dashboard",
        workspace.cyan(),
        workspace.cyan()
    );
    println!("Config        {}", config_path.display().to_string().dimmed());
    println!("{}", "─".repeat(50));
    println!();

    if args.dry_run {
        print_dry_run(workspace, &collector_password, &dashboard_password);
        return Ok(());
    }

    // Connect to ClickHouse
    print!("Connecting to ClickHouse... ");
    let client = SchemaClient::connect(&args.url).await?;
    println!("{}", "✓".green());

    // Check if database already exists
    if client.database_exists(workspace).await? {
        println!(
            "{}",
            format!("Database '{workspace}' already exists. Use 'tell clickhouse destroy {workspace}' first.").yellow()
        );
        bail!("database already exists");
    }

    // Create database
    print!("Creating database '{}'... ", workspace);
    client.execute(&schema::create_database(workspace)).await?;
    println!("{}", "✓".green());

    // Create tables
    let tables = schema::create_tables(workspace);
    print!("Creating tables ({})... ", tables.len());
    client.execute_all(&tables).await?;
    println!("{}", "✓".green());

    // Create indexes
    let indexes = schema::create_indexes(workspace);
    print!("Creating indexes ({})... ", indexes.len());
    client.execute_all(&indexes).await?;
    println!("{}", "✓".green());

    // Create users
    print!("Creating users... ");
    let users = schema::create_users(workspace, &collector_password, &dashboard_password);
    client.execute_all(&users).await?;
    println!("{}", "✓".green());

    // Grant permissions
    print!("Granting permissions... ");
    let collector_grants = schema::grant_collector(workspace);
    let dashboard_grants = schema::grant_dashboard(workspace);
    client.execute_all(&collector_grants).await?;
    client.execute_all(&dashboard_grants).await?;
    println!("{}", "✓".green());

    // Generate config file
    print!("Writing config... ");
    let config_content = generate_config(
        workspace,
        &args.url,
        &collector_password,
        &dashboard_password,
    );
    if let Some(parent) = config_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create directory {:?}", parent))?;
    }
    std::fs::write(&config_path, config_content)
        .with_context(|| format!("failed to write config to {:?}", config_path))?;
    println!("{}", "✓".green());

    // Success message
    println!();
    println!("{}", "Setup complete!".green().bold());
    println!();
    println!("Credentials (save these):");
    println!("  Collector: {}_collector / {}", workspace, collector_password);
    println!("  Dashboard: {}_dashboard / {}", workspace, dashboard_password);
    println!();
    println!("Next steps:");
    println!(
        "  {} {}",
        "tell --config".dimmed(),
        config_path.display()
    );
    println!("  {}", "tell tail".dimmed());
    println!();

    Ok(())
}

fn is_valid_workspace_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
        && !name.starts_with('_')
        && !name.chars().next().map_or(true, |c| c.is_ascii_digit())
}

fn print_dry_run(workspace: &str, collector_password: &str, dashboard_password: &str) {
    println!("{}", "[DRY RUN] Would execute:".yellow().bold());
    println!();

    println!("-- Create database");
    println!("{}", schema::create_database(workspace));
    println!();

    println!("-- Create tables");
    for sql in schema::create_tables(workspace) {
        println!("{};", sql.lines().next().unwrap_or(&sql));
    }
    println!();

    println!("-- Create indexes");
    for sql in schema::create_indexes(workspace) {
        println!("{};", sql);
    }
    println!();

    println!("-- Create users");
    for sql in schema::create_users(workspace, collector_password, dashboard_password) {
        // Mask password in output
        let masked = sql.replace(collector_password, "***").replace(dashboard_password, "***");
        println!("{};", masked);
    }
    println!();

    println!("-- Grant permissions");
    for sql in schema::grant_collector(workspace) {
        println!("{};", sql);
    }
    for sql in schema::grant_dashboard(workspace) {
        println!("{};", sql);
    }
    println!();
}
