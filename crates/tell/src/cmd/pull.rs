//! Pull command - Fetch snapshots from external connectors
//!
//! Pull data from external sources (GitHub, etc.) and output as JSON.

use anyhow::Result;
use clap::Args;

#[cfg(feature = "connector-github")]
use anyhow::Context;
#[cfg(feature = "connector-github")]
use tell_connectors::{Connector, GitHub, GitHubConfig};

/// Pull command arguments
#[derive(Args, Debug)]
pub struct PullArgs {
    /// Connector name (github)
    #[arg(value_name = "CONNECTOR")]
    connector: String,

    /// Entity to fetch (e.g., owner/repo for github)
    #[arg(value_name = "ENTITY")]
    entity: String,

    /// Authentication token (or set via environment variable)
    #[arg(short, long)]
    token: Option<String>,

    /// Output format: json (default), compact
    #[arg(short, long, default_value = "json")]
    output: String,
}

/// Run the pull command
pub async fn run(args: PullArgs) -> Result<()> {
    match args.connector.to_lowercase().as_str() {
        #[cfg(feature = "connector-github")]
        "github" | "gh" => run_github(args).await,
        other => {
            anyhow::bail!(
                "Unknown connector: {}. Available: {:?}",
                other,
                tell_connectors::available_connectors()
            );
        }
    }
}

/// Run GitHub connector
#[cfg(feature = "connector-github")]
async fn run_github(args: PullArgs) -> Result<()> {
    // Only use token if explicitly provided
    let config = GitHubConfig {
        token: args.token,
        ..Default::default()
    };

    let github = GitHub::new(config)
        .context("failed to create GitHub connector")?;

    tracing::info!(
        connector = "github",
        entity = %args.entity,
        "pulling snapshot"
    );

    let batch = github
        .pull(&args.entity)
        .await
        .context("failed to pull from GitHub")?;

    // Extract and print the payload
    if let Some(msg) = batch.get_message(0) {
        match args.output.as_str() {
            "compact" => {
                // Print without newlines
                print!("{}", String::from_utf8_lossy(msg));
            }
            _ => {
                // Pretty print JSON
                let value: serde_json::Value = serde_json::from_slice(msg)
                    .context("failed to parse snapshot as JSON")?;
                println!("{}", serde_json::to_string_pretty(&value)?);
            }
        }
    } else {
        anyhow::bail!("No snapshot data returned");
    }

    Ok(())
}
