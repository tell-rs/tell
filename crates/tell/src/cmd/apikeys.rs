//! API Keys management command
//!
//! List, create, show, and delete TCP API keys for SDK ingestion.
//!
//! # Usage
//!
//! ```bash
//! tell apikeys                    # List all keys (truncated)
//! tell apikeys new <name>         # Create new key
//! tell apikeys show <name>        # Show full key
//! tell apikeys delete <name>      # Delete key
//! ```

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Args, Subcommand};
use tell_auth::{ApiKeyStore, WorkspaceId};

/// API Keys command arguments
#[derive(Args, Debug)]
pub struct ApiKeysArgs {
    #[command(subcommand)]
    pub command: Option<ApiKeysCommand>,

    /// Config file path (uses [global] section for api_keys_file path)
    #[arg(short, long, global = true)]
    pub config: Option<PathBuf>,
}

#[derive(Subcommand, Debug)]
pub enum ApiKeysCommand {
    /// Create a new API key
    New {
        /// Name for the key (e.g., "mobile-prod", "backend")
        name: String,

        /// Workspace ID (default: 1)
        #[arg(short, long, default_value = "1")]
        workspace: u32,
    },

    /// Show full API key (by name)
    Show {
        /// Name of the key to show
        name: String,
    },

    /// Delete an API key
    Delete {
        /// Name of the key to delete
        name: String,

        /// Skip confirmation
        #[arg(short, long)]
        yes: bool,
    },
}

/// Run the apikeys command
pub async fn run(args: ApiKeysArgs) -> Result<()> {
    let keys_path = get_keys_path(args.config.as_ref())?;

    match args.command {
        None => list_keys(&keys_path),
        Some(ApiKeysCommand::New { name, workspace }) => create_key(&keys_path, &name, workspace),
        Some(ApiKeysCommand::Show { name }) => show_key(&keys_path, &name),
        Some(ApiKeysCommand::Delete { name, yes }) => delete_key(&keys_path, &name, yes),
    }
}

/// Get the path to the API keys file from config
fn get_keys_path(config_path: Option<&PathBuf>) -> Result<PathBuf> {
    // Try to load from config file
    if let Some(path) = config_path
        && path.exists()
    {
        let config = tell_config::Config::from_file(path).context("failed to load config")?;
        return Ok(PathBuf::from(&config.global.api_keys_file));
    }

    // Try default config paths
    let default_paths = ["configs/config.toml", "config.toml"];
    for path in default_paths {
        if std::path::Path::new(path).exists()
            && let Ok(config) = tell_config::Config::from_file(path)
        {
            return Ok(PathBuf::from(&config.global.api_keys_file));
        }
    }

    // Default path
    Ok(PathBuf::from("configs/apikeys.conf"))
}

/// List all API keys (with truncated key values)
fn list_keys(keys_path: &PathBuf) -> Result<()> {
    if !keys_path.exists() {
        println!("No API keys file found at: {}", keys_path.display());
        println!("\nCreate a new key with: tell apikeys new <name>");
        return Ok(());
    }

    let store = ApiKeyStore::from_file(keys_path).context("failed to load API keys")?;

    let keys = store.list();

    if keys.is_empty() {
        println!("No API keys configured.");
        println!("\nCreate a new key with: tell apikeys new <name>");
        return Ok(());
    }

    println!("API Keys:\n");
    println!("  {:<20} {:<12} {:<10}", "Name", "Key", "Workspace");
    println!("  {}", "-".repeat(44));

    for (hex_key, workspace, name) in keys {
        let display_name = name.as_deref().unwrap_or("(unnamed)");
        let truncated = format!("{}...", &hex_key[..8]);
        println!(
            "  {:<20} {:<12} {}",
            display_name,
            truncated,
            workspace.as_u32()
        );
    }

    println!("\nShow full key: tell apikeys show <name>");

    Ok(())
}

/// Create a new API key
fn create_key(keys_path: &PathBuf, name: &str, workspace: u32) -> Result<()> {
    // Load existing keys or create empty store
    let store = if keys_path.exists() {
        ApiKeyStore::from_file(keys_path).context("failed to load API keys")?
    } else {
        // Create parent directory if needed
        if let Some(parent) = keys_path.parent() {
            std::fs::create_dir_all(parent).context("failed to create keys directory")?;
        }
        ApiKeyStore::new()
    };

    // Check if name already exists
    if store.name_exists(name) {
        anyhow::bail!("API key with name '{}' already exists", name);
    }

    // Generate new key
    let workspace_id = WorkspaceId::new(workspace);
    let hex_key = store.generate(workspace_id, name.to_string());

    // Save to file
    store
        .save_to_file(keys_path)
        .context("failed to save API keys")?;

    println!("Created API key: {}\n", name);
    println!("  {}\n", hex_key);
    println!("View anytime: tell apikeys show {}", name);

    Ok(())
}

/// Show full API key by name
fn show_key(keys_path: &PathBuf, name: &str) -> Result<()> {
    if !keys_path.exists() {
        anyhow::bail!("No API keys file found at: {}", keys_path.display());
    }

    let store = ApiKeyStore::from_file(keys_path).context("failed to load API keys")?;

    match store.find_by_name(name) {
        Some(hex_key) => {
            println!("API Key: {}\n", name);
            println!("  {}", hex_key);
        }
        None => {
            anyhow::bail!("No API key found with name '{}'", name);
        }
    }

    Ok(())
}

/// Delete an API key by name
fn delete_key(keys_path: &PathBuf, name: &str, skip_confirm: bool) -> Result<()> {
    if !keys_path.exists() {
        anyhow::bail!("No API keys file found at: {}", keys_path.display());
    }

    let store = ApiKeyStore::from_file(keys_path).context("failed to load API keys")?;

    // Check if key exists
    if !store.name_exists(name) {
        anyhow::bail!("No API key found with name '{}'", name);
    }

    // Confirm deletion
    if !skip_confirm {
        print!("Delete API key '{}'? This cannot be undone. [y/N] ", name);
        std::io::Write::flush(&mut std::io::stdout())?;

        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;

        if !input.trim().eq_ignore_ascii_case("y") {
            println!("Cancelled.");
            return Ok(());
        }
    }

    // Remove key
    store.remove_by_name(name);

    // Save to file
    store
        .save_to_file(keys_path)
        .context("failed to save API keys")?;

    println!("Deleted API key: {}", name);

    Ok(())
}
