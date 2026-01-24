//! Auth command - Authenticate with the Tell API
//!
//! # Usage
//!
//! ```bash
//! # Login with email/password
//! tell auth login
//!
//! # Check current auth status
//! tell auth status
//!
//! # Logout (clear stored credentials)
//! tell auth logout
//! ```
//!
//! # Configuration
//!
//! The auth commands read the API server URL from the `[api]` section in config:
//!
//! ```toml
//! [api]
//! url = "http://localhost:3000"
//! ```

use std::fs;
use std::io::{self, Write};
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Args, Subcommand};
use serde::{Deserialize, Serialize};

/// Auth command arguments
#[derive(Args, Debug)]
pub struct AuthArgs {
    #[command(subcommand)]
    pub command: AuthCommand,

    /// Config file path (uses [api] section for server URL)
    #[arg(short, long, global = true)]
    pub config: Option<PathBuf>,
}

#[derive(Subcommand, Debug)]
pub enum AuthCommand {
    /// Login with email and password
    Login,

    /// Logout (clear stored credentials)
    Logout,

    /// Show current authentication status
    Status,
}

/// Stored authentication credentials
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthCredentials {
    /// JWT access token
    pub access_token: String,
    /// API server URL
    pub api_url: String,
    /// User email
    pub email: String,
}

/// Login response from API
#[derive(Debug, Deserialize)]
struct LoginResponse {
    access_token: String,
}

/// User info response from API
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct UserResponse {
    id: String,
    email: String,
    #[serde(default)]
    name: Option<String>,
}

/// Run the auth command (used by subcommand pattern)
#[allow(dead_code)]
pub async fn run(args: AuthArgs) -> Result<()> {
    match args.command {
        AuthCommand::Login => run_login(args.config.as_ref()).await,
        AuthCommand::Logout => run_logout(),
        AuthCommand::Status => run_status(args.config.as_ref()).await,
    }
}

/// Perform login (public for direct CLI access)
pub async fn run_login(config_path: Option<&PathBuf>) -> Result<()> {
    let api_url = get_api_url(config_path)?;

    // Prompt for credentials
    print!("Email: ");
    io::stdout().flush()?;
    let mut email = String::new();
    io::stdin().read_line(&mut email)?;
    let email = email.trim().to_string();

    print!("Password: ");
    io::stdout().flush()?;
    let password = read_password()?;
    println!(); // Newline after password

    // Make login request
    let client = reqwest::Client::new();
    let response = client
        .post(format!("{}/api/v1/auth/login", api_url))
        .json(&serde_json::json!({
            "email": email,
            "password": password,
        }))
        .send()
        .await
        .context("failed to connect to API server")?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        if status.as_u16() == 401 {
            return Err(anyhow::anyhow!("Invalid email or password"));
        }
        return Err(anyhow::anyhow!("Login failed ({}): {}", status, body));
    }

    let login_response: LoginResponse = response
        .json()
        .await
        .context("failed to parse login response")?;

    // Save credentials
    let credentials = AuthCredentials {
        access_token: login_response.access_token,
        api_url: api_url.clone(),
        email: email.clone(),
    };
    save_credentials(&credentials)?;

    println!("Logged in as {}", email);
    Ok(())
}

/// Perform logout (public for direct CLI access)
pub fn run_logout() -> Result<()> {
    let auth_file = auth_file_path()?;
    if auth_file.exists() {
        fs::remove_file(&auth_file).context("failed to remove auth file")?;
        println!("Logged out successfully");
    } else {
        println!("Not logged in");
    }
    Ok(())
}

/// Show current auth status (used by subcommand pattern)
#[allow(dead_code)]
async fn run_status(config_path: Option<&PathBuf>) -> Result<()> {
    let credentials = match load_credentials() {
        Ok(c) => c,
        Err(_) => {
            println!("Not logged in");
            println!("\nRun 'tell auth login' to authenticate");
            return Ok(());
        }
    };

    // Try to verify the token by calling the user endpoint
    let client = reqwest::Client::new();
    let response = client
        .get(format!("{}/api/v1/user/me", credentials.api_url))
        .header(
            "Authorization",
            format!("Bearer {}", credentials.access_token),
        )
        .send()
        .await;

    match response {
        Ok(resp) if resp.status().is_success() => {
            let user: UserResponse = resp.json().await.context("failed to parse user response")?;

            println!("Logged in as: {}", user.email);
            if let Some(name) = user.name {
                println!("Name: {}", name);
            }
            println!("User ID: {}", user.id);
            println!("API server: {}", credentials.api_url);
        }
        Ok(resp) if resp.status().as_u16() == 401 => {
            println!("Session expired. Please run 'tell auth login' to re-authenticate.");
            // Clean up expired credentials
            let _ = fs::remove_file(auth_file_path()?);
        }
        Ok(resp) => {
            println!("Email: {}", credentials.email);
            println!("API server: {}", credentials.api_url);
            println!("Status: Unable to verify token ({})", resp.status());
        }
        Err(e) => {
            println!("Email: {}", credentials.email);
            println!("API server: {}", credentials.api_url);
            println!("Status: Unable to connect to API server");

            // Check if it's a connection error
            let api_url = get_api_url(config_path).ok();
            if api_url.as_ref() != Some(&credentials.api_url) {
                println!("\nNote: Config API URL differs from stored credentials");
                if let Some(url) = api_url {
                    println!("  Config: {}", url);
                    println!("  Stored: {}", credentials.api_url);
                }
            }

            eprintln!("\nError: {}", e);
        }
    }

    Ok(())
}

/// Get the API URL from config file
fn get_api_url(config_path: Option<&PathBuf>) -> Result<String> {
    // Try to load from config file
    let path = if let Some(p) = config_path {
        if p.exists() {
            Some(p.clone())
        } else {
            return Err(anyhow::anyhow!("config file not found: {}", p.display()));
        }
    } else {
        // Try default paths
        let default_paths = ["configs/config.toml", "config.toml"];
        default_paths.iter().map(PathBuf::from).find(|p| p.exists())
    };

    if let Some(path) = path {
        let content = fs::read_to_string(&path)
            .with_context(|| format!("failed to read config file: {}", path.display()))?;

        let toml_value: toml::Value = toml::from_str(&content)
            .with_context(|| format!("failed to parse config file: {}", path.display()))?;

        if let Some(api_section) = toml_value.get("api")
            && let Some(url) = api_section.get("url").and_then(|v| v.as_str())
        {
            return Ok(url.to_string());
        }
    }

    // Default URL
    Ok("http://localhost:3000".to_string())
}

/// Get the path to the auth credentials file
fn auth_file_path() -> Result<PathBuf> {
    let home = dirs::home_dir().context("could not determine home directory")?;
    let tell_dir = home.join(".tell");
    Ok(tell_dir.join("auth.json"))
}

/// Save credentials to the auth file
pub fn save_credentials(credentials: &AuthCredentials) -> Result<()> {
    let auth_file = auth_file_path()?;

    // Create .tell directory if it doesn't exist
    if let Some(parent) = auth_file.parent() {
        fs::create_dir_all(parent).context("failed to create .tell directory")?;
    }

    let json = serde_json::to_string_pretty(credentials)?;
    fs::write(&auth_file, json).context("failed to write auth file")?;

    // Set file permissions to 600 (owner read/write only) on Unix
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&auth_file)?.permissions();
        perms.set_mode(0o600);
        fs::set_permissions(&auth_file, perms)?;
    }

    Ok(())
}

/// Load credentials from the auth file
pub fn load_credentials() -> Result<AuthCredentials> {
    let auth_file = auth_file_path()?;
    let content = fs::read_to_string(&auth_file).context("not logged in")?;
    let credentials: AuthCredentials =
        serde_json::from_str(&content).context("invalid auth file format")?;
    Ok(credentials)
}

/// Clear stored credentials (logout)
pub fn clear_credentials() -> Result<()> {
    let auth_file = auth_file_path()?;
    if auth_file.exists() {
        fs::remove_file(&auth_file).context("failed to remove auth file")?;
    }
    Ok(())
}

/// Read password from terminal (hides input)
fn read_password() -> Result<String> {
    rpassword::read_password().context("failed to read password")
}
