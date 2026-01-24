//! License management commands.
//!
//! Provides CLI commands for activating, viewing, and removing licenses.

use anyhow::{Context, Result};
use clap::{Args, Subcommand};

use tell_license::{
    LICENSE_ENV_VAR, LicenseState, activate_license, deactivate_license, default_license_path,
    load_license,
};

/// License management arguments
#[derive(Args, Debug)]
pub struct LicenseArgs {
    #[command(subcommand)]
    command: LicenseCommand,
}

#[derive(Subcommand, Debug)]
enum LicenseCommand {
    /// Show current license status
    Show,

    /// Activate a license key
    Activate {
        /// License key to activate
        key: String,
    },

    /// Remove the current license
    Deactivate,
}

/// Run the license command
pub async fn run(args: LicenseArgs) -> Result<()> {
    match args.command {
        LicenseCommand::Show => show_license(),
        LicenseCommand::Activate { key } => activate(&key),
        LicenseCommand::Deactivate => deactivate(),
    }
}

/// Show current license status
fn show_license() -> Result<()> {
    let state = load_license(None);

    println!("License Status");
    println!("==============\n");

    match &state {
        LicenseState::Free => {
            println!("Tier:     Tell (Free)");
            println!("Status:   No license key configured");
            println!("\nFree tier is for companies with revenue < $1M/year.");
            println!("Purchase a license at https://tell.rs/pricing");
        }
        LicenseState::Licensed(lic) => {
            println!("Tier:     {}", lic.tier().display_name());
            println!("Customer: {}", lic.customer_name());
            println!("Expires:  {}", lic.payload.expires);
            println!("Issued:   {}", lic.payload.issued);

            let days = lic.payload.days_until_expiry();
            if days <= 30 {
                println!(
                    "\n\x1b[33mWarning: License expires in {} days!\x1b[0m",
                    days
                );
                println!("Renew at https://tell.rs/renew");
            } else {
                println!("\nStatus:   Valid ({} days remaining)", days);
            }
        }
        LicenseState::Expired(lic) => {
            println!("Tier:     {}", lic.tier().display_name());
            println!("Customer: {}", lic.customer_name());
            println!("Expired:  {}", lic.payload.expires);
            println!("\n\x1b[31mLicense has expired! Server will not start.\x1b[0m");
            println!("Renew at https://tell.rs/renew");
        }
        LicenseState::Invalid(err) => {
            println!("Status:   Invalid license");
            println!("Error:    {}", err);
            println!("\nPlease check your license key or contact support.");
        }
    }

    // Show where license is stored
    println!("\n---");
    println!("License file: {}", default_license_path().display());
    println!("Environment:  {}", LICENSE_ENV_VAR);

    Ok(())
}

/// Activate a license key
fn activate(key: &str) -> Result<()> {
    let key = key.trim();

    if key.is_empty() {
        anyhow::bail!("License key cannot be empty");
    }

    println!("Validating license key...\n");

    match activate_license(key) {
        Ok(license) => {
            println!("\x1b[32mLicense activated successfully!\x1b[0m\n");
            println!("Tier:     {}", license.tier().display_name());
            println!("Customer: {}", license.customer_name());
            println!("Expires:  {}", license.payload.expires);
            println!("\nSaved to: {}", default_license_path().display());

            Ok(())
        }
        Err(e) => {
            eprintln!("\x1b[31mFailed to activate license:\x1b[0m {}", e);
            eprintln!("\nPlease verify your license key and try again.");
            eprintln!("Contact support at support@tell.rs if the issue persists.");
            Err(e.into())
        }
    }
}

/// Deactivate the current license
fn deactivate() -> Result<()> {
    // First check current state
    let state = load_license(None);

    match &state {
        LicenseState::Free => {
            println!("No license is currently active.");
            return Ok(());
        }
        LicenseState::Licensed(lic) | LicenseState::Expired(lic) => {
            println!(
                "Removing license for: {} ({})",
                lic.customer_name(),
                lic.tier().display_name()
            );
        }
        LicenseState::Invalid(_) => {
            println!("Removing invalid license file...");
        }
    }

    deactivate_license().context("failed to remove license")?;

    println!("\n\x1b[32mLicense removed.\x1b[0m");
    println!("Tell will run in free tier mode.");

    Ok(())
}
