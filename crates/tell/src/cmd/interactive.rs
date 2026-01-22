//! Interactive TUI command.
//!
//! Launches the interactive terminal UI for Tell.

use anyhow::Result;
use clap::Args;

use crate::tui::App;

/// Run Tell in interactive mode
#[derive(Args, Debug)]
pub struct InteractiveArgs {
    /// Path to configuration file
    #[arg(short, long)]
    pub config: Option<std::path::PathBuf>,
}

/// Run the interactive TUI.
pub async fn run(args: InteractiveArgs) -> Result<()> {
    let config_path = args.config.as_deref();
    let mut app = App::new(config_path)?;
    app.run().await
}
