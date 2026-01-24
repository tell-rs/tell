//! Command definitions and context-aware command list.
//!
//! Provides the available commands for each view context.

use super::action::{ServerStatus, View};

/// Context for filtering available commands.
#[derive(Debug, Clone, Copy)]
pub struct CommandContext {
    pub has_config: bool,
    pub server_running: bool,
    pub is_authenticated: bool,
}

impl CommandContext {
    pub fn new(has_config: bool, server_status: ServerStatus, is_authenticated: bool) -> Self {
        Self {
            has_config,
            server_running: server_status == ServerStatus::Running,
            is_authenticated,
        }
    }
}

/// Available commands with descriptions (context-aware).
pub fn commands_for_view(view: View, ctx: CommandContext) -> Vec<(&'static str, &'static str)> {
    match view {
        View::Boards => vec![
            ("new", "Create new board"),
            ("edit", "Edit selected board"),
            ("delete", "Delete selected board"),
            ("pin", "Pin/unpin selected board"),
            ("share", "Share/unshare selected board"),
            ("back", "Return to dashboard"),
        ],
        View::BoardDetail => vec![
            ("7d", "Set range to 7 days"),
            ("30d", "Set range to 30 days"),
            ("90d", "Set range to 90 days"),
            ("compare", "Toggle comparison"),
            ("breakdown", "Set breakdown (device, country, none)"),
            ("refresh", "Refresh metrics"),
            ("share", "Share/unshare board"),
            ("stickiness", "View stickiness metrics"),
            ("top", "View top events"),
            ("back", "Return to boards"),
        ],
        View::Query => vec![
            ("run", "Execute query"),
            ("clear", "Clear query and results"),
            ("back", "Return to dashboard"),
        ],
        View::Tail => vec![("clear", "Clear events"), ("back", "Return to dashboard")],
        View::ApiKeys => vec![
            ("new <name>", "Create new API key"),
            ("show <name>", "Reveal full API key"),
            ("delete <name>", "Delete API key"),
            ("back", "Return to dashboard"),
        ],
        _ => {
            let mut cmds = Vec::with_capacity(12);

            // Setup - only show init if no config
            if !ctx.has_config {
                cmds.push(("init", "Setup wizard"));
            }
            cmds.push(("config", "View config"));

            // Server - swap start/stop based on state
            if ctx.server_running {
                cmds.push(("stop", "Stop server"));
            } else {
                cmds.push(("start", "Start server"));
            }

            // Data - only available when server running
            if ctx.server_running {
                cmds.push(("test", "Send test events"));
                cmds.push(("tail", "Live event stream"));
            }
            cmds.push(("query", "SQL query interface"));

            // Auth - swap login/logout based on state
            if ctx.is_authenticated {
                cmds.push(("logout", "Sign out"));
            } else {
                cmds.push(("login", "Authenticate"));
            }

            // Navigation - boards requires auth
            if ctx.is_authenticated {
                cmds.push(("boards", "View boards"));
            }
            cmds.push(("apikeys", "Manage API keys"));
            cmds.push(("license", "Show license info"));
            cmds.push(("help", "Show help"));
            cmds.push(("quit", "Exit"));

            cmds
        }
    }
}
