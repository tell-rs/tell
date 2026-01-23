//! Main TUI application.
//!
//! Handles the event loop, rendering, and component coordination.

use std::io::{self, Stderr};
use std::panic::{set_hook, take_hook};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use crossterm::cursor;
use crossterm::event::{DisableMouseCapture, EnableMouseCapture, KeyCode, KeyModifiers};
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Layout};
use ratatui::prelude::*;
use ratatui::style::Color;
use ratatui::widgets::*;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tui_input::Input;
use tui_input::backend::crossterm::EventHandler;

use super::action::{
    Action, InitState, InitStep, ServerStatus, SinkType, SourceType, TimeRange, View,
};
use super::event::{Event, EventHandler as TellEventHandler};
use super::theme::Theme;

/// Server metrics fetched from control endpoint
#[derive(Debug, Clone, Default)]
pub struct ServerMetrics {
    pub uptime_secs: u64,
    pub messages_processed: u64,
    pub bytes_processed: u64,
    pub sources: Vec<SourceMetricsInfo>,
    pub sinks: Vec<SinkMetricsInfo>,
}

#[derive(Debug, Clone)]
pub struct SourceMetricsInfo {
    pub id: String,
    pub source_type: String,
    pub messages_received: u64,
    pub connections_active: u64,
}

#[derive(Debug, Clone)]
pub struct SinkMetricsInfo {
    pub id: String,
    pub sink_type: String,
    pub messages_written: u64,
    pub errors: u64,
}

/// Format a number with K/M/B suffix
fn format_number(n: u64) -> String {
    if n >= 1_000_000_000 {
        format!("{:.1}B", n as f64 / 1_000_000_000.0)
    } else if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.1}K", n as f64 / 1_000.0)
    } else {
        n.to_string()
    }
}

/// Format bytes with KB/MB/GB suffix
fn format_bytes(bytes: u64) -> String {
    if bytes >= 1_000_000_000 {
        format!("{:.1}GB", bytes as f64 / 1_000_000_000.0)
    } else if bytes >= 1_000_000 {
        format!("{:.1}MB", bytes as f64 / 1_000_000.0)
    } else if bytes >= 1_000 {
        format!("{:.1}KB", bytes as f64 / 1_000.0)
    } else {
        format!("{}B", bytes)
    }
}

/// Main TUI application state.
pub struct App {
    /// Terminal backend
    terminal: Terminal<CrosstermBackend<Stderr>>,
    /// Event handler
    events: TellEventHandler,
    /// Action sender
    action_tx: UnboundedSender<Action>,
    /// Action receiver
    action_rx: UnboundedReceiver<Action>,
    /// Current view
    view: View,
    /// View history for back navigation
    view_history: Vec<View>,
    /// Server status
    server_status: ServerStatus,
    /// Server child process
    server_process: Option<Child>,
    /// Theme
    theme: Theme,
    /// Config file path
    config_path: std::path::PathBuf,
    /// Whether a config file exists
    has_config: bool,
    /// Should quit
    should_quit: bool,
    /// Command input
    command_input: Input,
    /// Whether command input is focused
    command_focused: bool,
    /// Command history
    command_history: Vec<String>,
    /// Current history index (for up/down navigation)
    history_index: Option<usize>,
    /// Error message to display
    error_message: Option<String>,
    /// Init wizard state
    init_state: InitState,
    /// Selected index in list views (for init wizard)
    selected_index: usize,
    /// Selected command index in command popup
    selected_cmd_index: usize,
    /// Tail events buffer
    tail_events: Vec<String>,
    /// Tail scroll position
    tail_scroll: usize,
    /// Whether tail is connected
    tail_connected: bool,
    /// Server metrics from control endpoint
    server_metrics: Option<ServerMetrics>,
    /// Previous metrics for rate calculation
    prev_metrics: Option<ServerMetrics>,
    /// Last metrics fetch time
    last_metrics_fetch: Option<Instant>,
    /// Auth credentials (if logged in)
    auth: Option<crate::cmd::auth::AuthCredentials>,
    /// Login state
    login_state: LoginState,
    /// Boards list
    boards: Vec<super::action::BoardInfo>,
    /// Currently selected board for detail view
    current_board: Option<super::action::BoardData>,
    /// Last board metrics fetch time (for periodic refresh)
    last_board_fetch: Option<Instant>,
    /// Query view state
    query_state: super::action::QueryState,
    /// Current time range for metrics
    time_range: super::action::TimeRange,
    /// Whether to show comparison with previous period
    show_comparison: bool,
    /// Breakdown dimension (e.g., "device_type", "country")
    breakdown: Option<String>,
    /// Top events data
    top_events: Vec<super::action::TopEvent>,
    /// Stickiness data
    stickiness: Option<super::action::StickinessData>,
    /// Board create/edit mode
    board_edit_mode: Option<BoardEditMode>,
}

/// Login form state
#[derive(Debug, Clone, Default)]
pub struct LoginState {
    /// Email input
    pub email: Input,
    /// Password input
    pub password: String,
    /// Which field is focused (0 = email, 1 = password)
    pub focus: usize,
    /// Whether login is in progress
    pub loading: bool,
    /// Error message
    pub error: Option<String>,
}

/// Board edit mode state
#[derive(Debug, Clone)]
pub enum BoardEditMode {
    /// Creating a new board
    Create { title: Input },
    /// Editing an existing board's title
    Edit { board_id: String, title: Input },
    /// Confirming deletion
    Delete { board_id: String, title: String },
}

impl App {
    /// Create a new App instance.
    pub fn new(config_path: Option<&Path>) -> Result<Self> {
        let terminal = Terminal::new(CrosstermBackend::new(io::stderr()))
            .context("failed to create terminal")?;

        let events = TellEventHandler::new(Duration::from_millis(250));
        let (action_tx, action_rx) = mpsc::unbounded_channel();

        // Use provided path or default to config.toml in current dir
        let config_path = config_path
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| std::path::PathBuf::from("config.toml"));

        // Check if config exists
        let has_config = config_path.exists();

        // Start in Welcome view if no config, otherwise Dashboard
        let view = if has_config {
            View::Dashboard
        } else {
            View::Welcome
        };

        // Try to load existing auth credentials
        let auth = crate::cmd::auth::load_credentials().ok();

        Ok(Self {
            terminal,
            events,
            action_tx,
            action_rx,
            view,
            view_history: Vec::new(),
            server_status: ServerStatus::Stopped,
            server_process: None,
            theme: Theme::default(),
            config_path,
            has_config,
            should_quit: false,
            command_input: Input::default(),
            command_focused: false,
            command_history: Vec::new(),
            history_index: None,
            error_message: None,
            init_state: InitState::default(),
            selected_index: 0,
            selected_cmd_index: 0,
            tail_events: Vec::new(),
            tail_scroll: 0,
            tail_connected: false,
            server_metrics: None,
            prev_metrics: None,
            last_metrics_fetch: None,
            auth,
            login_state: LoginState::default(),
            boards: Vec::new(),
            current_board: None,
            last_board_fetch: None,
            query_state: super::action::QueryState::default(),
            time_range: super::action::TimeRange::default(),
            show_comparison: true,
            breakdown: None,
            top_events: Vec::new(),
            stickiness: None,
            board_edit_mode: None,
        })
    }

    /// Get the action sender for external use.
    pub fn action_sender(&self) -> UnboundedSender<Action> {
        self.action_tx.clone()
    }

    /// Run the TUI application.
    pub async fn run(&mut self) -> Result<()> {
        self.enter()?;

        // Auto-start the server if config exists
        if self.has_config {
            let _ = self.start_server();
        }

        loop {
            // Draw UI
            self.draw()?;

            // Handle events
            tokio::select! {
                Some(event) = self.events.next() => {
                    self.handle_event(event)?;
                }
                Some(action) = self.action_rx.recv() => {
                    self.handle_action(action)?;
                }
            }

            if self.should_quit {
                break;
            }
        }

        self.exit()?;
        Ok(())
    }

    /// Enter TUI mode.
    fn enter(&mut self) -> Result<()> {
        Self::init_panic_hook();
        enable_raw_mode().context("failed to enable raw mode")?;
        crossterm::execute!(
            io::stderr(),
            EnterAlternateScreen,
            EnableMouseCapture,
            cursor::Hide,
        )
        .context("failed to enter alternate screen")?;
        self.terminal.clear().context("failed to clear terminal")?;
        Ok(())
    }

    /// Exit TUI mode.
    fn exit(&mut self) -> Result<()> {
        if crossterm::terminal::is_raw_mode_enabled()? {
            disable_raw_mode().context("failed to disable raw mode")?;
            crossterm::execute!(
                io::stderr(),
                LeaveAlternateScreen,
                DisableMouseCapture,
                cursor::Show,
            )
            .context("failed to leave alternate screen")?;
        }
        Ok(())
    }

    /// Set up panic hook to restore terminal on panic.
    fn init_panic_hook() {
        let original_hook = take_hook();
        set_hook(Box::new(move |panic_info| {
            let _ = Self::restore_terminal();
            original_hook(panic_info);
        }));
    }

    /// Restore terminal state (for panic hook).
    fn restore_terminal() -> Result<()> {
        if crossterm::terminal::is_raw_mode_enabled()? {
            disable_raw_mode()?;
            crossterm::execute!(
                io::stderr(),
                LeaveAlternateScreen,
                DisableMouseCapture,
                cursor::Show,
            )?;
        }
        Ok(())
    }

    /// Draw the UI.
    fn draw(&mut self) -> Result<()> {
        let view = self.view;
        let theme = self.theme.clone();
        let server_status = self.server_status;
        let command_focused = self.command_focused;
        let command_value = self.command_input.value().to_string();
        let cursor_pos = self.command_input.visual_cursor();
        let error_message = self.error_message.clone();
        let init_state = self.init_state.clone();
        let selected_index = self.selected_index;
        let selected_cmd_index = self.selected_cmd_index;
        let config_path = self.config_path.clone();
        let tail_events = self.tail_events.clone();
        let tail_connected = self.tail_connected;
        let tail_scroll = self.tail_scroll;
        let server_metrics = self.server_metrics.clone();
        let auth = self.auth.clone();
        let login_state = self.login_state.clone();
        let boards = self.boards.clone();
        let current_board = self.current_board.clone();
        let query_state = self.query_state.clone();
        let time_range = self.time_range;
        let show_comparison = self.show_comparison;
        let board_edit_mode = self.board_edit_mode.clone();

        self.terminal.draw(|frame| {
            let area = frame.area();

            // Main layout: header + content + footer
            let main_chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Length(1), // Header
                    Constraint::Min(3),    // Content
                    Constraint::Length(1), // Footer
                ])
                .split(area);

            let header_area = main_chunks[0];
            let content_area = main_chunks[1];
            let footer_area = main_chunks[2];

            // === HEADER ===
            let version = env!("CARGO_PKG_VERSION");
            let title_version = format!("Tell v{}", version);

            let header_line = Line::from(vec![
                Span::raw(" "),
                Span::styled(title_version, theme.brand_style().bold()),
            ]);
            frame.render_widget(Paragraph::new(header_line), header_area);

            // === CONTENT ===
            let content = Self::render_view_content(
                view,
                server_status,
                &theme,
                error_message.as_deref(),
                &init_state,
                selected_index,
                &config_path,
                &tail_events,
                tail_connected,
                tail_scroll,
                content_area.height as usize,
                server_metrics.as_ref(),
                auth.as_ref(),
                &login_state,
                &boards,
                current_board.as_ref(),
                &query_state,
                time_range,
                show_comparison,
            );
            let paragraph = Paragraph::new(content).wrap(Wrap { trim: false });
            frame.render_widget(paragraph, content_area);

            // === FOOTER (shortcuts + auth status) ===
            let shortcuts = if command_focused {
                vec![("Enter", "execute"), ("Esc", "cancel"), ("↑↓", "navigate")]
            } else {
                vec![("/", "commands"), ("q", "quit"), ("?", "help")]
            };

            let mut footer_spans = vec![Span::raw(" ")];
            for (i, (key, desc)) in shortcuts.iter().enumerate() {
                if i > 0 {
                    footer_spans.push(Span::styled("  ", theme.muted_style()));
                }
                footer_spans.push(Span::styled(*key, Style::default().bold()));
                footer_spans.push(Span::raw(" "));
                footer_spans.push(Span::styled(*desc, theme.muted_style()));
            }

            // Auth status on the right
            let auth_status = if let Some(a) = &auth {
                Span::styled(format!("  {} ", a.email), theme.muted_style())
            } else {
                Span::styled("  not logged in ", theme.muted_style())
            };

            // Use a layout to split footer into left (shortcuts) and right (auth)
            let footer_layout = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([
                    Constraint::Min(1),
                    Constraint::Length(auth_status.width() as u16),
                ])
                .split(footer_area);

            frame.render_widget(Paragraph::new(Line::from(footer_spans)), footer_layout[0]);
            frame.render_widget(
                Paragraph::new(Line::from(auth_status)).alignment(Alignment::Right),
                footer_layout[1],
            );

            // === COMMAND POPUP (overlay when / is pressed) ===
            if command_focused {
                Self::render_command_popup(
                    frame,
                    area,
                    &theme,
                    &command_value,
                    cursor_pos,
                    selected_cmd_index,
                );
            }

            // === BOARD EDIT DIALOG (overlay for create/delete) ===
            if let Some(ref edit_mode) = board_edit_mode {
                Self::render_board_edit_dialog(frame, area, &theme, edit_mode);
            }
        })?;

        Ok(())
    }

    /// Render view-specific content.
    #[allow(clippy::too_many_arguments)]
    fn render_view_content<'a>(
        view: View,
        server_status: ServerStatus,
        theme: &Theme,
        error: Option<&str>,
        init_state: &InitState,
        selected_index: usize,
        config_path: &std::path::Path,
        tail_events: &[String],
        tail_connected: bool,
        tail_scroll: usize,
        visible_lines: usize,
        server_metrics: Option<&ServerMetrics>,
        auth: Option<&'a crate::cmd::auth::AuthCredentials>,
        login_state: &LoginState,
        boards: &[super::action::BoardInfo],
        current_board: Option<&'a super::action::BoardData>,
        query_state: &'a super::action::QueryState,
        time_range: TimeRange,
        show_comparison: bool,
    ) -> Vec<Line<'a>> {
        let mut lines = match view {
            View::Welcome => {
                vec![
                    Line::from(""),
                    Line::from(vec![Span::styled(
                        "  Analytics that tell the whole story.",
                        theme.muted_style(),
                    )]),
                    Line::from(""),
                    Line::from(""),
                    Line::from(vec![Span::styled(
                        "  No configuration found.",
                        theme.muted_style(),
                    )]),
                    Line::from(""),
                    Line::from(vec![
                        Span::raw("  Press "),
                        Span::styled("/", Style::default().bold()),
                        Span::raw(" and type "),
                        Span::styled("init", theme.brand_style()),
                        Span::raw(" to get started."),
                    ]),
                ]
            }
            View::Dashboard => {
                let status_style = match server_status {
                    ServerStatus::Running => theme.status_running(),
                    ServerStatus::Error => theme.status_error(),
                    _ => theme.status_stopped(),
                };

                // Format uptime from server metrics
                let uptime_str = server_metrics
                    .map(|m| {
                        let secs = m.uptime_secs;
                        if secs < 60 {
                            format!("{}s", secs)
                        } else if secs < 3600 {
                            format!("{}m {}s", secs / 60, secs % 60)
                        } else if secs < 86400 {
                            format!("{}h {}m", secs / 3600, (secs % 3600) / 60)
                        } else {
                            format!("{}d {}h", secs / 86400, (secs % 86400) / 3600)
                        }
                    })
                    .unwrap_or_else(|| "-".to_string());

                let mut lines = vec![
                    Line::from(""),
                    Line::from(vec![
                        Span::raw("  "),
                        Span::styled(server_status.indicator(), status_style),
                        Span::styled(format!(" Server {:?}", server_status), status_style),
                        if server_metrics.is_some() {
                            Span::styled(format!("  (uptime: {})", uptime_str), theme.muted_style())
                        } else {
                            Span::raw("")
                        },
                    ]),
                    Line::from(""),
                ];

                // Show real server metrics from control endpoint
                if let Some(metrics) = server_metrics {
                    lines.push(Line::from(""));
                    lines.push(Line::from(vec![Span::styled(
                        "  Pipeline",
                        theme.brand_style().bold(),
                    )]));
                    lines.push(Line::from(vec![
                        Span::styled("    Messages: ", theme.muted_style()),
                        Span::styled(
                            format_number(metrics.messages_processed),
                            Style::default().bold(),
                        ),
                    ]));
                    lines.push(Line::from(vec![
                        Span::styled("    Bytes:    ", theme.muted_style()),
                        Span::styled(
                            format_bytes(metrics.bytes_processed),
                            Style::default().bold(),
                        ),
                    ]));

                    // Sources
                    if !metrics.sources.is_empty() {
                        lines.push(Line::from(""));
                        lines.push(Line::from(vec![Span::styled(
                            "  Sources",
                            theme.brand_style().bold(),
                        )]));
                        for source in &metrics.sources {
                            lines.push(Line::from(vec![
                                Span::styled(format!("    {} ", source.id), theme.muted_style()),
                                Span::raw(format!(
                                    "{} msgs, {} conns",
                                    format_number(source.messages_received),
                                    source.connections_active
                                )),
                            ]));
                        }
                    }

                    // Sinks
                    if !metrics.sinks.is_empty() {
                        lines.push(Line::from(""));
                        lines.push(Line::from(vec![Span::styled(
                            "  Sinks",
                            theme.brand_style().bold(),
                        )]));
                        for sink in &metrics.sinks {
                            let errors_str = if sink.errors > 0 {
                                format!(", {} errors", sink.errors)
                            } else {
                                String::new()
                            };
                            lines.push(Line::from(vec![
                                Span::styled(format!("    {} ", sink.id), theme.muted_style()),
                                Span::raw(format!(
                                    "{} msgs{}",
                                    format_number(sink.messages_written),
                                    errors_str
                                )),
                            ]));
                        }
                    }
                }

                lines
            }
            View::Init => Self::render_init_step(theme, init_state, selected_index),
            View::Config => {
                // Try to read the config file
                let config_content = std::fs::read_to_string(config_path);

                let mut lines = vec![
                    Line::from(""),
                    Line::from("  Configuration"),
                    Line::from(""),
                ];

                match config_content {
                    Ok(content) => {
                        lines.push(Line::from(vec![
                            Span::styled("  Path: ", theme.muted_style()),
                            Span::styled(
                                config_path
                                    .canonicalize()
                                    .map(|p| p.display().to_string())
                                    .unwrap_or_else(|_| config_path.display().to_string()),
                                theme.brand_style(),
                            ),
                        ]));
                        lines.push(Line::from(""));
                        lines.push(Line::from(vec![Span::styled(
                            "  ─".to_string() + &"─".repeat(40),
                            theme.muted_style(),
                        )]));
                        lines.push(Line::from(""));

                        // Show full config contents
                        for line in content.lines() {
                            let styled_line = if line.starts_with('#') {
                                Span::styled(format!("  {}", line), theme.muted_style())
                            } else if line.starts_with('[') {
                                Span::styled(format!("  {}", line), theme.brand_style())
                            } else {
                                Span::raw(format!("  {}", line))
                            };
                            lines.push(Line::from(vec![styled_line]));
                        }
                    }
                    Err(_) => {
                        lines.push(Line::from(vec![Span::styled(
                            "  No config file found.",
                            theme.muted_style(),
                        )]));
                        lines.push(Line::from(""));
                        lines.push(Line::from(vec![
                            Span::raw("  Run "),
                            Span::styled("/init", theme.brand_style()),
                            Span::raw(" to create one."),
                        ]));
                    }
                }

                lines
            }
            View::Help => {
                vec![
                    Line::from(""),
                    Line::from("  Commands"),
                    Line::from(""),
                    Line::from(vec![
                        Span::styled("    /init", theme.brand_style()),
                        Span::styled("      Setup wizard", theme.muted_style()),
                    ]),
                    Line::from(vec![
                        Span::styled("    /start", theme.brand_style()),
                        Span::styled("     Start the server", theme.muted_style()),
                    ]),
                    Line::from(vec![
                        Span::styled("    /stop", theme.brand_style()),
                        Span::styled("      Stop the server", theme.muted_style()),
                    ]),
                    Line::from(vec![
                        Span::styled("    /sources", theme.brand_style()),
                        Span::styled("   Manage data sources", theme.muted_style()),
                    ]),
                    Line::from(vec![
                        Span::styled("    /sinks", theme.brand_style()),
                        Span::styled("     Manage data sinks", theme.muted_style()),
                    ]),
                    Line::from(vec![
                        Span::styled("    /plugins", theme.brand_style()),
                        Span::styled("   Browse connectors", theme.muted_style()),
                    ]),
                    Line::from(vec![
                        Span::styled("    /tail", theme.brand_style()),
                        Span::styled("      Live event stream", theme.muted_style()),
                    ]),
                    Line::from(vec![
                        Span::styled("    /query", theme.brand_style()),
                        Span::styled("     SQL query interface", theme.muted_style()),
                    ]),
                    Line::from(""),
                    Line::from("  Keyboard"),
                    Line::from(""),
                    Line::from(vec![
                        Span::styled("    /", Style::default().bold()),
                        Span::styled("          Command mode", theme.muted_style()),
                    ]),
                    Line::from(vec![
                        Span::styled("    Esc", Style::default().bold()),
                        Span::styled("        Go back", theme.muted_style()),
                    ]),
                    Line::from(vec![
                        Span::styled("    q", Style::default().bold()),
                        Span::styled("          Quit", theme.muted_style()),
                    ]),
                ]
            }
            View::Sources => {
                vec![
                    Line::from(""),
                    Line::from("  Sources"),
                    Line::from(""),
                    Line::from(vec![Span::styled(
                        "  No sources configured.",
                        theme.muted_style(),
                    )]),
                    Line::from(""),
                    Line::from(vec![
                        Span::raw("  Press "),
                        Span::styled("/", Style::default().bold()),
                        Span::raw(" and type "),
                        Span::styled("init", theme.brand_style()),
                        Span::raw(" to add sources."),
                    ]),
                ]
            }
            View::Sinks => {
                vec![
                    Line::from(""),
                    Line::from("  Sinks"),
                    Line::from(""),
                    Line::from(vec![Span::styled(
                        "  No sinks configured.",
                        theme.muted_style(),
                    )]),
                    Line::from(""),
                    Line::from(vec![
                        Span::raw("  Press "),
                        Span::styled("/", Style::default().bold()),
                        Span::raw(" and type "),
                        Span::styled("init", theme.brand_style()),
                        Span::raw(" to add sinks."),
                    ]),
                ]
            }
            View::Plugins => {
                vec![
                    Line::from(""),
                    Line::from("  Connectors"),
                    Line::from(""),
                    Line::from(vec![
                        Span::styled("    ●", theme.success_style()),
                        Span::raw(" GitHub"),
                        Span::styled("      Webhooks and events", theme.muted_style()),
                    ]),
                    Line::from(vec![
                        Span::styled("    ●", theme.success_style()),
                        Span::raw(" Shopify"),
                        Span::styled("     E-commerce analytics", theme.muted_style()),
                    ]),
                    Line::from(vec![
                        Span::styled("    ○", theme.muted_style()),
                        Span::raw(" Stripe"),
                        Span::styled("      Payment events", theme.muted_style()),
                    ]),
                    Line::from(vec![
                        Span::styled("    ○", theme.muted_style()),
                        Span::raw(" Linear"),
                        Span::styled("      Issue tracking", theme.muted_style()),
                    ]),
                ]
            }
            View::Query => {
                let mut lines = vec![Line::from(""), Line::from("  SQL Query"), Line::from("")];

                // Query input area
                lines.push(Line::from(vec![
                    Span::styled("  > ", theme.brand_style()),
                    Span::raw(&query_state.query),
                    Span::styled("_", Style::default().add_modifier(Modifier::SLOW_BLINK)),
                ]));
                lines.push(Line::from(""));

                // Show loading or error
                if query_state.loading {
                    lines.push(Line::from(vec![Span::styled(
                        "  Executing query...",
                        theme.muted_style(),
                    )]));
                } else if let Some(ref err) = query_state.error {
                    lines.push(Line::from(vec![Span::styled(
                        format!("  Error: {}", err),
                        theme.error_style(),
                    )]));
                } else if let Some(ref result) = query_state.result {
                    // Show result summary
                    lines.push(Line::from(vec![
                        Span::styled(
                            format!("  {} rows", result.row_count),
                            theme.success_style(),
                        ),
                        Span::styled(
                            format!(" ({} ms)", result.execution_time_ms),
                            theme.muted_style(),
                        ),
                    ]));
                    lines.push(Line::from(""));

                    if !result.columns.is_empty() {
                        // Column headers
                        let header: Vec<Span> = result
                            .columns
                            .iter()
                            .enumerate()
                            .flat_map(|(i, col)| {
                                let mut spans = vec![Span::styled(
                                    format!("{:<15}", col),
                                    Style::default().bold(),
                                )];
                                if i < result.columns.len() - 1 {
                                    spans.push(Span::raw(" │ "));
                                }
                                spans
                            })
                            .collect();
                        lines.push(Line::from(vec![Span::raw("  ")]).patch_style(Style::default()));
                        lines.push(Line::from(
                            std::iter::once(Span::raw("  "))
                                .chain(header)
                                .collect::<Vec<_>>(),
                        ));

                        // Separator
                        let sep_len = result.columns.len() * 18;
                        lines.push(Line::from(vec![
                            Span::raw("  "),
                            Span::styled("─".repeat(sep_len.min(60)), theme.muted_style()),
                        ]));

                        // Data rows (limited by scroll and visible lines)
                        let max_rows = visible_lines.saturating_sub(12);
                        let start = query_state.scroll;
                        let end = (start + max_rows).min(result.rows.len());

                        for row in &result.rows[start..end] {
                            let row_spans: Vec<Span> = row
                                .iter()
                                .enumerate()
                                .flat_map(|(i, val)| {
                                    let display = if val.len() > 15 {
                                        format!("{}…", &val[..14])
                                    } else {
                                        format!("{:<15}", val)
                                    };
                                    let mut spans = vec![Span::raw(display)];
                                    if i < row.len() - 1 {
                                        spans.push(Span::styled(" │ ", theme.muted_style()));
                                    }
                                    spans
                                })
                                .collect();
                            lines.push(Line::from(
                                std::iter::once(Span::raw("  "))
                                    .chain(row_spans)
                                    .collect::<Vec<_>>(),
                            ));
                        }

                        // Scroll indicator
                        if result.rows.len() > max_rows {
                            lines.push(Line::from(""));
                            lines.push(Line::from(vec![Span::styled(
                                format!(
                                    "  [{}-{}/{}] ↑↓ scroll",
                                    start + 1,
                                    end,
                                    result.rows.len()
                                ),
                                theme.muted_style(),
                            )]));
                        }
                    }
                } else {
                    // Help text
                    lines.push(Line::from(vec![Span::styled(
                        "  Type a SQL query and press Enter to execute.",
                        theme.muted_style(),
                    )]));
                    lines.push(Line::from(""));
                    lines.push(Line::from(vec![Span::styled(
                        "  Examples:",
                        theme.muted_style(),
                    )]));
                    lines.push(Line::from(vec![
                        Span::raw("    "),
                        Span::styled("SELECT * FROM events_v1 LIMIT 10", theme.brand_style()),
                    ]));
                    lines.push(Line::from(vec![
                        Span::raw("    "),
                        Span::styled(
                            "SELECT event_name, COUNT(*) FROM events_v1 GROUP BY event_name",
                            theme.brand_style(),
                        ),
                    ]));
                }

                lines.push(Line::from(""));
                lines.push(Line::from(vec![
                    Span::styled("  Enter", Style::default().bold()),
                    Span::styled(" run  ", theme.muted_style()),
                    Span::styled("Esc", Style::default().bold()),
                    Span::styled(" back  ", theme.muted_style()),
                    Span::styled("Ctrl+C", Style::default().bold()),
                    Span::styled(" clear", theme.muted_style()),
                ]));

                lines
            }
            View::Tail => {
                let mut lines = vec![
                    Line::from(""),
                    Line::from(vec![
                        Span::raw("  Live Events "),
                        if tail_connected {
                            Span::styled("● connected", theme.success_style())
                        } else {
                            Span::styled("○ disconnected", theme.muted_style())
                        },
                    ]),
                    Line::from(""),
                ];

                if tail_events.is_empty() {
                    if tail_connected {
                        lines.push(Line::from(vec![Span::styled(
                            "  Waiting for events...",
                            theme.muted_style(),
                        )]));
                    } else {
                        lines.push(Line::from(vec![
                            Span::styled("  Server not running. Use ", theme.muted_style()),
                            Span::styled("/start", theme.brand_style()),
                            Span::styled(" first.", theme.muted_style()),
                        ]));
                    }
                } else {
                    // Show events with scrolling (most recent at bottom)
                    let max_events = visible_lines.saturating_sub(4); // Reserve header lines
                    let start = tail_scroll.min(tail_events.len().saturating_sub(max_events));
                    let end = (start + max_events).min(tail_events.len());

                    for event in &tail_events[start..end] {
                        // Color-code based on event type
                        let style = if event.contains("[ERROR]") || event.contains("error") {
                            theme.error_style()
                        } else if event.contains("[WARN]") || event.contains("warning") {
                            Style::default().fg(Color::Yellow)
                        } else if event.contains("[event]") {
                            theme.brand_style()
                        } else {
                            Style::default()
                        };
                        lines.push(Line::from(vec![
                            Span::raw("  "),
                            Span::styled(event.clone(), style),
                        ]));
                    }

                    // Show scroll indicator if there's more
                    if tail_events.len() > max_events {
                        lines.push(Line::from(""));
                        lines.push(Line::from(vec![Span::styled(
                            format!("  [{}/{}] ↑↓ scroll", end, tail_events.len()),
                            theme.muted_style(),
                        )]));
                    }
                }

                lines
            }
            View::Login => {
                let mut lines = vec![Line::from(""), Line::from("  Login"), Line::from("")];

                if login_state.loading {
                    lines.push(Line::from(vec![Span::styled(
                        "  Logging in...",
                        theme.muted_style(),
                    )]));
                } else {
                    // Email field
                    let email_style = if login_state.focus == 0 {
                        theme.selected_style()
                    } else {
                        Style::default()
                    };
                    let email_indicator = if login_state.focus == 0 { "▸" } else { " " };
                    lines.push(Line::from(vec![
                        Span::styled(format!("  {} Email:    ", email_indicator), email_style),
                        Span::styled(login_state.email.value().to_string(), email_style),
                        if login_state.focus == 0 {
                            Span::styled("_", Style::default().add_modifier(Modifier::SLOW_BLINK))
                        } else {
                            Span::raw("")
                        },
                    ]));

                    // Password field
                    let password_style = if login_state.focus == 1 {
                        theme.selected_style()
                    } else {
                        Style::default()
                    };
                    let password_indicator = if login_state.focus == 1 { "▸" } else { " " };
                    let masked_password = "*".repeat(login_state.password.len());
                    lines.push(Line::from(vec![
                        Span::styled(
                            format!("  {} Password: ", password_indicator),
                            password_style,
                        ),
                        Span::styled(masked_password, password_style),
                        if login_state.focus == 1 {
                            Span::styled("_", Style::default().add_modifier(Modifier::SLOW_BLINK))
                        } else {
                            Span::raw("")
                        },
                    ]));

                    lines.push(Line::from(""));
                    lines.push(Line::from(vec![
                        Span::styled("  Tab", Style::default().bold()),
                        Span::styled(" switch field  ", theme.muted_style()),
                        Span::styled("Enter", Style::default().bold()),
                        Span::styled(" login  ", theme.muted_style()),
                        Span::styled("Esc", Style::default().bold()),
                        Span::styled(" cancel", theme.muted_style()),
                    ]));

                    if let Some(ref err) = login_state.error {
                        lines.push(Line::from(""));
                        lines.push(Line::from(vec![Span::styled(
                            format!("  {}", err),
                            theme.error_style(),
                        )]));
                    }
                }

                lines
            }
            View::Boards => {
                let mut lines = vec![
                    Line::from(""),
                    Line::from(vec![
                        Span::raw("  Boards "),
                        if let Some(a) = auth {
                            Span::styled(format!("({})", a.email), theme.muted_style())
                        } else {
                            Span::raw("")
                        },
                    ]),
                    Line::from(""),
                ];

                if boards.is_empty() {
                    lines.push(Line::from(vec![Span::styled(
                        "  No boards found.",
                        theme.muted_style(),
                    )]));
                    lines.push(Line::from(""));
                    lines.push(Line::from(vec![Span::styled(
                        "  Create boards in the web dashboard.",
                        theme.muted_style(),
                    )]));
                } else {
                    for (i, board) in boards.iter().enumerate() {
                        let marker = if i == selected_index { "▸" } else { " " };
                        let style = if i == selected_index {
                            theme.selected_style()
                        } else {
                            Style::default()
                        };
                        let pin_marker = if board.is_pinned { "📌 " } else { "" };
                        let share_marker = if board.share_hash.is_some() {
                            " 🔗"
                        } else {
                            ""
                        };
                        lines.push(Line::from(vec![
                            Span::styled(
                                format!("    {} {}{}", marker, pin_marker, board.title),
                                style,
                            ),
                            Span::styled(share_marker, Style::default().fg(Color::Cyan)),
                            Span::styled(
                                format!("  ({} blocks)", board.block_count),
                                theme.muted_style(),
                            ),
                        ]));
                        if let Some(ref desc) = board.description {
                            lines.push(Line::from(vec![Span::styled(
                                format!("        {}", desc),
                                theme.muted_style(),
                            )]));
                        }
                    }
                    lines.push(Line::from(""));
                    lines.push(Line::from(vec![
                        Span::styled("  Enter", Style::default().bold()),
                        Span::styled(" view  ", theme.muted_style()),
                        Span::styled("n", Style::default().bold()),
                        Span::styled(" new  ", theme.muted_style()),
                        Span::styled("d", Style::default().bold()),
                        Span::styled(" delete  ", theme.muted_style()),
                        Span::styled("p", Style::default().bold()),
                        Span::styled(" pin  ", theme.muted_style()),
                        Span::styled("s", Style::default().bold()),
                        Span::styled(" share", theme.muted_style()),
                    ]));
                }

                lines
            }
            View::BoardDetail => {
                let mut lines = vec![Line::from("")];

                if let Some(board) = current_board {
                    lines.push(Line::from(vec![
                        Span::raw("  "),
                        Span::styled(&board.title, theme.brand_style().bold()),
                    ]));

                    // Time range and comparison indicator
                    let comparison_indicator = if show_comparison { " vs prev" } else { "" };
                    lines.push(Line::from(vec![Span::styled(
                        format!("  {} {}", time_range.label(), comparison_indicator),
                        theme.muted_style(),
                    )]));
                    lines.push(Line::from(""));

                    if board.metrics.is_empty() {
                        lines.push(Line::from(vec![Span::styled(
                            "  No metrics in this board.",
                            theme.muted_style(),
                        )]));
                    } else {
                        for metric in &board.metrics {
                            let change_span = if show_comparison {
                                if let Some(pct) = metric.change_percent {
                                    let (sign, color) = if pct >= 0.0 {
                                        ("+", Color::Green)
                                    } else {
                                        ("", Color::Red)
                                    };
                                    Span::styled(
                                        format!("  {}{:.1}%", sign, pct),
                                        Style::default().fg(color),
                                    )
                                } else {
                                    Span::raw("")
                                }
                            } else {
                                Span::raw("")
                            };

                            lines.push(Line::from(vec![
                                Span::styled(format!("    {} ", metric.title), theme.muted_style()),
                                Span::styled(
                                    format!("{:.0}", metric.value),
                                    Style::default().bold(),
                                ),
                                change_span,
                            ]));
                        }
                    }
                } else {
                    lines.push(Line::from(vec![Span::styled(
                        "  Loading board...",
                        theme.muted_style(),
                    )]));
                }

                lines.push(Line::from(""));
                lines.push(Line::from(vec![
                    Span::styled("  1-3", Style::default().bold()),
                    Span::styled(" range  ", theme.muted_style()),
                    Span::styled("c", Style::default().bold()),
                    Span::styled(" compare  ", theme.muted_style()),
                    Span::styled("r", Style::default().bold()),
                    Span::styled(" refresh  ", theme.muted_style()),
                    Span::styled("s", Style::default().bold()),
                    Span::styled(" share  ", theme.muted_style()),
                    Span::styled("Esc", Style::default().bold()),
                    Span::styled(" back", theme.muted_style()),
                ]));

                lines
            }
        };

        // Add error message if present
        if let Some(err) = error {
            lines.push(Line::from(""));
            lines.push(Line::from(vec![Span::styled(
                format!("Error: {}", err),
                theme.error_style(),
            )]));
        }

        lines
    }

    /// Handle a terminal event.
    fn handle_event(&mut self, event: Event) -> Result<()> {
        // Clear error on any input
        self.error_message = None;

        match event {
            Event::Key(key) => {
                // When command input is focused, handle input
                if self.command_focused {
                    match key.code {
                        KeyCode::Enter => {
                            // Get filtered commands to potentially execute selected one
                            let input = self.command_input.value().to_string();
                            let filter = input.strip_prefix('/').unwrap_or(&input).to_lowercase();
                            let commands = Self::commands();
                            let filtered: Vec<_> = commands
                                .iter()
                                .filter(|(cmd, _)| filter.is_empty() || cmd.starts_with(&filter))
                                .collect();

                            // Execute selected command or typed input
                            let cmd = if !filtered.is_empty()
                                && self.selected_cmd_index < filtered.len()
                            {
                                format!("/{}", filtered[self.selected_cmd_index].0)
                            } else {
                                input.clone()
                            };

                            if !cmd.is_empty() && cmd != "/" {
                                self.command_history.push(cmd.clone());
                                self.history_index = None;
                                self.action_tx.send(Action::Command(cmd))?;
                            }
                            self.command_input.reset();
                            self.command_focused = false;
                            self.selected_cmd_index = 0;
                        }
                        KeyCode::Esc => {
                            self.command_input.reset();
                            self.command_focused = false;
                            self.selected_cmd_index = 0;
                        }
                        KeyCode::Up => {
                            // Navigate command list up
                            if self.selected_cmd_index > 0 {
                                self.selected_cmd_index -= 1;
                            }
                        }
                        KeyCode::Down => {
                            // Navigate command list down
                            let input = self.command_input.value();
                            let filter = input.strip_prefix('/').unwrap_or(input).to_lowercase();
                            let commands = Self::commands();
                            let filtered_count = commands
                                .iter()
                                .filter(|(cmd, _)| filter.is_empty() || cmd.starts_with(&filter))
                                .count();
                            if filtered_count > 0 && self.selected_cmd_index < filtered_count - 1 {
                                self.selected_cmd_index += 1;
                            }
                        }
                        _ => {
                            // Let tui-input handle the key
                            self.command_input
                                .handle_event(&crossterm::event::Event::Key(key));
                            // Reset selection when typing
                            self.selected_cmd_index = 0;
                        }
                    }
                } else if self.board_edit_mode.is_some() {
                    // Board edit dialog key handling
                    match (&mut self.board_edit_mode, key.code) {
                        (Some(BoardEditMode::Create { title }), KeyCode::Enter) => {
                            let new_title = title.value().to_string();
                            if !new_title.trim().is_empty() {
                                self.action_tx.send(Action::CreateBoard(new_title))?;
                            }
                            self.board_edit_mode = None;
                        }
                        (Some(BoardEditMode::Create { .. }), KeyCode::Esc) => {
                            self.board_edit_mode = None;
                        }
                        (Some(BoardEditMode::Create { title }), KeyCode::Backspace) => {
                            // Handle backspace for title input
                            let mut val = title.value().to_string();
                            val.pop();
                            *title = Input::new(val);
                        }
                        (Some(BoardEditMode::Create { title }), KeyCode::Char(c)) => {
                            // Type into title
                            let mut val = title.value().to_string();
                            val.push(c);
                            *title = Input::new(val);
                        }
                        (Some(BoardEditMode::Edit { board_id, title }), KeyCode::Enter) => {
                            let new_title = title.value().to_string();
                            if !new_title.trim().is_empty() {
                                self.action_tx
                                    .send(Action::UpdateBoard(board_id.clone(), new_title))?;
                            }
                            self.board_edit_mode = None;
                        }
                        (Some(BoardEditMode::Edit { .. }), KeyCode::Esc) => {
                            self.board_edit_mode = None;
                        }
                        (Some(BoardEditMode::Edit { title, .. }), KeyCode::Backspace) => {
                            let mut val = title.value().to_string();
                            val.pop();
                            *title = Input::new(val);
                        }
                        (Some(BoardEditMode::Edit { title, .. }), KeyCode::Char(c)) => {
                            let mut val = title.value().to_string();
                            val.push(c);
                            *title = Input::new(val);
                        }
                        (Some(BoardEditMode::Delete { board_id, .. }), KeyCode::Char('y')) => {
                            self.action_tx.send(Action::DeleteBoard(board_id.clone()))?;
                            // board_edit_mode will be cleared by the action handler
                        }
                        (Some(BoardEditMode::Delete { .. }), KeyCode::Char('n') | KeyCode::Esc) => {
                            self.board_edit_mode = None;
                        }
                        _ => {}
                    }
                } else {
                    // Normal mode key handling
                    match key.code {
                        KeyCode::Char('q') => {
                            self.action_tx.send(Action::Quit)?;
                        }
                        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                            self.action_tx.send(Action::Quit)?;
                        }
                        KeyCode::Esc => {
                            self.action_tx.send(Action::Back)?;
                        }
                        KeyCode::Char('/') => {
                            self.command_focused = true;
                            self.command_input = Input::new("/".to_string());
                        }
                        KeyCode::Char('?') => {
                            self.action_tx.send(Action::Navigate(View::Help))?;
                        }
                        // Init wizard navigation
                        KeyCode::Up if self.view == View::Init => {
                            self.init_navigate_up();
                        }
                        KeyCode::Down if self.view == View::Init => {
                            self.init_navigate_down();
                        }
                        KeyCode::Char(' ') if self.view == View::Init => {
                            self.init_toggle_selection();
                        }
                        KeyCode::Enter if self.view == View::Init => {
                            self.init_confirm()?;
                        }
                        // Number keys for quick selection in persona step
                        KeyCode::Char(c @ '1'..='5')
                            if self.view == View::Init
                                && self.init_state.step == InitStep::Persona =>
                        {
                            self.selected_index = (c as usize) - ('1' as usize);
                            self.init_confirm()?;
                        }
                        // Tail view scrolling
                        KeyCode::Up if self.view == View::Tail => {
                            self.tail_scroll = self.tail_scroll.saturating_sub(1);
                        }
                        KeyCode::Down if self.view == View::Tail => {
                            if self.tail_scroll < self.tail_events.len().saturating_sub(1) {
                                self.tail_scroll += 1;
                            }
                        }
                        KeyCode::PageUp if self.view == View::Tail => {
                            self.tail_scroll = self.tail_scroll.saturating_sub(10);
                        }
                        KeyCode::PageDown if self.view == View::Tail => {
                            self.tail_scroll = (self.tail_scroll + 10)
                                .min(self.tail_events.len().saturating_sub(1));
                        }
                        KeyCode::Home if self.view == View::Tail => {
                            self.tail_scroll = 0;
                        }
                        KeyCode::End if self.view == View::Tail => {
                            self.tail_scroll = self.tail_events.len().saturating_sub(20);
                        }
                        // Login view handling
                        KeyCode::Tab if self.view == View::Login => {
                            self.login_state.focus = (self.login_state.focus + 1) % 2;
                        }
                        KeyCode::BackTab if self.view == View::Login => {
                            self.login_state.focus =
                                if self.login_state.focus == 0 { 1 } else { 0 };
                        }
                        KeyCode::Enter if self.view == View::Login => {
                            self.submit_login()?;
                        }
                        KeyCode::Backspace if self.view == View::Login => {
                            if self.login_state.focus == 0 {
                                // Delete from email Input
                                let event = crossterm::event::Event::Key(key);
                                self.login_state.email.handle_event(&event);
                            } else {
                                // Delete from password
                                self.login_state.password.pop();
                            }
                        }
                        KeyCode::Char(c) if self.view == View::Login => {
                            if self.login_state.focus == 0 {
                                // Type into email Input
                                let event = crossterm::event::Event::Key(key);
                                self.login_state.email.handle_event(&event);
                            } else {
                                // Type into password
                                self.login_state.password.push(c);
                            }
                        }
                        // Boards view navigation
                        KeyCode::Up if self.view == View::Boards => {
                            if self.selected_index > 0 {
                                self.selected_index -= 1;
                            }
                        }
                        KeyCode::Down if self.view == View::Boards => {
                            if self.selected_index < self.boards.len().saturating_sub(1) {
                                self.selected_index += 1;
                            }
                        }
                        KeyCode::Enter if self.view == View::Boards => {
                            if let Some(board) = self.boards.get(self.selected_index) {
                                let board_id = board.id.clone();
                                self.load_board_detail(&board_id)?;
                                self.action_tx.send(Action::Navigate(View::BoardDetail))?;
                            }
                        }
                        // Board management keys
                        KeyCode::Char('n') if self.view == View::Boards => {
                            // Start creating new board
                            self.board_edit_mode = Some(BoardEditMode::Create {
                                title: Input::default(),
                            });
                        }
                        KeyCode::Char('d') if self.view == View::Boards => {
                            // Delete selected board (with confirmation)
                            if let Some(board) = self.boards.get(self.selected_index) {
                                self.board_edit_mode = Some(BoardEditMode::Delete {
                                    board_id: board.id.clone(),
                                    title: board.title.clone(),
                                });
                            }
                        }
                        KeyCode::Char('p') if self.view == View::Boards => {
                            // Pin/unpin selected board
                            if let Some(board) = self.boards.get(self.selected_index) {
                                self.action_tx
                                    .send(Action::TogglePinBoard(board.id.clone()))?;
                            }
                        }
                        KeyCode::Char('s') if self.view == View::Boards => {
                            // Share/unshare selected board
                            if let Some(board) = self.boards.get(self.selected_index) {
                                if board.share_hash.is_some() {
                                    self.action_tx
                                        .send(Action::UnshareBoard(board.id.clone()))?;
                                } else {
                                    self.action_tx.send(Action::ShareBoard(board.id.clone()))?;
                                }
                            }
                        }
                        // BoardDetail view keys
                        KeyCode::Char('1') if self.view == View::BoardDetail => {
                            self.action_tx.send(Action::SetTimeRange(TimeRange::Week))?;
                        }
                        KeyCode::Char('2') if self.view == View::BoardDetail => {
                            self.action_tx
                                .send(Action::SetTimeRange(TimeRange::Month))?;
                        }
                        KeyCode::Char('3') if self.view == View::BoardDetail => {
                            self.action_tx
                                .send(Action::SetTimeRange(TimeRange::Quarter))?;
                        }
                        KeyCode::Char('c') if self.view == View::BoardDetail => {
                            self.action_tx.send(Action::ToggleComparison)?;
                        }
                        KeyCode::Char('r') if self.view == View::BoardDetail => {
                            self.action_tx.send(Action::RefreshMetrics)?;
                        }
                        KeyCode::Char('s') if self.view == View::BoardDetail => {
                            // Share the current board
                            if let Some(ref board) = self.current_board {
                                // Check if already shared
                                let is_shared = self
                                    .boards
                                    .iter()
                                    .find(|b| b.id == board.id)
                                    .map(|b| b.share_hash.is_some())
                                    .unwrap_or(false);
                                if is_shared {
                                    self.action_tx
                                        .send(Action::UnshareBoard(board.id.clone()))?;
                                } else {
                                    self.action_tx.send(Action::ShareBoard(board.id.clone()))?;
                                }
                            }
                        }
                        // Query view handling
                        KeyCode::Enter if self.view == View::Query && !self.query_state.loading => {
                            if !self.query_state.query.trim().is_empty() {
                                self.execute_query()?;
                            }
                        }
                        KeyCode::Backspace if self.view == View::Query => {
                            self.query_state.query.pop();
                            self.query_state.cursor = self.query_state.query.len();
                        }
                        KeyCode::Char('c')
                            if self.view == View::Query
                                && key.modifiers.contains(KeyModifiers::CONTROL) =>
                        {
                            // Clear query and results
                            self.query_state = super::action::QueryState::default();
                        }
                        KeyCode::Char(c) if self.view == View::Query => {
                            self.query_state.query.push(c);
                            self.query_state.cursor = self.query_state.query.len();
                        }
                        KeyCode::Up
                            if self.view == View::Query && self.query_state.result.is_some() =>
                        {
                            self.query_state.scroll = self.query_state.scroll.saturating_sub(1);
                        }
                        KeyCode::Down
                            if self.view == View::Query && self.query_state.result.is_some() =>
                        {
                            if let Some(ref result) = self.query_state.result
                                && self.query_state.scroll < result.rows.len().saturating_sub(1)
                            {
                                self.query_state.scroll += 1;
                            }
                        }
                        KeyCode::PageUp
                            if self.view == View::Query && self.query_state.result.is_some() =>
                        {
                            self.query_state.scroll = self.query_state.scroll.saturating_sub(10);
                        }
                        KeyCode::PageDown
                            if self.view == View::Query && self.query_state.result.is_some() =>
                        {
                            if let Some(ref result) = self.query_state.result {
                                self.query_state.scroll = (self.query_state.scroll + 10)
                                    .min(result.rows.len().saturating_sub(1));
                            }
                        }
                        _ => {}
                    }
                }
            }
            Event::Resize(_, _) => {
                self.action_tx.send(Action::Render)?;
            }
            Event::Tick => {
                // Check if server process is still running
                self.check_server_status();

                // Fetch metrics from control endpoint every ~1 second
                if self.server_status == ServerStatus::Running {
                    let should_fetch = self
                        .last_metrics_fetch
                        .map(|t| t.elapsed() > Duration::from_secs(1))
                        .unwrap_or(true);

                    if should_fetch {
                        self.last_metrics_fetch = Some(Instant::now());
                        self.spawn_metrics_fetch();
                    }
                }

                // Refresh board metrics every ~10 seconds when viewing BoardDetail
                if self.view == View::BoardDetail {
                    let should_refresh = self
                        .last_board_fetch
                        .map(|t| t.elapsed() > Duration::from_secs(10))
                        .unwrap_or(false); // Don't refresh immediately on first view

                    if should_refresh && let Some(ref board) = self.current_board {
                        self.last_board_fetch = Some(Instant::now());
                        let board_id = board.id.clone();
                        let _ = self.load_board_detail(&board_id);
                    }
                }

                self.action_tx.send(Action::Tick)?;
            }
            _ => {}
        }

        Ok(())
    }

    /// Navigate command history up.
    fn history_up(&mut self) {
        if self.command_history.is_empty() {
            return;
        }

        let new_index = match self.history_index {
            None => self.command_history.len().saturating_sub(1),
            Some(i) => i.saturating_sub(1),
        };

        self.history_index = Some(new_index);
        if let Some(cmd) = self.command_history.get(new_index) {
            self.command_input = Input::new(cmd.clone());
        }
    }

    /// Navigate command history down.
    fn history_down(&mut self) {
        if self.command_history.is_empty() {
            return;
        }

        match self.history_index {
            None => {}
            Some(i) => {
                if i >= self.command_history.len() - 1 {
                    self.history_index = None;
                    self.command_input = Input::new("/".to_string());
                } else {
                    let new_index = i + 1;
                    self.history_index = Some(new_index);
                    if let Some(cmd) = self.command_history.get(new_index) {
                        self.command_input = Input::new(cmd.clone());
                    }
                }
            }
        }
    }

    /// Navigate up in init wizard list.
    fn init_navigate_up(&mut self) {
        let max = self.init_list_len();
        if max > 0 && self.selected_index > 0 {
            self.selected_index -= 1;
        }
    }

    /// Navigate down in init wizard list.
    fn init_navigate_down(&mut self) {
        let max = self.init_list_len();
        if max > 0 && self.selected_index < max - 1 {
            self.selected_index += 1;
        }
    }

    /// Get the length of the current init wizard list.
    fn init_list_len(&self) -> usize {
        match self.init_state.step {
            InitStep::Persona => 5, // 5 personas
            InitStep::Sources => SourceType::all().len(),
            InitStep::Sinks => SinkType::all().len(),
            _ => 0,
        }
    }

    /// Toggle selection in init wizard (for sources/sinks).
    fn init_toggle_selection(&mut self) {
        match self.init_state.step {
            InitStep::Sources => {
                let sources = SourceType::all();
                if let Some(source) = sources.get(self.selected_index) {
                    if self.init_state.sources.contains(source) {
                        self.init_state.sources.retain(|s| s != source);
                    } else {
                        self.init_state.sources.push(*source);
                    }
                }
            }
            InitStep::Sinks => {
                let sinks = SinkType::all();
                if let Some(sink) = sinks.get(self.selected_index) {
                    if self.init_state.sinks.contains(sink) {
                        self.init_state.sinks.retain(|s| s != sink);
                    } else {
                        self.init_state.sinks.push(*sink);
                    }
                }
            }
            _ => {}
        }
    }

    /// Confirm current init wizard step.
    fn init_confirm(&mut self) -> Result<()> {
        use super::action::Persona;

        match self.init_state.step {
            InitStep::Persona => {
                let personas = [
                    Persona::MobileApp,
                    Persona::WebApp,
                    Persona::Backend,
                    Persona::LogInfra,
                    Persona::Custom,
                ];
                if let Some(persona) = personas.get(self.selected_index) {
                    self.init_state.persona = Some(*persona);
                    // Pre-select recommended sources
                    self.init_state.sources = persona.recommended_sources();
                    self.init_state.step = InitStep::Sources;
                    self.selected_index = 0;
                }
            }
            InitStep::Sources => {
                if self.init_state.sources.is_empty() {
                    self.action_tx
                        .send(Action::Error("Select at least one source".to_string()))?;
                } else {
                    // Pre-select recommended sinks
                    if let Some(persona) = self.init_state.persona {
                        self.init_state.sinks = persona.recommended_sinks();
                    }
                    self.init_state.step = InitStep::Sinks;
                    self.selected_index = 0;
                }
            }
            InitStep::Sinks => {
                if self.init_state.sinks.is_empty() {
                    self.action_tx
                        .send(Action::Error("Select at least one sink".to_string()))?;
                } else {
                    // Generate config file at the configured path
                    // Create parent directories if needed
                    if let Some(parent) = self.config_path.parent()
                        && !parent.as_os_str().is_empty()
                        && !parent.exists()
                    {
                        let _ = std::fs::create_dir_all(parent);
                    }
                    match self.generate_config_file(&self.config_path) {
                        Ok(_) => {
                            self.init_state.api_key = Some(self.config_path.display().to_string());
                            self.init_state.step = InitStep::ApiKey;
                        }
                        Err(e) => {
                            self.action_tx
                                .send(Action::Error(format!("Failed to write config: {}", e)))?;
                        }
                    }
                }
            }
            InitStep::ApiKey => {
                self.init_state.step = InitStep::Complete;
                self.has_config = true;
                // Auto-start the server
                self.start_server()?;
                // Navigate to dashboard
                self.action_tx.send(Action::Navigate(View::Dashboard))?;
            }
            InitStep::Complete => {
                self.action_tx.send(Action::Navigate(View::Dashboard))?;
            }
        }
        Ok(())
    }

    /// Generate a config file from the init state.
    fn generate_config_file(&self, path: &std::path::Path) -> Result<()> {
        use std::io::Write;

        let mut config = String::new();
        config.push_str("# Tell Configuration\n");
        config.push_str("# Generated by `tell i` init wizard\n\n");

        // Metrics section
        config.push_str("[metrics]\n");
        config.push_str("interval = \"1h\"\n\n");

        // Sources section
        config.push_str("# Sources\n");
        for source in &self.init_state.sources {
            let key = source.config_key();
            let port = source.default_port();
            match source {
                SourceType::Http => {
                    config.push_str(&format!("[sources.{}]\n", key));
                    config.push_str(&format!("port = {}\n\n", port));
                }
                SourceType::Tcp => {
                    config.push_str(&format!("[[sources.{}]]\n", key));
                    config.push_str(&format!("port = {}\n\n", port));
                }
                SourceType::SyslogTcp => {
                    config.push_str(&format!("[sources.{}]\n", key));
                    config.push_str("enabled = true\n");
                    config.push_str(&format!("port = {}\n", port));
                    config.push_str("workspace_id = \"1\"\n\n");
                }
                SourceType::SyslogUdp => {
                    config.push_str(&format!("[sources.{}]\n", key));
                    config.push_str("enabled = true\n");
                    config.push_str(&format!("port = {}\n", port));
                    config.push_str("workspace_id = \"1\"\n\n");
                }
            }
        }

        // Sinks section
        config.push_str("# Sinks\n");
        for sink in &self.init_state.sinks {
            let key = sink.config_key();
            match sink {
                SinkType::ClickHouse => {
                    config.push_str(&format!("[sinks.{}]\n", key));
                    config.push_str(&format!("type = \"{}\"\n", key));
                    config.push_str("host = \"localhost:8123\"\n");
                    config.push_str("database = \"default\"\n\n");
                }
                SinkType::ArrowIpc => {
                    config.push_str(&format!("[sinks.{}]\n", key));
                    config.push_str(&format!("type = \"{}\"\n", key));
                    config.push_str("path = \"data/arrow/\"\n\n");
                }
                SinkType::DiskBinary => {
                    config.push_str(&format!("[sinks.{}]\n", key));
                    config.push_str(&format!("type = \"{}\"\n", key));
                    config.push_str("path = \"data/archive/\"\n\n");
                }
                SinkType::Stdout => {
                    config.push_str(&format!("[sinks.{}]\n", key));
                    config.push_str(&format!("type = \"{}\"\n\n", key));
                }
            }
        }

        // Routing section
        config.push_str("# Routing\n");
        config.push_str("[routing]\n");
        let sink_names: Vec<_> = self
            .init_state
            .sinks
            .iter()
            .map(|s| format!("\"{}\"", s.config_key()))
            .collect();
        config.push_str(&format!("default = [{}]\n", sink_names.join(", ")));

        // Write the file
        let mut file = std::fs::File::create(path).context("failed to create config file")?;
        file.write_all(config.as_bytes())
            .context("failed to write config file")?;

        Ok(())
    }

    /// Generate a random API key.
    fn generate_api_key() -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("{:032x}", timestamp)[..24].to_string()
    }

    /// Start the Tell server as a child process.
    fn start_server(&mut self) -> Result<()> {
        // Check if already running
        if self.server_process.is_some() {
            self.action_tx
                .send(Action::Error("Server is already running".to_string()))?;
            return Ok(());
        }

        // Check if config exists
        if !self.config_path.exists() {
            self.action_tx.send(Action::Error(
                "No config file. Run /init first.".to_string(),
            ))?;
            return Ok(());
        }

        // Get the path to the current executable
        let exe = std::env::current_exe().context("failed to get current exe")?;

        // Spawn the server process
        self.server_status = ServerStatus::Starting;
        let child = Command::new(&exe)
            .arg("serve")
            .arg("--config")
            .arg(&self.config_path)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .context("failed to start server")?;

        self.server_process = Some(child);
        self.server_status = ServerStatus::Running;
        self.server_metrics = None;
        self.prev_metrics = None;
        self.last_metrics_fetch = None;
        Ok(())
    }

    /// Spawn a background task to fetch metrics from control endpoint.
    fn spawn_metrics_fetch(&self) {
        let action_tx = self.action_tx.clone();
        tokio::spawn(async move {
            let Ok(client) = reqwest::Client::builder()
                .timeout(Duration::from_secs(2))
                .build()
            else {
                return;
            };

            if let Ok(resp) = client.get("http://127.0.0.1:3000/metrics").send().await
                && let Ok(text) = resp.text().await
            {
                let _ = action_tx.send(Action::MetricsUpdate(text));
            }
        });
    }

    /// Stop the Tell server.
    fn stop_server(&mut self) -> Result<()> {
        if let Some(mut child) = self.server_process.take() {
            self.server_status = ServerStatus::Stopping;

            // Try graceful shutdown first (SIGTERM on Unix via kill command)
            #[cfg(unix)]
            {
                let pid = child.id();
                let _ = Command::new("kill")
                    .arg("-TERM")
                    .arg(pid.to_string())
                    .output();
                // Give it a moment to shut down gracefully
                std::thread::sleep(Duration::from_millis(500));
            }

            // Force kill if still running
            let _ = child.kill();
            let _ = child.wait();

            self.server_status = ServerStatus::Stopped;
            self.server_metrics = None;
            Ok(())
        } else {
            self.action_tx
                .send(Action::Error("Server is not running".to_string()))?;
            Ok(())
        }
    }

    /// Check server process status (call periodically).
    fn check_server_status(&mut self) {
        if let Some(ref mut child) = self.server_process {
            match child.try_wait() {
                Ok(Some(status)) => {
                    // Process exited
                    self.server_process = None;
                    if status.success() {
                        self.server_status = ServerStatus::Stopped;
                    } else {
                        self.server_status = ServerStatus::Error;
                    }
                }
                Ok(None) => {
                    // Still running
                    self.server_status = ServerStatus::Running;
                }
                Err(_) => {
                    self.server_status = ServerStatus::Error;
                }
            }
        }
    }

    /// Start tailing events from the tap socket (Unix only).
    #[cfg(unix)]
    fn start_tail(&mut self) -> Result<()> {
        // Clear previous events
        self.tail_events.clear();
        self.tail_scroll = 0;

        // Check if server is running
        if self.server_process.is_none() {
            self.tail_connected = false;
            return Ok(());
        }

        let action_tx = self.action_tx.clone();

        // Spawn a task to connect and stream events
        tokio::spawn(async move {
            use bytes::{Buf, BytesMut};
            use tell_tap::{SubscribeRequest, TapMessage};
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            use tokio::net::UnixStream;

            let socket_path = "/tmp/tell-tap.sock";

            // Try to connect
            let mut stream = match UnixStream::connect(socket_path).await {
                Ok(s) => s,
                Err(e) => {
                    let _ = action_tx.send(Action::NewEvent(format!(
                        "[error] Could not connect to tap socket: {}",
                        e
                    )));
                    return;
                }
            };

            // Send subscribe request (subscribe to all with last 10 events)
            let subscribe = TapMessage::Subscribe(SubscribeRequest::new().with_last_n(10));
            let encoded = subscribe.encode();
            if let Err(e) = stream.write_all(&encoded).await {
                let _ = action_tx.send(Action::NewEvent(format!(
                    "[error] Failed to send subscribe: {}",
                    e
                )));
                return;
            }

            // Read loop with proper framing
            let mut read_buf = BytesMut::with_capacity(64 * 1024);

            loop {
                // Try to parse complete messages from buffer
                while read_buf.len() >= 4 {
                    let len =
                        u32::from_be_bytes([read_buf[0], read_buf[1], read_buf[2], read_buf[3]])
                            as usize;

                    if read_buf.len() < 4 + len {
                        break; // Need more data
                    }

                    // Extract complete message
                    read_buf.advance(4);
                    let payload = read_buf.split_to(len).freeze();

                    match TapMessage::decode(payload) {
                        Ok(TapMessage::Batch(envelope)) => {
                            let event_str = format_tap_envelope(&envelope);
                            // Split multi-line output into separate events
                            for line in event_str.lines() {
                                let _ = action_tx.send(Action::NewEvent(line.to_string()));
                            }
                        }
                        Ok(TapMessage::Heartbeat) => {
                            // Ignore heartbeats
                        }
                        Ok(TapMessage::Error(msg)) => {
                            let _ = action_tx.send(Action::NewEvent(format!("[error] {}", msg)));
                        }
                        Ok(TapMessage::Subscribe(_)) => {
                            // Shouldn't receive this from server
                        }
                        Err(e) => {
                            let _ = action_tx
                                .send(Action::NewEvent(format!("[error] Decode error: {}", e)));
                        }
                    }
                }

                // Read more data from socket
                match stream.read_buf(&mut read_buf).await {
                    Ok(0) => {
                        let _ = action_tx.send(Action::NewEvent(
                            "[system] Tap connection closed".to_string(),
                        ));
                        break;
                    }
                    Ok(_) => {
                        // Continue processing
                    }
                    Err(e) => {
                        let _ =
                            action_tx.send(Action::NewEvent(format!("[error] Tap read: {}", e)));
                        break;
                    }
                }
            }
        });

        self.tail_connected = true;
        Ok(())
    }

    /// Send test events to the running server via TCP (like `tell test`).
    /// Submit login form.
    fn submit_login(&mut self) -> Result<()> {
        let email = self.login_state.email.value().to_string();
        let password = self.login_state.password.clone();

        if email.is_empty() || password.is_empty() {
            self.login_state.error = Some("Email and password required".to_string());
            return Ok(());
        }

        // Get API URL from config
        let api_url = self.get_api_url();
        let action_tx = self.action_tx.clone();

        self.login_state.loading = true;
        self.login_state.error = None;

        tokio::spawn(async move {
            let client = reqwest::Client::new();
            let result = client
                .post(format!("{}/api/v1/auth/login", api_url))
                .json(&serde_json::json!({
                    "email": email,
                    "password": password,
                }))
                .send()
                .await;

            match result {
                Ok(response) if response.status().is_success() => {
                    #[derive(serde::Deserialize)]
                    struct LoginResponse {
                        access_token: String,
                    }

                    if let Ok(login_resp) = response.json::<LoginResponse>().await {
                        // Save credentials
                        let creds = crate::cmd::auth::AuthCredentials {
                            access_token: login_resp.access_token,
                            api_url: api_url.clone(),
                            email: email.clone(),
                        };
                        let _ = crate::cmd::auth::save_credentials(&creds);
                        let _ = action_tx.send(Action::LoginSuccess(email));
                    } else {
                        let _ = action_tx.send(Action::LoginFailed("Invalid response".to_string()));
                    }
                }
                Ok(response) if response.status().as_u16() == 401 => {
                    let _ = action_tx
                        .send(Action::LoginFailed("Invalid email or password".to_string()));
                }
                Ok(response) => {
                    let _ = action_tx.send(Action::LoginFailed(format!(
                        "Login failed ({})",
                        response.status()
                    )));
                }
                Err(e) => {
                    let _ = action_tx.send(Action::LoginFailed(format!("Connection error: {}", e)));
                }
            }
        });

        Ok(())
    }

    /// Get API URL from config or use default.
    fn get_api_url(&self) -> String {
        // Try to read from config
        if self.config_path.exists()
            && let Ok(content) = std::fs::read_to_string(&self.config_path)
            && let Ok(toml_value) = toml::from_str::<toml::Value>(&content)
            && let Some(api_section) = toml_value.get("api")
            && let Some(url) = api_section.get("url").and_then(|v| v.as_str())
        {
            return url.to_string();
        }
        "http://localhost:3000".to_string()
    }

    /// Load boards from API.
    fn load_boards(&mut self) -> Result<()> {
        let Some(ref auth) = self.auth else {
            return Ok(());
        };

        let api_url = auth.api_url.clone();
        let token = auth.access_token.clone();
        let action_tx = self.action_tx.clone();

        tokio::spawn(async move {
            let client = reqwest::Client::new();
            let result = client
                .get(format!("{}/api/v1/boards", api_url))
                .header("Authorization", format!("Bearer {}", token))
                .send()
                .await;

            match result {
                Ok(response) if response.status().is_success() => {
                    #[derive(serde::Deserialize)]
                    struct BoardsResponse {
                        boards: Vec<BoardApiInfo>,
                    }

                    #[derive(serde::Deserialize)]
                    struct BoardApiInfo {
                        id: String,
                        title: String,
                        description: Option<String>,
                        is_pinned: bool,
                        share_hash: Option<String>,
                        settings: Option<BoardSettings>,
                    }

                    #[derive(serde::Deserialize)]
                    struct BoardSettings {
                        blocks: Option<Vec<serde_json::Value>>,
                    }

                    if let Ok(resp) = response.json::<BoardsResponse>().await {
                        let boards: Vec<super::action::BoardInfo> = resp
                            .boards
                            .into_iter()
                            .map(|b| super::action::BoardInfo {
                                id: b.id,
                                title: b.title,
                                description: b.description,
                                is_pinned: b.is_pinned,
                                share_hash: b.share_hash,
                                block_count: b
                                    .settings
                                    .and_then(|s| s.blocks)
                                    .map(|b| b.len())
                                    .unwrap_or(0),
                            })
                            .collect();
                        let _ = action_tx.send(Action::BoardsLoaded(boards));
                    }
                }
                Ok(response) if response.status().as_u16() == 401 => {
                    let _ = action_tx.send(Action::Error(
                        "Session expired. Please /login again.".to_string(),
                    ));
                }
                Ok(response) => {
                    let _ = action_tx.send(Action::Error(format!(
                        "Failed to load boards ({})",
                        response.status()
                    )));
                }
                Err(e) => {
                    let _ = action_tx.send(Action::Error(format!("Connection error: {}", e)));
                }
            }
        });

        Ok(())
    }

    /// Load a specific board's details.
    fn load_board_detail(&mut self, board_id: &str) -> Result<()> {
        let Some(ref auth) = self.auth else {
            return Ok(());
        };

        let api_url = auth.api_url.clone();
        let token = auth.access_token.clone();
        let board_id = board_id.to_string();
        let action_tx = self.action_tx.clone();
        let time_range = self.time_range;
        let show_comparison = self.show_comparison;

        tokio::spawn(async move {
            let client = reqwest::Client::new();

            // First fetch the board to get metric blocks
            let result = client
                .get(format!("{}/api/v1/boards/{}", api_url, board_id))
                .header("Authorization", format!("Bearer {}", token))
                .send()
                .await;

            #[derive(serde::Deserialize)]
            struct BoardResponse {
                id: String,
                title: String,
                settings: Option<BoardSettings>,
            }

            #[derive(serde::Deserialize)]
            struct BoardSettings {
                blocks: Option<Vec<BlockInfo>>,
            }

            #[derive(serde::Deserialize, Clone)]
            struct BlockInfo {
                #[serde(rename = "type")]
                block_type: String,
                metric: Option<MetricInfo>,
            }

            #[derive(serde::Deserialize, Clone)]
            struct MetricInfo {
                title: Option<String>,
                metric_type: Option<String>,
            }

            // API response for metrics
            #[derive(serde::Deserialize)]
            struct ApiResponse<T> {
                data: T,
            }

            #[derive(serde::Deserialize)]
            struct TimeSeriesData {
                total: f64,
                comparison: Option<ComparisonData>,
            }

            #[derive(serde::Deserialize)]
            struct ComparisonData {
                percent_change: f64,
            }

            match result {
                Ok(response) if response.status().is_success() => {
                    if let Ok(resp) = response.json::<BoardResponse>().await {
                        let blocks = resp.settings.and_then(|s| s.blocks).unwrap_or_default();

                        // Collect metric types we need to fetch
                        let metric_blocks: Vec<_> = blocks
                            .iter()
                            .filter(|b| b.block_type == "metric")
                            .filter_map(|b| b.metric.as_ref())
                            .collect();

                        // Fetch actual values for each metric type
                        let mut metrics = Vec::new();
                        for m in metric_blocks {
                            let metric_type = m.metric_type.clone().unwrap_or_default();
                            let title = m
                                .title
                                .clone()
                                .or_else(|| Some(metric_type.to_uppercase()))
                                .unwrap_or_else(|| "Metric".to_string());

                            // Map metric_type to API endpoint
                            let endpoint = match metric_type.as_str() {
                                "dau" => "/api/v1/metrics/dau",
                                "wau" => "/api/v1/metrics/wau",
                                "mau" => "/api/v1/metrics/mau",
                                "events" => "/api/v1/metrics/events",
                                "logs" => "/api/v1/metrics/logs",
                                "sessions" => "/api/v1/metrics/sessions",
                                _ => continue, // Unknown metric type
                            };

                            // Build URL with range and optional comparison
                            let mut url =
                                format!("{}{}?range={}", api_url, endpoint, time_range.as_param());
                            if show_comparison {
                                url.push_str("&compare=previous");
                            }

                            // Fetch the metric value
                            let metric_result = client
                                .get(&url)
                                .header("Authorization", format!("Bearer {}", token))
                                .send()
                                .await;

                            let (value, change_percent) = match metric_result {
                                Ok(resp) if resp.status().is_success() => {
                                    if let Ok(data) =
                                        resp.json::<ApiResponse<TimeSeriesData>>().await
                                    {
                                        let change = data.data.comparison.map(|c| c.percent_change);
                                        (data.data.total, change)
                                    } else {
                                        (0.0, None)
                                    }
                                }
                                _ => (0.0, None),
                            };

                            metrics.push(super::action::MetricValue {
                                title,
                                value,
                                change_percent,
                            });
                        }

                        let board_data = super::action::BoardData {
                            id: resp.id,
                            title: resp.title,
                            metrics,
                        };
                        let _ = action_tx.send(Action::BoardDataLoaded(board_data));
                    }
                }
                Ok(response) => {
                    let _ = action_tx.send(Action::Error(format!(
                        "Failed to load board ({})",
                        response.status()
                    )));
                }
                Err(e) => {
                    let _ = action_tx.send(Action::Error(format!("Connection error: {}", e)));
                }
            }
        });

        Ok(())
    }

    /// Execute a SQL query via the API.
    fn execute_query(&mut self) -> Result<()> {
        let Some(ref auth) = self.auth else {
            self.query_state.error = Some("Login required. Use /login".to_string());
            return Ok(());
        };

        let api_url = auth.api_url.clone();
        let token = auth.access_token.clone();
        let query = self.query_state.query.clone();
        let action_tx = self.action_tx.clone();

        self.query_state.loading = true;
        self.query_state.error = None;
        self.query_state.result = None;
        self.query_state.scroll = 0;

        tokio::spawn(async move {
            let client = reqwest::Client::new();
            let result = client
                .post(format!("{}/api/v1/data/query", api_url))
                .header("Authorization", format!("Bearer {}", token))
                .json(&serde_json::json!({
                    "query": query,
                    "limit": 1000,
                }))
                .send()
                .await;

            #[derive(serde::Deserialize)]
            struct ApiResponse<T> {
                data: T,
            }

            #[derive(serde::Deserialize)]
            struct QueryResponse {
                columns: Vec<ColumnInfo>,
                rows: Vec<Vec<serde_json::Value>>,
                row_count: usize,
                execution_time_ms: u64,
            }

            #[derive(serde::Deserialize)]
            struct ColumnInfo {
                name: String,
            }

            match result {
                Ok(response) if response.status().is_success() => {
                    if let Ok(resp) = response.json::<ApiResponse<QueryResponse>>().await {
                        let query_result = super::action::QueryResult {
                            columns: resp.data.columns.into_iter().map(|c| c.name).collect(),
                            rows: resp
                                .data
                                .rows
                                .into_iter()
                                .map(|row| {
                                    row.into_iter()
                                        .map(|v| match v {
                                            serde_json::Value::Null => "NULL".to_string(),
                                            serde_json::Value::String(s) => s,
                                            other => other.to_string(),
                                        })
                                        .collect()
                                })
                                .collect(),
                            row_count: resp.data.row_count,
                            execution_time_ms: resp.data.execution_time_ms,
                        };
                        let _ = action_tx.send(Action::QueryResult(query_result));
                    } else {
                        let _ = action_tx.send(Action::QueryFailed("Invalid response".to_string()));
                    }
                }
                Ok(response) if response.status().as_u16() == 401 => {
                    let _ = action_tx.send(Action::QueryFailed(
                        "Session expired. Please /login again.".to_string(),
                    ));
                }
                Ok(response) if response.status().as_u16() == 400 => {
                    // Try to get error message from response
                    let body = response.text().await.unwrap_or_default();
                    let msg = if let Ok(json) = serde_json::from_str::<serde_json::Value>(&body) {
                        json["error"]["message"]
                            .as_str()
                            .or(json["message"].as_str())
                            .unwrap_or("Invalid query")
                            .to_string()
                    } else {
                        "Invalid query".to_string()
                    };
                    let _ = action_tx.send(Action::QueryFailed(msg));
                }
                Ok(response) => {
                    let _ = action_tx.send(Action::QueryFailed(format!(
                        "Query failed ({})",
                        response.status()
                    )));
                }
                Err(e) => {
                    let _ = action_tx.send(Action::QueryFailed(format!("Connection error: {}", e)));
                }
            }
        });

        Ok(())
    }

    /// Create a new board.
    fn create_board(&mut self, title: &str) -> Result<()> {
        let Some(ref auth) = self.auth else {
            self.error_message = Some("Login required".to_string());
            return Ok(());
        };

        let api_url = auth.api_url.clone();
        let token = auth.access_token.clone();
        let title = title.to_string();
        let action_tx = self.action_tx.clone();

        tokio::spawn(async move {
            let client = reqwest::Client::new();
            let result = client
                .post(format!("{}/api/v1/boards", api_url))
                .header("Authorization", format!("Bearer {}", token))
                .json(&serde_json::json!({ "title": title }))
                .send()
                .await;

            match result {
                Ok(response) if response.status().is_success() => {
                    let _ = action_tx.send(Action::BoardUpdated);
                }
                Ok(response) => {
                    let _ = action_tx.send(Action::Error(format!(
                        "Failed to create board ({})",
                        response.status()
                    )));
                }
                Err(e) => {
                    let _ = action_tx.send(Action::Error(format!("Connection error: {}", e)));
                }
            }
        });

        Ok(())
    }

    /// Update a board's title.
    fn update_board(&mut self, board_id: &str, title: &str) -> Result<()> {
        let Some(ref auth) = self.auth else {
            self.error_message = Some("Login required".to_string());
            return Ok(());
        };

        let api_url = auth.api_url.clone();
        let token = auth.access_token.clone();
        let board_id = board_id.to_string();
        let title = title.to_string();
        let action_tx = self.action_tx.clone();

        tokio::spawn(async move {
            let client = reqwest::Client::new();
            let result = client
                .put(format!("{}/api/v1/boards/{}", api_url, board_id))
                .header("Authorization", format!("Bearer {}", token))
                .json(&serde_json::json!({ "title": title }))
                .send()
                .await;

            match result {
                Ok(response) if response.status().is_success() => {
                    let _ = action_tx.send(Action::BoardUpdated);
                }
                Ok(response) => {
                    let _ = action_tx.send(Action::Error(format!(
                        "Failed to update board ({})",
                        response.status()
                    )));
                }
                Err(e) => {
                    let _ = action_tx.send(Action::Error(format!("Connection error: {}", e)));
                }
            }
        });

        Ok(())
    }

    /// Delete a board.
    fn delete_board(&mut self, board_id: &str) -> Result<()> {
        let Some(ref auth) = self.auth else {
            self.error_message = Some("Login required".to_string());
            return Ok(());
        };

        let api_url = auth.api_url.clone();
        let token = auth.access_token.clone();
        let board_id = board_id.to_string();
        let action_tx = self.action_tx.clone();

        tokio::spawn(async move {
            let client = reqwest::Client::new();
            let result = client
                .delete(format!("{}/api/v1/boards/{}", api_url, board_id))
                .header("Authorization", format!("Bearer {}", token))
                .send()
                .await;

            match result {
                Ok(response) if response.status().is_success() => {
                    let _ = action_tx.send(Action::BoardDeleted(board_id));
                }
                Ok(response) => {
                    let _ = action_tx.send(Action::Error(format!(
                        "Failed to delete board ({})",
                        response.status()
                    )));
                }
                Err(e) => {
                    let _ = action_tx.send(Action::Error(format!("Connection error: {}", e)));
                }
            }
        });

        Ok(())
    }

    /// Toggle pin status for a board.
    fn toggle_pin_board(&mut self, board_id: &str) -> Result<()> {
        let Some(ref auth) = self.auth else {
            self.error_message = Some("Login required".to_string());
            return Ok(());
        };

        // Find current pin status
        let is_currently_pinned = self
            .boards
            .iter()
            .find(|b| b.id == board_id)
            .map(|b| b.is_pinned)
            .unwrap_or(false);

        let api_url = auth.api_url.clone();
        let token = auth.access_token.clone();
        let board_id = board_id.to_string();
        let action_tx = self.action_tx.clone();

        tokio::spawn(async move {
            let client = reqwest::Client::new();
            let endpoint = if is_currently_pinned {
                format!("{}/api/v1/boards/{}/unpin", api_url, board_id)
            } else {
                format!("{}/api/v1/boards/{}/pin", api_url, board_id)
            };

            let result = client
                .post(&endpoint)
                .header("Authorization", format!("Bearer {}", token))
                .send()
                .await;

            match result {
                Ok(response) if response.status().is_success() => {
                    let _ = action_tx.send(Action::BoardUpdated);
                }
                Ok(response) => {
                    let _ = action_tx.send(Action::Error(format!(
                        "Failed to toggle pin ({})",
                        response.status()
                    )));
                }
                Err(e) => {
                    let _ = action_tx.send(Action::Error(format!("Connection error: {}", e)));
                }
            }
        });

        Ok(())
    }

    /// Share a board (create public link).
    fn share_board(&mut self, board_id: &str) -> Result<()> {
        let Some(ref auth) = self.auth else {
            self.error_message = Some("Login required".to_string());
            return Ok(());
        };

        let api_url = auth.api_url.clone();
        let token = auth.access_token.clone();
        let board_id = board_id.to_string();
        let action_tx = self.action_tx.clone();

        tokio::spawn(async move {
            let client = reqwest::Client::new();
            let result = client
                .post(format!("{}/api/v1/boards/{}/share", api_url, board_id))
                .header("Authorization", format!("Bearer {}", token))
                .send()
                .await;

            #[derive(serde::Deserialize)]
            struct ShareResponse {
                share_url: String,
            }

            match result {
                Ok(response) if response.status().is_success() => {
                    if let Ok(resp) = response.json::<ShareResponse>().await {
                        let _ = action_tx.send(Action::BoardShared(board_id, resp.share_url));
                    }
                }
                Ok(response) => {
                    let _ = action_tx.send(Action::Error(format!(
                        "Failed to share board ({})",
                        response.status()
                    )));
                }
                Err(e) => {
                    let _ = action_tx.send(Action::Error(format!("Connection error: {}", e)));
                }
            }
        });

        Ok(())
    }

    /// Unshare a board (revoke public link).
    fn unshare_board(&mut self, board_id: &str) -> Result<()> {
        let Some(ref auth) = self.auth else {
            self.error_message = Some("Login required".to_string());
            return Ok(());
        };

        let api_url = auth.api_url.clone();
        let token = auth.access_token.clone();
        let board_id = board_id.to_string();
        let action_tx = self.action_tx.clone();

        tokio::spawn(async move {
            let client = reqwest::Client::new();
            let result = client
                .delete(format!("{}/api/v1/boards/{}/share", api_url, board_id))
                .header("Authorization", format!("Bearer {}", token))
                .send()
                .await;

            match result {
                Ok(response) if response.status().is_success() => {
                    let _ = action_tx.send(Action::BoardUnshared(board_id));
                }
                Ok(response) => {
                    let _ = action_tx.send(Action::Error(format!(
                        "Failed to unshare board ({})",
                        response.status()
                    )));
                }
                Err(e) => {
                    let _ = action_tx.send(Action::Error(format!("Connection error: {}", e)));
                }
            }
        });

        Ok(())
    }

    /// Load top events.
    fn load_top_events(&mut self) -> Result<()> {
        let Some(ref auth) = self.auth else {
            return Ok(());
        };

        let api_url = auth.api_url.clone();
        let token = auth.access_token.clone();
        let time_range = self.time_range;
        let action_tx = self.action_tx.clone();

        tokio::spawn(async move {
            let client = reqwest::Client::new();
            let result = client
                .get(format!(
                    "{}/api/v1/metrics/events/top?range={}",
                    api_url,
                    time_range.as_param()
                ))
                .header("Authorization", format!("Bearer {}", token))
                .send()
                .await;

            #[derive(serde::Deserialize)]
            struct ApiResponse {
                data: TopEventsData,
            }

            #[derive(serde::Deserialize)]
            struct TopEventsData {
                events: Vec<EventInfo>,
            }

            #[derive(serde::Deserialize)]
            struct EventInfo {
                name: String,
                count: u64,
            }

            match result {
                Ok(response) if response.status().is_success() => {
                    if let Ok(resp) = response.json::<ApiResponse>().await {
                        let events: Vec<super::action::TopEvent> = resp
                            .data
                            .events
                            .into_iter()
                            .map(|e| super::action::TopEvent {
                                name: e.name,
                                count: e.count,
                            })
                            .collect();
                        let _ = action_tx.send(Action::TopEventsLoaded(events));
                    }
                }
                _ => {}
            }
        });

        Ok(())
    }

    /// Load stickiness metrics.
    fn load_stickiness(&mut self) -> Result<()> {
        let Some(ref auth) = self.auth else {
            return Ok(());
        };

        let api_url = auth.api_url.clone();
        let token = auth.access_token.clone();
        let time_range = self.time_range;
        let action_tx = self.action_tx.clone();

        tokio::spawn(async move {
            let client = reqwest::Client::new();

            #[derive(serde::Deserialize)]
            struct ApiResponse {
                data: StickinessResponse,
            }

            #[derive(serde::Deserialize)]
            struct StickinessResponse {
                ratio: f64,
            }

            // Fetch daily stickiness (DAU/MAU)
            let daily_result = client
                .get(format!(
                    "{}/api/v1/metrics/stickiness/daily?range={}",
                    api_url,
                    time_range.as_param()
                ))
                .header("Authorization", format!("Bearer {}", token))
                .send()
                .await;

            let daily_ratio = match daily_result {
                Ok(response) if response.status().is_success() => response
                    .json::<ApiResponse>()
                    .await
                    .map(|r| r.data.ratio)
                    .unwrap_or(0.0),
                _ => 0.0,
            };

            // Fetch weekly stickiness (DAU/WAU)
            let weekly_result = client
                .get(format!(
                    "{}/api/v1/metrics/stickiness/weekly?range={}",
                    api_url,
                    time_range.as_param()
                ))
                .header("Authorization", format!("Bearer {}", token))
                .send()
                .await;

            let weekly_ratio = match weekly_result {
                Ok(response) if response.status().is_success() => response
                    .json::<ApiResponse>()
                    .await
                    .map(|r| r.data.ratio)
                    .unwrap_or(0.0),
                _ => 0.0,
            };

            let _ = action_tx.send(Action::StickinessLoaded(super::action::StickinessData {
                daily_ratio,
                weekly_ratio,
            }));
        });

        Ok(())
    }

    fn send_test_events(&mut self, count: usize) -> Result<()> {
        // Check if server is running
        if self.server_process.is_none() {
            self.action_tx.send(Action::Error(
                "Server not running. Use /start first.".to_string(),
            ))?;
            return Ok(());
        }

        let action_tx = self.action_tx.clone();

        // Spawn a task to send test events via TCP (like CLI `tell test`)
        tokio::spawn(async move {
            use tell_client::batch::BatchBuilder;
            use tell_client::event::{EventBuilder, EventDataBuilder};
            use tell_client::log::{LogDataBuilder, LogEntryBuilder};
            use tell_client::test::TcpTestClient;

            // Default API key (matches typical test configs)
            let api_key: [u8; 16] = [
                0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
                0x0e, 0x0f,
            ];

            let server = "127.0.0.1:50000";
            let start = Instant::now();

            // Connect to TCP source
            let mut client = match TcpTestClient::connect(server).await {
                Ok(c) => c,
                Err(e) => {
                    let _ = action_tx.send(Action::NewEvent(format!(
                        "[test] Failed to connect to {}: {}",
                        server, e
                    )));
                    return;
                }
            };

            // Send events
            for i in 0..count {
                let device_id: [u8; 16] = {
                    let mut id = [0u8; 16];
                    id[15] = i as u8;
                    id
                };

                let Ok(event) = EventBuilder::new()
                    .track(&format!("test_event_{}", i))
                    .device_id(device_id)
                    .timestamp_now()
                    .payload_json(&format!(
                        r#"{{"index": {}, "test": true, "source": "tell tui"}}"#,
                        i
                    ))
                    .build()
                else {
                    continue;
                };

                let Ok(event_data) = EventDataBuilder::new().add(event).build() else {
                    continue;
                };

                let Ok(batch) = BatchBuilder::new()
                    .api_key(api_key)
                    .event_data(event_data)
                    .build()
                else {
                    continue;
                };

                let _ = client.send(&batch).await;
            }

            // Send logs
            for i in 0..count {
                let log = match (match i % 3 {
                    0 => LogEntryBuilder::new().info(),
                    1 => LogEntryBuilder::new().warning(),
                    _ => LogEntryBuilder::new().error(),
                })
                .source("localhost")
                .service("tell-test")
                .timestamp_now()
                .payload_json(&format!(
                    r#"{{"message": "Test log {}", "index": {}, "source": "tell tui"}}"#,
                    i, i
                ))
                .build()
                {
                    Ok(l) => l,
                    Err(_) => continue,
                };

                let log_data = match LogDataBuilder::new().add(log).build() {
                    Ok(d) => d,
                    Err(_) => continue,
                };

                let batch = match BatchBuilder::new()
                    .api_key(api_key)
                    .log_data(log_data)
                    .build()
                {
                    Ok(b) => b,
                    Err(_) => continue,
                };

                let _ = client.send(&batch).await;
            }

            let _ = client.flush().await;
            let _ = client.close().await;

            let elapsed = start.elapsed();
            let _ = action_tx.send(Action::NewEvent(format!(
                "[test] Sent {} events and {} logs in {:?}",
                count, count, elapsed
            )));
        });

        Ok(())
    }

    /// Available commands with descriptions.
    fn commands() -> Vec<(&'static str, &'static str)> {
        vec![
            ("init", "Setup wizard"),
            ("config", "View config"),
            ("start", "Start server"),
            ("stop", "Stop server"),
            ("test", "Send test events"),
            ("tail", "Live event stream"),
            ("login", "Authenticate"),
            ("boards", "View boards"),
            ("help", "Show help"),
            ("quit", "Exit"),
        ]
    }

    /// Render command popup overlay.
    fn render_command_popup(
        frame: &mut ratatui::Frame,
        area: Rect,
        theme: &Theme,
        input: &str,
        cursor_pos: usize,
        selected_cmd_index: usize,
    ) {
        let commands = Self::commands();

        // Filter commands based on input
        let filter = input.strip_prefix('/').unwrap_or(input).to_lowercase();
        let filtered: Vec<_> = commands
            .iter()
            .filter(|(cmd, _)| filter.is_empty() || cmd.starts_with(&filter))
            .collect();

        // Calculate popup size - full width with margins, minimum height to prevent jumping
        let min_visible_cmds = 4; // Show at least 4 command slots to prevent jumping
        let cmd_count = filtered.len().max(min_visible_cmds);
        let popup_height = (cmd_count + 2).min(14) as u16; // +2 for input + border
        let popup_width = area.width.saturating_sub(2); // Full width with 1 margin each side

        // Position popup at bottom of screen
        let popup_y = area.height.saturating_sub(popup_height + 1);
        let popup_x = 1;

        let popup_area = Rect {
            x: popup_x,
            y: popup_y,
            width: popup_width,
            height: popup_height,
        };

        // Clear background
        frame.render_widget(Clear, popup_area);

        // Draw popup with subtle border
        let popup_block = Block::default()
            .borders(Borders::TOP)
            .border_style(theme.muted_style());
        let inner = popup_block.inner(popup_area);
        frame.render_widget(popup_block, popup_area);

        // Split inner area: commands list + input line
        let inner_chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Min(1),    // Commands
                Constraint::Length(1), // Input
            ])
            .split(inner);

        // Render filtered commands
        let mut cmd_lines: Vec<Line> = Vec::new();
        for (i, (cmd, desc)) in filtered.iter().enumerate() {
            let is_selected = i == selected_cmd_index;
            let (cmd_style, desc_style, bg_style) = if is_selected {
                // Highlight selected command with brand color and background
                (
                    theme.selected_bg_style(),
                    theme.selection_bg_style(),
                    theme.selection_bg_style(),
                )
            } else {
                (Style::default(), theme.muted_style(), Style::default())
            };

            // Calculate padding to fill the row with background
            let content_len = 1 + 1 + 12 + 1 + desc.len(); // space + / + cmd + space + desc
            let padding = inner_chunks[0].width.saturating_sub(content_len as u16) as usize;

            cmd_lines.push(Line::from(vec![
                Span::styled(" ", bg_style),
                Span::styled(format!("/{:<12}", cmd), cmd_style),
                Span::styled(format!(" {}", desc), desc_style),
                Span::styled(" ".repeat(padding), bg_style),
            ]));
        }

        if cmd_lines.is_empty() {
            cmd_lines.push(Line::from(vec![Span::styled(
                " No matching commands",
                theme.muted_style(),
            )]));
        }

        frame.render_widget(Paragraph::new(cmd_lines), inner_chunks[0]);

        // Render input line with prompt
        let input_line = Line::from(vec![
            Span::raw(" "),
            Span::styled(input, theme.brand_style()),
        ]);
        frame.render_widget(Paragraph::new(input_line), inner_chunks[1]);

        // Position cursor after the input text (accounting for leading space)
        frame.set_cursor_position((inner_chunks[1].x + 1 + cursor_pos as u16, inner_chunks[1].y));
    }

    /// Render board edit dialog overlay.
    fn render_board_edit_dialog(
        frame: &mut ratatui::Frame,
        area: Rect,
        theme: &Theme,
        edit_mode: &BoardEditMode,
    ) {
        let (title, content_lines, show_input) = match edit_mode {
            BoardEditMode::Create { title } => (
                "New Board",
                vec![
                    Line::from(vec![
                        Span::raw(" Title: "),
                        Span::styled(title.value(), theme.brand_style()),
                        Span::styled("_", Style::default().add_modifier(Modifier::SLOW_BLINK)),
                    ]),
                    Line::from(""),
                    Line::from(vec![
                        Span::styled(" Enter", Style::default().bold()),
                        Span::styled(" create  ", theme.muted_style()),
                        Span::styled("Esc", Style::default().bold()),
                        Span::styled(" cancel", theme.muted_style()),
                    ]),
                ],
                true,
            ),
            BoardEditMode::Edit { title, .. } => (
                "Edit Board",
                vec![
                    Line::from(vec![
                        Span::raw(" Title: "),
                        Span::styled(title.value(), theme.brand_style()),
                        Span::styled("_", Style::default().add_modifier(Modifier::SLOW_BLINK)),
                    ]),
                    Line::from(""),
                    Line::from(vec![
                        Span::styled(" Enter", Style::default().bold()),
                        Span::styled(" save  ", theme.muted_style()),
                        Span::styled("Esc", Style::default().bold()),
                        Span::styled(" cancel", theme.muted_style()),
                    ]),
                ],
                true,
            ),
            BoardEditMode::Delete { title, .. } => (
                "Delete Board",
                vec![
                    Line::from(vec![Span::styled(
                        format!(" Delete \"{}\"?", title),
                        Style::default().fg(Color::Red),
                    )]),
                    Line::from(""),
                    Line::from(vec![
                        Span::styled(" y", Style::default().bold().fg(Color::Red)),
                        Span::styled(" delete  ", theme.muted_style()),
                        Span::styled("n/Esc", Style::default().bold()),
                        Span::styled(" cancel", theme.muted_style()),
                    ]),
                ],
                false,
            ),
        };

        // Calculate popup size - centered dialog
        let popup_height = (content_lines.len() + 2) as u16; // +2 for border
        let popup_width = 40.min(area.width.saturating_sub(4));

        // Center the popup
        let popup_x = (area.width.saturating_sub(popup_width)) / 2;
        let popup_y = (area.height.saturating_sub(popup_height)) / 2;

        let popup_area = Rect {
            x: popup_x,
            y: popup_y,
            width: popup_width,
            height: popup_height,
        };

        // Clear background
        frame.render_widget(Clear, popup_area);

        // Draw popup with border
        let popup_block = Block::default()
            .title(title)
            .borders(Borders::ALL)
            .border_style(theme.brand_style());
        let inner = popup_block.inner(popup_area);
        frame.render_widget(popup_block, popup_area);

        // Render content
        frame.render_widget(Paragraph::new(content_lines), inner);

        // Position cursor if input mode
        if show_input {
            let cursor_pos = match edit_mode {
                BoardEditMode::Create { title } => title.visual_cursor(),
                BoardEditMode::Edit { title, .. } => title.visual_cursor(),
                _ => 0,
            };
            frame.set_cursor_position((inner.x + 8 + cursor_pos as u16, inner.y));
        }
    }

    /// Handle an action.
    fn handle_action(&mut self, action: Action) -> Result<()> {
        match action {
            Action::Quit => {
                // Stop server before quitting
                if self.server_process.is_some() {
                    let _ = self.stop_server();
                }
                self.should_quit = true;
            }
            Action::Navigate(view) => {
                self.view_history.push(self.view);
                self.view = view;
            }
            Action::Back => {
                // Handle back navigation in init wizard
                if self.view == View::Init {
                    match self.init_state.step {
                        InitStep::Persona => {
                            // Exit init wizard
                            if let Some(prev) = self.view_history.pop() {
                                self.view = prev;
                            }
                        }
                        InitStep::Sources => {
                            self.init_state.step = InitStep::Persona;
                            self.selected_index = 0;
                        }
                        InitStep::Sinks => {
                            self.init_state.step = InitStep::Sources;
                            self.selected_index = 0;
                        }
                        InitStep::ApiKey | InitStep::Complete => {
                            self.init_state.step = InitStep::Sinks;
                            self.selected_index = 0;
                        }
                    }
                } else if let Some(prev) = self.view_history.pop() {
                    self.view = prev;
                }
            }
            Action::ServerStatus(status) => {
                self.server_status = status;
            }
            Action::StartServer => {
                self.start_server()?;
            }
            Action::StopServer => {
                self.stop_server()?;
            }
            Action::InitWizard => {
                self.action_tx.send(Action::Navigate(View::Init))?;
            }
            Action::Help => {
                self.action_tx.send(Action::Navigate(View::Help))?;
            }
            Action::Command(cmd) => {
                self.handle_command(&cmd)?;
            }
            Action::Error(msg) => {
                self.error_message = Some(msg);
            }
            Action::NewEvent(event) => {
                // Add event to tail buffer (keep last 1000)
                self.tail_events.push(event);
                if self.tail_events.len() > 1000 {
                    self.tail_events.remove(0);
                }
                // Auto-scroll to bottom if near end
                let visible = 20; // approximate
                if self.tail_scroll + visible >= self.tail_events.len().saturating_sub(1) {
                    self.tail_scroll = self.tail_events.len().saturating_sub(visible);
                }
            }
            Action::MetricsUpdate(json) => {
                // Parse metrics JSON from control endpoint
                if let Ok(response) = serde_json::from_str::<serde_json::Value>(&json) {
                    let metrics = ServerMetrics {
                        uptime_secs: response["uptime_secs"].as_u64().unwrap_or(0),
                        messages_processed: response["pipeline"]["messages_processed"]
                            .as_u64()
                            .unwrap_or(0),
                        bytes_processed: response["pipeline"]["bytes_processed"]
                            .as_u64()
                            .unwrap_or(0),
                        sources: response["sources"]
                            .as_array()
                            .map(|arr| {
                                arr.iter()
                                    .filter_map(|s| {
                                        Some(SourceMetricsInfo {
                                            id: s["id"].as_str()?.to_string(),
                                            source_type: s["type"].as_str()?.to_string(),
                                            messages_received: s["messages_received"]
                                                .as_u64()
                                                .unwrap_or(0),
                                            connections_active: s["connections_active"]
                                                .as_u64()
                                                .unwrap_or(0),
                                        })
                                    })
                                    .collect()
                            })
                            .unwrap_or_default(),
                        sinks: response["sinks"]
                            .as_array()
                            .map(|arr| {
                                arr.iter()
                                    .filter_map(|s| {
                                        Some(SinkMetricsInfo {
                                            id: s["id"].as_str()?.to_string(),
                                            sink_type: s["type"].as_str()?.to_string(),
                                            messages_written: s["messages_written"]
                                                .as_u64()
                                                .unwrap_or(0),
                                            errors: s["write_errors"].as_u64().unwrap_or(0),
                                        })
                                    })
                                    .collect()
                            })
                            .unwrap_or_default(),
                    };

                    // Store previous for rate calculation
                    self.prev_metrics = self.server_metrics.take();
                    self.server_metrics = Some(metrics);
                }
            }
            Action::LoginSuccess(email) => {
                // Reload credentials and navigate back
                self.auth = crate::cmd::auth::load_credentials().ok();
                self.login_state = LoginState::default();
                self.error_message = Some(format!("Logged in as {}", email));
                // Go back to previous view
                if let Some(prev) = self.view_history.pop() {
                    self.view = prev;
                } else {
                    self.view = View::Dashboard;
                }
            }
            Action::LoginFailed(msg) => {
                self.login_state.loading = false;
                self.login_state.error = Some(msg);
            }
            Action::Logout => {
                let _ = crate::cmd::auth::clear_credentials();
                self.auth = None;
                self.boards.clear();
                self.current_board = None;
            }
            Action::BoardsLoaded(boards) => {
                self.boards = boards;
                self.selected_index = 0;
            }
            Action::SelectBoard(board_id) => {
                let _ = self.load_board_detail(&board_id);
                self.action_tx.send(Action::Navigate(View::BoardDetail))?;
            }
            Action::BoardDataLoaded(data) => {
                self.current_board = Some(data);
                self.last_board_fetch = Some(Instant::now());
            }
            // Query actions
            Action::ExecuteQuery(query) => {
                self.query_state.query = query;
                let _ = self.execute_query();
            }
            Action::QueryResult(result) => {
                self.query_state.loading = false;
                self.query_state.result = Some(result);
                self.query_state.error = None;
            }
            Action::QueryFailed(msg) => {
                self.query_state.loading = false;
                self.query_state.error = Some(msg);
            }
            // Board management actions
            Action::CreateBoard(title) => {
                self.create_board(&title)?;
            }
            Action::UpdateBoard(board_id, title) => {
                self.update_board(&board_id, &title)?;
            }
            Action::DeleteBoard(board_id) => {
                self.delete_board(&board_id)?;
            }
            Action::TogglePinBoard(board_id) => {
                self.toggle_pin_board(&board_id)?;
            }
            Action::BoardUpdated => {
                // Refresh boards list
                self.board_edit_mode = None;
                self.load_boards()?;
            }
            Action::BoardDeleted(board_id) => {
                self.board_edit_mode = None;
                self.boards.retain(|b| b.id != board_id);
                if self.current_board.as_ref().map(|b| &b.id) == Some(&board_id) {
                    self.current_board = None;
                    self.action_tx.send(Action::Navigate(View::Boards))?;
                }
            }
            // Board sharing actions
            Action::ShareBoard(board_id) => {
                self.share_board(&board_id)?;
            }
            Action::UnshareBoard(board_id) => {
                self.unshare_board(&board_id)?;
            }
            Action::BoardShared(board_id, share_url) => {
                // Update the board in our list
                if let Some(board) = self.boards.iter_mut().find(|b| b.id == board_id) {
                    // Extract hash from URL
                    if let Some(hash) = share_url.rsplit('/').next() {
                        board.share_hash = Some(hash.to_string());
                    }
                }
                self.error_message = Some(format!("Shared: {}", share_url));
            }
            Action::BoardUnshared(board_id) => {
                if let Some(board) = self.boards.iter_mut().find(|b| b.id == board_id) {
                    board.share_hash = None;
                }
                self.error_message = Some("Board unshared".to_string());
            }
            // Enhanced metrics actions
            Action::SetTimeRange(range) => {
                self.time_range = range;
                if self.current_board.is_some() {
                    self.action_tx.send(Action::RefreshMetrics)?;
                }
            }
            Action::ToggleComparison => {
                self.show_comparison = !self.show_comparison;
                if self.current_board.is_some() {
                    self.action_tx.send(Action::RefreshMetrics)?;
                }
            }
            Action::SetBreakdown(dim) => {
                self.breakdown = dim;
                if self.current_board.is_some() {
                    self.action_tx.send(Action::RefreshMetrics)?;
                }
            }
            Action::RefreshMetrics => {
                if let Some(ref board) = self.current_board {
                    let board_id = board.id.clone();
                    self.load_board_detail(&board_id)?;
                }
            }
            Action::TopEventsLoaded(events) => {
                self.top_events = events;
            }
            Action::StickinessLoaded(data) => {
                self.stickiness = Some(data);
            }
            // Unused actions in TUI context
            Action::Render | Action::Tick | Action::FocusNext | Action::FocusPrevious => {}
            Action::SelectPersona(_)
            | Action::ToggleSource(_)
            | Action::ToggleSink(_)
            | Action::CompleteInit => {}
        }

        Ok(())
    }

    /// Handle a command string.
    fn handle_command(&mut self, cmd: &str) -> Result<()> {
        let cmd = cmd.trim();
        let cmd = cmd.strip_prefix('/').unwrap_or(cmd);

        match cmd {
            "init" | "i" => {
                // Reset init state when starting fresh
                self.init_state = InitState::default();
                self.selected_index = 0;
                self.action_tx.send(Action::Navigate(View::Init))?;
            }
            "help" | "h" | "?" => {
                self.action_tx.send(Action::Navigate(View::Help))?;
            }
            "config" | "cfg" => {
                self.action_tx.send(Action::Navigate(View::Config))?;
            }
            "sources" | "src" => {
                self.action_tx.send(Action::Navigate(View::Sources))?;
            }
            "sinks" | "sink" => {
                self.action_tx.send(Action::Navigate(View::Sinks))?;
            }
            "plugins" | "p" => {
                self.action_tx.send(Action::Navigate(View::Plugins))?;
            }
            "query" | "q" => {
                self.action_tx.send(Action::Navigate(View::Query))?;
            }
            "start" => {
                self.action_tx.send(Action::StartServer)?;
            }
            "stop" => {
                self.action_tx.send(Action::StopServer)?;
            }
            "tail" | "t" => {
                #[cfg(unix)]
                {
                    self.start_tail()?;
                    self.action_tx.send(Action::Navigate(View::Tail))?;
                }
                #[cfg(not(unix))]
                {
                    self.action_tx.send(Action::Error(
                        "Tail is not available on Windows (requires Unix sockets)".to_string(),
                    ))?;
                }
            }
            "test" => {
                self.send_test_events(5)?; // Send 5 test events
            }
            "login" => {
                if self.auth.is_some() {
                    self.action_tx.send(Action::Error(format!(
                        "Already logged in as {}",
                        self.auth.as_ref().unwrap().email
                    )))?;
                } else {
                    // Reset login state
                    self.login_state = LoginState::default();
                    self.action_tx.send(Action::Navigate(View::Login))?;
                }
            }
            "logout" => {
                if self.auth.is_some() {
                    // Clear credentials from disk and memory
                    let _ = crate::cmd::auth::clear_credentials();
                    let email = self
                        .auth
                        .as_ref()
                        .map(|a| a.email.clone())
                        .unwrap_or_default();
                    self.auth = None;
                    self.boards.clear();
                    self.current_board = None;
                    self.action_tx
                        .send(Action::Error(format!("Logged out from {}", email)))?;
                } else {
                    self.action_tx
                        .send(Action::Error("Not logged in".to_string()))?;
                }
            }
            "boards" | "b" => {
                if self.auth.is_none() {
                    self.action_tx
                        .send(Action::Error("Login required. Use /login".to_string()))?;
                } else {
                    // Navigate to boards and trigger load
                    self.action_tx.send(Action::Navigate(View::Boards))?;
                    self.load_boards()?;
                }
            }
            "quit" | "exit" => {
                self.action_tx.send(Action::Quit)?;
            }
            _ => {
                self.action_tx
                    .send(Action::Error(format!("Unknown command: /{}", cmd)))?;
            }
        }

        Ok(())
    }

    /// Render init wizard step content.
    fn render_init_step<'a>(
        theme: &Theme,
        init_state: &InitState,
        selected_index: usize,
    ) -> Vec<Line<'a>> {
        match init_state.step {
            InitStep::Persona => {
                use super::action::Persona;
                let personas = [
                    Persona::MobileApp,
                    Persona::WebApp,
                    Persona::Backend,
                    Persona::LogInfra,
                    Persona::Custom,
                ];

                let mut lines = vec![
                    Line::from(""),
                    Line::from("  Setup Wizard"),
                    Line::from(""),
                    Line::from(vec![Span::styled(
                        "  What are you building?",
                        theme.muted_style(),
                    )]),
                    Line::from(""),
                ];

                for (i, persona) in personas.iter().enumerate() {
                    let marker = if i == selected_index { "▸" } else { " " };
                    let style = if i == selected_index {
                        theme.selected_style()
                    } else {
                        Style::default()
                    };
                    let mut spans = vec![
                        Span::styled(format!("    {} ", marker), style),
                        Span::styled(persona.name_main().to_string(), style),
                    ];
                    if let Some(detail) = persona.name_detail() {
                        spans.push(Span::styled(format!(" ({})", detail), theme.muted_style()));
                    }
                    lines.push(Line::from(spans));
                }

                lines
            }

            InitStep::Sources => {
                let all_sources = SourceType::all();
                let recommended = init_state
                    .persona
                    .map(|p| p.recommended_sources())
                    .unwrap_or_default();

                let mut lines = vec![
                    Line::from(""),
                    Line::from("  Setup Wizard"),
                    Line::from(""),
                    Line::from(vec![Span::styled(
                        "  Select data sources:",
                        theme.muted_style(),
                    )]),
                    Line::from(""),
                ];

                for (i, source) in all_sources.iter().enumerate() {
                    let is_selected = init_state.sources.contains(source);
                    let is_recommended = recommended.contains(source);
                    let marker = if i == selected_index { "▸" } else { " " };
                    let checkbox = if is_selected { "●" } else { "○" };
                    let checkbox_style = if is_selected {
                        theme.success_style()
                    } else {
                        theme.muted_style()
                    };

                    let style = if i == selected_index {
                        theme.selected_style()
                    } else {
                        Style::default()
                    };

                    let mut spans = vec![
                        Span::styled(format!("    {} ", marker), style),
                        Span::styled(checkbox, checkbox_style),
                        Span::raw(" "),
                        Span::styled(source.name().to_string(), style),
                    ];

                    if is_recommended {
                        spans.push(Span::styled(" (recommended)", theme.muted_style()));
                    }

                    lines.push(Line::from(spans));
                }

                lines
            }

            InitStep::Sinks => {
                let all_sinks = SinkType::all();
                let recommended = init_state
                    .persona
                    .map(|p| p.recommended_sinks())
                    .unwrap_or_default();

                let mut lines = vec![
                    Line::from(""),
                    Line::from("  Setup Wizard"),
                    Line::from(""),
                    Line::from(vec![Span::styled(
                        "  Select data sinks:",
                        theme.muted_style(),
                    )]),
                    Line::from(""),
                ];

                for (i, sink) in all_sinks.iter().enumerate() {
                    let is_selected = init_state.sinks.contains(sink);
                    let is_recommended = recommended.contains(sink);
                    let marker = if i == selected_index { "▸" } else { " " };
                    let checkbox = if is_selected { "●" } else { "○" };
                    let checkbox_style = if is_selected {
                        theme.success_style()
                    } else {
                        theme.muted_style()
                    };

                    let style = if i == selected_index {
                        theme.selected_style()
                    } else {
                        Style::default()
                    };

                    let mut spans = vec![
                        Span::styled(format!("    {} ", marker), style),
                        Span::styled(checkbox, checkbox_style),
                        Span::raw(" "),
                        Span::styled(sink.name().to_string(), style),
                    ];

                    if is_recommended {
                        spans.push(Span::styled(" (recommended)", theme.muted_style()));
                    }

                    lines.push(Line::from(spans));
                }

                lines
            }

            InitStep::ApiKey => {
                let config_path = init_state
                    .api_key
                    .clone()
                    .unwrap_or_else(|| "config.toml".to_string());

                vec![
                    Line::from(""),
                    Line::from(vec![
                        Span::styled("  ✓", theme.success_style()),
                        Span::raw(" Config created"),
                    ]),
                    Line::from(""),
                    Line::from(vec![Span::styled("  Saved to:", theme.muted_style())]),
                    Line::from(""),
                    Line::from(vec![
                        Span::raw("    "),
                        Span::styled(config_path, theme.brand_style()),
                    ]),
                    Line::from(""),
                    Line::from(vec![Span::styled(
                        "  Press Enter to continue.",
                        theme.muted_style(),
                    )]),
                ]
            }

            InitStep::Complete => {
                vec![
                    Line::from(""),
                    Line::from(vec![
                        Span::styled("  ✓", theme.success_style()),
                        Span::raw(" Your Tell server is ready."),
                    ]),
                    Line::from(""),
                    Line::from(vec![Span::styled("  Next steps:", theme.muted_style())]),
                    Line::from(""),
                    Line::from(vec![
                        Span::raw("    "),
                        Span::styled("/start", theme.brand_style()),
                        Span::styled("  Start the server", theme.muted_style()),
                    ]),
                    Line::from(vec![
                        Span::raw("    "),
                        Span::styled("/tail", theme.brand_style()),
                        Span::styled("    Watch live events", theme.muted_style()),
                    ]),
                ]
            }
        }
    }
}

/// Batch type constants (aligned with SchemaType wire values)
// Tap formatting helpers (Unix only)
#[cfg(unix)]
const BATCH_TYPE_EVENT: u8 = 1;
#[cfg(unix)]
const BATCH_TYPE_LOG: u8 = 2;

/// Format a TapEnvelope into human-readable strings (one per decoded item)
#[cfg(unix)]
fn format_tap_envelope(envelope: &tell_tap::TapEnvelope) -> String {
    match envelope.batch_type {
        BATCH_TYPE_EVENT => format_events(envelope),
        BATCH_TYPE_LOG => format_logs(envelope),
        _ => {
            // Fallback for other types - show metadata
            let batch_type = match envelope.batch_type {
                3 => "METRIC",
                4 => "TRACE",
                5 => "SNAPSHOT",
                _ => "UNKNOWN",
            };
            format!(
                "[{}] source: {}, workspace: {}, count: {}",
                batch_type, envelope.source_id, envelope.workspace_id, envelope.count
            )
        }
    }
}

/// Get a message from the envelope by index
#[cfg(unix)]
fn get_envelope_message(envelope: &tell_tap::TapEnvelope, index: usize) -> Option<&[u8]> {
    if index >= envelope.offsets.len() {
        return None;
    }
    let start = envelope.offsets[index] as usize;
    let len = envelope.lengths[index] as usize;
    if start + len > envelope.payload.len() {
        return None;
    }
    Some(&envelope.payload[start..start + len])
}

/// Format events from envelope
#[cfg(unix)]
fn format_events(envelope: &tell_tap::TapEnvelope) -> String {
    use tell_protocol::{FlatBatch, decode_event_data};

    let mut lines = Vec::new();

    for i in 0..envelope.offsets.len() {
        let Some(msg) = get_envelope_message(envelope, i) else {
            continue;
        };

        let flat_batch = match FlatBatch::parse(msg) {
            Ok(fb) => fb,
            Err(e) => {
                lines.push(format!("[EVENT] parse error: {}", e));
                continue;
            }
        };

        let data = match flat_batch.data() {
            Ok(d) => d,
            Err(e) => {
                lines.push(format!("[EVENT] data error: {}", e));
                continue;
            }
        };

        let events = match decode_event_data(data) {
            Ok(e) => e,
            Err(e) => {
                lines.push(format!("[EVENT] decode error: {}", e));
                continue;
            }
        };

        for event in events {
            let name = event.event_name.unwrap_or("-");
            let event_type = event.event_type.as_str();
            let payload = format_payload_short(event.payload);

            lines.push(format!("[EVENT] {} {} {}", event_type, name, payload));
        }
    }

    if lines.is_empty() {
        format!(
            "[EVENT] source: {}, count: {}",
            envelope.source_id, envelope.count
        )
    } else {
        lines.join("\n")
    }
}

/// Format logs from envelope
#[cfg(unix)]
fn format_logs(envelope: &tell_tap::TapEnvelope) -> String {
    use tell_protocol::{FlatBatch, decode_log_data};

    let mut lines = Vec::new();

    for i in 0..envelope.offsets.len() {
        let Some(msg) = get_envelope_message(envelope, i) else {
            continue;
        };

        let flat_batch = match FlatBatch::parse(msg) {
            Ok(fb) => fb,
            Err(e) => {
                lines.push(format!("[LOG] parse error: {}", e));
                continue;
            }
        };

        let data = match flat_batch.data() {
            Ok(d) => d,
            Err(e) => {
                lines.push(format!("[LOG] data error: {}", e));
                continue;
            }
        };

        let logs = match decode_log_data(data) {
            Ok(l) => l,
            Err(e) => {
                lines.push(format!("[LOG] decode error: {}", e));
                continue;
            }
        };

        for log in logs {
            let level = log.level.as_str();
            let service = log.service.unwrap_or("-");
            let payload = format_payload_short(log.payload);

            lines.push(format!("[LOG] {} {} {}", level, service, payload));
        }
    }

    if lines.is_empty() {
        format!(
            "[LOG] source: {}, count: {}",
            envelope.source_id, envelope.count
        )
    } else {
        lines.join("\n")
    }
}

/// Format payload as short inline string
fn format_payload_short(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        return String::new();
    }

    // Try JSON first - compact it
    if let Ok(json) = serde_json::from_slice::<serde_json::Value>(bytes) {
        let compact = serde_json::to_string(&json).unwrap_or_default();
        if compact.len() > 60 {
            format!("{}...", &compact[..57])
        } else {
            compact
        }
    } else if let Ok(s) = std::str::from_utf8(bytes) {
        // UTF-8 string
        if s.len() > 60 {
            format!("{}...", &s[..57])
        } else {
            s.to_string()
        }
    } else {
        // Binary - show length
        format!("<{} bytes>", bytes.len())
    }
}
