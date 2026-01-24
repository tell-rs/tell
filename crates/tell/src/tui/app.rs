//! Main TUI application.
//!
//! Handles the event loop, state management, and component coordination.

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
use ratatui::layout::{Alignment, Constraint, Direction, Layout};
use ratatui::prelude::*;
use ratatui::widgets::Paragraph;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tui_input::Input;
use tui_input::backend::crossterm::EventHandler;

use tell_license::{LicenseState, load_license};

use super::action::{
    Action, InitState, InitStep, ServerStatus, SinkType, SourceType, TimeRange, View,
};
use super::event::{Event, EventHandler as TellEventHandler};
use super::theme::Theme;
use super::{api, commands, ui};

/// Server metrics fetched from control endpoint.
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

/// Login form state.
#[derive(Debug, Clone, Default)]
pub struct LoginState {
    pub email: Input,
    pub password: String,
    pub focus: usize,
    pub loading: bool,
    pub error: Option<String>,
}

/// Board edit mode state.
#[derive(Debug, Clone)]
pub enum BoardEditMode {
    Create { title: Input },
    Edit { board_id: String, title: Input },
    Delete { board_id: String, title: String },
}

/// Main TUI application state.
pub struct App {
    terminal: Terminal<CrosstermBackend<Stderr>>,
    events: TellEventHandler,
    action_tx: UnboundedSender<Action>,
    action_rx: UnboundedReceiver<Action>,
    view: View,
    view_history: Vec<View>,
    server_status: ServerStatus,
    server_process: Option<Child>,
    theme: Theme,
    config_path: std::path::PathBuf,
    has_config: bool,
    should_quit: bool,
    command_input: Input,
    command_focused: bool,
    command_history: Vec<String>,
    history_index: Option<usize>,
    error_message: Option<String>,
    init_state: InitState,
    selected_index: usize,
    selected_cmd_index: usize,
    tail_events: Vec<String>,
    tail_scroll: usize,
    tail_connected: bool,
    server_metrics: Option<ServerMetrics>,
    prev_metrics: Option<ServerMetrics>,
    last_metrics_fetch: Option<Instant>,
    auth: Option<crate::cmd::auth::AuthCredentials>,
    login_state: LoginState,
    boards: Vec<super::action::BoardInfo>,
    current_board: Option<super::action::BoardData>,
    last_board_fetch: Option<Instant>,
    query_state: super::action::QueryState,
    time_range: TimeRange,
    show_comparison: bool,
    breakdown: Option<String>,
    top_events: Vec<super::action::TopEvent>,
    stickiness: Option<super::action::StickinessData>,
    board_edit_mode: Option<BoardEditMode>,
    api_keys: Vec<super::action::ApiKeyInfo>,
    revealed_api_key: Option<(String, String)>, // (name, full_key)
    api_key_input: Option<Input>,               // For new key name input
    license_state: LicenseState,
}

impl App {
    /// Create a new App instance.
    pub fn new(config_path: Option<&Path>) -> Result<Self> {
        let terminal = Terminal::new(CrosstermBackend::new(io::stderr()))
            .context("failed to create terminal")?;

        let events = TellEventHandler::new(Duration::from_millis(250));
        let (action_tx, action_rx) = mpsc::unbounded_channel();

        let config_path = config_path
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| std::path::PathBuf::from("config.toml"));

        let has_config = config_path.exists();
        let view = if has_config {
            View::Dashboard
        } else {
            View::Welcome
        };
        let auth = crate::cmd::auth::load_credentials().ok();
        let license_state = load_license(None);

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
            time_range: TimeRange::default(),
            show_comparison: true,
            breakdown: None,
            top_events: Vec::new(),
            stickiness: None,
            board_edit_mode: None,
            api_keys: Vec::new(),
            revealed_api_key: None,
            api_key_input: None,
            license_state,
        })
    }

    /// Get the action sender for external use.
    pub fn action_sender(&self) -> UnboundedSender<Action> {
        self.action_tx.clone()
    }

    /// Run the TUI application.
    pub async fn run(&mut self) -> Result<()> {
        self.enter()?;

        if self.has_config {
            let _ = self.start_server();
        }

        loop {
            self.draw()?;

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

    fn init_panic_hook() {
        let original_hook = take_hook();
        set_hook(Box::new(move |panic_info| {
            let _ = Self::restore_terminal();
            original_hook(panic_info);
        }));
    }

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
        let breakdown = self.breakdown.clone();
        let board_edit_mode = self.board_edit_mode.clone();
        let api_keys = self.api_keys.clone();
        let revealed_api_key = self.revealed_api_key.clone();
        let cmd_ctx = self.command_context();

        self.terminal.draw(|frame| {
            let area = frame.area();

            let main_chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Length(1),
                    Constraint::Min(3),
                    Constraint::Length(1),
                ])
                .split(area);

            let header_area = main_chunks[0];
            let content_area = main_chunks[1];
            let footer_area = main_chunks[2];

            // Header with license status
            let version = env!("CARGO_PKG_VERSION");
            let license_display = match &self.license_state {
                LicenseState::Free => "Tell".to_string(),
                LicenseState::Licensed(lic) => {
                    format!("{} ({})", lic.tier().display_name(), lic.customer_name())
                }
                LicenseState::Expired(lic) => {
                    format!("{} EXPIRED", lic.tier().display_name())
                }
                LicenseState::Invalid(_) => "Tell".to_string(),
            };
            let license_style = match &self.license_state {
                LicenseState::Expired(_) => Style::default().fg(ratatui::style::Color::Red).bold(),
                LicenseState::Licensed(_) => theme.brand_style().bold(),
                _ => theme.brand_style().bold(),
            };
            let header_line = Line::from(vec![
                Span::raw(" "),
                Span::styled(format!("{} v{}", license_display, version), license_style),
            ]);
            frame.render_widget(Paragraph::new(header_line), header_area);

            // Content
            let content = ui::render_view_content(
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
                breakdown.as_deref(),
                &api_keys,
                revealed_api_key.as_ref(),
            );
            frame.render_widget(
                Paragraph::new(content).wrap(ratatui::widgets::Wrap { trim: false }),
                content_area,
            );

            // Footer
            let shortcuts = if command_focused {
                vec![("Enter", "execute"), ("Esc", "cancel"), ("↑↓", "navigate")]
            } else {
                vec![("/", "commands"), ("Esc", "back"), ("Ctrl+C", "quit")]
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

            let auth_status = if let Some(a) = &auth {
                Span::styled(format!("  {} ", a.email), theme.muted_style())
            } else {
                Span::styled("  not logged in ", theme.muted_style())
            };

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

            // Command popup
            if command_focused {
                let cmds = commands::commands_for_view(view, cmd_ctx);
                ui::render_command_popup(
                    frame,
                    area,
                    &theme,
                    &command_value,
                    cursor_pos,
                    selected_cmd_index,
                    &cmds,
                );
            }

            // Board edit dialog
            if let Some(ref edit_mode) = board_edit_mode {
                ui::render_board_edit_dialog(frame, area, &theme, edit_mode);
            }
        })?;

        Ok(())
    }

    /// Handle a terminal event.
    fn handle_event(&mut self, event: Event) -> Result<()> {
        self.error_message = None;

        match event {
            Event::Key(key) => {
                if self.command_focused {
                    self.handle_command_input(key)?;
                } else if self.board_edit_mode.is_some() {
                    self.handle_board_edit_input(key)?;
                } else {
                    self.handle_normal_input(key)?;
                }
            }
            Event::Resize(_, _) => {
                self.action_tx.send(Action::Render)?;
            }
            Event::Tick => {
                self.check_server_status();
                if self.server_status == ServerStatus::Running {
                    let should_fetch = self
                        .last_metrics_fetch
                        .map(|t| t.elapsed() > Duration::from_secs(1))
                        .unwrap_or(true);
                    if should_fetch {
                        self.last_metrics_fetch = Some(Instant::now());
                        api::fetch_server_metrics(self.action_tx.clone());
                    }
                }
                if self.view == View::BoardDetail {
                    let should_refresh = self
                        .last_board_fetch
                        .map(|t| t.elapsed() > Duration::from_secs(10))
                        .unwrap_or(false);
                    if should_refresh && let Some(ref board) = self.current_board {
                        self.last_board_fetch = Some(Instant::now());
                        let _ = self.refresh_board_detail(&board.id.clone());
                    }
                }
                self.action_tx.send(Action::Tick)?;
            }
            _ => {}
        }

        Ok(())
    }

    fn command_context(&self) -> commands::CommandContext {
        commands::CommandContext::new(self.has_config, self.server_status, self.auth.is_some())
    }

    fn handle_command_input(&mut self, key: crossterm::event::KeyEvent) -> Result<()> {
        match key.code {
            KeyCode::Enter => {
                let input = self.command_input.value().to_string();
                let filter = input.strip_prefix('/').unwrap_or(&input).to_lowercase();
                let cmds = commands::commands_for_view(self.view, self.command_context());
                let filtered: Vec<_> = cmds
                    .iter()
                    .filter(|(cmd, _)| filter.is_empty() || cmd.starts_with(&filter))
                    .collect();

                let cmd = if !filtered.is_empty() && self.selected_cmd_index < filtered.len() {
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
                if self.selected_cmd_index > 0 {
                    self.selected_cmd_index -= 1;
                }
            }
            KeyCode::Down => {
                let input = self.command_input.value();
                let filter = input.strip_prefix('/').unwrap_or(input).to_lowercase();
                let cmds = commands::commands_for_view(self.view, self.command_context());
                let filtered_count = cmds
                    .iter()
                    .filter(|(cmd, _)| filter.is_empty() || cmd.starts_with(&filter))
                    .count();
                if filtered_count > 0 && self.selected_cmd_index < filtered_count - 1 {
                    self.selected_cmd_index += 1;
                }
            }
            _ => {
                self.command_input
                    .handle_event(&crossterm::event::Event::Key(key));
                self.selected_cmd_index = 0;
            }
        }
        Ok(())
    }

    fn handle_board_edit_input(&mut self, key: crossterm::event::KeyEvent) -> Result<()> {
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
                let mut val = title.value().to_string();
                val.pop();
                *title = Input::new(val);
            }
            (Some(BoardEditMode::Create { title }), KeyCode::Char(c)) => {
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
            }
            (Some(BoardEditMode::Delete { .. }), KeyCode::Char('n') | KeyCode::Esc) => {
                self.board_edit_mode = None;
            }
            _ => {}
        }
        Ok(())
    }

    fn handle_normal_input(&mut self, key: crossterm::event::KeyEvent) -> Result<()> {
        match key.code {
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.action_tx.send(Action::Quit)?;
            }
            KeyCode::Char('d') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.action_tx.send(Action::Quit)?;
            }
            KeyCode::Esc => {
                self.action_tx.send(Action::Back)?;
            }
            KeyCode::Char('/') => {
                self.command_focused = true;
                self.command_input = Input::new("/".to_string());
            }
            // Init wizard navigation
            KeyCode::Up if self.view == View::Init => self.init_navigate_up(),
            KeyCode::Down if self.view == View::Init => self.init_navigate_down(),
            KeyCode::Char(' ') if self.view == View::Init => self.init_toggle_selection(),
            KeyCode::Enter if self.view == View::Init => self.init_confirm()?,
            KeyCode::Char(c @ '1'..='5')
                if self.view == View::Init && self.init_state.step == InitStep::Persona =>
            {
                self.selected_index = (c as usize) - ('1' as usize);
                self.init_confirm()?;
            }
            // View-specific scrolling
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
                self.tail_scroll =
                    (self.tail_scroll + 10).min(self.tail_events.len().saturating_sub(1));
            }
            // Login view
            KeyCode::Tab if self.view == View::Login => {
                self.login_state.focus = (self.login_state.focus + 1) % 2;
            }
            KeyCode::BackTab if self.view == View::Login => {
                self.login_state.focus = if self.login_state.focus == 0 { 1 } else { 0 };
            }
            KeyCode::Enter if self.view == View::Login => self.submit_login()?,
            KeyCode::Backspace if self.view == View::Login => {
                if self.login_state.focus == 0 {
                    let event = crossterm::event::Event::Key(key);
                    self.login_state.email.handle_event(&event);
                } else {
                    self.login_state.password.pop();
                }
            }
            KeyCode::Char(c) if self.view == View::Login => {
                if self.login_state.focus == 0 {
                    let event = crossterm::event::Event::Key(key);
                    self.login_state.email.handle_event(&event);
                } else {
                    self.login_state.password.push(c);
                }
            }
            // Boards navigation
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
                    self.refresh_board_detail(&board_id)?;
                    self.action_tx.send(Action::Navigate(View::BoardDetail))?;
                }
            }
            // Query view
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
                if self.view == View::Query && key.modifiers.contains(KeyModifiers::CONTROL) =>
            {
                self.query_state = super::action::QueryState::default();
            }
            KeyCode::Char(c) if self.view == View::Query => {
                self.query_state.query.push(c);
                self.query_state.cursor = self.query_state.query.len();
            }
            KeyCode::Up if self.view == View::Query && self.query_state.result.is_some() => {
                self.query_state.scroll = self.query_state.scroll.saturating_sub(1);
            }
            KeyCode::Down if self.view == View::Query && self.query_state.result.is_some() => {
                if let Some(ref result) = self.query_state.result
                    && self.query_state.scroll < result.rows.len().saturating_sub(1)
                {
                    self.query_state.scroll += 1;
                }
            }
            _ => {}
        }
        Ok(())
    }

    // Navigation helpers
    fn init_navigate_up(&mut self) {
        let max = self.init_list_len();
        if max > 0 && self.selected_index > 0 {
            self.selected_index -= 1;
        }
    }

    fn init_navigate_down(&mut self) {
        let max = self.init_list_len();
        if max > 0 && self.selected_index < max - 1 {
            self.selected_index += 1;
        }
    }

    fn init_list_len(&self) -> usize {
        match self.init_state.step {
            InitStep::Persona => 5,
            InitStep::Sources => SourceType::all().len(),
            InitStep::Sinks => SinkType::all().len(),
            _ => 0,
        }
    }

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
                    if let Some(parent) = self.config_path.parent()
                        && !parent.as_os_str().is_empty()
                        && !parent.exists()
                    {
                        let _ = std::fs::create_dir_all(parent);
                    }
                    match self.generate_config_file(&self.config_path) {
                        Ok(_) => {
                            // Generate API key for SDK usage
                            let api_key = self.generate_default_api_key()?;
                            self.init_state.api_key = Some(api_key);
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
                self.start_server()?;
                self.action_tx.send(Action::Navigate(View::Dashboard))?;
            }
            InitStep::Complete => {
                self.action_tx.send(Action::Navigate(View::Dashboard))?;
            }
        }
        Ok(())
    }

    fn generate_config_file(&self, path: &std::path::Path) -> Result<()> {
        use std::io::Write;

        let mut config = String::new();
        config.push_str("# Tell Configuration\n");
        config.push_str("# Generated by `tell` init wizard\n\n");
        config.push_str("[metrics]\ninterval = \"1h\"\n\n");
        config.push_str("# Sources\n");

        for source in &self.init_state.sources {
            let key = source.config_key();
            let port = source.default_port();
            match source {
                SourceType::Http => {
                    config.push_str(&format!("[sources.{}]\nport = {}\n\n", key, port));
                }
                SourceType::Tcp => {
                    config.push_str(&format!("[[sources.{}]]\nport = {}\n\n", key, port));
                }
                SourceType::SyslogTcp | SourceType::SyslogUdp => {
                    config.push_str(&format!(
                        "[sources.{}]\nenabled = true\nport = {}\nworkspace_id = \"1\"\n\n",
                        key, port
                    ));
                }
            }
        }

        config.push_str("# Sinks\n");
        for sink in &self.init_state.sinks {
            let key = sink.config_key();
            match sink {
                SinkType::ClickHouse => {
                    config.push_str(&format!(
                        "[sinks.{}]\ntype = \"{}\"\nhost = \"localhost:8123\"\ndatabase = \"default\"\n\n",
                        key, key
                    ));
                }
                SinkType::ArrowIpc => {
                    config.push_str(&format!(
                        "[sinks.{}]\ntype = \"{}\"\npath = \"data/arrow/\"\n\n",
                        key, key
                    ));
                }
                SinkType::DiskBinary => {
                    config.push_str(&format!(
                        "[sinks.{}]\ntype = \"{}\"\npath = \"data/archive/\"\n\n",
                        key, key
                    ));
                }
                SinkType::Stdout => {
                    config.push_str(&format!("[sinks.{}]\ntype = \"{}\"\n\n", key, key));
                }
            }
        }

        config.push_str("# Routing\n[routing]\n");
        let sink_names: Vec<_> = self
            .init_state
            .sinks
            .iter()
            .map(|s| format!("\"{}\"", s.config_key()))
            .collect();
        config.push_str(&format!("default = [{}]\n", sink_names.join(", ")));

        let mut file = std::fs::File::create(path).context("failed to create config file")?;
        file.write_all(config.as_bytes())
            .context("failed to write config file")?;

        Ok(())
    }

    fn start_server(&mut self) -> Result<()> {
        if self.server_process.is_some() {
            self.action_tx
                .send(Action::Error("Server is already running".to_string()))?;
            return Ok(());
        }

        // Check license before starting
        if !self.license_state.can_serve() {
            self.action_tx.send(Action::Error(
                "License expired. Renew at https://tell.rs/renew".to_string(),
            ))?;
            return Ok(());
        }

        if !self.config_path.exists() {
            self.action_tx.send(Action::Error(
                "No config file. Run /init first.".to_string(),
            ))?;
            return Ok(());
        }

        let exe = std::env::current_exe().context("failed to get current exe")?;
        self.server_status = ServerStatus::Starting;
        let child = Command::new(&exe)
            .arg("run")
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

    fn stop_server(&mut self) -> Result<()> {
        if let Some(mut child) = self.server_process.take() {
            self.server_status = ServerStatus::Stopping;
            #[cfg(unix)]
            {
                let pid = child.id();
                let _ = Command::new("kill")
                    .arg("-TERM")
                    .arg(pid.to_string())
                    .output();
                std::thread::sleep(Duration::from_millis(500));
            }
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

    fn check_server_status(&mut self) {
        if let Some(ref mut child) = self.server_process {
            match child.try_wait() {
                Ok(Some(status)) => {
                    self.server_process = None;
                    self.server_status = if status.success() {
                        ServerStatus::Stopped
                    } else {
                        ServerStatus::Error
                    };
                }
                Ok(None) => {
                    self.server_status = ServerStatus::Running;
                }
                Err(_) => {
                    self.server_status = ServerStatus::Error;
                }
            }
        }
    }

    #[cfg(unix)]
    fn start_tail(&mut self) -> Result<()> {
        self.tail_events.clear();
        self.tail_scroll = 0;

        if self.server_process.is_none() {
            self.tail_connected = false;
            return Ok(());
        }

        let action_tx = self.action_tx.clone();

        tokio::spawn(async move {
            use super::tap::format_tap_envelope;
            use bytes::{Buf, BytesMut};
            use tell_tap::{SubscribeRequest, TapMessage};
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            use tokio::net::UnixStream;

            let socket_path = "/tmp/tell-tap.sock";

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

            let subscribe = TapMessage::Subscribe(SubscribeRequest::new().with_last_n(10));
            let encoded = subscribe.encode();
            if let Err(e) = stream.write_all(&encoded).await {
                let _ = action_tx.send(Action::NewEvent(format!(
                    "[error] Failed to send subscribe: {}",
                    e
                )));
                return;
            }

            let mut read_buf = BytesMut::with_capacity(64 * 1024);

            loop {
                while read_buf.len() >= 4 {
                    let len =
                        u32::from_be_bytes([read_buf[0], read_buf[1], read_buf[2], read_buf[3]])
                            as usize;
                    if read_buf.len() < 4 + len {
                        break;
                    }
                    read_buf.advance(4);
                    let payload = read_buf.split_to(len).freeze();

                    match TapMessage::decode(payload) {
                        Ok(TapMessage::Batch(envelope)) => {
                            let event_str = format_tap_envelope(&envelope);
                            for line in event_str.lines() {
                                let _ = action_tx.send(Action::NewEvent(line.to_string()));
                            }
                        }
                        Ok(TapMessage::Heartbeat) => {}
                        Ok(TapMessage::Error(msg)) => {
                            let _ = action_tx.send(Action::NewEvent(format!("[error] {}", msg)));
                        }
                        Ok(TapMessage::Subscribe(_)) => {}
                        Err(e) => {
                            let _ =
                                action_tx.send(Action::NewEvent(format!("[error] Decode: {}", e)));
                        }
                    }
                }

                match stream.read_buf(&mut read_buf).await {
                    Ok(0) => {
                        let _ = action_tx.send(Action::NewEvent(
                            "[system] Tap connection closed".to_string(),
                        ));
                        break;
                    }
                    Ok(_) => {}
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

    fn submit_login(&mut self) -> Result<()> {
        let email = self.login_state.email.value().to_string();
        let password = self.login_state.password.clone();

        if email.is_empty() || password.is_empty() {
            self.login_state.error = Some("Email and password required".to_string());
            return Ok(());
        }

        let api_url = self.get_api_url();
        self.login_state.loading = true;
        self.login_state.error = None;

        api::submit_login(api_url, email, password, self.action_tx.clone())?;
        Ok(())
    }

    fn get_api_url(&self) -> String {
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

    fn refresh_board_detail(&mut self, board_id: &str) -> Result<()> {
        if let Some(ref auth) = self.auth {
            api::load_board_detail(
                auth,
                board_id,
                self.time_range,
                self.show_comparison,
                self.breakdown.clone(),
                self.action_tx.clone(),
            )?;
        }
        Ok(())
    }

    fn execute_query(&mut self) -> Result<()> {
        let Some(ref auth) = self.auth else {
            self.query_state.error = Some("Login required. Use /login".to_string());
            return Ok(());
        };

        self.query_state.loading = true;
        self.query_state.error = None;
        self.query_state.result = None;
        self.query_state.scroll = 0;

        api::execute_query(auth, &self.query_state.query, self.action_tx.clone())?;
        Ok(())
    }

    fn get_api_keys_path(&self) -> std::path::PathBuf {
        if self.config_path.exists()
            && let Ok(config) = tell_config::Config::from_file(&self.config_path)
        {
            return std::path::PathBuf::from(&config.global.api_keys_file);
        }
        std::path::PathBuf::from("configs/apikeys.conf")
    }

    fn load_api_keys(&mut self) -> Result<()> {
        use tell_auth::ApiKeyStore;

        let keys_path = self.get_api_keys_path();
        if !keys_path.exists() {
            self.api_keys = Vec::new();
            return Ok(());
        }

        let store = ApiKeyStore::from_file(&keys_path)
            .map_err(|e| anyhow::anyhow!("failed to load API keys: {}", e))?;

        self.api_keys = store
            .list()
            .into_iter()
            .map(|(hex_key, workspace, name)| super::action::ApiKeyInfo {
                name,
                key_preview: format!("{}...", &hex_key[..8]),
                workspace_id: workspace.as_u32(),
            })
            .collect();

        Ok(())
    }

    fn create_api_key(&mut self, name: &str) -> Result<()> {
        use tell_auth::{ApiKeyStore, WorkspaceId};

        let keys_path = self.get_api_keys_path();
        let store = if keys_path.exists() {
            ApiKeyStore::from_file(&keys_path)
                .map_err(|e| anyhow::anyhow!("failed to load API keys: {}", e))?
        } else {
            if let Some(parent) = keys_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            ApiKeyStore::new()
        };

        if store.name_exists(name) {
            self.error_message = Some(format!("API key '{}' already exists", name));
            return Ok(());
        }

        let hex_key = store.generate(WorkspaceId::new(1), name.to_string());
        store.save_to_file(&keys_path)?;

        self.action_tx
            .send(Action::ApiKeyCreated(name.to_string(), hex_key))?;
        Ok(())
    }

    fn delete_api_key(&mut self, name: &str) -> Result<()> {
        use tell_auth::ApiKeyStore;

        let keys_path = self.get_api_keys_path();
        if !keys_path.exists() {
            self.error_message = Some("No API keys file found".to_string());
            return Ok(());
        }

        let store = ApiKeyStore::from_file(&keys_path)
            .map_err(|e| anyhow::anyhow!("failed to load API keys: {}", e))?;

        if !store.name_exists(name) {
            self.error_message = Some(format!("API key '{}' not found", name));
            return Ok(());
        }

        store.remove_by_name(name);
        store.save_to_file(&keys_path)?;

        self.action_tx
            .send(Action::ApiKeyDeleted(name.to_string()))?;
        Ok(())
    }

    fn show_api_key(&mut self, name: &str) -> Result<()> {
        use tell_auth::ApiKeyStore;

        let keys_path = self.get_api_keys_path();
        if !keys_path.exists() {
            self.error_message = Some("No API keys file found".to_string());
            return Ok(());
        }

        let store = ApiKeyStore::from_file(&keys_path)
            .map_err(|e| anyhow::anyhow!("failed to load API keys: {}", e))?;

        if let Some(hex_key) = store.find_by_name(name) {
            self.action_tx
                .send(Action::ApiKeyRevealed(name.to_string(), hex_key))?;
        } else {
            self.error_message = Some(format!("API key '{}' not found", name));
        }
        Ok(())
    }

    fn generate_default_api_key(&mut self) -> Result<String> {
        use tell_auth::{ApiKeyStore, WorkspaceId};

        let keys_path = self.get_api_keys_path();

        // Load or create store
        let store = if keys_path.exists() {
            ApiKeyStore::from_file(&keys_path)
                .map_err(|e| anyhow::anyhow!("failed to load API keys: {}", e))?
        } else {
            if let Some(parent) = keys_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            ApiKeyStore::new()
        };

        // Check if "default" key already exists
        if let Some(existing_key) = store.find_by_name("default") {
            return Ok(existing_key);
        }

        // Generate new key
        let hex_key = store.generate(WorkspaceId::new(1), "default".to_string());
        store.save_to_file(&keys_path)?;

        Ok(hex_key)
    }

    fn send_test_events(&mut self, count: usize) -> Result<()> {
        if self.server_process.is_none() {
            self.action_tx.send(Action::Error(
                "Server not running. Use /start first.".to_string(),
            ))?;
            return Ok(());
        }

        let action_tx = self.action_tx.clone();

        tokio::spawn(async move {
            use tell_client::batch::BatchBuilder;
            use tell_client::event::{EventBuilder, EventDataBuilder};
            use tell_client::log::{LogDataBuilder, LogEntryBuilder};
            use tell_client::test::TcpTestClient;

            let api_key: [u8; 16] = [
                0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
                0x0e, 0x0f,
            ];

            let server = "127.0.0.1:50000";
            let start = Instant::now();

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

            for i in 0..count {
                let device_id: [u8; 16] = {
                    let mut id = [0u8; 16];
                    id[15] = i as u8;
                    id
                };

                if let Ok(event) = EventBuilder::new()
                    .track(&format!("test_event_{}", i))
                    .device_id(device_id)
                    .timestamp_now()
                    .payload_json(&format!(
                        r#"{{"index": {}, "test": true, "source": "tell tui"}}"#,
                        i
                    ))
                    .build()
                    && let Ok(event_data) = EventDataBuilder::new().add(event).build()
                    && let Ok(batch) = BatchBuilder::new()
                        .api_key(api_key)
                        .event_data(event_data)
                        .build()
                {
                    let _ = client.send(&batch).await;
                }
            }

            for i in 0..count {
                if let Ok(log) = (match i % 3 {
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
                    && let Ok(log_data) = LogDataBuilder::new().add(log).build()
                    && let Ok(batch) = BatchBuilder::new()
                        .api_key(api_key)
                        .log_data(log_data)
                        .build()
                {
                    let _ = client.send(&batch).await;
                }
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

    /// Handle an action.
    fn handle_action(&mut self, action: Action) -> Result<()> {
        match action {
            Action::Quit => {
                if self.server_process.is_some() {
                    let _ = self.stop_server();
                }
                self.should_quit = true;
            }
            Action::Navigate(view) => {
                self.view_history.push(self.view);
                self.view = view;
            }
            Action::Back => self.handle_back()?,
            Action::ServerStatus(status) => self.server_status = status,
            Action::StartServer => self.start_server()?,
            Action::StopServer => self.stop_server()?,
            Action::InitWizard => self.action_tx.send(Action::Navigate(View::Init))?,
            Action::Help => self.action_tx.send(Action::Navigate(View::Help))?,
            Action::Command(cmd) => self.handle_command(&cmd)?,
            Action::Error(msg) => self.error_message = Some(msg),
            Action::NewEvent(event) => {
                self.tail_events.push(event);
                if self.tail_events.len() > 1000 {
                    self.tail_events.remove(0);
                }
                let visible = 20;
                if self.tail_scroll + visible >= self.tail_events.len().saturating_sub(1) {
                    self.tail_scroll = self.tail_events.len().saturating_sub(visible);
                }
            }
            Action::MetricsUpdate(json) => self.parse_metrics_json(&json),
            Action::LoginSuccess(email) => {
                self.auth = crate::cmd::auth::load_credentials().ok();
                self.login_state = LoginState::default();
                self.error_message = Some(format!("Logged in as {}", email));
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
                let _ = self.refresh_board_detail(&board_id);
                self.action_tx.send(Action::Navigate(View::BoardDetail))?;
            }
            Action::BoardDataLoaded(data) => {
                self.current_board = Some(data);
                self.last_board_fetch = Some(Instant::now());
            }
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
            Action::CreateBoard(title) => {
                if let Some(ref auth) = self.auth {
                    api::create_board(auth, &title, self.action_tx.clone())?;
                }
            }
            Action::UpdateBoard(board_id, title) => {
                if let Some(ref auth) = self.auth {
                    api::update_board(auth, &board_id, &title, self.action_tx.clone())?;
                }
            }
            Action::DeleteBoard(board_id) => {
                if let Some(ref auth) = self.auth {
                    api::delete_board(auth, &board_id, self.action_tx.clone())?;
                }
            }
            Action::TogglePinBoard(board_id) => {
                if let Some(ref auth) = self.auth {
                    let is_pinned = self
                        .boards
                        .iter()
                        .find(|b| b.id == board_id)
                        .map(|b| b.is_pinned)
                        .unwrap_or(false);
                    api::toggle_pin_board(auth, &board_id, is_pinned, self.action_tx.clone())?;
                }
            }
            Action::BoardUpdated => {
                self.board_edit_mode = None;
                if let Some(ref auth) = self.auth {
                    api::load_boards(auth, self.action_tx.clone())?;
                }
            }
            Action::BoardDeleted(board_id) => {
                self.board_edit_mode = None;
                self.boards.retain(|b| b.id != board_id);
                if self.current_board.as_ref().map(|b| &b.id) == Some(&board_id) {
                    self.current_board = None;
                    self.action_tx.send(Action::Navigate(View::Boards))?;
                }
            }
            Action::ShareBoard(board_id) => {
                if let Some(ref auth) = self.auth {
                    api::share_board(auth, &board_id, self.action_tx.clone())?;
                }
            }
            Action::UnshareBoard(board_id) => {
                if let Some(ref auth) = self.auth {
                    api::unshare_board(auth, &board_id, self.action_tx.clone())?;
                }
            }
            Action::BoardShared(board_id, share_url) => {
                if let Some(board) = self.boards.iter_mut().find(|b| b.id == board_id)
                    && let Some(hash) = share_url.rsplit('/').next()
                {
                    board.share_hash = Some(hash.to_string());
                }
                self.error_message = Some(format!("Shared: {}", share_url));
            }
            Action::BoardUnshared(board_id) => {
                if let Some(board) = self.boards.iter_mut().find(|b| b.id == board_id) {
                    board.share_hash = None;
                }
                self.error_message = Some("Board unshared".to_string());
            }
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
                    self.refresh_board_detail(&board_id)?;
                }
            }
            Action::TopEventsLoaded(events) => {
                self.top_events = events.clone();
                if events.is_empty() {
                    self.error_message = Some("No events found".to_string());
                } else {
                    let top_3: Vec<String> = events
                        .iter()
                        .take(3)
                        .map(|e| format!("{}: {}", e.name, e.count))
                        .collect();
                    self.error_message = Some(format!("Top events: {}", top_3.join(", ")));
                }
            }
            Action::StickinessLoaded(data) => {
                self.stickiness = Some(data.clone());
                self.error_message = Some(format!(
                    "Stickiness: DAU/MAU {:.1}%, DAU/WAU {:.1}%",
                    data.daily_ratio * 100.0,
                    data.weekly_ratio * 100.0
                ));
            }
            // API Keys
            Action::ApiKeysLoaded(keys) => {
                self.api_keys = keys;
                self.revealed_api_key = None;
            }
            Action::CreateApiKey(name) => {
                self.create_api_key(&name)?;
            }
            Action::DeleteApiKey(name) => {
                self.delete_api_key(&name)?;
            }
            Action::ApiKeyCreated(name, hex_key) => {
                self.revealed_api_key = Some((name.clone(), hex_key));
                self.api_key_input = None;
                self.load_api_keys()?;
                self.error_message = Some(format!("Created API key: {}", name));
            }
            Action::ApiKeyDeleted(name) => {
                self.load_api_keys()?;
                self.revealed_api_key = None;
                self.error_message = Some(format!("Deleted API key: {}", name));
            }
            Action::ShowApiKey(name) => {
                self.show_api_key(&name)?;
            }
            Action::ApiKeyRevealed(name, hex_key) => {
                self.revealed_api_key = Some((name, hex_key));
            }
            Action::Render | Action::Tick | Action::FocusNext | Action::FocusPrevious => {}
            Action::SelectPersona(_)
            | Action::ToggleSource(_)
            | Action::ToggleSink(_)
            | Action::CompleteInit => {}
        }
        Ok(())
    }

    fn handle_back(&mut self) -> Result<()> {
        if self.view == View::Init {
            match self.init_state.step {
                InitStep::Persona => {
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
        Ok(())
    }

    fn parse_metrics_json(&mut self, json: &str) {
        if let Ok(response) = serde_json::from_str::<serde_json::Value>(json) {
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
                                    messages_received: s["messages_received"].as_u64().unwrap_or(0),
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
                                    messages_written: s["messages_written"].as_u64().unwrap_or(0),
                                    errors: s["write_errors"].as_u64().unwrap_or(0),
                                })
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
            };
            self.prev_metrics = self.server_metrics.take();
            self.server_metrics = Some(metrics);
        }
    }

    fn handle_command(&mut self, cmd: &str) -> Result<()> {
        let cmd = cmd.trim().strip_prefix('/').unwrap_or(cmd.trim());

        match self.view {
            View::Boards => match cmd {
                "new" | "n" => {
                    self.board_edit_mode = Some(BoardEditMode::Create {
                        title: Input::default(),
                    });
                }
                "edit" | "e" => {
                    if let Some(board) = self.boards.get(self.selected_index) {
                        self.board_edit_mode = Some(BoardEditMode::Edit {
                            board_id: board.id.clone(),
                            title: Input::new(board.title.clone()),
                        });
                    }
                }
                "delete" | "del" => {
                    if let Some(board) = self.boards.get(self.selected_index) {
                        self.board_edit_mode = Some(BoardEditMode::Delete {
                            board_id: board.id.clone(),
                            title: board.title.clone(),
                        });
                    }
                }
                "pin" => {
                    if let Some(board) = self.boards.get(self.selected_index) {
                        self.action_tx
                            .send(Action::TogglePinBoard(board.id.clone()))?;
                    }
                }
                "share" => {
                    if let Some(board) = self.boards.get(self.selected_index) {
                        if board.share_hash.is_some() {
                            self.action_tx
                                .send(Action::UnshareBoard(board.id.clone()))?;
                        } else {
                            self.action_tx.send(Action::ShareBoard(board.id.clone()))?;
                        }
                    }
                }
                "back" => self.action_tx.send(Action::Navigate(View::Dashboard))?,
                _ => self.handle_global_command(cmd)?,
            },
            View::BoardDetail => match cmd {
                "7d" => self.action_tx.send(Action::SetTimeRange(TimeRange::Week))?,
                "30d" => self
                    .action_tx
                    .send(Action::SetTimeRange(TimeRange::Month))?,
                "90d" => self
                    .action_tx
                    .send(Action::SetTimeRange(TimeRange::Quarter))?,
                "compare" | "cmp" => self.action_tx.send(Action::ToggleComparison)?,
                "refresh" | "r" => self.action_tx.send(Action::RefreshMetrics)?,
                "share" => {
                    if let Some(ref board) = self.current_board {
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
                "stickiness" | "sticky" => {
                    if let Some(ref auth) = self.auth {
                        api::load_stickiness(auth, self.time_range, self.action_tx.clone())?;
                        self.error_message = Some("Loading stickiness metrics...".to_string());
                    }
                }
                "top" => {
                    if let Some(ref auth) = self.auth {
                        api::load_top_events(auth, self.time_range, self.action_tx.clone())?;
                        self.error_message = Some("Loading top events...".to_string());
                    }
                }
                "breakdown device" | "breakdown device_type" => {
                    self.action_tx
                        .send(Action::SetBreakdown(Some("device_type".to_string())))?;
                }
                "breakdown country" => {
                    self.action_tx
                        .send(Action::SetBreakdown(Some("country".to_string())))?;
                }
                "breakdown os" => {
                    self.action_tx
                        .send(Action::SetBreakdown(Some("os".to_string())))?;
                }
                "breakdown none" | "breakdown clear" => {
                    self.action_tx.send(Action::SetBreakdown(None))?;
                }
                "breakdown" => {
                    let current = self.breakdown.as_deref().unwrap_or("none");
                    self.error_message = Some(format!(
                        "Breakdown: {} (use /breakdown device, country, os, or none)",
                        current
                    ));
                }
                "back" => self.action_tx.send(Action::Navigate(View::Boards))?,
                _ => self.handle_global_command(cmd)?,
            },
            View::Query => match cmd {
                "run" => {
                    if !self.query_state.query.trim().is_empty() {
                        self.execute_query()?;
                    }
                }
                "clear" => self.query_state = super::action::QueryState::default(),
                "back" => self.action_tx.send(Action::Navigate(View::Dashboard))?,
                _ => self.handle_global_command(cmd)?,
            },
            View::Tail => match cmd {
                "clear" => {
                    self.tail_events.clear();
                    self.tail_scroll = 0;
                }
                "back" => self.action_tx.send(Action::Navigate(View::Dashboard))?,
                _ => self.handle_global_command(cmd)?,
            },
            View::ApiKeys => {
                if cmd.starts_with("new ") {
                    let name = cmd.strip_prefix("new ").unwrap().trim();
                    if !name.is_empty() {
                        self.action_tx
                            .send(Action::CreateApiKey(name.to_string()))?;
                    }
                } else if cmd.starts_with("show ") {
                    let name = cmd.strip_prefix("show ").unwrap().trim();
                    if !name.is_empty() {
                        self.action_tx.send(Action::ShowApiKey(name.to_string()))?;
                    }
                } else if cmd.starts_with("delete ") {
                    let name = cmd.strip_prefix("delete ").unwrap().trim();
                    if !name.is_empty() {
                        self.action_tx
                            .send(Action::DeleteApiKey(name.to_string()))?;
                    }
                } else if cmd == "back" {
                    self.action_tx.send(Action::Navigate(View::Dashboard))?;
                } else {
                    self.handle_global_command(cmd)?;
                }
            }
            _ => self.handle_global_command(cmd)?,
        }
        Ok(())
    }

    fn handle_global_command(&mut self, cmd: &str) -> Result<()> {
        match cmd {
            "init" | "i" => {
                self.init_state = InitState::default();
                self.selected_index = 0;
                self.action_tx.send(Action::Navigate(View::Init))?;
            }
            "help" | "h" | "?" => self.action_tx.send(Action::Navigate(View::Help))?,
            "config" | "cfg" => self.action_tx.send(Action::Navigate(View::Config))?,
            "query" => self.action_tx.send(Action::Navigate(View::Query))?,
            "start" => self.action_tx.send(Action::StartServer)?,
            "stop" => self.action_tx.send(Action::StopServer)?,
            "tail" => {
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
            "test" => self.send_test_events(5)?,
            "login" => {
                if self.auth.is_some() {
                    self.action_tx.send(Action::Error(format!(
                        "Already logged in as {}",
                        self.auth.as_ref().unwrap().email
                    )))?;
                } else {
                    self.login_state = LoginState::default();
                    self.action_tx.send(Action::Navigate(View::Login))?;
                }
            }
            "logout" => {
                if self.auth.is_some() {
                    let email = self
                        .auth
                        .as_ref()
                        .map(|a| a.email.clone())
                        .unwrap_or_default();
                    let _ = crate::cmd::auth::clear_credentials();
                    self.auth = None;
                    self.boards.clear();
                    self.current_board = None;
                    self.error_message = Some(format!("Logged out from {}", email));
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
                    self.action_tx.send(Action::Navigate(View::Boards))?;
                    if let Some(ref auth) = self.auth {
                        api::load_boards(auth, self.action_tx.clone())?;
                    }
                }
            }
            "apikeys" | "keys" => {
                self.load_api_keys()?;
                self.action_tx.send(Action::Navigate(View::ApiKeys))?;
            }
            "license" | "lic" => {
                let msg = match &self.license_state {
                    LicenseState::Free => {
                        "License: Tell (Free tier). Purchase at https://tell.rs/pricing".to_string()
                    }
                    LicenseState::Licensed(lic) => {
                        let days = lic.payload.days_until_expiry();
                        format!(
                            "License: {} for {} (expires {} - {} days left)",
                            lic.tier().display_name(),
                            lic.customer_name(),
                            lic.payload.expires,
                            days
                        )
                    }
                    LicenseState::Expired(lic) => {
                        format!(
                            "License EXPIRED for {}. Renew at https://tell.rs/renew",
                            lic.customer_name()
                        )
                    }
                    LicenseState::Invalid(err) => {
                        format!("License: Invalid ({})", err)
                    }
                };
                self.error_message = Some(msg);
            }
            "quit" | "exit" => self.action_tx.send(Action::Quit)?,
            "back" => self.action_tx.send(Action::Back)?,
            _ => self
                .action_tx
                .send(Action::Error(format!("Unknown command: /{}", cmd)))?,
        }
        Ok(())
    }
}
