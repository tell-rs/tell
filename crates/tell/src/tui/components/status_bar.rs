//! Status bar component showing server status and shortcuts.

use ratatui::layout::Rect;
use ratatui::prelude::*;
use ratatui::widgets::Paragraph;
use tokio::sync::mpsc::UnboundedSender;

use crate::tui::action::{Action, ServerStatus};
use crate::tui::component::{Component, Shortcut};
use crate::tui::theme::Theme;

/// Status bar showing server status and keyboard shortcuts.
pub struct StatusBar {
    /// Server status
    status: ServerStatus,
    /// Current shortcuts to display
    shortcuts: Vec<Shortcut>,
    /// Action sender
    action_tx: Option<UnboundedSender<Action>>,
}

impl Default for StatusBar {
    fn default() -> Self {
        Self::new()
    }
}

impl StatusBar {
    /// Create a new status bar.
    pub fn new() -> Self {
        Self {
            status: ServerStatus::Stopped,
            shortcuts: Vec::new(),
            action_tx: None,
        }
    }

    /// Update the server status.
    pub fn set_status(&mut self, status: ServerStatus) {
        self.status = status;
    }

    /// Update the displayed shortcuts.
    pub fn set_shortcuts(&mut self, shortcuts: Vec<Shortcut>) {
        self.shortcuts = shortcuts;
    }
}

impl Component for StatusBar {
    fn id(&self) -> &'static str {
        "status_bar"
    }

    fn register_action_handler(&mut self, tx: UnboundedSender<Action>) {
        self.action_tx = Some(tx);
    }

    fn update(&mut self, action: &Action) -> anyhow::Result<Option<Action>> {
        if let Action::ServerStatus(status) = action {
            self.status = *status;
        }
        Ok(None)
    }

    fn draw(&mut self, frame: &mut ratatui::Frame, area: Rect, theme: &Theme) {
        // Build status line
        let status_style = match self.status {
            ServerStatus::Running => theme.status_running(),
            ServerStatus::Error => theme.status_error(),
            _ => theme.status_stopped(),
        };

        let mut spans = vec![
            Span::styled(self.status.indicator(), status_style),
            Span::raw(" "),
            Span::styled(format!("{:?}", self.status), status_style),
        ];

        // Add separator
        spans.push(Span::styled(" │ ", theme.muted_style()));

        // Add shortcuts
        for (i, shortcut) in self.shortcuts.iter().enumerate() {
            if i > 0 {
                spans.push(Span::styled("  ", Style::default()));
            }
            spans.push(Span::styled(shortcut.key, theme.brand_style()));
            spans.push(Span::raw(" "));
            spans.push(Span::styled(shortcut.description, theme.muted_style()));
        }

        let line = Line::from(spans);
        let paragraph = Paragraph::new(line);
        frame.render_widget(paragraph, area);
    }

    fn focusable(&self) -> bool {
        false
    }
}
