//! Command input component for entering `/` commands.

use crossterm::event::{KeyCode, KeyEvent};
use ratatui::layout::Rect;
use ratatui::prelude::*;
use ratatui::widgets::{Block, Borders, Paragraph};
use tokio::sync::mpsc::UnboundedSender;
use tui_input::Input;
use tui_input::backend::crossterm::EventHandler;

use crate::tui::action::Action;
use crate::tui::component::{Component, Shortcut};
use crate::tui::theme::Theme;

/// Command input for entering `/` commands.
pub struct CommandInput {
    /// Text input state
    input: Input,
    /// Whether the input is focused
    focused: bool,
    /// Action sender
    action_tx: Option<UnboundedSender<Action>>,
    /// Command history
    history: Vec<String>,
    /// Current history index
    history_index: Option<usize>,
}

impl Default for CommandInput {
    fn default() -> Self {
        Self::new()
    }
}

impl CommandInput {
    /// Create a new command input.
    pub fn new() -> Self {
        Self {
            input: Input::default(),
            focused: false,
            action_tx: None,
            history: Vec::new(),
            history_index: None,
        }
    }

    /// Check if the input is focused.
    pub fn is_focused(&self) -> bool {
        self.focused
    }

    /// Focus the input.
    pub fn focus(&mut self) {
        self.focused = true;
        // Start with "/" prefix
        self.input = Input::new("/".to_string());
    }

    /// Unfocus the input.
    pub fn unfocus(&mut self) {
        self.focused = false;
        self.input.reset();
    }

    /// Get the current input value.
    pub fn value(&self) -> &str {
        self.input.value()
    }

    /// Submit the current command.
    fn submit(&mut self) -> Option<Action> {
        let value = self.input.value().to_string();
        if !value.is_empty() && value != "/" {
            // Add to history
            self.history.push(value.clone());
            self.history_index = None;
            self.input.reset();
            self.focused = false;
            Some(Action::Command(value))
        } else {
            self.unfocus();
            None
        }
    }

    /// Navigate history up.
    fn history_up(&mut self) {
        if self.history.is_empty() {
            return;
        }

        let new_index = match self.history_index {
            None => self.history.len().saturating_sub(1),
            Some(i) => i.saturating_sub(1),
        };

        self.history_index = Some(new_index);
        if let Some(cmd) = self.history.get(new_index) {
            self.input = Input::new(cmd.clone());
        }
    }

    /// Navigate history down.
    fn history_down(&mut self) {
        if self.history.is_empty() {
            return;
        }

        match self.history_index {
            None => {}
            Some(i) => {
                if i >= self.history.len() - 1 {
                    self.history_index = None;
                    self.input = Input::new("/".to_string());
                } else {
                    let new_index = i + 1;
                    self.history_index = Some(new_index);
                    if let Some(cmd) = self.history.get(new_index) {
                        self.input = Input::new(cmd.clone());
                    }
                }
            }
        }
    }
}

impl Component for CommandInput {
    fn id(&self) -> &'static str {
        "command_input"
    }

    fn register_action_handler(&mut self, tx: UnboundedSender<Action>) {
        self.action_tx = Some(tx);
    }

    fn handle_key(&mut self, key: KeyEvent) -> anyhow::Result<Option<Action>> {
        if !self.focused {
            return Ok(None);
        }

        match key.code {
            KeyCode::Enter => {
                return Ok(self.submit());
            }
            KeyCode::Esc => {
                self.unfocus();
                return Ok(None);
            }
            KeyCode::Up => {
                self.history_up();
                return Ok(None);
            }
            KeyCode::Down => {
                self.history_down();
                return Ok(None);
            }
            _ => {
                // Let tui-input handle the key
                self.input.handle_event(&crossterm::event::Event::Key(key));
            }
        }

        Ok(None)
    }

    fn draw(&mut self, frame: &mut ratatui::Frame, area: Rect, theme: &Theme) {
        let style = if self.focused {
            theme.focused_border()
        } else {
            theme.unfocused_border()
        };

        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(style)
            .title(" Command ");

        let inner = block.inner(area);

        // Render the input
        let input_style = if self.focused {
            Style::default().fg(theme.brand)
        } else {
            Style::default().fg(theme.muted)
        };

        let paragraph = Paragraph::new(self.input.value()).style(input_style);

        frame.render_widget(block, area);
        frame.render_widget(paragraph, inner);

        // Show cursor when focused
        if self.focused {
            let cursor_pos = self.input.visual_cursor();
            frame.set_cursor_position((inner.x + cursor_pos as u16, inner.y));
        }
    }

    fn shortcuts(&self) -> Vec<Shortcut> {
        if self.focused {
            vec![
                Shortcut::new("Enter", "Execute"),
                Shortcut::new("Esc", "Cancel"),
                Shortcut::new("↑/↓", "History"),
            ]
        } else {
            vec![Shortcut::new("/", "Command")]
        }
    }

    fn focusable(&self) -> bool {
        true
    }
}
