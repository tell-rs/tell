//! Component trait for TUI widgets.

use crossterm::event::{KeyEvent, MouseEvent};
use ratatui::Frame;
use ratatui::layout::Rect;
use tokio::sync::mpsc::UnboundedSender;

use super::action::Action;
use super::event::Event;
use super::theme::Theme;

/// A TUI component that can be rendered and handle events.
pub trait Component: Send {
    /// Unique identifier for this component.
    fn id(&self) -> &'static str;

    /// Register the action handler for sending actions.
    fn register_action_handler(&mut self, _tx: UnboundedSender<Action>) {}

    /// Initialize the component.
    fn init(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    /// Handle an event, returning an optional action.
    fn handle_event(&mut self, event: &Event) -> anyhow::Result<Option<Action>> {
        match event {
            Event::Key(key) => self.handle_key(*key),
            Event::Mouse(mouse) => self.handle_mouse(*mouse),
            _ => Ok(None),
        }
    }

    /// Handle a key event.
    fn handle_key(&mut self, _key: KeyEvent) -> anyhow::Result<Option<Action>> {
        Ok(None)
    }

    /// Handle a mouse event.
    fn handle_mouse(&mut self, _mouse: MouseEvent) -> anyhow::Result<Option<Action>> {
        Ok(None)
    }

    /// Update component state based on an action.
    fn update(&mut self, _action: &Action) -> anyhow::Result<Option<Action>> {
        Ok(None)
    }

    /// Draw the component.
    fn draw(&mut self, frame: &mut Frame, area: Rect, theme: &Theme);

    /// Get keyboard shortcuts for this component.
    fn shortcuts(&self) -> Vec<Shortcut> {
        vec![]
    }

    /// Whether this component is focusable.
    fn focusable(&self) -> bool {
        false
    }
}

/// A keyboard shortcut.
#[derive(Debug, Clone)]
pub struct Shortcut {
    /// Key combination (e.g., "Ctrl+C", "Enter")
    pub key: &'static str,
    /// Description of what it does
    pub description: &'static str,
}

impl Shortcut {
    /// Create a new shortcut.
    pub const fn new(key: &'static str, description: &'static str) -> Self {
        Self { key, description }
    }
}
