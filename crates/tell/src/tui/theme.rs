//! Theme and styling for the TUI.

use ratatui::style::{Color, Modifier, Style};

/// Theme colors and styles for the TUI.
#[derive(Debug, Clone)]
pub struct Theme {
    /// Brand color (orange/coral)
    pub brand: Color,
    /// Success color (green)
    pub success: Color,
    /// Warning color (amber/yellow)
    pub warning: Color,
    /// Error color (red)
    pub error: Color,
    /// Muted/secondary text
    pub muted: Color,
    /// Background color
    pub bg: Color,
    /// Foreground color
    pub fg: Color,
    /// Selection background (subtle highlight)
    pub selection_bg: Color,
}

impl Default for Theme {
    fn default() -> Self {
        Self {
            // Orange/coral brand color (matches installer)
            brand: Color::Rgb(255, 143, 115), // ~209 in 256-color
            success: Color::Green,
            warning: Color::Yellow,
            error: Color::Red,
            muted: Color::DarkGray,
            bg: Color::Reset,
            fg: Color::Reset,
            // Subtle dark background for selection
            selection_bg: Color::Rgb(50, 50, 55),
        }
    }
}

impl Theme {
    /// Style for the brand/accent elements.
    pub fn brand_style(&self) -> Style {
        Style::default().fg(self.brand)
    }

    /// Style for success messages.
    pub fn success_style(&self) -> Style {
        Style::default().fg(self.success)
    }

    /// Style for warning messages.
    pub fn warning_style(&self) -> Style {
        Style::default().fg(self.warning)
    }

    /// Style for error messages.
    pub fn error_style(&self) -> Style {
        Style::default().fg(self.error)
    }

    /// Style for muted/secondary text.
    pub fn muted_style(&self) -> Style {
        Style::default().fg(self.muted)
    }

    /// Style for focused border.
    pub fn focused_border(&self) -> Style {
        Style::default().fg(self.brand)
    }

    /// Style for unfocused border.
    pub fn unfocused_border(&self) -> Style {
        Style::default().fg(self.muted)
    }

    /// Style for selected item.
    pub fn selected_style(&self) -> Style {
        Style::default().fg(self.brand).add_modifier(Modifier::BOLD)
    }

    /// Style for selected item with background highlight.
    pub fn selected_bg_style(&self) -> Style {
        Style::default()
            .fg(self.brand)
            .bg(self.selection_bg)
            .add_modifier(Modifier::BOLD)
    }

    /// Background style for selection.
    pub fn selection_bg_style(&self) -> Style {
        Style::default().bg(self.selection_bg)
    }

    /// Style for header text.
    pub fn header_style(&self) -> Style {
        Style::default().fg(self.brand).add_modifier(Modifier::BOLD)
    }

    /// Style for status indicator (running).
    pub fn status_running(&self) -> Style {
        Style::default().fg(self.success)
    }

    /// Style for status indicator (stopped).
    pub fn status_stopped(&self) -> Style {
        Style::default().fg(self.muted)
    }

    /// Style for status indicator (error).
    pub fn status_error(&self) -> Style {
        Style::default().fg(self.error)
    }
}
