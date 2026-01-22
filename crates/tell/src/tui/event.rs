//! Event handling for the TUI.

use std::time::Duration;

use crossterm::event::{self, Event as CrosstermEvent, KeyEvent, MouseEvent};
use tokio::sync::mpsc;

/// TUI events.
#[derive(Debug, Clone)]
pub enum Event {
    /// Terminal tick (for animations, periodic updates)
    Tick,
    /// Key press
    Key(KeyEvent),
    /// Mouse event
    Mouse(MouseEvent),
    /// Terminal resize
    Resize(u16, u16),
    /// Focus gained
    FocusGained,
    /// Focus lost
    FocusLost,
}

/// Event handler that polls for terminal events.
pub struct EventHandler {
    /// Event sender
    tx: mpsc::UnboundedSender<Event>,
    /// Event receiver
    rx: mpsc::UnboundedReceiver<Event>,
}

impl EventHandler {
    /// Create a new event handler.
    pub fn new(tick_rate: Duration) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let event_tx = tx.clone();

        // Spawn event polling task
        tokio::spawn(async move {
            let mut tick_interval = tokio::time::interval(tick_rate);

            loop {
                let event = tokio::select! {
                    _ = tick_interval.tick() => Event::Tick,
                    maybe_event = poll_event() => {
                        match maybe_event {
                            Some(e) => e,
                            None => continue,
                        }
                    }
                };

                if event_tx.send(event).is_err() {
                    break;
                }
            }
        });

        Self { tx, rx }
    }

    /// Get the next event.
    pub async fn next(&mut self) -> Option<Event> {
        self.rx.recv().await
    }

    /// Get the event sender for external events.
    pub fn sender(&self) -> mpsc::UnboundedSender<Event> {
        self.tx.clone()
    }
}

/// Poll for a crossterm event (non-blocking).
async fn poll_event() -> Option<Event> {
    // Use tokio::task::spawn_blocking for the blocking poll
    let result = tokio::task::spawn_blocking(|| {
        if event::poll(Duration::from_millis(10)).ok()? {
            event::read().ok()
        } else {
            None
        }
    })
    .await
    .ok()?;

    result.map(|e| match e {
        CrosstermEvent::Key(key) => {
            // Only handle key press events (not release on Windows)
            if key.kind == event::KeyEventKind::Press {
                Some(Event::Key(key))
            } else {
                None
            }
        }
        CrosstermEvent::Mouse(mouse) => Some(Event::Mouse(mouse)),
        CrosstermEvent::Resize(w, h) => Some(Event::Resize(w, h)),
        CrosstermEvent::FocusGained => Some(Event::FocusGained),
        CrosstermEvent::FocusLost => Some(Event::FocusLost),
        CrosstermEvent::Paste(_) => None,
    })?
}
