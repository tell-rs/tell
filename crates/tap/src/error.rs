//! Error types for the tap crate

use std::io;
use thiserror::Error;

/// Errors that can occur in the tap system
#[derive(Error, Debug)]
pub enum TapError {
    /// I/O error (socket operations)
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// Protocol error (malformed messages)
    #[error("protocol error: {0}")]
    Protocol(String),

    /// Maximum subscribers reached
    #[error("maximum subscribers reached ({max})")]
    MaxSubscribers { max: usize },

    /// Subscriber not found
    #[error("subscriber not found: {id}")]
    SubscriberNotFound { id: u64 },

    /// Channel closed (subscriber disconnected)
    #[error("channel closed")]
    ChannelClosed,

    /// Server not running
    #[error("tap server not running")]
    ServerNotRunning,

    /// Invalid configuration
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
}

/// Result type for tap operations
pub type Result<T> = std::result::Result<T, TapError>;
