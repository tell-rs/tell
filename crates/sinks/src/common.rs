//! Common types and utilities for sinks
//!
//! Shared functionality across all sink types.

use std::sync::atomic::{AtomicU64, Ordering};
use thiserror::Error;

/// Common configuration for sinks
#[derive(Debug, Clone)]
pub struct SinkConfig {
    /// Unique identifier for this sink (used in routing)
    pub id: String,

    /// Whether this sink is enabled
    pub enabled: bool,

    /// Channel queue size for incoming batches
    pub queue_size: usize,

    /// Whether metrics reporting is enabled
    pub metrics_enabled: bool,

    /// Metrics reporting interval in seconds
    pub metrics_interval_secs: u64,
}

impl Default for SinkConfig {
    fn default() -> Self {
        Self {
            id: String::new(),
            enabled: true,
            queue_size: 1000,
            metrics_enabled: true,
            metrics_interval_secs: 10,
        }
    }
}

/// Metrics shared by all sink types
#[derive(Debug, Default)]
pub struct SinkMetrics {
    /// Total batches received
    pub batches_received: AtomicU64,

    /// Total batches successfully written
    pub batches_written: AtomicU64,

    /// Total messages written (sum of batch.count)
    pub messages_written: AtomicU64,

    /// Total bytes written
    pub bytes_written: AtomicU64,

    /// Write errors encountered
    pub write_errors: AtomicU64,

    /// Flush operations performed
    pub flush_count: AtomicU64,
}

impl SinkMetrics {
    /// Create new metrics instance
    pub const fn new() -> Self {
        Self {
            batches_received: AtomicU64::new(0),
            batches_written: AtomicU64::new(0),
            messages_written: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            write_errors: AtomicU64::new(0),
            flush_count: AtomicU64::new(0),
        }
    }

    /// Record a received batch
    #[inline]
    pub fn batch_received(&self) {
        self.batches_received.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a successfully written batch
    #[inline]
    pub fn batch_written(&self, message_count: u64, bytes: u64) {
        self.batches_written.fetch_add(1, Ordering::Relaxed);
        self.messages_written
            .fetch_add(message_count, Ordering::Relaxed);
        self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record a write error
    #[inline]
    pub fn write_error(&self) {
        self.write_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a flush operation
    #[inline]
    pub fn flush(&self) {
        self.flush_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Get snapshot of all metrics
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            batches_received: self.batches_received.load(Ordering::Relaxed),
            batches_written: self.batches_written.load(Ordering::Relaxed),
            messages_written: self.messages_written.load(Ordering::Relaxed),
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
            write_errors: self.write_errors.load(Ordering::Relaxed),
            flush_count: self.flush_count.load(Ordering::Relaxed),
        }
    }

    /// Reset all metrics to zero
    pub fn reset(&self) {
        self.batches_received.store(0, Ordering::Relaxed);
        self.batches_written.store(0, Ordering::Relaxed);
        self.messages_written.store(0, Ordering::Relaxed);
        self.bytes_written.store(0, Ordering::Relaxed);
        self.write_errors.store(0, Ordering::Relaxed);
        self.flush_count.store(0, Ordering::Relaxed);
    }
}

/// Point-in-time snapshot of sink metrics
#[derive(Debug, Clone, Copy)]
pub struct MetricsSnapshot {
    pub batches_received: u64,
    pub batches_written: u64,
    pub messages_written: u64,
    pub bytes_written: u64,
    pub write_errors: u64,
    pub flush_count: u64,
}

/// Common sink errors
#[derive(Debug, Error)]
pub enum SinkError {
    /// Sink initialization failed
    #[error("failed to initialize sink: {0}")]
    Init(String),

    /// Failed to write data
    #[error("write failed: {0}")]
    Write(String),

    /// Failed to flush data
    #[error("flush failed: {0}")]
    Flush(String),

    /// Connection error (for network sinks)
    #[error("connection error: {0}")]
    Connection(String),

    /// Configuration error
    #[error("configuration error: {0}")]
    Config(String),

    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Serialization error
    #[error("serialization error: {0}")]
    Serialization(String),

    /// Channel closed unexpectedly
    #[error("channel closed")]
    ChannelClosed,

    /// Write queue overflow (backpressure)
    #[error("queue full for workspace {workspace_id}")]
    QueueFull { workspace_id: String },
}

impl SinkError {
    /// Create an initialization error
    pub fn init(msg: impl Into<String>) -> Self {
        Self::Init(msg.into())
    }

    /// Create a write error
    pub fn write(msg: impl Into<String>) -> Self {
        Self::Write(msg.into())
    }

    /// Create a config error
    pub fn config(msg: impl Into<String>) -> Self {
        Self::Config(msg.into())
    }

    /// Create a queue full error
    pub fn queue_full(workspace_id: impl Into<String>) -> Self {
        Self::QueueFull {
            workspace_id: workspace_id.into(),
        }
    }
}

#[cfg(test)]
#[path = "common_test.rs"]
mod common_test;
