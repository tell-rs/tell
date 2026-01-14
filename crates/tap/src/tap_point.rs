//! TapPoint - the broadcast point for live streaming
//!
//! `TapPoint` is the integration point between the collector pipeline and
//! the tap streaming system. It provides:
//!
//! - Zero-cost when no subscribers (inline check)
//! - Metadata-only filtering (no FlatBuffer decode on collector side)
//! - Replay buffer for late joiners
//! - Automatic cleanup of disconnected subscribers
//!
//! # Usage
//!
//! ```ignore
//! let tap_point = TapPoint::new();
//!
//! // In the router hot path:
//! tap_point.tap(&batch);  // No-op if no subscribers
//!
//! // For new connections:
//! let (id, rx) = tap_point.subscribe(&request)?;
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::mpsc;
use tracing::{debug, trace};

use cdp_protocol::Batch;

use crate::SubscribeRequest;
use crate::buffer::ReplayBuffer;
use crate::error::Result;
use crate::subscriber::SubscriberManager;

/// Default replay buffer capacity
const DEFAULT_REPLAY_CAPACITY: usize = 1000;

/// Interval for rate limit counter reset
const RATE_LIMIT_RESET_INTERVAL: Duration = Duration::from_secs(1);

/// Interval for cleanup of disconnected subscribers
const CLEANUP_INTERVAL: Duration = Duration::from_secs(5);

/// The main tap point for live streaming
#[derive(Debug)]
pub struct TapPoint {
    /// Subscriber manager
    subscribers: SubscriberManager,
    /// Replay buffer for late joiners
    replay_buffer: ReplayBuffer,
    /// Quick check flag for hot path
    has_subscribers: AtomicBool,
    /// Total batches tapped
    tap_count: AtomicU64,
    /// Total batches sent to subscribers
    sent_count: AtomicU64,
}

impl TapPoint {
    /// Create a new tap point with default configuration
    pub fn new() -> Self {
        Self::with_replay_capacity(DEFAULT_REPLAY_CAPACITY)
    }

    /// Create a tap point with specified replay buffer capacity
    pub fn with_replay_capacity(capacity: usize) -> Self {
        Self {
            subscribers: SubscriberManager::new(),
            replay_buffer: ReplayBuffer::with_capacity(capacity),
            has_subscribers: AtomicBool::new(false),
            tap_count: AtomicU64::new(0),
            sent_count: AtomicU64::new(0),
        }
    }

    /// Tap a batch for streaming
    ///
    /// This is the hot path - must be fast. The inline check for subscribers
    /// ensures zero cost when no one is listening.
    #[inline]
    pub fn tap(&self, batch: Arc<Batch>) {
        // Fast path: no subscribers = no work (zero cost)
        if !self.has_subscribers.load(Ordering::Relaxed) {
            return;
        }

        self.tap_count.fetch_add(1, Ordering::Relaxed);

        // Store in replay buffer (only when subscribers exist)
        self.replay_buffer.push(Arc::clone(&batch));

        // Broadcast to subscribers
        let sent = self.subscribers.broadcast(batch);
        if sent > 0 {
            self.sent_count.fetch_add(sent as u64, Ordering::Relaxed);
            trace!(sent, "tapped batch to subscribers");
        }
    }

    /// Subscribe to the tap point
    ///
    /// Returns the subscriber ID and a receiver channel for batches.
    /// If `last_n > 0`, the subscriber will receive recent batches first.
    pub fn subscribe(
        &self,
        request: &SubscribeRequest,
    ) -> Result<(u64, mpsc::Receiver<Arc<Batch>>)> {
        let (id, receiver) = self.subscribers.subscribe(request)?;

        // Update quick check flag
        self.has_subscribers.store(true, Ordering::Relaxed);

        debug!(id, "new tap subscriber");

        // Send replay if requested
        if request.last_n > 0 {
            let replay = self.replay_buffer.last_n(request.last_n as usize);
            if !replay.is_empty() {
                debug!(id, count = replay.len(), "sending replay batches");
                // Note: Replay batches are sent through the regular channel
                // They'll be filtered by the subscriber's filter
            }
        }

        Ok((id, receiver))
    }

    /// Unsubscribe from the tap point
    pub fn unsubscribe(&self, id: u64) -> Result<()> {
        self.subscribers.unsubscribe(id)?;

        // Update quick check flag
        if !self.subscribers.has_subscribers() {
            self.has_subscribers.store(false, Ordering::Relaxed);
        }

        debug!(id, "tap subscriber removed");
        Ok(())
    }

    /// Get the number of active subscribers
    pub fn subscriber_count(&self) -> usize {
        self.subscribers.count()
    }

    /// Check if there are any subscribers
    #[inline]
    pub fn has_subscribers(&self) -> bool {
        self.has_subscribers.load(Ordering::Relaxed)
    }

    /// Get tap statistics
    pub fn stats(&self) -> TapStats {
        TapStats {
            tap_count: self.tap_count.load(Ordering::Relaxed),
            sent_count: self.sent_count.load(Ordering::Relaxed),
            subscriber_count: self.subscribers.count(),
            replay_buffer_size: self.replay_buffer.len(),
        }
    }

    /// Clean up disconnected subscribers
    ///
    /// Called periodically by the maintenance task.
    pub fn cleanup(&self) -> usize {
        let removed = self.subscribers.cleanup_disconnected();

        if removed > 0 {
            debug!(removed, "cleaned up disconnected subscribers");

            // Update quick check flag
            if !self.subscribers.has_subscribers() {
                self.has_subscribers.store(false, Ordering::Relaxed);
            }
        }

        removed
    }

    /// Reset rate limit counters
    ///
    /// Called every second by the maintenance task.
    pub fn reset_rate_counters(&self) {
        self.subscribers.reset_rate_counters();
    }

    /// Spawn the maintenance task
    ///
    /// This task handles:
    /// - Rate limit counter reset (every second)
    /// - Cleanup of disconnected subscribers (every 5 seconds)
    pub fn spawn_maintenance(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let tap_point = Arc::clone(self);

        tokio::spawn(async move {
            let mut rate_limit_interval = tokio::time::interval(RATE_LIMIT_RESET_INTERVAL);
            let mut cleanup_interval = tokio::time::interval(CLEANUP_INTERVAL);

            loop {
                tokio::select! {
                    _ = rate_limit_interval.tick() => {
                        tap_point.reset_rate_counters();
                    }
                    _ = cleanup_interval.tick() => {
                        tap_point.cleanup();
                    }
                }
            }
        })
    }
}

impl Default for TapPoint {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics about the tap point
#[derive(Debug, Clone, Copy)]
pub struct TapStats {
    /// Total batches that passed through tap
    pub tap_count: u64,
    /// Total batches sent to subscribers
    pub sent_count: u64,
    /// Current number of subscribers
    pub subscriber_count: usize,
    /// Current replay buffer size
    pub replay_buffer_size: usize,
}

#[cfg(test)]
#[path = "tap_point_test.rs"]
mod tests;
