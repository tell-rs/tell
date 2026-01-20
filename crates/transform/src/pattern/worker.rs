//! Background Pattern Worker - Async pattern persistence
//!
//! Provides non-blocking pattern registration by running persistence
//! operations in a background task.
//!
//! # Design
//!
//! - Patterns are sent to a channel (non-blocking)
//! - Background worker collects patterns and batches them
//! - Flushes to persistence on batch threshold or time interval
//! - Graceful shutdown flushes remaining patterns

use super::drain::Pattern;
use super::persistence::PatternPersistence;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

#[cfg(test)]
#[path = "worker_test.rs"]
mod tests;

/// Default channel capacity for pattern queue
const DEFAULT_CHANNEL_CAPACITY: usize = 10_000;

/// Default flush interval
const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_secs(5);

/// Default batch size before flush
const DEFAULT_BATCH_SIZE: usize = 100;

/// Handle for sending patterns to the background worker
#[derive(Clone)]
pub struct PatternWorkerHandle {
    /// Channel sender for new patterns
    sender: mpsc::Sender<Pattern>,
}

impl PatternWorkerHandle {
    /// Send a pattern to the background worker (non-blocking)
    ///
    /// Returns `true` if pattern was queued, `false` if channel is full.
    /// A full channel means the worker is overwhelmed - pattern will
    /// be picked up by periodic stats update instead.
    pub fn send(&self, pattern: Pattern) -> bool {
        self.sender.try_send(pattern).is_ok()
    }

    /// Send a pattern, waiting if channel is full
    ///
    /// Use this when you must ensure the pattern is queued.
    pub async fn send_async(&self, pattern: Pattern) -> bool {
        self.sender.send(pattern).await.is_ok()
    }

    /// Check if the worker is still running (channel not closed)
    pub fn is_active(&self) -> bool {
        !self.sender.is_closed()
    }
}

/// Configuration for the background worker
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// Channel capacity for pattern queue
    pub channel_capacity: usize,

    /// Time interval between flushes
    pub flush_interval: Duration,

    /// Number of patterns to batch before flush
    pub batch_size: usize,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
            flush_interval: DEFAULT_FLUSH_INTERVAL,
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }
}

impl WorkerConfig {
    /// Create config with custom flush interval
    pub fn with_flush_interval(mut self, interval: Duration) -> Self {
        self.flush_interval = interval;
        self
    }

    /// Create config with custom batch size
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size.max(1);
        self
    }

    /// Create config with custom channel capacity
    pub fn with_channel_capacity(mut self, capacity: usize) -> Self {
        self.channel_capacity = capacity.max(1);
        self
    }
}

/// Background worker for pattern persistence
pub struct PatternWorker {
    /// Channel receiver for new patterns
    receiver: mpsc::Receiver<Pattern>,

    /// Persistence manager
    persistence: Arc<PatternPersistence>,

    /// Worker configuration
    config: WorkerConfig,

    /// Cancellation token for graceful shutdown
    cancel: CancellationToken,
}

impl PatternWorker {
    /// Create a new background worker and return handle
    ///
    /// Returns a tuple of (worker, handle). The worker should be spawned
    /// as a tokio task, and the handle is used to send patterns.
    pub fn new(
        persistence: Arc<PatternPersistence>,
        config: WorkerConfig,
        cancel: CancellationToken,
    ) -> (Self, PatternWorkerHandle) {
        let (sender, receiver) = mpsc::channel(config.channel_capacity);

        let worker = Self {
            receiver,
            persistence,
            config,
            cancel,
        };

        let handle = PatternWorkerHandle { sender };

        (worker, handle)
    }

    /// Run the background worker
    ///
    /// This should be spawned as a tokio task:
    /// ```ignore
    /// tokio::spawn(worker.run());
    /// ```
    pub async fn run(mut self) {
        let mut batch: Vec<Pattern> = Vec::with_capacity(self.config.batch_size);
        let mut interval = tokio::time::interval(self.config.flush_interval);

        tracing::info!(
            flush_interval_secs = self.config.flush_interval.as_secs(),
            batch_size = self.config.batch_size,
            "Pattern background worker started"
        );

        loop {
            tokio::select! {
                // Receive new pattern
                Some(pattern) = self.receiver.recv() => {
                    batch.push(pattern);

                    // Flush if batch is full
                    if batch.len() >= self.config.batch_size {
                        self.flush_batch(&mut batch);
                    }
                }

                // Periodic flush
                _ = interval.tick() => {
                    if !batch.is_empty() {
                        self.flush_batch(&mut batch);
                    }
                }

                // Cancellation
                _ = self.cancel.cancelled() => {
                    tracing::info!("Pattern worker received shutdown signal");
                    break;
                }
            }
        }

        // Drain remaining patterns from channel
        while let Ok(pattern) = self.receiver.try_recv() {
            batch.push(pattern);
        }

        // Final flush
        if !batch.is_empty() {
            tracing::info!(
                count = batch.len(),
                "Flushing remaining patterns on shutdown"
            );
            self.flush_batch(&mut batch);
        }

        tracing::info!("Pattern background worker stopped");
    }

    /// Flush batch to persistence
    fn flush_batch(&self, batch: &mut Vec<Pattern>) {
        if batch.is_empty() {
            return;
        }

        let count = batch.len();

        // Add patterns to persistence
        for pattern in batch.drain(..) {
            self.persistence.add_pattern(&pattern);
        }

        // Flush to disk
        match self.persistence.flush() {
            Ok(flushed) => {
                tracing::debug!(patterns = flushed, "Flushed patterns to persistence");
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    patterns = count,
                    "Failed to flush patterns to persistence"
                );
            }
        }
    }
}

/// Spawn a background pattern worker
///
/// Convenience function that creates and spawns the worker.
/// Returns the handle for sending patterns.
pub fn spawn_pattern_worker(
    persistence: Arc<PatternPersistence>,
    config: WorkerConfig,
    cancel: CancellationToken,
) -> PatternWorkerHandle {
    let (worker, handle) = PatternWorker::new(persistence, config, cancel);
    tokio::spawn(worker.run());
    handle
}
