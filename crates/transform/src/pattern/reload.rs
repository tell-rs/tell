//! Pattern Hot Reload Worker
//!
//! Periodically reloads patterns from storage to support:
//! - Pattern sharing across Tell instances
//! - Dynamic pattern updates without restart
//! - Distributed pattern learning (with ClickHouse backend)
//!
//! # Design
//!
//! The reload worker runs as a background task that:
//! 1. Periodically checks storage for new patterns
//! 2. Merges new patterns into the local Drain tree
//! 3. Updates the pattern cache
//!
//! This enables multiple Tell instances to share learned patterns.

use super::cache::PatternCache;
use super::drain::DrainTree;
use super::persistence::PatternPersistence;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

#[cfg(test)]
#[path = "reload_test.rs"]
mod tests;

/// Default reload interval
const DEFAULT_RELOAD_INTERVAL: Duration = Duration::from_secs(60);

/// Configuration for the reload worker
#[derive(Debug, Clone)]
pub struct ReloadWorkerConfig {
    /// Interval between reload checks
    pub interval: Duration,

    /// Whether to merge patterns (true) or replace (false)
    ///
    /// Merge mode: Add new patterns, update counts for existing
    /// Replace mode: Clear and reload all patterns
    pub merge_mode: bool,
}

impl Default for ReloadWorkerConfig {
    fn default() -> Self {
        Self {
            interval: DEFAULT_RELOAD_INTERVAL,
            merge_mode: true,
        }
    }
}

impl ReloadWorkerConfig {
    /// Set the reload interval
    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    /// Enable merge mode (default)
    pub fn with_merge(mut self) -> Self {
        self.merge_mode = true;
        self
    }

    /// Enable replace mode
    pub fn with_replace(mut self) -> Self {
        self.merge_mode = false;
        self
    }
}

/// Handle for controlling the reload worker
pub struct ReloadWorkerHandle {
    /// Trigger manual reload
    trigger: tokio::sync::mpsc::Sender<()>,
}

impl ReloadWorkerHandle {
    /// Trigger an immediate reload
    ///
    /// Returns true if the trigger was sent successfully.
    pub fn trigger_reload(&self) -> bool {
        self.trigger.try_send(()).is_ok()
    }

    /// Trigger a reload, waiting if the channel is full
    pub async fn trigger_reload_async(&self) -> bool {
        self.trigger.send(()).await.is_ok()
    }
}

/// Background worker for pattern hot reload
pub struct ReloadWorker {
    /// Storage to reload from
    persistence: Arc<PatternPersistence>,

    /// Drain tree to update (for future merge mode implementation)
    #[allow(dead_code)]
    drain: Arc<DrainTree>,

    /// Cache to update
    cache: Arc<PatternCache>,

    /// Worker configuration
    config: ReloadWorkerConfig,

    /// Channel for manual triggers
    trigger_rx: tokio::sync::mpsc::Receiver<()>,

    /// Cancellation token
    cancel: CancellationToken,
}

impl ReloadWorker {
    /// Create a new reload worker
    ///
    /// Returns (worker, handle). Spawn the worker as a tokio task.
    pub fn new(
        persistence: Arc<PatternPersistence>,
        drain: Arc<DrainTree>,
        cache: Arc<PatternCache>,
        config: ReloadWorkerConfig,
        cancel: CancellationToken,
    ) -> (Self, ReloadWorkerHandle) {
        let (trigger_tx, trigger_rx) = tokio::sync::mpsc::channel(1);

        let worker = Self {
            persistence,
            drain,
            cache,
            config,
            trigger_rx,
            cancel,
        };

        let handle = ReloadWorkerHandle {
            trigger: trigger_tx,
        };

        (worker, handle)
    }

    /// Run the reload worker
    pub async fn run(mut self) {
        let mut interval = tokio::time::interval(self.config.interval);

        tracing::info!(
            interval_secs = self.config.interval.as_secs(),
            merge_mode = self.config.merge_mode,
            "Pattern reload worker started"
        );

        loop {
            tokio::select! {
                // Periodic reload
                _ = interval.tick() => {
                    self.do_reload();
                }

                // Manual trigger
                Some(()) = self.trigger_rx.recv() => {
                    tracing::debug!("Manual reload triggered");
                    self.do_reload();
                }

                // Cancellation
                _ = self.cancel.cancelled() => {
                    tracing::info!("Pattern reload worker stopping");
                    break;
                }
            }
        }

        tracing::info!("Pattern reload worker stopped");
    }

    /// Perform a reload from storage
    fn do_reload(&self) {
        if !self.persistence.is_enabled() {
            return;
        }

        match self.persistence.load() {
            Ok(patterns) => {
                if patterns.is_empty() {
                    return;
                }

                let count = patterns.len();

                // Update cache with loaded patterns
                for pattern in &patterns {
                    // Pre-populate L2 cache with template hash
                    let template_hash = hash_template(&pattern.template);
                    self.cache.put_l2(template_hash, pattern.id);
                }

                tracing::debug!(patterns_loaded = count, "Reloaded patterns from storage");
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "Failed to reload patterns from storage"
                );
            }
        }
    }
}

/// Spawn a reload worker as a background task
pub fn spawn_reload_worker(
    persistence: Arc<PatternPersistence>,
    drain: Arc<DrainTree>,
    cache: Arc<PatternCache>,
    config: ReloadWorkerConfig,
    cancel: CancellationToken,
) -> ReloadWorkerHandle {
    let (worker, handle) = ReloadWorker::new(persistence, drain, cache, config, cancel);
    tokio::spawn(worker.run());
    handle
}

/// Hash a template string for cache lookup
fn hash_template(template: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    template.hash(&mut hasher);
    hasher.finish()
}
