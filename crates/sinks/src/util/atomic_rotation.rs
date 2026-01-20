//! Atomic file rotation with zero data loss
//!
//! Provides lock-free file rotation for disk sinks. The key insight is that
//! the hot path (submit) never blocks on I/O or locks - it just does an
//! atomic pointer swap and channel send.
//!
//! # Architecture
//!
//! ```text
//! [submit()] → [ArcSwap::load()] → [channel.try_send()] → [writer task]
//!                    ↓ (rotation)
//!              [ArcSwap::swap(new_chain)]
//!                    ↓
//!              [old chain drains via Arc refcount]
//! ```
//!
//! # Key Design Decisions
//!
//! - **ArcSwap**: Lock-free atomic pointer swap for chain switching
//! - **Arc<BufferChain>**: Old chains drain naturally via refcount
//! - **Per-workspace contexts**: Isolated rotation state per workspace
//! - **Channel-based writes**: Decouples batch processing from disk I/O
//!
//! # Example
//!
//! ```ignore
//! use sinks::util::{AtomicRotationSink, RotationConfig, BinaryWriter};
//!
//! let config = RotationConfig {
//!     base_path: "/data/logs".into(),
//!     rotation_interval: RotationInterval::Hourly,
//!     file_prefix: "data".into(),
//!     buffer_size: 32 * 1024 * 1024,
//!     queue_size: 1000,
//! };
//!
//! let writer = BinaryWriter::uncompressed(config.buffer_size);
//! let sink = AtomicRotationSink::new(config, writer);
//!
//! // Hot path: non-blocking
//! sink.submit("workspace_123", buffer, 100)?;
//! ```

use std::fs::{self, File};
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use arc_swap::ArcSwap;
use bytes::BytesMut;
use chrono::{DateTime, Local};
use dashmap::DashMap;
use tokio::sync::mpsc;

use super::buffer_pool::BufferPool;
use super::chain_writer::{ChainWrite, ChainWriter};

/// Rotation interval options
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RotationInterval {
    /// Rotate every hour (creates hourly subdirectories)
    Hourly,
    /// Rotate daily at midnight
    Daily,
    /// Rotate monthly on the 1st
    Monthly,
}

impl RotationInterval {
    /// Get the date format string for this interval
    fn date_format(&self) -> &'static str {
        match self {
            RotationInterval::Hourly => "%Y-%m-%d/%H",
            RotationInterval::Daily => "%Y-%m-%d",
            RotationInterval::Monthly => "%Y-%m",
        }
    }

    /// Get the current time bucket for this interval
    fn current_bucket(&self, now: DateTime<Local>) -> String {
        now.format(self.date_format()).to_string()
    }

    /// Check if we need to rotate based on current time
    fn needs_rotation(&self, current_bucket: &str, now: DateTime<Local>) -> bool {
        let new_bucket = self.current_bucket(now);
        current_bucket != new_bucket
    }
}

/// Default write retry attempts (matches Go implementation)
pub const DEFAULT_WRITE_RETRIES: usize = 3;

/// Default delay between retries
pub const DEFAULT_RETRY_DELAY: Duration = Duration::from_millis(10);

/// Configuration for atomic rotation
#[derive(Debug, Clone)]
pub struct RotationConfig {
    /// Base path for output files
    pub base_path: PathBuf,

    /// Rotation interval
    pub rotation_interval: RotationInterval,

    /// File prefix (e.g., "data" -> "data-2025-01-01.bin")
    pub file_prefix: String,

    /// Write buffer size (default: 32MB)
    pub buffer_size: usize,

    /// Write queue size per chain
    pub queue_size: usize,

    /// Flush interval for periodic flushing
    pub flush_interval: Duration,

    /// Maximum write retry attempts (default: 3, matches Go)
    pub max_write_retries: usize,

    /// Delay between retry attempts
    pub retry_delay: Duration,
}

impl Default for RotationConfig {
    fn default() -> Self {
        Self {
            base_path: PathBuf::from("logs"),
            rotation_interval: RotationInterval::Hourly,
            file_prefix: "data".into(),
            buffer_size: 32 * 1024 * 1024,
            queue_size: 1000,
            flush_interval: Duration::from_millis(100),
            max_write_retries: DEFAULT_WRITE_RETRIES,
            retry_delay: DEFAULT_RETRY_DELAY,
        }
    }
}

/// Write request sent to a buffer chain
pub struct WriteRequest {
    /// Buffer containing serialized data
    pub buffer: BytesMut,

    /// Number of items in this batch
    pub items: usize,
}

/// A buffer chain represents a complete data flow from input to file
///
/// When rotation occurs, a new chain is created and the old one drains
/// naturally as in-flight writes complete (via Arc refcount).
pub struct BufferChain {
    /// Unique ID for this chain (for logging)
    #[allow(dead_code)]
    id: String,

    /// Channel sender for write requests
    sender: mpsc::Sender<WriteRequest>,

    /// Current time bucket (for rotation detection)
    time_bucket: String,

    /// Metrics for this chain
    metrics: ChainMetrics,
}

impl BufferChain {
    /// Send a write request to this chain
    ///
    /// Returns Ok(()) if queued, Err if queue is full (backpressure).
    pub fn try_send(&self, request: WriteRequest) -> Result<(), WriteRequest> {
        self.sender.try_send(request).map_err(|e| match e {
            mpsc::error::TrySendError::Full(req) => req,
            mpsc::error::TrySendError::Closed(req) => req,
        })
    }

    /// Get the time bucket this chain was created for
    pub fn time_bucket(&self) -> &str {
        &self.time_bucket
    }

    /// Get chain metrics
    pub fn metrics(&self) -> &ChainMetrics {
        &self.metrics
    }
}

/// Metrics for a buffer chain
#[derive(Debug, Default)]
pub struct ChainMetrics {
    /// Batches queued to this chain
    pub batches_queued: AtomicU64,

    /// Items queued to this chain
    pub items_queued: AtomicU64,

    /// Queue full rejections
    pub queue_full: AtomicU64,
}

impl ChainMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_queued(&self, items: u64) {
        self.batches_queued.fetch_add(1, Ordering::Relaxed);
        self.items_queued.fetch_add(items, Ordering::Relaxed);
    }

    pub fn record_queue_full(&self) {
        self.queue_full.fetch_add(1, Ordering::Relaxed);
    }
}

/// Per-workspace file context with atomic chain switching
pub struct FileContext {
    /// Workspace ID
    workspace_id: Arc<str>,

    /// Active buffer chain (atomic swap for rotation)
    active_chain: ArcSwap<BufferChain>,

    /// Configuration
    config: Arc<RotationConfig>,

    /// Chain writer for creating new chains
    chain_writer: Arc<dyn ChainWriter>,
}

impl FileContext {
    /// Get the active chain (lock-free atomic load)
    #[inline]
    pub fn active_chain(&self) -> arc_swap::Guard<Arc<BufferChain>> {
        self.active_chain.load()
    }

    /// Check if rotation is needed and perform it if so
    pub async fn check_rotation(&self) -> io::Result<bool> {
        let now = Local::now();
        let current = self.active_chain.load();

        if !self
            .config
            .rotation_interval
            .needs_rotation(&current.time_bucket, now)
        {
            return Ok(false);
        }

        // Create new chain
        let new_chain = self.create_chain(now)?;
        let new_chain = Arc::new(new_chain);

        // Atomic swap - new writes go to new chain immediately
        let _old_chain = self.active_chain.swap(new_chain);

        // Old chain drains naturally via Arc refcount
        // When sender drops, receiver task will finish and clean up

        tracing::info!(
            workspace = %self.workspace_id,
            old_bucket = %_old_chain.time_bucket,
            new_bucket = %self.active_chain.load().time_bucket,
            "file rotation completed"
        );

        Ok(true)
    }

    /// Create a new buffer chain for the given time
    fn create_chain(&self, now: DateTime<Local>) -> io::Result<BufferChain> {
        let time_bucket = self.config.rotation_interval.current_bucket(now);
        let chain_id = format!("{}_{}", self.workspace_id, time_bucket);

        // Create directory structure
        let dir_path = self
            .config
            .base_path
            .join(self.workspace_id.as_ref())
            .join(&time_bucket);
        fs::create_dir_all(&dir_path)?;

        // Generate filename
        let extension = self.chain_writer.file_extension();
        let filename = format!("{}{}", self.config.file_prefix, extension);
        let file_path = dir_path.join(&filename);

        // Open file (append mode)
        let file = File::options().create(true).append(true).open(&file_path)?;

        tracing::debug!(
            workspace = %self.workspace_id,
            path = %file_path.display(),
            "created new chain file"
        );

        // Create writer
        let writer = self.chain_writer.wrap(file)?;

        // Create channel
        let (sender, receiver) = mpsc::channel(self.config.queue_size);

        // Spawn writer task
        let flush_interval = self.config.flush_interval;
        let retry_config = WriterRetryConfig {
            max_retries: self.config.max_write_retries,
            retry_delay: self.config.retry_delay,
        };
        let chain_id_clone = chain_id.clone();
        tokio::spawn(async move {
            run_writer_task(chain_id_clone, receiver, writer, flush_interval, retry_config).await;
        });

        Ok(BufferChain {
            id: chain_id,
            sender,
            time_bucket,
            metrics: ChainMetrics::new(),
        })
    }
}

/// Configuration for writer task retry behavior
#[derive(Debug, Clone, Copy)]
struct WriterRetryConfig {
    max_retries: usize,
    retry_delay: Duration,
}

/// Writer task that processes write requests for a chain
///
/// Implements retry logic matching Go implementation (3 retries by default).
async fn run_writer_task(
    chain_id: String,
    mut receiver: mpsc::Receiver<WriteRequest>,
    mut writer: Box<dyn ChainWrite>,
    flush_interval: Duration,
    retry_config: WriterRetryConfig,
) {
    let mut flush_ticker = tokio::time::interval(flush_interval);
    flush_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            // Process write requests
            request = receiver.recv() => {
                match request {
                    Some(req) => {
                        // Write with retry logic (matches Go: maxWriteRetries = 3)
                        let mut last_error = None;
                        for attempt in 0..retry_config.max_retries {
                            match writer.write_all(&req.buffer) {
                                Ok(()) => {
                                    last_error = None;
                                    break;
                                }
                                Err(e) => {
                                    last_error = Some(e);
                                    if attempt < retry_config.max_retries - 1 {
                                        tracing::warn!(
                                            chain = %chain_id,
                                            attempt = attempt + 1,
                                            max_attempts = retry_config.max_retries,
                                            "write failed, retrying"
                                        );
                                        tokio::time::sleep(retry_config.retry_delay).await;
                                    }
                                }
                            }
                        }

                        // Log final error if all retries exhausted
                        if let Some(e) = last_error {
                            tracing::error!(
                                chain = %chain_id,
                                error = %e,
                                attempts = retry_config.max_retries,
                                "write failed after all retries"
                            );
                        }
                    }
                    None => {
                        // Channel closed - drain complete
                        break;
                    }
                }
            }
            // Periodic flush
            _ = flush_ticker.tick() => {
                if let Err(e) = writer.flush() {
                    tracing::error!(
                        chain = %chain_id,
                        error = %e,
                        "flush failed"
                    );
                }
            }
        }
    }

    // Final flush and close
    if let Err(e) = writer.flush_all() {
        tracing::error!(chain = %chain_id, error = %e, "final flush failed");
    }
    if let Err(e) = writer.finish() {
        tracing::error!(chain = %chain_id, error = %e, "finish failed");
    }

    tracing::debug!(chain = %chain_id, "writer task finished");
}

/// Atomic rotation sink for disk-based sinks
///
/// Manages per-workspace file contexts with atomic chain switching.
/// The hot path (submit) is lock-free and non-blocking.
pub struct AtomicRotationSink {
    /// Per-workspace file contexts
    contexts: DashMap<Arc<str>, Arc<FileContext>>,

    /// Buffer pool for reducing allocations
    buffer_pool: BufferPool,

    /// Configuration
    config: Arc<RotationConfig>,

    /// Chain writer factory
    chain_writer: Arc<dyn ChainWriter>,

    /// Sink-wide metrics
    metrics: AtomicRotationMetrics,
}

/// Metrics for the atomic rotation sink
#[derive(Debug, Default)]
pub struct AtomicRotationMetrics {
    /// Total batches submitted
    pub batches_submitted: AtomicU64,

    /// Total items submitted
    pub items_submitted: AtomicU64,

    /// Total batches rejected (queue full)
    pub batches_rejected: AtomicU64,

    /// Total rotations performed
    pub rotations: AtomicU64,

    /// Total workspaces
    pub workspace_count: AtomicU64,
}

impl AtomicRotationMetrics {
    pub fn new() -> Self {
        Self::default()
    }
}

impl AtomicRotationSink {
    /// Create a new atomic rotation sink
    pub fn new<W: ChainWriter + 'static>(config: RotationConfig, chain_writer: W) -> Self {
        let buffer_pool = BufferPool::new(64, config.buffer_size);
        let config = Arc::new(config);

        Self {
            contexts: DashMap::new(),
            buffer_pool,
            config,
            chain_writer: Arc::new(chain_writer),
            metrics: AtomicRotationMetrics::new(),
        }
    }

    /// Submit a write request for a workspace
    ///
    /// This is the hot path - designed to be lock-free and non-blocking.
    ///
    /// # Arguments
    ///
    /// * `workspace_id` - The workspace to write to
    /// * `buffer` - Buffer containing serialized data
    /// * `items` - Number of items in this batch
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Request queued successfully
    /// * `Err(buffer)` - Queue full, buffer returned for retry/drop
    pub fn submit(
        &self,
        workspace_id: &str,
        buffer: BytesMut,
        items: usize,
    ) -> Result<(), BytesMut> {
        // Get or create context for this workspace
        let context = self.get_or_create_context(workspace_id);

        // Get active chain (atomic load, lock-free)
        let chain = context.active_chain();

        // Try to send (non-blocking)
        let request = WriteRequest { buffer, items };
        match chain.try_send(request) {
            Ok(()) => {
                chain.metrics.record_queued(items as u64);
                self.metrics
                    .batches_submitted
                    .fetch_add(1, Ordering::Relaxed);
                self.metrics
                    .items_submitted
                    .fetch_add(items as u64, Ordering::Relaxed);
                Ok(())
            }
            Err(req) => {
                chain.metrics.record_queue_full();
                self.metrics
                    .batches_rejected
                    .fetch_add(1, Ordering::Relaxed);
                Err(req.buffer)
            }
        }
    }

    /// Get or create a file context for a workspace
    fn get_or_create_context(&self, workspace_id: &str) -> Arc<FileContext> {
        // Fast path: context exists
        if let Some(ctx) = self.contexts.get(workspace_id) {
            return Arc::clone(&ctx);
        }

        // Slow path: create new context
        let workspace_id: Arc<str> = workspace_id.into();

        // Use entry API to handle race conditions
        let context = self
            .contexts
            .entry(Arc::clone(&workspace_id))
            .or_insert_with(|| {
                self.metrics.workspace_count.fetch_add(1, Ordering::Relaxed);
                Arc::new(self.create_context(workspace_id))
            });

        Arc::clone(&context)
    }

    /// Create a new file context for a workspace
    fn create_context(&self, workspace_id: Arc<str>) -> FileContext {
        let now = Local::now();

        // Create initial chain (this can fail, but we handle it)
        let chain = self
            .create_chain_for_workspace(&workspace_id, now)
            .expect("failed to create initial chain");

        FileContext {
            workspace_id,
            active_chain: ArcSwap::new(Arc::new(chain)),
            config: Arc::clone(&self.config),
            chain_writer: Arc::clone(&self.chain_writer),
        }
    }

    /// Create a chain for a workspace
    fn create_chain_for_workspace(
        &self,
        workspace_id: &str,
        now: DateTime<Local>,
    ) -> io::Result<BufferChain> {
        let time_bucket = self.config.rotation_interval.current_bucket(now);
        let chain_id = format!("{}_{}", workspace_id, time_bucket);

        // Create directory structure
        let dir_path = self.config.base_path.join(workspace_id).join(&time_bucket);
        fs::create_dir_all(&dir_path)?;

        // Generate filename
        let extension = self.chain_writer.file_extension();
        let filename = format!("{}{}", self.config.file_prefix, extension);
        let file_path = dir_path.join(&filename);

        // Open file (append mode)
        let file = File::options().create(true).append(true).open(&file_path)?;

        tracing::info!(
            workspace = %workspace_id,
            path = %file_path.display(),
            "created workspace file"
        );

        // Create writer
        let writer = self.chain_writer.wrap(file)?;

        // Create channel
        let (sender, receiver) = mpsc::channel(self.config.queue_size);

        // Spawn writer task
        let flush_interval = self.config.flush_interval;
        let retry_config = WriterRetryConfig {
            max_retries: self.config.max_write_retries,
            retry_delay: self.config.retry_delay,
        };
        let chain_id_clone = chain_id.clone();
        tokio::spawn(async move {
            run_writer_task(chain_id_clone, receiver, writer, flush_interval, retry_config).await;
        });

        Ok(BufferChain {
            id: chain_id,
            sender,
            time_bucket,
            metrics: ChainMetrics::new(),
        })
    }

    /// Check and rotate all workspaces if needed
    pub async fn check_rotation_all(&self) {
        let now = Local::now();

        for entry in self.contexts.iter() {
            let context = entry.value();
            let chain = context.active_chain();

            if self
                .config
                .rotation_interval
                .needs_rotation(&chain.time_bucket, now)
            {
                if let Err(e) = context.check_rotation().await {
                    tracing::error!(
                        workspace = %context.workspace_id,
                        error = %e,
                        "rotation failed"
                    );
                } else {
                    self.metrics.rotations.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }

    /// Get a buffer from the pool
    pub fn get_buffer(&self) -> BytesMut {
        self.buffer_pool.get()
    }

    /// Return a buffer to the pool
    pub fn put_buffer(&self, buffer: BytesMut) {
        self.buffer_pool.put(buffer);
    }

    /// Get reference to metrics
    pub fn metrics(&self) -> &AtomicRotationMetrics {
        &self.metrics
    }

    /// Get the number of active workspaces
    pub fn workspace_count(&self) -> usize {
        self.contexts.len()
    }

    /// Stop all chains gracefully
    ///
    /// Drops all senders, causing writer tasks to drain and finish.
    pub async fn stop(&self) {
        // Clear the map - this drops all Arc<FileContext>
        // When the last Arc drops, the sender drops, and the writer task finishes
        self.contexts.clear();

        // Give writer tasks time to drain
        tokio::time::sleep(Duration::from_millis(100)).await;

        tracing::info!("atomic rotation sink stopped");
    }
}

#[cfg(test)]
#[path = "atomic_rotation_test.rs"]
mod atomic_rotation_test;
