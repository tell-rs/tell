//! Sharded sender for distributing batches across multiple router workers
//!
//! This module provides a `ShardedSender` that distributes batches across N channels,
//! each feeding a separate router worker. This reduces contention when many connections
//! are sending batches simultaneously.
//!
//! # Design
//!
//! - Each connection is assigned to a shard based on a connection ID
//! - Connection ID is assigned atomically when calling `get_sender()`
//! - All batches from the same connection go to the same shard (preserves ordering)
//! - Number of shards typically equals CPU cores for optimal parallelism

use std::sync::atomic::{AtomicU64, Ordering};

use tell_protocol::Batch;
use crossfire::{MAsyncTx, TrySendError};

/// Sharded sender that distributes batches across multiple channels
///
/// Each channel feeds a separate router worker, reducing contention
/// when many connections are sending batches simultaneously.
#[derive(Clone)]
pub struct ShardedSender {
    /// One sender per shard
    shards: Vec<MAsyncTx<Batch>>,
    /// Counter for assigning connection IDs
    next_connection_id: std::sync::Arc<AtomicU64>,
}

impl ShardedSender {
    /// Create a new sharded sender from a vector of channel senders
    ///
    /// # Panics
    ///
    /// Panics if `shards` is empty.
    pub fn new(shards: Vec<MAsyncTx<Batch>>) -> Self {
        assert!(!shards.is_empty(), "ShardedSender requires at least one shard");
        Self {
            shards,
            next_connection_id: std::sync::Arc::new(AtomicU64::new(0)),
        }
    }

    /// Get the number of shards
    #[inline]
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Allocate a new connection ID
    ///
    /// Each connection should call this once and use the returned ID
    /// for all subsequent `try_send` calls to maintain ordering.
    #[inline]
    pub fn allocate_connection_id(&self) -> u64 {
        self.next_connection_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Try to send a batch to the appropriate shard
    ///
    /// The shard is selected based on `connection_id % shard_count`.
    /// This ensures all batches from the same connection go to the same shard,
    /// preserving ordering within a connection.
    #[inline]
    #[allow(clippy::result_large_err)] // TrySendError returns the Batch for retry/recovery
    pub fn try_send(&self, batch: Batch, connection_id: u64) -> Result<(), TrySendError<Batch>> {
        let shard_idx = (connection_id as usize) % self.shards.len();
        self.shards[shard_idx].try_send(batch)
    }

    /// Send a batch to the appropriate shard (blocking if full)
    ///
    /// The shard is selected based on `connection_id % shard_count`.
    #[inline]
    pub async fn send(&self, batch: Batch, connection_id: u64) -> Result<(), crossfire::SendError<Batch>> {
        let shard_idx = (connection_id as usize) % self.shards.len();
        self.shards[shard_idx].send(batch).await
    }
}

impl std::fmt::Debug for ShardedSender {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardedSender")
            .field("shard_count", &self.shards.len())
            .field("next_connection_id", &self.next_connection_id.load(Ordering::Relaxed))
            .finish()
    }
}
