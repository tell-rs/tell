//! Ring buffer for batch replay
//!
//! The `ReplayBuffer` stores the last N batches in a fixed-size ring buffer.
//! When a new subscriber connects with `last_n > 0`, they receive the most
//! recent batches before starting the live stream.
//!
//! This is useful for CLI users who want to see recent activity immediately.

use std::sync::Arc;

use parking_lot::RwLock;

use tell_protocol::Batch;

/// Default capacity for the replay buffer
const DEFAULT_CAPACITY: usize = 1000;

/// Maximum capacity to prevent memory issues
const MAX_CAPACITY: usize = 100_000;

/// Ring buffer for storing recent batches
#[derive(Debug)]
pub struct ReplayBuffer {
    /// Internal storage
    inner: RwLock<ReplayBufferInner>,
}

#[derive(Debug)]
struct ReplayBufferInner {
    /// The ring buffer
    buffer: Vec<Option<Arc<Batch>>>,
    /// Current write position
    write_pos: usize,
    /// Total batches ever written (for sequencing)
    total_written: u64,
    /// Capacity
    capacity: usize,
}

impl ReplayBuffer {
    /// Create a new replay buffer with default capacity
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_CAPACITY)
    }

    /// Create a replay buffer with specified capacity
    pub fn with_capacity(capacity: usize) -> Self {
        let capacity = capacity.min(MAX_CAPACITY);
        Self {
            inner: RwLock::new(ReplayBufferInner {
                buffer: vec![None; capacity],
                write_pos: 0,
                total_written: 0,
                capacity,
            }),
        }
    }

    /// Push a batch into the buffer
    pub fn push(&self, batch: Arc<Batch>) {
        let mut inner = self.inner.write();
        let pos = inner.write_pos;
        inner.buffer[pos] = Some(batch);
        inner.write_pos = (pos + 1) % inner.capacity;
        inner.total_written += 1;
    }

    /// Get the last N batches (oldest first)
    ///
    /// Returns up to `n` batches, or fewer if not enough are available.
    pub fn last_n(&self, n: usize) -> Vec<Arc<Batch>> {
        let inner = self.inner.read();

        let available = inner.total_written.min(inner.capacity as u64) as usize;
        let n = n.min(available);

        if n == 0 {
            return Vec::new();
        }

        let mut result = Vec::with_capacity(n);

        // Calculate start position
        // write_pos points to next write slot, so we read backwards from there
        let start = if inner.total_written >= inner.capacity as u64 {
            // Buffer is full, read from write_pos (oldest) forward
            (inner.write_pos + inner.capacity - n) % inner.capacity
        } else {
            // Buffer not full, oldest is at position 0
            inner.total_written as usize - n
        };

        for i in 0..n {
            let pos = (start + i) % inner.capacity;
            if let Some(ref batch) = inner.buffer[pos] {
                result.push(Arc::clone(batch));
            }
        }

        result
    }

    /// Get the total number of batches written
    pub fn total_written(&self) -> u64 {
        self.inner.read().total_written
    }

    /// Get the current fill level
    pub fn len(&self) -> usize {
        let inner = self.inner.read();
        inner.total_written.min(inner.capacity as u64) as usize
    }

    /// Check if buffer is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get capacity
    pub fn capacity(&self) -> usize {
        self.inner.read().capacity
    }

    /// Clear the buffer
    pub fn clear(&self) {
        let mut inner = self.inner.write();
        for slot in inner.buffer.iter_mut() {
            *slot = None;
        }
        inner.write_pos = 0;
        inner.total_written = 0;
    }
}

impl Default for ReplayBuffer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[path = "buffer_test.rs"]
mod tests;
