//! Lock-free buffer pool for high-performance batch processing
//!
//! Provides pre-allocated `BytesMut` buffers to eliminate allocations
//! in the hot path. Uses a lock-free queue for O(1) get/put operations.
//!
//! # Performance
//!
//! - **Get**: ~10ns (lock-free pop)
//! - **Put**: ~10ns (lock-free push)
//! - **Fallback allocation**: Only when pool is empty
//!
//! # Example
//!
//! ```ignore
//! let pool = BufferPool::new(64, 32 * 1024 * 1024); // 64 buffers, 32MB each
//!
//! // Hot path - get buffer
//! let mut buf = pool.get();
//! serialize_batch(&batch, &mut buf);
//!
//! // After write completes - return buffer
//! pool.put(buf);
//! ```

use bytes::BytesMut;
use crossbeam::queue::ArrayQueue;
use std::sync::atomic::{AtomicU64, Ordering};

/// Lock-free pool of reusable `BytesMut` buffers
///
/// Pre-allocates buffers at construction time to avoid allocations
/// in the hot path. When the pool is exhausted, new buffers are
/// allocated on demand (and can be returned to the pool later).
pub struct BufferPool {
    /// Lock-free queue of available buffers
    queue: ArrayQueue<BytesMut>,

    /// Capacity for each buffer
    buffer_capacity: usize,

    /// Metrics
    metrics: BufferPoolMetrics,
}

/// Metrics for buffer pool monitoring
#[derive(Debug, Default)]
pub struct BufferPoolMetrics {
    /// Number of successful pool hits (buffer reused)
    pub hits: AtomicU64,

    /// Number of pool misses (new allocation required)
    pub misses: AtomicU64,

    /// Number of buffers returned to pool
    pub returns: AtomicU64,

    /// Number of buffers dropped (pool was full)
    pub drops: AtomicU64,
}

impl BufferPoolMetrics {
    /// Create new metrics instance
    pub const fn new() -> Self {
        Self {
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            returns: AtomicU64::new(0),
            drops: AtomicU64::new(0),
        }
    }

    /// Record a pool hit
    #[inline]
    pub fn record_hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a pool miss
    #[inline]
    pub fn record_miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a buffer return
    #[inline]
    pub fn record_return(&self) {
        self.returns.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a buffer drop (pool full)
    #[inline]
    pub fn record_drop(&self) {
        self.drops.fetch_add(1, Ordering::Relaxed);
    }

    /// Get snapshot of metrics
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            returns: self.returns.load(Ordering::Relaxed),
            drops: self.drops.load(Ordering::Relaxed),
        }
    }

    /// Calculate hit rate (0.0 to 1.0)
    pub fn hit_rate(&self) -> f64 {
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 {
            1.0
        } else {
            hits as f64 / total as f64
        }
    }
}

/// Point-in-time snapshot of buffer pool metrics
#[derive(Debug, Clone, Copy, Default)]
pub struct MetricsSnapshot {
    pub hits: u64,
    pub misses: u64,
    pub returns: u64,
    pub drops: u64,
}

impl MetricsSnapshot {
    /// Calculate hit rate from snapshot
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            1.0
        } else {
            self.hits as f64 / total as f64
        }
    }
}

impl BufferPool {
    /// Create a new buffer pool with pre-allocated buffers
    ///
    /// # Arguments
    ///
    /// * `pool_size` - Number of buffers to pre-allocate
    /// * `buffer_capacity` - Capacity of each buffer in bytes
    ///
    /// # Example
    ///
    /// ```ignore
    /// // 64 buffers of 32MB each = 2GB total pool
    /// let pool = BufferPool::new(64, 32 * 1024 * 1024);
    /// ```
    pub fn new(pool_size: usize, buffer_capacity: usize) -> Self {
        let queue = ArrayQueue::new(pool_size);

        // Pre-allocate all buffers
        for _ in 0..pool_size {
            let buf = BytesMut::with_capacity(buffer_capacity);
            // This should always succeed since we're filling an empty queue
            let _ = queue.push(buf);
        }

        Self {
            queue,
            buffer_capacity,
            metrics: BufferPoolMetrics::new(),
        }
    }

    /// Get a buffer from the pool
    ///
    /// Returns a pooled buffer if available, otherwise allocates a new one.
    /// This is the hot path - designed to be lock-free and fast.
    ///
    /// # Performance
    ///
    /// - Pool hit: ~10ns (lock-free pop)
    /// - Pool miss: ~1µs (allocation)
    #[inline]
    pub fn get(&self) -> BytesMut {
        match self.queue.pop() {
            Some(buf) => {
                self.metrics.record_hit();
                buf
            }
            None => {
                self.metrics.record_miss();
                BytesMut::with_capacity(self.buffer_capacity)
            }
        }
    }

    /// Return a buffer to the pool
    ///
    /// Clears the buffer and returns it to the pool if space is available.
    /// If the pool is full, the buffer is dropped (let GC handle it).
    ///
    /// # Performance
    ///
    /// - Pool not full: ~10ns (lock-free push)
    /// - Pool full: buffer dropped
    #[inline]
    pub fn put(&self, mut buf: BytesMut) {
        buf.clear();

        // Only return buffers that have sufficient capacity
        // (avoids pooling small buffers that grew from splits)
        if buf.capacity() >= self.buffer_capacity {
            match self.queue.push(buf) {
                Ok(()) => {
                    self.metrics.record_return();
                }
                Err(_) => {
                    // Pool is full, drop the buffer
                    self.metrics.record_drop();
                }
            }
        } else {
            self.metrics.record_drop();
        }
    }

    /// Get the number of buffers currently available in the pool
    #[inline]
    pub fn available(&self) -> usize {
        self.queue.len()
    }

    /// Get the pool capacity (maximum number of buffers)
    #[inline]
    pub fn capacity(&self) -> usize {
        self.queue.capacity()
    }

    /// Get the buffer capacity (size of each buffer)
    #[inline]
    pub fn buffer_capacity(&self) -> usize {
        self.buffer_capacity
    }

    /// Get reference to metrics
    #[inline]
    pub fn metrics(&self) -> &BufferPoolMetrics {
        &self.metrics
    }

    /// Check if the pool is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// Check if the pool is full
    #[inline]
    pub fn is_full(&self) -> bool {
        self.queue.is_full()
    }
}

// Safety: BufferPool is safe to share across threads
// - ArrayQueue is lock-free and thread-safe
// - AtomicU64 metrics are thread-safe
unsafe impl Send for BufferPool {}
unsafe impl Sync for BufferPool {}

#[cfg(test)]
#[path = "buffer_pool_test.rs"]
mod buffer_pool_test;
