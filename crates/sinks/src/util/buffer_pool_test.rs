//! Tests for the lock-free buffer pool

use crate::util::buffer_pool::{BufferPool, BufferPoolMetrics};
use bytes::BytesMut;
use std::sync::Arc;
use std::thread;

#[test]
fn test_new_pool() {
    let pool = BufferPool::new(10, 1024);

    assert_eq!(pool.capacity(), 10);
    assert_eq!(pool.available(), 10);
    assert_eq!(pool.buffer_capacity(), 1024);
    assert!(!pool.is_empty());
    assert!(pool.is_full());
}

#[test]
fn test_get_returns_buffer_with_capacity() {
    let pool = BufferPool::new(5, 4096);

    let buf = pool.get();
    assert!(buf.capacity() >= 4096);
    assert_eq!(pool.available(), 4);
}

#[test]
fn test_get_from_empty_pool_allocates() {
    let pool = BufferPool::new(2, 1024);

    // Drain the pool
    let _b1 = pool.get();
    let _b2 = pool.get();
    assert!(pool.is_empty());

    // Should still get a buffer (newly allocated)
    let b3 = pool.get();
    assert!(b3.capacity() >= 1024);

    // Check metrics
    let snapshot = pool.metrics().snapshot();
    assert_eq!(snapshot.hits, 2);
    assert_eq!(snapshot.misses, 1);
}

#[test]
fn test_put_returns_buffer_to_pool() {
    let pool = BufferPool::new(5, 1024);

    let buf = pool.get();
    assert_eq!(pool.available(), 4);

    pool.put(buf);
    assert_eq!(pool.available(), 5);

    let snapshot = pool.metrics().snapshot();
    assert_eq!(snapshot.returns, 1);
}

#[test]
fn test_put_clears_buffer() {
    let pool = BufferPool::new(5, 1024);

    let mut buf = pool.get();
    buf.extend_from_slice(b"hello world");
    assert!(!buf.is_empty());

    pool.put(buf);

    // Get the buffer back and verify it's cleared
    let buf2 = pool.get();
    assert!(buf2.is_empty());
}

#[test]
fn test_put_drops_when_pool_full() {
    let pool = BufferPool::new(2, 1024);

    // Pool is already full
    assert!(pool.is_full());

    // Create a new buffer and try to return it
    let buf = BytesMut::with_capacity(1024);
    pool.put(buf);

    // Should have been dropped
    let snapshot = pool.metrics().snapshot();
    assert_eq!(snapshot.drops, 1);
    assert_eq!(pool.available(), 2);
}

#[test]
fn test_put_drops_small_buffers() {
    let pool = BufferPool::new(5, 1024);

    let _buf = pool.get(); // Remove one to make room

    // Create a small buffer (below threshold)
    let small_buf = BytesMut::with_capacity(100);
    pool.put(small_buf);

    // Should have been dropped due to small capacity
    let snapshot = pool.metrics().snapshot();
    assert_eq!(snapshot.drops, 1);
}

#[test]
fn test_metrics_hit_rate() {
    let pool = BufferPool::new(2, 1024);

    // 2 hits
    let _b1 = pool.get();
    let _b2 = pool.get();

    // 1 miss
    let _b3 = pool.get();

    let rate = pool.metrics().hit_rate();
    assert!((rate - 0.666).abs() < 0.01);
}

#[test]
fn test_metrics_snapshot() {
    let pool = BufferPool::new(3, 1024);

    let b1 = pool.get(); // hit
    let _b2 = pool.get(); // hit
    let _b3 = pool.get(); // hit
    let _b4 = pool.get(); // miss

    pool.put(b1); // return

    let snapshot = pool.metrics().snapshot();
    assert_eq!(snapshot.hits, 3);
    assert_eq!(snapshot.misses, 1);
    assert_eq!(snapshot.returns, 1);
    assert_eq!(snapshot.drops, 0);
}

#[test]
fn test_concurrent_access() {
    let pool = Arc::new(BufferPool::new(100, 1024));
    let mut handles = vec![];

    // Spawn 10 threads, each doing 100 get/put cycles
    for _ in 0..10 {
        let pool = Arc::clone(&pool);
        handles.push(thread::spawn(move || {
            for _ in 0..100 {
                let buf = pool.get();
                // Simulate some work
                std::hint::black_box(&buf);
                pool.put(buf);
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // All buffers should be returned
    // (may be less than 100 due to timing, but should be close)
    assert!(pool.available() >= 90);

    let snapshot = pool.metrics().snapshot();
    // Total operations = 10 threads * 100 iterations
    assert_eq!(snapshot.hits + snapshot.misses, 1000);
}

#[test]
fn test_empty_pool_metrics() {
    let metrics = BufferPoolMetrics::new();

    // No operations yet - hit rate should be 1.0 (not NaN or panic)
    assert_eq!(metrics.hit_rate(), 1.0);

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.hit_rate(), 1.0);
}

#[test]
fn test_snapshot_hit_rate_calculation() {
    let pool = BufferPool::new(1, 1024);

    // 1 hit
    let _b1 = pool.get();

    // 1 miss
    let _b2 = pool.get();

    let snapshot = pool.metrics().snapshot();
    assert_eq!(snapshot.hit_rate(), 0.5);
}

#[test]
fn test_multiple_get_put_cycles() {
    let pool = BufferPool::new(3, 1024);

    for _ in 0..100 {
        let buf = pool.get();
        pool.put(buf);
    }

    // Pool should still be healthy
    assert_eq!(pool.available(), 3);

    let snapshot = pool.metrics().snapshot();
    assert_eq!(snapshot.hits, 100);
    assert_eq!(snapshot.returns, 100);
    assert_eq!(snapshot.misses, 0);
    assert_eq!(snapshot.drops, 0);
}
