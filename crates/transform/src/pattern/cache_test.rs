//! Tests for pattern cache

use super::*;

#[test]
fn test_cache_creation() {
    let cache = PatternCache::new(1000, 100);

    assert_eq!(cache.l1_capacity(), 1000);
    assert_eq!(cache.l2_capacity(), 100);
    assert_eq!(cache.l1_size(), 0);
    assert_eq!(cache.l2_size(), 0);
}

#[test]
fn test_default_cache() {
    let cache = PatternCache::default();

    assert_eq!(cache.l1_capacity(), 100_000);
    assert_eq!(cache.l2_capacity(), 10_000);
}

#[test]
fn test_put_and_get() {
    let cache = PatternCache::new(100, 100);

    cache.put("test message", 42);

    let result = cache.get("test message");
    assert_eq!(result, Some(42));
}

#[test]
fn test_cache_miss() {
    let cache = PatternCache::new(100, 100);

    let result = cache.get("nonexistent message");
    assert_eq!(result, None);
}

#[test]
fn test_l1_hit() {
    let cache = PatternCache::new(100, 100);

    cache.put("exact message", 1);
    let _ = cache.get("exact message");

    assert_eq!(cache.stats().l1_hits.load(Ordering::Relaxed), 1);
    assert_eq!(cache.stats().l1_misses.load(Ordering::Relaxed), 0);
}

#[test]
fn test_l2_hit_from_similar_message() {
    let cache = PatternCache::new(100, 100);

    // Store with one number
    cache.put("User 12345 logged in", 1);

    // Clear L1 to force L2 lookup
    cache.l1_cache.lock().clear();

    // Lookup with different number (should hit L2 due to template normalization)
    let result = cache.get("User 67890 logged in");

    assert_eq!(result, Some(1));
    assert!(cache.stats().l2_hits.load(Ordering::Relaxed) >= 1);
}

#[test]
fn test_stats_tracking() {
    let cache = PatternCache::new(100, 100);

    // Miss
    cache.get("message 1");
    assert_eq!(cache.stats().total_lookups.load(Ordering::Relaxed), 1);
    assert_eq!(cache.stats().l1_misses.load(Ordering::Relaxed), 1);

    // Insert and hit
    cache.put("message 2", 42);
    cache.get("message 2");
    assert_eq!(cache.stats().total_lookups.load(Ordering::Relaxed), 2);
    assert_eq!(cache.stats().l1_hits.load(Ordering::Relaxed), 1);
}

#[test]
fn test_hit_rate_calculation() {
    let cache = PatternCache::new(100, 100);

    // 0 lookups = 0% hit rate
    assert_eq!(cache.stats().l1_hit_rate(), 0.0);

    // Add some data and lookups
    cache.put("message", 1);
    cache.get("message"); // hit
    cache.get("other"); // miss

    let hit_rate = cache.stats().l1_hit_rate();
    assert!(hit_rate > 0.0 && hit_rate < 1.0);
}

#[test]
fn test_combined_hit_rate() {
    let cache = PatternCache::new(100, 100);

    cache.put("User 123 action", 1);

    // L1 hit
    cache.get("User 123 action");

    // Clear L1, get L2 hit
    cache.l1_cache.lock().clear();
    cache.get("User 456 action");

    // L1 miss + L2 miss
    cache.get("completely different");

    let combined = cache.stats().combined_hit_rate();
    assert!(combined > 0.0);
}

#[test]
fn test_clear() {
    let cache = PatternCache::new(100, 100);

    cache.put("message 1", 1);
    cache.put("message 2", 2);
    cache.get("message 1");

    assert!(cache.l1_size() > 0);
    assert!(cache.stats().total_lookups.load(Ordering::Relaxed) > 0);

    cache.clear();

    assert_eq!(cache.l1_size(), 0);
    assert_eq!(cache.l2_size(), 0);
    assert_eq!(cache.stats().total_lookups.load(Ordering::Relaxed), 0);
}

#[test]
fn test_lru_eviction() {
    let cache = PatternCache::new(3, 3);

    // Fill cache
    cache.put_l1("msg1", 1);
    cache.put_l1("msg2", 2);
    cache.put_l1("msg3", 3);

    assert_eq!(cache.l1_size(), 3);

    // Access msg1 to make it recent
    cache.get("msg1");

    // Add new entry - should evict msg2 (oldest)
    cache.put_l1("msg4", 4);

    assert_eq!(cache.l1_size(), 3);
    assert_eq!(cache.get("msg1"), Some(1)); // Still there
    assert_eq!(cache.get("msg4"), Some(4)); // New entry
}

#[test]
fn test_put_l1_only() {
    let cache = PatternCache::new(100, 100);

    cache.put_l1("message", 42);

    // Should be in L1
    assert_eq!(cache.l1_size(), 1);
    // L2 not affected by put_l1
}

#[test]
fn test_put_l2_only() {
    let cache = PatternCache::new(100, 100);

    let template_hash = hash_template("User 123 logged in");
    cache.put_l2(template_hash, 42);

    assert_eq!(cache.l2_size(), 1);
    assert_eq!(cache.l1_size(), 0);
}

#[test]
fn test_hash_message_consistency() {
    let msg = "test message";

    let hash1 = hash_message(msg);
    let hash2 = hash_message(msg);

    assert_eq!(hash1, hash2);
}

#[test]
fn test_hash_message_different() {
    let hash1 = hash_message("message 1");
    let hash2 = hash_message("message 2");

    assert_ne!(hash1, hash2);
}

#[test]
fn test_hash_template_normalizes_numbers() {
    let hash1 = hash_template("User 12345 logged in");
    let hash2 = hash_template("User 67890 logged in");

    assert_eq!(hash1, hash2, "Numbers should be normalized to same template");
}

#[test]
fn test_hash_template_normalizes_ids() {
    let hash1 = hash_template("Request abc123def456 completed");
    let hash2 = hash_template("Request xyz789ghi012 completed");

    assert_eq!(hash1, hash2, "Long alphanumeric IDs should normalize");
}

#[test]
fn test_is_likely_variable() {
    assert!(is_likely_variable("12345"));
    assert!(is_likely_variable("0x1234"));
    assert!(is_likely_variable("abcdef123456")); // Long alphanumeric
    assert!(is_likely_variable("/var/log"));
    assert!(is_likely_variable("user@example.com"));

    assert!(!is_likely_variable("User"));
    assert!(!is_likely_variable("logged"));
    assert!(!is_likely_variable("ERROR"));
}

#[test]
fn test_concurrent_access() {
    use std::sync::Arc;
    use std::thread;

    let cache = Arc::new(PatternCache::new(1000, 1000));
    let mut handles = vec![];

    // Writers
    for i in 0..5 {
        let cache = Arc::clone(&cache);
        handles.push(thread::spawn(move || {
            for j in 0..100 {
                cache.put(&format!("message {} {}", i, j), (i * 100 + j) as u64);
            }
        }));
    }

    // Readers
    for i in 0..5 {
        let cache = Arc::clone(&cache);
        handles.push(thread::spawn(move || {
            for j in 0..100 {
                let _ = cache.get(&format!("message {} {}", i, j));
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Should complete without deadlock or panic
    assert!(cache.l1_size() > 0);
}

#[test]
fn test_stats_reset() {
    let stats = CacheStats::default();

    stats.l1_hits.store(100, Ordering::Relaxed);
    stats.total_lookups.store(200, Ordering::Relaxed);

    stats.reset();

    assert_eq!(stats.l1_hits.load(Ordering::Relaxed), 0);
    assert_eq!(stats.total_lookups.load(Ordering::Relaxed), 0);
}
