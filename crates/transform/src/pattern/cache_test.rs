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

    cache.put("my-service", "test message", 42);

    let result = cache.get("my-service", "test message");
    assert_eq!(result, Some(42));
}

#[test]
fn test_cache_miss() {
    let cache = PatternCache::new(100, 100);

    let result = cache.get("my-service", "nonexistent message");
    assert_eq!(result, None);
}

#[test]
fn test_l1_hit() {
    let cache = PatternCache::new(100, 100);

    cache.put("svc", "exact message", 1);
    let _ = cache.get("svc", "exact message");

    assert_eq!(cache.stats().l1_hits.load(Ordering::Relaxed), 1);
    assert_eq!(cache.stats().l1_misses.load(Ordering::Relaxed), 0);
}

#[test]
fn test_l2_hit_from_similar_message() {
    let cache = PatternCache::new(100, 100);

    // Store with one number
    cache.put("auth", "User 12345 logged in", 1);

    // Clear L1 to force L2 lookup
    cache.l1_cache.lock().clear();

    // Lookup with different number (should hit L2 due to template normalization)
    let result = cache.get("auth", "User 67890 logged in");

    assert_eq!(result, Some(1));
    assert!(cache.stats().l2_hits.load(Ordering::Relaxed) >= 1);
}

#[test]
fn test_service_isolation() {
    let cache = PatternCache::new(100, 100);

    // Same message, different services
    cache.put("service-a", "User logged in", 1);
    cache.put("service-b", "User logged in", 2);

    // Each service should get its own pattern
    assert_eq!(cache.get("service-a", "User logged in"), Some(1));
    assert_eq!(cache.get("service-b", "User logged in"), Some(2));
}

#[test]
fn test_stats_tracking() {
    let cache = PatternCache::new(100, 100);

    // Miss
    cache.get("svc", "message 1");
    assert_eq!(cache.stats().total_lookups.load(Ordering::Relaxed), 1);
    assert_eq!(cache.stats().l1_misses.load(Ordering::Relaxed), 1);

    // Insert and hit
    cache.put("svc", "message 2", 42);
    cache.get("svc", "message 2");
    assert_eq!(cache.stats().total_lookups.load(Ordering::Relaxed), 2);
    assert_eq!(cache.stats().l1_hits.load(Ordering::Relaxed), 1);
}

#[test]
fn test_hit_rate_calculation() {
    let cache = PatternCache::new(100, 100);

    // 0 lookups = 0% hit rate
    assert_eq!(cache.stats().l1_hit_rate(), 0.0);

    // Add some data and lookups
    cache.put("svc", "message", 1);
    cache.get("svc", "message"); // hit
    cache.get("svc", "other"); // miss

    let hit_rate = cache.stats().l1_hit_rate();
    assert!(hit_rate > 0.0 && hit_rate < 1.0);
}

#[test]
fn test_combined_hit_rate() {
    let cache = PatternCache::new(100, 100);

    cache.put("svc", "User 123 action", 1);

    // L1 hit
    cache.get("svc", "User 123 action");

    // Clear L1, get L2 hit
    cache.l1_cache.lock().clear();
    cache.get("svc", "User 456 action");

    // L1 miss + L2 miss
    cache.get("svc", "completely different");

    let combined = cache.stats().combined_hit_rate();
    assert!(combined > 0.0);
}

#[test]
fn test_clear() {
    let cache = PatternCache::new(100, 100);

    cache.put("svc", "message 1", 1);
    cache.put("svc", "message 2", 2);
    cache.get("svc", "message 1");

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
    cache.put_l1("svc", "msg1", 1);
    cache.put_l1("svc", "msg2", 2);
    cache.put_l1("svc", "msg3", 3);

    assert_eq!(cache.l1_size(), 3);

    // Access msg1 to make it recent
    cache.get("svc", "msg1");

    // Add new entry - should evict msg2 (oldest)
    cache.put_l1("svc", "msg4", 4);

    assert_eq!(cache.l1_size(), 3);
    assert_eq!(cache.get("svc", "msg1"), Some(1)); // Still there
    assert_eq!(cache.get("svc", "msg4"), Some(4)); // New entry
}

#[test]
fn test_put_l1_only() {
    let cache = PatternCache::new(100, 100);

    cache.put_l1("svc", "message", 42);

    // Should be in L1
    assert_eq!(cache.l1_size(), 1);
    // L2 not affected by put_l1
}

#[test]
fn test_put_l2_only() {
    let cache = PatternCache::new(100, 100);

    let template_hash = hash_template("svc", "User 123 logged in");
    cache.put_l2(template_hash, 42);

    assert_eq!(cache.l2_size(), 1);
    assert_eq!(cache.l1_size(), 0);
}

#[test]
fn test_hash_message_consistency() {
    let hash1 = hash_message("svc", "test message");
    let hash2 = hash_message("svc", "test message");

    assert_eq!(hash1, hash2);
}

#[test]
fn test_hash_message_different() {
    let hash1 = hash_message("svc", "message 1");
    let hash2 = hash_message("svc", "message 2");

    assert_ne!(hash1, hash2);
}

#[test]
fn test_hash_message_service_matters() {
    let hash1 = hash_message("service-a", "same message");
    let hash2 = hash_message("service-b", "same message");

    assert_ne!(hash1, hash2, "Same message with different service should have different hash");
}

#[test]
fn test_hash_template_normalizes_numbers() {
    let hash1 = hash_template("svc", "User 12345 logged in");
    let hash2 = hash_template("svc", "User 67890 logged in");

    assert_eq!(hash1, hash2, "Numbers should be normalized to same template");
}

#[test]
fn test_hash_template_normalizes_ids() {
    let hash1 = hash_template("svc", "Request abc123def456 completed");
    let hash2 = hash_template("svc", "Request xyz789ghi012 completed");

    assert_eq!(hash1, hash2, "Long alphanumeric IDs should normalize");
}

#[test]
fn test_hash_template_service_matters() {
    let hash1 = hash_template("service-a", "User 123 logged in");
    let hash2 = hash_template("service-b", "User 123 logged in");

    assert_ne!(hash1, hash2, "Same template with different service should have different hash");
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
                cache.put("svc", &format!("message {} {}", i, j), (i * 100 + j) as u64);
            }
        }));
    }

    // Readers
    for i in 0..5 {
        let cache = Arc::clone(&cache);
        handles.push(thread::spawn(move || {
            for j in 0..100 {
                let _ = cache.get("svc", &format!("message {} {}", i, j));
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
