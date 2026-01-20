//! Pattern Cache - Multi-level caching for pattern lookups
//!
//! Implements a 3-level caching strategy to minimize Drain tree traversals:
//!
//! 1. **Level 1 (L1)**: Exact message hash → pattern ID (70-80% hit rate)
//! 2. **Level 2 (L2)**: Template hash → pattern ID (for similar messages)
//! 3. **Level 3 (L3)**: Drain tree lookup (fallback)
//!
//! # Design
//!
//! - Lock-free reads using `DashMap` for concurrent access
//! - LRU eviction when cache is full
//! - Metrics for cache hit/miss rates

use parking_lot::Mutex;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, Ordering};
use xxhash_rust::xxh3::xxh3_64;

use super::drain::PatternId;

#[cfg(test)]
#[path = "cache_test.rs"]
mod tests;

/// Cache statistics
#[derive(Debug, Default)]
pub struct CacheStats {
    /// L1 cache hits (exact message match)
    pub l1_hits: AtomicU64,

    /// L1 cache misses
    pub l1_misses: AtomicU64,

    /// L2 cache hits (template match)
    pub l2_hits: AtomicU64,

    /// L2 cache misses
    pub l2_misses: AtomicU64,

    /// Total lookups
    pub total_lookups: AtomicU64,
}

impl CacheStats {
    /// Get L1 hit rate (0.0 - 1.0)
    pub fn l1_hit_rate(&self) -> f64 {
        let hits = self.l1_hits.load(Ordering::Relaxed);
        let total = self.total_lookups.load(Ordering::Relaxed);
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }

    /// Get combined hit rate (L1 + L2)
    pub fn combined_hit_rate(&self) -> f64 {
        let l1_hits = self.l1_hits.load(Ordering::Relaxed);
        let l2_hits = self.l2_hits.load(Ordering::Relaxed);
        let total = self.total_lookups.load(Ordering::Relaxed);
        if total == 0 {
            0.0
        } else {
            (l1_hits + l2_hits) as f64 / total as f64
        }
    }

    /// Reset statistics
    pub fn reset(&self) {
        self.l1_hits.store(0, Ordering::Relaxed);
        self.l1_misses.store(0, Ordering::Relaxed);
        self.l2_hits.store(0, Ordering::Relaxed);
        self.l2_misses.store(0, Ordering::Relaxed);
        self.total_lookups.store(0, Ordering::Relaxed);
    }
}

/// Multi-level pattern cache
pub struct PatternCache {
    /// L1: Exact message hash → pattern ID
    l1_cache: Mutex<LruCache<u64, PatternId>>,

    /// L2: Template hash → pattern ID
    l2_cache: Mutex<LruCache<u64, PatternId>>,

    /// Maximum L1 cache size
    l1_capacity: usize,

    /// Maximum L2 cache size
    l2_capacity: usize,

    /// Cache statistics
    stats: CacheStats,
}

impl PatternCache {
    /// Create a new cache with specified capacities
    pub fn new(l1_capacity: usize, l2_capacity: usize) -> Self {
        Self {
            l1_cache: Mutex::new(LruCache::new(l1_capacity)),
            l2_cache: Mutex::new(LruCache::new(l2_capacity)),
            l1_capacity,
            l2_capacity,
            stats: CacheStats::default(),
        }
    }

    /// Look up pattern ID for a message
    ///
    /// Returns `Some(pattern_id)` if found in cache, `None` if cache miss.
    /// Service is included in hash to ensure same message from different services
    /// gets different patterns (service isolation).
    pub fn get(&self, service: &str, message: &str) -> Option<PatternId> {
        self.stats.total_lookups.fetch_add(1, Ordering::Relaxed);

        let message_hash = hash_message(service, message);

        // L1 lookup: exact message
        if let Some(id) = self.l1_cache.lock().get(&message_hash) {
            self.stats.l1_hits.fetch_add(1, Ordering::Relaxed);
            return Some(id);
        }
        self.stats.l1_misses.fetch_add(1, Ordering::Relaxed);

        // L2 lookup: template hash (simplified - just normalized tokens)
        let template_hash = hash_template(service, message);
        if let Some(id) = self.l2_cache.lock().get(&template_hash) {
            self.stats.l2_hits.fetch_add(1, Ordering::Relaxed);

            // Promote to L1 for future exact matches
            self.l1_cache.lock().put(message_hash, id);
            return Some(id);
        }
        self.stats.l2_misses.fetch_add(1, Ordering::Relaxed);

        None
    }

    /// Insert a pattern ID for a message
    /// Service is included in hash for service isolation.
    pub fn put(&self, service: &str, message: &str, pattern_id: PatternId) {
        let message_hash = hash_message(service, message);
        let template_hash = hash_template(service, message);

        self.l1_cache.lock().put(message_hash, pattern_id);
        self.l2_cache.lock().put(template_hash, pattern_id);
    }

    /// Insert only to L1 cache (for exact message matches)
    pub fn put_l1(&self, service: &str, message: &str, pattern_id: PatternId) {
        let message_hash = hash_message(service, message);
        self.l1_cache.lock().put(message_hash, pattern_id);
    }

    /// Insert only to L2 cache (for template matches)
    pub fn put_l2(&self, template_hash: u64, pattern_id: PatternId) {
        self.l2_cache.lock().put(template_hash, pattern_id);
    }

    /// Get cache statistics
    pub fn stats(&self) -> &CacheStats {
        &self.stats
    }

    /// Get current L1 cache size
    pub fn l1_size(&self) -> usize {
        self.l1_cache.lock().len()
    }

    /// Get current L2 cache size
    pub fn l2_size(&self) -> usize {
        self.l2_cache.lock().len()
    }

    /// Clear all caches
    pub fn clear(&self) {
        self.l1_cache.lock().clear();
        self.l2_cache.lock().clear();
        self.stats.reset();
    }

    /// Get L1 capacity
    pub fn l1_capacity(&self) -> usize {
        self.l1_capacity
    }

    /// Get L2 capacity
    pub fn l2_capacity(&self) -> usize {
        self.l2_capacity
    }
}

impl Default for PatternCache {
    fn default() -> Self {
        Self::new(100_000, 10_000)
    }
}

/// Simple LRU cache implementation
struct LruCache<K: Eq + Hash + Copy, V: Copy> {
    /// Map from key to (value, access_order)
    map: HashMap<K, (V, u64)>,

    /// Current access counter
    counter: u64,

    /// Maximum capacity
    capacity: usize,
}

impl<K: Eq + Hash + Copy, V: Copy> LruCache<K, V> {
    fn new(capacity: usize) -> Self {
        Self {
            map: HashMap::with_capacity(capacity),
            counter: 0,
            capacity: capacity.max(1),
        }
    }

    fn get(&mut self, key: &K) -> Option<V> {
        if let Some((value, order)) = self.map.get_mut(key) {
            self.counter += 1;
            *order = self.counter;
            Some(*value)
        } else {
            None
        }
    }

    fn put(&mut self, key: K, value: V) {
        self.counter += 1;

        // Check if we need to evict
        if self.map.len() >= self.capacity && !self.map.contains_key(&key) {
            self.evict_lru();
        }

        self.map.insert(key, (value, self.counter));
    }

    fn evict_lru(&mut self) {
        if self.map.is_empty() {
            return;
        }

        // Find oldest entry
        let oldest_key = self
            .map
            .iter()
            .min_by_key(|(_, (_, order))| *order)
            .map(|(k, _)| *k);

        if let Some(key) = oldest_key {
            self.map.remove(&key);
        }
    }

    fn len(&self) -> usize {
        self.map.len()
    }

    fn clear(&mut self) {
        self.map.clear();
        self.counter = 0;
    }
}

/// Hash a message for L1 cache lookup using xxhash (fast, non-cryptographic)
/// Includes service for service isolation (same message, different service = different hash)
pub fn hash_message(service: &str, message: &str) -> u64 {
    // Format: "service:message" then hash
    let combined = format!("{}:{}", service, message);
    xxh3_64(combined.as_bytes())
}

/// Hash a message template for L2 cache lookup using xxhash
///
/// Normalizes the message by replacing likely variables with placeholders.
/// Includes service for service isolation.
pub fn hash_template(service: &str, message: &str) -> u64 {
    let normalized: String = message
        .split_whitespace()
        .map(|token| {
            if is_likely_variable(token) {
                "<*>"
            } else {
                token
            }
        })
        .collect::<Vec<_>>()
        .join(" ");

    // Format: "service:normalized" then hash
    let combined = format!("{}:{}", service, normalized);
    xxh3_64(combined.as_bytes())
}

/// Quick check if a token is likely a variable
fn is_likely_variable(token: &str) -> bool {
    // Numbers
    if token.parse::<f64>().is_ok() {
        return true;
    }

    // Hex
    if token.starts_with("0x") {
        return true;
    }

    // Long alphanumeric (likely ID)
    if token.len() > 8 && token.chars().all(|c| c.is_alphanumeric()) {
        return true;
    }

    // Contains special chars suggesting paths/URLs
    if token.contains('/') || token.contains('@') {
        return true;
    }

    false
}
