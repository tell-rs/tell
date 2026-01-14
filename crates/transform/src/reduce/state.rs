//! Reduce state management
//!
//! Handles grouping of events and flush conditions.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::time::Instant;

use siphasher::sip::SipHasher13;

#[cfg(test)]
#[path = "state_test.rs"]
mod tests;

/// A single reduce group accumulating events
#[derive(Debug)]
pub struct ReduceGroup {
    /// The original first message (preserved for output)
    pub first_message: Vec<u8>,

    /// Hash of the group key (for verification)
    pub key_hash: u64,

    /// Timestamp when group was created
    pub created_at: Instant,

    /// Timestamp of most recent event
    pub last_event_at: Instant,

    /// Number of events in this group
    pub count: u64,
}

impl ReduceGroup {
    /// Create a new group from the first message
    pub fn new(message: &[u8], key_hash: u64) -> Self {
        let now = Instant::now();
        Self {
            first_message: message.to_vec(),
            key_hash,
            created_at: now,
            last_event_at: now,
            count: 1,
        }
    }

    /// Add another event to this group
    pub fn add_event(&mut self) {
        self.count += 1;
        self.last_event_at = Instant::now();
    }

    /// Check if group should flush based on max events
    #[inline]
    pub fn should_flush_count(&self, max_events: usize) -> bool {
        self.count as usize >= max_events
    }

    /// Check if group should flush based on time window
    #[inline]
    pub fn should_flush_time(&self, window: std::time::Duration) -> bool {
        self.created_at.elapsed() >= window
    }

    /// Get duration since group was created
    #[inline]
    pub fn age(&self) -> std::time::Duration {
        self.created_at.elapsed()
    }
}

/// State container for all active reduce groups
#[derive(Debug, Default)]
pub struct ReduceState {
    /// Active groups keyed by group hash
    groups: HashMap<u64, ReduceGroup>,
}

impl ReduceState {
    /// Create new empty state
    pub fn new() -> Self {
        Self {
            groups: HashMap::new(),
        }
    }

    /// Get or create a group for the given key hash
    ///
    /// Returns (group, is_new) where is_new indicates if this is a new group
    pub fn get_or_create(&mut self, key_hash: u64, message: &[u8]) -> (&mut ReduceGroup, bool) {
        use std::collections::hash_map::Entry;

        match self.groups.entry(key_hash) {
            Entry::Occupied(entry) => {
                let group = entry.into_mut();
                group.add_event();
                (group, false)
            }
            Entry::Vacant(entry) => {
                let group = entry.insert(ReduceGroup::new(message, key_hash));
                (group, true)
            }
        }
    }

    /// Remove and return a group by key hash
    pub fn remove(&mut self, key_hash: u64) -> Option<ReduceGroup> {
        self.groups.remove(&key_hash)
    }

    /// Get all groups that should be flushed
    ///
    /// Returns key hashes of groups that exceed time window or max events.
    pub fn groups_to_flush(
        &self,
        window: std::time::Duration,
        max_events: usize,
    ) -> Vec<u64> {
        self.groups
            .iter()
            .filter(|(_, group)| {
                group.should_flush_time(window) || group.should_flush_count(max_events)
            })
            .map(|(&hash, _)| hash)
            .collect()
    }

    /// Flush all groups, returning them
    pub fn flush_all(&mut self) -> Vec<ReduceGroup> {
        self.groups.drain().map(|(_, group)| group).collect()
    }

    /// Get number of active groups
    #[inline]
    pub fn group_count(&self) -> usize {
        self.groups.len()
    }

    /// Check if state is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.groups.is_empty()
    }
}

/// Compute group key hash from message and group_by fields
///
/// If group_by is empty, hashes the entire message.
/// Otherwise, extracts field values and hashes them.
pub fn compute_group_key(message: &[u8], group_by: &[String]) -> u64 {
    let mut hasher = SipHasher13::new();

    if group_by.is_empty() {
        // Hash entire message
        message.hash(&mut hasher);
    } else {
        // Try to parse as JSON and extract fields
        if let Ok(json) = serde_json::from_slice::<serde_json::Value>(message) {
            for field in group_by {
                if let Some(value) = get_json_field(&json, field) {
                    // Hash field name + value for uniqueness
                    field.hash(&mut hasher);
                    hash_json_value(value, &mut hasher);
                }
            }
        } else {
            // Fallback: hash entire message for non-JSON
            message.hash(&mut hasher);
        }
    }

    hasher.finish()
}

/// Get a field from JSON using dot notation
///
/// Supports paths like "error.code" or "user.profile.name"
fn get_json_field<'a>(json: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
    let mut current = json;

    for part in path.split('.') {
        match current {
            serde_json::Value::Object(map) => {
                current = map.get(part)?;
            }
            _ => return None,
        }
    }

    Some(current)
}

/// Hash a JSON value
fn hash_json_value<H: Hasher>(value: &serde_json::Value, hasher: &mut H) {
    match value {
        serde_json::Value::Null => 0u8.hash(hasher),
        serde_json::Value::Bool(b) => {
            1u8.hash(hasher);
            b.hash(hasher);
        }
        serde_json::Value::Number(n) => {
            2u8.hash(hasher);
            // Hash as string to handle both int and float
            n.to_string().hash(hasher);
        }
        serde_json::Value::String(s) => {
            3u8.hash(hasher);
            s.hash(hasher);
        }
        serde_json::Value::Array(arr) => {
            4u8.hash(hasher);
            for item in arr {
                hash_json_value(item, hasher);
            }
        }
        serde_json::Value::Object(map) => {
            5u8.hash(hasher);
            for (k, v) in map {
                k.hash(hasher);
                hash_json_value(v, hasher);
            }
        }
    }
}
