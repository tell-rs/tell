//! API key store for zero-allocation validation
//!
//! The `ApiKeyStore` holds a mapping from API keys to workspace IDs.
//! Designed for O(1) lookup with no allocations in the hot path.
//!
//! # Security
//!
//! API key validation uses constant-time comparison to prevent timing attacks.

use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

use parking_lot::RwLock;
use rand::RngCore;
use subtle::ConstantTimeEq;

use crate::error::{AuthError, Result};
use crate::workspace::WorkspaceId;
use crate::{API_KEY_HEX_LENGTH, API_KEY_LENGTH};

/// API key type (16 bytes)
pub type ApiKey = [u8; API_KEY_LENGTH];

/// Thread-safe API key store
///
/// Provides O(1) key validation with no allocations in the hot path.
/// Supports atomic reload for hot configuration updates.
///
/// # Example
///
/// ```
/// use tell_auth::{ApiKeyStore, WorkspaceId};
///
/// let mut store = ApiKeyStore::new();
///
/// // Add a key
/// let key = [0u8; 16];
/// store.insert(key, WorkspaceId::new(1));
///
/// // Validate (hot path - zero allocation, zero-cost copy)
/// assert!(store.validate(&key).is_some());
/// ```
#[derive(Debug)]
pub struct ApiKeyStore {
    /// Inner store protected by RwLock for concurrent access
    inner: RwLock<StoreInner>,
}

#[derive(Debug, Default)]
struct StoreInner {
    /// Map from API key to (workspace ID, optional name)
    keys: HashMap<ApiKey, (WorkspaceId, Option<String>)>,
}

impl Default for ApiKeyStore {
    fn default() -> Self {
        Self::new()
    }
}

impl ApiKeyStore {
    /// Create a new empty store
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(StoreInner::default()),
        }
    }

    /// Create a store with pre-allocated capacity
    #[inline]
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: RwLock::new(StoreInner {
                keys: HashMap::with_capacity(capacity),
            }),
        }
    }

    /// Load API keys from a file
    ///
    /// File format:
    /// ```text
    /// # comments start with #
    /// 000102030405060708090a0b0c0d0e0f:workspace_id
    /// 000102030405060708090a0b0c0d0e0f:workspace_id:name
    /// ```
    ///
    /// The name field is optional for backward compatibility.
    ///
    /// # Errors
    ///
    /// Returns error if file cannot be read or contains invalid entries.
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        let contents = fs::read_to_string(path)
            .map_err(|e| AuthError::io_error(path.display().to_string(), e))?;

        Self::from_str(&contents)
    }

    /// Parse API keys from a string
    ///
    /// Prefer using the `FromStr` trait implementation.
    fn parse(contents: &str) -> Result<Self> {
        let store = Self::new();

        {
            let mut inner = store.inner.write();

            for (line_num, line) in contents.lines().enumerate() {
                let line_num = line_num + 1; // 1-based line numbers
                let line = line.trim();

                // Skip empty lines and comments
                if line.is_empty() || line.starts_with('#') {
                    continue;
                }

                // Parse line
                let (key, workspace, name) = parse_line(line, line_num)?;

                // Check for duplicates
                if inner.keys.contains_key(&key) {
                    return Err(AuthError::duplicate_key(line_num));
                }

                inner.keys.insert(key, (workspace, name));
            }
        }

        Ok(store)
    }
}

impl FromStr for ApiKeyStore {
    type Err = AuthError;

    fn from_str(s: &str) -> Result<Self> {
        Self::parse(s)
    }
}

impl ApiKeyStore {
    /// Validate an API key and return the associated workspace ID
    ///
    /// Uses constant-time comparison to prevent timing attacks.
    /// Returns `None` if the key is not found.
    ///
    /// # Security
    ///
    /// This function iterates all keys using constant-time comparison,
    /// ensuring an attacker cannot determine key validity through timing.
    #[inline]
    pub fn validate(&self, key: &ApiKey) -> Option<WorkspaceId> {
        let inner = self.inner.read();

        // Iterate all keys with constant-time comparison
        // This prevents timing attacks at the cost of O(n) instead of O(1)
        let mut result: Option<WorkspaceId> = None;

        for (stored_key, (workspace_id, _name)) in inner.keys.iter() {
            // Constant-time comparison: always compare all bytes
            if bool::from(key.ct_eq(stored_key)) {
                result = Some(*workspace_id);
                // Don't break early - continue iterating to maintain constant time
            }
        }

        result
    }

    /// Validate an API key slice (must be exactly 16 bytes)
    ///
    /// Returns `None` if the key is invalid length or not found.
    /// Uses constant-time comparison.
    #[inline]
    pub fn validate_slice(&self, key: &[u8]) -> Option<WorkspaceId> {
        if key.len() != API_KEY_LENGTH {
            return None;
        }

        let key_array: &ApiKey = key.try_into().ok()?;
        self.validate(key_array)
    }

    /// Fast validation using HashMap (NOT constant-time)
    ///
    /// Use this only when timing attacks are not a concern
    /// (e.g., internal services, rate-limited endpoints).
    #[inline]
    pub fn validate_fast(&self, key: &ApiKey) -> Option<WorkspaceId> {
        self.inner.read().keys.get(key).map(|(ws, _)| *ws)
    }

    /// Insert an API key
    ///
    /// Returns the previous workspace ID if the key already existed.
    pub fn insert(&self, key: ApiKey, workspace: WorkspaceId) -> Option<WorkspaceId> {
        self.insert_named(key, workspace, None)
    }

    /// Insert an API key with a name
    ///
    /// Returns the previous workspace ID if the key already existed.
    pub fn insert_named(
        &self,
        key: ApiKey,
        workspace: WorkspaceId,
        name: Option<String>,
    ) -> Option<WorkspaceId> {
        self.inner
            .write()
            .keys
            .insert(key, (workspace, name))
            .map(|(ws, _)| ws)
    }

    /// Remove an API key
    ///
    /// Returns the workspace ID if the key existed.
    pub fn remove(&self, key: &ApiKey) -> Option<WorkspaceId> {
        self.inner.write().keys.remove(key).map(|(ws, _)| ws)
    }

    /// Check if a key exists
    #[inline]
    pub fn contains(&self, key: &ApiKey) -> bool {
        self.inner.read().keys.contains_key(key)
    }

    /// Get the number of keys in the store
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.read().keys.len()
    }

    /// Check if the store is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.read().keys.is_empty()
    }

    /// Reload keys from a file, atomically replacing all existing keys
    ///
    /// # Errors
    ///
    /// Returns error if file cannot be read or contains invalid entries.
    /// On error, the existing keys are preserved.
    pub fn reload<P: AsRef<Path>>(&self, path: P) -> Result<usize> {
        let path = path.as_ref();
        let contents = fs::read_to_string(path)
            .map_err(|e| AuthError::io_error(path.display().to_string(), e))?;

        self.reload_from_str(&contents)
    }

    /// Reload keys from a string, atomically replacing all existing keys
    ///
    /// # Errors
    ///
    /// Returns error if string contains invalid entries.
    /// On error, the existing keys are preserved.
    pub fn reload_from_str(&self, contents: &str) -> Result<usize> {
        // Parse into a new map first (so we don't modify on error)
        let mut new_keys = HashMap::new();

        for (line_num, line) in contents.lines().enumerate() {
            let line_num = line_num + 1;
            let line = line.trim();

            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            let (key, workspace, name) = parse_line(line, line_num)?;

            if new_keys.contains_key(&key) {
                return Err(AuthError::duplicate_key(line_num));
            }

            new_keys.insert(key, (workspace, name));
        }

        // Atomic swap
        let count = new_keys.len();
        self.inner.write().keys = new_keys;

        Ok(count)
    }

    /// Save all keys to a file
    ///
    /// Format: `hex_key:workspace_id:name` (name omitted if None)
    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> std::io::Result<()> {
        let path = path.as_ref();
        let mut contents = String::from("# API keys format: hex_key:workspace_id:name\n");

        let inner = self.inner.read();
        for (key, (workspace, name)) in inner.keys.iter() {
            let hex: String = key.iter().map(|b| format!("{:02x}", b)).collect();
            if let Some(name) = name {
                contents.push_str(&format!("{}:{}:{}\n", hex, workspace.as_u32(), name));
            } else {
                contents.push_str(&format!("{}:{}\n", hex, workspace.as_u32()));
            }
        }

        fs::write(path, contents)
    }

    /// Clear all keys
    pub fn clear(&self) {
        self.inner.write().keys.clear();
    }

    /// Get all workspace IDs (for debugging/metrics)
    pub fn workspaces(&self) -> Vec<WorkspaceId> {
        self.inner.read().keys.values().map(|(ws, _)| *ws).collect()
    }

    /// List all API keys with their names (for management UI)
    ///
    /// Returns tuples of (hex_key, workspace_id, optional_name)
    pub fn list(&self) -> Vec<(String, WorkspaceId, Option<String>)> {
        self.inner
            .read()
            .keys
            .iter()
            .map(|(key, (ws, name))| {
                let hex = key.iter().map(|b| format!("{:02x}", b)).collect();
                (hex, *ws, name.clone())
            })
            .collect()
    }

    /// Find a key by name
    ///
    /// Returns the hex key string if found.
    pub fn find_by_name(&self, name: &str) -> Option<String> {
        self.inner
            .read()
            .keys
            .iter()
            .find(|(_, (_, n))| n.as_deref() == Some(name))
            .map(|(key, _)| key.iter().map(|b| format!("{:02x}", b)).collect())
    }

    /// Remove a key by name
    ///
    /// Returns the workspace ID if found and removed.
    pub fn remove_by_name(&self, name: &str) -> Option<WorkspaceId> {
        let mut inner = self.inner.write();
        let key_to_remove = inner
            .keys
            .iter()
            .find(|(_, (_, n))| n.as_deref() == Some(name))
            .map(|(k, _)| *k);

        if let Some(key) = key_to_remove {
            inner.keys.remove(&key).map(|(ws, _)| ws)
        } else {
            None
        }
    }

    /// Generate a new random API key with a name
    ///
    /// Returns the hex string of the generated key.
    pub fn generate(&self, workspace: WorkspaceId, name: String) -> String {
        let mut key = [0u8; API_KEY_LENGTH];
        rand::thread_rng().fill_bytes(&mut key);

        let hex: String = key.iter().map(|b| format!("{:02x}", b)).collect();
        self.inner.write().keys.insert(key, (workspace, Some(name)));

        hex
    }

    /// Check if a name is already in use
    pub fn name_exists(&self, name: &str) -> bool {
        self.inner
            .read()
            .keys
            .values()
            .any(|(_, n)| n.as_deref() == Some(name))
    }
}

/// Parse a single line from the API keys file
///
/// Format: `hex_key:workspace_id` or `hex_key:workspace_id:name`
fn parse_line(line: &str, line_num: usize) -> Result<(ApiKey, WorkspaceId, Option<String>)> {
    let parts: Vec<&str> = line.splitn(3, ':').collect();

    if parts.len() < 2 {
        return Err(AuthError::parse_error(line_num, "missing colon separator"));
    }

    let hex_key = parts[0];
    let workspace_id = parts[1].trim();
    let name = parts
        .get(2)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());

    // Validate hex key length
    if hex_key.len() != API_KEY_HEX_LENGTH {
        return Err(AuthError::invalid_key(
            line_num,
            format!(
                "expected {} hex characters, got {}",
                API_KEY_HEX_LENGTH,
                hex_key.len()
            ),
        ));
    }

    // Parse hex key
    let key = parse_hex_key(hex_key, line_num)?;

    // Validate and parse workspace ID as u32
    if workspace_id.is_empty() {
        return Err(AuthError::empty_workspace(line_num));
    }

    let ws_id: u32 = workspace_id.parse().map_err(|_| {
        AuthError::parse_error(
            line_num,
            format!(
                "invalid workspace ID '{}' (must be numeric u32)",
                workspace_id
            ),
        )
    })?;

    Ok((key, WorkspaceId::new(ws_id), name))
}

/// Parse a hex string into a 16-byte API key
fn parse_hex_key(hex: &str, line_num: usize) -> Result<ApiKey> {
    let mut key = [0u8; API_KEY_LENGTH];

    for (i, chunk) in hex.as_bytes().chunks(2).enumerate() {
        if chunk.len() != 2 {
            return Err(AuthError::invalid_key(line_num, "incomplete hex pair"));
        }

        let high = hex_digit(chunk[0], line_num)?;
        let low = hex_digit(chunk[1], line_num)?;
        key[i] = (high << 4) | low;
    }

    Ok(key)
}

/// Parse a single hex digit
#[inline]
fn hex_digit(c: u8, line_num: usize) -> Result<u8> {
    match c {
        b'0'..=b'9' => Ok(c - b'0'),
        b'a'..=b'f' => Ok(c - b'a' + 10),
        b'A'..=b'F' => Ok(c - b'A' + 10),
        _ => Err(AuthError::invalid_key(
            line_num,
            format!("invalid hex character: '{}'", c as char),
        )),
    }
}

/// Create an ApiKey from a hex string (for testing)
///
/// # Panics
///
/// Panics if the hex string is invalid.
#[cfg(test)]
pub fn key_from_hex(hex: &str) -> ApiKey {
    parse_hex_key(hex, 0).expect("invalid hex key")
}

/// Shared store using Arc for multi-threaded access
pub type SharedApiKeyStore = Arc<ApiKeyStore>;
