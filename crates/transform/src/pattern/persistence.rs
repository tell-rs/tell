//! Pattern Persistence - File-based pattern storage
//!
//! Stores discovered patterns to disk for persistence across restarts.
//! Patterns are stored as JSON for human readability and debugging.
//!
//! # File Format
//!
//! ```json
//! {
//!   "version": 1,
//!   "patterns": [
//!     {"id": 1, "template": "User <*> logged in", "count": 42},
//!     {"id": 2, "template": "Error in <*>", "count": 15}
//!   ]
//! }
//! ```
//!
//! # Future: ClickHouse Integration
//!
//! For distributed deployments, patterns can be stored in ClickHouse.
//! This enables pattern sharing across collector instances and hot reload.

use super::drain::{Pattern, PatternId};
use crate::TransformResult;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Write};
use std::path::Path;

#[cfg(test)]
#[path = "persistence_test.rs"]
mod tests;

/// File format version
const FORMAT_VERSION: u32 = 1;

/// Serializable pattern for JSON storage
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StoredPattern {
    /// Pattern ID
    pub id: PatternId,

    /// Pattern template string
    pub template: String,

    /// Match count
    pub count: u64,

    /// Token sequence (for reconstruction)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tokens: Vec<Option<String>>,
}

impl From<&Pattern> for StoredPattern {
    fn from(p: &Pattern) -> Self {
        Self {
            id: p.id,
            template: p.template.clone(),
            count: p.count,
            tokens: p.tokens.clone(),
        }
    }
}

impl From<Pattern> for StoredPattern {
    fn from(p: Pattern) -> Self {
        Self {
            id: p.id,
            template: p.template,
            count: p.count,
            tokens: p.tokens,
        }
    }
}

/// Pattern file structure
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct PatternFile {
    /// File format version
    version: u32,

    /// Stored patterns
    patterns: Vec<StoredPattern>,
}


/// Pattern persistence manager
pub struct PatternPersistence {
    /// File path for pattern storage
    file_path: Option<std::path::PathBuf>,

    /// Pending patterns to flush
    pending: parking_lot::Mutex<Vec<StoredPattern>>,

    /// Maximum pending before auto-flush
    batch_size: usize,
}

impl PatternPersistence {
    /// Create a new persistence manager
    pub fn new(file_path: Option<std::path::PathBuf>, batch_size: usize) -> Self {
        Self {
            file_path,
            pending: parking_lot::Mutex::new(Vec::new()),
            batch_size: batch_size.max(1),
        }
    }

    /// Create a disabled persistence manager
    pub fn disabled() -> Self {
        Self::new(None, 100)
    }

    /// Check if persistence is enabled
    pub fn is_enabled(&self) -> bool {
        self.file_path.is_some()
    }

    /// Add a pattern to the pending queue
    ///
    /// Returns true if auto-flush threshold reached.
    pub fn add_pattern(&self, pattern: &Pattern) -> bool {
        if !self.is_enabled() {
            return false;
        }

        let mut pending = self.pending.lock();
        pending.push(StoredPattern::from(pattern));

        pending.len() >= self.batch_size
    }

    /// Get number of pending patterns
    pub fn pending_count(&self) -> usize {
        self.pending.lock().len()
    }

    /// Flush pending patterns to disk
    pub fn flush(&self) -> TransformResult<usize> {
        let file_path = match &self.file_path {
            Some(p) => p,
            None => return Ok(0),
        };

        let pending: Vec<StoredPattern> = {
            let mut guard = self.pending.lock();
            std::mem::take(&mut *guard)
        };

        if pending.is_empty() {
            return Ok(0);
        }

        let count = pending.len();

        // Load existing patterns
        let mut patterns = self.load_patterns_map(file_path)?;

        // Merge pending patterns
        for pattern in pending {
            patterns.insert(pattern.id, pattern);
        }

        // Write back
        self.write_patterns(file_path, patterns.into_values().collect())?;

        Ok(count)
    }

    /// Load all patterns from disk
    pub fn load(&self) -> TransformResult<Vec<StoredPattern>> {
        let file_path = match &self.file_path {
            Some(p) => p,
            None => return Ok(Vec::new()),
        };

        if !file_path.exists() {
            return Ok(Vec::new());
        }

        self.load_patterns(file_path)
    }

    /// Load patterns from file
    fn load_patterns(&self, path: &Path) -> TransformResult<Vec<StoredPattern>> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);

        let pattern_file: PatternFile = serde_json::from_reader(reader)
            .map_err(|e| crate::TransformError::decode(format!("failed to parse pattern file: {}", e)))?;

        if pattern_file.version != FORMAT_VERSION {
            tracing::warn!(
                "Pattern file version mismatch: expected {}, got {}",
                FORMAT_VERSION,
                pattern_file.version
            );
        }

        Ok(pattern_file.patterns)
    }

    /// Load patterns into a map for merging
    fn load_patterns_map(&self, path: &Path) -> TransformResult<HashMap<PatternId, StoredPattern>> {
        let patterns = if path.exists() {
            self.load_patterns(path)?
        } else {
            Vec::new()
        };

        Ok(patterns.into_iter().map(|p| (p.id, p)).collect())
    }

    /// Write patterns to file
    fn write_patterns(&self, path: &Path, patterns: Vec<StoredPattern>) -> TransformResult<()> {
        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        // Write to temp file first
        let temp_path = path.with_extension("tmp");
        let file = File::create(&temp_path)?;
        let mut writer = BufWriter::new(file);

        let pattern_file = PatternFile {
            version: FORMAT_VERSION,
            patterns,
        };

        serde_json::to_writer_pretty(&mut writer, &pattern_file)
            .map_err(|e| crate::TransformError::failed(format!("failed to write patterns: {}", e)))?;

        writer.flush()?;

        // Atomic rename
        fs::rename(&temp_path, path)?;

        Ok(())
    }

    /// Save all patterns from a drain tree
    pub fn save_all(&self, patterns: &[Pattern]) -> TransformResult<()> {
        let file_path = match &self.file_path {
            Some(p) => p,
            None => return Ok(()),
        };

        let stored: Vec<StoredPattern> = patterns.iter().map(StoredPattern::from).collect();
        self.write_patterns(file_path, stored)
    }
}

impl Default for PatternPersistence {
    fn default() -> Self {
        Self::disabled()
    }
}
