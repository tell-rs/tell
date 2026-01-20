//! Pattern Storage Trait
//!
//! Defines an abstract interface for pattern storage backends.
//! This allows the pattern transformer to work with different storage systems:
//! - File-based (default, via `PatternPersistence`)
//! - ClickHouse (distributed, via external implementation)
//! - Redis, PostgreSQL, etc. (future)
//!
//! # Design
//!
//! The trait is designed for async operations to support network-based storage
//! while remaining efficient for local file storage (using blocking IO wrapped
//! in spawn_blocking when needed).
//!
//! # Example Implementation
//!
//! ```ignore
//! struct ClickHousePatternStorage {
//!     client: clickhouse::Client,
//!     table: String,
//! }
//!
//! impl PatternStorage for ClickHousePatternStorage {
//!     async fn load(&self) -> Result<Vec<StoredPattern>, StorageError> {
//!         // Query patterns from ClickHouse
//!     }
//!
//!     async fn save(&self, patterns: &[StoredPattern]) -> Result<(), StorageError> {
//!         // Insert patterns into ClickHouse
//!     }
//! }
//! ```

use super::persistence::StoredPattern;
use std::future::Future;
use std::pin::Pin;

#[cfg(test)]
#[path = "storage_test.rs"]
mod tests;

/// Error type for storage operations
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    /// Connection error
    #[error("connection error: {0}")]
    Connection(String),

    /// Query error
    #[error("query error: {0}")]
    Query(String),

    /// Serialization error
    #[error("serialization error: {0}")]
    Serialization(String),

    /// IO error
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// Other error
    #[error("{0}")]
    Other(String),
}

/// Result type for storage operations
pub type StorageResult<T> = Result<T, StorageError>;

/// Trait for pattern storage backends
///
/// Implementations must be Send + Sync to allow use across async tasks.
pub trait PatternStorage: Send + Sync {
    /// Load all patterns from storage
    ///
    /// Returns an empty vector if no patterns exist or storage is disabled.
    fn load(&self) -> Pin<Box<dyn Future<Output = StorageResult<Vec<StoredPattern>>> + Send + '_>>;

    /// Save patterns to storage
    ///
    /// This should be an upsert operation - existing patterns (by ID) should be updated,
    /// new patterns should be inserted.
    fn save(
        &self,
        patterns: &[StoredPattern],
    ) -> Pin<Box<dyn Future<Output = StorageResult<()>> + Send + '_>>;

    /// Check if storage is enabled/connected
    fn is_enabled(&self) -> bool;

    /// Get storage backend name (for logging)
    fn name(&self) -> &'static str;
}

/// File-based storage implementation wrapping PatternPersistence
///
/// This adapts the existing `PatternPersistence` to the `PatternStorage` trait,
/// allowing it to be used interchangeably with ClickHouse or other backends.
pub struct FilePatternStorage {
    persistence: std::sync::Arc<super::PatternPersistence>,
}

impl FilePatternStorage {
    /// Create a new file-based storage wrapper
    pub fn new(persistence: std::sync::Arc<super::PatternPersistence>) -> Self {
        Self { persistence }
    }
}

impl PatternStorage for FilePatternStorage {
    fn load(&self) -> Pin<Box<dyn Future<Output = StorageResult<Vec<StoredPattern>>> + Send + '_>> {
        Box::pin(async move {
            // PatternPersistence::load is sync, wrap for async interface
            self.persistence
                .load()
                .map_err(|e| StorageError::Other(e.to_string()))
        })
    }

    fn save(
        &self,
        patterns: &[StoredPattern],
    ) -> Pin<Box<dyn Future<Output = StorageResult<()>> + Send + '_>> {
        let patterns_owned: Vec<super::drain::Pattern> = patterns
            .iter()
            .map(|sp| super::drain::Pattern {
                id: sp.id,
                template: sp.template.clone(),
                canonical_name: sp.canonical_name.clone(),
                tokens: sp.tokens.clone(),
                count: sp.count,
                first_seen: sp.first_seen,
                last_seen: sp.last_seen,
            })
            .collect();

        Box::pin(async move {
            self.persistence
                .save_all(&patterns_owned)
                .map_err(|e| StorageError::Other(e.to_string()))
        })
    }

    fn is_enabled(&self) -> bool {
        self.persistence.is_enabled()
    }

    fn name(&self) -> &'static str {
        "file"
    }
}

/// No-op storage implementation for disabled persistence
pub struct NullPatternStorage;

impl PatternStorage for NullPatternStorage {
    fn load(&self) -> Pin<Box<dyn Future<Output = StorageResult<Vec<StoredPattern>>> + Send + '_>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn save(
        &self,
        _patterns: &[StoredPattern],
    ) -> Pin<Box<dyn Future<Output = StorageResult<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }

    fn is_enabled(&self) -> bool {
        false
    }

    fn name(&self) -> &'static str {
        "null"
    }
}

/// Box type for dynamic storage dispatch
pub type BoxedPatternStorage = Box<dyn PatternStorage>;
