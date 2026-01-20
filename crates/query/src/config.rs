//! Query configuration types

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Query configuration
///
/// Can reference a sink by name or specify inline connection details.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueryConfig {
    /// Reference to a sink by name (pulls connection details from sinks config)
    pub sink: Option<String>,

    /// Backend type (clickhouse, local) - used when sink is not set
    pub backend: Option<String>,

    /// ClickHouse/HTTP URL - used when sink is not set
    pub url: Option<String>,

    /// Database name - used when sink is not set
    pub database: Option<String>,

    /// Username for authentication
    pub username: Option<String>,

    /// Password for authentication
    pub password: Option<String>,

    /// Path to Arrow IPC files - used when sink is not set or for local backend
    pub path: Option<PathBuf>,

    /// Workspace ID for local backend queries (defaults to 1)
    #[serde(default = "default_workspace_id")]
    pub workspace_id: u64,
}

fn default_workspace_id() -> u64 {
    1
}

impl QueryConfig {
    /// Create config for ClickHouse backend
    pub fn clickhouse(url: impl Into<String>, database: impl Into<String>) -> Self {
        Self {
            backend: Some("clickhouse".to_string()),
            url: Some(url.into()),
            database: Some(database.into()),
            ..Default::default()
        }
    }

    /// Create config for local Arrow IPC backend
    pub fn local(path: impl Into<PathBuf>, workspace_id: u64) -> Self {
        Self {
            backend: Some("local".to_string()),
            path: Some(path.into()),
            workspace_id,
            ..Default::default()
        }
    }

    /// Create config referencing a sink
    pub fn from_sink(sink_name: impl Into<String>) -> Self {
        Self {
            sink: Some(sink_name.into()),
            ..Default::default()
        }
    }

    /// Get the effective backend type
    pub fn effective_backend(&self) -> Option<&str> {
        self.backend.as_deref()
    }

    /// Check if this config references a sink
    pub fn references_sink(&self) -> bool {
        self.sink.is_some()
    }
}

/// Resolved query configuration (after sink reference resolution)
#[derive(Debug, Clone)]
pub struct ResolvedQueryConfig {
    /// Backend type
    pub backend: QueryBackendType,

    /// ClickHouse URL (for clickhouse backend)
    pub url: Option<String>,

    /// Database name (for clickhouse backend)
    pub database: Option<String>,

    /// Username for authentication
    pub username: Option<String>,

    /// Password for authentication
    pub password: Option<String>,

    /// Path to Arrow IPC files (for local backend)
    pub path: Option<PathBuf>,

    /// Workspace ID (for local backend)
    pub workspace_id: u64,
}

/// Query backend type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryBackendType {
    /// ClickHouse backend
    ClickHouse,
    /// Local Arrow IPC backend (via Polars)
    Local,
}

impl ResolvedQueryConfig {
    /// Create from QueryConfig (without sink resolution - for standalone use)
    pub fn from_config(config: &QueryConfig) -> Result<Self, crate::QueryError> {
        let backend = match config.backend.as_deref() {
            Some("clickhouse") => QueryBackendType::ClickHouse,
            Some("local") | Some("polars") => QueryBackendType::Local,
            Some(other) => {
                return Err(crate::QueryError::Config(format!(
                    "unknown backend: {}",
                    other
                )));
            }
            None => {
                // Infer backend from what's configured
                if config.url.is_some() {
                    QueryBackendType::ClickHouse
                } else if config.path.is_some() {
                    QueryBackendType::Local
                } else {
                    return Err(crate::QueryError::Config(
                        "no query backend configured. Options:\n  \
                         1. sink = \"clickhouse\"  (reference a ClickHouse sink)\n  \
                         2. url = \"http://localhost:8123\"  (ClickHouse URL)\n  \
                         3. path = \"arrow/\"  (local Arrow IPC files)\n\
                         See configs/example.toml [query] section for examples."
                            .to_string(),
                    ));
                }
            }
        };

        Ok(Self {
            backend,
            url: config.url.clone(),
            database: config.database.clone(),
            username: config.username.clone(),
            password: config.password.clone(),
            path: config.path.clone(),
            workspace_id: config.workspace_id,
        })
    }
}
