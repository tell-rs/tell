//! Tell Query - SQL query execution for Tell analytics
//!
//! Provides a unified interface for querying data from multiple backends:
//! - **ClickHouse**: Production analytics database
//! - **Polars**: Local Arrow IPC files for development/edge deployments
//!
//! # Usage
//!
//! ```ignore
//! use tell_query::{QueryEngine, QueryConfig};
//!
//! // Create engine from config
//! let config = QueryConfig::local("/var/lib/tell/arrow_ipc", 1);
//! let engine = QueryEngine::from_query_config(&config)?;
//!
//! // Execute query
//! let result = engine.query("SELECT COUNT(*) FROM events").await?;
//! println!("Rows: {}", result.row_count);
//! ```
//!
//! # CLI
//!
//! ```bash
//! tell query "SELECT * FROM events LIMIT 10"
//! tell query "SELECT event_name, COUNT(*) FROM events GROUP BY event_name" --format json
//! ```

pub mod backend;
pub mod config;
pub mod error;
pub mod result;

// Re-exports
pub use backend::clickhouse::{ClickHouseBackend, ClickHouseBackendConfig};
pub use backend::polars::PolarsBackend;
pub use backend::QueryBackend;
pub use config::{QueryBackendType, QueryConfig, ResolvedQueryConfig};
pub use error::QueryError;
pub use result::{Column, DataType, QueryResult, TableInfo};

use std::sync::Arc;

/// Query engine that routes queries to the appropriate backend
pub struct QueryEngine {
    backend: Arc<dyn QueryBackend>,
}

impl QueryEngine {
    /// Create a new query engine with a specific backend
    pub fn new(backend: impl QueryBackend + 'static) -> Self {
        Self {
            backend: Arc::new(backend),
        }
    }

    /// Create a query engine from resolved config
    pub fn from_resolved_config(config: &ResolvedQueryConfig) -> Result<Self, QueryError> {
        match config.backend {
            QueryBackendType::Local => {
                let path = config.path.as_ref().ok_or_else(|| {
                    QueryError::Config("path required for local backend".to_string())
                })?;
                let backend = PolarsBackend::new(path, config.workspace_id);
                Ok(Self::new(backend))
            }
            QueryBackendType::ClickHouse => {
                let url = config.url.as_ref().ok_or_else(|| {
                    QueryError::Config("url required for clickhouse backend".to_string())
                })?;
                let database = config.database.as_ref().ok_or_else(|| {
                    QueryError::Config("database required for clickhouse backend".to_string())
                })?;

                let mut ch_config = ClickHouseBackendConfig::new(url, database);

                // Add credentials if provided
                if let (Some(user), Some(pass)) = (&config.username, &config.password) {
                    ch_config = ch_config.with_credentials(user, pass);
                }

                let backend = ClickHouseBackend::new(&ch_config);
                Ok(Self::new(backend))
            }
        }
    }

    /// Create a query engine from query config (without sink resolution)
    pub fn from_query_config(config: &QueryConfig) -> Result<Self, QueryError> {
        let resolved = ResolvedQueryConfig::from_config(config)?;
        Self::from_resolved_config(&resolved)
    }

    /// Execute a SQL query
    pub async fn query(&self, sql: &str) -> Result<QueryResult, QueryError> {
        self.backend.execute(sql).await
    }

    /// Check if the backend is healthy
    pub async fn health_check(&self) -> Result<(), QueryError> {
        self.backend.health_check().await
    }

    /// List available tables
    pub async fn list_tables(&self) -> Result<Vec<TableInfo>, QueryError> {
        self.backend.list_tables().await
    }

    /// Get the backend name
    pub fn backend_name(&self) -> &'static str {
        self.backend.name()
    }
}

/// Output format for query results
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum OutputFormat {
    /// ASCII table format (default)
    #[default]
    Table,
    /// JSON array of objects
    Json,
    /// CSV format
    Csv,
}

impl std::str::FromStr for OutputFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "table" => Ok(OutputFormat::Table),
            "json" => Ok(OutputFormat::Json),
            "csv" => Ok(OutputFormat::Csv),
            _ => Err(format!("unknown format: {}", s)),
        }
    }
}

impl std::fmt::Display for OutputFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OutputFormat::Table => write!(f, "table"),
            OutputFormat::Json => write!(f, "json"),
            OutputFormat::Csv => write!(f, "csv"),
        }
    }
}

// Implement QueryBackend for QueryEngine so it can be used with MetricsEngine
#[async_trait::async_trait]
impl QueryBackend for QueryEngine {
    async fn execute(&self, sql: &str) -> Result<QueryResult, QueryError> {
        self.backend.execute(sql).await
    }

    async fn health_check(&self) -> Result<(), QueryError> {
        self.backend.health_check().await
    }

    fn name(&self) -> &'static str {
        self.backend.name()
    }

    async fn list_tables(&self) -> Result<Vec<TableInfo>, QueryError> {
        self.backend.list_tables().await
    }
}
