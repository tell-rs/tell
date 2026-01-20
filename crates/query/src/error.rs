//! Query error types

/// Errors that can occur during query execution
#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    /// Backend not configured
    #[error("backend not configured: {0}")]
    BackendNotConfigured(String),

    /// Connection failed
    #[error("connection failed: {0}")]
    Connection(String),

    /// Query execution failed
    #[error("query execution failed: {0}")]
    Execution(String),

    /// Invalid SQL (only SELECT/WITH allowed)
    #[error("invalid SQL: {0}")]
    InvalidSql(String),

    /// Table not found
    #[error("table not found: {0}")]
    TableNotFound(String),

    /// Workspace required for local backend
    #[error("workspace_id required for local backend")]
    WorkspaceRequired,

    /// No data files found
    #[error("no data files found: {0}")]
    NoDataFiles(String),

    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Configuration error
    #[error("configuration error: {0}")]
    Config(String),

    /// Polars error
    #[error("polars error: {0}")]
    Polars(String),

    /// ClickHouse error
    #[error("clickhouse error: {0}")]
    ClickHouse(String),

    /// Serialization error
    #[error("serialization error: {0}")]
    Serialization(String),
}

impl From<polars::error::PolarsError> for QueryError {
    fn from(err: polars::error::PolarsError) -> Self {
        QueryError::Polars(err.to_string())
    }
}

impl From<glob::PatternError> for QueryError {
    fn from(err: glob::PatternError) -> Self {
        QueryError::Config(format!("invalid glob pattern: {}", err))
    }
}

impl From<glob::GlobError> for QueryError {
    fn from(err: glob::GlobError) -> Self {
        QueryError::Io(std::io::Error::other(err.to_string()))
    }
}

impl From<serde_json::Error> for QueryError {
    fn from(err: serde_json::Error) -> Self {
        QueryError::Serialization(err.to_string())
    }
}
