//! ClickHouse sink errors

/// Errors from ClickHouse sink
#[derive(Debug, thiserror::Error)]
pub enum ClickHouseSinkError {
    /// ClickHouse client error
    #[error("clickhouse error: {0}")]
    ClickHouse(#[from] clickhouse::error::Error),

    /// Insert error
    #[error("insert error: {0}")]
    InsertError(String),

    /// Insert failed (for Arrow sink)
    #[error("insert failed for table {table}: {message}")]
    InsertFailed { table: String, message: String },

    /// Decode error
    #[error("failed to decode message: {0}")]
    DecodeError(String),

    /// Configuration error
    #[error("configuration error: {0}")]
    ConfigError(String),

    /// Arrow error
    #[error("arrow error: {0}")]
    ArrowError(String),
}
