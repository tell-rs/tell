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

    /// Decode error
    #[error("failed to decode message: {0}")]
    DecodeError(String),

    /// Configuration error
    #[error("configuration error: {0}")]
    ConfigError(String),
}
