//! Analytics error types

use thiserror::Error;

/// Analytics errors
#[derive(Debug, Error)]
pub enum AnalyticsError {
    /// Invalid filter syntax
    #[error("invalid filter: {0}")]
    InvalidFilter(String),

    /// Invalid time range
    #[error("invalid time range: {0}")]
    InvalidTimeRange(String),

    /// Invalid granularity
    #[error("invalid granularity: {0}")]
    InvalidGranularity(String),

    /// Invalid breakdown field
    #[error("invalid breakdown: {0}")]
    InvalidBreakdown(String),

    /// Invalid operator
    #[error("invalid operator: {0}")]
    InvalidOperator(String),

    /// Query execution failed
    #[error("query failed: {0}")]
    QueryFailed(String),

    /// Missing required field
    #[error("missing required field: {0}")]
    MissingField(String),

    /// Value out of range
    #[error("value out of range: {0}")]
    OutOfRange(String),

    /// Backend error (from tell-query)
    #[error("backend error: {0}")]
    Backend(#[from] tell_query::QueryError),
}

/// Result type for analytics operations
pub type Result<T> = std::result::Result<T, AnalyticsError>;
