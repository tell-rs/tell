//! Query result types
//!
//! Unified result format across all backends (ClickHouse, Polars).

use serde::{Deserialize, Serialize};

/// Unified query result across all backends
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    /// Column definitions
    pub columns: Vec<Column>,

    /// Row data as JSON values (backend-agnostic)
    pub rows: Vec<Vec<serde_json::Value>>,

    /// Total row count
    pub row_count: usize,

    /// Query execution time in milliseconds
    pub execution_time_ms: u64,
}

impl QueryResult {
    /// Create a new query result
    pub fn new(
        columns: Vec<Column>,
        rows: Vec<Vec<serde_json::Value>>,
        execution_time_ms: u64,
    ) -> Self {
        let row_count = rows.len();
        Self {
            columns,
            rows,
            row_count,
            execution_time_ms,
        }
    }

    /// Create an empty result
    pub fn empty() -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
            row_count: 0,
            execution_time_ms: 0,
        }
    }

    /// Check if result is empty
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Get column names
    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }
}

/// Column definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    /// Column name
    pub name: String,

    /// Data type
    pub data_type: DataType,

    /// Whether the column is nullable
    pub nullable: bool,
}

impl Column {
    /// Create a new column definition
    pub fn new(name: impl Into<String>, data_type: DataType, nullable: bool) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable,
        }
    }
}

/// Data types supported in query results
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DataType {
    /// Signed 64-bit integer
    Int64,
    /// Unsigned 64-bit integer
    UInt64,
    /// 64-bit floating point
    Float64,
    /// UTF-8 string
    String,
    /// Binary data
    Binary,
    /// Boolean
    Boolean,
    /// Timestamp (milliseconds since epoch)
    Timestamp,
    /// JSON object
    Json,
    /// Unknown/other type
    Unknown,
}

impl DataType {
    /// Convert from Polars data type
    pub fn from_polars(dtype: &polars::datatypes::DataType) -> Self {
        use polars::datatypes::DataType as PDT;
        match dtype {
            PDT::Int8 | PDT::Int16 | PDT::Int32 | PDT::Int64 => DataType::Int64,
            PDT::UInt8 | PDT::UInt16 | PDT::UInt32 | PDT::UInt64 => DataType::UInt64,
            PDT::Float32 | PDT::Float64 => DataType::Float64,
            PDT::String => DataType::String,
            PDT::Binary => DataType::Binary,
            PDT::Boolean => DataType::Boolean,
            PDT::Datetime(_, _) | PDT::Date | PDT::Time => DataType::Timestamp,
            PDT::Struct(_) | PDT::List(_) => DataType::Json,
            _ => DataType::Unknown,
        }
    }
}

/// Table information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableInfo {
    /// Table name
    pub name: String,

    /// Estimated row count (if available)
    pub row_count: Option<u64>,

    /// Column definitions
    pub columns: Vec<Column>,
}

impl TableInfo {
    /// Create new table info with just a name
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            row_count: None,
            columns: Vec::new(),
        }
    }

    /// Add columns to table info
    pub fn with_columns(mut self, columns: Vec<Column>) -> Self {
        self.columns = columns;
        self
    }
}
