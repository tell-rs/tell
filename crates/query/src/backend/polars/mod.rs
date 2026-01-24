//! Polars backend for querying local Arrow IPC files
//!
//! Discovers and registers Arrow IPC files as virtual tables for SQL queries.
//!
//! # File Organization
//!
//! Expects files organized by the Arrow IPC sink:
//! ```text
//! {base_path}/
//! └── {workspace_id}/
//!     └── {date}/
//!         ├── events.arrow
//!         ├── logs.arrow
//!         ├── snapshots.arrow
//!         ├── context.arrow
//!         ├── users.arrow
//!         ├── user_devices.arrow
//!         └── user_traits.arrow
//! ```

use std::path::{Path, PathBuf};
use std::time::Instant;

use async_trait::async_trait;
use polars::prelude::*;
use regex::Regex;

use crate::backend::{QueryBackend, validate_sql};
use crate::error::QueryError;
use crate::result::{Column, DataType, QueryResult, TableInfo};

/// Known table names that map to Arrow IPC files (matching ClickHouse naming)
const KNOWN_TABLES: &[&str] = &[
    "events_v1",
    "logs_v1",
    "snapshots_v1",
    "context_v1",
    "users_v1",
    "user_devices",
    "user_traits_v1",
];

/// Polars backend for querying Arrow IPC files
#[derive(Debug, Clone)]
pub struct PolarsBackend {
    /// Base path to Arrow IPC files
    base_path: PathBuf,

    /// Workspace ID for file discovery
    workspace_id: u64,
}

impl PolarsBackend {
    /// Create a new Polars backend
    pub fn new(base_path: impl Into<PathBuf>, workspace_id: u64) -> Self {
        Self {
            base_path: base_path.into(),
            workspace_id,
        }
    }

    /// Discover all Arrow IPC files for a table
    fn discover_files(&self, table: &str) -> Result<Vec<PathBuf>, QueryError> {
        let pattern = format!(
            "{}/{}/**/{}.arrow",
            self.base_path.display(),
            self.workspace_id,
            table
        );

        let files: Vec<PathBuf> = glob::glob(&pattern)?.filter_map(Result::ok).collect();

        if files.is_empty() {
            return Err(QueryError::NoDataFiles(format!(
                "no {} files found matching pattern: {}",
                table, pattern
            )));
        }

        tracing::debug!(
            table = table,
            file_count = files.len(),
            "discovered Arrow IPC files"
        );

        Ok(files)
    }

    /// Check if a table has data files
    fn table_exists(&self, table: &str) -> bool {
        self.discover_files(table).is_ok()
    }

    /// Create a LazyFrame from discovered files
    fn scan_table(&self, table: &str) -> Result<LazyFrame, QueryError> {
        let files = self.discover_files(table)?;

        // Scan all files as a single LazyFrame
        let args = ScanArgsIpc {
            rechunk: false,
            ..Default::default()
        };

        LazyFrame::scan_ipc_files(files.into(), args).map_err(QueryError::from)
    }

    /// Strip workspace prefix from table names in SQL
    ///
    /// Analytics crate generates queries like `SELECT ... FROM 1.events_v1`
    /// but Polars registers tables without the workspace prefix.
    /// This converts `{workspace_id}.{table}` → `{table}`.
    fn strip_workspace_prefix(&self, sql: &str) -> String {
        // Match patterns like `1.events_v1`, `123.logs_v1`, etc.
        // Using word boundary to avoid matching decimals or other patterns
        let re = Regex::new(r"\b(\d+)\.([a-zA-Z_][a-zA-Z0-9_]*)\b").unwrap();

        re.replace_all(sql, |caps: &regex::Captures| {
            let table_name = &caps[2];
            // Only strip prefix for known tables
            if KNOWN_TABLES.contains(&table_name) {
                table_name.to_string()
            } else {
                // Keep original for unknown patterns (might be something else)
                caps[0].to_string()
            }
        })
        .to_string()
    }

    /// Register all available tables in the SQL context
    fn register_tables(&self, ctx: &mut polars::sql::SQLContext) -> Result<(), QueryError> {
        for table in KNOWN_TABLES {
            match self.scan_table(table) {
                Ok(lf) => {
                    ctx.register(table, lf);
                    tracing::debug!(table = table, "registered table");
                }
                Err(QueryError::NoDataFiles(_)) => {
                    // Table doesn't exist, skip silently
                    tracing::debug!(table = table, "table has no data files, skipping");
                }
                Err(e) => {
                    // Other errors should propagate
                    return Err(e);
                }
            }
        }

        Ok(())
    }

    /// Convert a Polars DataFrame to QueryResult
    fn dataframe_to_result(
        &self,
        df: DataFrame,
        execution_time_ms: u64,
    ) -> Result<QueryResult, QueryError> {
        let columns: Vec<Column> = df
            .schema()
            .iter()
            .map(|(name, dtype)| {
                Column::new(
                    name.as_str(),
                    DataType::from_polars(dtype),
                    true, // Polars doesn't track nullability in schema
                )
            })
            .collect();

        let rows = dataframe_to_rows(&df)?;

        Ok(QueryResult::new(columns, rows, execution_time_ms))
    }
}

#[async_trait]
impl QueryBackend for PolarsBackend {
    async fn execute(&self, sql: &str) -> Result<QueryResult, QueryError> {
        // Validate SQL first
        validate_sql(sql)?;

        let start = Instant::now();

        // Strip workspace prefix from table names (e.g., "1.events_v1" → "events_v1")
        let processed_sql = self.strip_workspace_prefix(sql);

        tracing::debug!(
            original_sql = sql,
            processed_sql = %processed_sql,
            "preprocessed SQL for Polars"
        );

        // Create SQL context and register tables
        let mut ctx = polars::sql::SQLContext::new();
        self.register_tables(&mut ctx)?;

        // Execute SQL
        let lf = ctx
            .execute(&processed_sql)
            .map_err(|e| QueryError::Execution(format!("SQL execution failed: {}", e)))?;

        // Collect results
        let df = lf
            .collect()
            .map_err(|e| QueryError::Execution(format!("failed to collect results: {}", e)))?;

        let execution_time_ms = start.elapsed().as_millis() as u64;

        tracing::debug!(
            rows = df.height(),
            cols = df.width(),
            time_ms = execution_time_ms,
            "query executed"
        );

        self.dataframe_to_result(df, execution_time_ms)
    }

    async fn health_check(&self) -> Result<(), QueryError> {
        // Check that at least one table has data
        for table in KNOWN_TABLES {
            if self.table_exists(table) {
                return Ok(());
            }
        }

        Err(QueryError::NoDataFiles(format!(
            "no data files found in {}",
            self.base_path.display()
        )))
    }

    fn name(&self) -> &'static str {
        "polars"
    }

    async fn list_tables(&self) -> Result<Vec<TableInfo>, QueryError> {
        let mut tables = Vec::new();

        for table_name in KNOWN_TABLES {
            if let Ok(files) = self.discover_files(table_name) {
                // Get schema from first file
                let columns = if let Some(first_file) = files.first() {
                    get_schema_from_file(first_file)?
                } else {
                    Vec::new()
                };

                tables.push(TableInfo {
                    name: table_name.to_string(),
                    row_count: None, // Would need to scan all files
                    columns,
                });
            }
        }

        Ok(tables)
    }
}

/// Get schema from an Arrow IPC file
fn get_schema_from_file(path: &Path) -> Result<Vec<Column>, QueryError> {
    let args = ScanArgsIpc::default();
    let mut lf = LazyFrame::scan_ipc(path, args)?;
    let schema = lf.collect_schema()?;

    Ok(schema
        .iter()
        .map(|(name, dtype)| Column::new(name.as_str(), DataType::from_polars(dtype), true))
        .collect())
}

/// Convert DataFrame rows to JSON values
fn dataframe_to_rows(df: &DataFrame) -> Result<Vec<Vec<serde_json::Value>>, QueryError> {
    let mut rows = Vec::with_capacity(df.height());

    for i in 0..df.height() {
        let mut row = Vec::with_capacity(df.width());
        for col in df.get_columns() {
            let series = col.as_materialized_series();
            let value = series_value_to_json(series, i)?;
            row.push(value);
        }
        rows.push(row);
    }

    Ok(rows)
}

/// Convert a single Series value to JSON
fn series_value_to_json(series: &Series, idx: usize) -> Result<serde_json::Value, QueryError> {
    use polars::datatypes::DataType as PDT;

    // Check if value at index is null
    let is_null = series.is_null().get(idx).unwrap_or(false);
    if is_null {
        return Ok(serde_json::Value::Null);
    }

    let value = match series.dtype() {
        PDT::Int8 => {
            let val = series.i8()?.get(idx).unwrap_or_default();
            serde_json::Value::Number(val.into())
        }
        PDT::Int16 => {
            let val = series.i16()?.get(idx).unwrap_or_default();
            serde_json::Value::Number(val.into())
        }
        PDT::Int32 => {
            let val = series.i32()?.get(idx).unwrap_or_default();
            serde_json::Value::Number(val.into())
        }
        PDT::Int64 => {
            let val = series.i64()?.get(idx).unwrap_or_default();
            serde_json::Value::Number(val.into())
        }
        PDT::UInt8 => {
            let val = series.u8()?.get(idx).unwrap_or_default();
            serde_json::Value::Number(val.into())
        }
        PDT::UInt16 => {
            let val = series.u16()?.get(idx).unwrap_or_default();
            serde_json::Value::Number(val.into())
        }
        PDT::UInt32 => {
            let val = series.u32()?.get(idx).unwrap_or_default();
            serde_json::Value::Number(val.into())
        }
        PDT::UInt64 => {
            let val = series.u64()?.get(idx).unwrap_or_default();
            serde_json::Value::Number(val.into())
        }
        PDT::Float32 => {
            let val = series.f32()?.get(idx).unwrap_or_default();
            serde_json::Number::from_f64(val as f64)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null)
        }
        PDT::Float64 => {
            let val = series.f64()?.get(idx).unwrap_or_default();
            serde_json::Number::from_f64(val)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null)
        }
        PDT::Boolean => {
            let val = series.bool()?.get(idx).unwrap_or_default();
            serde_json::Value::Bool(val)
        }
        PDT::String => {
            let val = series.str()?.get(idx).unwrap_or_default();
            serde_json::Value::String(val.to_string())
        }
        PDT::Binary => {
            let val = series.binary()?.get(idx).unwrap_or_default();
            // Encode as base64
            use std::io::Write;
            let mut buf = Vec::new();
            write!(buf, "{}", base64_encode(val)).ok();
            serde_json::Value::String(String::from_utf8_lossy(&buf).to_string())
        }
        PDT::Datetime(_, _) | PDT::Date | PDT::Time => {
            // Convert to string representation
            let formatted = format!("{}", series.get(idx)?);
            serde_json::Value::String(formatted)
        }
        _ => {
            // Fallback: convert to string
            let formatted = format!("{}", series.get(idx)?);
            serde_json::Value::String(formatted)
        }
    };

    Ok(value)
}

/// Simple base64 encoding for binary data
fn base64_encode(data: &[u8]) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    let mut result = String::with_capacity(data.len().div_ceil(3) * 4);

    for chunk in data.chunks(3) {
        let b0 = chunk[0] as usize;
        let b1 = chunk.get(1).copied().unwrap_or(0) as usize;
        let b2 = chunk.get(2).copied().unwrap_or(0) as usize;

        result.push(ALPHABET[b0 >> 2] as char);
        result.push(ALPHABET[((b0 & 0x03) << 4) | (b1 >> 4)] as char);

        if chunk.len() > 1 {
            result.push(ALPHABET[((b1 & 0x0f) << 2) | (b2 >> 6)] as char);
        } else {
            result.push('=');
        }

        if chunk.len() > 2 {
            result.push(ALPHABET[b2 & 0x3f] as char);
        } else {
            result.push('=');
        }
    }

    result
}

#[cfg(test)]
#[path = "polars_test.rs"]
mod polars_test;
