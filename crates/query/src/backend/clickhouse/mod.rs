//! ClickHouse backend for querying analytics data
//!
//! Executes SQL queries against a ClickHouse database using the HTTP interface.

use std::collections::HashMap;
use std::time::Instant;

use async_trait::async_trait;
use serde::Deserialize;

use crate::backend::{validate_sql, QueryBackend};
use crate::error::QueryError;
use crate::result::{Column, DataType, QueryResult, TableInfo};

// =============================================================================
// Configuration
// =============================================================================

/// ClickHouse backend configuration
#[derive(Debug, Clone)]
pub struct ClickHouseBackendConfig {
    /// ClickHouse HTTP URL (e.g., "http://localhost:8123")
    pub url: String,

    /// Database name
    pub database: String,

    /// Username for authentication (optional)
    pub username: Option<String>,

    /// Password for authentication (optional)
    pub password: Option<String>,

    /// Enable LZ4 compression
    pub compression_enabled: bool,

    /// Max execution time in seconds
    pub max_execution_time: u64,
}

impl Default for ClickHouseBackendConfig {
    fn default() -> Self {
        Self {
            url: "http://localhost:8123".into(),
            database: "default".into(),
            username: None,
            password: None,
            compression_enabled: true,
            max_execution_time: 60,
        }
    }
}

impl ClickHouseBackendConfig {
    /// Create a new config with URL and database
    pub fn new(url: impl Into<String>, database: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            database: database.into(),
            ..Default::default()
        }
    }

    /// Set authentication credentials
    pub fn with_credentials(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.username = Some(username.into());
        self.password = Some(password.into());
        self
    }
}

// =============================================================================
// Backend Implementation
// =============================================================================

/// ClickHouse backend for SQL queries using HTTP interface
#[derive(Clone)]
pub struct ClickHouseBackend {
    client: reqwest::Client,
    config: ClickHouseBackendConfig,
}

impl std::fmt::Debug for ClickHouseBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClickHouseBackend")
            .field("url", &self.config.url)
            .field("database", &self.config.database)
            .finish()
    }
}

impl ClickHouseBackend {
    /// Create a new ClickHouse backend from config
    pub fn new(config: &ClickHouseBackendConfig) -> Self {
        Self {
            client: reqwest::Client::new(),
            config: config.clone(),
        }
    }

    /// Create from URL and database directly
    pub fn from_url(url: impl Into<String>, database: impl Into<String>) -> Self {
        let config = ClickHouseBackendConfig::new(url, database);
        Self::new(&config)
    }

    /// Build the query URL with parameters
    fn build_url(&self, query: &str) -> String {
        let mut url = format!(
            "{}/?database={}&max_execution_time={}",
            self.config.url, self.config.database, self.config.max_execution_time
        );

        // Add query as parameter
        url.push_str("&query=");
        url.push_str(&urlencoding::encode(query));

        url
    }

    /// Execute a query and get raw JSON response
    async fn execute_query(&self, sql: &str) -> Result<String, QueryError> {
        let url = self.build_url(sql);

        let mut request = self.client.get(&url);

        // Add authentication if configured
        if let (Some(user), Some(pass)) = (&self.config.username, &self.config.password) {
            request = request.basic_auth(user, Some(pass));
        }

        let response = request.send().await.map_err(|e| {
            QueryError::Connection(format!("ClickHouse connection failed: {}", e))
        })?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(QueryError::Execution(format!(
                "ClickHouse error ({}): {}",
                status, body
            )));
        }

        response.text().await.map_err(|e| {
            QueryError::Execution(format!("failed to read response: {}", e))
        })
    }
}

#[async_trait]
impl QueryBackend for ClickHouseBackend {
    async fn execute(&self, sql: &str) -> Result<QueryResult, QueryError> {
        // Validate SQL first
        validate_sql(sql)?;

        let start = Instant::now();

        // Execute query with JSONEachRow format for structured output
        let query_with_format = format!("{} FORMAT JSONEachRow", sql.trim().trim_end_matches(';'));
        let response_text = self.execute_query(&query_with_format).await?;

        let execution_time_ms = start.elapsed().as_millis() as u64;

        // Parse JSON lines
        if response_text.trim().is_empty() {
            return Ok(QueryResult::new(Vec::new(), Vec::new(), execution_time_ms));
        }

        let json_rows: Vec<HashMap<String, serde_json::Value>> = response_text
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| {
                serde_json::from_str(line).map_err(|e| {
                    QueryError::Serialization(format!("failed to parse JSON row: {}", e))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        if json_rows.is_empty() {
            return Ok(QueryResult::new(Vec::new(), Vec::new(), execution_time_ms));
        }

        // Extract columns from first row's keys (preserve order by using first row)
        let first_row = &json_rows[0];
        let column_names: Vec<String> = first_row.keys().cloned().collect();

        let columns: Vec<Column> = column_names
            .iter()
            .map(|name| {
                let value = first_row.get(name).unwrap_or(&serde_json::Value::Null);
                Column::new(name.clone(), infer_data_type(value), true)
            })
            .collect();

        // Extract row values in column order
        let rows: Vec<Vec<serde_json::Value>> = json_rows
            .iter()
            .map(|row| {
                column_names
                    .iter()
                    .map(|name| row.get(name).cloned().unwrap_or(serde_json::Value::Null))
                    .collect()
            })
            .collect();

        tracing::debug!(
            rows = rows.len(),
            cols = columns.len(),
            time_ms = execution_time_ms,
            "ClickHouse query executed"
        );

        Ok(QueryResult::new(columns, rows, execution_time_ms))
    }

    async fn health_check(&self) -> Result<(), QueryError> {
        self.execute_query("SELECT 1").await?;
        Ok(())
    }

    fn name(&self) -> &'static str {
        "clickhouse"
    }

    async fn list_tables(&self) -> Result<Vec<TableInfo>, QueryError> {
        // Query system.tables for the current database
        let sql = format!(
            "SELECT name, total_rows FROM system.tables WHERE database = '{}' FORMAT JSONEachRow",
            self.config.database
        );

        let response_text = self.execute_query(&sql).await?;

        let table_rows: Vec<TableRowJson> = response_text
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| {
                serde_json::from_str(line)
                    .map_err(|e| QueryError::Serialization(format!("parse error: {}", e)))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let mut tables = Vec::with_capacity(table_rows.len());

        for table_row in table_rows {
            // Get columns for this table
            let columns = self.get_table_columns(&table_row.name).await?;

            tables.push(TableInfo {
                name: table_row.name,
                row_count: table_row.total_rows,
                columns,
            });
        }

        Ok(tables)
    }
}

impl ClickHouseBackend {
    /// Get column information for a table
    async fn get_table_columns(&self, table: &str) -> Result<Vec<Column>, QueryError> {
        let sql = format!(
            "SELECT name, type FROM system.columns WHERE database = '{}' AND table = '{}' FORMAT JSONEachRow",
            self.config.database, table
        );

        let response_text = self.execute_query(&sql).await?;

        let column_rows: Vec<ColumnRowJson> = response_text
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| {
                serde_json::from_str(line)
                    .map_err(|e| QueryError::Serialization(format!("parse error: {}", e)))
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(column_rows
            .into_iter()
            .map(|col| {
                Column::new(
                    col.name,
                    clickhouse_type_to_datatype(&col.r#type),
                    col.r#type.contains("Nullable"),
                )
            })
            .collect())
    }
}

// =============================================================================
// Helper Types
// =============================================================================

/// Table metadata row
#[derive(Debug, Deserialize)]
struct TableRowJson {
    name: String,
    total_rows: Option<u64>,
}

/// Column metadata row
#[derive(Debug, Deserialize)]
struct ColumnRowJson {
    name: String,
    r#type: String,
}

// =============================================================================
// Type Conversion
// =============================================================================

/// Infer DataType from a JSON value
fn infer_data_type(value: &serde_json::Value) -> DataType {
    match value {
        serde_json::Value::Null => DataType::Unknown,
        serde_json::Value::Bool(_) => DataType::Boolean,
        serde_json::Value::Number(n) => {
            if n.is_f64() {
                DataType::Float64
            } else if n.is_u64() {
                DataType::UInt64
            } else {
                DataType::Int64
            }
        }
        serde_json::Value::String(_) => DataType::String,
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => DataType::Json,
    }
}

/// Convert ClickHouse type string to DataType
fn clickhouse_type_to_datatype(ch_type: &str) -> DataType {
    // Strip Nullable wrapper
    let inner_type = ch_type
        .strip_prefix("Nullable(")
        .and_then(|s| s.strip_suffix(')'))
        .unwrap_or(ch_type);

    // Strip LowCardinality wrapper
    let inner_type = inner_type
        .strip_prefix("LowCardinality(")
        .and_then(|s| s.strip_suffix(')'))
        .unwrap_or(inner_type);

    match inner_type {
        // Integers
        "Int8" | "Int16" | "Int32" | "Int64" | "Int128" | "Int256" => DataType::Int64,
        "UInt8" | "UInt16" | "UInt32" | "UInt64" | "UInt128" | "UInt256" => DataType::UInt64,

        // Floats
        "Float32" | "Float64" => DataType::Float64,

        // Strings
        "String" | "FixedString" => DataType::String,
        t if t.starts_with("FixedString(") => DataType::String,
        t if t.starts_with("Enum") => DataType::String,

        // Binary
        t if t.starts_with("UUID") => DataType::String, // UUID as string
        t if t.starts_with("IPv") => DataType::String,  // IP addresses as string

        // Boolean
        "Bool" => DataType::Boolean,

        // Timestamps
        "Date" | "Date32" => DataType::Timestamp,
        t if t.starts_with("DateTime") => DataType::Timestamp,

        // JSON/Complex
        "JSON" => DataType::Json,
        t if t.starts_with("Array(") => DataType::Json,
        t if t.starts_with("Map(") => DataType::Json,
        t if t.starts_with("Tuple(") => DataType::Json,

        // Default
        _ => DataType::Unknown,
    }
}

/// URL encoding helper
mod urlencoding {
    pub fn encode(s: &str) -> String {
        let mut result = String::with_capacity(s.len() * 3);
        for c in s.chars() {
            match c {
                'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' | '.' | '~' => {
                    result.push(c);
                }
                ' ' => result.push_str("%20"),
                _ => {
                    for byte in c.to_string().as_bytes() {
                        result.push_str(&format!("%{:02X}", byte));
                    }
                }
            }
        }
        result
    }
}

#[cfg(test)]
#[path = "clickhouse_test.rs"]
mod clickhouse_test;
