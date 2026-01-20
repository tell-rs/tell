//! ClickHouse client for schema operations
//!
//! Provides connection and query execution for init/check/destroy commands.

use anyhow::{Context, Result};
use clickhouse::Client;

/// Default ClickHouse URL
pub const DEFAULT_URL: &str = "http://localhost:8123";

/// ClickHouse client wrapper for schema operations
pub struct SchemaClient {
    client: Client,
}

impl SchemaClient {
    /// Create a new client and test connection
    pub async fn connect(url: &str) -> Result<Self> {
        let client = Client::default().with_url(url);

        // Test connection
        client
            .query("SELECT 1")
            .execute()
            .await
            .with_context(|| format!("failed to connect to ClickHouse at {url}"))?;

        Ok(Self { client })
    }

    /// Execute a single SQL statement
    pub async fn execute(&self, sql: &str) -> Result<()> {
        self.client
            .query(sql)
            .execute()
            .await
            .with_context(|| format!("failed to execute: {}", truncate_sql(sql)))?;
        Ok(())
    }

    /// Execute multiple SQL statements
    pub async fn execute_all(&self, statements: &[String]) -> Result<()> {
        for sql in statements {
            self.execute(sql).await?;
        }
        Ok(())
    }

    /// Check if a database exists
    pub async fn database_exists(&self, name: &str) -> Result<bool> {
        let sql = format!(
            "SELECT 1 FROM system.databases WHERE name = '{name}' LIMIT 1"
        );
        let result = self
            .client
            .query(&sql)
            .fetch_optional::<u8>()
            .await
            .context("failed to check database existence")?;
        Ok(result.is_some())
    }

    /// Check if a table exists
    pub async fn table_exists(&self, database: &str, table: &str) -> Result<bool> {
        let sql = format!(
            "SELECT 1 FROM system.tables WHERE database = '{database}' AND name = '{table}' LIMIT 1"
        );
        let result = self
            .client
            .query(&sql)
            .fetch_optional::<u8>()
            .await
            .context("failed to check table existence")?;
        Ok(result.is_some())
    }

    /// Check if a user exists
    pub async fn user_exists(&self, name: &str) -> Result<bool> {
        let sql = format!(
            "SELECT 1 FROM system.users WHERE name = '{name}' LIMIT 1"
        );
        let result = self
            .client
            .query(&sql)
            .fetch_optional::<u8>()
            .await
            .context("failed to check user existence")?;
        Ok(result.is_some())
    }

    /// List all tables in a database
    #[allow(dead_code)]
    pub async fn list_tables(&self, database: &str) -> Result<Vec<String>> {
        let sql = format!(
            "SELECT name FROM system.tables WHERE database = '{database}' ORDER BY name"
        );
        let tables = self
            .client
            .query(&sql)
            .fetch_all::<String>()
            .await
            .context("failed to list tables")?;
        Ok(tables)
    }

    /// Get ClickHouse version
    pub async fn version(&self) -> Result<String> {
        let version = self
            .client
            .query("SELECT version()")
            .fetch_one::<String>()
            .await
            .context("failed to get ClickHouse version")?;
        Ok(version)
    }
}

/// Truncate SQL for error messages
fn truncate_sql(sql: &str) -> String {
    let first_line = sql.lines().next().unwrap_or(sql);
    if first_line.len() > 80 {
        format!("{}...", &first_line[..77])
    } else {
        first_line.to_string()
    }
}
