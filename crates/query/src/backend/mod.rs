//! Query backend trait and implementations

pub mod clickhouse;
pub mod polars;

use async_trait::async_trait;

use crate::error::QueryError;
use crate::result::{QueryResult, TableInfo};

/// Query backend trait
///
/// Implemented by ClickHouse and Polars backends.
#[async_trait]
pub trait QueryBackend: Send + Sync {
    /// Execute a SQL query
    async fn execute(&self, sql: &str) -> Result<QueryResult, QueryError>;

    /// Check if backend is available
    async fn health_check(&self) -> Result<(), QueryError>;

    /// Backend name for logging
    fn name(&self) -> &'static str;

    /// List available tables
    async fn list_tables(&self) -> Result<Vec<TableInfo>, QueryError>;
}

/// Validate SQL query - only allow SELECT and WITH (CTE) queries
///
/// This is a guardrail to prevent accidental destructive queries.
/// The user is trusted (they have CLI access and credentials), so this
/// is not a security boundary - just protection against mistakes.
pub fn validate_sql(sql: &str) -> Result<(), QueryError> {
    let trimmed = sql.trim();
    let upper = trimmed.to_uppercase();

    // Must start with SELECT or WITH (CTE)
    if !upper.starts_with("SELECT") && !upper.starts_with("WITH") {
        return Err(QueryError::InvalidSql(
            "only SELECT and WITH queries are allowed".to_string(),
        ));
    }

    // Block SELECT ... INTO (creates tables in some databases)
    // Look for INTO that's not part of INSERT INTO (which is already blocked)
    if upper.contains(" INTO ") && !upper.contains("INSERT INTO") {
        return Err(QueryError::InvalidSql(
            "SELECT INTO is not allowed".to_string(),
        ));
    }

    // Disallow multiple statements (e.g., "SELECT 1; DROP TABLE x")
    // Allow trailing semicolon for convenience
    if trimmed.contains(';') && !trimmed.ends_with(';') {
        return Err(QueryError::InvalidSql(
            "multiple statements not allowed".to_string(),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_sql_select() {
        assert!(validate_sql("SELECT * FROM events").is_ok());
        assert!(validate_sql("  SELECT count(*) FROM logs  ").is_ok());
        assert!(validate_sql("select * from events").is_ok());
    }

    #[test]
    fn test_validate_sql_with() {
        assert!(validate_sql("WITH cte AS (SELECT 1) SELECT * FROM cte").is_ok());
        assert!(validate_sql("with x as (select 1) select * from x").is_ok());
    }

    #[test]
    fn test_validate_sql_invalid() {
        assert!(validate_sql("INSERT INTO events VALUES (1)").is_err());
        assert!(validate_sql("DELETE FROM events").is_err());
        assert!(validate_sql("DROP TABLE events").is_err());
        assert!(validate_sql("UPDATE events SET x=1").is_err());
        assert!(validate_sql("TRUNCATE TABLE events").is_err());
        assert!(validate_sql("ALTER TABLE events ADD COLUMN x INT").is_err());
        assert!(validate_sql("CREATE TABLE foo (id INT)").is_err());
    }

    #[test]
    fn test_validate_sql_multiple_statements() {
        assert!(validate_sql("SELECT 1; DROP TABLE events").is_err());
        assert!(validate_sql("SELECT 1; SELECT 2").is_err());
    }

    #[test]
    fn test_validate_sql_trailing_semicolon_ok() {
        assert!(validate_sql("SELECT * FROM events;").is_ok());
    }

    #[test]
    fn test_validate_sql_select_into_blocked() {
        // SELECT INTO can create tables in some databases
        assert!(validate_sql("SELECT * INTO new_table FROM events").is_err());
        assert!(validate_sql("select * into backup from logs").is_err());
    }

    #[test]
    fn test_validate_sql_subqueries_ok() {
        // Subqueries should be allowed
        assert!(validate_sql("SELECT * FROM (SELECT 1 as x) sub").is_ok());
        assert!(validate_sql("SELECT * FROM events WHERE id IN (SELECT id FROM logs)").is_ok());
    }

    #[test]
    fn test_validate_sql_case_insensitive() {
        assert!(validate_sql("Select * From events").is_ok());
        assert!(validate_sql("WITH CTE AS (Select 1) Select * From CTE").is_ok());
    }

    #[test]
    fn test_validate_sql_comments_ok() {
        // Trailing comments are fine
        assert!(validate_sql("SELECT * FROM events -- comment").is_ok());
        // Leading comments would fail (doesn't start with SELECT) - that's ok
        // Users can just remove the comment or put it after SELECT
    }
}
