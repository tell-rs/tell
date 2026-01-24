//! Query builder for generating SQL from filters
//!
//! Builds ClickHouse-compatible SQL queries with support for:
//! - Time range filtering
//! - Conditions (WHERE clauses)
//! - Granularity-based aggregation (or rolling window for single values)
//! - Breakdown dimensions (GROUP BY)
//! - JSON field access (properties.field)
//! - IPv6 normalization for source_ip

use crate::filter::{Condition, ConditionValue, Filter, Granularity, Operator};

/// Query builder for analytics SQL
pub struct QueryBuilder {
    table: String,
    select: Vec<String>,
    where_clauses: Vec<String>,
    group_by: Vec<String>,
    order_by: Vec<String>,
    limit: Option<u32>,
}

impl QueryBuilder {
    /// Create a new query builder for a table
    pub fn new(table: impl Into<String>) -> Self {
        Self {
            table: table.into(),
            select: Vec::new(),
            where_clauses: Vec::new(),
            group_by: Vec::new(),
            order_by: Vec::new(),
            limit: None,
        }
    }

    /// Add a SELECT column
    pub fn select(mut self, column: impl Into<String>) -> Self {
        self.select.push(column.into());
        self
    }

    /// Add a SELECT column with alias
    pub fn select_as(mut self, expr: impl Into<String>, alias: impl Into<String>) -> Self {
        self.select
            .push(format!("{} AS {}", expr.into(), alias.into()));
        self
    }

    /// Add a WHERE clause
    pub fn where_clause(mut self, clause: impl Into<String>) -> Self {
        self.where_clauses.push(clause.into());
        self
    }

    /// Add a GROUP BY column
    pub fn group_by(mut self, column: impl Into<String>) -> Self {
        self.group_by.push(column.into());
        self
    }

    /// Add an ORDER BY column
    pub fn order_by(mut self, column: impl Into<String>) -> Self {
        self.order_by.push(column.into());
        self
    }

    /// Add ORDER BY with direction
    pub fn order_by_desc(mut self, column: impl Into<String>) -> Self {
        self.order_by.push(format!("{} DESC", column.into()));
        self
    }

    /// Set LIMIT
    pub fn limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Apply a filter to this query
    pub fn apply_filter(mut self, filter: &Filter, timestamp_col: &str) -> Self {
        // Add time range condition
        let start = filter
            .time_range
            .start
            .format("%Y-%m-%d %H:%M:%S")
            .to_string();
        let end = filter
            .time_range
            .end
            .format("%Y-%m-%d %H:%M:%S")
            .to_string();
        self.where_clauses.push(format!(
            "{} >= '{}' AND {} <= '{}'",
            timestamp_col, start, timestamp_col, end
        ));

        // Add conditions
        for condition in &filter.conditions {
            if let Some(clause) = condition_to_sql(condition) {
                self.where_clauses.push(clause);
            }
        }

        // Apply limit from filter
        if let Some(limit) = filter.limit {
            self.limit = Some(limit);
        }

        self
    }

    /// Add time bucket SELECT and GROUP BY for granularity
    pub fn with_time_bucket(
        mut self,
        granularity: Granularity,
        timestamp_col: &str,
        alias: &str,
    ) -> Self {
        let fn_name = granularity.clickhouse_fn();
        self.select
            .insert(0, format!("{}({}) AS {}", fn_name, timestamp_col, alias));
        self.group_by.insert(0, alias.to_string());
        self.order_by.insert(0, alias.to_string());
        self
    }

    /// Optionally add time bucket if granularity is set
    ///
    /// For rolling window mode (None granularity), no time bucketing is added,
    /// resulting in a single aggregate value.
    pub fn with_optional_time_bucket(
        self,
        granularity: Option<Granularity>,
        timestamp_col: &str,
        alias: &str,
    ) -> Self {
        match granularity {
            Some(g) => self.with_time_bucket(g, timestamp_col, alias),
            None => self, // Rolling window - no time bucketing
        }
    }

    /// Add breakdown dimension
    pub fn with_breakdown(mut self, field: &str) -> Self {
        self.select.push(field.to_string());
        self.group_by.push(field.to_string());
        self
    }

    /// Build the final SQL query
    ///
    /// Debug logging can be enabled via RUST_LOG=tell_analytics=debug
    pub fn build(self) -> String {
        let mut sql = String::new();

        // SELECT
        sql.push_str("SELECT ");
        if self.select.is_empty() {
            sql.push('*');
        } else {
            sql.push_str(&self.select.join(", "));
        }

        // FROM
        sql.push_str(" FROM ");
        sql.push_str(&self.table);

        // WHERE
        if !self.where_clauses.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&self.where_clauses.join(" AND "));
        }

        // GROUP BY
        if !self.group_by.is_empty() {
            sql.push_str(" GROUP BY ");
            sql.push_str(&self.group_by.join(", "));
        }

        // ORDER BY
        if !self.order_by.is_empty() {
            sql.push_str(" ORDER BY ");
            sql.push_str(&self.order_by.join(", "));
        }

        // LIMIT
        if let Some(limit) = self.limit {
            sql.push_str(&format!(" LIMIT {}", limit));
        }

        // Debug logging for SQL queries
        tracing::debug!(
            sql = %sql,
            table = %self.table,
            "analytics query built"
        );

        sql
    }
}

/// Convert a Condition to a SQL WHERE clause
fn condition_to_sql(condition: &Condition) -> Option<String> {
    let field_name = &condition.field;

    // Special handling for source_ip field (IPv6 normalization)
    if field_name == "source_ip" {
        return condition_to_sql_ipv6(condition);
    }

    let field = format_field(field_name);

    match (&condition.operator, &condition.value) {
        (Operator::Eq, ConditionValue::Single(v)) => {
            Some(format!("{} = '{}'", field, escape_string(v)))
        }
        (Operator::Ne, ConditionValue::Single(v)) => {
            Some(format!("{} != '{}'", field, escape_string(v)))
        }
        (Operator::Gt, ConditionValue::Single(v)) => {
            Some(format!("{} > '{}'", field, escape_string(v)))
        }
        (Operator::Gte, ConditionValue::Single(v)) => {
            Some(format!("{} >= '{}'", field, escape_string(v)))
        }
        (Operator::Lt, ConditionValue::Single(v)) => {
            Some(format!("{} < '{}'", field, escape_string(v)))
        }
        (Operator::Lte, ConditionValue::Single(v)) => {
            Some(format!("{} <= '{}'", field, escape_string(v)))
        }
        (Operator::Contains, ConditionValue::Single(v)) => {
            Some(format!("{} LIKE '%{}%'", field, escape_like(v)))
        }
        (Operator::NotContains, ConditionValue::Single(v)) => {
            Some(format!("{} NOT LIKE '%{}%'", field, escape_like(v)))
        }
        (Operator::StartsWith, ConditionValue::Single(v)) => {
            Some(format!("{} LIKE '{}%'", field, escape_like(v)))
        }
        (Operator::EndsWith, ConditionValue::Single(v)) => {
            Some(format!("{} LIKE '%{}'", field, escape_like(v)))
        }
        (Operator::In, ConditionValue::Multiple(values)) => {
            let escaped: Vec<String> = values.iter().map(|v| escape_string(v)).collect();
            Some(format!("{} IN ('{}')", field, escaped.join("', '")))
        }
        (Operator::NotIn, ConditionValue::Multiple(values)) => {
            let escaped: Vec<String> = values.iter().map(|v| escape_string(v)).collect();
            Some(format!("{} NOT IN ('{}')", field, escaped.join("', '")))
        }
        (Operator::IsSet, _) => Some(format!("{} IS NOT NULL", field)),
        (Operator::IsNotSet, _) => Some(format!("{} IS NULL", field)),
        (Operator::Regex, ConditionValue::Single(v)) => {
            Some(format!("match({}, '{}')", field, escape_string(v)))
        }
        _ => None,
    }
}

/// Handle IPv6 source_ip field with proper normalization
///
/// ClickHouse stores IPs as FixedString(16) in IPv6 format, so we need to use
/// IPv6StringToNum() for comparison.
fn condition_to_sql_ipv6(condition: &Condition) -> Option<String> {
    match (&condition.operator, &condition.value) {
        (Operator::Eq, ConditionValue::Single(v)) => {
            let ip = normalize_ip(v);
            Some(format!(
                "source_ip = IPv6StringToNum('{}')",
                escape_string(&ip)
            ))
        }
        (Operator::Ne, ConditionValue::Single(v)) => {
            let ip = normalize_ip(v);
            Some(format!(
                "source_ip != IPv6StringToNum('{}')",
                escape_string(&ip)
            ))
        }
        (Operator::In, ConditionValue::Multiple(values)) => {
            let ips: Vec<String> = values
                .iter()
                .map(|v| format!("IPv6StringToNum('{}')", escape_string(&normalize_ip(v))))
                .collect();
            Some(format!("source_ip IN ({})", ips.join(", ")))
        }
        (Operator::NotIn, ConditionValue::Multiple(values)) => {
            let ips: Vec<String> = values
                .iter()
                .map(|v| format!("IPv6StringToNum('{}')", escape_string(&normalize_ip(v))))
                .collect();
            Some(format!("source_ip NOT IN ({})", ips.join(", ")))
        }
        (Operator::IsSet, _) => Some("source_ip IS NOT NULL".to_string()),
        (Operator::IsNotSet, _) => Some("source_ip IS NULL".to_string()),
        _ => None, // Other operators don't make sense for IPs
    }
}

/// Normalize an IP address to IPv6 format for ClickHouse
///
/// IPv4 addresses are converted to IPv4-mapped IPv6 format (::ffff:x.x.x.x)
fn normalize_ip(ip: &str) -> String {
    let ip = ip.trim();
    // If it's an IPv4 address (contains dots, no colons), convert to IPv4-mapped IPv6
    if ip.contains('.') && !ip.contains(':') {
        format!("::ffff:{}", ip)
    } else {
        ip.to_string()
    }
}

/// Format a field name for SQL, handling JSON path access
///
/// - Simple field: `event_name` → `event_name`
/// - JSON path: `properties.page` → `JSONExtractString(properties, 'page')`
/// - Nested JSON: `properties.user.id` → `JSONExtractString(properties, 'user', 'id')`
fn format_field(field: &str) -> String {
    if field.contains('.') {
        // JSON path access
        let parts: Vec<&str> = field.splitn(2, '.').collect();
        if parts.len() == 2 {
            let base = escape_identifier(parts[0]);
            let path_parts: Vec<&str> = parts[1].split('.').collect();
            let path_args: String = path_parts
                .iter()
                .map(|p| format!("'{}'", escape_string(p)))
                .collect::<Vec<_>>()
                .join(", ");
            // Use toString() wrapper for ClickHouse JSON compatibility
            format!("toString(JSONExtractString({}, {}))", base, path_args)
        } else {
            escape_identifier(field)
        }
    } else {
        escape_identifier(field)
    }
}

/// Escape a string value for SQL (prevent injection)
fn escape_string(s: &str) -> String {
    s.replace('\'', "''").replace('\\', "\\\\")
}

/// Escape identifier (column/table name)
fn escape_identifier(s: &str) -> String {
    // Only allow alphanumeric and underscore
    if s.chars().all(|c| c.is_alphanumeric() || c == '_') {
        s.to_string()
    } else {
        // Quote with backticks for safety
        format!("`{}`", s.replace('`', "``"))
    }
}

/// Escape LIKE pattern special characters
fn escape_like(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
        .replace('\'', "''")
}

/// Build a count distinct query
///
/// Supports both time series (with granularity) and rolling window (without) modes.
pub fn count_distinct_query(
    table: &str,
    distinct_col: &str,
    filter: &Filter,
    timestamp_col: &str,
) -> String {
    QueryBuilder::new(table)
        .with_optional_time_bucket(filter.granularity, timestamp_col, "date")
        .select_as(format!("COUNT(DISTINCT {})", distinct_col), "value")
        .apply_filter(filter, timestamp_col)
        .build()
}

/// Build a count query
///
/// Supports both time series (with granularity) and rolling window (without) modes.
pub fn count_query(table: &str, filter: &Filter, timestamp_col: &str) -> String {
    QueryBuilder::new(table)
        .with_optional_time_bucket(filter.granularity, timestamp_col, "date")
        .select_as("COUNT(*)", "value")
        .apply_filter(filter, timestamp_col)
        .build()
}

/// Build a top N query (count grouped by a field)
pub fn top_n_query(
    table: &str,
    group_field: &str,
    filter: &Filter,
    timestamp_col: &str,
    limit: u32,
) -> String {
    QueryBuilder::new(table)
        .select(group_field)
        .select_as("COUNT(*)", "count")
        .apply_filter(filter, timestamp_col)
        .group_by(group_field)
        .order_by_desc("count")
        .limit(limit)
        .build()
}

/// Build a query with breakdown dimension
///
/// Supports both time series (with granularity) and rolling window (without) modes.
pub fn breakdown_query(
    table: &str,
    agg_expr: &str,
    breakdown_field: &str,
    filter: &Filter,
    timestamp_col: &str,
) -> String {
    QueryBuilder::new(table)
        .with_optional_time_bucket(filter.granularity, timestamp_col, "date")
        .select(breakdown_field)
        .select_as(agg_expr, "value")
        .apply_filter(filter, timestamp_col)
        .group_by(breakdown_field)
        .build()
}

/// Build a raw data query (no aggregation)
///
/// Returns row-level data with specified columns filtered by the filter's time range
/// and conditions. Used for drill-down into aggregated metrics.
pub fn raw_data_query(
    table: &str,
    columns: &[&str],
    filter: &Filter,
    timestamp_col: &str,
    limit: u32,
) -> String {
    let mut builder = QueryBuilder::new(table)
        .apply_filter(filter, timestamp_col)
        .order_by_desc(timestamp_col)
        .limit(limit);

    for col in columns {
        builder = builder.select(*col);
    }

    builder.build()
}

/// Build a distinct values query
///
/// Returns distinct values for a column within the filter's time range.
/// Useful for listing unique users, events, etc.
pub fn distinct_values_query(
    table: &str,
    distinct_col: &str,
    filter: &Filter,
    timestamp_col: &str,
    limit: u32,
) -> String {
    QueryBuilder::new(table)
        .select(format!("DISTINCT {}", distinct_col))
        .apply_filter(filter, timestamp_col)
        .order_by(distinct_col)
        .limit(limit)
        .build()
}
