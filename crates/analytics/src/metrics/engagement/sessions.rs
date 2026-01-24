//! Session metrics
//!
//! Session volume and unique session counts from `context_v1`.

use async_trait::async_trait;

use crate::builder::QueryBuilder;
use crate::error::Result;
use crate::filter::Filter;
use crate::metrics::{Metric, parse_timeseries, table_name};
use crate::timeseries::TimeSeriesData;
use tell_query::QueryBackend;

/// Type of session metric
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionsType {
    /// Total session count (all records)
    Volume,
    /// Unique sessions (distinct session_id)
    Unique,
}

impl SessionsType {
    fn name(&self) -> &'static str {
        match self {
            Self::Volume => "session_volume",
            Self::Unique => "unique_sessions",
        }
    }

    fn aggregation(&self) -> &'static str {
        match self {
            Self::Volume => "COUNT(*)",
            Self::Unique => "COUNT(DISTINCT session_id)",
        }
    }
}

/// Session metric
pub struct SessionsMetric {
    metric_type: SessionsType,
    breakdown: Option<String>,
}

impl SessionsMetric {
    /// Create a new session metric
    pub fn new(metric_type: SessionsType) -> Self {
        Self {
            metric_type,
            breakdown: None,
        }
    }

    /// Create session volume metric
    pub fn volume() -> Self {
        Self::new(SessionsType::Volume)
    }

    /// Create unique sessions metric
    pub fn unique() -> Self {
        Self::new(SessionsType::Unique)
    }

    /// Add a breakdown dimension (device_type, os, country, etc.)
    pub fn with_breakdown(mut self, field: impl Into<String>) -> Self {
        self.breakdown = Some(field.into());
        self
    }

    pub(crate) fn build_query(&self, filter: &Filter, workspace_id: u64) -> String {
        let table = table_name(workspace_id, "context_v1");

        let mut builder = QueryBuilder::new(table)
            .with_optional_time_bucket(filter.granularity, "timestamp", "date")
            .select_as(self.metric_type.aggregation(), "value")
            .apply_filter(filter, "timestamp");

        if let Some(breakdown) = &self.breakdown {
            builder = builder.with_breakdown(breakdown);
        }

        builder.build()
    }
}

#[async_trait]
impl Metric for SessionsMetric {
    async fn execute(
        &self,
        backend: &dyn QueryBackend,
        filter: &Filter,
        workspace_id: u64,
    ) -> Result<TimeSeriesData> {
        let sql = self.build_query(filter, workspace_id);
        let result = backend.execute(&sql).await?;
        parse_timeseries(&result)
    }

    fn name(&self) -> &'static str {
        self.metric_type.name()
    }
}

// =============================================================================
// Top Sessions Metric
// =============================================================================

/// Top sessions grouped by a field (device_type, country, etc.)
pub struct TopSessionsMetric {
    /// Field to group by
    group_field: String,
    /// Number of entries to return
    limit: u32,
}

impl TopSessionsMetric {
    /// Create a new top sessions metric
    pub fn new(group_field: impl Into<String>, limit: u32) -> Self {
        Self {
            group_field: group_field.into(),
            limit,
        }
    }

    /// Create top sessions by device type
    pub fn by_device_type(limit: u32) -> Self {
        Self::new("device_type", limit)
    }

    /// Create top sessions by country
    pub fn by_country(limit: u32) -> Self {
        Self::new("country", limit)
    }

    /// Create top sessions by operating system
    pub fn by_os(limit: u32) -> Self {
        Self::new("operating_system", limit)
    }

    pub(crate) fn build_query(&self, filter: &Filter, workspace_id: u64) -> String {
        let table = table_name(workspace_id, "context_v1");
        crate::builder::top_n_query(&table, &self.group_field, filter, "timestamp", self.limit)
    }
}

#[async_trait]
impl Metric for TopSessionsMetric {
    async fn execute(
        &self,
        backend: &dyn QueryBackend,
        filter: &Filter,
        workspace_id: u64,
    ) -> Result<TimeSeriesData> {
        let sql = self.build_query(filter, workspace_id);
        let result = backend.execute(&sql).await?;

        // Convert top N results to time series format
        let points: Vec<crate::timeseries::TimeSeriesPoint> = result
            .rows
            .iter()
            .filter_map(|row| {
                let name = row.first()?.as_str()?;
                let count = row
                    .get(1)?
                    .as_f64()
                    .or_else(|| row.get(1)?.as_i64().map(|i| i as f64))?;
                Some(crate::timeseries::TimeSeriesPoint::with_dimension(
                    name, count, name,
                ))
            })
            .collect();

        Ok(TimeSeriesData::from_points(points))
    }

    fn name(&self) -> &'static str {
        "top_sessions"
    }
}

// =============================================================================
// Users Metric
// =============================================================================

/// User count metric (distinct users over time)
pub struct UsersMetric {
    /// Count type
    count_type: UsersCountType,
}

/// Type of user count
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UsersCountType {
    /// Count all user records
    Count,
    /// Count distinct device IDs
    Distinct,
}

impl UsersMetric {
    /// Create a user count metric (uses users_v1 table)
    pub fn count() -> Self {
        Self {
            count_type: UsersCountType::Count,
        }
    }

    /// Create a distinct users metric (uses context_v1 table)
    pub fn distinct() -> Self {
        Self {
            count_type: UsersCountType::Distinct,
        }
    }

    pub(crate) fn build_query(&self, filter: &Filter, workspace_id: u64) -> String {
        match self.count_type {
            UsersCountType::Count => {
                let table = table_name(workspace_id, "users_v1");
                QueryBuilder::new(table)
                    .with_optional_time_bucket(filter.granularity, "updated_at", "date")
                    .select_as("COUNT(*)", "value")
                    .apply_filter(filter, "updated_at")
                    .build()
            }
            UsersCountType::Distinct => {
                let table = table_name(workspace_id, "context_v1");
                QueryBuilder::new(table)
                    .with_optional_time_bucket(filter.granularity, "timestamp", "date")
                    .select_as("COUNT(DISTINCT device_id)", "value")
                    .apply_filter(filter, "timestamp")
                    .build()
            }
        }
    }
}

#[async_trait]
impl Metric for UsersMetric {
    async fn execute(
        &self,
        backend: &dyn QueryBackend,
        filter: &Filter,
        workspace_id: u64,
    ) -> Result<TimeSeriesData> {
        let sql = self.build_query(filter, workspace_id);
        let result = backend.execute(&sql).await?;
        parse_timeseries(&result)
    }

    fn name(&self) -> &'static str {
        match self.count_type {
            UsersCountType::Count => "users_count",
            UsersCountType::Distinct => "distinct_users",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filter::Granularity;
    use crate::timerange::TimeRange;

    #[test]
    fn test_session_volume_query() {
        let range = TimeRange::parse("7d").unwrap();
        let filter = Filter::new(range).with_granularity(Granularity::Daily);

        let metric = SessionsMetric::volume();
        let sql = metric.build_query(&filter, 1);

        assert!(sql.contains("1.context_v1"));
        assert!(sql.contains("COUNT(*) AS value"));
        assert!(sql.contains("toDate(timestamp) AS date"));
    }

    #[test]
    fn test_unique_sessions_query() {
        let range = TimeRange::parse("7d").unwrap();
        let filter = Filter::new(range).with_granularity(Granularity::Daily);

        let metric = SessionsMetric::unique();
        let sql = metric.build_query(&filter, 1);

        assert!(sql.contains("COUNT(DISTINCT session_id) AS value"));
    }

    #[test]
    fn test_sessions_with_breakdown() {
        let range = TimeRange::parse("7d").unwrap();
        let filter = Filter::new(range).with_granularity(Granularity::Daily);

        let metric = SessionsMetric::volume().with_breakdown("device_type");
        let sql = metric.build_query(&filter, 1);

        assert!(sql.contains("device_type"));
        assert!(sql.contains("GROUP BY date, device_type"));
    }

    #[test]
    fn test_top_sessions_query() {
        let range = TimeRange::parse("7d").unwrap();
        let filter = Filter::new(range);

        let metric = TopSessionsMetric::by_device_type(10);
        let sql = metric.build_query(&filter, 1);

        assert!(sql.contains("SELECT device_type"));
        assert!(sql.contains("GROUP BY device_type"));
        assert!(sql.contains("ORDER BY count DESC"));
        assert!(sql.contains("LIMIT 10"));
    }

    #[test]
    fn test_users_count_query() {
        let range = TimeRange::parse("7d").unwrap();
        let filter = Filter::new(range).with_granularity(Granularity::Daily);

        let metric = UsersMetric::count();
        let sql = metric.build_query(&filter, 1);

        assert!(sql.contains("1.users_v1"));
        assert!(sql.contains("COUNT(*)"));
    }
}
