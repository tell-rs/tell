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
            .with_time_bucket(filter.granularity, "timestamp", "date")
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
}
