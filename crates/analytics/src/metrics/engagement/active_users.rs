//! Active users metrics (DAU, WAU, MAU)
//!
//! Counts distinct devices/users over time periods from `context_v1`.

use async_trait::async_trait;

use crate::builder::QueryBuilder;
use crate::error::Result;
use crate::filter::Filter;
use crate::metrics::{parse_timeseries, table_name, Metric};
use crate::timeseries::TimeSeriesData;
use tell_query::QueryBackend;

/// Type of active users metric
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActiveUsersType {
    /// Daily active users
    Daily,
    /// Weekly active users
    Weekly,
    /// Monthly active users
    Monthly,
}

impl ActiveUsersType {
    fn name(&self) -> &'static str {
        match self {
            Self::Daily => "dau",
            Self::Weekly => "wau",
            Self::Monthly => "mau",
        }
    }
}

/// Active users metric
pub struct ActiveUsersMetric {
    metric_type: ActiveUsersType,
    breakdown: Option<String>,
}

impl ActiveUsersMetric {
    /// Create a new active users metric
    pub fn new(metric_type: ActiveUsersType) -> Self {
        Self {
            metric_type,
            breakdown: None,
        }
    }

    /// Add a breakdown dimension
    pub fn with_breakdown(mut self, field: impl Into<String>) -> Self {
        self.breakdown = Some(field.into());
        self
    }

    pub(crate) fn build_query(&self, filter: &Filter, workspace_id: u64) -> String {
        let table = table_name(workspace_id, "context_v1");

        let mut builder = QueryBuilder::new(table)
            .with_time_bucket(filter.granularity, "timestamp", "date")
            .select_as("COUNT(DISTINCT device_id)", "value")
            .apply_filter(filter, "timestamp");

        if let Some(breakdown) = &self.breakdown {
            builder = builder.with_breakdown(breakdown);
        }

        builder.build()
    }
}

#[async_trait]
impl Metric for ActiveUsersMetric {
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
    fn test_dau_query() {
        let range = TimeRange::parse("7d").unwrap();
        let filter = Filter::new(range).with_granularity(Granularity::Daily);

        let metric = ActiveUsersMetric::new(ActiveUsersType::Daily);
        let sql = metric.build_query(&filter, 1);

        assert!(sql.contains("1.context_v1"));
        assert!(sql.contains("COUNT(DISTINCT device_id) AS value"));
        assert!(sql.contains("toDate(timestamp) AS date"));
        assert!(sql.contains("GROUP BY date"));
    }

    #[test]
    fn test_dau_with_breakdown() {
        let range = TimeRange::parse("7d").unwrap();
        let filter = Filter::new(range).with_granularity(Granularity::Daily);

        let metric = ActiveUsersMetric::new(ActiveUsersType::Daily).with_breakdown("device_type");
        let sql = metric.build_query(&filter, 1);

        assert!(sql.contains("device_type"));
        assert!(sql.contains("GROUP BY date, device_type"));
    }
}
