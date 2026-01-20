//! Log volume metric
//!
//! Log counts over time from `logs_v1`.

use async_trait::async_trait;

use crate::builder::QueryBuilder;
use crate::error::Result;
use crate::filter::{Condition, Filter};
use crate::metrics::{Metric, parse_timeseries, table_name};
use crate::timeseries::TimeSeriesData;
use tell_query::QueryBackend;

/// Log volume metric
pub struct LogVolumeMetric {
    /// Optional log level filter
    level: Option<String>,
    /// Optional breakdown field
    breakdown: Option<String>,
}

impl LogVolumeMetric {
    /// Create a new log volume metric
    pub fn new(level: Option<String>) -> Self {
        Self {
            level,
            breakdown: None,
        }
    }

    /// Create metric for all logs
    pub fn all() -> Self {
        Self::new(None)
    }

    /// Create metric for a specific log level
    pub fn for_level(level: impl Into<String>) -> Self {
        Self::new(Some(level.into()))
    }

    /// Add a breakdown dimension
    pub fn with_breakdown(mut self, field: impl Into<String>) -> Self {
        self.breakdown = Some(field.into());
        self
    }

    pub(crate) fn build_query(&self, filter: &Filter, workspace_id: u64) -> String {
        let table = table_name(workspace_id, "logs_v1");

        let mut filter = filter.clone();
        if let Some(level) = &self.level {
            filter = filter.with_condition(Condition::eq("level", level.clone()));
        }

        let mut builder = QueryBuilder::new(table)
            .with_time_bucket(filter.granularity, "timestamp", "date")
            .select_as("COUNT(*)", "value")
            .apply_filter(&filter, "timestamp");

        if let Some(breakdown) = &self.breakdown {
            builder = builder.with_breakdown(breakdown);
        }

        builder.build()
    }
}

#[async_trait]
impl Metric for LogVolumeMetric {
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
        "log_volume"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filter::Granularity;
    use crate::timerange::TimeRange;

    #[test]
    fn test_log_volume_all() {
        let range = TimeRange::parse("7d").unwrap();
        let filter = Filter::new(range).with_granularity(Granularity::Daily);

        let metric = LogVolumeMetric::all();
        let sql = metric.build_query(&filter, 1);

        assert!(sql.contains("1.logs_v1"));
        assert!(sql.contains("COUNT(*) AS value"));
        assert!(sql.contains("toDate(timestamp) AS date"));
    }

    #[test]
    fn test_log_volume_with_level() {
        let range = TimeRange::parse("7d").unwrap();
        let filter = Filter::new(range).with_granularity(Granularity::Daily);

        let metric = LogVolumeMetric::for_level("error");
        let sql = metric.build_query(&filter, 1);

        assert!(sql.contains("level = 'error'"));
    }

    #[test]
    fn test_log_volume_with_breakdown() {
        let range = TimeRange::parse("7d").unwrap();
        let filter = Filter::new(range).with_granularity(Granularity::Daily);

        let metric = LogVolumeMetric::all().with_breakdown("level");
        let sql = metric.build_query(&filter, 1);

        assert!(sql.contains("level"));
        assert!(sql.contains("GROUP BY date, level"));
    }
}
