//! Top logs metric
//!
//! Most frequent logs by level, source, etc. from `logs_v1`.

use async_trait::async_trait;

use crate::builder::top_n_query;
use crate::error::Result;
use crate::filter::Filter;
use crate::metrics::{Metric, table_name};
use crate::timeseries::{TimeSeriesData, TimeSeriesPoint};
use tell_query::QueryBackend;

/// Top logs metric
pub struct TopLogsMetric {
    /// Field to group by (e.g., "level", "source")
    group_field: String,
    /// Number of entries to return
    limit: u32,
}

impl TopLogsMetric {
    /// Create a new top logs metric
    pub fn new(group_field: impl Into<String>, limit: u32) -> Self {
        Self {
            group_field: group_field.into(),
            limit,
        }
    }

    /// Create top logs by level
    pub fn by_level(limit: u32) -> Self {
        Self::new("level", limit)
    }

    /// Create top logs by source
    pub fn by_source(limit: u32) -> Self {
        Self::new("source", limit)
    }

    pub(crate) fn build_query(&self, filter: &Filter, workspace_id: u64) -> String {
        let table = table_name(workspace_id, "logs_v1");
        top_n_query(&table, &self.group_field, filter, "timestamp", self.limit)
    }
}

#[async_trait]
impl Metric for TopLogsMetric {
    async fn execute(
        &self,
        backend: &dyn QueryBackend,
        filter: &Filter,
        workspace_id: u64,
    ) -> Result<TimeSeriesData> {
        let sql = self.build_query(filter, workspace_id);
        let result = backend.execute(&sql).await?;

        // Convert top N results to time series format
        let points: Vec<TimeSeriesPoint> = result
            .rows
            .iter()
            .filter_map(|row| {
                let name = row.first()?.as_str()?;
                let count = row
                    .get(1)?
                    .as_f64()
                    .or_else(|| row.get(1)?.as_i64().map(|i| i as f64))?;
                Some(TimeSeriesPoint::with_dimension(name, count, name))
            })
            .collect();

        Ok(TimeSeriesData::from_points(points))
    }

    fn name(&self) -> &'static str {
        "top_logs"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::timerange::TimeRange;

    #[test]
    fn test_top_logs_by_level() {
        let range = TimeRange::parse("7d").unwrap();
        let filter = Filter::new(range);

        let metric = TopLogsMetric::by_level(5);
        let sql = metric.build_query(&filter, 1);

        assert!(sql.contains("SELECT level, COUNT(*) AS count"));
        assert!(sql.contains("1.logs_v1"));
        assert!(sql.contains("GROUP BY level"));
        assert!(sql.contains("ORDER BY count DESC"));
        assert!(sql.contains("LIMIT 5"));
    }

    #[test]
    fn test_top_logs_by_source() {
        let range = TimeRange::parse("7d").unwrap();
        let filter = Filter::new(range);

        let metric = TopLogsMetric::by_source(10);
        let sql = metric.build_query(&filter, 1);

        assert!(sql.contains("SELECT source, COUNT(*) AS count"));
        assert!(sql.contains("GROUP BY source"));
    }
}
