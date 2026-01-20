//! Top events metric
//!
//! Most frequent events from `events_v1`.

use async_trait::async_trait;

use crate::builder::top_n_query;
use crate::error::Result;
use crate::filter::Filter;
use crate::metrics::{table_name, Metric};
use crate::timeseries::{TimeSeriesData, TimeSeriesPoint};
use tell_query::QueryBackend;

/// Top events metric
pub struct TopEventsMetric {
    /// Number of events to return
    limit: u32,
}

impl TopEventsMetric {
    /// Create a new top events metric
    pub fn new(limit: u32) -> Self {
        Self { limit }
    }

    pub(crate) fn build_query(&self, filter: &Filter, workspace_id: u64) -> String {
        let table = table_name(workspace_id, "events_v1");
        top_n_query(&table, "event_name", filter, "timestamp", self.limit)
    }
}

#[async_trait]
impl Metric for TopEventsMetric {
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
                let count = row.get(1)?.as_f64().or_else(|| row.get(1)?.as_i64().map(|i| i as f64))?;
                Some(TimeSeriesPoint::with_dimension(name, count, name))
            })
            .collect();

        Ok(TimeSeriesData::from_points(points))
    }

    fn name(&self) -> &'static str {
        "top_events"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::timerange::TimeRange;

    #[test]
    fn test_top_events_query() {
        let range = TimeRange::parse("7d").unwrap();
        let filter = Filter::new(range);

        let metric = TopEventsMetric::new(10);
        let sql = metric.build_query(&filter, 1);

        assert!(sql.contains("SELECT event_name, COUNT(*) AS count"));
        assert!(sql.contains("1.events_v1"));
        assert!(sql.contains("GROUP BY event_name"));
        assert!(sql.contains("ORDER BY count DESC"));
        assert!(sql.contains("LIMIT 10"));
    }
}
