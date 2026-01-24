//! Event count metrics
//!
//! Event volume over time from `events_v1`.

use async_trait::async_trait;

use crate::builder::QueryBuilder;
use crate::error::Result;
use crate::filter::{Condition, Filter};
use crate::metrics::{Metric, parse_timeseries, table_name};
use crate::timeseries::TimeSeriesData;
use tell_query::QueryBackend;

/// Event count metric
pub struct EventCountMetric {
    /// Optional event name filter
    event_name: Option<String>,
    /// Optional breakdown field
    breakdown: Option<String>,
}

impl EventCountMetric {
    /// Create a new event count metric (all events)
    pub fn new(event_name: Option<String>) -> Self {
        Self {
            event_name,
            breakdown: None,
        }
    }

    /// Create metric for all events
    pub fn all() -> Self {
        Self::new(None)
    }

    /// Create metric for a specific event
    pub fn for_event(name: impl Into<String>) -> Self {
        Self::new(Some(name.into()))
    }

    /// Add a breakdown dimension
    pub fn with_breakdown(mut self, field: impl Into<String>) -> Self {
        self.breakdown = Some(field.into());
        self
    }

    pub(crate) fn build_query(&self, filter: &Filter, workspace_id: u64) -> String {
        let table = table_name(workspace_id, "events_v1");

        let mut filter = filter.clone();
        if let Some(event_name) = &self.event_name {
            filter = filter.with_condition(Condition::eq("event_name", event_name.clone()));
        }

        let mut builder = QueryBuilder::new(table)
            .with_optional_time_bucket(filter.granularity, "timestamp", "date")
            .select_as("COUNT(*)", "value")
            .apply_filter(&filter, "timestamp");

        if let Some(breakdown) = &self.breakdown {
            builder = builder.with_breakdown(breakdown);
        }

        builder.build()
    }
}

#[async_trait]
impl Metric for EventCountMetric {
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
        "event_count"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filter::Granularity;
    use crate::timerange::TimeRange;

    #[test]
    fn test_event_count_all() {
        let range = TimeRange::parse("7d").unwrap();
        let filter = Filter::new(range).with_granularity(Granularity::Daily);

        let metric = EventCountMetric::all();
        let sql = metric.build_query(&filter, 1);

        assert!(sql.contains("1.events_v1"));
        assert!(sql.contains("COUNT(*) AS value"));
        assert!(sql.contains("toDate(timestamp) AS date"));
    }

    #[test]
    fn test_event_count_filtered() {
        let range = TimeRange::parse("7d").unwrap();
        let filter = Filter::new(range).with_granularity(Granularity::Daily);

        let metric = EventCountMetric::for_event("page_view");
        let sql = metric.build_query(&filter, 1);

        assert!(sql.contains("event_name = 'page_view'"));
    }

    #[test]
    fn test_event_count_with_breakdown() {
        let range = TimeRange::parse("7d").unwrap();
        let filter = Filter::new(range).with_granularity(Granularity::Daily);

        let metric = EventCountMetric::all().with_breakdown("event_name");
        let sql = metric.build_query(&filter, 1);

        assert!(sql.contains("event_name"));
        assert!(sql.contains("GROUP BY date, event_name"));
    }
}
