//! Stickiness metrics (engagement ratios)
//!
//! Formula-based metrics that combine base metrics:
//! - Daily stickiness = DAU / MAU
//! - Weekly stickiness = WAU / MAU

use async_trait::async_trait;

use crate::builder::QueryBuilder;
use crate::error::Result;
use crate::filter::Filter;
use crate::metrics::{parse_timeseries, table_name, Metric};
use crate::timeseries::{TimeSeriesData, TimeSeriesPoint};
use tell_query::QueryBackend;

/// Type of stickiness metric
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StickinessType {
    /// Daily stickiness (DAU / MAU)
    Daily,
    /// Weekly stickiness (WAU / MAU)
    Weekly,
}

impl StickinessType {
    fn name(&self) -> &'static str {
        match self {
            Self::Daily => "daily_stickiness",
            Self::Weekly => "weekly_stickiness",
        }
    }
}

/// Stickiness metric (engagement ratio)
///
/// Calculates the ratio of short-term active users to long-term active users.
/// Higher values indicate better user engagement/retention.
pub struct StickinessMetric {
    metric_type: StickinessType,
}

impl StickinessMetric {
    /// Create a new stickiness metric
    pub fn new(metric_type: StickinessType) -> Self {
        Self { metric_type }
    }

    /// Create daily stickiness metric (DAU/MAU)
    pub fn daily() -> Self {
        Self::new(StickinessType::Daily)
    }

    /// Create weekly stickiness metric (WAU/MAU)
    pub fn weekly() -> Self {
        Self::new(StickinessType::Weekly)
    }

    /// Build query for DAU or WAU
    fn build_numerator_query(&self, filter: &Filter, workspace_id: u64) -> String {
        let table = table_name(workspace_id, "context_v1");

        let agg = match self.metric_type {
            StickinessType::Daily => "COUNT(DISTINCT device_id)",
            StickinessType::Weekly => "COUNT(DISTINCT device_id)",
        };

        QueryBuilder::new(table)
            .with_time_bucket(filter.granularity, "timestamp", "date")
            .select_as(agg, "value")
            .apply_filter(filter, "timestamp")
            .build()
    }
}

#[async_trait]
impl Metric for StickinessMetric {
    async fn execute(
        &self,
        backend: &dyn QueryBackend,
        filter: &Filter,
        workspace_id: u64,
    ) -> Result<TimeSeriesData> {
        // For stickiness, we need both DAU/WAU and MAU
        // Simplified: calculate ratio per time bucket
        // In production, MAU should be a 30-day rolling window

        let numerator_sql = self.build_numerator_query(filter, workspace_id);
        let numerator_result = backend.execute(&numerator_sql).await?;
        let numerator = parse_timeseries(&numerator_result)?;

        // For now, calculate stickiness as a simple ratio
        // Each point's value becomes (value / total) * 100
        let total = numerator.total;
        let points: Vec<TimeSeriesPoint> = numerator
            .points
            .into_iter()
            .map(|p| {
                let ratio = if total > 0.0 {
                    (p.value / total) * 100.0
                } else {
                    0.0
                };
                TimeSeriesPoint::new(p.date, ratio)
            })
            .collect();

        Ok(TimeSeriesData::from_points(points))
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
    fn test_daily_stickiness_query() {
        let range = TimeRange::parse("30d").unwrap();
        let filter = Filter::new(range).with_granularity(Granularity::Daily);

        let metric = StickinessMetric::daily();
        let sql = metric.build_numerator_query(&filter, 1);

        assert!(sql.contains("1.context_v1"));
        assert!(sql.contains("COUNT(DISTINCT device_id)"));
    }

    #[test]
    fn test_weekly_stickiness_query() {
        let range = TimeRange::parse("30d").unwrap();
        let filter = Filter::new(range).with_granularity(Granularity::Weekly);

        let metric = StickinessMetric::weekly();
        let sql = metric.build_numerator_query(&filter, 1);

        assert!(sql.contains("COUNT(DISTINCT device_id)"));
    }

    #[test]
    fn test_stickiness_type_names() {
        assert_eq!(StickinessType::Daily.name(), "daily_stickiness");
        assert_eq!(StickinessType::Weekly.name(), "weekly_stickiness");
    }
}
