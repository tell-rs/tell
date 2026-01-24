//! Metrics engine for analytics queries
//!
//! Provides high-level metrics organized by domain:
//!
//! - **engagement**: User and session metrics (DAU, WAU, MAU, sessions, stickiness)
//! - **product**: Product analytics (event counts, top events)
//! - **logs**: Log analytics (volume, top logs)

pub mod engagement;
pub mod logs;
pub mod product;

// Re-exports for convenience
pub use engagement::{
    ActiveUsersMetric, ActiveUsersType, SessionsMetric, SessionsType, StickinessMetric,
    StickinessType, TopSessionsMetric, UsersMetric,
};
pub use logs::{LogVolumeMetric, TopLogsMetric};
pub use product::{EventCountMetric, TopEventsMetric};

use async_trait::async_trait;

use crate::error::Result;
use crate::filter::Filter;
use crate::timeseries::TimeSeriesData;
use tell_query::QueryBackend;

/// A metric that can be executed against a query backend
#[async_trait]
pub trait Metric: Send + Sync {
    /// Execute this metric and return time series data
    async fn execute(
        &self,
        backend: &dyn QueryBackend,
        filter: &Filter,
        workspace_id: u64,
    ) -> Result<TimeSeriesData>;

    /// Get the metric name for logging/identification
    fn name(&self) -> &'static str;
}

/// Metrics engine for executing analytics queries
pub struct MetricsEngine {
    backend: Box<dyn QueryBackend>,
}

impl MetricsEngine {
    /// Create a new metrics engine with a backend
    pub fn new(backend: Box<dyn QueryBackend>) -> Self {
        Self { backend }
    }

    /// Get a reference to the underlying query backend
    ///
    /// Useful for executing raw queries outside of the metrics API.
    pub fn backend(&self) -> &dyn QueryBackend {
        self.backend.as_ref()
    }

    /// Execute a metric query
    pub async fn execute(
        &self,
        metric: &dyn Metric,
        filter: &Filter,
        workspace_id: u64,
    ) -> Result<TimeSeriesData> {
        metric
            .execute(self.backend.as_ref(), filter, workspace_id)
            .await
    }

    // Engagement metrics

    /// Get daily active users
    pub async fn dau(&self, filter: &Filter, workspace_id: u64) -> Result<TimeSeriesData> {
        let metric = ActiveUsersMetric::new(ActiveUsersType::Daily);
        self.execute(&metric, filter, workspace_id).await
    }

    /// Get weekly active users
    pub async fn wau(&self, filter: &Filter, workspace_id: u64) -> Result<TimeSeriesData> {
        let metric = ActiveUsersMetric::new(ActiveUsersType::Weekly);
        self.execute(&metric, filter, workspace_id).await
    }

    /// Get monthly active users
    pub async fn mau(&self, filter: &Filter, workspace_id: u64) -> Result<TimeSeriesData> {
        let metric = ActiveUsersMetric::new(ActiveUsersType::Monthly);
        self.execute(&metric, filter, workspace_id).await
    }

    /// Get session volume
    pub async fn sessions(&self, filter: &Filter, workspace_id: u64) -> Result<TimeSeriesData> {
        let metric = SessionsMetric::volume();
        self.execute(&metric, filter, workspace_id).await
    }

    /// Get unique sessions
    pub async fn unique_sessions(
        &self,
        filter: &Filter,
        workspace_id: u64,
    ) -> Result<TimeSeriesData> {
        let metric = SessionsMetric::unique();
        self.execute(&metric, filter, workspace_id).await
    }

    /// Get daily stickiness (DAU/MAU)
    pub async fn daily_stickiness(
        &self,
        filter: &Filter,
        workspace_id: u64,
    ) -> Result<TimeSeriesData> {
        let metric = StickinessMetric::daily();
        self.execute(&metric, filter, workspace_id).await
    }

    /// Get weekly stickiness (WAU/MAU)
    pub async fn weekly_stickiness(
        &self,
        filter: &Filter,
        workspace_id: u64,
    ) -> Result<TimeSeriesData> {
        let metric = StickinessMetric::weekly();
        self.execute(&metric, filter, workspace_id).await
    }

    // Product metrics

    /// Get event count time series
    pub async fn event_count(&self, filter: &Filter, workspace_id: u64) -> Result<TimeSeriesData> {
        let metric = EventCountMetric::all();
        self.execute(&metric, filter, workspace_id).await
    }

    /// Get event count for a specific event
    pub async fn event_count_by_name(
        &self,
        event_name: &str,
        filter: &Filter,
        workspace_id: u64,
    ) -> Result<TimeSeriesData> {
        let metric = EventCountMetric::for_event(event_name);
        self.execute(&metric, filter, workspace_id).await
    }

    /// Get top events
    pub async fn top_events(
        &self,
        filter: &Filter,
        workspace_id: u64,
        limit: u32,
    ) -> Result<TimeSeriesData> {
        let metric = TopEventsMetric::new(limit);
        self.execute(&metric, filter, workspace_id).await
    }

    // Log metrics

    /// Get log volume time series
    pub async fn log_volume(&self, filter: &Filter, workspace_id: u64) -> Result<TimeSeriesData> {
        let metric = LogVolumeMetric::all();
        self.execute(&metric, filter, workspace_id).await
    }

    /// Get log volume for a specific level
    pub async fn log_volume_by_level(
        &self,
        level: &str,
        filter: &Filter,
        workspace_id: u64,
    ) -> Result<TimeSeriesData> {
        let metric = LogVolumeMetric::for_level(level);
        self.execute(&metric, filter, workspace_id).await
    }

    /// Get top logs by level
    pub async fn top_logs_by_level(
        &self,
        filter: &Filter,
        workspace_id: u64,
        limit: u32,
    ) -> Result<TimeSeriesData> {
        let metric = TopLogsMetric::by_level(limit);
        self.execute(&metric, filter, workspace_id).await
    }

    /// Get the backend name
    pub fn backend_name(&self) -> &'static str {
        self.backend.name()
    }

    // Comparison support

    /// Execute a metric query with comparison to previous period
    ///
    /// When the filter has a compare mode set, this runs two queries:
    /// one for the current period and one for the comparison period,
    /// then combines them into a single result with comparison data.
    pub async fn execute_with_comparison(
        &self,
        metric: &dyn Metric,
        filter: &Filter,
        workspace_id: u64,
    ) -> Result<TimeSeriesData> {
        // Get current period data
        let current = metric
            .execute(self.backend.as_ref(), filter, workspace_id)
            .await?;

        // If no comparison mode, return current data only
        let compare_mode = match &filter.compare {
            Some(mode) => mode,
            None => return Ok(current),
        };

        // Build filter for comparison period
        let comparison_range = match compare_mode {
            crate::filter::CompareMode::Previous => filter.time_range.previous_period(),
            crate::filter::CompareMode::PreviousYear => filter.time_range.previous_year(),
        };

        let mut comparison_filter = Filter::new(comparison_range);

        // Copy granularity (if set)
        if let Some(granularity) = filter.granularity {
            comparison_filter = comparison_filter.with_granularity(granularity);
        } else {
            comparison_filter = comparison_filter.with_rolling_window();
        }

        // Copy conditions
        for condition in &filter.conditions {
            comparison_filter = comparison_filter.with_condition(condition.clone());
        }

        // Copy breakdown if present
        if let Some(breakdown) = &filter.breakdown {
            comparison_filter = comparison_filter.with_breakdown(breakdown.clone());
        }

        // Get comparison period data
        let previous = metric
            .execute(self.backend.as_ref(), &comparison_filter, workspace_id)
            .await?;

        // Build comparison data
        let comparison =
            crate::timeseries::ComparisonData::calculate(current.total, previous.total)
                .with_points(previous.points);

        Ok(current.with_comparison(comparison))
    }

    /// Execute DAU with comparison
    pub async fn dau_with_comparison(
        &self,
        filter: &Filter,
        workspace_id: u64,
    ) -> Result<TimeSeriesData> {
        let metric = ActiveUsersMetric::new(ActiveUsersType::Daily);
        self.execute_with_comparison(&metric, filter, workspace_id)
            .await
    }

    /// Execute WAU with comparison
    pub async fn wau_with_comparison(
        &self,
        filter: &Filter,
        workspace_id: u64,
    ) -> Result<TimeSeriesData> {
        let metric = ActiveUsersMetric::new(ActiveUsersType::Weekly);
        self.execute_with_comparison(&metric, filter, workspace_id)
            .await
    }

    /// Execute MAU with comparison
    pub async fn mau_with_comparison(
        &self,
        filter: &Filter,
        workspace_id: u64,
    ) -> Result<TimeSeriesData> {
        let metric = ActiveUsersMetric::new(ActiveUsersType::Monthly);
        self.execute_with_comparison(&metric, filter, workspace_id)
            .await
    }

    /// Execute event count with comparison
    pub async fn event_count_with_comparison(
        &self,
        filter: &Filter,
        workspace_id: u64,
    ) -> Result<TimeSeriesData> {
        let metric = EventCountMetric::all();
        self.execute_with_comparison(&metric, filter, workspace_id)
            .await
    }

    /// Execute log volume with comparison
    pub async fn log_volume_with_comparison(
        &self,
        filter: &Filter,
        workspace_id: u64,
    ) -> Result<TimeSeriesData> {
        let metric = LogVolumeMetric::all();
        self.execute_with_comparison(&metric, filter, workspace_id)
            .await
    }

    // Event property metrics

    /// Get event property breakdown over time
    ///
    /// Returns counts grouped by property values within events matching the filter.
    pub async fn event_property_breakdown(
        &self,
        event: &str,
        property: &str,
        filter: &Filter,
        workspace_id: u64,
    ) -> Result<TimeSeriesData> {
        let table = table_name(workspace_id, "events_v1");

        // Build filter with event condition
        let mut event_filter = filter.clone();
        event_filter =
            event_filter.with_condition(crate::filter::Condition::eq("event_name", event));

        // Format property for JSON extraction if needed
        let property_expr = if property.starts_with("properties.") {
            let path = property.strip_prefix("properties.").unwrap_or(property);
            format!("JSONExtractString(properties, '{}')", path)
        } else {
            property.to_string()
        };

        let sql = crate::builder::breakdown_query(
            &table,
            "COUNT(*)",
            &property_expr,
            &event_filter,
            "timestamp",
        );

        let result = self.backend.execute(&sql).await?;
        parse_timeseries(&result)
    }

    /// Get custom event metric aggregation
    ///
    /// Allows running custom aggregations (sum, avg, min, max) on event properties.
    pub async fn custom_event_metric(
        &self,
        event: &str,
        property: &str,
        metric_fn: &str,
        filter: &Filter,
        workspace_id: u64,
    ) -> Result<TimeSeriesData> {
        let table = table_name(workspace_id, "events_v1");

        // Build filter with event condition
        let mut event_filter = filter.clone();
        event_filter =
            event_filter.with_condition(crate::filter::Condition::eq("event_name", event));

        // Format property for JSON extraction if needed
        let property_expr = if property.starts_with("properties.") {
            let path = property.strip_prefix("properties.").unwrap_or(property);
            format!("toFloat64OrNull(JSONExtractString(properties, '{}'))", path)
        } else {
            property.to_string()
        };

        // Build aggregation expression
        let agg_expr = match metric_fn.to_lowercase().as_str() {
            "sum" => format!("sum({})", property_expr),
            "avg" => format!("avg({})", property_expr),
            "min" => format!("min({})", property_expr),
            "max" => format!("max({})", property_expr),
            "count" => format!("count({})", property_expr),
            _ => format!("sum({})", property_expr), // default to sum
        };

        let sql = crate::builder::QueryBuilder::new(&table)
            .with_optional_time_bucket(filter.granularity, "timestamp", "date")
            .select_as(&agg_expr, "value")
            .apply_filter(&event_filter, "timestamp")
            .build();

        let result = self.backend.execute(&sql).await?;
        parse_timeseries(&result)
    }

    // Drill-down queries (raw data)

    /// Drill down to raw user data
    ///
    /// Returns distinct device/user IDs within the filter's time range.
    /// Useful for seeing which users are counted in DAU/WAU/MAU.
    pub async fn drill_down_users(
        &self,
        filter: &Filter,
        workspace_id: u64,
        limit: u32,
    ) -> Result<tell_query::QueryResult> {
        let table = table_name(workspace_id, "context_v1");
        let sql =
            crate::builder::distinct_values_query(&table, "device_id", filter, "timestamp", limit);
        Ok(self.backend.execute(&sql).await?)
    }

    /// Drill down to raw event data
    ///
    /// Returns raw events within the filter's time range.
    pub async fn drill_down_events(
        &self,
        filter: &Filter,
        workspace_id: u64,
        limit: u32,
    ) -> Result<tell_query::QueryResult> {
        let table = table_name(workspace_id, "events_v1");
        let columns = &["timestamp", "event_name", "device_id", "session_id"];
        let sql = crate::builder::raw_data_query(&table, columns, filter, "timestamp", limit);
        Ok(self.backend.execute(&sql).await?)
    }

    /// Drill down to raw log data
    ///
    /// Returns raw logs within the filter's time range.
    pub async fn drill_down_logs(
        &self,
        filter: &Filter,
        workspace_id: u64,
        limit: u32,
    ) -> Result<tell_query::QueryResult> {
        let table = table_name(workspace_id, "logs_v1");
        let columns = &["timestamp", "level", "message", "source"];
        let sql = crate::builder::raw_data_query(&table, columns, filter, "timestamp", limit);
        Ok(self.backend.execute(&sql).await?)
    }

    /// Drill down to raw session data
    ///
    /// Returns distinct session IDs within the filter's time range.
    pub async fn drill_down_sessions(
        &self,
        filter: &Filter,
        workspace_id: u64,
        limit: u32,
    ) -> Result<tell_query::QueryResult> {
        let table = table_name(workspace_id, "context_v1");
        let sql =
            crate::builder::distinct_values_query(&table, "session_id", filter, "timestamp", limit);
        Ok(self.backend.execute(&sql).await?)
    }
}

/// Parse query results into TimeSeriesData
pub(crate) fn parse_timeseries(result: &tell_query::QueryResult) -> Result<TimeSeriesData> {
    use crate::timeseries::TimeSeriesPoint;

    let mut points = Vec::with_capacity(result.row_count);

    // Find date and value column indices
    let date_idx = result
        .columns
        .iter()
        .position(|c| c.name == "date" || c.name == "time" || c.name == "hour")
        .unwrap_or(0);

    let value_idx = result
        .columns
        .iter()
        .position(|c| c.name == "value" || c.name == "count")
        .unwrap_or(1);

    // Check for dimension column (breakdown)
    let dim_idx = result
        .columns
        .iter()
        .position(|c| !["date", "time", "hour", "value", "count"].contains(&c.name.as_str()));

    for row in &result.rows {
        let date = row
            .get(date_idx)
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let value = row
            .get(value_idx)
            .and_then(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
            .unwrap_or(0.0);

        let dimension =
            dim_idx.and_then(|idx| row.get(idx).and_then(|v| v.as_str().map(|s| s.to_string())));

        let point = if let Some(dim) = dimension {
            TimeSeriesPoint::with_dimension(date, value, dim)
        } else {
            TimeSeriesPoint::new(date, value)
        };

        points.push(point);
    }

    Ok(TimeSeriesData::from_points(points))
}

/// Format workspace-qualified table name
pub(crate) fn table_name(workspace_id: u64, table: &str) -> String {
    format!("{}.{}", workspace_id, table)
}
