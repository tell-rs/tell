//! Tell Analytics Engine
//!
//! High-level analytics for product metrics, events, and logs.
//!
//! # Overview
//!
//! This crate provides the analytics layer for Tell, built on top of `tell-query`.
//! It includes:
//!
//! - **Filters**: Time ranges, conditions, granularity, breakdowns
//! - **Query Builder**: SQL generation for ClickHouse
//! - **Metrics**: DAU, WAU, MAU, event counts, log volumes
//! - **Time Series**: Data structures for analytics results
//!
//! # Usage
//!
//! ```ignore
//! use tell_analytics::{Filter, MetricsEngine, TimeRange};
//!
//! // Parse a time range
//! let range = TimeRange::parse("30d")?;
//!
//! // Create a filter
//! let filter = Filter::new(range)
//!     .with_granularity(Granularity::Daily)
//!     .with_breakdown("device_type");
//!
//! // Execute metrics
//! let engine = MetricsEngine::new(backend);
//! let dau = engine.dau(&filter, workspace_id).await?;
//! ```
//!
//! # Comparison Support
//!
//! ```ignore
//! let filter = Filter::new(TimeRange::parse("30d")?)
//!     .with_compare(CompareMode::Previous);
//!
//! // Returns data with comparison to previous 30 days
//! let dau = engine.dau(&filter, workspace_id).await?;
//! ```

pub mod builder;
pub mod error;
pub mod filter;
pub mod metrics;
pub mod timerange;
pub mod timeseries;

#[cfg(test)]
mod builder_test;
#[cfg(test)]
mod filter_test;
#[cfg(test)]
mod timerange_test;
#[cfg(test)]
mod timeseries_test;

// Re-exports for convenience
pub use builder::QueryBuilder;
pub use error::{AnalyticsError, Result};
pub use filter::{
    CompareMode, Condition, ConditionValue, Filter, Granularity, MAX_LIMIT, Operator,
};
pub use metrics::{
    ActiveUsersMetric, ActiveUsersType, EventCountMetric, LogVolumeMetric, Metric, MetricsEngine,
    SessionsMetric, SessionsType, StickinessMetric, StickinessType, TopEventsMetric, TopLogsMetric,
    TopSessionsMetric, UsersMetric,
};
pub use timerange::TimeRange;
pub use timeseries::{
    ComparisonData, GroupedTimeSeries, TimeSeriesData, TimeSeriesGroup, TimeSeriesPoint,
};
