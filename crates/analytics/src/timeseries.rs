//! Time series data types
//!
//! Represents analytics query results with time-based aggregations,
//! optional dimensional breakdowns, and comparison data.

use serde::{Deserialize, Serialize};

/// A single data point in a time series
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeSeriesPoint {
    /// Date/time of this point (ISO 8601)
    pub date: String,
    /// The aggregated value
    pub value: f64,
    /// Optional dimension value (for breakdowns)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dimension: Option<String>,
}

impl TimeSeriesPoint {
    /// Create a new point
    pub fn new(date: impl Into<String>, value: f64) -> Self {
        Self {
            date: date.into(),
            value,
            dimension: None,
        }
    }

    /// Create a point with a dimension
    pub fn with_dimension(
        date: impl Into<String>,
        value: f64,
        dimension: impl Into<String>,
    ) -> Self {
        Self {
            date: date.into(),
            value,
            dimension: Some(dimension.into()),
        }
    }
}

/// Time series data with aggregated statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeSeriesData {
    /// Data points
    pub points: Vec<TimeSeriesPoint>,
    /// Total (sum of all values)
    pub total: f64,
    /// Minimum value
    pub min: f64,
    /// Maximum value
    pub max: f64,
    /// Average value
    pub avg: f64,
    /// Optional comparison data
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comparison: Option<ComparisonData>,
}

impl TimeSeriesData {
    /// Create empty time series
    pub fn empty() -> Self {
        Self {
            points: Vec::new(),
            total: 0.0,
            min: 0.0,
            max: 0.0,
            avg: 0.0,
            comparison: None,
        }
    }

    /// Create time series from points, calculating stats
    pub fn from_points(points: Vec<TimeSeriesPoint>) -> Self {
        let stats = calculate_stats(&points);
        Self {
            points,
            total: stats.total,
            min: stats.min,
            max: stats.max,
            avg: stats.avg,
            comparison: None,
        }
    }

    /// Add comparison data
    pub fn with_comparison(mut self, comparison: ComparisonData) -> Self {
        self.comparison = Some(comparison);
        self
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.points.is_empty()
    }

    /// Get number of points
    pub fn len(&self) -> usize {
        self.points.len()
    }
}

/// Comparison metrics between current and previous period
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComparisonData {
    /// Previous period total
    pub previous_total: f64,
    /// Absolute change (current - previous)
    pub change: f64,
    /// Percent change ((current - previous) / previous * 100)
    pub percent_change: f64,
    /// Previous period data points (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_points: Option<Vec<TimeSeriesPoint>>,
}

impl ComparisonData {
    /// Calculate comparison from current and previous totals
    pub fn calculate(current_total: f64, previous_total: f64) -> Self {
        let change = current_total - previous_total;
        let percent_change = if previous_total != 0.0 {
            (change / previous_total) * 100.0
        } else if current_total > 0.0 {
            100.0
        } else {
            0.0
        };

        Self {
            previous_total,
            change,
            percent_change,
            previous_points: None,
        }
    }

    /// Add previous period points
    pub fn with_points(mut self, points: Vec<TimeSeriesPoint>) -> Self {
        self.previous_points = Some(points);
        self
    }
}

/// Statistics for a set of points
struct Stats {
    total: f64,
    min: f64,
    max: f64,
    avg: f64,
}

fn calculate_stats(points: &[TimeSeriesPoint]) -> Stats {
    if points.is_empty() {
        return Stats {
            total: 0.0,
            min: 0.0,
            max: 0.0,
            avg: 0.0,
        };
    }

    let total: f64 = points.iter().map(|p| p.value).sum();
    let min = points.iter().map(|p| p.value).fold(f64::INFINITY, f64::min);
    let max = points
        .iter()
        .map(|p| p.value)
        .fold(f64::NEG_INFINITY, f64::max);
    let avg = total / points.len() as f64;

    Stats {
        total,
        min,
        max,
        avg,
    }
}

/// Grouped time series for breakdown queries
///
/// When a breakdown dimension is specified, results are grouped by that dimension.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupedTimeSeries {
    /// Data grouped by dimension value
    pub groups: Vec<TimeSeriesGroup>,
    /// Combined total across all groups
    pub total: f64,
}

impl GroupedTimeSeries {
    /// Create from groups
    pub fn from_groups(groups: Vec<TimeSeriesGroup>) -> Self {
        let total = groups.iter().map(|g| g.data.total).sum();
        Self { groups, total }
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.groups.is_empty()
    }
}

/// A single group in a breakdown
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeSeriesGroup {
    /// The dimension value (e.g., "mobile", "US")
    pub dimension: String,
    /// Time series data for this group
    pub data: TimeSeriesData,
}

impl TimeSeriesGroup {
    /// Create a new group
    pub fn new(dimension: impl Into<String>, data: TimeSeriesData) -> Self {
        Self {
            dimension: dimension.into(),
            data,
        }
    }
}
