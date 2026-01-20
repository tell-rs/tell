//! API request and response types
//!
//! Shared types for API endpoints including query parameters and response wrappers.

use serde::{Deserialize, Serialize};
use tell_analytics::{CompareMode, Filter, Granularity, TimeRange};

use crate::error::{ApiError, Result};

/// Query parameters for metric endpoints
#[derive(Debug, Deserialize)]
pub struct MetricParams {
    /// Time range (e.g., "7d", "30d", "2024-01-01,2024-01-31")
    #[serde(default = "default_range")]
    pub range: String,

    /// Time granularity (minute, hourly, daily, weekly, monthly)
    #[serde(default = "default_granularity")]
    pub granularity: String,

    /// Breakdown dimension (e.g., "device_type", "country")
    pub breakdown: Option<String>,

    /// Comparison mode (previous, yoy)
    pub compare: Option<String>,
}

fn default_range() -> String {
    "30d".to_string()
}

fn default_granularity() -> String {
    "daily".to_string()
}

impl MetricParams {
    /// Convert to analytics Filter
    pub fn to_filter(&self) -> Result<Filter> {
        let range =
            TimeRange::parse(&self.range).map_err(|e| ApiError::InvalidTimeRange(e.to_string()))?;

        let granularity = Granularity::parse(&self.granularity)
            .map_err(|e| ApiError::InvalidFilter(e.to_string()))?;

        let mut filter = Filter::new(range).with_granularity(granularity);

        if let Some(breakdown) = &self.breakdown {
            filter = filter.with_breakdown(breakdown.clone());
        }

        if let Some(compare) = &self.compare {
            let compare_mode =
                CompareMode::parse(compare).map_err(|e| ApiError::InvalidFilter(e.to_string()))?;
            filter = filter.with_compare(compare_mode);
        }

        Ok(filter)
    }
}

/// Query parameters for drill-down endpoints
#[derive(Debug, Deserialize)]
pub struct DrillDownParams {
    /// Time range
    #[serde(default = "default_range")]
    pub range: String,

    /// Result limit
    #[serde(default = "default_limit")]
    pub limit: u32,
}

fn default_limit() -> u32 {
    100
}

impl DrillDownParams {
    /// Convert to analytics Filter
    pub fn to_filter(&self) -> Result<Filter> {
        let range =
            TimeRange::parse(&self.range).map_err(|e| ApiError::InvalidTimeRange(e.to_string()))?;

        Ok(Filter::new(range).with_limit(self.limit))
    }
}

/// Query parameters for top N endpoints
#[derive(Debug, Deserialize)]
pub struct TopParams {
    /// Time range
    #[serde(default = "default_range")]
    pub range: String,

    /// Number of results
    #[serde(default = "default_top_limit")]
    pub limit: u32,
}

fn default_top_limit() -> u32 {
    10
}

impl TopParams {
    /// Convert to analytics Filter
    pub fn to_filter(&self) -> Result<Filter> {
        let range =
            TimeRange::parse(&self.range).map_err(|e| ApiError::InvalidTimeRange(e.to_string()))?;

        Ok(Filter::new(range).with_limit(self.limit))
    }
}

/// Generic API response wrapper
#[derive(Debug, Serialize)]
pub struct ApiResponse<T> {
    /// Response data
    pub data: T,
}

impl<T> ApiResponse<T> {
    /// Create a new API response
    pub fn new(data: T) -> Self {
        Self { data }
    }
}

/// Paginated response for list endpoints
#[derive(Debug, Serialize)]
pub struct PaginatedResponse<T> {
    /// Response data
    pub data: Vec<T>,
    /// Total count
    pub total: usize,
    /// Current offset
    pub offset: usize,
    /// Page limit
    pub limit: usize,
}

impl<T> PaginatedResponse<T> {
    /// Create a new paginated response
    pub fn new(data: Vec<T>, total: usize, offset: usize, limit: usize) -> Self {
        Self {
            data,
            total,
            offset,
            limit,
        }
    }
}
