//! API request and response types
//!
//! Shared types for API endpoints including query parameters and response wrappers.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use tell_analytics::{
    CompareMode, Condition, ConditionValue, Filter, Granularity, Operator, TimeRange,
};

use crate::error::{ApiError, Result};

// =============================================================================
// Query Parameters
// =============================================================================

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

    /// Filter by specific event name
    pub event: Option<String>,

    /// Filter conditions as flattened query params
    /// Format: conditions[0][field]=X&conditions[0][operator]=eq&conditions[0][value]=Y
    #[serde(default, flatten)]
    pub conditions_raw: HashMap<String, String>,
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

        // Add event filter as condition if specified
        if let Some(event) = &self.event {
            filter = filter.with_condition(Condition::eq("event_name", event.as_str()));
        }

        // Parse conditions from flattened query params
        for condition in parse_conditions(&self.conditions_raw)? {
            filter = filter.with_condition(condition);
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

    /// Pagination offset
    #[serde(default)]
    pub offset: u32,

    /// Include accurate total count (slower query)
    #[serde(default)]
    pub count_total: bool,

    /// Filter by specific event name
    pub event: Option<String>,

    /// Filter conditions
    #[serde(default, flatten)]
    pub conditions_raw: HashMap<String, String>,
}

fn default_limit() -> u32 {
    100
}

impl DrillDownParams {
    /// Convert to analytics Filter
    pub fn to_filter(&self) -> Result<Filter> {
        let range =
            TimeRange::parse(&self.range).map_err(|e| ApiError::InvalidTimeRange(e.to_string()))?;

        let mut filter = Filter::new(range).with_limit(self.limit);

        // Add event filter as condition if specified
        if let Some(event) = &self.event {
            filter = filter.with_condition(Condition::eq("event_name", event.as_str()));
        }

        // Parse conditions
        for condition in parse_conditions(&self.conditions_raw)? {
            filter = filter.with_condition(condition);
        }

        Ok(filter)
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

    /// Filter by specific event name (for event property breakdown)
    pub event: Option<String>,

    /// Property to break down by
    pub property: Option<String>,

    /// Sort order: "count" (default) or "users"
    #[serde(default = "default_order_by")]
    pub order_by: String,

    /// Filter conditions
    #[serde(default, flatten)]
    pub conditions_raw: HashMap<String, String>,
}

fn default_top_limit() -> u32 {
    10
}

fn default_order_by() -> String {
    "count".to_string()
}

impl TopParams {
    /// Convert to analytics Filter
    pub fn to_filter(&self) -> Result<Filter> {
        let range =
            TimeRange::parse(&self.range).map_err(|e| ApiError::InvalidTimeRange(e.to_string()))?;

        let mut filter = Filter::new(range).with_limit(self.limit);

        // Add event filter as condition if specified
        if let Some(event) = &self.event {
            filter = filter.with_condition(Condition::eq("event_name", event.as_str()));
        }

        // Parse conditions
        for condition in parse_conditions(&self.conditions_raw)? {
            filter = filter.with_condition(condition);
        }

        Ok(filter)
    }
}

/// Query parameters for log endpoints with specific filters
#[derive(Debug, Deserialize)]
pub struct LogParams {
    /// Time range
    #[serde(default = "default_range")]
    pub range: String,

    /// Time granularity
    #[serde(default = "default_granularity")]
    pub granularity: String,

    /// Breakdown dimension
    pub breakdown: Option<String>,

    /// Comparison mode
    pub compare: Option<String>,

    /// Filter by service name
    pub service: Option<String>,

    /// Filter by log level (DEBUG, INFO, WARN, ERROR, CRITICAL)
    pub level: Option<String>,

    /// Filter by source
    pub source: Option<String>,

    /// Filter conditions
    #[serde(default, flatten)]
    pub conditions_raw: HashMap<String, String>,
}

impl LogParams {
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

        // Add log-specific filters
        if let Some(service) = &self.service {
            filter = filter.with_condition(Condition::eq("service", service.as_str()));
        }
        if let Some(level) = &self.level {
            filter = filter.with_condition(Condition::eq("level", level.as_str()));
        }
        if let Some(source) = &self.source {
            filter = filter.with_condition(Condition::eq("source", source.as_str()));
        }

        // Parse conditions
        for condition in parse_conditions(&self.conditions_raw)? {
            filter = filter.with_condition(condition);
        }

        Ok(filter)
    }
}

/// Query parameters for custom event metrics (aggregations)
#[derive(Debug, Deserialize)]
pub struct CustomMetricParams {
    /// Time range
    #[serde(default = "default_range")]
    pub range: String,

    /// Time granularity
    #[serde(default = "default_granularity")]
    pub granularity: String,

    /// Event name (required)
    pub event: String,

    /// Property to aggregate (required)
    pub property: String,

    /// Aggregation function: sum, avg, min, max, count
    #[serde(default = "default_metric_fn")]
    pub metric: String,

    /// Filter conditions
    #[serde(default, flatten)]
    pub conditions_raw: HashMap<String, String>,
}

fn default_metric_fn() -> String {
    "sum".to_string()
}

impl CustomMetricParams {
    /// Convert to analytics Filter
    pub fn to_filter(&self) -> Result<Filter> {
        let range =
            TimeRange::parse(&self.range).map_err(|e| ApiError::InvalidTimeRange(e.to_string()))?;

        let granularity = Granularity::parse(&self.granularity)
            .map_err(|e| ApiError::InvalidFilter(e.to_string()))?;

        let mut filter = Filter::new(range).with_granularity(granularity);

        // Add event filter
        filter = filter.with_condition(Condition::eq("event_name", self.event.as_str()));

        // Parse conditions
        for condition in parse_conditions(&self.conditions_raw)? {
            filter = filter.with_condition(condition);
        }

        Ok(filter)
    }

    /// Validate the metric function
    pub fn validate(&self) -> Result<()> {
        match self.metric.as_str() {
            "sum" | "avg" | "min" | "max" | "count" => Ok(()),
            _ => Err(ApiError::InvalidFilter(format!(
                "invalid metric function '{}', valid: sum, avg, min, max, count",
                self.metric
            ))),
        }
    }
}

// =============================================================================
// Condition Parsing
// =============================================================================

/// Parse conditions from flattened query params
///
/// Supports format: conditions[0][field]=X&conditions[0][operator]=eq&conditions[0][value]=Y
/// Also supports array values: conditions[0][value][]=A&conditions[0][value][]=B
fn parse_conditions(raw: &HashMap<String, String>) -> Result<Vec<Condition>> {
    // Group by condition index
    let mut grouped: HashMap<u32, HashMap<String, String>> = HashMap::new();
    let mut array_values: HashMap<(u32, String), Vec<String>> = HashMap::new();

    for (key, value) in raw {
        // Parse conditions[N][key] or conditions[N][key][]
        if let Some(rest) = key.strip_prefix("conditions[")
            && let Some(bracket_pos) = rest.find(']')
            && let Ok(idx) = rest[..bracket_pos].parse::<u32>()
        {
            let remainder = &rest[bracket_pos + 1..];
            if let Some(inner) = remainder.strip_prefix('[')
                && let Some(end) = inner.find(']')
            {
                let field_key = &inner[..end];
                // Check if it's an array value (ends with [])
                if remainder.ends_with("[]") {
                    array_values
                        .entry((idx, field_key.to_string()))
                        .or_default()
                        .push(value.clone());
                } else {
                    grouped
                        .entry(idx)
                        .or_default()
                        .insert(field_key.to_string(), value.clone());
                }
            }
        }
    }

    // Build conditions
    let mut conditions = Vec::new();
    for (idx, fields) in grouped {
        let field = fields.get("field").cloned().unwrap_or_default();
        let operator_str = fields
            .get("operator")
            .cloned()
            .unwrap_or_else(|| "eq".to_string());
        let single_value = fields.get("value").cloned();

        if field.is_empty() {
            continue;
        }

        let operator = Operator::parse(&operator_str)
            .map_err(|e| ApiError::InvalidFilter(format!("condition {}: {}", idx, e)))?;

        // Check for array values
        let array_vals = array_values.get(&(idx, "value".to_string()));

        let value = if let Some(vals) = array_vals {
            ConditionValue::Multiple(vals.clone())
        } else if let Some(v) = single_value {
            ConditionValue::Single(v)
        } else if matches!(operator, Operator::IsSet | Operator::IsNotSet) {
            ConditionValue::None
        } else {
            continue; // Skip conditions without values
        };

        conditions.push(Condition {
            field,
            operator,
            value,
        });
    }

    Ok(conditions)
}

// =============================================================================
// Response Types
// =============================================================================

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

/// API response with metadata
#[derive(Debug, Serialize)]
pub struct ApiResponseWithMeta<T> {
    /// Response data
    pub data: T,
    /// Response metadata
    pub meta: ResponseMeta,
}

impl<T> ApiResponseWithMeta<T> {
    /// Create a new API response with metadata
    pub fn new(data: T, meta: ResponseMeta) -> Self {
        Self { data, meta }
    }
}

/// Response metadata
#[derive(Debug, Serialize)]
pub struct ResponseMeta {
    /// Start of time range
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_date: Option<String>,
    /// End of time range
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_date: Option<String>,
    /// Time range string
    #[serde(skip_serializing_if = "Option::is_none")]
    pub range: Option<String>,
    /// Metric type
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metric_type: Option<String>,
    /// Event name (if filtered)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event: Option<String>,
    /// Property name (if breakdown)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub property: Option<String>,
}

impl ResponseMeta {
    /// Create empty metadata
    pub fn empty() -> Self {
        Self {
            start_date: None,
            end_date: None,
            range: None,
            metric_type: None,
            event: None,
            property: None,
        }
    }

    /// Create metadata from filter
    pub fn from_filter(filter: &Filter, metric_type: &str) -> Self {
        Self {
            start_date: Some(
                filter
                    .time_range
                    .start
                    .format("%Y-%m-%dT%H:%M:%SZ")
                    .to_string(),
            ),
            end_date: Some(
                filter
                    .time_range
                    .end
                    .format("%Y-%m-%dT%H:%M:%SZ")
                    .to_string(),
            ),
            range: None,
            metric_type: Some(metric_type.to_string()),
            event: None,
            property: None,
        }
    }

    /// Add event to metadata
    pub fn with_event(mut self, event: &str) -> Self {
        self.event = Some(event.to_string());
        self
    }

    /// Add property to metadata
    pub fn with_property(mut self, property: &str) -> Self {
        self.property = Some(property.to_string());
        self
    }
}

/// Paginated response for list endpoints
#[derive(Debug, Serialize)]
pub struct PaginatedResponse<T> {
    /// Response data
    pub data: Vec<T>,
    /// Total count (if count_total was requested)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_count: Option<u64>,
    /// Whether more results are available
    pub has_more: bool,
    /// Response metadata
    pub meta: DrillDownMeta,
}

impl<T> PaginatedResponse<T> {
    /// Create a new paginated response
    pub fn new(data: Vec<T>, total_count: Option<u64>, limit: u32, offset: u32) -> Self {
        let has_more = data.len() as u32 >= limit;
        Self {
            data,
            total_count,
            has_more,
            meta: DrillDownMeta {
                result_count: 0, // Will be set by caller
                offset,
                limit,
            },
        }
    }

    /// Set the result count
    pub fn with_count(mut self, count: usize) -> Self {
        self.meta.result_count = count as u32;
        self
    }
}

/// Metadata for drill-down responses
#[derive(Debug, Serialize)]
pub struct DrillDownMeta {
    /// Number of results returned
    pub result_count: u32,
    /// Pagination offset
    pub offset: u32,
    /// Page limit
    pub limit: u32,
}
