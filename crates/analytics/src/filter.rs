//! Filter parsing and condition handling
//!
//! Filters define what data to query: time range, conditions, granularity, and breakdowns.

use serde::{Deserialize, Serialize};

use crate::error::{AnalyticsError, Result};
use crate::timerange::TimeRange;

/// Maximum allowed limit for query results
pub const MAX_LIMIT: u32 = 10_000;

/// A complete filter for analytics queries
#[derive(Debug, Clone)]
pub struct Filter {
    /// Time range for the query
    pub time_range: TimeRange,
    /// Filter conditions (WHERE clauses)
    pub conditions: Vec<Condition>,
    /// Time granularity for aggregation (None = rolling window / single aggregate)
    pub granularity: Option<Granularity>,
    /// Optional breakdown dimension (GROUP BY)
    pub breakdown: Option<String>,
    /// Comparison mode
    pub compare: Option<CompareMode>,
    /// Result limit (max 10,000)
    pub limit: Option<u32>,
}

impl Filter {
    /// Create a new filter with a time range
    ///
    /// Defaults to Daily granularity. Use `with_rolling_window()` for aggregate queries.
    pub fn new(time_range: TimeRange) -> Self {
        Self {
            time_range,
            conditions: Vec::new(),
            granularity: Some(Granularity::Daily),
            breakdown: None,
            compare: None,
            limit: None,
        }
    }

    /// Add a condition to the filter
    pub fn with_condition(mut self, condition: Condition) -> Self {
        self.conditions.push(condition);
        self
    }

    /// Set the granularity
    pub fn with_granularity(mut self, granularity: Granularity) -> Self {
        self.granularity = Some(granularity);
        self
    }

    /// Set rolling window mode (no time bucketing, returns single aggregate)
    ///
    /// Use this for queries that should return a single value instead of time series.
    pub fn with_rolling_window(mut self) -> Self {
        self.granularity = None;
        self
    }

    /// Set the breakdown dimension
    pub fn with_breakdown(mut self, field: impl Into<String>) -> Self {
        self.breakdown = Some(field.into());
        self
    }

    /// Set comparison mode
    pub fn with_compare(mut self, compare: CompareMode) -> Self {
        self.compare = Some(compare);
        self
    }

    /// Set result limit (capped at MAX_LIMIT)
    pub fn with_limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit.min(MAX_LIMIT));
        self
    }

    /// Check if this filter uses rolling window mode (no granularity)
    pub fn is_rolling_window(&self) -> bool {
        self.granularity.is_none()
    }
}

/// A single filter condition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Condition {
    /// Field name to filter on
    pub field: String,
    /// Operator for comparison
    pub operator: Operator,
    /// Value(s) to compare against
    pub value: ConditionValue,
}

impl Condition {
    /// Create an equality condition
    pub fn eq(field: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            operator: Operator::Eq,
            value: ConditionValue::Single(value.into()),
        }
    }

    /// Create a not-equal condition
    pub fn ne(field: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            operator: Operator::Ne,
            value: ConditionValue::Single(value.into()),
        }
    }

    /// Create a contains condition
    pub fn contains(field: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            operator: Operator::Contains,
            value: ConditionValue::Single(value.into()),
        }
    }

    /// Create an IN condition
    pub fn is_in(field: impl Into<String>, values: Vec<String>) -> Self {
        Self {
            field: field.into(),
            operator: Operator::In,
            value: ConditionValue::Multiple(values),
        }
    }

    /// Create an is_set condition (field is not null)
    pub fn is_set(field: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            operator: Operator::IsSet,
            value: ConditionValue::None,
        }
    }

    /// Create an is_not_set condition (field is null)
    pub fn is_not_set(field: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            operator: Operator::IsNotSet,
            value: ConditionValue::None,
        }
    }
}

/// Filter operators
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Operator {
    /// Equal
    Eq,
    /// Not equal
    Ne,
    /// Greater than
    Gt,
    /// Greater than or equal
    Gte,
    /// Less than
    Lt,
    /// Less than or equal
    Lte,
    /// Contains substring
    Contains,
    /// Does not contain substring
    NotContains,
    /// Starts with
    StartsWith,
    /// Ends with
    EndsWith,
    /// In list
    In,
    /// Not in list
    NotIn,
    /// Field is set (not null)
    IsSet,
    /// Field is not set (null)
    IsNotSet,
    /// Regex match
    Regex,
}

impl Operator {
    /// Parse operator from string
    pub fn parse(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "eq" | "=" | "==" => Ok(Self::Eq),
            "ne" | "!=" | "<>" => Ok(Self::Ne),
            "gt" | ">" => Ok(Self::Gt),
            "gte" | ">=" => Ok(Self::Gte),
            "lt" | "<" => Ok(Self::Lt),
            "lte" | "<=" => Ok(Self::Lte),
            "contains" | "like" => Ok(Self::Contains),
            "not_contains" | "not_like" => Ok(Self::NotContains),
            "starts_with" => Ok(Self::StartsWith),
            "ends_with" => Ok(Self::EndsWith),
            "in" => Ok(Self::In),
            "not_in" => Ok(Self::NotIn),
            "is_set" | "isset" => Ok(Self::IsSet),
            "is_not_set" | "isnotset" => Ok(Self::IsNotSet),
            "regex" | "~" => Ok(Self::Regex),
            _ => Err(AnalyticsError::InvalidOperator(s.to_string())),
        }
    }
}

/// Condition value (single, multiple, or none)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ConditionValue {
    /// No value (for is_set/is_not_set)
    None,
    /// Single value
    Single(String),
    /// Multiple values (for IN)
    Multiple(Vec<String>),
}

/// Time granularity for aggregation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Granularity {
    /// Per minute
    Minute,
    /// Per hour
    Hourly,
    /// Per day
    #[default]
    Daily,
    /// Per week
    Weekly,
    /// Per month
    Monthly,
    /// Per quarter
    Quarterly,
    /// Per year
    Yearly,
}

impl Granularity {
    /// Parse granularity from string
    pub fn parse(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "minute" | "min" | "1m" => Ok(Self::Minute),
            "hourly" | "hour" | "1h" => Ok(Self::Hourly),
            "daily" | "day" | "1d" => Ok(Self::Daily),
            "weekly" | "week" | "1w" => Ok(Self::Weekly),
            "monthly" | "month" => Ok(Self::Monthly),
            "quarterly" | "quarter" => Ok(Self::Quarterly),
            "yearly" | "year" | "1y" => Ok(Self::Yearly),
            _ => Err(AnalyticsError::InvalidGranularity(s.to_string())),
        }
    }

    /// Get ClickHouse date function for this granularity
    pub fn clickhouse_fn(&self) -> &'static str {
        match self {
            Self::Minute => "toStartOfMinute",
            Self::Hourly => "toStartOfHour",
            Self::Daily => "toDate",
            Self::Weekly => "toStartOfWeek",
            Self::Monthly => "toStartOfMonth",
            Self::Quarterly => "toStartOfQuarter",
            Self::Yearly => "toStartOfYear",
        }
    }
}

/// Comparison mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompareMode {
    /// Compare to previous period of same duration
    Previous,
    /// Compare to same period last year
    PreviousYear,
}

impl CompareMode {
    /// Parse comparison mode from string
    pub fn parse(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "previous" | "prev" | "previous_period" => Ok(Self::Previous),
            "previous_year" | "yoy" | "year_over_year" => Ok(Self::PreviousYear),
            _ => Err(AnalyticsError::InvalidFilter(format!(
                "unknown compare mode: {}",
                s
            ))),
        }
    }
}
