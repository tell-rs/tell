//! Saved metric models
//!
//! Saved metrics are reusable metric configurations that can be shared.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::board::{DisplayConfig, FilterConfig};

/// A saved metric configuration
///
/// Represents a reusable metric query that can be saved, shared, and embedded in boards.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SavedMetric {
    pub id: String,
    pub workspace_id: String,
    pub owner_id: String,
    pub title: String,
    pub description: Option<String>,
    /// The metric query configuration
    pub query: MetricQueryConfig,
    /// Display formatting
    #[serde(default)]
    pub display: Option<DisplayConfig>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl SavedMetric {
    /// Create a new saved metric
    pub fn new(workspace_id: &str, owner_id: &str, title: &str, query: MetricQueryConfig) -> Self {
        let now = Utc::now();
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            workspace_id: workspace_id.to_string(),
            owner_id: owner_id.to_string(),
            title: title.to_string(),
            description: None,
            query,
            display: None,
            created_at: now,
            updated_at: now,
        }
    }

    /// Add a description
    pub fn with_description(mut self, description: &str) -> Self {
        self.description = Some(description.to_string());
        self
    }

    /// Add display configuration
    pub fn with_display(mut self, display: DisplayConfig) -> Self {
        self.display = Some(display);
        self
    }
}

/// Query configuration for a saved metric
///
/// This is similar to QueryConfig in board.rs but standalone for saved metrics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricQueryConfig {
    /// Metric type: "dau", "wau", "mau", "events", "log_volume", "sessions", "stickiness", "top_events", "top_logs"
    pub metric: String,
    /// Time range: "7d", "30d", "90d", "mtd", "ytd", or custom "2024-01-01,2024-01-31"
    pub range: String,
    /// Filter conditions
    #[serde(default)]
    pub filters: Vec<FilterConfig>,
    /// Breakdown dimension (e.g., "device_type", "country")
    #[serde(default)]
    pub breakdown: Option<String>,
    /// Comparison mode: "previous" (previous period) or "previous_year" (YoY)
    #[serde(default)]
    pub compare_mode: Option<String>,
    /// Result limit for top-N queries
    #[serde(default)]
    pub limit: Option<u32>,
    /// Time granularity: "minute", "hourly", "daily", "weekly", "monthly", "quarterly", "yearly"
    #[serde(default)]
    pub granularity: Option<String>,
}

impl MetricQueryConfig {
    pub fn new(metric: &str, range: &str) -> Self {
        Self {
            metric: metric.to_string(),
            range: range.to_string(),
            filters: Vec::new(),
            breakdown: None,
            compare_mode: None,
            limit: None,
            granularity: None,
        }
    }

    /// Add a breakdown dimension
    pub fn with_breakdown(mut self, breakdown: &str) -> Self {
        self.breakdown = Some(breakdown.to_string());
        self
    }

    /// Add comparison mode
    pub fn with_compare(mut self, mode: &str) -> Self {
        self.compare_mode = Some(mode.to_string());
        self
    }

    /// Add granularity
    pub fn with_granularity(mut self, granularity: &str) -> Self {
        self.granularity = Some(granularity.to_string());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_saved_metric() {
        let query = MetricQueryConfig::new("dau", "30d");
        let metric = SavedMetric::new("ws_1", "user_1", "Daily Active Users", query);

        assert!(!metric.id.is_empty());
        assert_eq!(metric.workspace_id, "ws_1");
        assert_eq!(metric.owner_id, "user_1");
        assert_eq!(metric.title, "Daily Active Users");
        assert_eq!(metric.query.metric, "dau");
        assert_eq!(metric.query.range, "30d");
    }

    #[test]
    fn test_metric_with_options() {
        let query = MetricQueryConfig::new("events", "7d")
            .with_breakdown("country")
            .with_compare("previous")
            .with_granularity("daily");

        assert_eq!(query.breakdown, Some("country".to_string()));
        assert_eq!(query.compare_mode, Some("previous".to_string()));
        assert_eq!(query.granularity, Some("daily".to_string()));
    }

    #[test]
    fn test_metric_serialization() {
        let query = MetricQueryConfig::new("mau", "90d");
        let metric = SavedMetric::new("ws_1", "user_1", "Monthly Users", query)
            .with_description("Track monthly active users");

        let json = serde_json::to_string(&metric).unwrap();
        assert!(json.contains("\"metric\":\"mau\""));
        assert!(json.contains("\"range\":\"90d\""));
        assert!(json.contains("\"description\":\"Track monthly active users\""));
    }
}
