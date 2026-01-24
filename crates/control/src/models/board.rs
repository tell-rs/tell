//! Board models
//!
//! Boards are dashboards containing metric blocks and notes.
//!
//! # Structure
//!
//! A board contains blocks (metric visualizations and notes) arranged on a grid.
//! Each metric block can have multiple series for overlaying data (e.g., DAU + MAU).
//!
//! # Limits
//!
//! - Max 50 metric blocks per board
//! - Max 50 note blocks per board
//! - Max 20 series per metric block

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Maximum metric blocks per board
pub const MAX_METRIC_BLOCKS: usize = 50;
/// Maximum note blocks per board
pub const MAX_NOTE_BLOCKS: usize = 50;
/// Maximum series per metric block
pub const MAX_SERIES_PER_BLOCK: usize = 20;

/// A board (dashboard) containing metric blocks
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Board {
    pub id: String,
    pub workspace_id: String,
    pub owner_id: String,
    pub title: String,
    pub description: Option<String>,
    pub settings: BoardSettings,
    pub is_pinned: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Board {
    /// Create a new board
    pub fn new(workspace_id: &str, owner_id: &str, title: &str) -> Self {
        let now = Utc::now();
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            workspace_id: workspace_id.to_string(),
            owner_id: owner_id.to_string(),
            title: title.to_string(),
            description: None,
            settings: BoardSettings::default(),
            is_pinned: false,
            created_at: now,
            updated_at: now,
        }
    }

    /// Create a board with description
    pub fn with_description(mut self, description: &str) -> Self {
        self.description = Some(description.to_string());
        self
    }
}

/// Board display settings
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BoardSettings {
    /// Blocks on this board
    #[serde(default)]
    pub blocks: Vec<Block>,
    /// Layout mode
    pub layout: Option<BoardLayout>,
    /// Default time range for metrics
    pub default_time_range: Option<String>,
}

/// Layout mode for boards
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BoardLayout {
    Grid,
    Freeform,
}

/// A block on a board
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Block {
    Metric(Box<MetricBlock>),
    Note(NoteBlock),
}

/// A metric visualization block
///
/// Contains one or more series that can be overlaid on the same chart.
/// For example, overlay DAU and MAU on the same line chart.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricBlock {
    pub id: String,
    /// Custom title for the block
    pub title: Option<String>,
    /// Visualization type (line_chart, bar_chart, etc.)
    pub visualization: VisualizationType,
    /// Data series (1-20 per block)
    #[serde(default)]
    pub series: Vec<SeriesConfig>,
    /// Position and size on the board
    pub position: BlockPosition,
    /// Axis configuration for charts
    #[serde(default)]
    pub axis_config: Option<ChartAxisConfig>,
}

impl MetricBlock {
    /// Create a new metric block with a single series
    pub fn new(visualization: VisualizationType, position: BlockPosition) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            title: None,
            visualization,
            series: Vec::new(),
            position,
            axis_config: None,
        }
    }

    /// Add a series to this block
    pub fn with_series(mut self, series: SeriesConfig) -> Self {
        self.series.push(series);
        self
    }
}

/// Configuration for a data series within a metric block
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeriesConfig {
    pub id: String,
    /// Display name for this series
    pub name: String,
    /// Query configuration
    pub query: QueryConfig,
    /// Display formatting
    #[serde(default)]
    pub display: Option<DisplayConfig>,
}

impl SeriesConfig {
    pub fn new(name: &str, query: QueryConfig) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            name: name.to_string(),
            query,
            display: None,
        }
    }
}

/// Query configuration for a metric series
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryConfig {
    /// Metric type: "dau", "wau", "mau", "events", "log_volume", "sessions", "stickiness"
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

impl QueryConfig {
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
}

/// Filter condition for queries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterConfig {
    /// Field name (e.g., "country", "device_type", "properties.page")
    pub field: String,
    /// Operator: "eq", "ne", "contains", "gt", "lt", "gte", "lte", "in", "not_in", "is_set", "is_not_set"
    pub operator: String,
    /// Value(s) to compare against
    #[serde(default)]
    pub value: serde_json::Value,
}

/// Display configuration for a series
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DisplayConfig {
    /// Unit label: "users", "events", "logs", "%", "$", or custom
    #[serde(default)]
    pub unit: Option<String>,
    /// Abbreviate large numbers (1.2M vs 1,200,000)
    #[serde(default)]
    pub abbreviate: Option<bool>,
    /// Decimal places for values
    #[serde(default)]
    pub decimals: Option<u8>,
    /// Show comparison percentage change
    #[serde(default)]
    pub show_comparison: Option<bool>,
    /// Hex color for this series (e.g., "#9333ea")
    #[serde(default)]
    pub color: Option<String>,
    /// Line style: "solid", "dashed", "dotted"
    #[serde(default)]
    pub line_style: Option<String>,
    /// Axis assignment for dual-axis charts: "left" or "right"
    #[serde(default)]
    pub axis_assignment: Option<String>,
}

/// Chart axis configuration
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ChartAxisConfig {
    /// Left Y-axis configuration
    #[serde(default)]
    pub left_y: Option<AxisConfig>,
    /// Right Y-axis configuration (for dual-axis charts)
    #[serde(default)]
    pub right_y: Option<AxisConfig>,
    /// X-axis configuration
    #[serde(default)]
    pub x: Option<AxisConfig>,
}

/// Configuration for a single axis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AxisConfig {
    /// Show/hide this axis
    #[serde(default)]
    pub visible: Option<bool>,
    /// Custom axis label
    #[serde(default)]
    pub label: Option<String>,
    /// Unit for values: "$", "%", "#", or custom
    #[serde(default)]
    pub unit: Option<String>,
    /// Abbreviate large numbers
    #[serde(default)]
    pub abbreviate: Option<bool>,
    /// Decimal places
    #[serde(default)]
    pub decimals: Option<u8>,
    /// Scale type: "linear" or "logarithmic"
    #[serde(default)]
    pub scale: Option<String>,
    /// Minimum value bound
    #[serde(default)]
    pub min_bound: Option<f64>,
    /// Maximum value bound
    #[serde(default)]
    pub max_bound: Option<f64>,
}

/// A markdown note block
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoteBlock {
    pub id: String,
    /// Markdown content
    pub content: String,
    /// Position and size on the board
    pub position: BlockPosition,
}

impl NoteBlock {
    pub fn new(content: &str, position: BlockPosition) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            content: content.to_string(),
            position,
        }
    }
}

/// Position and size of a block
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockPosition {
    pub x: u32,
    pub y: u32,
    pub width: u32,
    pub height: u32,
}

impl BlockPosition {
    pub fn new(x: u32, y: u32, width: u32, height: u32) -> Self {
        Self {
            x,
            y,
            width,
            height,
        }
    }
}

/// Visualization type for metric blocks
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VisualizationType {
    LineChart,
    BarChart,
    AreaChart,
    Number,
    Table,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_board() {
        let board = Board::new("ws_1", "user_1", "My Dashboard");
        assert!(!board.id.is_empty());
        assert_eq!(board.workspace_id, "ws_1");
        assert_eq!(board.owner_id, "user_1");
        assert_eq!(board.title, "My Dashboard");
        assert!(!board.is_pinned);
    }

    #[test]
    fn test_board_with_description() {
        let board = Board::new("ws_1", "user_1", "Dashboard").with_description("A test dashboard");
        assert_eq!(board.description, Some("A test dashboard".to_string()));
    }

    #[test]
    fn test_metric_block() {
        let pos = BlockPosition::new(0, 0, 4, 2);
        let block = MetricBlock::new(VisualizationType::LineChart, pos);
        assert_eq!(block.visualization, VisualizationType::LineChart);
        assert!(block.title.is_none());
        assert!(block.series.is_empty());
    }

    #[test]
    fn test_metric_block_with_series() {
        let pos = BlockPosition::new(0, 0, 6, 4);
        let query = QueryConfig::new("dau", "30d");
        let series = SeriesConfig::new("Daily Active Users", query);

        let block = MetricBlock::new(VisualizationType::LineChart, pos).with_series(series);

        assert_eq!(block.series.len(), 1);
        assert_eq!(block.series[0].name, "Daily Active Users");
        assert_eq!(block.series[0].query.metric, "dau");
        assert_eq!(block.series[0].query.range, "30d");
    }

    #[test]
    fn test_multi_series_block() {
        let pos = BlockPosition::new(0, 0, 6, 4);

        let dau_query = QueryConfig::new("dau", "30d");
        let dau_series = SeriesConfig::new("DAU", dau_query);

        let mau_query = QueryConfig::new("mau", "30d");
        let mau_series = SeriesConfig::new("MAU", mau_query);

        let block = MetricBlock::new(VisualizationType::LineChart, pos)
            .with_series(dau_series)
            .with_series(mau_series);

        assert_eq!(block.series.len(), 2);
        assert_eq!(block.series[0].query.metric, "dau");
        assert_eq!(block.series[1].query.metric, "mau");
    }

    #[test]
    fn test_block_serialization() {
        let pos = BlockPosition::new(0, 0, 4, 2);
        let query = QueryConfig::new("dau", "7d");
        let series = SeriesConfig::new("DAU", query);
        let metric_block = MetricBlock::new(VisualizationType::LineChart, pos).with_series(series);
        let block = Block::Metric(Box::new(metric_block));

        let json = serde_json::to_string(&block).unwrap();
        assert!(json.contains("\"type\":\"metric\""));
        assert!(json.contains("\"visualization\":\"line_chart\""));
        assert!(json.contains("\"metric\":\"dau\""));
    }

    #[test]
    fn test_query_config_with_options() {
        let mut query = QueryConfig::new("events", "7d");
        query.breakdown = Some("country".to_string());
        query.compare_mode = Some("previous".to_string());
        query.granularity = Some("daily".to_string());
        query.limit = Some(10);

        assert_eq!(query.breakdown, Some("country".to_string()));
        assert_eq!(query.compare_mode, Some("previous".to_string()));
        assert_eq!(query.granularity, Some("daily".to_string()));
        assert_eq!(query.limit, Some(10));
    }

    #[test]
    fn test_display_config_serialization() {
        let display = DisplayConfig {
            unit: Some("users".to_string()),
            abbreviate: Some(true),
            decimals: Some(0),
            show_comparison: Some(true),
            color: Some("#9333ea".to_string()),
            line_style: Some("solid".to_string()),
            axis_assignment: Some("left".to_string()),
        };

        let json = serde_json::to_string(&display).unwrap();
        assert!(json.contains("\"unit\":\"users\""));
        assert!(json.contains("\"color\":\"#9333ea\""));
    }

    #[test]
    fn test_axis_config() {
        let axis_config = ChartAxisConfig {
            left_y: Some(AxisConfig {
                visible: Some(true),
                label: Some("Users".to_string()),
                unit: Some("#".to_string()),
                abbreviate: Some(true),
                decimals: Some(0),
                scale: Some("linear".to_string()),
                min_bound: Some(0.0),
                max_bound: None,
            }),
            right_y: Some(AxisConfig {
                visible: Some(true),
                label: Some("Revenue".to_string()),
                unit: Some("$".to_string()),
                abbreviate: Some(true),
                decimals: Some(2),
                scale: Some("linear".to_string()),
                min_bound: None,
                max_bound: None,
            }),
            x: None,
        };

        let json = serde_json::to_string(&axis_config).unwrap();
        assert!(json.contains("\"left_y\""));
        assert!(json.contains("\"right_y\""));
        assert!(json.contains("\"label\":\"Users\""));
        assert!(json.contains("\"label\":\"Revenue\""));
    }
}
