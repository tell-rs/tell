//! Board models
//!
//! Boards are dashboards containing metric blocks and notes.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

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
    Metric(MetricBlock),
    Note(NoteBlock),
}

/// A metric visualization block
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricBlock {
    pub id: String,
    /// Type of metric: "dau", "wau", "mau", "events", "retention", "funnel"
    pub metric_type: String,
    /// Custom title (optional, defaults to metric type name)
    pub title: Option<String>,
    /// Filter to apply to the metric
    #[serde(default)]
    pub filter: serde_json::Value,
    /// Position and size on the board
    pub position: BlockPosition,
    /// Visualization type
    pub visualization: Option<VisualizationType>,
}

impl MetricBlock {
    pub fn new(metric_type: &str, position: BlockPosition) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            metric_type: metric_type.to_string(),
            title: None,
            filter: serde_json::Value::Object(serde_json::Map::new()),
            position,
            visualization: None,
        }
    }
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
        Self { x, y, width, height }
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
        let board = Board::new("ws_1", "user_1", "Dashboard")
            .with_description("A test dashboard");
        assert_eq!(board.description, Some("A test dashboard".to_string()));
    }

    #[test]
    fn test_metric_block() {
        let pos = BlockPosition::new(0, 0, 4, 2);
        let block = MetricBlock::new("dau", pos);
        assert_eq!(block.metric_type, "dau");
        assert!(block.title.is_none());
    }

    #[test]
    fn test_block_serialization() {
        let pos = BlockPosition::new(0, 0, 4, 2);
        let block = Block::Metric(MetricBlock::new("dau", pos));
        let json = serde_json::to_string(&block).unwrap();
        assert!(json.contains("\"type\":\"metric\""));
        assert!(json.contains("\"metric_type\":\"dau\""));
    }
}
