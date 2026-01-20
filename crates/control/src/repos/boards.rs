//! Board repository
//!
//! CRUD operations for boards within a workspace database.

use chrono::{DateTime, Utc};
use turso::Database;

use crate::error::{ControlError, Result};
use crate::models::{Board, BoardSettings};

/// Board repository
///
/// Operates on a workspace database (not the control database).
pub struct BoardRepo<'a> {
    db: &'a Database,
}

impl<'a> BoardRepo<'a> {
    /// Create a new board repository
    pub fn new(db: &'a Database) -> Self {
        Self { db }
    }

    // =========================================================================
    // Board CRUD
    // =========================================================================

    /// Create a new board
    pub async fn create(&self, board: &Board) -> Result<()> {
        let conn = self.db.connect()?;

        let settings_json = serde_json::to_string(&board.settings)?;
        let created_at = board.created_at.to_rfc3339();
        let updated_at = board.updated_at.to_rfc3339();

        conn.execute(
            r#"
            INSERT INTO boards (id, workspace_id, owner_id, title, description, settings, is_pinned, created_at, updated_at)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
            "#,
            [
                board.id.as_str(),
                board.workspace_id.as_str(),
                board.owner_id.as_str(),
                board.title.as_str(),
                board.description.as_deref().unwrap_or(""),
                settings_json.as_str(),
                if board.is_pinned { "1" } else { "0" },
                created_at.as_str(),
                updated_at.as_str(),
            ],
        )
        .await?;

        Ok(())
    }

    /// Get a board by ID
    pub async fn get_by_id(&self, id: &str) -> Result<Option<Board>> {
        let conn = self.db.connect()?;

        let mut rows = conn
            .query("SELECT * FROM boards WHERE id = ?1", [id])
            .await?;

        if let Some(row) = rows.next().await? {
            Ok(Some(Self::row_to_board(&row)?))
        } else {
            Ok(None)
        }
    }

    /// List all boards in the workspace
    pub async fn list_for_workspace(&self, workspace_id: &str) -> Result<Vec<Board>> {
        let conn = self.db.connect()?;

        let mut rows = conn
            .query(
                "SELECT * FROM boards WHERE workspace_id = ?1 ORDER BY created_at DESC",
                [workspace_id],
            )
            .await?;

        let mut boards = Vec::new();
        while let Some(row) = rows.next().await? {
            boards.push(Self::row_to_board(&row)?);
        }

        Ok(boards)
    }

    /// List boards owned by a user
    pub async fn list_for_owner(&self, owner_id: &str) -> Result<Vec<Board>> {
        let conn = self.db.connect()?;

        let mut rows = conn
            .query(
                "SELECT * FROM boards WHERE owner_id = ?1 ORDER BY created_at DESC",
                [owner_id],
            )
            .await?;

        let mut boards = Vec::new();
        while let Some(row) = rows.next().await? {
            boards.push(Self::row_to_board(&row)?);
        }

        Ok(boards)
    }

    /// List pinned boards in the workspace
    pub async fn list_pinned(&self, workspace_id: &str) -> Result<Vec<Board>> {
        let conn = self.db.connect()?;

        let mut rows = conn
            .query(
                "SELECT * FROM boards WHERE workspace_id = ?1 AND is_pinned = 1 ORDER BY title",
                [workspace_id],
            )
            .await?;

        let mut boards = Vec::new();
        while let Some(row) = rows.next().await? {
            boards.push(Self::row_to_board(&row)?);
        }

        Ok(boards)
    }

    /// Update a board
    pub async fn update(&self, board: &Board) -> Result<()> {
        let conn = self.db.connect()?;

        let settings_json = serde_json::to_string(&board.settings)?;
        let updated_at = Utc::now().to_rfc3339();

        let affected = conn
            .execute(
                r#"
                UPDATE boards
                SET title = ?1, description = ?2, settings = ?3, is_pinned = ?4, updated_at = ?5
                WHERE id = ?6
                "#,
                [
                    board.title.as_str(),
                    board.description.as_deref().unwrap_or(""),
                    settings_json.as_str(),
                    if board.is_pinned { "1" } else { "0" },
                    updated_at.as_str(),
                    board.id.as_str(),
                ],
            )
            .await?;

        if affected == 0 {
            return Err(ControlError::not_found("board", &board.id));
        }

        Ok(())
    }

    /// Set a board's pinned status
    pub async fn set_pinned(&self, id: &str, pinned: bool) -> Result<()> {
        let conn = self.db.connect()?;

        let updated_at = Utc::now().to_rfc3339();

        let affected = conn
            .execute(
                "UPDATE boards SET is_pinned = ?1, updated_at = ?2 WHERE id = ?3",
                [if pinned { "1" } else { "0" }, updated_at.as_str(), id],
            )
            .await?;

        if affected == 0 {
            return Err(ControlError::not_found("board", id));
        }

        Ok(())
    }

    /// Delete a board
    pub async fn delete(&self, id: &str) -> Result<()> {
        let conn = self.db.connect()?;

        let affected = conn
            .execute("DELETE FROM boards WHERE id = ?1", [id])
            .await?;

        if affected == 0 {
            return Err(ControlError::not_found("board", id));
        }

        Ok(())
    }

    // =========================================================================
    // Row conversion helper
    // =========================================================================

    fn row_to_board(row: &turso::Row) -> Result<Board> {
        let id = row.get_value(0)?.as_text().unwrap_or(&String::new()).clone();
        let workspace_id = row.get_value(1)?.as_text().unwrap_or(&String::new()).clone();
        let owner_id = row.get_value(2)?.as_text().unwrap_or(&String::new()).clone();
        let title = row.get_value(3)?.as_text().unwrap_or(&String::new()).clone();
        let description_raw = row.get_value(4)?.as_text().cloned();
        let settings_json = row.get_value(5)?.as_text().unwrap_or(&String::from("{}")).clone();
        let is_pinned_int = *row.get_value(6)?.as_integer().unwrap_or(&0);
        let created_at_str = row.get_value(7)?.as_text().unwrap_or(&String::new()).clone();
        let updated_at_str = row.get_value(8)?.as_text().unwrap_or(&String::new()).clone();

        let description = description_raw.filter(|s| !s.is_empty());
        let settings: BoardSettings = serde_json::from_str(&settings_json).unwrap_or_default();
        let is_pinned = is_pinned_int != 0;

        let created_at = DateTime::parse_from_rfc3339(&created_at_str)
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now());
        let updated_at = DateTime::parse_from_rfc3339(&updated_at_str)
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now());

        Ok(Board {
            id,
            workspace_id,
            owner_id,
            title,
            description,
            settings,
            is_pinned,
            created_at,
            updated_at,
        })
    }
}
