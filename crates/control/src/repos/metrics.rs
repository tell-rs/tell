//! Metric repository
//!
//! CRUD operations for saved metrics within a workspace database.

use chrono::{DateTime, Utc};
use turso::Database;

use crate::error::{ControlError, Result};
use crate::models::{DisplayConfig, MetricQueryConfig, SavedMetric};

/// Saved metric repository
///
/// Operates on a workspace database (not the control database).
pub struct MetricRepo<'a> {
    db: &'a Database,
}

impl<'a> MetricRepo<'a> {
    /// Create a new metric repository
    pub fn new(db: &'a Database) -> Self {
        Self { db }
    }

    // =========================================================================
    // Metric CRUD
    // =========================================================================

    /// Create a new saved metric
    pub async fn create(&self, metric: &SavedMetric) -> Result<()> {
        let conn = self.db.connect()?;

        let query_json = serde_json::to_string(&metric.query)?;
        let display_json = metric
            .display
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?;
        let created_at = metric.created_at.to_rfc3339();
        let updated_at = metric.updated_at.to_rfc3339();

        conn.execute(
            r#"
            INSERT INTO saved_metrics (id, workspace_id, owner_id, title, description, query, display, created_at, updated_at)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
            "#,
            [
                metric.id.as_str(),
                metric.workspace_id.as_str(),
                metric.owner_id.as_str(),
                metric.title.as_str(),
                metric.description.as_deref().unwrap_or(""),
                query_json.as_str(),
                display_json.as_deref().unwrap_or(""),
                created_at.as_str(),
                updated_at.as_str(),
            ],
        )
        .await?;

        Ok(())
    }

    /// Get a saved metric by ID
    pub async fn get_by_id(&self, id: &str) -> Result<Option<SavedMetric>> {
        let conn = self.db.connect()?;

        let mut rows = conn
            .query("SELECT * FROM saved_metrics WHERE id = ?1", [id])
            .await?;

        if let Some(row) = rows.next().await? {
            Ok(Some(Self::row_to_metric(&row)?))
        } else {
            Ok(None)
        }
    }

    /// List all saved metrics in the workspace
    pub async fn list_for_workspace(&self, workspace_id: &str) -> Result<Vec<SavedMetric>> {
        let conn = self.db.connect()?;

        let mut rows = conn
            .query(
                "SELECT * FROM saved_metrics WHERE workspace_id = ?1 ORDER BY created_at DESC",
                [workspace_id],
            )
            .await?;

        let mut metrics = Vec::new();
        while let Some(row) = rows.next().await? {
            metrics.push(Self::row_to_metric(&row)?);
        }

        Ok(metrics)
    }

    /// List saved metrics owned by a user
    pub async fn list_for_owner(&self, owner_id: &str) -> Result<Vec<SavedMetric>> {
        let conn = self.db.connect()?;

        let mut rows = conn
            .query(
                "SELECT * FROM saved_metrics WHERE owner_id = ?1 ORDER BY created_at DESC",
                [owner_id],
            )
            .await?;

        let mut metrics = Vec::new();
        while let Some(row) = rows.next().await? {
            metrics.push(Self::row_to_metric(&row)?);
        }

        Ok(metrics)
    }

    /// Update a saved metric
    pub async fn update(&self, metric: &SavedMetric) -> Result<()> {
        let conn = self.db.connect()?;

        let query_json = serde_json::to_string(&metric.query)?;
        let display_json = metric
            .display
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?;
        let updated_at = Utc::now().to_rfc3339();

        let affected = conn
            .execute(
                r#"
                UPDATE saved_metrics
                SET title = ?1, description = ?2, query = ?3, display = ?4, updated_at = ?5
                WHERE id = ?6
                "#,
                [
                    metric.title.as_str(),
                    metric.description.as_deref().unwrap_or(""),
                    query_json.as_str(),
                    display_json.as_deref().unwrap_or(""),
                    updated_at.as_str(),
                    metric.id.as_str(),
                ],
            )
            .await?;

        if affected == 0 {
            return Err(ControlError::not_found("saved_metric", &metric.id));
        }

        Ok(())
    }

    /// Delete a saved metric
    pub async fn delete(&self, id: &str) -> Result<()> {
        let conn = self.db.connect()?;

        let affected = conn
            .execute("DELETE FROM saved_metrics WHERE id = ?1", [id])
            .await?;

        if affected == 0 {
            return Err(ControlError::not_found("saved_metric", id));
        }

        Ok(())
    }

    // =========================================================================
    // Row conversion helper
    // =========================================================================

    fn row_to_metric(row: &turso::Row) -> Result<SavedMetric> {
        let id = row
            .get_value(0)?
            .as_text()
            .unwrap_or(&String::new())
            .clone();
        let workspace_id = row
            .get_value(1)?
            .as_text()
            .unwrap_or(&String::new())
            .clone();
        let owner_id = row
            .get_value(2)?
            .as_text()
            .unwrap_or(&String::new())
            .clone();
        let title = row
            .get_value(3)?
            .as_text()
            .unwrap_or(&String::new())
            .clone();
        let description_raw = row.get_value(4)?.as_text().cloned();
        let query_json = row
            .get_value(5)?
            .as_text()
            .unwrap_or(&String::from("{}"))
            .clone();
        let display_json = row.get_value(6)?.as_text().cloned();
        let created_at_str = row
            .get_value(7)?
            .as_text()
            .unwrap_or(&String::new())
            .clone();
        let updated_at_str = row
            .get_value(8)?
            .as_text()
            .unwrap_or(&String::new())
            .clone();

        let description = description_raw.filter(|s| !s.is_empty());
        let query: MetricQueryConfig = serde_json::from_str(&query_json)?;
        let display: Option<DisplayConfig> = display_json
            .filter(|s| !s.is_empty())
            .map(|s| serde_json::from_str(&s))
            .transpose()?;

        let created_at = DateTime::parse_from_rfc3339(&created_at_str)
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now());
        let updated_at = DateTime::parse_from_rfc3339(&updated_at_str)
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now());

        Ok(SavedMetric {
            id,
            workspace_id,
            owner_id,
            title,
            description,
            query,
            display,
            created_at,
            updated_at,
        })
    }
}
