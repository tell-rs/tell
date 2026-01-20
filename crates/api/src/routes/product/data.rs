//! Data query routes
//!
//! Endpoints for raw data queries and schema discovery.
//!
//! # Routes
//!
//! - `POST /api/v1/data/query` - Execute a data query
//! - `GET /api/v1/data/sources` - List available data sources
//! - `GET /api/v1/data/sources/{source}/fields` - Get fields for a source
//! - `GET /api/v1/data/sources/{source}/values/{field}` - Get distinct values for a field

use axum::{
    extract::{Path, Query, State},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};

use crate::auth::{AuthUser, WorkspaceId};
use crate::error::ApiError;
use crate::state::AppState;
use crate::types::ApiResponse;

/// Build the data query router
pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/query", post(execute_query))
        .route("/sources", get(list_sources))
        .route("/sources/{source}/fields", get(get_source_fields))
        .route("/sources/{source}/values/{field}", get(get_field_values))
}

// ============================================================================
// Types
// ============================================================================

/// Data query request
#[derive(Debug, Deserialize)]
pub struct DataQueryRequest {
    /// SQL query to execute
    pub query: String,
    /// Maximum rows to return (default 1000, max 10000)
    #[serde(default = "default_limit")]
    pub limit: u32,
}

fn default_limit() -> u32 {
    1000
}

/// Data query response
#[derive(Debug, Serialize)]
pub struct DataQueryResponse {
    /// Column names
    pub columns: Vec<ColumnInfo>,
    /// Row data
    pub rows: Vec<Vec<serde_json::Value>>,
    /// Total rows returned
    pub row_count: usize,
    /// Query execution time in milliseconds
    pub execution_time_ms: u64,
}

/// Column information
#[derive(Debug, Serialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
}

/// Available data source
#[derive(Debug, Serialize)]
pub struct DataSource {
    pub id: String,
    pub name: String,
    pub description: String,
    pub table: String,
}

/// List sources response
#[derive(Debug, Serialize)]
pub struct ListSourcesResponse {
    pub sources: Vec<DataSource>,
}

/// Field information
#[derive(Debug, Serialize)]
pub struct FieldInfo {
    pub name: String,
    pub data_type: String,
    pub description: Option<String>,
}

/// List fields response
#[derive(Debug, Serialize)]
pub struct ListFieldsResponse {
    pub source: String,
    pub fields: Vec<FieldInfo>,
}

/// Field values query params
#[derive(Debug, Deserialize)]
pub struct FieldValuesQuery {
    /// Search filter for values
    #[serde(default)]
    pub search: Option<String>,
    /// Maximum values to return (default 100)
    #[serde(default = "default_values_limit")]
    pub limit: u32,
}

fn default_values_limit() -> u32 {
    100
}

/// Field values response
#[derive(Debug, Serialize)]
pub struct FieldValuesResponse {
    pub source: String,
    pub field: String,
    pub values: Vec<String>,
}

// ============================================================================
// Handlers
// ============================================================================

/// Execute a data query
///
/// POST /api/v1/data/query
///
/// Executes a SQL query against the analytics database.
/// Queries are scoped to the user's workspace.
async fn execute_query(
    State(state): State<AppState>,
    user: AuthUser,
    workspace: WorkspaceId,
    Json(req): Json<DataQueryRequest>,
) -> Result<Json<ApiResponse<DataQueryResponse>>, ApiError> {
    // Verify user has access to workspace
    check_workspace_membership(&state, workspace.0, &user).await?;

    // Validate query
    let query = req.query.trim();
    if query.is_empty() {
        return Err(ApiError::validation("query", "cannot be empty"));
    }

    // Enforce SELECT-only for security
    let query_upper = query.to_uppercase();
    if !query_upper.starts_with("SELECT") {
        return Err(ApiError::validation(
            "query",
            "only SELECT queries are allowed",
        ));
    }

    // Check for dangerous keywords
    let dangerous = ["INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE"];
    for keyword in dangerous {
        if query_upper.contains(keyword) {
            return Err(ApiError::validation(
                "query",
                format!("{} is not allowed in queries", keyword),
            ));
        }
    }

    // Enforce limit
    let limit = req.limit.min(10000);

    // Build workspace-scoped query
    let workspace_id = workspace.0;
    let scoped_query = scope_query_to_workspace(query, workspace_id, limit);

    // Execute query
    let start = std::time::Instant::now();
    let result = state
        .metrics
        .backend()
        .execute(&scoped_query)
        .await
        .map_err(|e| ApiError::internal(format!("Query execution failed: {}", e)))?;
    let execution_time_ms = start.elapsed().as_millis() as u64;

    // Convert to response
    let columns: Vec<ColumnInfo> = result
        .columns
        .iter()
        .map(|c| ColumnInfo {
            name: c.name.clone(),
            data_type: format!("{:?}", c.data_type).to_lowercase(),
        })
        .collect();

    Ok(Json(ApiResponse::new(DataQueryResponse {
        columns,
        rows: result.rows,
        row_count: result.row_count,
        execution_time_ms,
    })))
}

/// List available data sources
///
/// GET /api/v1/data/sources
async fn list_sources(
    State(state): State<AppState>,
    user: AuthUser,
    workspace: WorkspaceId,
) -> Result<Json<ApiResponse<ListSourcesResponse>>, ApiError> {
    // Verify user has access to workspace
    check_workspace_membership(&state, workspace.0, &user).await?;

    let sources = vec![
        DataSource {
            id: "events".to_string(),
            name: "Events".to_string(),
            description: "Product analytics events".to_string(),
            table: "events_v1".to_string(),
        },
        DataSource {
            id: "logs".to_string(),
            name: "Logs".to_string(),
            description: "Application logs".to_string(),
            table: "logs_v1".to_string(),
        },
        DataSource {
            id: "context".to_string(),
            name: "Context".to_string(),
            description: "User and session context".to_string(),
            table: "context_v1".to_string(),
        },
        DataSource {
            id: "user_traits".to_string(),
            name: "User Traits".to_string(),
            description: "User properties and attributes".to_string(),
            table: "user_traits_v1".to_string(),
        },
    ];

    Ok(Json(ApiResponse::new(ListSourcesResponse { sources })))
}

/// Get fields for a data source
///
/// GET /api/v1/data/sources/{source}/fields
async fn get_source_fields(
    State(state): State<AppState>,
    Path(source): Path<String>,
    user: AuthUser,
    workspace: WorkspaceId,
) -> Result<Json<ApiResponse<ListFieldsResponse>>, ApiError> {
    // Verify user has access to workspace
    check_workspace_membership(&state, workspace.0, &user).await?;

    let fields = match source.as_str() {
        "events" => vec![
            FieldInfo {
                name: "timestamp".to_string(),
                data_type: "DateTime".to_string(),
                description: Some("Event timestamp".to_string()),
            },
            FieldInfo {
                name: "event_name".to_string(),
                data_type: "String".to_string(),
                description: Some("Name of the event".to_string()),
            },
            FieldInfo {
                name: "device_id".to_string(),
                data_type: "String".to_string(),
                description: Some("Unique device identifier".to_string()),
            },
            FieldInfo {
                name: "session_id".to_string(),
                data_type: "String".to_string(),
                description: Some("Session identifier".to_string()),
            },
            FieldInfo {
                name: "properties".to_string(),
                data_type: "JSON".to_string(),
                description: Some("Event properties".to_string()),
            },
        ],
        "logs" => vec![
            FieldInfo {
                name: "timestamp".to_string(),
                data_type: "DateTime".to_string(),
                description: Some("Log timestamp".to_string()),
            },
            FieldInfo {
                name: "level".to_string(),
                data_type: "String".to_string(),
                description: Some("Log level (debug, info, warn, error)".to_string()),
            },
            FieldInfo {
                name: "message".to_string(),
                data_type: "String".to_string(),
                description: Some("Log message".to_string()),
            },
            FieldInfo {
                name: "source".to_string(),
                data_type: "String".to_string(),
                description: Some("Log source/component".to_string()),
            },
        ],
        "context" => vec![
            FieldInfo {
                name: "timestamp".to_string(),
                data_type: "DateTime".to_string(),
                description: Some("Context timestamp".to_string()),
            },
            FieldInfo {
                name: "device_id".to_string(),
                data_type: "String".to_string(),
                description: Some("Device identifier".to_string()),
            },
            FieldInfo {
                name: "session_id".to_string(),
                data_type: "String".to_string(),
                description: Some("Session identifier".to_string()),
            },
            FieldInfo {
                name: "user_id".to_string(),
                data_type: "String".to_string(),
                description: Some("User identifier".to_string()),
            },
            FieldInfo {
                name: "device_type".to_string(),
                data_type: "String".to_string(),
                description: Some("Device type (mobile, desktop, tablet)".to_string()),
            },
            FieldInfo {
                name: "os".to_string(),
                data_type: "String".to_string(),
                description: Some("Operating system".to_string()),
            },
            FieldInfo {
                name: "country".to_string(),
                data_type: "String".to_string(),
                description: Some("Country code".to_string()),
            },
        ],
        "user_traits" => vec![
            FieldInfo {
                name: "user_id".to_string(),
                data_type: "String".to_string(),
                description: Some("User identifier".to_string()),
            },
            FieldInfo {
                name: "trait_name".to_string(),
                data_type: "String".to_string(),
                description: Some("Trait name".to_string()),
            },
            FieldInfo {
                name: "trait_value".to_string(),
                data_type: "String".to_string(),
                description: Some("Trait value".to_string()),
            },
            FieldInfo {
                name: "updated_at".to_string(),
                data_type: "DateTime".to_string(),
                description: Some("Last update timestamp".to_string()),
            },
        ],
        _ => {
            return Err(ApiError::not_found("data_source", &source));
        }
    };

    Ok(Json(ApiResponse::new(ListFieldsResponse {
        source,
        fields,
    })))
}

/// Get distinct values for a field
///
/// GET /api/v1/data/sources/{source}/values/{field}
async fn get_field_values(
    State(state): State<AppState>,
    Path((source, field)): Path<(String, String)>,
    user: AuthUser,
    workspace: WorkspaceId,
    Query(params): Query<FieldValuesQuery>,
) -> Result<Json<ApiResponse<FieldValuesResponse>>, ApiError> {
    // Verify user has access to workspace
    check_workspace_membership(&state, workspace.0, &user).await?;

    // Get table name for source (whitelist validation)
    let table = match source.as_str() {
        "events" => "events_v1",
        "logs" => "logs_v1",
        "context" => "context_v1",
        "user_traits" => "user_traits_v1",
        _ => {
            return Err(ApiError::not_found("data_source", &source));
        }
    };

    // Whitelist valid field names per table to prevent SQL injection
    let valid_fields = match source.as_str() {
        "events" => &["timestamp", "event_name", "device_id", "session_id", "properties"][..],
        "logs" => &["timestamp", "level", "message", "source"][..],
        "context" => &["timestamp", "device_id", "session_id", "user_id", "device_type", "os", "country"][..],
        "user_traits" => &["user_id", "trait_name", "trait_value", "updated_at"][..],
        _ => &[][..],
    };

    if !valid_fields.contains(&field.as_str()) {
        return Err(ApiError::validation("field", "invalid field name for this source"));
    }

    let limit = params.limit.min(1000);
    let workspace_id = workspace.0;

    // Build query for distinct values
    // Field name is validated via whitelist above, so safe to include directly
    let query = if let Some(search) = &params.search {
        // Validate search string length and content
        if search.len() > 100 {
            return Err(ApiError::validation("search", "search term too long (max 100 chars)"));
        }
        // Allow only safe characters in search
        if !search.chars().all(|c| c.is_alphanumeric() || c == ' ' || c == '-' || c == '_' || c == '.') {
            return Err(ApiError::validation("search", "search contains invalid characters"));
        }
        format!(
            "SELECT DISTINCT {} FROM {}.{} WHERE {} LIKE '%{}%' LIMIT {}",
            field, workspace_id, table, field, search, limit
        )
    } else {
        format!(
            "SELECT DISTINCT {} FROM {}.{} LIMIT {}",
            field, workspace_id, table, limit
        )
    };

    // Execute query
    let result = state
        .metrics
        .backend()
        .execute(&query)
        .await
        .map_err(|e| ApiError::internal(format!("Query failed: {}", e)))?;

    // Extract values
    let values: Vec<String> = result
        .rows
        .iter()
        .filter_map(|row| row.first())
        .filter_map(|v| v.as_str().map(|s| s.to_string()))
        .collect();

    Ok(Json(ApiResponse::new(FieldValuesResponse {
        source,
        field,
        values,
    })))
}

// ============================================================================
// Helpers
// ============================================================================

/// Scope a query to a specific workspace by rewriting table names
fn scope_query_to_workspace(query: &str, workspace_id: u64, limit: u32) -> String {
    // Simple table name rewriting
    // In production, you'd want a proper SQL parser
    let tables = ["events_v1", "logs_v1", "context_v1", "user_traits_v1"];

    let mut result = query.to_string();
    for table in tables {
        // Replace standalone table references
        let from_pattern = format!("FROM {}", table);
        let from_replacement = format!("FROM {}.{}", workspace_id, table);
        result = result.replace(&from_pattern, &from_replacement);

        let from_pattern_lower = format!("from {}", table);
        result = result.replace(&from_pattern_lower, &from_replacement);

        // Replace JOIN table references
        let join_pattern = format!("JOIN {}", table);
        let join_replacement = format!("JOIN {}.{}", workspace_id, table);
        result = result.replace(&join_pattern, &join_replacement);

        let join_pattern_lower = format!("join {}", table);
        result = result.replace(&join_pattern_lower, &join_replacement);
    }

    // Append LIMIT if not present
    let upper = result.to_uppercase();
    if !upper.contains("LIMIT") {
        result.push_str(&format!(" LIMIT {}", limit));
    }

    result
}

/// Verify user has access to the workspace
async fn check_workspace_membership(
    state: &AppState,
    workspace_id: u64,
    user: &AuthUser,
) -> Result<(), ApiError> {
    // Platform admins can access any workspace
    if user.has_permission(tell_auth::Permission::Platform) {
        return Ok(());
    }

    // Check workspace membership via control plane
    if let Some(control) = state.control.as_ref() {
        let ws_id = workspace_id.to_string();
        let membership = control
            .workspaces()
            .get_member(&ws_id, &user.id)
            .await
            .map_err(|e| ApiError::internal(format!("Failed to check membership: {}", e)))?;

        if membership.is_some() {
            return Ok(());
        }
    } else {
        // No control plane - allow access (development mode)
        return Ok(());
    }

    Err(ApiError::forbidden("Not a member of this workspace"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scope_query_simple() {
        let query = "SELECT * FROM events_v1";
        let scoped = scope_query_to_workspace(query, 123, 100);
        assert!(scoped.contains("FROM 123.events_v1"));
        assert!(scoped.contains("LIMIT 100"));
    }

    #[test]
    fn test_scope_query_with_join() {
        let query = "SELECT * FROM events_v1 JOIN context_v1 ON events_v1.device_id = context_v1.device_id";
        let scoped = scope_query_to_workspace(query, 456, 50);
        assert!(scoped.contains("FROM 456.events_v1"));
        assert!(scoped.contains("JOIN 456.context_v1"));
    }

    #[test]
    fn test_scope_query_preserves_existing_limit() {
        let query = "SELECT * FROM logs_v1 LIMIT 10";
        let scoped = scope_query_to_workspace(query, 789, 1000);
        assert!(scoped.contains("FROM 789.logs_v1"));
        // Should not add another LIMIT
        assert_eq!(scoped.matches("LIMIT").count(), 1);
    }
}
