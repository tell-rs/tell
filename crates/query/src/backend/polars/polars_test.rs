//! Tests for Polars backend

use super::*;
use std::fs;
use tempfile::tempdir;

// =============================================================================
// Test Helpers
// =============================================================================

/// Create a test Arrow IPC file with sample event data
fn create_test_events_file(dir: &Path, workspace_id: u64) -> PathBuf {
    use arrow::array::{BinaryArray, Int64Array, StringArray, UInt64Array};
    use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
    use arrow::ipc::writer::FileWriter;
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    // Create directory structure: {workspace_id}/2024-01-15/14/
    let data_dir = dir.join(format!("{}/2024-01-15/14", workspace_id));
    fs::create_dir_all(&data_dir).unwrap();

    let file_path = data_dir.join("events_v1.arrow");

    // Create schema matching our Arrow IPC sink
    let schema = Schema::new(vec![
        Field::new("timestamp", ArrowDataType::Int64, false),
        Field::new("batch_timestamp", ArrowDataType::Int64, false),
        Field::new("workspace_id", ArrowDataType::UInt64, false),
        Field::new("event_type", ArrowDataType::Utf8, false),
        Field::new("event_name", ArrowDataType::Utf8, true),
        Field::new("device_id", ArrowDataType::Binary, true),
        Field::new("session_id", ArrowDataType::Binary, true),
        Field::new("source_ip", ArrowDataType::Binary, false),
        Field::new("payload", ArrowDataType::Binary, false),
    ]);

    // Create test data
    let timestamps = Int64Array::from(vec![1700000000000i64, 1700000001000, 1700000002000]);
    let batch_timestamps = Int64Array::from(vec![1700000000100i64, 1700000001100, 1700000002100]);
    let workspace_ids = UInt64Array::from(vec![workspace_id, workspace_id, workspace_id]);
    let event_types = StringArray::from(vec!["track", "track", "identify"]);
    let event_names = StringArray::from(vec![Some("page_view"), Some("button_click"), None]);
    let device_ids = BinaryArray::from(vec![
        Some(vec![1u8; 16].as_slice()),
        Some(vec![1u8; 16].as_slice()),
        Some(vec![2u8; 16].as_slice()),
    ]);
    let session_ids = BinaryArray::from(vec![
        Some(vec![10u8; 16].as_slice()),
        Some(vec![10u8; 16].as_slice()),
        None::<&[u8]>,
    ]);
    let source_ips = BinaryArray::from(vec![
        vec![0u8; 16].as_slice(),
        vec![0u8; 16].as_slice(),
        vec![0u8; 16].as_slice(),
    ]);
    let payloads = BinaryArray::from(vec![
        b"{\"page\": \"/home\"}".as_slice(),
        b"{\"button\": \"submit\"}".as_slice(),
        b"{\"user_id\": \"u123\"}".as_slice(),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(timestamps),
            Arc::new(batch_timestamps),
            Arc::new(workspace_ids),
            Arc::new(event_types),
            Arc::new(event_names),
            Arc::new(device_ids),
            Arc::new(session_ids),
            Arc::new(source_ips),
            Arc::new(payloads),
        ],
    )
    .unwrap();

    // Write to file
    let file = fs::File::create(&file_path).unwrap();
    let mut writer = FileWriter::try_new(file, &schema).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();

    file_path
}

/// Create a test Arrow IPC file with sample log data
fn create_test_logs_file(dir: &Path, workspace_id: u64) -> PathBuf {
    use arrow::array::{BinaryArray, Int64Array, StringArray, UInt64Array};
    use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
    use arrow::ipc::writer::FileWriter;
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    let data_dir = dir.join(format!("{}/2024-01-15/14", workspace_id));
    fs::create_dir_all(&data_dir).unwrap();

    let file_path = data_dir.join("logs_v1.arrow");

    let schema = Schema::new(vec![
        Field::new("timestamp", ArrowDataType::Int64, false),
        Field::new("batch_timestamp", ArrowDataType::Int64, false),
        Field::new("workspace_id", ArrowDataType::UInt64, false),
        Field::new("level", ArrowDataType::Utf8, false),
        Field::new("event_type", ArrowDataType::Utf8, false),
        Field::new("source", ArrowDataType::Utf8, true),
        Field::new("service", ArrowDataType::Utf8, true),
        Field::new("session_id", ArrowDataType::Binary, true),
        Field::new("source_ip", ArrowDataType::Binary, false),
        Field::new("payload", ArrowDataType::Binary, false),
    ]);

    let timestamps = Int64Array::from(vec![1700000000000i64, 1700000001000]);
    let batch_timestamps = Int64Array::from(vec![1700000000100i64, 1700000001100]);
    let workspace_ids = UInt64Array::from(vec![workspace_id, workspace_id]);
    let levels = StringArray::from(vec!["info", "error"]);
    let event_types = StringArray::from(vec!["log", "log"]);
    let sources = StringArray::from(vec![Some("web-1"), Some("api-1")]);
    let services = StringArray::from(vec![Some("nginx"), Some("api")]);
    let session_ids = BinaryArray::from(vec![None::<&[u8]>, None::<&[u8]>]);
    let source_ips = BinaryArray::from(vec![vec![0u8; 16].as_slice(), vec![0u8; 16].as_slice()]);
    let payloads = BinaryArray::from(vec![
        b"GET /health 200".as_slice(),
        b"Connection refused".as_slice(),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(timestamps),
            Arc::new(batch_timestamps),
            Arc::new(workspace_ids),
            Arc::new(levels),
            Arc::new(event_types),
            Arc::new(sources),
            Arc::new(services),
            Arc::new(session_ids),
            Arc::new(source_ips),
            Arc::new(payloads),
        ],
    )
    .unwrap();

    let file = fs::File::create(&file_path).unwrap();
    let mut writer = FileWriter::try_new(file, &schema).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();

    file_path
}

// =============================================================================
// Backend Tests
// =============================================================================

#[tokio::test]
async fn test_polars_backend_new() {
    let backend = PolarsBackend::new("/tmp/test", 42);
    assert_eq!(backend.name(), "polars");
    assert_eq!(backend.workspace_id, 42);
}

#[tokio::test]
async fn test_discover_files() {
    let dir = tempdir().unwrap();
    let workspace_id = 1;

    // Create test file
    create_test_events_file(dir.path(), workspace_id);

    let backend = PolarsBackend::new(dir.path(), workspace_id);
    let files = backend.discover_files("events_v1").unwrap();

    assert_eq!(files.len(), 1);
    assert!(files[0].ends_with("events_v1.arrow"));
}

#[tokio::test]
async fn test_discover_files_not_found() {
    let dir = tempdir().unwrap();
    let backend = PolarsBackend::new(dir.path(), 999);

    let result = backend.discover_files("events_v1");
    assert!(result.is_err());
}

#[tokio::test]
async fn test_table_exists() {
    let dir = tempdir().unwrap();
    let workspace_id = 1;

    create_test_events_file(dir.path(), workspace_id);

    let backend = PolarsBackend::new(dir.path(), workspace_id);

    assert!(backend.table_exists("events_v1"));
    assert!(!backend.table_exists("logs_v1"));
    assert!(!backend.table_exists("nonexistent"));
}

#[tokio::test]
async fn test_health_check_ok() {
    let dir = tempdir().unwrap();
    let workspace_id = 1;

    create_test_events_file(dir.path(), workspace_id);

    let backend = PolarsBackend::new(dir.path(), workspace_id);
    let result = backend.health_check().await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_health_check_no_data() {
    let dir = tempdir().unwrap();
    let backend = PolarsBackend::new(dir.path(), 999);

    let result = backend.health_check().await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_list_tables() {
    let dir = tempdir().unwrap();
    let workspace_id = 1;

    create_test_events_file(dir.path(), workspace_id);
    create_test_logs_file(dir.path(), workspace_id);

    let backend = PolarsBackend::new(dir.path(), workspace_id);
    let tables = backend.list_tables().await.unwrap();

    assert_eq!(tables.len(), 2);

    let table_names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
    assert!(table_names.contains(&"events_v1"));
    assert!(table_names.contains(&"logs_v1"));
}

// =============================================================================
// Query Execution Tests
// =============================================================================

#[tokio::test]
async fn test_simple_select() {
    let dir = tempdir().unwrap();
    let workspace_id = 1;

    create_test_events_file(dir.path(), workspace_id);

    let backend = PolarsBackend::new(dir.path(), workspace_id);
    let result = backend.execute("SELECT * FROM events_v1").await.unwrap();

    assert_eq!(result.row_count, 3);
    assert_eq!(result.columns.len(), 9);
}

#[tokio::test]
async fn test_select_with_limit() {
    let dir = tempdir().unwrap();
    let workspace_id = 1;

    create_test_events_file(dir.path(), workspace_id);

    let backend = PolarsBackend::new(dir.path(), workspace_id);
    let result = backend
        .execute("SELECT * FROM events_v1 LIMIT 2")
        .await
        .unwrap();

    assert_eq!(result.row_count, 2);
}

#[tokio::test]
async fn test_select_with_where() {
    let dir = tempdir().unwrap();
    let workspace_id = 1;

    create_test_events_file(dir.path(), workspace_id);

    let backend = PolarsBackend::new(dir.path(), workspace_id);
    let result = backend
        .execute("SELECT * FROM events_v1 WHERE event_type = 'track'")
        .await
        .unwrap();

    assert_eq!(result.row_count, 2);
}

#[tokio::test]
async fn test_select_count() {
    let dir = tempdir().unwrap();
    let workspace_id = 1;

    create_test_events_file(dir.path(), workspace_id);

    let backend = PolarsBackend::new(dir.path(), workspace_id);

    // Note: Polars SQL broadcasts aggregates to all rows without explicit GROUP BY
    // Use LIMIT 1 to get a single result row
    let result = backend
        .execute("SELECT COUNT(*) as count FROM events_v1 LIMIT 1")
        .await
        .unwrap();

    assert_eq!(result.row_count, 1);
    assert_eq!(result.columns[0].name, "count");

    // Check the count value (3 events in test data)
    let count = &result.rows[0][0];
    assert_eq!(count.as_u64(), Some(3));
}

#[tokio::test]
async fn test_select_group_by() {
    let dir = tempdir().unwrap();
    let workspace_id = 1;

    create_test_events_file(dir.path(), workspace_id);

    let backend = PolarsBackend::new(dir.path(), workspace_id);
    let result = backend
        .execute("SELECT event_type, COUNT(*) as count FROM events_v1 GROUP BY event_type")
        .await
        .unwrap();

    assert_eq!(result.row_count, 2); // track and identify
}

#[tokio::test]
async fn test_select_specific_columns() {
    let dir = tempdir().unwrap();
    let workspace_id = 1;

    create_test_events_file(dir.path(), workspace_id);

    let backend = PolarsBackend::new(dir.path(), workspace_id);
    let result = backend
        .execute("SELECT timestamp, event_name FROM events_v1")
        .await
        .unwrap();

    assert_eq!(result.columns.len(), 2);
    assert_eq!(result.columns[0].name, "timestamp");
    assert_eq!(result.columns[1].name, "event_name");
}

#[tokio::test]
async fn test_query_logs_table() {
    let dir = tempdir().unwrap();
    let workspace_id = 1;

    create_test_logs_file(dir.path(), workspace_id);

    let backend = PolarsBackend::new(dir.path(), workspace_id);
    let result = backend
        .execute("SELECT * FROM logs_v1 WHERE level = 'error'")
        .await
        .unwrap();

    assert_eq!(result.row_count, 1);
}

#[tokio::test]
async fn test_invalid_sql_insert() {
    let dir = tempdir().unwrap();
    let workspace_id = 1;

    create_test_events_file(dir.path(), workspace_id);

    let backend = PolarsBackend::new(dir.path(), workspace_id);
    let result = backend.execute("INSERT INTO events VALUES (1)").await;

    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), QueryError::InvalidSql(_)));
}

#[tokio::test]
async fn test_invalid_sql_delete() {
    let dir = tempdir().unwrap();
    let workspace_id = 1;

    create_test_events_file(dir.path(), workspace_id);

    let backend = PolarsBackend::new(dir.path(), workspace_id);
    let result = backend.execute("DELETE FROM events_v1").await;

    assert!(result.is_err());
}

#[tokio::test]
async fn test_table_not_found() {
    let dir = tempdir().unwrap();
    let workspace_id = 1;

    create_test_events_file(dir.path(), workspace_id);

    let backend = PolarsBackend::new(dir.path(), workspace_id);
    let result = backend.execute("SELECT * FROM nonexistent").await;

    assert!(result.is_err());
}

// =============================================================================
// Result Conversion Tests
// =============================================================================

#[tokio::test]
async fn test_result_column_types() {
    let dir = tempdir().unwrap();
    let workspace_id = 1;

    create_test_events_file(dir.path(), workspace_id);

    let backend = PolarsBackend::new(dir.path(), workspace_id);
    let result = backend
        .execute("SELECT * FROM events_v1 LIMIT 1")
        .await
        .unwrap();

    // Check column types
    assert_eq!(result.columns[0].name, "timestamp");
    assert_eq!(result.columns[0].data_type, DataType::Int64);

    assert_eq!(result.columns[2].name, "workspace_id");
    assert_eq!(result.columns[2].data_type, DataType::UInt64);

    assert_eq!(result.columns[3].name, "event_type");
    assert_eq!(result.columns[3].data_type, DataType::String);
}

#[tokio::test]
async fn test_null_values() {
    let dir = tempdir().unwrap();
    let workspace_id = 1;

    create_test_events_file(dir.path(), workspace_id);

    let backend = PolarsBackend::new(dir.path(), workspace_id);
    let result = backend
        .execute("SELECT event_name FROM events_v1 WHERE event_type = 'identify'")
        .await
        .unwrap();

    assert_eq!(result.row_count, 1);
    // identify event has null event_name
    assert!(result.rows[0][0].is_null());
}

// =============================================================================
// Base64 Encoding Tests
// =============================================================================

#[test]
fn test_base64_encode_empty() {
    assert_eq!(base64_encode(&[]), "");
}

#[test]
fn test_base64_encode_simple() {
    assert_eq!(base64_encode(b"hello"), "aGVsbG8=");
    assert_eq!(base64_encode(b"world"), "d29ybGQ=");
}

#[test]
fn test_base64_encode_binary() {
    let data = vec![0u8, 1, 2, 3, 255];
    let encoded = base64_encode(&data);
    assert!(!encoded.is_empty());
}

// =============================================================================
// Workspace Prefix Stripping Tests
// =============================================================================

#[test]
fn test_strip_workspace_prefix_basic() {
    let backend = PolarsBackend::new("/tmp/test", 1);

    // Should strip workspace prefix for known tables
    assert_eq!(
        backend.strip_workspace_prefix("SELECT * FROM 1.events_v1"),
        "SELECT * FROM events_v1"
    );
    assert_eq!(
        backend.strip_workspace_prefix("SELECT * FROM 123.logs_v1 WHERE x = 1"),
        "SELECT * FROM logs_v1 WHERE x = 1"
    );
}

#[test]
fn test_strip_workspace_prefix_multiple_tables() {
    let backend = PolarsBackend::new("/tmp/test", 1);

    let sql = "SELECT e.* FROM 1.events_v1 e JOIN 1.context_v1 c ON e.device_id = c.device_id";
    let expected = "SELECT e.* FROM events_v1 e JOIN context_v1 c ON e.device_id = c.device_id";
    assert_eq!(backend.strip_workspace_prefix(sql), expected);
}

#[test]
fn test_strip_workspace_prefix_preserves_unknown() {
    let backend = PolarsBackend::new("/tmp/test", 1);

    // Should NOT strip prefix for unknown tables
    assert_eq!(
        backend.strip_workspace_prefix("SELECT * FROM 1.unknown_table"),
        "SELECT * FROM 1.unknown_table"
    );

    // Should preserve floating point numbers
    assert_eq!(
        backend.strip_workspace_prefix("SELECT 1.5 FROM events_v1"),
        "SELECT 1.5 FROM events_v1"
    );
}

#[test]
fn test_strip_workspace_prefix_no_prefix() {
    let backend = PolarsBackend::new("/tmp/test", 1);

    // Should leave queries without prefix unchanged
    assert_eq!(
        backend.strip_workspace_prefix("SELECT * FROM events_v1"),
        "SELECT * FROM events_v1"
    );
}

#[tokio::test]
async fn test_execute_with_workspace_prefix() {
    let dir = tempdir().unwrap();
    let workspace_id = 42;

    create_test_events_file(dir.path(), workspace_id);

    let backend = PolarsBackend::new(dir.path(), workspace_id);

    // Query with workspace prefix (as analytics crate generates)
    let result = backend
        .execute("SELECT COUNT(*) as count FROM 42.events_v1 LIMIT 1")
        .await
        .unwrap();

    assert_eq!(result.row_count, 1);
    assert_eq!(result.rows[0][0].as_u64(), Some(3));
}
