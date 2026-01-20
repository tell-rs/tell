//! Tests for ClickHouse backend

use super::*;

// =============================================================================
// Type Conversion Tests
// =============================================================================

#[test]
fn test_infer_data_type_null() {
    assert_eq!(infer_data_type(&serde_json::Value::Null), DataType::Unknown);
}

#[test]
fn test_infer_data_type_bool() {
    assert_eq!(
        infer_data_type(&serde_json::Value::Bool(true)),
        DataType::Boolean
    );
}

#[test]
fn test_infer_data_type_int() {
    assert_eq!(
        infer_data_type(&serde_json::json!(42)),
        DataType::UInt64 // positive integers are UInt64
    );
    assert_eq!(infer_data_type(&serde_json::json!(-42)), DataType::Int64);
}

#[test]
fn test_infer_data_type_float() {
    assert_eq!(infer_data_type(&serde_json::json!(3.14)), DataType::Float64);
}

#[test]
fn test_infer_data_type_string() {
    assert_eq!(
        infer_data_type(&serde_json::json!("hello")),
        DataType::String
    );
}

#[test]
fn test_infer_data_type_array() {
    assert_eq!(
        infer_data_type(&serde_json::json!([1, 2, 3])),
        DataType::Json
    );
}

#[test]
fn test_infer_data_type_object() {
    assert_eq!(
        infer_data_type(&serde_json::json!({"key": "value"})),
        DataType::Json
    );
}

// =============================================================================
// ClickHouse Type Conversion Tests
// =============================================================================

#[test]
fn test_clickhouse_type_integers() {
    assert_eq!(clickhouse_type_to_datatype("Int8"), DataType::Int64);
    assert_eq!(clickhouse_type_to_datatype("Int16"), DataType::Int64);
    assert_eq!(clickhouse_type_to_datatype("Int32"), DataType::Int64);
    assert_eq!(clickhouse_type_to_datatype("Int64"), DataType::Int64);
    assert_eq!(clickhouse_type_to_datatype("UInt8"), DataType::UInt64);
    assert_eq!(clickhouse_type_to_datatype("UInt16"), DataType::UInt64);
    assert_eq!(clickhouse_type_to_datatype("UInt32"), DataType::UInt64);
    assert_eq!(clickhouse_type_to_datatype("UInt64"), DataType::UInt64);
}

#[test]
fn test_clickhouse_type_floats() {
    assert_eq!(clickhouse_type_to_datatype("Float32"), DataType::Float64);
    assert_eq!(clickhouse_type_to_datatype("Float64"), DataType::Float64);
}

#[test]
fn test_clickhouse_type_strings() {
    assert_eq!(clickhouse_type_to_datatype("String"), DataType::String);
    assert_eq!(
        clickhouse_type_to_datatype("FixedString(16)"),
        DataType::String
    );
}

#[test]
fn test_clickhouse_type_timestamps() {
    assert_eq!(clickhouse_type_to_datatype("Date"), DataType::Timestamp);
    assert_eq!(clickhouse_type_to_datatype("Date32"), DataType::Timestamp);
    assert_eq!(clickhouse_type_to_datatype("DateTime"), DataType::Timestamp);
    assert_eq!(
        clickhouse_type_to_datatype("DateTime64(3)"),
        DataType::Timestamp
    );
}

#[test]
fn test_clickhouse_type_nullable() {
    assert_eq!(
        clickhouse_type_to_datatype("Nullable(Int64)"),
        DataType::Int64
    );
    assert_eq!(
        clickhouse_type_to_datatype("Nullable(String)"),
        DataType::String
    );
}

#[test]
fn test_clickhouse_type_low_cardinality() {
    assert_eq!(
        clickhouse_type_to_datatype("LowCardinality(String)"),
        DataType::String
    );
}

#[test]
fn test_clickhouse_type_complex() {
    assert_eq!(clickhouse_type_to_datatype("Array(String)"), DataType::Json);
    assert_eq!(
        clickhouse_type_to_datatype("Map(String, Int64)"),
        DataType::Json
    );
    assert_eq!(
        clickhouse_type_to_datatype("Tuple(String, Int64)"),
        DataType::Json
    );
    assert_eq!(clickhouse_type_to_datatype("JSON"), DataType::Json);
}

#[test]
fn test_clickhouse_type_special() {
    assert_eq!(clickhouse_type_to_datatype("UUID"), DataType::String);
    assert_eq!(clickhouse_type_to_datatype("IPv4"), DataType::String);
    assert_eq!(clickhouse_type_to_datatype("IPv6"), DataType::String);
    assert_eq!(clickhouse_type_to_datatype("Bool"), DataType::Boolean);
}

#[test]
fn test_clickhouse_type_enum() {
    assert_eq!(
        clickhouse_type_to_datatype("Enum8('a' = 1, 'b' = 2)"),
        DataType::String
    );
}

// =============================================================================
// URL Encoding Tests
// =============================================================================

#[test]
fn test_url_encode_simple() {
    assert_eq!(urlencoding::encode("hello"), "hello");
    assert_eq!(urlencoding::encode("hello world"), "hello%20world");
}

#[test]
fn test_url_encode_special_chars() {
    assert_eq!(urlencoding::encode("a=b"), "a%3Db");
    assert_eq!(urlencoding::encode("foo&bar"), "foo%26bar");
}

#[test]
fn test_url_encode_sql() {
    let sql = "SELECT * FROM events WHERE name = 'test'";
    let encoded = urlencoding::encode(sql);
    assert!(encoded.contains("%20"));
    assert!(encoded.contains("%27")); // single quote
}

// =============================================================================
// Config Tests
// =============================================================================

#[test]
fn test_config_default() {
    let config = ClickHouseBackendConfig::default();
    assert_eq!(config.url, "http://localhost:8123");
    assert_eq!(config.database, "default");
    assert!(config.username.is_none());
    assert!(config.password.is_none());
    assert!(config.compression_enabled);
    assert_eq!(config.max_execution_time, 60);
}

#[test]
fn test_config_new() {
    let config = ClickHouseBackendConfig::new("http://example.com:8123", "analytics");
    assert_eq!(config.url, "http://example.com:8123");
    assert_eq!(config.database, "analytics");
}

#[test]
fn test_config_with_credentials() {
    let config = ClickHouseBackendConfig::default().with_credentials("admin", "secret");
    assert_eq!(config.username, Some("admin".to_string()));
    assert_eq!(config.password, Some("secret".to_string()));
}

#[test]
fn test_backend_name() {
    let config = ClickHouseBackendConfig::default();
    let backend = ClickHouseBackend::new(&config);
    assert_eq!(backend.name(), "clickhouse");
}

#[test]
fn test_backend_debug() {
    let config = ClickHouseBackendConfig::new("http://test:8123", "mydb");
    let backend = ClickHouseBackend::new(&config);
    let debug = format!("{:?}", backend);
    assert!(debug.contains("http://test:8123"));
    assert!(debug.contains("mydb"));
}

#[test]
fn test_build_url() {
    let backend = ClickHouseBackend::from_url("http://localhost:8123", "default");
    let url = backend.build_url("SELECT 1");
    assert!(url.contains("database=default"));
    assert!(url.contains("max_execution_time=60"));
    assert!(url.contains("query=SELECT%201"));
}

// =============================================================================
// Integration Tests (require running ClickHouse)
// =============================================================================

/// Integration tests that require a running ClickHouse instance.
/// Run with: cargo test -p tell-query -- --ignored
#[tokio::test]
#[ignore = "requires running ClickHouse instance"]
async fn test_health_check() {
    let backend = ClickHouseBackend::from_url("http://localhost:8123", "default");
    let result = backend.health_check().await;
    assert!(result.is_ok(), "health check failed: {:?}", result);
}

#[tokio::test]
#[ignore = "requires running ClickHouse instance"]
async fn test_simple_query() {
    let backend = ClickHouseBackend::from_url("http://localhost:8123", "default");
    let result = backend.execute("SELECT 1 as num, 'hello' as str").await;

    assert!(result.is_ok(), "query failed: {:?}", result);
    let result = result.unwrap();

    assert_eq!(result.row_count, 1);
    assert_eq!(result.columns.len(), 2);
}

#[tokio::test]
#[ignore = "requires running ClickHouse instance"]
async fn test_list_tables() {
    let backend = ClickHouseBackend::from_url("http://localhost:8123", "system");
    let result = backend.list_tables().await;

    assert!(result.is_ok(), "list tables failed: {:?}", result);
    let tables = result.unwrap();

    // system database should have tables
    assert!(!tables.is_empty());
}

#[tokio::test]
#[ignore = "requires running ClickHouse instance"]
async fn test_query_system_one() {
    let backend = ClickHouseBackend::from_url("http://localhost:8123", "system");
    let result = backend.execute("SELECT * FROM system.one").await;

    assert!(result.is_ok(), "query failed: {:?}", result);
    let result = result.unwrap();

    assert_eq!(result.row_count, 1);
}
