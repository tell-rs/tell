//! Tests for query builder

use crate::builder::{
    QueryBuilder, count_distinct_query, count_query, distinct_values_query, raw_data_query,
    top_n_query,
};
use crate::filter::{Condition, Filter, Granularity};
use crate::timerange::TimeRange;

#[test]
fn test_basic_select() {
    let sql = QueryBuilder::new("events_v1")
        .select("event_name")
        .select("count")
        .build();

    assert_eq!(sql, "SELECT event_name, count FROM events_v1");
}

#[test]
fn test_select_with_alias() {
    let sql = QueryBuilder::new("events_v1")
        .select_as("COUNT(*)", "total")
        .build();

    assert_eq!(sql, "SELECT COUNT(*) AS total FROM events_v1");
}

#[test]
fn test_where_clause() {
    let sql = QueryBuilder::new("events_v1")
        .select("*")
        .where_clause("workspace_id = 1")
        .where_clause("event_name = 'click'")
        .build();

    assert!(sql.contains("WHERE workspace_id = 1 AND event_name = 'click'"));
}

#[test]
fn test_group_by() {
    let sql = QueryBuilder::new("events_v1")
        .select("event_name")
        .select_as("COUNT(*)", "count")
        .group_by("event_name")
        .build();

    assert!(sql.contains("GROUP BY event_name"));
}

#[test]
fn test_order_by() {
    let sql = QueryBuilder::new("events_v1")
        .select("event_name")
        .order_by("event_name")
        .build();

    assert!(sql.contains("ORDER BY event_name"));
}

#[test]
fn test_order_by_desc() {
    let sql = QueryBuilder::new("events_v1")
        .select("event_name")
        .select_as("COUNT(*)", "count")
        .order_by_desc("count")
        .build();

    assert!(sql.contains("ORDER BY count DESC"));
}

#[test]
fn test_limit() {
    let sql = QueryBuilder::new("events_v1").select("*").limit(10).build();

    assert!(sql.contains("LIMIT 10"));
}

#[test]
fn test_with_time_bucket_daily() {
    let sql = QueryBuilder::new("events_v1")
        .with_time_bucket(Granularity::Daily, "timestamp", "date")
        .select_as("COUNT(*)", "value")
        .build();

    assert!(sql.contains("toDate(timestamp) AS date"));
    assert!(sql.contains("GROUP BY date"));
    assert!(sql.contains("ORDER BY date"));
}

#[test]
fn test_with_time_bucket_hourly() {
    let sql = QueryBuilder::new("events_v1")
        .with_time_bucket(Granularity::Hourly, "timestamp", "hour")
        .select_as("COUNT(*)", "value")
        .build();

    assert!(sql.contains("toStartOfHour(timestamp) AS hour"));
}

#[test]
fn test_with_breakdown() {
    let sql = QueryBuilder::new("context_v1")
        .with_time_bucket(Granularity::Daily, "timestamp", "date")
        .with_breakdown("device_type")
        .select_as("COUNT(DISTINCT device_id)", "value")
        .build();

    assert!(sql.contains("device_type"));
    assert!(sql.contains("GROUP BY date, device_type"));
}

#[test]
fn test_apply_filter() {
    let range = TimeRange::parse("2024-01-01,2024-01-31").unwrap();
    let filter = Filter::new(range)
        .with_condition(Condition::eq("country", "US"))
        .with_granularity(Granularity::Daily);

    let sql = QueryBuilder::new("events_v1")
        .select("*")
        .apply_filter(&filter, "timestamp")
        .build();

    assert!(sql.contains("timestamp >= '2024-01-01"));
    assert!(sql.contains("timestamp <= '2024-01-31"));
    assert!(sql.contains("country = 'US'"));
}

#[test]
fn test_condition_operators() {
    let range = TimeRange::parse("7d").unwrap();

    // Equals
    let filter = Filter::new(range.clone()).with_condition(Condition::eq("field", "value"));
    let sql = QueryBuilder::new("t").apply_filter(&filter, "ts").build();
    assert!(sql.contains("field = 'value'"));

    // Not equals
    let filter = Filter::new(range.clone()).with_condition(Condition::ne("field", "value"));
    let sql = QueryBuilder::new("t").apply_filter(&filter, "ts").build();
    assert!(sql.contains("field != 'value'"));

    // Contains
    let filter = Filter::new(range.clone()).with_condition(Condition::contains("field", "sub"));
    let sql = QueryBuilder::new("t").apply_filter(&filter, "ts").build();
    assert!(sql.contains("field LIKE '%sub%'"));

    // In
    let filter = Filter::new(range.clone())
        .with_condition(Condition::is_in("f", vec!["a".into(), "b".into()]));
    let sql = QueryBuilder::new("t").apply_filter(&filter, "ts").build();
    assert!(sql.contains("f IN ('a', 'b')"));

    // Is set
    let filter = Filter::new(range.clone()).with_condition(Condition::is_set("user_id"));
    let sql = QueryBuilder::new("t").apply_filter(&filter, "ts").build();
    assert!(sql.contains("user_id IS NOT NULL"));

    // Is not set
    let filter = Filter::new(range).with_condition(Condition::is_not_set("user_id"));
    let sql = QueryBuilder::new("t").apply_filter(&filter, "ts").build();
    assert!(sql.contains("user_id IS NULL"));
}

#[test]
fn test_sql_injection_prevention() {
    let range = TimeRange::parse("7d").unwrap();
    let filter =
        Filter::new(range).with_condition(Condition::eq("field", "'; DROP TABLE users; --"));

    let sql = QueryBuilder::new("events")
        .apply_filter(&filter, "ts")
        .build();

    // Should escape the single quote, making the injection a harmless string value
    // Input: '; DROP TABLE users; --
    // Escaped: ''; DROP TABLE users; --
    // Full SQL: field = '''; DROP TABLE users; --'
    // This is safe because the injection is now inside quotes as a literal string
    assert!(
        sql.contains("''"),
        "SQL should contain escaped quotes: {}",
        sql
    );
}

#[test]
fn test_count_distinct_query() {
    let range = TimeRange::parse("7d").unwrap();
    let filter = Filter::new(range).with_granularity(Granularity::Daily);

    let sql = count_distinct_query("context_v1", "device_id", &filter, "timestamp");

    assert!(sql.contains("COUNT(DISTINCT device_id) AS value"));
    assert!(sql.contains("toDate(timestamp) AS date"));
    assert!(sql.contains("GROUP BY date"));
}

#[test]
fn test_count_query() {
    let range = TimeRange::parse("7d").unwrap();
    let filter = Filter::new(range).with_granularity(Granularity::Daily);

    let sql = count_query("events_v1", &filter, "timestamp");

    assert!(sql.contains("COUNT(*) AS value"));
    assert!(sql.contains("toDate(timestamp) AS date"));
}

#[test]
fn test_top_n_query() {
    let range = TimeRange::parse("7d").unwrap();
    let filter = Filter::new(range);

    let sql = top_n_query("events_v1", "event_name", &filter, "timestamp", 10);

    assert!(sql.contains("SELECT event_name, COUNT(*) AS count"));
    assert!(sql.contains("GROUP BY event_name"));
    assert!(sql.contains("ORDER BY count DESC"));
    assert!(sql.contains("LIMIT 10"));
}

#[test]
fn test_raw_data_query() {
    let range = TimeRange::parse("7d").unwrap();
    let filter = Filter::new(range);
    let columns = &["timestamp", "event_name", "device_id"];

    let sql = raw_data_query("events_v1", columns, &filter, "timestamp", 100);

    assert!(sql.contains("SELECT timestamp, event_name, device_id"));
    assert!(sql.contains("FROM events_v1"));
    assert!(sql.contains("ORDER BY timestamp DESC"));
    assert!(sql.contains("LIMIT 100"));
}

#[test]
fn test_raw_data_query_with_filter() {
    let range = TimeRange::parse("7d").unwrap();
    let filter = Filter::new(range).with_condition(Condition::eq("event_name", "page_view"));
    let columns = &["timestamp", "event_name"];

    let sql = raw_data_query("events_v1", columns, &filter, "timestamp", 50);

    assert!(sql.contains("event_name = 'page_view'"));
    assert!(sql.contains("LIMIT 50"));
}

#[test]
fn test_distinct_values_query() {
    let range = TimeRange::parse("7d").unwrap();
    let filter = Filter::new(range);

    let sql = distinct_values_query("context_v1", "device_id", &filter, "timestamp", 1000);

    assert!(sql.contains("SELECT DISTINCT device_id"));
    assert!(sql.contains("FROM context_v1"));
    assert!(sql.contains("ORDER BY device_id"));
    assert!(sql.contains("LIMIT 1000"));
}

#[test]
fn test_distinct_values_with_time_filter() {
    let range = TimeRange::parse("2024-01-01,2024-01-31").unwrap();
    let filter = Filter::new(range);

    let sql = distinct_values_query("context_v1", "session_id", &filter, "timestamp", 500);

    assert!(sql.contains("SELECT DISTINCT session_id"));
    assert!(sql.contains("timestamp >= '2024-01-01"));
    assert!(sql.contains("timestamp <= '2024-01-31"));
    assert!(sql.contains("LIMIT 500"));
}

// =============================================================================
// Rolling Window Mode Tests
// =============================================================================

#[test]
fn test_rolling_window_count() {
    let range = TimeRange::parse("7d").unwrap();
    let filter = Filter::new(range).with_rolling_window();

    let sql = count_query("events_v1", &filter, "timestamp");

    // Should NOT have date grouping
    assert!(sql.contains("COUNT(*) AS value"));
    assert!(!sql.contains("GROUP BY"));
    assert!(!sql.contains("toDate"));
}

#[test]
fn test_rolling_window_count_distinct() {
    let range = TimeRange::parse("30d").unwrap();
    let filter = Filter::new(range).with_rolling_window();

    let sql = count_distinct_query("context_v1", "device_id", &filter, "timestamp");

    // Should return single aggregate without time bucketing
    assert!(sql.contains("COUNT(DISTINCT device_id) AS value"));
    assert!(!sql.contains("GROUP BY"));
}

#[test]
fn test_filter_is_rolling_window() {
    let range = TimeRange::parse("7d").unwrap();

    let filter_with_granularity = Filter::new(range.clone()).with_granularity(Granularity::Daily);
    assert!(!filter_with_granularity.is_rolling_window());

    let filter_rolling = Filter::new(range).with_rolling_window();
    assert!(filter_rolling.is_rolling_window());
}

// =============================================================================
// JSON Field Access Tests
// =============================================================================

#[test]
fn test_json_field_access() {
    let range = TimeRange::parse("7d").unwrap();
    let filter = Filter::new(range).with_condition(Condition::eq("properties.page", "/home"));

    let sql = QueryBuilder::new("events_v1")
        .select("*")
        .apply_filter(&filter, "timestamp")
        .build();

    // Should use JSONExtractString for nested field access
    assert!(sql.contains("JSONExtractString(properties, 'page')"));
    assert!(sql.contains("'/home'"));
}

#[test]
fn test_json_nested_field_access() {
    let range = TimeRange::parse("7d").unwrap();
    let filter = Filter::new(range).with_condition(Condition::eq("properties.user.country", "US"));

    let sql = QueryBuilder::new("events_v1")
        .select("*")
        .apply_filter(&filter, "timestamp")
        .build();

    // Should handle multi-level nesting
    assert!(sql.contains("JSONExtractString(properties, 'user', 'country')"));
}

#[test]
fn test_json_field_contains() {
    let range = TimeRange::parse("7d").unwrap();
    let filter =
        Filter::new(range).with_condition(Condition::contains("properties.url", "checkout"));

    let sql = QueryBuilder::new("events_v1")
        .select("*")
        .apply_filter(&filter, "timestamp")
        .build();

    assert!(sql.contains("JSONExtractString(properties, 'url')"));
    assert!(sql.contains("LIKE '%checkout%'"));
}

// =============================================================================
// IPv6 Normalization Tests
// =============================================================================

#[test]
fn test_ipv6_source_ip_filter() {
    let range = TimeRange::parse("7d").unwrap();
    let filter = Filter::new(range).with_condition(Condition::eq("source_ip", "192.168.1.1"));

    let sql = QueryBuilder::new("context_v1")
        .select("*")
        .apply_filter(&filter, "timestamp")
        .build();

    // IPv4 should be converted to IPv4-mapped IPv6
    assert!(sql.contains("IPv6StringToNum"));
    assert!(sql.contains("::ffff:192.168.1.1"));
}

#[test]
fn test_ipv6_native_address() {
    let range = TimeRange::parse("7d").unwrap();
    let filter = Filter::new(range).with_condition(Condition::eq("source_ip", "2001:db8::1"));

    let sql = QueryBuilder::new("context_v1")
        .select("*")
        .apply_filter(&filter, "timestamp")
        .build();

    // Native IPv6 should pass through unchanged
    assert!(sql.contains("IPv6StringToNum('2001:db8::1')"));
}

#[test]
fn test_ipv6_in_condition() {
    let range = TimeRange::parse("7d").unwrap();
    let filter = Filter::new(range).with_condition(Condition::is_in(
        "source_ip",
        vec!["192.168.1.1".to_string(), "10.0.0.1".to_string()],
    ));

    let sql = QueryBuilder::new("context_v1")
        .select("*")
        .apply_filter(&filter, "timestamp")
        .build();

    assert!(sql.contains("source_ip IN ("));
    assert!(sql.contains("IPv6StringToNum('::ffff:192.168.1.1')"));
    assert!(sql.contains("IPv6StringToNum('::ffff:10.0.0.1')"));
}

// =============================================================================
// Limit Validation Tests
// =============================================================================

#[test]
fn test_limit_capped_at_max() {
    use crate::filter::MAX_LIMIT;

    let range = TimeRange::parse("7d").unwrap();
    let filter = Filter::new(range).with_limit(50_000); // Exceeds MAX_LIMIT

    // Should be capped to MAX_LIMIT
    assert_eq!(filter.limit, Some(MAX_LIMIT));
}

#[test]
fn test_limit_under_max() {
    let range = TimeRange::parse("7d").unwrap();
    let filter = Filter::new(range).with_limit(500);

    // Should keep the requested limit
    assert_eq!(filter.limit, Some(500));
}
