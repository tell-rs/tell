//! Tests for schema types module

use crate::schema::{BatchType, SchemaType};

// =============================================================================
// SchemaType::from_u8 tests
// =============================================================================

#[test]
fn test_schema_type_from_u8_unknown() {
    assert_eq!(SchemaType::from_u8(0), SchemaType::Unknown);
}

#[test]
fn test_schema_type_from_u8_event() {
    assert_eq!(SchemaType::from_u8(1), SchemaType::Event);
}

#[test]
fn test_schema_type_from_u8_log() {
    assert_eq!(SchemaType::from_u8(2), SchemaType::Log);
}

#[test]
fn test_schema_type_from_u8_metric() {
    assert_eq!(SchemaType::from_u8(3), SchemaType::Metric);
}

#[test]
fn test_schema_type_from_u8_trace() {
    assert_eq!(SchemaType::from_u8(4), SchemaType::Trace);
}

#[test]
fn test_schema_type_from_u8_inventory() {
    assert_eq!(SchemaType::from_u8(5), SchemaType::Inventory);
}

#[test]
fn test_schema_type_from_u8_snapshot() {
    assert_eq!(SchemaType::from_u8(6), SchemaType::Snapshot);
}

#[test]
fn test_schema_type_from_u8_invalid_returns_unknown() {
    assert_eq!(SchemaType::from_u8(7), SchemaType::Unknown);
    assert_eq!(SchemaType::from_u8(100), SchemaType::Unknown);
    assert_eq!(SchemaType::from_u8(255), SchemaType::Unknown);
}

// =============================================================================
// SchemaType::as_u8 roundtrip tests
// =============================================================================

#[test]
fn test_schema_type_roundtrip_valid_values() {
    for value in 0..=6u8 {
        let schema_type = SchemaType::from_u8(value);
        assert_eq!(schema_type.as_u8(), value);
    }
}

#[test]
fn test_schema_type_roundtrip_unknown() {
    let unknown = SchemaType::Unknown;
    assert_eq!(unknown.as_u8(), 0);
    assert_eq!(SchemaType::from_u8(unknown.as_u8()), SchemaType::Unknown);
}

// =============================================================================
// SchemaType::is_supported tests
// =============================================================================

#[test]
fn test_schema_type_is_supported_event() {
    assert!(SchemaType::Event.is_supported());
}

#[test]
fn test_schema_type_is_supported_log() {
    assert!(SchemaType::Log.is_supported());
}

#[test]
fn test_schema_type_is_supported_metric_false() {
    assert!(!SchemaType::Metric.is_supported());
}

#[test]
fn test_schema_type_is_supported_trace_false() {
    assert!(!SchemaType::Trace.is_supported());
}

#[test]
fn test_schema_type_is_supported_inventory_false() {
    assert!(!SchemaType::Inventory.is_supported());
}

#[test]
fn test_schema_type_is_supported_snapshot() {
    assert!(SchemaType::Snapshot.is_supported());
}

#[test]
fn test_schema_type_is_supported_unknown_false() {
    assert!(!SchemaType::Unknown.is_supported());
}

// =============================================================================
// SchemaType::as_str tests
// =============================================================================

#[test]
fn test_schema_type_as_str() {
    assert_eq!(SchemaType::Unknown.as_str(), "unknown");
    assert_eq!(SchemaType::Event.as_str(), "event");
    assert_eq!(SchemaType::Log.as_str(), "log");
    assert_eq!(SchemaType::Metric.as_str(), "metric");
    assert_eq!(SchemaType::Trace.as_str(), "trace");
    assert_eq!(SchemaType::Inventory.as_str(), "inventory");
    assert_eq!(SchemaType::Snapshot.as_str(), "snapshot");
}

// =============================================================================
// SchemaType Display tests
// =============================================================================

#[test]
fn test_schema_type_display() {
    assert_eq!(format!("{}", SchemaType::Unknown), "unknown");
    assert_eq!(format!("{}", SchemaType::Event), "event");
    assert_eq!(format!("{}", SchemaType::Log), "log");
    assert_eq!(format!("{}", SchemaType::Metric), "metric");
    assert_eq!(format!("{}", SchemaType::Trace), "trace");
    assert_eq!(format!("{}", SchemaType::Inventory), "inventory");
    assert_eq!(format!("{}", SchemaType::Snapshot), "snapshot");
}

// =============================================================================
// BatchType::from_schema_type tests
// =============================================================================

#[test]
fn test_batch_type_from_schema_type_event() {
    assert_eq!(
        BatchType::from_schema_type(SchemaType::Event),
        Some(BatchType::Event)
    );
}

#[test]
fn test_batch_type_from_schema_type_log() {
    assert_eq!(
        BatchType::from_schema_type(SchemaType::Log),
        Some(BatchType::Log)
    );
}

#[test]
fn test_batch_type_from_schema_type_metric() {
    assert_eq!(
        BatchType::from_schema_type(SchemaType::Metric),
        Some(BatchType::Metric)
    );
}

#[test]
fn test_batch_type_from_schema_type_trace() {
    assert_eq!(
        BatchType::from_schema_type(SchemaType::Trace),
        Some(BatchType::Trace)
    );
}

#[test]
fn test_batch_type_from_schema_type_snapshot() {
    assert_eq!(
        BatchType::from_schema_type(SchemaType::Snapshot),
        Some(BatchType::Snapshot)
    );
}

#[test]
fn test_batch_type_from_schema_type_unknown() {
    assert_eq!(BatchType::from_schema_type(SchemaType::Unknown), None);
}

#[test]
fn test_batch_type_from_schema_type_inventory() {
    assert_eq!(BatchType::from_schema_type(SchemaType::Inventory), None);
}

// =============================================================================
// BatchType::is_event tests
// =============================================================================

#[test]
fn test_batch_type_is_event_true() {
    assert!(BatchType::Event.is_event());
}

#[test]
fn test_batch_type_is_event_false_for_others() {
    assert!(!BatchType::Log.is_event());
    assert!(!BatchType::Syslog.is_event());
    assert!(!BatchType::Metric.is_event());
    assert!(!BatchType::Trace.is_event());
    assert!(!BatchType::Snapshot.is_event());
}

// =============================================================================
// BatchType::is_log tests
// =============================================================================

#[test]
fn test_batch_type_is_log_true_for_log() {
    assert!(BatchType::Log.is_log());
}

#[test]
fn test_batch_type_is_log_true_for_syslog() {
    assert!(BatchType::Syslog.is_log());
}

#[test]
fn test_batch_type_is_log_false_for_others() {
    assert!(!BatchType::Event.is_log());
    assert!(!BatchType::Metric.is_log());
    assert!(!BatchType::Trace.is_log());
    assert!(!BatchType::Snapshot.is_log());
}

// =============================================================================
// BatchType::is_snapshot tests
// =============================================================================

#[test]
fn test_batch_type_is_snapshot_true() {
    assert!(BatchType::Snapshot.is_snapshot());
}

#[test]
fn test_batch_type_is_snapshot_false_for_others() {
    assert!(!BatchType::Event.is_snapshot());
    assert!(!BatchType::Log.is_snapshot());
    assert!(!BatchType::Syslog.is_snapshot());
    assert!(!BatchType::Metric.is_snapshot());
    assert!(!BatchType::Trace.is_snapshot());
}

// =============================================================================
// BatchType::as_str tests
// =============================================================================

#[test]
fn test_batch_type_as_str() {
    assert_eq!(BatchType::Event.as_str(), "event");
    assert_eq!(BatchType::Log.as_str(), "log");
    assert_eq!(BatchType::Syslog.as_str(), "syslog");
    assert_eq!(BatchType::Metric.as_str(), "metric");
    assert_eq!(BatchType::Trace.as_str(), "trace");
    assert_eq!(BatchType::Snapshot.as_str(), "snapshot");
}

// =============================================================================
// BatchType Display tests
// =============================================================================

#[test]
fn test_batch_type_display() {
    assert_eq!(format!("{}", BatchType::Event), "event");
    assert_eq!(format!("{}", BatchType::Log), "log");
    assert_eq!(format!("{}", BatchType::Syslog), "syslog");
    assert_eq!(format!("{}", BatchType::Metric), "metric");
    assert_eq!(format!("{}", BatchType::Trace), "trace");
    assert_eq!(format!("{}", BatchType::Snapshot), "snapshot");
}

// =============================================================================
// SchemaType trait tests
// =============================================================================

#[test]
fn test_schema_type_clone() {
    let original = SchemaType::Event;
    let cloned = original.clone();
    assert_eq!(original, cloned);
}

#[test]
fn test_schema_type_copy() {
    let original = SchemaType::Log;
    let copied = original;
    assert_eq!(original, copied);
}

#[test]
fn test_schema_type_debug() {
    let debug_str = format!("{:?}", SchemaType::Event);
    assert!(debug_str.contains("Event"));
}

#[test]
fn test_schema_type_eq() {
    assert_eq!(SchemaType::Event, SchemaType::Event);
    assert_ne!(SchemaType::Event, SchemaType::Log);
}

#[test]
fn test_schema_type_hash() {
    use std::collections::HashSet;

    let mut set = HashSet::new();
    set.insert(SchemaType::Event);
    set.insert(SchemaType::Event); // duplicate
    set.insert(SchemaType::Log);

    assert_eq!(set.len(), 2);
    assert!(set.contains(&SchemaType::Event));
    assert!(set.contains(&SchemaType::Log));
}

// =============================================================================
// BatchType trait tests
// =============================================================================

#[test]
fn test_batch_type_clone() {
    let original = BatchType::Event;
    let cloned = original.clone();
    assert_eq!(original, cloned);
}

#[test]
fn test_batch_type_copy() {
    let original = BatchType::Log;
    let copied = original;
    assert_eq!(original, copied);
}

#[test]
fn test_batch_type_debug() {
    let debug_str = format!("{:?}", BatchType::Event);
    assert!(debug_str.contains("Event"));
}

#[test]
fn test_batch_type_eq() {
    assert_eq!(BatchType::Event, BatchType::Event);
    assert_ne!(BatchType::Event, BatchType::Log);
    assert_ne!(BatchType::Log, BatchType::Syslog);
}

#[test]
fn test_batch_type_hash() {
    use std::collections::HashSet;

    let mut set = HashSet::new();
    set.insert(BatchType::Event);
    set.insert(BatchType::Event); // duplicate
    set.insert(BatchType::Log);
    set.insert(BatchType::Syslog);

    assert_eq!(set.len(), 3);
    assert!(set.contains(&BatchType::Event));
    assert!(set.contains(&BatchType::Log));
    assert!(set.contains(&BatchType::Syslog));
}
