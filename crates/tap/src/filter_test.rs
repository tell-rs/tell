//! Tests for TapFilter

use super::*;
use cdp_protocol::{Batch, BatchBuilder, BatchType, SourceId};

/// Helper to create a test batch with specific metadata
fn make_batch(workspace_id: u32, source_id: &str, batch_type: BatchType) -> Batch {
    let mut builder = BatchBuilder::new(batch_type, SourceId::new(source_id.to_string()));
    builder.set_workspace_id(workspace_id);
    builder.finish()
}

// ============================================================================
// Empty filter tests
// ============================================================================

#[test]
fn test_empty_filter_matches_everything() {
    let filter = TapFilter::new();
    assert!(filter.is_empty());

    let batch = make_batch(42, "tcp_main", BatchType::Event);
    assert!(filter.matches(&batch));

    let batch = make_batch(1, "syslog", BatchType::Log);
    assert!(filter.matches(&batch));
}

#[test]
fn test_default_filter_is_empty() {
    let filter = TapFilter::default();
    assert!(filter.is_empty());
}

// ============================================================================
// Workspace filter tests
// ============================================================================

#[test]
fn test_workspace_filter_single() {
    let filter = TapFilter::new().with_workspaces(vec![42]);

    let batch = make_batch(42, "tcp", BatchType::Event);
    assert!(filter.matches(&batch));

    let batch = make_batch(99, "tcp", BatchType::Event);
    assert!(!filter.matches(&batch));
}

#[test]
fn test_workspace_filter_multiple() {
    let filter = TapFilter::new().with_workspaces(vec![1, 2, 3]);

    assert!(filter.matches(&make_batch(1, "tcp", BatchType::Event)));
    assert!(filter.matches(&make_batch(2, "tcp", BatchType::Event)));
    assert!(filter.matches(&make_batch(3, "tcp", BatchType::Event)));
    assert!(!filter.matches(&make_batch(4, "tcp", BatchType::Event)));
}

#[test]
fn test_workspace_filter_empty_list() {
    let filter = TapFilter::new().with_workspaces(vec![]);

    // Empty list means nothing matches
    let batch = make_batch(42, "tcp", BatchType::Event);
    assert!(!filter.matches(&batch));
}

// ============================================================================
// Source filter tests
// ============================================================================

#[test]
fn test_source_filter_single() {
    let filter = TapFilter::new().with_sources(vec![SourceId::new("tcp_main".to_string())]);

    let batch = make_batch(1, "tcp_main", BatchType::Event);
    assert!(filter.matches(&batch));

    let batch = make_batch(1, "syslog", BatchType::Event);
    assert!(!filter.matches(&batch));
}

#[test]
fn test_source_filter_multiple() {
    let filter = TapFilter::new().with_sources(vec![
        SourceId::new("tcp".to_string()),
        SourceId::new("udp".to_string()),
    ]);

    assert!(filter.matches(&make_batch(1, "tcp", BatchType::Event)));
    assert!(filter.matches(&make_batch(1, "udp", BatchType::Event)));
    assert!(!filter.matches(&make_batch(1, "syslog", BatchType::Event)));
}

// ============================================================================
// Batch type filter tests
// ============================================================================

#[test]
fn test_batch_type_filter_single() {
    let filter = TapFilter::new().with_batch_types(vec![BatchType::Event]);

    assert!(filter.matches(&make_batch(1, "tcp", BatchType::Event)));
    assert!(!filter.matches(&make_batch(1, "tcp", BatchType::Log)));
}

#[test]
fn test_batch_type_filter_multiple() {
    let filter = TapFilter::new().with_batch_types(vec![BatchType::Event, BatchType::Log]);

    assert!(filter.matches(&make_batch(1, "tcp", BatchType::Event)));
    assert!(filter.matches(&make_batch(1, "tcp", BatchType::Log)));
    assert!(!filter.matches(&make_batch(1, "tcp", BatchType::Metric)));
}

// ============================================================================
// Combined filter tests (AND logic)
// ============================================================================

#[test]
fn test_combined_workspace_and_source() {
    let filter = TapFilter::new()
        .with_workspaces(vec![42])
        .with_sources(vec![SourceId::new("tcp".to_string())]);

    // Both match
    assert!(filter.matches(&make_batch(42, "tcp", BatchType::Event)));

    // Workspace matches, source doesn't
    assert!(!filter.matches(&make_batch(42, "udp", BatchType::Event)));

    // Source matches, workspace doesn't
    assert!(!filter.matches(&make_batch(99, "tcp", BatchType::Event)));

    // Neither matches
    assert!(!filter.matches(&make_batch(99, "udp", BatchType::Event)));
}

#[test]
fn test_combined_all_three_filters() {
    let filter = TapFilter::new()
        .with_workspaces(vec![1, 2])
        .with_sources(vec![SourceId::new("tcp".to_string())])
        .with_batch_types(vec![BatchType::Event]);

    // All match
    assert!(filter.matches(&make_batch(1, "tcp", BatchType::Event)));
    assert!(filter.matches(&make_batch(2, "tcp", BatchType::Event)));

    // Wrong workspace
    assert!(!filter.matches(&make_batch(3, "tcp", BatchType::Event)));

    // Wrong source
    assert!(!filter.matches(&make_batch(1, "udp", BatchType::Event)));

    // Wrong type
    assert!(!filter.matches(&make_batch(1, "tcp", BatchType::Log)));
}

// ============================================================================
// from_subscribe tests
// ============================================================================

#[test]
fn test_from_subscribe_empty() {
    let req = SubscribeRequest::new();
    let filter = TapFilter::from_subscribe(&req);

    assert!(filter.is_empty());
}

#[test]
fn test_from_subscribe_with_workspaces() {
    let req = SubscribeRequest::new().with_workspaces(vec![1, 2, 3]);
    let filter = TapFilter::from_subscribe(&req);

    assert!(filter.workspace_ids().is_some());
    assert_eq!(filter.workspace_ids().unwrap().len(), 3);
    assert!(filter.source_ids().is_none());
    assert!(filter.batch_types().is_none());
}

#[test]
fn test_from_subscribe_with_sources() {
    let req = SubscribeRequest::new().with_sources(vec!["tcp".into(), "udp".into()]);
    let filter = TapFilter::from_subscribe(&req);

    assert!(filter.source_ids().is_some());
    assert_eq!(filter.source_ids().unwrap().len(), 2);
}

#[test]
fn test_from_subscribe_with_batch_types() {
    let req = SubscribeRequest::new().with_types(vec![0, 1]); // Event, Log
    let filter = TapFilter::from_subscribe(&req);

    assert!(filter.batch_types().is_some());
    assert_eq!(filter.batch_types().unwrap().len(), 2);
    assert!(filter.batch_types().unwrap().contains(&BatchType::Event));
    assert!(filter.batch_types().unwrap().contains(&BatchType::Log));
}

#[test]
fn test_from_subscribe_filters_invalid_batch_types() {
    let req = SubscribeRequest::new().with_types(vec![0, 255]); // Event, Invalid
    let filter = TapFilter::from_subscribe(&req);

    // Invalid type 255 should be filtered out
    assert!(filter.batch_types().is_some());
    assert_eq!(filter.batch_types().unwrap().len(), 1);
    assert!(filter.batch_types().unwrap().contains(&BatchType::Event));
}

#[test]
fn test_from_subscribe_full() {
    let req = SubscribeRequest::new()
        .with_workspaces(vec![42])
        .with_sources(vec!["tcp".into()])
        .with_types(vec![0])
        .with_last_n(100)
        .with_sample_rate(0.5)
        .with_rate_limit(1000);

    let filter = TapFilter::from_subscribe(&req);

    assert!(filter.workspace_ids().is_some());
    assert!(filter.source_ids().is_some());
    assert!(filter.batch_types().is_some());
    assert!(!filter.is_empty());

    // Verify actual matching
    assert!(filter.matches(&make_batch(42, "tcp", BatchType::Event)));
    assert!(!filter.matches(&make_batch(99, "tcp", BatchType::Event)));
}

// ============================================================================
// Accessor tests
// ============================================================================

#[test]
fn test_workspace_ids_accessor() {
    let filter = TapFilter::new().with_workspaces(vec![1, 2, 3]);
    let ids = filter.workspace_ids().unwrap();
    assert!(ids.contains(&1));
    assert!(ids.contains(&2));
    assert!(ids.contains(&3));
    assert!(!ids.contains(&4));
}

#[test]
fn test_source_ids_accessor() {
    let filter = TapFilter::new().with_sources(vec![SourceId::new("test".to_string())]);
    let ids = filter.source_ids().unwrap();
    assert!(ids.contains(&SourceId::new("test".to_string())));
}

#[test]
fn test_batch_types_accessor() {
    let filter = TapFilter::new().with_batch_types(vec![BatchType::Event, BatchType::Log]);
    let types = filter.batch_types().unwrap();
    assert!(types.contains(&BatchType::Event));
    assert!(types.contains(&BatchType::Log));
    assert!(!types.contains(&BatchType::Metric));
}
