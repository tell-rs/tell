//! Tests for RoutingTable
//!
//! Tests cover O(1) lookup, default fallback, builder pattern, and edge cases.

use crate::{RoutingTable, RoutingTableBuilder, SinkId, SourceId};

// =============================================================================
// Basic RoutingTable tests
// =============================================================================

#[test]
fn test_new_table_is_empty() {
    let table = RoutingTable::new();
    assert!(table.is_empty());
    assert_eq!(table.route_count(), 0);
    assert_eq!(table.sink_count(), 0);
}

#[test]
fn test_with_capacity() {
    let table = RoutingTable::with_capacity(10, 5);
    assert!(table.is_empty());
}

#[test]
fn test_default_returns_unit_struct() {
    let table = RoutingTable::default();
    assert!(table.is_empty());
}

// =============================================================================
// Route tests
// =============================================================================

#[test]
fn test_add_route() {
    let mut table = RoutingTable::new();
    let source = SourceId::new("tcp");
    let sinks = vec![SinkId::new(0), SinkId::new(1)];

    table.add_route(source.clone(), sinks.clone());

    assert_eq!(table.route_count(), 1);
    assert!(table.has_route(&source));
    assert_eq!(table.route(&source), &sinks);
}

#[test]
fn test_add_multiple_routes() {
    let mut table = RoutingTable::new();

    table.add_route(SourceId::new("tcp"), vec![SinkId::new(0)]);
    table.add_route(
        SourceId::new("syslog"),
        vec![SinkId::new(1), SinkId::new(2)],
    );
    table.add_route(
        SourceId::new("udp"),
        vec![SinkId::new(0), SinkId::new(1), SinkId::new(2)],
    );

    assert_eq!(table.route_count(), 3);
    assert_eq!(table.route(&SourceId::new("tcp")), &[SinkId::new(0)]);
    assert_eq!(
        table.route(&SourceId::new("syslog")),
        &[SinkId::new(1), SinkId::new(2)]
    );
    assert_eq!(
        table.route(&SourceId::new("udp")),
        &[SinkId::new(0), SinkId::new(1), SinkId::new(2)]
    );
}

#[test]
fn test_replace_route() {
    let mut table = RoutingTable::new();
    let source = SourceId::new("tcp");

    table.add_route(source.clone(), vec![SinkId::new(0)]);
    assert_eq!(table.route(&source), &[SinkId::new(0)]);

    // Replace with different sinks
    table.add_route(source.clone(), vec![SinkId::new(1), SinkId::new(2)]);
    assert_eq!(table.route(&source), &[SinkId::new(1), SinkId::new(2)]);

    // Still only one route
    assert_eq!(table.route_count(), 1);
}

// =============================================================================
// Default sinks tests
// =============================================================================

#[test]
fn test_set_default() {
    let mut table = RoutingTable::new();
    let defaults = vec![SinkId::new(0), SinkId::new(1)];

    table.set_default(defaults.clone());

    assert_eq!(table.default_sinks(), &defaults);
}

#[test]
fn test_unknown_source_returns_default() {
    let mut table = RoutingTable::new();
    table.set_default(vec![SinkId::new(99)]);
    table.add_route(SourceId::new("tcp"), vec![SinkId::new(0)]);

    // Known source gets its route
    assert_eq!(table.route(&SourceId::new("tcp")), &[SinkId::new(0)]);

    // Unknown source gets default
    assert_eq!(table.route(&SourceId::new("unknown")), &[SinkId::new(99)]);
    assert_eq!(
        table.route(&SourceId::new("not_configured")),
        &[SinkId::new(99)]
    );
}

#[test]
fn test_no_default_returns_empty() {
    let table = RoutingTable::new();

    // No default set, unknown source gets empty slice
    let result = table.route(&SourceId::new("unknown"));
    assert!(result.is_empty());
}

#[test]
fn test_empty_default() {
    let mut table = RoutingTable::new();
    table.set_default(vec![]);

    let result = table.route(&SourceId::new("unknown"));
    assert!(result.is_empty());
}

// =============================================================================
// Sink registry tests
// =============================================================================

#[test]
fn test_register_sink() {
    let mut table = RoutingTable::new();

    let id0 = table.register_sink("stdout");
    let id1 = table.register_sink("clickhouse");
    let id2 = table.register_sink("disk");

    assert_eq!(id0, SinkId::new(0));
    assert_eq!(id1, SinkId::new(1));
    assert_eq!(id2, SinkId::new(2));
    assert_eq!(table.sink_count(), 3);
}

#[test]
fn test_sink_name_lookup() {
    let mut table = RoutingTable::new();

    table.register_sink("stdout");
    table.register_sink("clickhouse");

    assert_eq!(table.sink_name(SinkId::new(0)), Some("stdout"));
    assert_eq!(table.sink_name(SinkId::new(1)), Some("clickhouse"));
    assert_eq!(table.sink_name(SinkId::new(2)), None);
}

#[test]
fn test_sink_names() {
    let mut table = RoutingTable::new();

    table.register_sink("a");
    table.register_sink("b");
    table.register_sink("c");

    assert_eq!(table.sink_names(), &["a", "b", "c"]);
}

// =============================================================================
// Iterator tests
// =============================================================================

#[test]
fn test_iter_routes() {
    let mut table = RoutingTable::new();

    table.add_route(SourceId::new("tcp"), vec![SinkId::new(0)]);
    table.add_route(SourceId::new("syslog"), vec![SinkId::new(1)]);

    let routes: Vec<_> = table.iter().collect();
    assert_eq!(routes.len(), 2);

    // Check that both routes are present (order not guaranteed due to HashMap)
    let has_tcp = routes
        .iter()
        .any(|(src, sinks)| src.as_str() == "tcp" && sinks == &[SinkId::new(0)]);
    let has_syslog = routes
        .iter()
        .any(|(src, sinks)| src.as_str() == "syslog" && sinks == &[SinkId::new(1)]);

    assert!(has_tcp);
    assert!(has_syslog);
}

#[test]
fn test_iter_empty() {
    let table = RoutingTable::new();
    assert_eq!(table.iter().count(), 0);
}

// =============================================================================
// Builder tests
// =============================================================================

#[test]
fn test_builder_register_sink() {
    let mut builder = RoutingTableBuilder::new();

    let id0 = builder.register_sink("stdout");
    let id1 = builder.register_sink("clickhouse");

    assert_eq!(id0, SinkId::new(0));
    assert_eq!(id1, SinkId::new(1));
}

#[test]
fn test_builder_register_sink_idempotent() {
    let mut builder = RoutingTableBuilder::new();

    let id1 = builder.register_sink("stdout");
    let id2 = builder.register_sink("stdout"); // Same name
    let id3 = builder.register_sink("clickhouse");

    // Same name returns same ID
    assert_eq!(id1, id2);
    assert_eq!(id1, SinkId::new(0));
    assert_eq!(id3, SinkId::new(1));
}

#[test]
fn test_builder_get_sink_id() {
    let mut builder = RoutingTableBuilder::new();

    builder.register_sink("stdout");
    builder.register_sink("clickhouse");

    assert_eq!(builder.get_sink_id("stdout"), Some(SinkId::new(0)));
    assert_eq!(builder.get_sink_id("clickhouse"), Some(SinkId::new(1)));
    assert_eq!(builder.get_sink_id("unknown"), None);
}

#[test]
fn test_builder_add_route_by_name() {
    let mut builder = RoutingTableBuilder::new();

    builder.register_sink("stdout");
    builder.register_sink("clickhouse");

    let result = builder.add_route_by_name("tcp", &["stdout", "clickhouse"]);
    assert!(result.is_some());

    let table = builder.build().unwrap();
    assert_eq!(
        table.route(&SourceId::new("tcp")),
        &[SinkId::new(0), SinkId::new(1)]
    );
}

#[test]
fn test_builder_add_route_unknown_sink() {
    let mut builder = RoutingTableBuilder::new();

    builder.register_sink("stdout");

    // Try to route to unregistered sink
    let result = builder.add_route_by_name("tcp", &["stdout", "unknown"]);
    assert!(result.is_none());
}

#[test]
fn test_builder_set_default_by_name() {
    let mut builder = RoutingTableBuilder::new();

    builder.register_sink("stdout");
    builder.register_sink("disk");

    builder.set_default_by_name(vec!["stdout".into(), "disk".into()]);

    let table = builder.build().unwrap();
    assert_eq!(table.default_sinks(), &[SinkId::new(0), SinkId::new(1)]);
}

#[test]
fn test_builder_build_unknown_default() {
    let mut builder = RoutingTableBuilder::new();

    builder.register_sink("stdout");
    builder.set_default_by_name(vec!["stdout".into(), "unknown".into()]);

    // Build fails because "unknown" is not registered
    let result = builder.build();
    assert!(result.is_none());
}

#[test]
fn test_builder_full_workflow() {
    let mut builder = RoutingTableBuilder::new();

    // Register sinks
    builder.register_sink("stdout");
    builder.register_sink("clickhouse");
    builder.register_sink("disk");

    // Set default
    builder.set_default_by_name(vec!["stdout".into()]);

    // Add routes
    builder
        .add_route_by_name("tcp", &["clickhouse", "disk"])
        .unwrap();
    builder.add_route_by_name("tcp_debug", &["stdout"]).unwrap();
    builder
        .add_route_by_name("syslog", &["disk", "stdout"])
        .unwrap();

    // Build
    let table = builder.build().unwrap();

    // Verify
    assert_eq!(table.sink_count(), 3);
    assert_eq!(table.route_count(), 3);

    assert_eq!(
        table.route(&SourceId::new("tcp")),
        &[SinkId::new(1), SinkId::new(2)]
    );
    assert_eq!(table.route(&SourceId::new("tcp_debug")), &[SinkId::new(0)]);
    assert_eq!(
        table.route(&SourceId::new("syslog")),
        &[SinkId::new(2), SinkId::new(0)]
    );
    assert_eq!(table.route(&SourceId::new("unknown")), &[SinkId::new(0)]);

    // Verify sink names
    assert_eq!(table.sink_name(SinkId::new(0)), Some("stdout"));
    assert_eq!(table.sink_name(SinkId::new(1)), Some("clickhouse"));
    assert_eq!(table.sink_name(SinkId::new(2)), Some("disk"));
}

// =============================================================================
// Zero-copy verification tests
// =============================================================================

#[test]
fn test_route_returns_slice() {
    let mut table = RoutingTable::new();
    table.add_route(SourceId::new("tcp"), vec![SinkId::new(0), SinkId::new(1)]);

    // Multiple calls should return the same slice (same memory)
    let slice1 = table.route(&SourceId::new("tcp"));
    let slice2 = table.route(&SourceId::new("tcp"));

    // Verify they point to the same data
    assert_eq!(slice1.as_ptr(), slice2.as_ptr());
}

#[test]
fn test_sink_id_is_copy() {
    let id = SinkId::new(42);
    let copy1 = id;
    let copy2 = id;

    // All copies are equal
    assert_eq!(id, copy1);
    assert_eq!(id, copy2);
    assert_eq!(copy1, copy2);

    // Can use original after copies
    assert_eq!(id.index(), 42);
}

#[test]
fn test_sink_id_size() {
    // Verify SinkId is small (2 bytes = u16)
    assert_eq!(std::mem::size_of::<SinkId>(), 2);
}

// =============================================================================
// Edge cases
// =============================================================================

#[test]
fn test_empty_route() {
    let mut table = RoutingTable::new();
    table.add_route(SourceId::new("tcp"), vec![]);

    // Empty route is valid (drops all batches from this source)
    let result = table.route(&SourceId::new("tcp"));
    assert!(result.is_empty());
}

#[test]
fn test_single_sink_route() {
    let mut table = RoutingTable::new();
    table.add_route(SourceId::new("tcp"), vec![SinkId::new(0)]);

    assert_eq!(table.route(&SourceId::new("tcp")), &[SinkId::new(0)]);
}

#[test]
fn test_many_sinks_route() {
    let mut table = RoutingTable::new();

    let many_sinks: Vec<SinkId> = (0..100).map(SinkId::new).collect();
    table.add_route(SourceId::new("tcp"), many_sinks.clone());

    assert_eq!(table.route(&SourceId::new("tcp")), &many_sinks);
}

#[test]
fn test_source_id_with_special_chars() {
    let mut table = RoutingTable::new();

    table.add_route(SourceId::new("tcp:8080"), vec![SinkId::new(0)]);
    table.add_route(SourceId::new("syslog/tcp"), vec![SinkId::new(1)]);
    table.add_route(SourceId::new("source-with-dashes"), vec![SinkId::new(2)]);

    assert_eq!(table.route(&SourceId::new("tcp:8080")), &[SinkId::new(0)]);
    assert_eq!(table.route(&SourceId::new("syslog/tcp")), &[SinkId::new(1)]);
    assert_eq!(
        table.route(&SourceId::new("source-with-dashes")),
        &[SinkId::new(2)]
    );
}

#[test]
fn test_clone_table() {
    let mut table = RoutingTable::new();
    table.set_default(vec![SinkId::new(0)]);
    table.add_route(SourceId::new("tcp"), vec![SinkId::new(1)]);
    table.register_sink("stdout");

    let cloned = table.clone();

    assert_eq!(cloned.route_count(), table.route_count());
    assert_eq!(cloned.sink_count(), table.sink_count());
    assert_eq!(cloned.default_sinks(), table.default_sinks());
    assert_eq!(
        cloned.route(&SourceId::new("tcp")),
        table.route(&SourceId::new("tcp"))
    );
}
