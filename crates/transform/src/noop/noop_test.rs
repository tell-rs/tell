//! Tests for NoopTransformer

use super::*;
use tell_protocol::{BatchBuilder, BatchType, SourceId};

fn create_test_batch() -> Batch {
    let source_id = SourceId::new("test");
    let mut builder = BatchBuilder::new(BatchType::Event, source_id);
    builder.set_workspace_id(42);
    builder.add(b"test message 1", 1);
    builder.add(b"test message 2", 1);
    builder.finish()
}

#[tokio::test]
async fn test_noop_passes_through() {
    let transformer = NoopTransformer::new();
    let batch = create_test_batch();

    let original_count = batch.count();
    let original_workspace = batch.workspace_id();

    let result = transformer.transform(batch).await;
    assert!(result.is_ok());

    let transformed = result.unwrap();
    assert_eq!(transformed.count(), original_count);
    assert_eq!(transformed.workspace_id(), original_workspace);
}

#[test]
fn test_noop_name() {
    let transformer = NoopTransformer::new();
    assert_eq!(transformer.name(), "noop");
}

#[test]
fn test_noop_enabled() {
    let transformer = NoopTransformer::new();
    assert!(transformer.enabled());
}

#[test]
fn test_noop_is_copy() {
    let t1 = NoopTransformer::new();
    let t2 = t1; // Copy
    assert_eq!(t1.name(), t2.name());
}

#[test]
fn test_noop_default() {
    let t = NoopTransformer::default();
    assert_eq!(t.name(), "noop");
}

#[test]
fn test_noop_debug() {
    let t = NoopTransformer::new();
    let debug_str = format!("{:?}", t);
    assert_eq!(debug_str, "NoopTransformer");
}
