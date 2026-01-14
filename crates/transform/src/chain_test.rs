//! Tests for transformer chain

use super::*;
use crate::noop::NoopTransformer;
use crate::TransformError;
use cdp_protocol::{BatchBuilder, BatchType, SourceId};

fn create_test_batch() -> Batch {
    let source_id = SourceId::new("test");
    let mut builder = BatchBuilder::new(BatchType::Event, source_id);
    builder.add(b"test data", 1);
    builder.finish()
}

#[tokio::test]
async fn test_empty_chain() {
    let chain = Chain::empty();

    assert!(!chain.is_enabled());
    assert!(chain.is_empty());
    assert_eq!(chain.len(), 0);

    let batch = create_test_batch();
    let result = chain.transform(batch).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_default_chain_is_empty() {
    let chain = Chain::default();
    assert!(!chain.is_enabled());
    assert!(chain.is_empty());
}

#[tokio::test]
async fn test_chain_with_noop() {
    let chain = Chain::new(vec![Box::new(NoopTransformer)]);

    assert!(chain.is_enabled());
    assert!(!chain.is_empty());
    assert_eq!(chain.len(), 1);
    assert_eq!(chain.names(), vec!["noop"]);

    let batch = create_test_batch();
    let original_count = batch.count();

    let result = chain.transform(batch).await;
    assert!(result.is_ok());

    let transformed = result.unwrap();
    assert_eq!(transformed.count(), original_count);
}

#[tokio::test]
async fn test_chain_filters_disabled() {
    // Create a disabled transformer
    struct DisabledTransformer;

    impl Transformer for DisabledTransformer {
        fn transform<'a>(
            &'a self,
            batch: Batch,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = TransformResult<Batch>> + Send + 'a>,
        > {
            Box::pin(async move { Ok(batch) })
        }

        fn name(&self) -> &'static str {
            "disabled"
        }

        fn enabled(&self) -> bool {
            false
        }
    }

    let chain = Chain::new(vec![
        Box::new(DisabledTransformer),
        Box::new(NoopTransformer),
    ]);

    // Only noop should be active
    assert_eq!(chain.len(), 1);
    assert_eq!(chain.names(), vec!["noop"]);
}

#[test]
fn test_get_transformer_by_name() {
    let chain = Chain::new(vec![Box::new(NoopTransformer)]);

    let found = chain.get("noop");
    assert!(found.is_some());
    assert_eq!(found.unwrap().name(), "noop");

    let not_found = chain.get("nonexistent");
    assert!(not_found.is_none());
}

#[tokio::test]
async fn test_chain_sequential_execution() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    // Track execution order
    let counter = Arc::new(AtomicUsize::new(0));

    struct OrderedTransformer {
        expected_order: usize,
        counter: Arc<AtomicUsize>,
        name: &'static str,
    }

    impl Transformer for OrderedTransformer {
        fn transform<'a>(
            &'a self,
            batch: Batch,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = TransformResult<Batch>> + Send + 'a>,
        > {
            let current = self.counter.fetch_add(1, Ordering::SeqCst);
            assert_eq!(
                current, self.expected_order,
                "Transformer {} executed out of order",
                self.name
            );
            Box::pin(async move { Ok(batch) })
        }

        fn name(&self) -> &'static str {
            self.name
        }
    }

    let chain = Chain::new(vec![
        Box::new(OrderedTransformer {
            expected_order: 0,
            counter: Arc::clone(&counter),
            name: "first",
        }),
        Box::new(OrderedTransformer {
            expected_order: 1,
            counter: Arc::clone(&counter),
            name: "second",
        }),
        Box::new(OrderedTransformer {
            expected_order: 2,
            counter: Arc::clone(&counter),
            name: "third",
        }),
    ]);

    let batch = create_test_batch();
    let result = chain.transform(batch).await;
    assert!(result.is_ok());
    assert_eq!(counter.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn test_chain_error_stops_execution() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    let second_called = Arc::new(AtomicBool::new(false));

    struct FailingTransformer;

    impl Transformer for FailingTransformer {
        fn transform<'a>(
            &'a self,
            _batch: Batch,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = TransformResult<Batch>> + Send + 'a>,
        > {
            Box::pin(async move { Err(TransformError::failed("intentional failure")) })
        }

        fn name(&self) -> &'static str {
            "failing"
        }
    }

    struct TrackingTransformer {
        called: Arc<AtomicBool>,
    }

    impl Transformer for TrackingTransformer {
        fn transform<'a>(
            &'a self,
            batch: Batch,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = TransformResult<Batch>> + Send + 'a>,
        > {
            self.called.store(true, Ordering::SeqCst);
            Box::pin(async move { Ok(batch) })
        }

        fn name(&self) -> &'static str {
            "tracking"
        }
    }

    let chain = Chain::new(vec![
        Box::new(FailingTransformer),
        Box::new(TrackingTransformer {
            called: Arc::clone(&second_called),
        }),
    ]);

    let batch = create_test_batch();
    let result = chain.transform(batch).await;

    assert!(result.is_err());
    assert!(!second_called.load(Ordering::SeqCst), "Second transformer should not be called after error");
}
