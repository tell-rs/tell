//! Tests for transformer chain

use super::*;
use crate::noop::NoopTransformer;
use crate::TransformError;
use tell_protocol::{BatchBuilder, BatchType, SourceId};
use tokio_util::sync::CancellationToken;

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

#[test]
fn test_chain_close_empty() {
    let chain = Chain::empty();
    let result = chain.close();
    assert!(result.is_ok());
}

#[test]
fn test_chain_close_with_noop() {
    let chain = Chain::new(vec![Box::new(NoopTransformer)]);
    let result = chain.close();
    assert!(result.is_ok());
}

#[test]
fn test_chain_close_calls_all_transformers() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let close_count = Arc::new(AtomicUsize::new(0));

    struct CountingTransformer {
        close_count: Arc<AtomicUsize>,
        name: &'static str,
    }

    impl Transformer for CountingTransformer {
        fn transform<'a>(
            &'a self,
            batch: Batch,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = TransformResult<Batch>> + Send + 'a>,
        > {
            Box::pin(async move { Ok(batch) })
        }

        fn name(&self) -> &'static str {
            self.name
        }

        fn close(&self) -> TransformResult<()> {
            self.close_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    let chain = Chain::new(vec![
        Box::new(CountingTransformer {
            close_count: Arc::clone(&close_count),
            name: "first",
        }),
        Box::new(CountingTransformer {
            close_count: Arc::clone(&close_count),
            name: "second",
        }),
        Box::new(CountingTransformer {
            close_count: Arc::clone(&close_count),
            name: "third",
        }),
    ]);

    let result = chain.close();
    assert!(result.is_ok());
    assert_eq!(close_count.load(Ordering::SeqCst), 3);
}

#[test]
fn test_chain_close_continues_after_error() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let close_count = Arc::new(AtomicUsize::new(0));

    struct FailingCloseTransformer;

    impl Transformer for FailingCloseTransformer {
        fn transform<'a>(
            &'a self,
            batch: Batch,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = TransformResult<Batch>> + Send + 'a>,
        > {
            Box::pin(async move { Ok(batch) })
        }

        fn name(&self) -> &'static str {
            "failing_close"
        }

        fn close(&self) -> TransformResult<()> {
            Err(TransformError::failed("intentional close failure"))
        }
    }

    struct CountingTransformer {
        close_count: Arc<AtomicUsize>,
    }

    impl Transformer for CountingTransformer {
        fn transform<'a>(
            &'a self,
            batch: Batch,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = TransformResult<Batch>> + Send + 'a>,
        > {
            Box::pin(async move { Ok(batch) })
        }

        fn name(&self) -> &'static str {
            "counting"
        }

        fn close(&self) -> TransformResult<()> {
            self.close_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    let chain = Chain::new(vec![
        Box::new(CountingTransformer {
            close_count: Arc::clone(&close_count),
        }),
        Box::new(FailingCloseTransformer),
        Box::new(CountingTransformer {
            close_count: Arc::clone(&close_count),
        }),
    ]);

    let result = chain.close();

    // Should return error from failing transformer
    assert!(result.is_err());

    // But all transformers should have been closed
    assert_eq!(close_count.load(Ordering::SeqCst), 2, "All non-failing transformers should be closed");
}

#[tokio::test]
async fn test_transform_with_cancel_not_cancelled() {
    let chain = Chain::new(vec![Box::new(NoopTransformer)]);
    let cancel = CancellationToken::new();

    let batch = create_test_batch();
    let result = chain.transform_with_cancel(batch, Some(&cancel)).await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_transform_with_cancel_already_cancelled() {
    let chain = Chain::new(vec![Box::new(NoopTransformer)]);
    let cancel = CancellationToken::new();
    cancel.cancel(); // Cancel before transform

    let batch = create_test_batch();
    let result = chain.transform_with_cancel(batch, Some(&cancel)).await;

    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), TransformError::Cancelled));
}

#[tokio::test]
async fn test_transform_with_cancel_stops_between_transformers() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let transform_count = Arc::new(AtomicUsize::new(0));

    struct CountingTransformer {
        count: Arc<AtomicUsize>,
        cancel_after: usize,
        cancel_token: CancellationToken,
    }

    impl Transformer for CountingTransformer {
        fn transform<'a>(
            &'a self,
            batch: Batch,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = TransformResult<Batch>> + Send + 'a>,
        > {
            let current = self.count.fetch_add(1, Ordering::SeqCst);
            if current >= self.cancel_after {
                self.cancel_token.cancel();
            }
            Box::pin(async move { Ok(batch) })
        }

        fn name(&self) -> &'static str {
            "counting"
        }
    }

    let cancel = CancellationToken::new();

    let chain = Chain::new(vec![
        Box::new(CountingTransformer {
            count: Arc::clone(&transform_count),
            cancel_after: 0, // Cancel after first transform
            cancel_token: cancel.clone(),
        }) as Box<dyn Transformer>,
        Box::new(CountingTransformer {
            count: Arc::clone(&transform_count),
            cancel_after: 999, // Never cancel
            cancel_token: cancel.clone(),
        }),
        Box::new(CountingTransformer {
            count: Arc::clone(&transform_count),
            cancel_after: 999,
            cancel_token: cancel.clone(),
        }),
    ]);

    let batch = create_test_batch();
    let result = chain.transform_with_cancel(batch, Some(&cancel)).await;

    // Should be cancelled
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), TransformError::Cancelled));

    // Only first transformer should have run
    assert_eq!(transform_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_transform_with_cancel_none_is_same_as_transform() {
    let chain = Chain::new(vec![Box::new(NoopTransformer)]);

    let batch1 = create_test_batch();
    let batch2 = create_test_batch();

    let result1 = chain.transform(batch1).await;
    let result2 = chain.transform_with_cancel(batch2, None).await;

    assert!(result1.is_ok());
    assert!(result2.is_ok());
}

#[tokio::test]
async fn test_transform_empty_chain_ignores_cancel() {
    let chain = Chain::empty();
    let cancel = CancellationToken::new();
    cancel.cancel();

    let batch = create_test_batch();
    // Empty chain returns immediately without checking cancel
    let result = chain.transform_with_cancel(batch, Some(&cancel)).await;

    // Empty chain ignores cancel since it does nothing
    assert!(result.is_ok());
}
