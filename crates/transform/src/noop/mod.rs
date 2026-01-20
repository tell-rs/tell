//! Noop Transformer - Pass-through transformer for testing
//!
//! The `NoopTransformer` passes batches through unchanged. It's useful for:
//! - Testing the transformer chain infrastructure
//! - Benchmarking transformer overhead
//! - Placeholder in development

use crate::{TransformResult, Transformer};
use tell_protocol::Batch;
use std::future::Future;
use std::pin::Pin;

#[cfg(test)]
mod noop_test;

/// A transformer that passes batches through unchanged
///
/// This is useful for:
/// - Testing transformer chain infrastructure
/// - Benchmarking to measure transformer overhead
/// - As a placeholder during development
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopTransformer;

impl NoopTransformer {
    /// Create a new noop transformer
    #[inline]
    pub const fn new() -> Self {
        Self
    }
}

impl Transformer for NoopTransformer {
    fn transform<'a>(
        &'a self,
        batch: Batch,
    ) -> Pin<Box<dyn Future<Output = TransformResult<Batch>> + Send + 'a>> {
        Box::pin(async move { Ok(batch) })
    }

    fn name(&self) -> &'static str {
        "noop"
    }

    fn enabled(&self) -> bool {
        true
    }
}
