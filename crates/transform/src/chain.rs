//! Transformer Chain - Sequential batch transformation
//!
//! The `Chain` applies multiple transformers in sequence to batches
//! flowing through the pipeline.
//!
//! # Design
//!
//! - **Zero-cost when empty**: Empty chain is a no-op with no overhead
//! - **Sequential execution**: Transformers run in order, each receiving
//!   the output of the previous
//! - **Fail-fast**: First error stops the chain and drops the batch
//! - **Cancellation-aware**: Checks for cancellation between transformers
//! - **Async-ready**: Supports async transformers for future use cases

use crate::{TransformResult, Transformer};
use tell_protocol::Batch;
use tokio_util::sync::CancellationToken;

#[cfg(test)]
#[path = "chain_test.rs"]
mod tests;

/// Chain of transformers applied sequentially
///
/// Transformers are applied in the order they were added.
/// If any transformer returns an error, the chain stops and
/// returns that error (the batch is dropped).
pub struct Chain {
    /// Ordered list of transformers
    transformers: Vec<Box<dyn Transformer>>,

    /// Whether any transformers are active
    enabled: bool,
}

impl Chain {
    /// Create a new transformer chain
    ///
    /// Only enabled transformers are included in the chain.
    /// If all transformers are disabled, the chain is a no-op.
    pub fn new(transformers: Vec<Box<dyn Transformer>>) -> Self {
        // Filter to only enabled transformers
        let active: Vec<_> = transformers.into_iter().filter(|t| t.enabled()).collect();

        let enabled = !active.is_empty();

        Self {
            transformers: active,
            enabled,
        }
    }

    /// Create an empty chain (no-op)
    pub fn empty() -> Self {
        Self {
            transformers: Vec::new(),
            enabled: false,
        }
    }

    /// Check if the chain has any active transformers
    #[inline]
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Get the number of active transformers
    #[inline]
    pub fn len(&self) -> usize {
        self.transformers.len()
    }

    /// Check if the chain is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.transformers.is_empty()
    }

    /// Get the names of all active transformers
    pub fn names(&self) -> Vec<&'static str> {
        self.transformers.iter().map(|t| t.name()).collect()
    }

    /// Transform a batch through all transformers in sequence
    ///
    /// # Fast Path
    ///
    /// If no transformers are enabled, this returns the batch unchanged
    /// with zero overhead (just a boolean check).
    ///
    /// # Error Handling
    ///
    /// If any transformer fails, the chain stops immediately and returns
    /// the error. The batch is considered dropped in this case.
    ///
    /// # Performance
    ///
    /// Each transformer should complete in microseconds. If a transformer
    /// takes longer than 10ms, consider logging a warning.
    pub async fn transform(&self, batch: Batch) -> TransformResult<Batch> {
        self.transform_with_cancel(batch, None).await
    }

    /// Transform a batch with cancellation support
    ///
    /// Like `transform`, but checks for cancellation between each transformer.
    /// If cancelled, returns a `Cancelled` error immediately.
    ///
    /// # Cancellation
    ///
    /// Cancellation is checked between transformers, allowing graceful shutdown.
    /// Individual transformers are not interrupted, but no new transformers
    /// will be started once cancelled.
    pub async fn transform_with_cancel(
        &self,
        batch: Batch,
        cancel: Option<&CancellationToken>,
    ) -> TransformResult<Batch> {
        // Fast path: no transformers enabled
        if !self.enabled {
            return Ok(batch);
        }

        let mut current = batch;

        for transformer in &self.transformers {
            // Check for cancellation between transformers
            if let Some(token) = cancel
                && token.is_cancelled()
            {
                return Err(crate::TransformError::cancelled());
            }

            current = transformer.transform(current).await?;
        }

        Ok(current)
    }

    /// Get a transformer by name
    ///
    /// Useful for accessing specific transformers for configuration
    /// or metrics.
    pub fn get(&self, name: &str) -> Option<&dyn Transformer> {
        self.transformers
            .iter()
            .find(|t| t.name() == name)
            .map(|t| t.as_ref())
    }

    /// Close all transformers in the chain
    ///
    /// Called during graceful shutdown. Closes all transformers
    /// in sequence, collecting errors but continuing to close remaining
    /// transformers even if one fails.
    ///
    /// # Returns
    ///
    /// Returns the first error encountered, or `Ok(())` if all
    /// transformers closed successfully.
    pub fn close(&self) -> TransformResult<()> {
        let mut first_error: Option<crate::TransformError> = None;

        for transformer in &self.transformers {
            if let Err(e) = transformer.close() {
                tracing::warn!(
                    transformer = transformer.name(),
                    error = %e,
                    "Failed to close transformer"
                );
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
        }

        match first_error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

impl Default for Chain {
    fn default() -> Self {
        Self::empty()
    }
}
