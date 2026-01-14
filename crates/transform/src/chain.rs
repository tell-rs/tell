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
//! - **Async-ready**: Supports async transformers for future use cases

use crate::{TransformResult, Transformer};
use cdp_protocol::Batch;

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
        // Fast path: no transformers enabled
        if !self.enabled {
            return Ok(batch);
        }

        let mut current = batch;

        for transformer in &self.transformers {
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
}

impl Default for Chain {
    fn default() -> Self {
        Self::empty()
    }
}
