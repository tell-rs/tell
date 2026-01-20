//! Metadata filter for tap subscriptions
//!
//! `TapFilter` performs O(1) filtering on batch metadata without decoding
//! the FlatBuffer payload. This keeps Tell at 22M/sec throughput.
//!
//! # Filter Logic
//!
//! - All filters are optional (None = match all)
//! - Multiple values in a filter are OR'd (match any)
//! - Different filters are AND'd (must match all specified filters)
//!
//! # Example
//!
//! ```
//! use tell_tap::TapFilter;
//! use tell_protocol::BatchType;
//!
//! // Match workspace 42, any event or log type
//! let filter = TapFilter::new()
//!     .with_workspaces(vec![42])
//!     .with_batch_types(vec![BatchType::Event, BatchType::Log]);
//! ```

use std::collections::HashSet;

use tell_protocol::{Batch, BatchType, SourceId};

use crate::SubscribeRequest;

/// Metadata-only filter for batch matching
///
/// All checks are O(1) operations on data already present in the Batch struct.
/// No FlatBuffer decoding is performed.
#[derive(Debug, Clone, Default)]
pub struct TapFilter {
    /// Workspace IDs to match (None = match all)
    workspace_ids: Option<HashSet<u32>>,
    /// Source IDs to match (None = match all)
    source_ids: Option<HashSet<SourceId>>,
    /// Batch types to match (None = match all)
    batch_types: Option<HashSet<BatchType>>,
}

impl TapFilter {
    /// Create an empty filter (matches everything)
    pub fn new() -> Self {
        Self::default()
    }

    /// Create filter from a subscribe request
    pub fn from_subscribe(req: &SubscribeRequest) -> Self {
        let workspace_ids = req
            .workspace_ids
            .as_ref()
            .map(|ids| ids.iter().copied().collect());

        let source_ids = req
            .source_ids
            .as_ref()
            .map(|ids| ids.iter().map(|s| SourceId::new(s.clone())).collect());

        let batch_types = req.batch_types.as_ref().map(|types| {
            types
                .iter()
                .filter_map(|&t| BatchType::try_from(t).ok())
                .collect()
        });

        Self {
            workspace_ids,
            source_ids,
            batch_types,
        }
    }

    /// Add workspace filter
    pub fn with_workspaces(mut self, ids: Vec<u32>) -> Self {
        self.workspace_ids = Some(ids.into_iter().collect());
        self
    }

    /// Add source filter
    pub fn with_sources(mut self, ids: Vec<SourceId>) -> Self {
        self.source_ids = Some(ids.into_iter().collect());
        self
    }

    /// Add batch type filter
    pub fn with_batch_types(mut self, types: Vec<BatchType>) -> Self {
        self.batch_types = Some(types.into_iter().collect());
        self
    }

    /// Check if filter is empty (matches everything)
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.workspace_ids.is_none() && self.source_ids.is_none() && self.batch_types.is_none()
    }

    /// Check if a batch matches this filter
    ///
    /// This is the hot path - must be fast. All operations are O(1)
    /// using HashSet lookups on batch metadata.
    #[inline]
    pub fn matches(&self, batch: &Batch) -> bool {
        // Fast path: empty filter matches everything
        if self.is_empty() {
            return true;
        }

        // Check workspace
        if let Some(ref ids) = self.workspace_ids
            && !ids.contains(&batch.workspace_id())
        {
            return false;
        }

        // Check source
        if let Some(ref ids) = self.source_ids
            && !ids.contains(batch.source_id())
        {
            return false;
        }

        // Check batch type
        if let Some(ref types) = self.batch_types
            && !types.contains(&batch.batch_type())
        {
            return false;
        }

        true
    }

    /// Get workspace filter (for debugging/logging)
    pub fn workspace_ids(&self) -> Option<&HashSet<u32>> {
        self.workspace_ids.as_ref()
    }

    /// Get source filter (for debugging/logging)
    pub fn source_ids(&self) -> Option<&HashSet<SourceId>> {
        self.source_ids.as_ref()
    }

    /// Get batch type filter (for debugging/logging)
    pub fn batch_types(&self) -> Option<&HashSet<BatchType>> {
        self.batch_types.as_ref()
    }
}

#[cfg(test)]
#[path = "filter_test.rs"]
mod tests;
