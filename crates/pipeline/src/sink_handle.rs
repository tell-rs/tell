//! Sink handle for pipeline communication
//!
//! `SinkHandle` wraps a channel sender and sink identifier, allowing the router
//! to send batches to sinks without knowing their concrete types.

use std::sync::Arc;

use cdp_protocol::Batch;
use cdp_routing::SinkId;
use tokio::sync::mpsc;

/// Handle to a sink for sending batches
///
/// The router uses `SinkHandle` to send batches to sinks without knowing
/// their concrete implementation. Each sink creates a handle during
/// initialization and registers it with the router.
///
/// # Design
///
/// - Uses `cdp_routing::SinkId` (u16) for O(1) array indexing
/// - Stores sink name for debugging/logging
/// - Wraps `mpsc::Sender<Arc<Batch>>` for zero-copy fan-out
///
/// # Example
///
/// ```ignore
/// let (tx, rx) = mpsc::channel(1000);
/// let handle = SinkHandle::new(SinkId::new(0), "clickhouse", tx);
///
/// // Register with router
/// router.register_sink(handle);
///
/// // Router can now send batches to this sink
/// ```
pub struct SinkHandle {
    /// Unique identifier for this sink (u16 index)
    id: SinkId,

    /// Human-readable name for debugging/metrics
    name: String,

    /// Channel sender for batches
    ///
    /// Uses `Arc<Batch>` to allow zero-copy fan-out to multiple sinks.
    sender: mpsc::Sender<Arc<Batch>>,
}

impl SinkHandle {
    /// Create a new sink handle
    ///
    /// # Arguments
    ///
    /// * `id` - The sink's unique identifier from the routing table
    /// * `name` - Human-readable name for logging/debugging
    /// * `sender` - Channel sender for receiving batches
    #[inline]
    pub fn new(id: SinkId, name: impl Into<String>, sender: mpsc::Sender<Arc<Batch>>) -> Self {
        Self {
            id,
            name: name.into(),
            sender,
        }
    }

    /// Get the sink's unique identifier
    #[inline]
    pub fn id(&self) -> SinkId {
        self.id
    }

    /// Get the sink's name
    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get a reference to the underlying sender
    #[inline]
    pub fn sender(&self) -> &mpsc::Sender<Arc<Batch>> {
        &self.sender
    }

    /// Try to send a batch without blocking
    ///
    /// Returns `Ok(())` if the batch was sent, or the batch back if the
    /// channel is full (backpressure) or closed.
    #[inline]
    pub fn try_send(&self, batch: Arc<Batch>) -> Result<(), Arc<Batch>> {
        self.sender.try_send(batch).map_err(|e| match e {
            mpsc::error::TrySendError::Full(b) => b,
            mpsc::error::TrySendError::Closed(b) => b,
        })
    }

    /// Send a batch, waiting if the channel is full
    ///
    /// Returns `Ok(())` if the batch was sent, or `Err` if the channel is closed.
    #[inline]
    pub async fn send(&self, batch: Arc<Batch>) -> Result<(), Arc<Batch>> {
        self.sender.send(batch).await.map_err(|e| e.0)
    }

    /// Check if the sink channel is closed
    #[inline]
    pub fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }

    /// Get the current capacity of the channel
    #[inline]
    pub fn capacity(&self) -> usize {
        self.sender.capacity()
    }

    /// Get the maximum capacity of the channel
    #[inline]
    pub fn max_capacity(&self) -> usize {
        self.sender.max_capacity()
    }
}

impl std::fmt::Debug for SinkHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SinkHandle")
            .field("id", &self.id)
            .field("name", &self.name)
            .field("closed", &self.is_closed())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sink_handle_creation() {
        let (tx, _rx) = mpsc::channel::<Arc<Batch>>(10);
        let handle = SinkHandle::new(SinkId::new(5), "test_sink", tx);

        assert_eq!(handle.id(), SinkId::new(5));
        assert_eq!(handle.name(), "test_sink");
        assert!(!handle.is_closed());
    }

    #[test]
    fn test_sink_handle_debug() {
        let (tx, _rx) = mpsc::channel::<Arc<Batch>>(10);
        let handle = SinkHandle::new(SinkId::new(1), "debug_sink", tx);

        let debug = format!("{:?}", handle);
        assert!(debug.contains("debug_sink"));
        assert!(debug.contains("SinkHandle"));
    }

    #[tokio::test]
    async fn test_sink_handle_closed_detection() {
        let (tx, rx) = mpsc::channel::<Arc<Batch>>(10);
        let handle = SinkHandle::new(SinkId::new(0), "test", tx);

        assert!(!handle.is_closed());

        // Drop receiver to close channel
        drop(rx);

        assert!(handle.is_closed());
    }

    #[tokio::test]
    async fn test_sink_handle_capacity() {
        let (tx, _rx) = mpsc::channel::<Arc<Batch>>(100);
        let handle = SinkHandle::new(SinkId::new(0), "test", tx);

        assert_eq!(handle.max_capacity(), 100);
        assert_eq!(handle.capacity(), 100);
    }
}
