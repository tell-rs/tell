//! Router - Async batch routing with channel-based fan-out
//!
//! The `Router` receives batches from sources and routes them to the appropriate
//! sinks based on the pre-compiled routing table. Optionally transforms batches
//! before routing.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use crossfire::AsyncRx;
use tell_metrics::{PipelineMetricsProvider, PipelineSnapshot};
use tell_protocol::{Batch, SourceId};
use tell_routing::{RoutingTable, SinkId};
use tell_tap::TapPoint;
use tell_transform::Chain;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::metrics::{BackpressureTracker, RouterMetrics};
use crate::sink_handle::SinkHandle;

/// Async router that connects sources to sinks
///
/// # Design
///
/// - Receives `Batch` from sources via an input channel
/// - Wraps batches in `Arc` for zero-copy fan-out to multiple sinks
/// - Uses pre-compiled `RoutingTable` for O(1) routing decisions
/// - Handles backpressure via `try_send` with metrics tracking
/// - Uses `Vec<Option<SinkHandle>>` indexed by `SinkId` for O(1) sink lookup
///
/// # Example
///
/// ```ignore
/// let (tx, rx) = mpsc::channel(1000);
/// let router = Router::new(routing_table);
///
/// // Register sinks
/// let (sink_tx, sink_rx) = mpsc::channel(1000);
/// let handle = SinkHandle::new(SinkId::new(0), "clickhouse", sink_tx);
/// router.register_sink(handle);
///
/// // Run router (typically spawned as a task)
/// router.run(rx).await;
/// ```
pub struct Router {
    /// Pre-compiled routing table for O(1) source→sinks lookup
    routing_table: RoutingTable,

    /// Registered sink handles indexed by SinkId for O(1) lookup
    /// Uses Option to allow sparse registration
    sinks: Vec<Option<SinkHandle>>,

    /// Router metrics (Arc for sharing with metrics handle)
    metrics: Arc<RouterMetrics>,

    /// Rate-limited backpressure logging
    backpressure_tracker: BackpressureTracker,

    /// Global transformer chain applied to all batches (fallback)
    transformers: Option<Chain>,

    /// Per-source transformer chains (takes precedence over global)
    source_transformers: HashMap<SourceId, Chain>,

    /// Optional tap point for live streaming to CLI clients
    tap_point: Option<Arc<TapPoint>>,
}

/// Handle for accessing router metrics externally
///
/// Implements `PipelineMetricsProvider` for use with the unified metrics reporter.
/// This handle remains valid even after the router is consumed by `run()`.
#[derive(Clone)]
pub struct RouterMetricsHandle {
    metrics: Arc<RouterMetrics>,
}

impl PipelineMetricsProvider for RouterMetricsHandle {
    fn pipeline_snapshot(&self) -> PipelineSnapshot {
        let s = self.metrics.snapshot();
        PipelineSnapshot {
            batches_received: s.batches_received,
            batches_routed: s.batches_routed,
            batches_dropped: s.batches_dropped,
            sink_sends_success: s.sink_sends_success,
            sink_sends_failed: s.sink_sends_failed,
            backpressure_events: s.backpressure_events,
            messages_processed: s.messages_processed,
            bytes_processed: s.bytes_processed,
        }
    }
}

impl Router {
    /// Create a new router with the given routing table
    ///
    /// The sink vector is pre-allocated based on the number of sinks
    /// registered in the routing table.
    pub fn new(routing_table: RoutingTable) -> Self {
        let sink_count = routing_table.sink_count();
        let mut sinks = Vec::with_capacity(sink_count);
        sinks.resize_with(sink_count, || None);

        Self {
            routing_table,
            sinks,
            metrics: Arc::new(RouterMetrics::new()),
            backpressure_tracker: BackpressureTracker::new(),
            transformers: None,
            source_transformers: HashMap::new(),
            tap_point: None,
        }
    }

    /// Get a metrics handle for reporting
    ///
    /// The handle implements `PipelineMetricsProvider` and can be registered
    /// with the unified metrics reporter. It remains valid even after `run()`
    /// consumes the router.
    pub fn metrics_handle(&self) -> RouterMetricsHandle {
        RouterMetricsHandle {
            metrics: Arc::clone(&self.metrics),
        }
    }

    /// Set the global transformer chain for this router
    ///
    /// Global transformers are applied to batches that don't have
    /// a source-specific transformer chain. Pass an empty chain to disable.
    pub fn set_transformers(&mut self, chain: Chain) {
        if chain.is_enabled() {
            tracing::info!(
                transformers = ?chain.names(),
                "global transformer chain configured"
            );
            self.transformers = Some(chain);
        } else {
            self.transformers = None;
        }
    }

    /// Set a transformer chain for a specific source
    ///
    /// Source-specific transformers take precedence over the global chain.
    /// Use this for per-route transformer configurations.
    pub fn set_source_transformers(&mut self, source_id: SourceId, chain: Chain) {
        if chain.is_enabled() {
            tracing::info!(
                source = %source_id,
                transformers = ?chain.names(),
                "source-specific transformer chain configured"
            );
            self.source_transformers.insert(source_id, chain);
        }
    }

    /// Check if any transformers are configured (global or source-specific)
    #[inline]
    pub fn has_transformers(&self) -> bool {
        self.transformers.as_ref().is_some_and(|c| c.is_enabled())
            || !self.source_transformers.is_empty()
    }

    /// Get the number of source-specific transformer chains
    #[inline]
    pub fn source_transformer_count(&self) -> usize {
        self.source_transformers.len()
    }

    /// Get transformer names (for diagnostics - global chain only)
    pub fn transformer_names(&self) -> Vec<&'static str> {
        self.transformers
            .as_ref()
            .map(|c| c.names())
            .unwrap_or_default()
    }

    /// Set the tap point for live streaming to CLI clients
    ///
    /// The tap point receives all batches after transformation and broadcasts
    /// them to connected subscribers. It uses metadata-only filtering for
    /// minimal performance impact on the hot path.
    pub fn set_tap_point(&mut self, tap_point: Arc<TapPoint>) {
        tracing::info!("tap point configured for live streaming");
        self.tap_point = Some(tap_point);
    }

    /// Get the tap point (if configured)
    pub fn tap_point(&self) -> Option<&Arc<TapPoint>> {
        self.tap_point.as_ref()
    }

    /// Check if tap point is configured
    #[inline]
    pub fn has_tap_point(&self) -> bool {
        self.tap_point.is_some()
    }

    /// Register a sink with the router
    ///
    /// The sink's `SinkId` determines its position in the internal array.
    /// If the sink ID is beyond the current capacity, the array is extended.
    ///
    /// # Panics
    ///
    /// Panics if the sink ID is larger than `u16::MAX`.
    pub fn register_sink(&mut self, handle: SinkHandle) {
        let index = handle.id().as_usize();

        // Extend the vector if necessary
        if index >= self.sinks.len() {
            self.sinks.resize_with(index + 1, || None);
        }

        tracing::debug!(
            sink_id = %handle.id(),
            sink_name = %handle.name(),
            "registered sink with router"
        );

        self.sinks[index] = Some(handle);
    }

    /// Unregister a sink from the router
    ///
    /// Returns the sink handle if it was registered.
    pub fn unregister_sink(&mut self, id: SinkId) -> Option<SinkHandle> {
        let index = id.as_usize();
        if index < self.sinks.len() {
            self.sinks[index].take()
        } else {
            None
        }
    }

    /// Check if a sink is registered
    #[inline]
    pub fn has_sink(&self, id: SinkId) -> bool {
        let index = id.as_usize();
        index < self.sinks.len() && self.sinks[index].is_some()
    }

    /// Get the number of registered sinks
    pub fn sink_count(&self) -> usize {
        self.sinks.iter().filter(|s| s.is_some()).count()
    }

    /// Get the current router metrics
    #[inline]
    pub fn metrics(&self) -> &RouterMetrics {
        &self.metrics
    }

    /// Get a reference to the routing table
    #[inline]
    pub fn routing_table(&self) -> &RoutingTable {
        &self.routing_table
    }

    /// Route a batch to the appropriate sinks
    ///
    /// This is the hot path - optimized for minimal allocations:
    /// - Optional transformer chain (applied first)
    /// - O(1) routing table lookup
    /// - O(1) sink lookup by index
    /// - Single Arc allocation for multi-sink fan-out
    /// - Non-blocking try_send with backpressure tracking
    ///
    /// # Returns
    ///
    /// The number of sinks the batch was successfully sent to.
    pub async fn route(&self, batch: Batch) -> usize {
        // Record batch received
        self.metrics
            .record_received(batch.count() as u64, batch.total_bytes() as u64);

        // Apply transformers if configured
        let batch = self.apply_transformers(batch).await;

        // O(1) lookup - no allocation
        let sink_ids = self.routing_table.route(batch.source_id());

        if sink_ids.is_empty() {
            tracing::trace!(
                source_id = %batch.source_id(),
                "no sinks configured for source, dropping batch"
            );
            self.metrics.record_dropped();
            return 0;
        }

        // Wrap in Arc for zero-copy fan-out
        // This is the only allocation in the hot path
        let batch = Arc::new(batch);

        // Tap the batch for live streaming (if configured)
        // This is a no-op if no subscribers are connected
        if let Some(ref tap_point) = self.tap_point {
            tap_point.tap(Arc::clone(&batch));
        }

        let mut success_count = 0;

        for &sink_id in sink_ids {
            let index = sink_id.as_usize();

            // O(1) lookup by index
            let handle = match self.sinks.get(index).and_then(|h| h.as_ref()) {
                Some(h) => h,
                None => {
                    tracing::warn!(
                        sink_id = %sink_id,
                        "sink not registered, skipping"
                    );
                    self.metrics.record_sink_send_failed();
                    continue;
                }
            };

            // Check if sink is closed
            if handle.is_closed() {
                tracing::warn!(
                    sink_id = %sink_id,
                    sink_name = %handle.name(),
                    "sink channel closed, skipping"
                );
                self.metrics.record_sink_send_failed();
                continue;
            }

            // Try non-blocking send
            match handle.try_send(Arc::clone(&batch)) {
                Ok(()) => {
                    self.metrics.record_sink_send_success();
                    success_count += 1;
                }
                Err(_) => {
                    // Channel full - backpressure
                    self.metrics.record_backpressure();
                    self.metrics.record_sink_send_failed();

                    // Rate-limited logging (aggregates to 1 log/sec)
                    self.backpressure_tracker.record_drop(batch.count() as u64);

                    tracing::debug!(
                        sink_id = %sink_id,
                        sink_name = %handle.name(),
                        capacity = handle.capacity(),
                        "sink channel full (backpressure)"
                    );
                }
            }
        }

        if success_count > 0 {
            self.metrics.record_routed();
        } else {
            self.metrics.record_dropped();
            tracing::warn!(
                source_id = %batch.source_id(),
                target_sinks = sink_ids.len(),
                "batch dropped: all sink sends failed"
            );
        }

        success_count
    }

    /// Route a batch with blocking sends (waits for channel capacity)
    ///
    /// Unlike `route()`, this method will wait if a sink channel is full.
    /// Use this when you want guaranteed delivery over performance.
    ///
    /// # Returns
    ///
    /// The number of sinks the batch was successfully sent to.
    pub async fn route_blocking(&self, batch: Batch) -> usize {
        self.metrics
            .record_received(batch.count() as u64, batch.total_bytes() as u64);

        // Apply transformers if configured
        let batch = self.apply_transformers(batch).await;

        let sink_ids = self.routing_table.route(batch.source_id());

        if sink_ids.is_empty() {
            self.metrics.record_dropped();
            return 0;
        }

        let batch = Arc::new(batch);

        // Tap the batch for live streaming (if configured)
        if let Some(ref tap_point) = self.tap_point {
            tap_point.tap(Arc::clone(&batch));
        }

        let mut success_count = 0;

        for &sink_id in sink_ids {
            let index = sink_id.as_usize();

            let handle = match self.sinks.get(index).and_then(|h| h.as_ref()) {
                Some(h) => h,
                None => {
                    self.metrics.record_sink_send_failed();
                    continue;
                }
            };

            if handle.is_closed() {
                self.metrics.record_sink_send_failed();
                continue;
            }

            // Blocking send - waits for capacity
            match handle.send(Arc::clone(&batch)).await {
                Ok(()) => {
                    self.metrics.record_sink_send_success();
                    success_count += 1;
                }
                Err(_) => {
                    // Channel closed
                    self.metrics.record_sink_send_failed();
                }
            }
        }

        if success_count > 0 {
            self.metrics.record_routed();
        } else {
            self.metrics.record_dropped();
        }

        success_count
    }

    /// Apply transformer chain to a batch
    ///
    /// First checks for source-specific transformers, then falls back to global.
    /// Returns the original batch on error (graceful degradation).
    /// Logs warnings for transform errors and slow transforms (>10ms).
    async fn apply_transformers(&self, batch: Batch) -> Batch {
        // Check for source-specific transformer first
        let chain = if let Some(source_chain) = self.source_transformers.get(batch.source_id()) {
            if source_chain.is_enabled() {
                source_chain
            } else {
                return batch;
            }
        } else {
            // Fall back to global transformer
            match &self.transformers {
                Some(c) if c.is_enabled() => c,
                _ => return batch,
            }
        };

        let start = Instant::now();

        match chain.transform(batch.clone()).await {
            Ok(transformed) => {
                let duration = start.elapsed();

                // Warn on slow transforms
                if duration.as_millis() > 10 {
                    tracing::warn!(
                        duration_ms = duration.as_millis(),
                        transformers = ?chain.names(),
                        "slow transformer chain"
                    );
                }

                self.metrics.record_transform_success(duration);
                transformed
            }
            Err(e) => {
                // Graceful degradation: log error and continue with original batch
                tracing::warn!(
                    error = %e,
                    transformers = ?chain.names(),
                    "transformer chain failed, using original batch"
                );
                self.metrics.record_transform_error();
                batch
            }
        }
    }

    /// Run the router, processing batches from the input channel
    ///
    /// This method consumes the router and runs until the input channel is closed.
    /// Each batch is routed using the non-blocking `route()` method.
    pub async fn run(self, receiver: AsyncRx<Batch>) {
        tracing::info!(
            sink_count = self.sink_count(),
            route_count = self.routing_table.route_count(),
            global_transformers = ?self.transformer_names(),
            source_transformer_count = self.source_transformer_count(),
            tap_enabled = self.tap_point.is_some(),
            "router starting"
        );

        while let Ok(batch) = receiver.recv().await {
            self.route(batch).await;
        }

        // Log final metrics
        let snapshot = self.metrics.snapshot();
        tracing::info!(
            batches_received = snapshot.batches_received,
            batches_routed = snapshot.batches_routed,
            batches_dropped = snapshot.batches_dropped,
            sink_sends_success = snapshot.sink_sends_success,
            sink_sends_failed = snapshot.sink_sends_failed,
            backpressure_events = snapshot.backpressure_events,
            messages_processed = snapshot.messages_processed,
            bytes_processed = snapshot.bytes_processed,
            "router shutting down"
        );
    }

    /// Run the router with blocking sends
    ///
    /// Same as `run()` but uses `route_blocking()` for guaranteed delivery.
    pub async fn run_blocking(self, mut receiver: mpsc::Receiver<Batch>) {
        tracing::info!(
            sink_count = self.sink_count(),
            route_count = self.routing_table.route_count(),
            transformers = ?self.transformer_names(),
            tap_enabled = self.tap_point.is_some(),
            "router starting (blocking mode)"
        );

        while let Some(batch) = receiver.recv().await {
            self.route_blocking(batch).await;
        }

        let snapshot = self.metrics.snapshot();
        tracing::info!(
            batches_received = snapshot.batches_received,
            batches_routed = snapshot.batches_routed,
            batches_dropped = snapshot.batches_dropped,
            "router shutting down"
        );
    }

    /// Run the router with multiple workers processing from sharded channels
    ///
    /// This method spawns N worker tasks, each processing from its own receiver.
    /// This reduces contention when many sources are sending batches simultaneously.
    ///
    /// # Arguments
    ///
    /// * `receivers` - One receiver per worker/shard
    ///
    /// # Returns
    ///
    /// A vector of JoinHandles for the spawned worker tasks.
    pub fn run_sharded(self, receivers: Vec<AsyncRx<Batch>>) -> Vec<JoinHandle<()>> {
        let worker_count = receivers.len();

        tracing::info!(
            sink_count = self.sink_count(),
            route_count = self.routing_table.route_count(),
            global_transformers = ?self.transformer_names(),
            source_transformer_count = self.source_transformer_count(),
            tap_enabled = self.tap_point.is_some(),
            worker_count = worker_count,
            "router starting (sharded mode)"
        );

        // Wrap self in Arc for sharing across workers
        let router = Arc::new(self);
        let mut handles = Vec::with_capacity(worker_count);

        for (worker_id, receiver) in receivers.into_iter().enumerate() {
            let router = Arc::clone(&router);
            let handle = tokio::spawn(async move {
                tracing::debug!(worker_id, "router worker starting");

                while let Ok(batch) = receiver.recv().await {
                    router.route(batch).await;
                }

                tracing::debug!(worker_id, "router worker stopping");
            });
            handles.push(handle);
        }

        handles
    }
}

impl std::fmt::Debug for Router {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Router")
            .field("sink_count", &self.sink_count())
            .field("route_count", &self.routing_table.route_count())
            .finish()
    }
}
