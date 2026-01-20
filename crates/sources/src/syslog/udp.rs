//! Syslog UDP Source
//!
//! High-performance syslog receiver over UDP with multi-worker support.
//!
//! # Protocol Support
//!
//! - **RFC 3164** (BSD syslog) - Legacy format, still widely used
//! - **RFC 5424** (IETF syslog) - Structured data support
//!
//! Messages are stored raw - parsing happens downstream in sinks/transformers.
//!
//! # Design
//!
//! Unlike TCP, UDP is connectionless so we use a different strategy:
//! - Multiple workers share the same socket (SO_REUSEPORT)
//! - Each worker receives datagrams and batches messages
//! - Periodic flush ensures timely delivery
//!
//! # Features
//!
//! - **Multi-worker** - Parallel packet processing for high throughput
//! - **SO_REUSEPORT** - Kernel-level load balancing across workers
//! - **Zero-copy where possible** - Minimize allocations in hot path
//! - **Graceful degradation** - Drops packets rather than blocking
//!
//! # Example
//!
//! ```ignore
//! let config = SyslogUdpSourceConfig {
//!     address: "0.0.0.0".into(),
//!     port: 514,
//!     workspace_id: 1,
//!     num_workers: 4,
//!     ..Default::default()
//! };
//!
//! let (batch_tx, batch_rx) = mpsc::channel(1000);
//! let source = SyslogUdpSource::new(config, batch_tx);
//! source.run().await?;
//! ```

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use tell_protocol::{BatchBuilder, BatchType, SourceId};
use crossfire::TrySendError;
use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::UdpSocket;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;

use tell_metrics::{SourceMetricsProvider, SourceMetricsSnapshot};

use crate::common::SourceMetrics;
use crate::ShardedSender;

// =============================================================================
// Constants
// =============================================================================

/// Default syslog port (privileged - may need root)
const DEFAULT_PORT: u16 = 514;

/// Default maximum syslog message size (8KB)
const DEFAULT_MAX_MESSAGE_SIZE: usize = 8192;

/// Default socket buffer size (64KB for UDP)
const DEFAULT_BUFFER_SIZE: usize = 64 * 1024;

/// Default batch size (number of messages before flush)
const DEFAULT_BATCH_SIZE: usize = 500;

/// Default flush interval (50ms - faster than TCP for UDP responsiveness)
const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_millis(50);

/// Default number of workers
const DEFAULT_NUM_WORKERS: usize = 4;

/// Default queue size (larger for UDP bursts)
const DEFAULT_QUEUE_SIZE: usize = 1000;

/// Socket buffer multiplier for UDP (4x like Go)
const UDP_BUFFER_MULTIPLIER: usize = 4;

// =============================================================================
// Configuration
// =============================================================================

/// Syslog UDP source configuration
#[derive(Debug, Clone)]
pub struct SyslogUdpSourceConfig {
    /// Source identifier for routing
    pub id: String,

    /// Bind address (e.g., "0.0.0.0")
    pub address: String,

    /// Listen port
    pub port: u16,

    /// Socket buffer size
    pub buffer_size: usize,

    /// Channel queue size
    pub queue_size: usize,

    /// Maximum messages per batch
    pub batch_size: usize,

    /// Flush interval for partial batches
    pub flush_interval: Duration,

    /// Number of worker tasks
    pub num_workers: usize,

    /// Workspace ID (syslog has no API keys)
    pub workspace_id: u32,

    /// Maximum syslog message size
    pub max_message_size: usize,
}

impl Default for SyslogUdpSourceConfig {
    fn default() -> Self {
        Self {
            id: "syslog_udp".into(),
            address: "0.0.0.0".into(),
            port: DEFAULT_PORT,
            buffer_size: DEFAULT_BUFFER_SIZE,
            queue_size: DEFAULT_QUEUE_SIZE,
            batch_size: DEFAULT_BATCH_SIZE,
            flush_interval: DEFAULT_FLUSH_INTERVAL,
            num_workers: DEFAULT_NUM_WORKERS,
            workspace_id: 1,
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
        }
    }
}

impl SyslogUdpSourceConfig {
    /// Create config with custom port
    pub fn with_port(port: u16) -> Self {
        Self {
            port,
            ..Default::default()
        }
    }

    /// Get the socket address to bind to
    pub fn bind_address(&self) -> String {
        format!("{}:{}", self.address, self.port)
    }

    /// Get the source ID
    pub fn source_id(&self) -> SourceId {
        SourceId::new(&self.id)
    }
}

// =============================================================================
// Metrics
// =============================================================================

/// Syslog UDP source metrics
#[derive(Debug, Default)]
pub struct SyslogUdpSourceMetrics {
    /// Base source metrics (repurposed: connections_active = workers_active)
    pub base: SourceMetrics,

    /// Packets received
    pub packets_received: AtomicU64,

    /// Packets dropped (queue full, etc.)
    pub packets_dropped: AtomicU64,

    /// Malformed messages (oversized, etc.)
    pub messages_malformed: AtomicU64,

    /// Worker errors
    pub worker_errors: AtomicU64,
}

impl SyslogUdpSourceMetrics {
    /// Create new metrics instance
    pub const fn new() -> Self {
        Self {
            base: SourceMetrics::new(),
            packets_received: AtomicU64::new(0),
            packets_dropped: AtomicU64::new(0),
            messages_malformed: AtomicU64::new(0),
            worker_errors: AtomicU64::new(0),
        }
    }

    /// Record a packet received
    #[inline]
    pub fn packet_received(&self, bytes: u64) {
        self.packets_received.fetch_add(1, Ordering::Relaxed);
        self.base.bytes_received.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record a malformed message (oversized, etc.)
    #[inline]
    pub fn message_malformed(&self) {
        self.messages_malformed.fetch_add(1, Ordering::Relaxed);
        self.base.errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a dropped packet
    #[inline]
    pub fn packet_dropped(&self) {
        self.packets_dropped.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a worker error
    #[inline]
    pub fn worker_error(&self) {
        self.worker_errors.fetch_add(1, Ordering::Relaxed);
        self.base.errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Record worker started
    #[inline]
    pub fn worker_started(&self) {
        self.base.connections_active.fetch_add(1, Ordering::Relaxed);
        self.base.connections_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record worker stopped
    #[inline]
    pub fn worker_stopped(&self) {
        self.base.connections_active.fetch_sub(1, Ordering::Relaxed);
    }

    /// Get extended metrics snapshot
    pub fn snapshot(&self) -> SyslogUdpMetricsSnapshot {
        SyslogUdpMetricsSnapshot {
            workers_active: self.base.connections_active.load(Ordering::Relaxed),
            workers_total: self.base.connections_total.load(Ordering::Relaxed),
            packets_received: self.packets_received.load(Ordering::Relaxed),
            bytes_received: self.base.bytes_received.load(Ordering::Relaxed),
            batches_sent: self.base.batches_sent.load(Ordering::Relaxed),
            errors: self.base.errors.load(Ordering::Relaxed),
            packets_dropped: self.packets_dropped.load(Ordering::Relaxed),
            messages_malformed: self.messages_malformed.load(Ordering::Relaxed),
            worker_errors: self.worker_errors.load(Ordering::Relaxed),
        }
    }
}

/// Extended metrics snapshot for Syslog UDP source
#[derive(Debug, Clone, Copy)]
pub struct SyslogUdpMetricsSnapshot {
    pub workers_active: u64,
    pub workers_total: u64,
    pub packets_received: u64,
    pub bytes_received: u64,
    pub batches_sent: u64,
    pub errors: u64,
    pub packets_dropped: u64,
    pub messages_malformed: u64,
    pub worker_errors: u64,
}

/// Handle for accessing Syslog UDP source metrics
///
/// This can be obtained from the source and used for metrics reporting.
/// It remains valid even during source operation.
///
/// Note: For UDP sources, `connections_active` represents active workers,
/// and `connections_total` represents total workers started.
#[derive(Clone)]
pub struct SyslogUdpMetricsHandle {
    id: String,
    metrics: Arc<SyslogUdpSourceMetrics>,
}

impl SyslogUdpMetricsHandle {
    /// Get an extended snapshot including syslog UDP-specific metrics
    pub fn extended_snapshot(&self) -> SyslogUdpMetricsSnapshot {
        self.metrics.snapshot()
    }
}

impl SourceMetricsProvider for SyslogUdpMetricsHandle {
    fn source_id(&self) -> &str {
        &self.id
    }

    fn source_type(&self) -> &str {
        "syslog_udp"
    }

    fn snapshot(&self) -> SourceMetricsSnapshot {
        // Note: For UDP, we map workers to connections for the generic snapshot
        let s = self.metrics.snapshot();
        SourceMetricsSnapshot {
            connections_active: s.workers_active,
            connections_total: s.workers_total,
            messages_received: s.packets_received,
            bytes_received: s.bytes_received,
            batches_sent: s.batches_sent,
            errors: s.errors,
        }
    }
}

// =============================================================================
// Errors
// =============================================================================

/// Syslog UDP source errors
#[derive(Debug, thiserror::Error)]
pub enum SyslogUdpSourceError {
    /// Failed to bind to address
    #[error("failed to bind to {address}: {source}")]
    Bind {
        address: String,
        #[source]
        source: std::io::Error,
    },

    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Channel closed
    #[error("batch channel closed")]
    ChannelClosed,

    /// Packet too large
    #[error("packet size {size} exceeds limit {limit}")]
    PacketTooLarge { size: usize, limit: usize },

    /// Failed to create worker
    #[error("failed to create worker {worker_id}: {source}")]
    WorkerCreation {
        worker_id: usize,
        #[source]
        source: std::io::Error,
    },
}

// =============================================================================
// Source Implementation
// =============================================================================

/// High-performance Syslog UDP source
///
/// Receives UDP datagrams and batches syslog messages for efficient
/// pipeline processing. Uses multiple workers for parallel processing.
pub struct SyslogUdpSource {
    /// Configuration
    config: SyslogUdpSourceConfig,

    /// Sharded sender to distribute batches across router workers
    batch_sender: ShardedSender,

    /// Metrics
    metrics: Arc<SyslogUdpSourceMetrics>,

    /// Running flag
    running: Arc<AtomicBool>,
}

impl SyslogUdpSource {
    /// Create a new Syslog UDP source
    pub fn new(config: SyslogUdpSourceConfig, batch_sender: ShardedSender) -> Self {
        Self {
            config,
            batch_sender,
            metrics: Arc::new(SyslogUdpSourceMetrics::new()),
            running: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Get metrics reference
    pub fn metrics(&self) -> &Arc<SyslogUdpSourceMetrics> {
        &self.metrics
    }

    /// Get a metrics handle for reporting
    ///
    /// The handle implements `SourceMetricsProvider` and can be registered
    /// with the metrics reporter.
    pub fn metrics_handle(&self) -> SyslogUdpMetricsHandle {
        SyslogUdpMetricsHandle {
            id: self.config.id.clone(),
            metrics: Arc::clone(&self.metrics),
        }
    }

    /// Check if source is running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    /// Stop the source
    pub fn stop(&self) {
        self.running.store(false, Ordering::Relaxed);
    }

    /// Run the source (main entry point)
    pub async fn run(&self, cancel: CancellationToken) -> Result<(), SyslogUdpSourceError> {
        let bind_addr = self.config.bind_address();
        let socket_addr: SocketAddr = bind_addr
            .parse()
            .map_err(|_| SyslogUdpSourceError::Bind {
                address: bind_addr.clone(),
                source: std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "invalid socket address",
                ),
            })?;

        self.running.store(true, Ordering::Relaxed);

        tracing::info!(
            source_id = %self.config.id,
            address = %bind_addr,
            workspace_id = %self.config.workspace_id,
            num_workers = %self.config.num_workers,
            max_message_size = %self.config.max_message_size,
            "syslog UDP source starting"
        );

        // Create workers with separate sockets using SO_REUSEPORT
        let mut worker_handles = Vec::with_capacity(self.config.num_workers);

        for worker_id in 0..self.config.num_workers {
            // Create socket with SO_REUSEPORT for kernel-level load balancing
            let socket = self
                .create_reuseport_socket(socket_addr)
                .map_err(|e| SyslogUdpSourceError::WorkerCreation {
                    worker_id,
                    source: e,
                })?;

            // Each worker gets a stable connection_id for sharding
            let connection_id = self.batch_sender.allocate_connection_id();

            let worker = UdpWorker {
                id: worker_id,
                socket,
                config: self.config.clone(),
                batch_sender: self.batch_sender.clone(),
                metrics: Arc::clone(&self.metrics),
                running: Arc::clone(&self.running),
                connection_id,
                cancel: cancel.clone(),
            };

            self.metrics.worker_started();

            let handle = tokio::spawn(async move {
                worker.run().await;
            });

            worker_handles.push(handle);
        }

        tracing::info!(
            source_id = %self.config.id,
            workers_started = %self.config.num_workers,
            "syslog UDP source listening"
        );

        // Wait for all workers to complete
        for handle in worker_handles {
            let _ = handle.await;
        }

        tracing::info!(
            source_id = %self.config.id,
            "syslog UDP source stopped"
        );

        Ok(())
    }

    /// Create a UDP socket with SO_REUSEPORT and optimized buffer sizes
    ///
    /// This enables kernel-level load balancing across multiple workers,
    /// matching Go's high-performance UDP implementation.
    fn create_reuseport_socket(&self, addr: SocketAddr) -> std::io::Result<UdpSocket> {
        let domain = if addr.is_ipv4() {
            Domain::IPV4
        } else {
            Domain::IPV6
        };

        let socket = Socket::new(domain, Type::DGRAM, Some(Protocol::UDP))?;

        // Enable SO_REUSEADDR
        socket.set_reuse_address(true)?;

        // Enable SO_REUSEPORT for kernel-level load balancing across workers
        #[cfg(unix)]
        socket.set_reuse_port(true)?;

        // Set larger receive buffer for UDP bursts (4x like Go)
        let recv_buffer_size = self.config.buffer_size * UDP_BUFFER_MULTIPLIER;
        if let Err(e) = socket.set_recv_buffer_size(recv_buffer_size) {
            tracing::warn!(
                error = %e,
                requested_size = recv_buffer_size,
                "Failed to set UDP SO_RCVBUF"
            );
        }

        // Bind the socket
        socket.bind(&addr.into())?;

        // Set non-blocking for tokio
        socket.set_nonblocking(true)?;

        // Convert to tokio UdpSocket
        let std_socket: std::net::UdpSocket = socket.into();
        UdpSocket::from_std(std_socket)
    }
}

// =============================================================================
// UDP Worker
// =============================================================================

/// Individual UDP worker that processes datagrams
///
/// Each worker owns its own socket with SO_REUSEPORT, enabling
/// kernel-level load balancing across workers.
struct UdpWorker {
    id: usize,
    /// Owned socket - each worker has its own with SO_REUSEPORT
    socket: UdpSocket,
    config: SyslogUdpSourceConfig,
    batch_sender: ShardedSender,
    metrics: Arc<SyslogUdpSourceMetrics>,
    running: Arc<AtomicBool>,
    /// Connection ID for sharding (uses worker_id as stable assignment)
    connection_id: u64,
    /// Cancellation token for graceful shutdown
    cancel: CancellationToken,
}

impl UdpWorker {
    /// Run the worker
    async fn run(self) {
        tracing::debug!(
            worker_id = %self.id,
            "syslog UDP worker started"
        );

        let source_id = self.config.source_id();

        // Create batch builder
        let mut batch_builder =
            BatchBuilder::with_capacity(BatchType::Syslog, source_id, self.config.buffer_size);
        batch_builder.set_workspace_id(self.config.workspace_id);
        batch_builder.set_max_items(self.config.batch_size);

        // Receive buffer
        let mut recv_buf = vec![0u8; self.config.max_message_size];

        // Flush timer
        let mut flush_interval = interval(self.config.flush_interval);
        flush_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            if !self.running.load(Ordering::Relaxed) {
                break;
            }

            tokio::select! {
                biased;

                // Check for cancellation
                _ = self.cancel.cancelled() => {
                    self.running.store(false, Ordering::Relaxed);
                    break;
                }

                // Flush timer
                _ = flush_interval.tick() => {
                    self.flush_batch(&mut batch_builder).await;
                    batch_builder.set_workspace_id(self.config.workspace_id);
                }

                // Receive datagram
                recv_result = self.socket.recv_from(&mut recv_buf) => {
                    match recv_result {
                        Ok((len, peer_addr)) => {
                            self.process_packet(
                                &recv_buf[..len],
                                peer_addr,
                                &mut batch_builder,
                            ).await;
                        }
                        Err(e) => {
                            if self.running.load(Ordering::Relaxed) {
                                self.metrics.worker_error();
                                tracing::debug!(
                                    worker_id = %self.id,
                                    error = %e,
                                    "syslog UDP recv error"
                                );
                            }
                        }
                    }
                }
            }
        }

        // Flush remaining messages
        self.flush_batch(&mut batch_builder).await;

        self.metrics.worker_stopped();

        tracing::debug!(
            worker_id = %self.id,
            "syslog UDP worker stopped"
        );
    }

    /// Process a received UDP packet
    async fn process_packet(
        &self,
        data: &[u8],
        peer_addr: SocketAddr,
        batch_builder: &mut BatchBuilder,
    ) {
        // Check packet size
        if data.len() > self.config.max_message_size {
            self.metrics.message_malformed();
            tracing::debug!(
                worker_id = %self.id,
                peer = %peer_addr,
                size = data.len(),
                max = self.config.max_message_size,
                "syslog UDP packet too large, dropping"
            );
            return;
        }

        // Trim trailing newline if present (some syslog clients add it)
        let message = trim_trailing_newline(data);

        if message.is_empty() {
            return;
        }

        // Record metrics
        self.metrics.packet_received(data.len() as u64);
        self.metrics
            .base
            .messages_received
            .fetch_add(1, Ordering::Relaxed);

        // Set source IP from this packet
        batch_builder.set_source_ip(peer_addr.ip());

        // Add to batch
        let batch_full = batch_builder.add(message, 1);

        if batch_full {
            self.flush_batch(batch_builder).await;
            batch_builder.set_workspace_id(self.config.workspace_id);
        }
    }

    /// Flush the current batch if not empty
    async fn flush_batch(&self, batch_builder: &mut BatchBuilder) {
        if batch_builder.count() == 0 {
            return;
        }

        // Swap with a new builder to take ownership
        let source_id = self.config.source_id();
        let old_builder = std::mem::replace(
            batch_builder,
            BatchBuilder::new(BatchType::Syslog, source_id),
        );

        // Finish the old builder to get the batch
        let batch = old_builder.finish();

        // Try non-blocking send first (using worker's connection_id for sharding)
        match self.batch_sender.try_send(batch, self.connection_id) {
            Ok(()) => {
                self.metrics.base.batch_sent();
            }
            Err(TrySendError::Full(_)) => {
                // Queue full - drop the batch (UDP semantics - best effort)
                self.metrics.packet_dropped();
                tracing::debug!(
                    worker_id = %self.id,
                    "syslog UDP batch dropped (queue full)"
                );
            }
            Err(TrySendError::Disconnected(_)) => {
                // Channel closed - source is shutting down
                tracing::debug!(
                    worker_id = %self.id,
                    "syslog UDP channel closed"
                );
            }
        }

        // Builder was already replaced above, no need to reset
    }
}

/// Trim trailing newline from message (LF or CRLF)
#[inline]
pub fn trim_trailing_newline(data: &[u8]) -> &[u8] {
    let mut end = data.len();

    if end > 0 && data[end - 1] == b'\n' {
        end -= 1;
        if end > 0 && data[end - 1] == b'\r' {
            end -= 1;
        }
    }

    &data[..end]
}

#[cfg(test)]
#[path = "udp_test.rs"]
mod udp_test;
