//! TCP Source - High-performance FlatBuffers protocol source
//!
//! This is the primary data source for Tell, optimized for
//! maximum throughput with zero-copy buffer management.
//!
//! # Protocol
//!
//! Each message is framed with a 4-byte big-endian length prefix:
//! ```text
//! [4 bytes: length (big-endian)][N bytes: FlatBuffer Batch message]
//! ```
//!
//! The FlatBuffer Batch message contains:
//! - API key (16 bytes) for authentication
//! - Schema type (Event, Log, etc.) for routing
//! - Data payload (schema-specific)
//! - Optional source_ip (for forwarded batches)
//!
//! # Design
//!
//! - **Zero-copy reads**: Uses `bytes::BytesMut` for buffer management
//! - **Async I/O**: Built on `tokio::net::TcpListener`
//! - **Batch building**: Accumulates messages before sending to pipeline
//! - **Source identification**: Each TCP source has a `SourceId` for routing
//! - **Per-connection tasks**: Each connection spawns its own handler task
//!
//! # Example
//!
//! ```ignore
//! use tell_sources::tcp::{TcpSource, TcpSourceConfig};
//! use tell_auth::ApiKeyStore;
//! use tokio::sync::mpsc;
//!
//! let config = TcpSourceConfig {
//!     address: "0.0.0.0".into(),
//!     port: 50000,
//!     ..Default::default()
//! };
//!
//! let (batch_tx, batch_rx) = mpsc::channel(1000);
//! let auth_store = ApiKeyStore::new();
//!
//! let source = TcpSource::new(config, auth_store, batch_tx);
//! source.run().await?;
//! ```

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
#[cfg(unix)]
use std::os::fd::{AsRawFd, FromRawFd};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use bytes::{Buf, BytesMut};
use crossfire::TrySendError;
#[cfg(unix)]
use socket2::{Socket, TcpKeepalive};
use tell_auth::ApiKeyStore;
use tell_protocol::{BatchBuilder, BatchType, FlatBatch, ProtocolError, SchemaType, SourceId};
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

use tell_metrics::{SourceMetricsProvider, SourceMetricsSnapshot};

use crate::ShardedSender;
use crate::common::{ConnectionInfo, SourceMetrics};

/// Maximum message size (16MB)
const MAX_MESSAGE_SIZE: u32 = 16 * 1024 * 1024;

/// Length prefix size (4 bytes, big-endian u32)
const LENGTH_PREFIX_SIZE: usize = 4;

/// Default batch size (number of messages before flush)
const DEFAULT_BATCH_SIZE: usize = 500;

/// Default flush interval (100ms)
const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_millis(100);

/// Default buffer size (1MB - larger buffer reduces syscall frequency)
const DEFAULT_BUFFER_SIZE: usize = 1024 * 1024;

/// Default TCP socket buffer size (256KB - matches Go implementation)
const DEFAULT_SOCKET_BUFFER_SIZE: usize = 256 * 1024;

/// TCP source configuration
#[derive(Debug, Clone)]
pub struct TcpSourceConfig {
    /// Source identifier for routing
    pub id: String,

    /// Bind address (e.g., "0.0.0.0")
    pub address: String,

    /// Listen port
    pub port: u16,

    /// Read buffer size per connection
    pub buffer_size: usize,

    /// Maximum messages per batch
    pub batch_size: usize,

    /// Flush interval for partial batches
    pub flush_interval: Duration,

    /// Enable forwarding mode (trust source_ip from batches)
    pub forwarding_mode: bool,

    /// TCP keepalive enabled
    pub keepalive: bool,

    /// TCP nodelay (disable Nagle's algorithm)
    pub nodelay: bool,

    /// TCP socket buffer size for read/write (OS level)
    pub socket_buffer_size: usize,

    /// Connection timeout (0 = no timeout)
    pub connection_timeout: Duration,
}

impl Default for TcpSourceConfig {
    fn default() -> Self {
        Self {
            id: "tcp".into(),
            address: "0.0.0.0".into(),
            port: 50000,
            buffer_size: DEFAULT_BUFFER_SIZE,
            batch_size: DEFAULT_BATCH_SIZE,
            flush_interval: DEFAULT_FLUSH_INTERVAL,
            forwarding_mode: false,
            keepalive: true,
            nodelay: true,
            socket_buffer_size: DEFAULT_SOCKET_BUFFER_SIZE,
            connection_timeout: Duration::ZERO,
        }
    }
}

impl TcpSourceConfig {
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

/// TCP source metrics
#[derive(Debug, Default)]
pub struct TcpSourceMetrics {
    /// Base source metrics
    pub base: SourceMetrics,

    /// Messages with invalid API keys
    pub auth_failures: AtomicU64,

    /// Malformed messages (oversized, invalid format, etc.)
    pub messages_malformed: AtomicU64,
}

impl TcpSourceMetrics {
    /// Create new metrics instance
    pub const fn new() -> Self {
        Self {
            base: SourceMetrics::new(),
            auth_failures: AtomicU64::new(0),
            messages_malformed: AtomicU64::new(0),
        }
    }

    /// Record an authentication failure
    #[inline]
    pub fn auth_failure(&self) {
        self.auth_failures.fetch_add(1, Ordering::Relaxed);
        self.base.errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a malformed message (oversized, invalid format, etc.)
    #[inline]
    pub fn message_malformed(&self) {
        self.messages_malformed.fetch_add(1, Ordering::Relaxed);
        self.base.errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Get extended metrics snapshot
    pub fn snapshot(&self) -> TcpMetricsSnapshot {
        TcpMetricsSnapshot {
            connections_active: self.base.connections_active.load(Ordering::Relaxed),
            connections_total: self.base.connections_total.load(Ordering::Relaxed),
            messages_received: self.base.messages_received.load(Ordering::Relaxed),
            bytes_received: self.base.bytes_received.load(Ordering::Relaxed),
            batches_sent: self.base.batches_sent.load(Ordering::Relaxed),
            errors: self.base.errors.load(Ordering::Relaxed),
            auth_failures: self.auth_failures.load(Ordering::Relaxed),
            messages_malformed: self.messages_malformed.load(Ordering::Relaxed),
        }
    }
}

/// Extended metrics snapshot for TCP source
#[derive(Debug, Clone, Copy)]
pub struct TcpMetricsSnapshot {
    pub connections_active: u64,
    pub connections_total: u64,
    pub messages_received: u64,
    pub bytes_received: u64,
    pub batches_sent: u64,
    pub errors: u64,
    pub auth_failures: u64,
    pub messages_malformed: u64,
}

/// Handle for accessing TCP source metrics
///
/// This can be obtained before running the source and used for metrics reporting.
/// It holds an Arc to the metrics, so it remains valid even after the source
/// is consumed by `run()`.
#[derive(Clone)]
pub struct TcpSourceMetricsHandle {
    id: String,
    metrics: Arc<TcpSourceMetrics>,
}

impl TcpSourceMetricsHandle {
    /// Get an extended snapshot including TCP-specific metrics
    pub fn extended_snapshot(&self) -> TcpMetricsSnapshot {
        self.metrics.snapshot()
    }
}

impl SourceMetricsProvider for TcpSourceMetricsHandle {
    fn source_id(&self) -> &str {
        &self.id
    }

    fn source_type(&self) -> &str {
        "tcp"
    }

    fn snapshot(&self) -> SourceMetricsSnapshot {
        let s = self.metrics.base.snapshot();
        SourceMetricsSnapshot {
            connections_active: s.connections_active,
            connections_total: s.connections_total,
            messages_received: s.messages_received,
            bytes_received: s.bytes_received,
            batches_sent: s.batches_sent,
            errors: s.errors,
        }
    }
}

/// TCP source errors
#[derive(Debug, thiserror::Error)]
pub enum TcpSourceError {
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

    /// Protocol error
    #[error("protocol error: {0}")]
    Protocol(#[from] ProtocolError),

    /// Channel closed
    #[error("batch channel closed")]
    ChannelClosed,

    /// Message too large
    #[error("message size {size} exceeds limit {limit}")]
    MessageTooLarge { size: u32, limit: u32 },

    /// Authentication failed
    #[error("authentication failed")]
    AuthFailed,
}

/// High-performance TCP source
///
/// Accepts TCP connections and parses FlatBuffer messages, routing them
/// to the appropriate batch types based on schema type.
pub struct TcpSource {
    /// Configuration
    config: TcpSourceConfig,

    /// API key store for authentication
    auth_store: Arc<ApiKeyStore>,

    /// Sharded sender to distribute batches across router workers
    batch_sender: ShardedSender,

    /// Metrics
    metrics: Arc<TcpSourceMetrics>,

    /// Running flag
    running: Arc<AtomicBool>,
}

impl TcpSource {
    /// Create a new TCP source
    pub fn new(
        config: TcpSourceConfig,
        auth_store: Arc<ApiKeyStore>,
        batch_sender: ShardedSender,
    ) -> Self {
        Self {
            config,
            auth_store,
            batch_sender,
            metrics: Arc::new(TcpSourceMetrics::new()),
            running: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Get reference to metrics
    pub fn metrics(&self) -> &TcpSourceMetrics {
        &self.metrics
    }

    /// Get a metrics handle for reporting
    ///
    /// The handle implements `SourceMetricsProvider` and can be registered
    /// with the metrics reporter. It remains valid even after `run()` consumes
    /// the source.
    pub fn metrics_handle(&self) -> TcpSourceMetricsHandle {
        TcpSourceMetricsHandle {
            id: self.config.id.clone(),
            metrics: Arc::clone(&self.metrics),
        }
    }

    /// Get the source ID
    pub fn source_id(&self) -> SourceId {
        self.config.source_id()
    }

    /// Check if the source is running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    /// Run the TCP source
    ///
    /// This binds to the configured address and starts accepting connections.
    /// Each connection is handled in a separate task.
    ///
    /// Returns when the source is stopped, cancelled, or an unrecoverable error occurs.
    pub async fn run(self, cancel: CancellationToken) -> Result<(), TcpSourceError> {
        let bind_addr = self.config.bind_address();

        let listener = TcpListener::bind(&bind_addr)
            .await
            .map_err(|e| TcpSourceError::Bind {
                address: bind_addr.clone(),
                source: e,
            })?;

        self.running.store(true, Ordering::Relaxed);

        tracing::info!(
            source_id = %self.config.id,
            address = %bind_addr,
            forwarding_mode = self.config.forwarding_mode,
            "TCP source listening"
        );

        self.accept_loop(listener, cancel).await
    }

    /// Main accept loop
    async fn accept_loop(
        self,
        listener: TcpListener,
        cancel: CancellationToken,
    ) -> Result<(), TcpSourceError> {
        let source = Arc::new(self);

        loop {
            tokio::select! {
                // Check for cancellation
                _ = cancel.cancelled() => {
                    source.running.store(false, Ordering::Relaxed);
                    break;
                }
                // Accept new connections
                result = listener.accept() => {
                    if !source.running.load(Ordering::Relaxed) {
                        break;
                    }
                    match result {
                        Ok((stream, peer_addr)) => {
                            source.metrics.base.connection_opened();

                            let source = Arc::clone(&source);
                            tokio::spawn(async move {
                                if let Err(e) = source.handle_connection(stream, peer_addr).await {
                                    // Only log non-EOF errors
                                    if !matches!(e, TcpSourceError::Io(ref io_err) if io_err.kind() == std::io::ErrorKind::UnexpectedEof)
                                    {
                                        tracing::debug!(
                                            peer = %peer_addr,
                                            error = %e,
                                            "connection error"
                                        );
                                    }
                                }
                                source.metrics.base.connection_closed();
                            });
                        }
                        Err(e) => {
                            // Transient accept errors - log and continue
                            tracing::warn!(error = %e, "accept error");
                            source.metrics.base.error();
                        }
                    }
                }
            }
        }

        tracing::info!(
            source_id = %source.config.id,
            "TCP source stopped"
        );

        Ok(())
    }

    /// Handle a single connection
    async fn handle_connection(
        &self,
        mut stream: TcpStream,
        peer_addr: SocketAddr,
    ) -> Result<(), TcpSourceError> {
        // Configure socket using socket2 for low-level options
        self.configure_socket(&stream)?;

        // Allocate a connection ID for sharding - all batches from this connection
        // will go to the same router worker to preserve ordering
        let connection_id = self.batch_sender.allocate_connection_id();

        let conn_info = ConnectionInfo::new(peer_addr, self.config.port);
        let source_id = self.config.source_id();

        // Read buffer
        let mut buf = BytesMut::with_capacity(self.config.buffer_size);

        // Batch builders for different schema types
        let mut event_batch = BatchBuilder::new(BatchType::Event, source_id.clone());
        let mut log_batch = BatchBuilder::new(BatchType::Log, source_id.clone());

        event_batch.set_max_items(self.config.batch_size);
        log_batch.set_max_items(self.config.batch_size);

        // Track time for periodic flush instead of using interval in select!
        // This avoids timer polling overhead in the hot path
        let mut last_flush = std::time::Instant::now();
        let flush_interval = self.config.flush_interval;

        loop {
            // Read data from socket
            match stream.read_buf(&mut buf).await {
                Ok(0) => {
                    // EOF - flush and exit
                    self.flush_batches(&mut event_batch, &mut log_batch, connection_id)
                        .await?;
                    return Ok(());
                }
                Ok(_) => {
                    // Process all complete messages in buffer
                    while let Some(msg_len) = self.peek_message_len(&buf)? {
                        // Get slice to message data (after 4-byte length prefix)
                        let msg_start = LENGTH_PREFIX_SIZE;
                        let msg_end = msg_start + msg_len;
                        let msg = &buf[msg_start..msg_end];

                        let result =
                            self.process_message(msg, &conn_info, &mut event_batch, &mut log_batch);

                        // Advance buffer past this message (length prefix + data)
                        buf.advance(msg_end);

                        match result {
                            Ok((event_full, log_full)) => {
                                if event_full {
                                    self.send_batch(&mut event_batch, &source_id, connection_id)
                                        .await?;
                                }
                                if log_full {
                                    self.send_batch(&mut log_batch, &source_id, connection_id)
                                        .await?;
                                }
                            }
                            Err(e) => {
                                // Log and continue on message processing errors
                                tracing::debug!(
                                    peer = %peer_addr,
                                    error = %e,
                                    "message processing error"
                                );
                            }
                        }
                    }

                    // Check for periodic flush after processing messages
                    if last_flush.elapsed() >= flush_interval {
                        self.flush_batches(&mut event_batch, &mut log_batch, connection_id)
                            .await?;
                        last_flush = std::time::Instant::now();
                    }
                }
                Err(e) => {
                    // Flush before returning error
                    let _ = self
                        .flush_batches(&mut event_batch, &mut log_batch, connection_id)
                        .await;
                    return Err(e.into());
                }
            }
        }
    }

    /// Peek at the next message length without consuming the buffer
    ///
    /// Returns:
    /// - Ok(Some(len)) if a complete message is available (len = message data size, not including prefix)
    /// - Ok(None) if more data is needed
    /// - Err if message is invalid
    ///
    /// This is an optimization over split_to - we return just the length and let
    /// the caller process the message in-place, then advance the buffer.
    #[inline]
    fn peek_message_len(&self, buf: &BytesMut) -> Result<Option<usize>, TcpSourceError> {
        // Need at least 4 bytes for length prefix
        if buf.len() < LENGTH_PREFIX_SIZE {
            return Ok(None);
        }

        // Read length (big-endian u32)
        let msg_len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);

        // Validate message size
        if msg_len > MAX_MESSAGE_SIZE {
            self.metrics.message_malformed();
            return Err(TcpSourceError::MessageTooLarge {
                size: msg_len,
                limit: MAX_MESSAGE_SIZE,
            });
        }

        let total_len = LENGTH_PREFIX_SIZE + msg_len as usize;

        // Check if we have the complete message
        if buf.len() < total_len {
            return Ok(None);
        }

        Ok(Some(msg_len as usize))
    }

    /// Process a single message
    ///
    /// Returns (event_batch_full, log_batch_full)
    ///
    /// Note: This is intentionally NOT async - all operations are synchronous.
    /// Keeping it sync avoids async state machine overhead in the hot path.
    #[inline]
    fn process_message(
        &self,
        msg: &[u8],
        conn_info: &ConnectionInfo,
        event_batch: &mut BatchBuilder,
        log_batch: &mut BatchBuilder,
    ) -> Result<(bool, bool), TcpSourceError> {
        // Parse FlatBuffer
        let flat_batch = match FlatBatch::parse(msg) {
            Ok(fb) => fb,
            Err(e) => {
                self.metrics.message_malformed();
                return Err(e.into());
            }
        };

        // Validate API key
        let api_key = match flat_batch.api_key_bytes() {
            Ok(key) => key,
            Err(e) => {
                self.metrics.message_malformed();
                return Err(e.into());
            }
        };

        let workspace_id = match self.auth_store.validate_slice(api_key) {
            Some(ws) => ws,
            None => {
                self.metrics.auth_failure();
                return Err(TcpSourceError::AuthFailed);
            }
        };

        // WorkspaceId is already u32, no conversion needed
        let workspace_id_num: u32 = workspace_id.as_u32();

        // Determine source IP
        let source_ip = self.determine_source_ip(&flat_batch, conn_info)?;

        // Get data payload
        let _data = match flat_batch.data() {
            Ok(d) => d,
            Err(e) => {
                self.metrics.message_malformed();
                return Err(e.into());
            }
        };

        // Count items in payload (simplified - actual count depends on schema)
        // For now, we count each message as 1 item
        let item_count = 1;

        // Record metrics
        self.metrics.base.message_received(msg.len() as u64);

        // Route based on schema type
        let schema_type = flat_batch.schema_type();
        let (event_full, log_full) = match schema_type {
            SchemaType::Event => {
                event_batch.set_workspace_id(workspace_id_num);
                event_batch.set_source_ip(source_ip);
                let full = event_batch.add(msg, item_count);
                (full, false)
            }
            SchemaType::Log => {
                log_batch.set_workspace_id(workspace_id_num);
                log_batch.set_source_ip(source_ip);
                let full = log_batch.add(msg, item_count);
                (false, full)
            }
            _ => {
                // Unsupported schema type - treat as event for now
                tracing::trace!(schema_type = ?schema_type, "unsupported schema type, treating as event");
                event_batch.set_workspace_id(workspace_id_num);
                event_batch.set_source_ip(source_ip);
                let full = event_batch.add(msg, item_count);
                (full, false)
            }
        };

        Ok((event_full, log_full))
    }

    /// Determine the source IP based on forwarding mode
    fn determine_source_ip(
        &self,
        flat_batch: &FlatBatch<'_>,
        conn_info: &ConnectionInfo,
    ) -> Result<IpAddr, TcpSourceError> {
        // If forwarding mode is disabled, always use connection IP
        if !self.config.forwarding_mode {
            return Ok(conn_info.remote_ip);
        }

        // Forwarding mode enabled - trust source_ip field if present
        match flat_batch.source_ip() {
            Ok(Some(ip_bytes)) => {
                // Convert 16-byte IPv6 format to IpAddr
                Ok(bytes_to_ip(ip_bytes))
            }
            Ok(None) => {
                // No source_ip field - use connection IP
                Ok(conn_info.remote_ip)
            }
            Err(e) => {
                // Invalid source_ip - use connection IP
                tracing::debug!(error = %e, "invalid source_ip in forwarded batch");
                Ok(conn_info.remote_ip)
            }
        }
    }

    /// Flush any pending batches
    async fn flush_batches(
        &self,
        event_batch: &mut BatchBuilder,
        log_batch: &mut BatchBuilder,
        connection_id: u64,
    ) -> Result<(), TcpSourceError> {
        let source_id = self.config.source_id();

        if !event_batch.is_empty() {
            self.send_batch(event_batch, &source_id, connection_id)
                .await?;
        }

        if !log_batch.is_empty() {
            self.send_batch(log_batch, &source_id, connection_id)
                .await?;
        }

        Ok(())
    }

    /// Send a batch to the pipeline and reset the builder
    ///
    /// Uses non-blocking sends like Go - drops batch if channel is full.
    /// This trades some reliability for higher throughput under load.
    async fn send_batch(
        &self,
        builder: &mut BatchBuilder,
        source_id: &SourceId,
        connection_id: u64,
    ) -> Result<(), TcpSourceError> {
        if builder.is_empty() {
            return Ok(());
        }

        // We need to swap first, then finish.
        // Create a temporary builder with the same batch type
        // Note: We peek at the batch type from the current batch being built
        let batch_type = BatchType::Event; // Default, will be set correctly below

        // Swap with a temporary builder
        let old_builder =
            std::mem::replace(builder, BatchBuilder::new(batch_type, source_id.clone()));

        // Now finish the old builder to get the batch
        let batch = old_builder.finish();

        // Reset the new builder to have the correct batch type
        builder.reset_with_type(batch.batch_type());

        // Non-blocking send (like Go's select/default pattern)
        // Drop batch if channel is full - trades reliability for throughput
        // Connection ID is used to select the shard, ensuring all batches from
        // the same connection go to the same router worker (preserves ordering)
        match self.batch_sender.try_send(batch, connection_id) {
            Ok(()) => {
                self.metrics.base.batch_sent();
            }
            Err(TrySendError::Full(_batch)) => {
                // Channel full - drop batch (Go behavior)
                // This prevents backpressure from blocking all connections
                self.metrics.base.error();
                tracing::debug!("channel full, dropping batch");
            }
            Err(TrySendError::Disconnected(_)) => {
                return Err(TcpSourceError::ChannelClosed);
            }
        }

        Ok(())
    }

    /// Configure socket with low-level options (buffer sizes, keepalive, nodelay)
    ///
    /// Uses socket2 to set options not directly exposed by tokio::net::TcpStream.
    #[cfg(unix)]
    fn configure_socket(&self, stream: &TcpStream) -> Result<(), TcpSourceError> {
        // Get raw fd and wrap in socket2::Socket for configuration
        // Safety: We're borrowing the fd, not taking ownership
        let fd = stream.as_raw_fd();
        let socket = unsafe { Socket::from_raw_fd(fd) };

        // Set TCP_NODELAY (disable Nagle's algorithm)
        if self.config.nodelay && socket.set_tcp_nodelay(true).is_err() {
            tracing::debug!("failed to set TCP_NODELAY");
        }

        // Set socket buffer sizes
        if self.config.socket_buffer_size > 0 {
            if let Err(e) = socket.set_recv_buffer_size(self.config.socket_buffer_size) {
                tracing::debug!(error = %e, "failed to set SO_RCVBUF");
            }
            if let Err(e) = socket.set_send_buffer_size(self.config.socket_buffer_size) {
                tracing::debug!(error = %e, "failed to set SO_SNDBUF");
            }
        }

        // Set TCP keepalive to detect dead connections
        if self.config.keepalive {
            let keepalive = TcpKeepalive::new()
                .with_time(Duration::from_secs(60)) // Start probes after 60s idle
                .with_interval(Duration::from_secs(10)); // Probe every 10s

            if let Err(e) = socket.set_tcp_keepalive(&keepalive) {
                tracing::debug!(error = %e, "failed to set TCP keepalive");
            }
        }

        // IMPORTANT: Don't let socket2 close the fd - tokio still owns it
        std::mem::forget(socket);

        Ok(())
    }

    /// Configure socket - no-op on Windows (tokio handles defaults)
    #[cfg(not(unix))]
    fn configure_socket(&self, _stream: &TcpStream) -> Result<(), TcpSourceError> {
        // On Windows, we skip the advanced socket configuration.
        // Tokio's defaults are generally sufficient.
        Ok(())
    }

    /// Stop the source
    pub fn stop(&self) {
        self.running.store(false, Ordering::Relaxed);
    }
}

/// Convert 16-byte IPv6 format to IpAddr
pub fn bytes_to_ip(bytes: &[u8; 16]) -> IpAddr {
    // Check if this is an IPv4-mapped IPv6 address (::ffff:x.x.x.x)
    // Format: [0,0,0,0,0,0,0,0,0,0,0xff,0xff,a,b,c,d]
    if bytes[..10] == [0; 10] && bytes[10] == 0xff && bytes[11] == 0xff {
        IpAddr::V4(Ipv4Addr::new(bytes[12], bytes[13], bytes[14], bytes[15]))
    } else {
        // Full IPv6 address
        let segments: [u16; 8] = [
            u16::from_be_bytes([bytes[0], bytes[1]]),
            u16::from_be_bytes([bytes[2], bytes[3]]),
            u16::from_be_bytes([bytes[4], bytes[5]]),
            u16::from_be_bytes([bytes[6], bytes[7]]),
            u16::from_be_bytes([bytes[8], bytes[9]]),
            u16::from_be_bytes([bytes[10], bytes[11]]),
            u16::from_be_bytes([bytes[12], bytes[13]]),
            u16::from_be_bytes([bytes[14], bytes[15]]),
        ];
        IpAddr::V6(std::net::Ipv6Addr::from(segments))
    }
}

/// Convert IpAddr to 16-byte IPv6 format
pub fn ip_to_bytes(ip: IpAddr) -> [u8; 16] {
    match ip {
        IpAddr::V4(v4) => {
            let octets = v4.octets();
            [
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, octets[0], octets[1], octets[2],
                octets[3],
            ]
        }
        IpAddr::V6(v6) => v6.octets(),
    }
}
