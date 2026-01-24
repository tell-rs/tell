//! Syslog TCP Source
//!
//! High-performance syslog receiver over TCP with line-based framing.
//!
//! # Protocol Support
//!
//! - **RFC 3164** (BSD syslog) - Legacy format, still widely used
//! - **RFC 5424** (IETF syslog) - Structured data support
//!
//! Messages are stored raw - parsing happens downstream in sinks/transformers.
//!
//! # Framing
//!
//! TCP syslog uses newline-delimited messages (non-transparent framing).
//! Each message ends with LF or CRLF.
//!
//! # Design
//!
//! - **No API key authentication** - workspace ID from config
//! - **Line-based reads** - efficient buffered reader
//! - **Per-connection batching** - flush on interval or batch full
//! - **Zero-copy where possible** - minimize allocations in hot path
//!
//! # Example
//!
//! ```ignore
//! let config = SyslogTcpSourceConfig {
//!     address: "0.0.0.0".into(),
//!     port: 514,
//!     workspace_id: 1,
//!     ..Default::default()
//! };
//!
//! let (batch_tx, batch_rx) = mpsc::channel(1000);
//! let source = SyslogTcpSource::new(config, batch_tx);
//! source.run().await?;
//! ```

use std::io;
use std::net::SocketAddr;
#[cfg(unix)]
use std::os::fd::{AsRawFd, FromRawFd};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use crossfire::TrySendError;
#[cfg(unix)]
use socket2::{Socket, TcpKeepalive};
use tell_protocol::{BatchBuilder, BatchType, SourceId};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::interval;
use tokio_util::sync::CancellationToken;

use tell_metrics::{SourceMetricsProvider, SourceMetricsSnapshot};

use crate::ShardedSender;
use crate::common::SourceMetrics;

// =============================================================================
// Constants
// =============================================================================

/// Default syslog port (privileged - may need root)
const DEFAULT_PORT: u16 = 514;

/// Default maximum syslog message size (8KB)
const DEFAULT_MAX_MESSAGE_SIZE: usize = 8192;

/// Default buffer size (256KB)
const DEFAULT_BUFFER_SIZE: usize = 256 * 1024;

/// Default batch size (number of messages before flush)
const DEFAULT_BATCH_SIZE: usize = 500;

/// Default flush interval (100ms)
const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_millis(100);

/// Default connection timeout (30s)
const DEFAULT_CONNECTION_TIMEOUT: Duration = Duration::from_secs(30);

/// Default queue size
const DEFAULT_QUEUE_SIZE: usize = 1000;

/// Default socket buffer size (256KB)
const DEFAULT_SOCKET_BUFFER_SIZE: usize = 256 * 1024;

/// Default keepalive interval (30s)
const DEFAULT_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);

// =============================================================================
// Configuration
// =============================================================================

/// Syslog TCP source configuration
#[derive(Debug, Clone)]
pub struct SyslogTcpSourceConfig {
    /// Source identifier for routing
    pub id: String,

    /// Bind address (e.g., "0.0.0.0")
    pub address: String,

    /// Listen port
    pub port: u16,

    /// Read buffer size per connection
    pub buffer_size: usize,

    /// Channel queue size
    pub queue_size: usize,

    /// Maximum messages per batch
    pub batch_size: usize,

    /// Flush interval for partial batches
    pub flush_interval: Duration,

    /// TCP nodelay (disable Nagle's algorithm)
    pub nodelay: bool,

    /// Connection timeout (0 = no timeout)
    pub connection_timeout: Duration,

    /// Workspace ID (syslog has no API keys)
    pub workspace_id: u32,

    /// Maximum syslog message size
    pub max_message_size: usize,

    /// Socket buffer size for SO_RCVBUF/SO_SNDBUF
    pub socket_buffer_size: usize,
}

impl Default for SyslogTcpSourceConfig {
    fn default() -> Self {
        Self {
            id: "syslog_tcp".into(),
            address: "0.0.0.0".into(),
            port: DEFAULT_PORT,
            buffer_size: DEFAULT_BUFFER_SIZE,
            queue_size: DEFAULT_QUEUE_SIZE,
            batch_size: DEFAULT_BATCH_SIZE,
            flush_interval: DEFAULT_FLUSH_INTERVAL,
            nodelay: true,
            connection_timeout: DEFAULT_CONNECTION_TIMEOUT,
            workspace_id: 1,
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
            socket_buffer_size: DEFAULT_SOCKET_BUFFER_SIZE,
        }
    }
}

impl SyslogTcpSourceConfig {
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

/// Syslog TCP source metrics
#[derive(Debug, Default)]
pub struct SyslogTcpSourceMetrics {
    /// Base source metrics
    pub base: SourceMetrics,

    /// Lines read from connections
    pub lines_read: AtomicU64,

    /// Malformed messages (oversized, etc.)
    pub messages_malformed: AtomicU64,

    /// Messages dropped (queue full, etc.)
    pub messages_dropped: AtomicU64,
}

impl SyslogTcpSourceMetrics {
    /// Create new metrics instance
    pub const fn new() -> Self {
        Self {
            base: SourceMetrics::new(),
            lines_read: AtomicU64::new(0),
            messages_malformed: AtomicU64::new(0),
            messages_dropped: AtomicU64::new(0),
        }
    }

    /// Record a line read
    #[inline]
    pub fn line_read(&self) {
        self.lines_read.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a malformed message (oversized, etc.)
    #[inline]
    pub fn message_malformed(&self) {
        self.messages_malformed.fetch_add(1, Ordering::Relaxed);
        self.base.errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a dropped message
    #[inline]
    pub fn message_dropped(&self) {
        self.messages_dropped.fetch_add(1, Ordering::Relaxed);
    }

    /// Get extended metrics snapshot
    pub fn snapshot(&self) -> SyslogTcpMetricsSnapshot {
        SyslogTcpMetricsSnapshot {
            connections_active: self.base.connections_active.load(Ordering::Relaxed),
            connections_total: self.base.connections_total.load(Ordering::Relaxed),
            messages_received: self.base.messages_received.load(Ordering::Relaxed),
            bytes_received: self.base.bytes_received.load(Ordering::Relaxed),
            batches_sent: self.base.batches_sent.load(Ordering::Relaxed),
            errors: self.base.errors.load(Ordering::Relaxed),
            lines_read: self.lines_read.load(Ordering::Relaxed),
            messages_malformed: self.messages_malformed.load(Ordering::Relaxed),
            messages_dropped: self.messages_dropped.load(Ordering::Relaxed),
        }
    }
}

/// Extended metrics snapshot for Syslog TCP source
#[derive(Debug, Clone, Copy)]
pub struct SyslogTcpMetricsSnapshot {
    pub connections_active: u64,
    pub connections_total: u64,
    pub messages_received: u64,
    pub bytes_received: u64,
    pub batches_sent: u64,
    pub errors: u64,
    pub lines_read: u64,
    pub messages_malformed: u64,
    pub messages_dropped: u64,
}

/// Handle for accessing Syslog TCP source metrics
///
/// This can be obtained from the source and used for metrics reporting.
/// It remains valid even during source operation.
#[derive(Clone)]
pub struct SyslogTcpMetricsHandle {
    id: String,
    metrics: Arc<SyslogTcpSourceMetrics>,
}

impl SyslogTcpMetricsHandle {
    /// Get an extended snapshot including syslog-specific metrics
    pub fn extended_snapshot(&self) -> SyslogTcpMetricsSnapshot {
        self.metrics.snapshot()
    }
}

impl SourceMetricsProvider for SyslogTcpMetricsHandle {
    fn source_id(&self) -> &str {
        &self.id
    }

    fn source_type(&self) -> &str {
        "syslog_tcp"
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

// =============================================================================
// Errors
// =============================================================================

/// Syslog TCP source errors
#[derive(Debug, thiserror::Error)]
pub enum SyslogTcpSourceError {
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

    /// Message too large
    #[error("message size {size} exceeds limit {limit}")]
    MessageTooLarge { size: usize, limit: usize },
}

// =============================================================================
// Source Implementation
// =============================================================================

/// High-performance Syslog TCP source
///
/// Accepts TCP connections and reads line-delimited syslog messages,
/// batching them for efficient pipeline processing.
pub struct SyslogTcpSource {
    /// Configuration
    config: SyslogTcpSourceConfig,

    /// Sharded sender to distribute batches across router workers
    batch_sender: ShardedSender,

    /// Metrics
    metrics: Arc<SyslogTcpSourceMetrics>,

    /// Running flag
    running: Arc<AtomicBool>,
}

impl SyslogTcpSource {
    /// Create a new Syslog TCP source
    pub fn new(config: SyslogTcpSourceConfig, batch_sender: ShardedSender) -> Self {
        Self {
            config,
            batch_sender,
            metrics: Arc::new(SyslogTcpSourceMetrics::new()),
            running: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Get metrics reference
    pub fn metrics(&self) -> &Arc<SyslogTcpSourceMetrics> {
        &self.metrics
    }

    /// Get a metrics handle for reporting
    ///
    /// The handle implements `SourceMetricsProvider` and can be registered
    /// with the metrics reporter.
    pub fn metrics_handle(&self) -> SyslogTcpMetricsHandle {
        SyslogTcpMetricsHandle {
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

    /// Configure socket options using socket2 (Unix only)
    ///
    /// Sets nodelay, buffer sizes, and keepalive matching Go's SetupTCPConn.
    #[cfg(unix)]
    fn configure_socket(&self, stream: &TcpStream) {
        let fd = stream.as_raw_fd();

        // SAFETY: We're borrowing the fd temporarily. We use forget() to prevent
        // socket2 from closing the fd when it drops - tokio still owns it.
        let socket = unsafe { Socket::from_raw_fd(fd) };

        // TCP_NODELAY - disable Nagle's algorithm
        if self.config.nodelay
            && let Err(e) = socket.set_tcp_nodelay(true)
        {
            tracing::warn!(error = %e, "Failed to set TCP_NODELAY");
        }

        // Socket buffer sizes (matching Go's SetReadBuffer/SetWriteBuffer)
        if let Err(e) = socket.set_recv_buffer_size(self.config.socket_buffer_size) {
            tracing::warn!(error = %e, "Failed to set SO_RCVBUF");
        }
        if let Err(e) = socket.set_send_buffer_size(self.config.socket_buffer_size) {
            tracing::warn!(error = %e, "Failed to set SO_SNDBUF");
        }

        // Keepalive
        let keepalive = TcpKeepalive::new().with_time(DEFAULT_KEEPALIVE_INTERVAL);
        if let Err(e) = socket.set_tcp_keepalive(&keepalive) {
            tracing::warn!(error = %e, "Failed to set TCP keepalive");
        }

        // Don't close the fd - tokio owns it
        std::mem::forget(socket);
    }

    /// Configure socket - no-op on Windows (tokio handles defaults)
    #[cfg(not(unix))]
    fn configure_socket(&self, _stream: &TcpStream) {
        // On Windows, we skip the advanced socket configuration.
        // Tokio's defaults are generally sufficient.
    }

    /// Run the source (main entry point)
    pub async fn run(&self, cancel: CancellationToken) -> Result<(), SyslogTcpSourceError> {
        let bind_addr = self.config.bind_address();

        let listener =
            TcpListener::bind(&bind_addr)
                .await
                .map_err(|e| SyslogTcpSourceError::Bind {
                    address: bind_addr.clone(),
                    source: e,
                })?;

        self.running.store(true, Ordering::Relaxed);

        tracing::info!(
            source_id = %self.config.id,
            address = %bind_addr,
            workspace_id = %self.config.workspace_id,
            max_message_size = %self.config.max_message_size,
            "syslog TCP source listening"
        );

        self.accept_loop(listener, cancel).await
    }

    /// Accept loop - handles incoming connections
    async fn accept_loop(
        &self,
        listener: TcpListener,
        cancel: CancellationToken,
    ) -> Result<(), SyslogTcpSourceError> {
        loop {
            tokio::select! {
                // Check for cancellation
                _ = cancel.cancelled() => {
                    self.running.store(false, Ordering::Relaxed);
                    break;
                }
                // Accept new connections
                accept_result = listener.accept() => {
                    if !self.running.load(Ordering::Relaxed) {
                        break;
                    }
                    match accept_result {
                        Ok((stream, peer_addr)) => {
                            self.metrics.base.connection_opened();

                            // Configure TCP socket using socket2
                            self.configure_socket(&stream);

                            // Allocate connection ID for sharding
                            let connection_id = self.batch_sender.allocate_connection_id();

                            // Spawn connection handler
                            let handler = ConnectionHandler {
                                config: self.config.clone(),
                                batch_sender: self.batch_sender.clone(),
                                metrics: Arc::clone(&self.metrics),
                                running: Arc::clone(&self.running),
                                peer_addr,
                                connection_id,
                            };

                            tokio::spawn(async move {
                                let peer = handler.peer_addr;
                                if let Err(e) = handler.handle(stream).await {
                                    tracing::debug!(
                                        peer = %peer,
                                        error = %e,
                                        "syslog connection error"
                                    );
                                }
                            });
                        }
                        Err(e) => {
                            if self.running.load(Ordering::Relaxed) {
                                tracing::warn!(error = %e, "syslog TCP accept error");
                                self.metrics.base.error();
                            }
                        }
                    }
                }
            }
        }

        tracing::info!(
            source_id = %self.config.id,
            "syslog TCP source stopped"
        );

        Ok(())
    }
}

// =============================================================================
// Connection Handler
// =============================================================================

/// Handles a single TCP connection
struct ConnectionHandler {
    config: SyslogTcpSourceConfig,
    batch_sender: ShardedSender,
    metrics: Arc<SyslogTcpSourceMetrics>,
    running: Arc<AtomicBool>,
    peer_addr: SocketAddr,
    connection_id: u64,
}

impl ConnectionHandler {
    /// Handle the connection
    async fn handle(self, stream: TcpStream) -> Result<(), SyslogTcpSourceError> {
        let source_ip = self.peer_addr.ip();
        let source_id = self.config.source_id();

        // Create buffered reader for line-based reading
        let mut reader = BufReader::with_capacity(self.config.buffer_size, stream);

        // Create batch builder
        let mut batch_builder =
            BatchBuilder::with_capacity(BatchType::Syslog, source_id, self.config.buffer_size);
        batch_builder.set_workspace_id(self.config.workspace_id);
        batch_builder.set_source_ip(source_ip);
        batch_builder.set_max_items(self.config.batch_size);

        // Line buffer (reused across reads) - bounded to max_message_size
        let mut line_buf = Vec::with_capacity(self.config.max_message_size);

        // Flush timer
        let mut flush_interval = interval(self.config.flush_interval);
        flush_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // Connection timeout tracking
        let timeout = if self.config.connection_timeout.is_zero() {
            None
        } else {
            Some(self.config.connection_timeout)
        };

        loop {
            if !self.running.load(Ordering::Relaxed) {
                break;
            }

            tokio::select! {
                biased;

                // Flush timer
                _ = flush_interval.tick() => {
                    self.flush_batch(&mut batch_builder).await?;
                    // Re-set metadata after flush
                    batch_builder.set_workspace_id(self.config.workspace_id);
                    batch_builder.set_source_ip(source_ip);
                }

                // Read line with optional timeout (bounded to prevent memory exhaustion)
                read_result = async {
                    if let Some(timeout_duration) = timeout {
                        tokio::time::timeout(
                            timeout_duration,
                            read_bounded_line(&mut reader, &mut line_buf, self.config.max_message_size)
                        ).await
                    } else {
                        Ok(read_bounded_line(&mut reader, &mut line_buf, self.config.max_message_size).await)
                    }
                } => {
                    match read_result {
                        Ok(Ok(ReadLineResult::Line(bytes_read))) => {
                            self.metrics.line_read();

                            // Trim trailing newline (LF or CRLF)
                            let mut line_len = line_buf.len();
                            if line_len > 0 && line_buf[line_len - 1] == b'\n' {
                                line_len -= 1;
                            }
                            if line_len > 0 && line_buf[line_len - 1] == b'\r' {
                                line_len -= 1;
                            }
                            let line = &line_buf[..line_len];

                            if !line.is_empty() {
                                // Add to batch
                                let batch_full = batch_builder.add(line, 1);

                                self.metrics.base.message_received(bytes_read as u64);

                                if batch_full {
                                    self.flush_batch(&mut batch_builder).await?;
                                    // Re-set metadata after flush
                                    batch_builder.set_workspace_id(self.config.workspace_id);
                                    batch_builder.set_source_ip(source_ip);
                                }
                            }

                            // Clear buffer for next line
                            line_buf.clear();
                        }
                        Ok(Ok(ReadLineResult::TooLong)) => {
                            // Line exceeded max size - already consumed, just record metric
                            self.metrics.message_malformed();
                            tracing::debug!(
                                peer = %self.peer_addr,
                                max = self.config.max_message_size,
                                "syslog message too large, dropped"
                            );
                            line_buf.clear();
                        }
                        Ok(Ok(ReadLineResult::Eof)) => {
                            // EOF - connection closed
                            break;
                        }
                        Ok(Err(e)) => {
                            // I/O error
                            if !is_connection_reset(&e) {
                                self.metrics.base.error();
                                tracing::debug!(
                                    peer = %self.peer_addr,
                                    error = %e,
                                    "syslog TCP read error"
                                );
                            }
                            break;
                        }
                        Err(_) => {
                            // Timeout
                            tracing::debug!(
                                peer = %self.peer_addr,
                                "syslog TCP connection timeout"
                            );
                            break;
                        }
                    }
                }
            }
        }

        // Flush any remaining messages
        self.flush_batch(&mut batch_builder).await?;

        // Update metrics
        self.metrics.base.connection_closed();

        Ok(())
    }

    /// Flush the current batch if not empty
    async fn flush_batch(
        &self,
        batch_builder: &mut BatchBuilder,
    ) -> Result<(), SyslogTcpSourceError> {
        if batch_builder.count() == 0 {
            return Ok(());
        }

        // Swap with a new builder to take ownership of the current one
        let source_id = self.config.source_id();
        let old_builder = std::mem::replace(
            batch_builder,
            BatchBuilder::new(BatchType::Syslog, source_id),
        );

        // Finish the old builder to get the batch
        let batch = old_builder.finish();

        match self.batch_sender.try_send(batch, self.connection_id) {
            Ok(()) => {
                self.metrics.base.batch_sent();
            }
            Err(TrySendError::Full(batch)) => {
                // Queue full - try blocking send with small timeout
                match tokio::time::timeout(
                    Duration::from_millis(10),
                    self.batch_sender.send(batch, self.connection_id),
                )
                .await
                {
                    Ok(Ok(())) => {
                        self.metrics.base.batch_sent();
                    }
                    Ok(Err(_)) => {
                        return Err(SyslogTcpSourceError::ChannelClosed);
                    }
                    Err(_) => {
                        // Timeout - drop batch
                        self.metrics.message_dropped();
                        tracing::warn!(
                            peer = %self.peer_addr,
                            "syslog batch dropped (queue full)"
                        );
                    }
                }
            }
            Err(TrySendError::Disconnected(_)) => {
                return Err(SyslogTcpSourceError::ChannelClosed);
            }
        }

        // Builder was already replaced above, no need to reset

        Ok(())
    }
}

// =============================================================================
// Bounded Line Reading
// =============================================================================

/// Result of reading a bounded line
enum ReadLineResult {
    /// Successfully read a line (with byte count including newline)
    Line(usize),
    /// Line exceeded max size and was consumed/discarded
    TooLong,
    /// End of stream
    Eof,
}

/// Read a line with bounded memory allocation
///
/// This matches Go's `ReadSlice` behavior:
/// - Reads until newline or max_size bytes
/// - If max_size reached without newline, consumes the rest of the line
/// - Prevents memory exhaustion from malicious long lines
async fn read_bounded_line<R: AsyncBufReadExt + Unpin>(
    reader: &mut R,
    buf: &mut Vec<u8>,
    max_size: usize,
) -> io::Result<ReadLineResult> {
    buf.clear();

    let mut total_bytes = 0;
    let mut found_newline = false;
    let mut exceeded_limit = false;

    loop {
        let available = reader.fill_buf().await?;

        if available.is_empty() {
            // EOF
            if total_bytes == 0 {
                return Ok(ReadLineResult::Eof);
            }
            // Return what we have
            break;
        }

        // Find newline in available data
        let newline_pos = available.iter().position(|&b| b == b'\n');

        let (bytes_to_consume, done) = match newline_pos {
            Some(pos) => (pos + 1, true), // Include newline
            None => (available.len(), false),
        };

        // Check if we would exceed the limit
        let space_remaining = max_size.saturating_sub(buf.len());

        if !exceeded_limit && bytes_to_consume <= space_remaining {
            // Can fit in buffer
            buf.extend_from_slice(&available[..bytes_to_consume]);
        } else if !exceeded_limit {
            // First time exceeding - take what we can
            if space_remaining > 0 {
                buf.extend_from_slice(&available[..space_remaining]);
            }
            exceeded_limit = true;
        }
        // If exceeded_limit, we just consume without storing

        total_bytes += bytes_to_consume;
        reader.consume(bytes_to_consume);

        if done {
            found_newline = true;
            break;
        }
    }

    if exceeded_limit {
        // We exceeded the limit - consume rest of line if we haven't hit newline
        if !found_newline {
            // Consume bytes until we find newline
            loop {
                let available = reader.fill_buf().await?;
                if available.is_empty() {
                    break;
                }
                if let Some(pos) = available.iter().position(|&b| b == b'\n') {
                    reader.consume(pos + 1);
                    break;
                }
                let len = available.len();
                reader.consume(len);
            }
        }
        return Ok(ReadLineResult::TooLong);
    }

    Ok(ReadLineResult::Line(total_bytes))
}

/// Check if error is a connection reset (expected during shutdown)
fn is_connection_reset(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::BrokenPipe
    )
}

#[cfg(test)]
#[path = "tcp_test.rs"]
mod tcp_test;
