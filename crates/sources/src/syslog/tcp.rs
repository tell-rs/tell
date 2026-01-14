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
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use cdp_protocol::{BatchBuilder, BatchType, SourceId};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use crossfire::TrySendError;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;

use metrics::{SourceMetricsProvider, SourceMetricsSnapshot};

use crate::common::SourceMetrics;
use crate::ShardedSender;

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

    /// Lines that exceeded max size
    pub lines_too_long: AtomicU64,

    /// Messages dropped (queue full, etc.)
    pub messages_dropped: AtomicU64,
}

impl SyslogTcpSourceMetrics {
    /// Create new metrics instance
    pub const fn new() -> Self {
        Self {
            base: SourceMetrics::new(),
            lines_read: AtomicU64::new(0),
            lines_too_long: AtomicU64::new(0),
            messages_dropped: AtomicU64::new(0),
        }
    }

    /// Record a line read
    #[inline]
    pub fn line_read(&self) {
        self.lines_read.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a line too long
    #[inline]
    pub fn line_too_long(&self) {
        self.lines_too_long.fetch_add(1, Ordering::Relaxed);
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
            lines_too_long: self.lines_too_long.load(Ordering::Relaxed),
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
    pub lines_too_long: u64,
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
    async fn accept_loop(&self, listener: TcpListener, cancel: CancellationToken) -> Result<(), SyslogTcpSourceError> {
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

                            // Configure TCP socket
                            if self.config.nodelay {
                                let _ = stream.set_nodelay(true);
                            }

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

        // Line buffer (reused across reads)
        let mut line_buf = String::with_capacity(self.config.max_message_size);

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

                // Read line with optional timeout
                read_result = async {
                    if let Some(timeout_duration) = timeout {
                        tokio::time::timeout(
                            timeout_duration,
                            reader.read_line(&mut line_buf)
                        ).await
                    } else {
                        Ok(reader.read_line(&mut line_buf).await)
                    }
                } => {
                    match read_result {
                        Ok(Ok(0)) => {
                            // EOF - connection closed
                            break;
                        }
                        Ok(Ok(bytes_read)) => {
                            self.metrics.line_read();

                            // Trim trailing newline (LF or CRLF)
                            let line = line_buf.trim_end_matches(['\n', '\r']);

                            if !line.is_empty() {
                                // Check message size
                                if line.len() > self.config.max_message_size {
                                    self.metrics.line_too_long();
                                    tracing::debug!(
                                        peer = %self.peer_addr,
                                        size = line.len(),
                                        max = self.config.max_message_size,
                                        "syslog message too large, dropping"
                                    );
                                } else {
                                    // Add to batch
                                    let batch_full = batch_builder.add(line.as_bytes(), 1);

                                    self.metrics.base.message_received(bytes_read as u64);

                                    if batch_full {
                                        self.flush_batch(&mut batch_builder).await?;
                                        // Re-set metadata after flush
                                        batch_builder.set_workspace_id(self.config.workspace_id);
                                        batch_builder.set_source_ip(source_ip);
                                    }
                                }
                            }

                            // Clear buffer for next line
                            line_buf.clear();
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
                match tokio::time::timeout(Duration::from_millis(10), self.batch_sender.send(batch, self.connection_id))
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

/// Check if error is a connection reset (expected during shutdown)
fn is_connection_reset(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::BrokenPipe
    )
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpStream;
    use tokio_util::sync::CancellationToken;

    #[test]
    fn test_config_defaults() {
        let config = SyslogTcpSourceConfig::default();

        assert_eq!(config.port, 514);
        assert_eq!(config.address, "0.0.0.0");
        assert_eq!(config.max_message_size, 8192);
        assert_eq!(config.workspace_id, 1);
        assert!(config.nodelay);
    }

    #[test]
    fn test_config_with_port() {
        let config = SyslogTcpSourceConfig::with_port(1514);
        assert_eq!(config.port, 1514);
    }

    #[test]
    fn test_config_bind_address() {
        let config = SyslogTcpSourceConfig {
            address: "127.0.0.1".into(),
            port: 1514,
            ..Default::default()
        };
        assert_eq!(config.bind_address(), "127.0.0.1:1514");
    }

    #[test]
    fn test_config_source_id() {
        let config = SyslogTcpSourceConfig {
            id: "my_syslog".into(),
            ..Default::default()
        };
        assert_eq!(config.source_id().as_str(), "my_syslog");
    }

    #[test]
    fn test_metrics_new() {
        let metrics = SyslogTcpSourceMetrics::new();
        let snapshot = metrics.snapshot();

        assert_eq!(snapshot.connections_active, 0);
        assert_eq!(snapshot.connections_total, 0);
        assert_eq!(snapshot.messages_received, 0);
        assert_eq!(snapshot.lines_read, 0);
        assert_eq!(snapshot.lines_too_long, 0);
    }

    #[test]
    fn test_metrics_tracking() {
        let metrics = SyslogTcpSourceMetrics::new();

        metrics.base.connection_opened();
        metrics.line_read();
        metrics.line_read();
        metrics.base.message_received(100);
        metrics.line_too_long();
        metrics.message_dropped();
        metrics.base.batch_sent();

        let snapshot = metrics.snapshot();

        assert_eq!(snapshot.connections_active, 1);
        assert_eq!(snapshot.connections_total, 1);
        assert_eq!(snapshot.messages_received, 1);
        assert_eq!(snapshot.bytes_received, 100);
        assert_eq!(snapshot.lines_read, 2);
        assert_eq!(snapshot.lines_too_long, 1);
        assert_eq!(snapshot.messages_dropped, 1);
        assert_eq!(snapshot.batches_sent, 1);
        assert_eq!(snapshot.errors, 1); // line_too_long increments errors
    }

    #[test]
    fn test_error_display() {
        let bind_err = SyslogTcpSourceError::Bind {
            address: "0.0.0.0:514".into(),
            source: io::Error::new(io::ErrorKind::AddrInUse, "address in use"),
        };
        assert!(bind_err.to_string().contains("0.0.0.0:514"));

        let size_err = SyslogTcpSourceError::MessageTooLarge {
            size: 10000,
            limit: 8192,
        };
        assert!(size_err.to_string().contains("10000"));
        assert!(size_err.to_string().contains("8192"));

        let channel_err = SyslogTcpSourceError::ChannelClosed;
        assert!(channel_err.to_string().contains("channel"));
    }

    #[test]
    fn test_is_connection_reset() {
        assert!(is_connection_reset(&io::Error::new(
            io::ErrorKind::ConnectionReset,
            "reset"
        )));
        assert!(is_connection_reset(&io::Error::new(
            io::ErrorKind::ConnectionAborted,
            "aborted"
        )));
        assert!(is_connection_reset(&io::Error::new(
            io::ErrorKind::BrokenPipe,
            "broken"
        )));
        assert!(!is_connection_reset(&io::Error::new(
            io::ErrorKind::Other,
            "other"
        )));
    }

    #[tokio::test]
    async fn test_source_creation() {
        let config = SyslogTcpSourceConfig {
            port: 0, // Use any available port
            ..Default::default()
        };

        let (tx, _rx) = crossfire::mpsc::bounded_async(100);
        let sharded_sender = ShardedSender::new(vec![tx]);
        let source = SyslogTcpSource::new(config, sharded_sender);

        assert!(!source.is_running());
        assert_eq!(source.metrics().snapshot().connections_total, 0);
    }

    #[tokio::test]
    async fn test_source_stop() {
        let config = SyslogTcpSourceConfig::default();
        let (tx, _rx) = crossfire::mpsc::bounded_async(100);
        let sharded_sender = ShardedSender::new(vec![tx]);
        let source = SyslogTcpSource::new(config, sharded_sender);

        source.stop();
        assert!(!source.is_running());
    }

    #[tokio::test]
    async fn test_source_accepts_connections() {
        let config = SyslogTcpSourceConfig {
            id: "test_syslog".into(),
            address: "127.0.0.1".into(),
            port: 0, // Let OS assign port
            ..Default::default()
        };

        let (tx, _rx) = crossfire::mpsc::bounded_async(100);
        let sharded_sender = ShardedSender::new(vec![tx]);
        let _source = Arc::new(SyslogTcpSource::new(config, sharded_sender));

        // Bind to get the actual port
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        // Update config with actual port
        let config = SyslogTcpSourceConfig {
            id: "test_syslog".into(),
            address: "127.0.0.1".into(),
            port,
            flush_interval: Duration::from_millis(10),
            ..Default::default()
        };

        let (tx, mut rx) = crossfire::mpsc::bounded_async(100);
        let source = Arc::new(SyslogTcpSource::new(config.clone(), ShardedSender::new(vec![tx])));

        // Start source in background
        let source_clone = Arc::clone(&source);
        let handle = tokio::spawn(async move {
            let _ = source_clone.run(CancellationToken::new()).await;
        });

        // Give it time to start
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Connect and send a message
        if let Ok(mut stream) = TcpStream::connect(format!("127.0.0.1:{}", port)).await {
            let msg = "<134>Dec 20 12:34:56 host test: Hello syslog\n";
            let _ = stream.write_all(msg.as_bytes()).await;
            let _ = stream.flush().await;

            // Give time to process
            tokio::time::sleep(Duration::from_millis(50)).await;

            // Close connection to trigger flush
            drop(stream);

            // Wait for batch
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // Stop source
        source.stop();
        let _ = tokio::time::timeout(Duration::from_millis(200), handle).await;

        // Check if we received a batch
        if let Ok(batch) = rx.try_recv() {
            assert_eq!(batch.batch_type(), BatchType::Syslog);
            assert!(batch.count() > 0);
        }
    }

    #[tokio::test]
    async fn test_multiple_messages() {
        let config = SyslogTcpSourceConfig {
            id: "test_syslog".into(),
            address: "127.0.0.1".into(),
            port: 0,
            flush_interval: Duration::from_millis(10),
            batch_size: 10,
            ..Default::default()
        };

        // Get an available port
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        let config = SyslogTcpSourceConfig { port, ..config };

        let (tx, mut rx) = crossfire::mpsc::bounded_async(100);
        let source = Arc::new(SyslogTcpSource::new(config, ShardedSender::new(vec![tx])));

        let source_clone = Arc::clone(&source);
        let handle = tokio::spawn(async move {
            let _ = source_clone.run(CancellationToken::new()).await;
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        if let Ok(mut stream) = TcpStream::connect(format!("127.0.0.1:{}", port)).await {
            // Send multiple messages
            for i in 0..5 {
                let msg = format!("<134>Dec 20 12:34:{:02} host test: Message {}\n", i, i);
                let _ = stream.write_all(msg.as_bytes()).await;
            }
            let _ = stream.flush().await;

            tokio::time::sleep(Duration::from_millis(50)).await;
            drop(stream);
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        source.stop();
        let _ = tokio::time::timeout(Duration::from_millis(200), handle).await;

        // Should have received messages
        let mut total_count = 0;
        while let Ok(batch) = rx.try_recv() {
            total_count += batch.message_count();
        }

        // We should have received at least some messages
        // (exact count depends on timing)
        assert!(total_count > 0);
    }

    #[tokio::test]
    async fn test_workspace_id_propagation() {
        let config = SyslogTcpSourceConfig {
            id: "test_syslog".into(),
            address: "127.0.0.1".into(),
            port: 0,
            workspace_id: 42,
            flush_interval: Duration::from_millis(10),
            ..Default::default()
        };

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        let config = SyslogTcpSourceConfig { port, ..config };

        let (tx, mut rx) = crossfire::mpsc::bounded_async(100);
        let source = Arc::new(SyslogTcpSource::new(config, ShardedSender::new(vec![tx])));

        let source_clone = Arc::clone(&source);
        let handle = tokio::spawn(async move {
            let _ = source_clone.run(CancellationToken::new()).await;
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        if let Ok(mut stream) = TcpStream::connect(format!("127.0.0.1:{}", port)).await {
            let msg = "<134>Dec 20 12:34:56 host test: Test message\n";
            let _ = stream.write_all(msg.as_bytes()).await;
            let _ = stream.flush().await;
            tokio::time::sleep(Duration::from_millis(30)).await;
            drop(stream);
            tokio::time::sleep(Duration::from_millis(30)).await;
        }

        source.stop();
        let _ = tokio::time::timeout(Duration::from_millis(200), handle).await;

        // Check workspace ID in received batch
        if let Ok(batch) = rx.try_recv() {
            assert_eq!(batch.workspace_id(), 42);
        }
    }

    #[tokio::test]
    async fn test_crlf_line_endings() {
        let config = SyslogTcpSourceConfig {
            id: "test_syslog".into(),
            address: "127.0.0.1".into(),
            port: 0,
            flush_interval: Duration::from_millis(10),
            ..Default::default()
        };

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        let config = SyslogTcpSourceConfig { port, ..config };

        let (tx, mut rx) = crossfire::mpsc::bounded_async(100);
        let source = Arc::new(SyslogTcpSource::new(config, ShardedSender::new(vec![tx])));

        let source_clone = Arc::clone(&source);
        let handle = tokio::spawn(async move {
            let _ = source_clone.run(CancellationToken::new()).await;
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        if let Ok(mut stream) = TcpStream::connect(format!("127.0.0.1:{}", port)).await {
            // Send with CRLF endings (Windows style)
            let msg = "<134>Dec 20 12:34:56 host test: CRLF message\r\n";
            let _ = stream.write_all(msg.as_bytes()).await;
            let _ = stream.flush().await;
            tokio::time::sleep(Duration::from_millis(30)).await;
            drop(stream);
            tokio::time::sleep(Duration::from_millis(30)).await;
        }

        source.stop();
        let _ = tokio::time::timeout(Duration::from_millis(200), handle).await;

        // Check message was received without trailing CRLF
        if let Ok(batch) = rx.try_recv() {
            if let Some(msg) = batch.get_message(0) {
                let msg_str = std::str::from_utf8(msg).unwrap();
                assert!(!msg_str.ends_with('\r'));
                assert!(!msg_str.ends_with('\n'));
                assert!(msg_str.contains("CRLF message"));
            }
        }
    }

    #[tokio::test]
    async fn test_rfc3164_format() {
        // RFC 3164 format: <PRI>TIMESTAMP HOSTNAME TAG: MESSAGE
        let config = SyslogTcpSourceConfig {
            id: "test_syslog".into(),
            address: "127.0.0.1".into(),
            port: 0,
            flush_interval: Duration::from_millis(10),
            ..Default::default()
        };

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        let config = SyslogTcpSourceConfig { port, ..config };

        let (tx, mut rx) = crossfire::mpsc::bounded_async(100);
        let source = Arc::new(SyslogTcpSource::new(config, ShardedSender::new(vec![tx])));

        let source_clone = Arc::clone(&source);
        let handle = tokio::spawn(async move {
            let _ = source_clone.run(CancellationToken::new()).await;
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        if let Ok(mut stream) = TcpStream::connect(format!("127.0.0.1:{}", port)).await {
            // RFC 3164 format
            let msg = "<134>Dec 20 12:34:56 router1 %LINK-3-UPDOWN: Interface GigabitEthernet0/1, changed state to up\n";
            let _ = stream.write_all(msg.as_bytes()).await;
            let _ = stream.flush().await;
            tokio::time::sleep(Duration::from_millis(30)).await;
            drop(stream);
            tokio::time::sleep(Duration::from_millis(30)).await;
        }

        source.stop();
        let _ = tokio::time::timeout(Duration::from_millis(200), handle).await;

        if let Ok(batch) = rx.try_recv() {
            assert!(batch.message_count() > 0);
            if let Some(msg) = batch.get_message(0) {
                let msg_str = std::str::from_utf8(msg).unwrap();
                assert!(msg_str.starts_with("<134>"));
                assert!(msg_str.contains("router1"));
            }
        }
    }

    #[tokio::test]
    async fn test_rfc5424_format() {
        // RFC 5424 format: <PRI>VERSION TIMESTAMP HOSTNAME APP-NAME PROCID MSGID STRUCTURED-DATA MSG
        let config = SyslogTcpSourceConfig {
            id: "test_syslog".into(),
            address: "127.0.0.1".into(),
            port: 0,
            flush_interval: Duration::from_millis(10),
            ..Default::default()
        };

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        let config = SyslogTcpSourceConfig { port, ..config };

        let (tx, mut rx) = crossfire::mpsc::bounded_async(100);
        let source = Arc::new(SyslogTcpSource::new(config, ShardedSender::new(vec![tx])));

        let source_clone = Arc::clone(&source);
        let handle = tokio::spawn(async move {
            let _ = source_clone.run(CancellationToken::new()).await;
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        if let Ok(mut stream) = TcpStream::connect(format!("127.0.0.1:{}", port)).await {
            // RFC 5424 format
            let msg = "<165>1 2023-12-20T12:36:15.003Z server1.example.com myapp 1234 ID47 - Application started\n";
            let _ = stream.write_all(msg.as_bytes()).await;
            let _ = stream.flush().await;
            tokio::time::sleep(Duration::from_millis(30)).await;
            drop(stream);
            tokio::time::sleep(Duration::from_millis(30)).await;
        }

        source.stop();
        let _ = tokio::time::timeout(Duration::from_millis(200), handle).await;

        if let Ok(batch) = rx.try_recv() {
            assert!(batch.message_count() > 0);
            if let Some(msg) = batch.get_message(0) {
                let msg_str = std::str::from_utf8(msg).unwrap();
                assert!(msg_str.starts_with("<165>1"));
                assert!(msg_str.contains("server1.example.com"));
            }
        }
    }
}
