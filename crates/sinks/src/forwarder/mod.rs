//! Forwarder Sink - Tell-to-Tell Forwarding
//!
//! Forwards batches to a remote Tell server while preserving original client source IPs.
//!
//! # Design
//!
//! The forwarder receives batches, parses each FlatBuffer message to extract
//! the data payload, then rebuilds the message with:
//! - The target's API key (replacing the original)
//! - The source_ip field set to the original client's IP
//!
//! # Protocol
//!
//! Uses the same length-prefixed FlatBuffer protocol as the TCP source:
//! ```text
//! [4 bytes: length (big-endian)][N bytes: FlatBuffer Batch with source_ip field]
//! ```
//!
//! The receiving Tell server must have `forwarding_mode: true` to honor
//! the source_ip field in incoming batches.
//!
//! # Example
//!
//! ```ignore
//! let config = ForwarderConfig::new(
//!     "tell.example.com:8081",
//!     "0123456789abcdef0123456789abcdef"
//! )?;
//!
//! let (tx, rx) = mpsc::channel(1000);
//! let sink = ForwarderSink::new(config, rx);
//!
//! // Run sink
//! sink.run().await;
//! ```

use std::io::ErrorKind;
use std::net::IpAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tell_client::BatchBuilder;
use tell_protocol::{Batch, FlatBatch};
use tell_metrics::{SinkMetricsConfig, SinkMetricsProvider, SinkMetricsSnapshot};
use socket2::{SockRef, TcpKeepalive};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::{Mutex, mpsc};
use tokio::time::timeout;

/// Configuration for forwarder sink
#[derive(Debug, Clone)]
pub struct ForwarderConfig {
    /// Target Tell server address (host:port)
    pub target: String,

    /// Hex-encoded API key (32 hex chars = 16 bytes)
    pub api_key: String,

    /// Decoded API key bytes
    api_key_bytes: [u8; 16],

    /// Connection timeout
    pub connection_timeout: Duration,

    /// Write timeout per message
    pub write_timeout: Duration,

    /// Number of retry attempts on failure
    pub retry_attempts: usize,

    /// Wait time between retries
    pub retry_interval: Duration,

    /// Wait time before reconnecting
    pub reconnect_interval: Duration,

    /// TCP keep-alive enabled
    pub tcp_keepalive: bool,

    /// TCP keep-alive interval (only used if tcp_keepalive is true)
    pub tcp_keepalive_interval: Duration,
}

impl ForwarderConfig {
    /// Create a new forwarder config
    ///
    /// # Arguments
    ///
    /// * `target` - Target Tell server address (host:port)
    /// * `api_key` - Hex-encoded API key (32 hex chars)
    ///
    /// # Errors
    ///
    /// Returns error if API key is invalid.
    pub fn new(
        target: impl Into<String>,
        api_key: impl Into<String>,
    ) -> Result<Self, ForwarderError> {
        let api_key_str = api_key.into();
        let api_key_bytes = decode_api_key(&api_key_str)?;

        Ok(Self {
            target: target.into(),
            api_key: api_key_str,
            api_key_bytes,
            connection_timeout: Duration::from_secs(10),
            write_timeout: Duration::from_secs(5),
            retry_attempts: 3,
            retry_interval: Duration::from_secs(1),
            reconnect_interval: Duration::from_secs(5),
            tcp_keepalive: true,
            tcp_keepalive_interval: Duration::from_secs(30),
        })
    }

    /// Set connection timeout
    #[must_use]
    pub fn with_connection_timeout(mut self, timeout: Duration) -> Self {
        self.connection_timeout = timeout;
        self
    }

    /// Set write timeout
    #[must_use]
    pub fn with_write_timeout(mut self, timeout: Duration) -> Self {
        self.write_timeout = timeout;
        self
    }

    /// Set retry attempts
    #[must_use]
    pub fn with_retry_attempts(mut self, attempts: usize) -> Self {
        self.retry_attempts = attempts;
        self
    }

    /// Set retry interval
    #[must_use]
    pub fn with_retry_interval(mut self, interval: Duration) -> Self {
        self.retry_interval = interval;
        self
    }

    /// Set reconnect interval
    #[must_use]
    pub fn with_reconnect_interval(mut self, interval: Duration) -> Self {
        self.reconnect_interval = interval;
        self
    }

    /// Enable or disable TCP keep-alive
    #[must_use]
    pub fn with_tcp_keepalive(mut self, enabled: bool) -> Self {
        self.tcp_keepalive = enabled;
        self
    }

    /// Set TCP keep-alive interval
    #[must_use]
    pub fn with_tcp_keepalive_interval(mut self, interval: Duration) -> Self {
        self.tcp_keepalive_interval = interval;
        self
    }
}

/// Metrics for forwarder sink
#[derive(Debug, Default)]
pub struct ForwarderMetrics {
    /// Total batches received
    pub batches_received: AtomicU64,

    /// Total batches sent successfully
    pub batches_sent: AtomicU64,

    /// Total batches that failed to send
    pub batches_failed: AtomicU64,

    /// Total bytes sent
    pub bytes_sent: AtomicU64,

    /// Total messages forwarded
    pub messages_forwarded: AtomicU64,

    /// Number of reconnection attempts
    pub reconnect_count: AtomicU64,
}

impl ForwarderMetrics {
    /// Create new metrics instance
    pub const fn new() -> Self {
        Self {
            batches_received: AtomicU64::new(0),
            batches_sent: AtomicU64::new(0),
            batches_failed: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
            messages_forwarded: AtomicU64::new(0),
            reconnect_count: AtomicU64::new(0),
        }
    }

    /// Record a batch received
    #[inline]
    pub fn record_received(&self) {
        self.batches_received.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a successfully sent batch
    #[inline]
    pub fn record_sent(&self, message_count: u64, byte_count: u64) {
        self.batches_sent.fetch_add(1, Ordering::Relaxed);
        self.messages_forwarded
            .fetch_add(message_count, Ordering::Relaxed);
        self.bytes_sent.fetch_add(byte_count, Ordering::Relaxed);
    }

    /// Record a failed batch
    #[inline]
    pub fn record_failed(&self) {
        self.batches_failed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a reconnection attempt
    #[inline]
    pub fn record_reconnect(&self) {
        self.reconnect_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Get snapshot of metrics
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            batches_received: self.batches_received.load(Ordering::Relaxed),
            batches_sent: self.batches_sent.load(Ordering::Relaxed),
            batches_failed: self.batches_failed.load(Ordering::Relaxed),
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
            messages_forwarded: self.messages_forwarded.load(Ordering::Relaxed),
            reconnect_count: self.reconnect_count.load(Ordering::Relaxed),
        }
    }
}

/// Point-in-time snapshot of forwarder metrics
#[derive(Debug, Clone, Copy, Default)]
pub struct MetricsSnapshot {
    pub batches_received: u64,
    pub batches_sent: u64,
    pub batches_failed: u64,
    pub bytes_sent: u64,
    pub messages_forwarded: u64,
    pub reconnect_count: u64,
}

/// Handle for accessing forwarder sink metrics
///
/// This can be obtained before running the sink and used for metrics reporting.
/// It holds an Arc to the metrics, so it remains valid even after the sink
/// is consumed by `run()`.
#[derive(Clone)]
pub struct ForwarderSinkMetricsHandle {
    id: String,
    metrics: Arc<ForwarderMetrics>,
    config: SinkMetricsConfig,
}

impl SinkMetricsProvider for ForwarderSinkMetricsHandle {
    fn sink_id(&self) -> &str {
        &self.id
    }

    fn sink_type(&self) -> &str {
        "forwarder"
    }

    fn metrics_config(&self) -> SinkMetricsConfig {
        self.config
    }

    fn snapshot(&self) -> SinkMetricsSnapshot {
        let s = self.metrics.snapshot();
        SinkMetricsSnapshot {
            batches_received: s.batches_received,
            batches_written: s.batches_sent,
            messages_written: s.messages_forwarded,
            bytes_written: s.bytes_sent,
            write_errors: s.batches_failed,
            flush_count: s.batches_sent,
        }
    }
}

/// Errors from forwarder sink
#[derive(Debug, thiserror::Error)]
pub enum ForwarderError {
    /// Invalid API key format
    #[error("invalid API key: {0}")]
    InvalidApiKey(String),

    /// Connection failed
    #[error("connection failed to {target}: {source}")]
    ConnectionFailed {
        target: String,
        #[source]
        source: std::io::Error,
    },

    /// Write failed
    #[error("write failed: {0}")]
    WriteFailed(#[from] std::io::Error),

    /// All retry attempts exhausted
    #[error("all {attempts} retry attempts failed: {last_error}")]
    RetriesExhausted { attempts: usize, last_error: String },

    /// Connection not established
    #[error("no connection to target")]
    NoConnection,

    /// Timeout
    #[error("operation timed out")]
    Timeout,

    /// Protocol error (FlatBuffer parsing)
    #[error("protocol error: {0}")]
    Protocol(String),

    /// Builder error
    #[error("failed to build message: {0}")]
    BuildError(String),
}

/// Forwarder sink for Tell-to-Tell forwarding
pub struct ForwarderSink {
    /// Channel receiver for incoming batches
    receiver: mpsc::Receiver<Arc<Batch>>,

    /// Configuration
    config: ForwarderConfig,

    /// TCP connection (protected by mutex for reconnection)
    connection: Mutex<Option<TcpStream>>,

    /// Sink name for logging
    name: String,

    /// Metrics for this sink (Arc for sharing with metrics handle)
    metrics: Arc<ForwarderMetrics>,
}

impl ForwarderSink {
    /// Create a new forwarder sink
    pub fn new(config: ForwarderConfig, receiver: mpsc::Receiver<Arc<Batch>>) -> Self {
        Self::with_name(config, receiver, "forwarder")
    }

    /// Create a new forwarder sink with a custom name
    pub fn with_name(
        config: ForwarderConfig,
        receiver: mpsc::Receiver<Arc<Batch>>,
        name: impl Into<String>,
    ) -> Self {
        Self {
            receiver,
            config,
            connection: Mutex::new(None),
            name: name.into(),
            metrics: Arc::new(ForwarderMetrics::new()),
        }
    }

    /// Get reference to metrics
    pub fn metrics(&self) -> &ForwarderMetrics {
        &self.metrics
    }

    /// Get a metrics handle for reporting
    ///
    /// The handle implements `SinkMetricsProvider` and can be registered
    /// with the metrics reporter. It remains valid even after `run()` consumes
    /// the sink.
    pub fn metrics_handle(&self) -> ForwarderSinkMetricsHandle {
        ForwarderSinkMetricsHandle {
            id: self.name.clone(),
            metrics: Arc::clone(&self.metrics),
            config: SinkMetricsConfig {
                enabled: true,
                interval: Duration::from_secs(10),
            },
        }
    }

    /// Get the sink name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Run the sink, forwarding batches until the channel is closed
    pub async fn run(mut self) -> MetricsSnapshot {
        tracing::info!(
            sink = %self.name,
            target = %self.config.target,
            "forwarder sink starting"
        );

        // Attempt initial connection
        if let Err(e) = self.connect().await {
            tracing::warn!(
                sink = %self.name,
                target = %self.config.target,
                error = %e,
                "initial connection failed, will retry on first batch"
            );
        }

        // Process batches
        while let Some(batch) = self.receiver.recv().await {
            self.metrics.record_received();
            self.process_batch(&batch).await;
        }

        // Shutdown - close connection
        {
            let mut conn = self.connection.lock().await;
            if let Some(stream) = conn.take() {
                let _ = stream.into_std();
            }
        }

        let snapshot = self.metrics.snapshot();
        tracing::info!(
            sink = %self.name,
            batches_received = snapshot.batches_received,
            batches_sent = snapshot.batches_sent,
            batches_failed = snapshot.batches_failed,
            messages_forwarded = snapshot.messages_forwarded,
            bytes_sent = snapshot.bytes_sent,
            reconnects = snapshot.reconnect_count,
            "forwarder sink shutting down"
        );

        snapshot
    }

    /// Connect to the target Tell server
    async fn connect(&self) -> Result<(), ForwarderError> {
        let mut conn = self.connection.lock().await;

        // Close existing connection if any
        if let Some(stream) = conn.take() {
            let _ = stream.into_std();
        }

        // Connect with timeout
        let connect_result = timeout(
            self.config.connection_timeout,
            TcpStream::connect(&self.config.target),
        )
        .await;

        let stream = match connect_result {
            Ok(Ok(stream)) => stream,
            Ok(Err(e)) => {
                return Err(ForwarderError::ConnectionFailed {
                    target: self.config.target.clone(),
                    source: e,
                });
            }
            Err(_) => {
                return Err(ForwarderError::ConnectionFailed {
                    target: self.config.target.clone(),
                    source: std::io::Error::new(ErrorKind::TimedOut, "connection timed out"),
                });
            }
        };

        // Set TCP_NODELAY for lower latency (non-fatal if it fails)
        if let Err(e) = stream.set_nodelay(true) {
            tracing::debug!(
                sink = %self.name,
                error = %e,
                "failed to set TCP_NODELAY, continuing with default buffering"
            );
        }

        // Set TCP keep-alive (non-fatal if it fails)
        if self.config.tcp_keepalive {
            let sock_ref = SockRef::from(&stream);
            let keepalive = TcpKeepalive::new().with_time(self.config.tcp_keepalive_interval);

            // On Linux, also set the interval between probes
            #[cfg(target_os = "linux")]
            let keepalive = keepalive.with_interval(self.config.tcp_keepalive_interval);

            if let Err(e) = sock_ref.set_tcp_keepalive(&keepalive) {
                tracing::debug!(
                    sink = %self.name,
                    error = %e,
                    "failed to set TCP keep-alive, continuing without keep-alive"
                );
            } else {
                tracing::trace!(
                    sink = %self.name,
                    interval_secs = self.config.tcp_keepalive_interval.as_secs(),
                    "TCP keep-alive enabled"
                );
            }
        }

        self.metrics.record_reconnect();
        tracing::debug!(
            sink = %self.name,
            target = %self.config.target,
            "connected to target"
        );

        *conn = Some(stream);
        Ok(())
    }

    /// Process a single batch
    async fn process_batch(&self, batch: &Batch) {
        let mut total_bytes = 0u64;
        let mut messages_sent = 0u64;

        // Forward each message in the batch
        for i in 0..batch.message_count() {
            let Some(msg) = batch.get_message(i) else {
                continue;
            };

            match self.forward_message(msg, batch.source_ip()).await {
                Ok(bytes) => {
                    total_bytes += bytes as u64;
                    messages_sent += 1;
                }
                Err(e) => {
                    tracing::error!(
                        sink = %self.name,
                        error = %e,
                        message_index = i,
                        "failed to forward message"
                    );
                    self.metrics.record_failed();

                    // Try to reconnect and continue with remaining messages
                    if let Err(reconnect_err) = self.connect().await {
                        tracing::error!(
                            sink = %self.name,
                            error = %reconnect_err,
                            "reconnection failed, will retry on next message"
                        );
                        tokio::time::sleep(self.config.reconnect_interval).await;
                    }
                    // Continue processing remaining messages in batch (like Go implementation)
                }
            }
        }

        if messages_sent > 0 {
            self.metrics.record_sent(messages_sent, total_bytes);
        }
    }

    /// Forward a single message to the target, rebuilding with source_ip
    async fn forward_message(
        &self,
        msg: &[u8],
        source_ip: IpAddr,
    ) -> Result<usize, ForwarderError> {
        // Parse the original FlatBuffer to extract fields
        let flat_batch =
            FlatBatch::parse(msg).map_err(|e| ForwarderError::Protocol(e.to_string()))?;

        // Build new FlatBuffer with source_ip and SOC API key
        let new_msg = self.build_forwarded_message(&flat_batch, source_ip)?;

        // Send with retry
        self.send_with_retry(&new_msg).await?;

        Ok(new_msg.len() + 4) // +4 for length prefix
    }

    /// Build a new FlatBuffer message with source_ip and target API key
    fn build_forwarded_message(
        &self,
        original: &FlatBatch<'_>,
        source_ip: IpAddr,
    ) -> Result<Vec<u8>, ForwarderError> {
        // Get original data payload
        let data = original
            .data()
            .map_err(|e| ForwarderError::Protocol(e.to_string()))?;

        // Convert source IP to 16-byte IPv6 format
        let source_ip_bytes = ip_to_bytes(source_ip);

        // Build new batch using tell-client::BatchBuilder
        let built = BatchBuilder::new()
            .api_key(self.config.api_key_bytes)
            .schema_type(original.schema_type())
            .version(original.version())
            .batch_id(original.batch_id())
            .data(data)
            .source_ip(source_ip_bytes)
            .build()
            .map_err(|e| ForwarderError::BuildError(e.to_string()))?;

        Ok(built.as_bytes().to_vec())
    }

    /// Send a message with retry logic
    async fn send_with_retry(&self, msg: &[u8]) -> Result<(), ForwarderError> {
        let mut last_error = String::new();

        for attempt in 0..self.config.retry_attempts {
            if attempt > 0 {
                tokio::time::sleep(self.config.retry_interval).await;
            }

            match self.send_message(msg).await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    last_error = e.to_string();
                    tracing::debug!(
                        sink = %self.name,
                        attempt = attempt + 1,
                        max_attempts = self.config.retry_attempts,
                        error = %e,
                        "send attempt failed"
                    );
                }
            }
        }

        Err(ForwarderError::RetriesExhausted {
            attempts: self.config.retry_attempts,
            last_error,
        })
    }

    /// Send a single message (length-prefixed)
    async fn send_message(&self, msg: &[u8]) -> Result<(), ForwarderError> {
        let mut conn = self.connection.lock().await;
        let stream = conn.as_mut().ok_or(ForwarderError::NoConnection)?;

        // Prepare length prefix (4 bytes, big-endian)
        let len_bytes = (msg.len() as u32).to_be_bytes();

        // Send with timeout
        let write_result = timeout(self.config.write_timeout, async {
            stream.write_all(&len_bytes).await?;
            stream.write_all(msg).await?;
            stream.flush().await?;
            Ok::<(), std::io::Error>(())
        })
        .await;

        match write_result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => {
                // Connection error - invalidate connection
                *conn = None;
                Err(ForwarderError::WriteFailed(e))
            }
            Err(_) => {
                // Timeout - invalidate connection
                *conn = None;
                Err(ForwarderError::Timeout)
            }
        }
    }
}

/// Decode hex-encoded API key to bytes
fn decode_api_key(hex_key: &str) -> Result<[u8; 16], ForwarderError> {
    if hex_key.len() != 32 {
        return Err(ForwarderError::InvalidApiKey(format!(
            "expected 32 hex chars, got {}",
            hex_key.len()
        )));
    }

    let mut bytes = [0u8; 16];
    for (i, chunk) in hex_key.as_bytes().chunks(2).enumerate() {
        let hex_str = std::str::from_utf8(chunk)
            .map_err(|_| ForwarderError::InvalidApiKey("invalid UTF-8".into()))?;
        bytes[i] = u8::from_str_radix(hex_str, 16).map_err(|_| {
            ForwarderError::InvalidApiKey(format!("invalid hex at position {}", i * 2))
        })?;
    }

    Ok(bytes)
}

/// Convert IP address to 16-byte IPv6 format
fn ip_to_bytes(ip: IpAddr) -> [u8; 16] {
    match ip {
        IpAddr::V4(v4) => {
            // IPv4-mapped IPv6: ::ffff:x.x.x.x
            let mut bytes = [0u8; 16];
            bytes[10] = 0xff;
            bytes[11] = 0xff;
            bytes[12..16].copy_from_slice(&v4.octets());
            bytes
        }
        IpAddr::V6(v6) => v6.octets(),
    }
}

#[cfg(test)]
#[path = "forwarder_test.rs"]
mod forwarder_test;
