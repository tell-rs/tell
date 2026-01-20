//! TCP Debug Source - Debug variant with hex dump and detailed tracing
//!
//! This is a separate source type from `TcpSource` to ensure **zero overhead**
//! in production. No runtime `if debug` checks - the debug code simply doesn't
//! exist in the production `TcpSource`.
//!
//! # Design
//!
//! Following Go's pattern of separate `TCPSource` and `TCPDebugSource` types,
//! this ensures the hot path in production TCP source has no debug-related
//! branches or function calls.
//!
//! # Features
//!
//! - Full hex dump of incoming messages
//! - FlatBuffer field-by-field analysis with inner data parsing
//! - Detailed tracing at every step
//! - ASCII representation alongside hex
//!
//! # Usage
//!
//! ```ignore
//! let config = TcpDebugSourceConfig {
//!     port: 50001,
//!     ..Default::default()
//! };
//!
//! let source = TcpDebugSource::new(config, auth_store, batch_tx);
//! source.run().await?;
//! ```
//!
//! # Performance Warning
//!
//! This source is **NOT for production use**. The hex dump and detailed
//! logging add significant overhead. Use only for debugging protocol issues.

use std::net::SocketAddr;
use std::os::fd::{AsRawFd, FromRawFd};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use bytes::{Buf, BytesMut};
use tell_auth::ApiKeyStore;
use tell_protocol::{
    BatchBuilder, BatchType, FlatBatch, SchemaType, SourceId, MAX_REASONABLE_SIZE,
    decode_event_data, decode_log_data,
};
use socket2::{Socket, TcpKeepalive};
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::interval;
use tokio_util::sync::CancellationToken;

use crate::common::{ConnectionInfo, SourceMetrics};
use crate::tcp::{TcpSourceError, bytes_to_ip};
use crate::ShardedSender;

// =============================================================================
// Constants
// =============================================================================

/// Length prefix size (4 bytes, big-endian u32)
const LENGTH_PREFIX_SIZE: usize = 4;

/// Default batch size
const DEFAULT_BATCH_SIZE: usize = 500;

/// Default flush interval
const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_millis(100);

/// Default buffer size (256KB)
const DEFAULT_BUFFER_SIZE: usize = 256 * 1024;

/// Default socket buffer size (256KB)
const DEFAULT_SOCKET_BUFFER_SIZE: usize = 256 * 1024;

/// Default keepalive interval
const DEFAULT_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);

// =============================================================================
// Configuration
// =============================================================================

/// TCP debug source configuration
#[derive(Debug, Clone)]
pub struct TcpDebugSourceConfig {
    /// Source identifier for routing
    pub id: String,

    /// Bind address
    pub address: String,

    /// Listen port
    pub port: u16,

    /// Read buffer size per connection
    pub buffer_size: usize,

    /// Maximum messages per batch
    pub batch_size: usize,

    /// Flush interval for partial batches
    pub flush_interval: Duration,

    /// TCP nodelay
    pub nodelay: bool,

    /// Socket buffer size for SO_RCVBUF/SO_SNDBUF
    pub socket_buffer_size: usize,
}

impl Default for TcpDebugSourceConfig {
    fn default() -> Self {
        Self {
            id: "tcp_debug".into(),
            address: "0.0.0.0".into(),
            port: 50001,
            buffer_size: DEFAULT_BUFFER_SIZE,
            batch_size: DEFAULT_BATCH_SIZE,
            flush_interval: DEFAULT_FLUSH_INTERVAL,
            nodelay: true,
            socket_buffer_size: DEFAULT_SOCKET_BUFFER_SIZE,
        }
    }
}

impl TcpDebugSourceConfig {
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

/// TCP debug source metrics
#[derive(Debug, Default)]
pub struct TcpDebugSourceMetrics {
    /// Base source metrics
    pub base: SourceMetrics,

    /// Messages with invalid API keys
    pub auth_failures: AtomicU64,

    /// Messages with invalid FlatBuffer format
    pub parse_errors: AtomicU64,

    /// Messages that exceeded size limit
    pub oversized_messages: AtomicU64,
}

impl TcpDebugSourceMetrics {
    /// Create new metrics instance
    pub const fn new() -> Self {
        Self {
            base: SourceMetrics::new(),
            auth_failures: AtomicU64::new(0),
            parse_errors: AtomicU64::new(0),
            oversized_messages: AtomicU64::new(0),
        }
    }

    /// Record an authentication failure
    #[inline]
    pub fn auth_failure(&self) {
        self.auth_failures.fetch_add(1, Ordering::Relaxed);
        self.base.errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a parse error
    #[inline]
    pub fn parse_error(&self) {
        self.parse_errors.fetch_add(1, Ordering::Relaxed);
        self.base.errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an oversized message
    #[inline]
    pub fn oversized_message(&self) {
        self.oversized_messages.fetch_add(1, Ordering::Relaxed);
        self.base.errors.fetch_add(1, Ordering::Relaxed);
    }
}

// =============================================================================
// Source Implementation
// =============================================================================

/// TCP Debug Source - with hex dump and detailed tracing
///
/// This is a separate type from `TcpSource` to ensure zero overhead in production.
/// Use this only for debugging protocol and parsing issues.
pub struct TcpDebugSource {
    /// Configuration
    config: TcpDebugSourceConfig,

    /// API key store for authentication
    auth_store: Arc<ApiKeyStore>,

    /// Sharded sender to distribute batches across router workers
    batch_sender: ShardedSender,

    /// Metrics
    metrics: Arc<TcpDebugSourceMetrics>,

    /// Running flag
    running: Arc<AtomicBool>,
}

impl TcpDebugSource {
    /// Create a new TCP debug source
    pub fn new(
        config: TcpDebugSourceConfig,
        auth_store: Arc<ApiKeyStore>,
        batch_sender: ShardedSender,
    ) -> Self {
        Self {
            config,
            auth_store,
            batch_sender,
            metrics: Arc::new(TcpDebugSourceMetrics::new()),
            running: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Get metrics reference
    pub fn metrics(&self) -> &Arc<TcpDebugSourceMetrics> {
        &self.metrics
    }

    /// Check if source is running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    /// Stop the source
    pub fn stop(&self) {
        self.running.store(false, Ordering::Relaxed);
    }

    /// Run the source
    pub async fn run(&self, cancel: CancellationToken) -> Result<(), TcpSourceError> {
        let bind_addr = self.config.bind_address();

        let listener = TcpListener::bind(&bind_addr)
            .await
            .map_err(|e| TcpSourceError::Bind {
                address: bind_addr.clone(),
                source: e,
            })?;

        self.running.store(true, Ordering::Relaxed);

        tracing::warn!(
            source_id = %self.config.id,
            address = %bind_addr,
            "🔬 TCP DEBUG source listening - NOT FOR PRODUCTION USE"
        );

        self.accept_loop(listener, cancel).await
    }

    /// Accept loop
    async fn accept_loop(&self, listener: TcpListener, cancel: CancellationToken) -> Result<(), TcpSourceError> {
        loop {
            tokio::select! {
                biased;

                _ = cancel.cancelled() => {
                    self.running.store(false, Ordering::Relaxed);
                    tracing::info!(
                        source_id = %self.config.id,
                        "🔬 TCP DEBUG source received cancellation signal"
                    );
                    break;
                }

                accept_result = listener.accept() => {
                    if !self.running.load(Ordering::Relaxed) {
                        break;
                    }
                    match accept_result {
                        Ok((stream, peer_addr)) => {
                            self.metrics.base.connection_opened();

                            tracing::info!(
                                peer = %peer_addr,
                                "🔌 [DEBUG] New connection accepted"
                            );

                            let source = TcpDebugSource {
                                config: self.config.clone(),
                                auth_store: Arc::clone(&self.auth_store),
                                batch_sender: self.batch_sender.clone(),
                                metrics: Arc::clone(&self.metrics),
                                running: Arc::clone(&self.running),
                            };

                            tokio::spawn(async move {
                                if let Err(e) = source.handle_connection(stream, peer_addr).await {
                                    tracing::debug!(
                                        peer = %peer_addr,
                                        error = %e,
                                        "🔌 [DEBUG] Connection error"
                                    );
                                }
                                source.metrics.base.connection_closed();
                            });
                        }
                        Err(e) => {
                            if self.running.load(Ordering::Relaxed) {
                                tracing::warn!(error = %e, "❌ [DEBUG] Accept error");
                                self.metrics.base.error();
                            }
                        }
                    }
                }
            }
        }

        tracing::info!(
            source_id = %self.config.id,
            "🔬 TCP DEBUG source stopped"
        );

        Ok(())
    }

    /// Handle a single connection with debug output
    async fn handle_connection(
        &self,
        mut stream: TcpStream,
        peer_addr: SocketAddr,
    ) -> Result<(), TcpSourceError> {
        // Configure socket options
        self.configure_socket(&stream)?;

        // Allocate a connection ID for sharding
        let connection_id = self.batch_sender.allocate_connection_id();

        let conn_info = ConnectionInfo::new(peer_addr, self.config.port);
        let source_id = self.config.source_id();

        let mut buf = BytesMut::with_capacity(self.config.buffer_size);

        let mut event_batch = BatchBuilder::new(BatchType::Event, source_id.clone());
        let mut log_batch = BatchBuilder::new(BatchType::Log, source_id.clone());

        event_batch.set_max_items(self.config.batch_size);
        log_batch.set_max_items(self.config.batch_size);

        let mut flush_timer = interval(self.config.flush_interval);
        flush_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                read_result = stream.read_buf(&mut buf) => {
                    match read_result {
                        Ok(0) => {
                            tracing::info!(peer = %peer_addr, "🔌 [DEBUG] Connection closed (EOF)");
                            self.flush_batches(&mut event_batch, &mut log_batch, connection_id).await?;
                            return Ok(());
                        }
                        Ok(n) => {
                            tracing::debug!(
                                peer = %peer_addr,
                                bytes = n,
                                buffer_len = buf.len(),
                                "📥 [DEBUG] Received data"
                            );

                            while let Some(msg_result) = self.try_read_message(&mut buf)? {
                                // Hex dump the message
                                self.hex_dump(&msg_result);

                                match self.process_message(
                                    &msg_result,
                                    &conn_info,
                                    &mut event_batch,
                                    &mut log_batch,
                                ).await {
                                    Ok((event_full, log_full)) => {
                                        if event_full {
                                            tracing::info!(
                                                count = event_batch.count(),
                                                "🚀 [DEBUG] Sending FULL event batch"
                                            );
                                            self.send_batch(&mut event_batch, &source_id, connection_id).await?;
                                        }
                                        if log_full {
                                            tracing::info!(
                                                count = log_batch.count(),
                                                "🚀 [DEBUG] Sending FULL log batch"
                                            );
                                            self.send_batch(&mut log_batch, &source_id, connection_id).await?;
                                        }
                                    }
                                    Err(e) => {
                                        tracing::warn!(
                                            peer = %peer_addr,
                                            error = %e,
                                            "❌ [DEBUG] Message processing error"
                                        );
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            tracing::debug!(peer = %peer_addr, error = %e, "❌ [DEBUG] Read error");
                            let _ = self.flush_batches(&mut event_batch, &mut log_batch, connection_id).await;
                            return Err(e.into());
                        }
                    }
                }

                _ = flush_timer.tick() => {
                    self.flush_batches(&mut event_batch, &mut log_batch, connection_id).await?;
                }
            }
        }
    }

    /// Hex dump a message with ASCII representation
    fn hex_dump(&self, data: &[u8]) {
        tracing::info!("📊 [DEBUG] BINARY DUMP: {} bytes", data.len());

        for (i, chunk) in data.chunks(16).enumerate() {
            let offset = i * 16;

            // Hex representation
            let hex: String = chunk
                .iter()
                .map(|b| format!("{:02x}", b))
                .collect::<Vec<_>>()
                .join(" ");

            // ASCII representation
            let ascii: String = chunk
                .iter()
                .map(|&b| if (32..=126).contains(&b) { b as char } else { '.' })
                .collect();

            // Pad hex to consistent width
            let padded_hex = format!("{:47}", hex);

            tracing::info!("📊 {:04x}: {} |{}|", offset, padded_hex, ascii);
        }

        tracing::info!("📊 [DEBUG] END DUMP");

        // FlatBuffer analysis
        self.analyze_flatbuffer(data);
    }

    /// Analyze FlatBuffer structure with inner data parsing
    fn analyze_flatbuffer(&self, data: &[u8]) {
        tracing::info!("🔬 [DEBUG] FlatBuffer Analysis:");

        match FlatBatch::parse(data) {
            Ok(flat_batch) => {
                tracing::info!("  ✅ Valid FlatBuffer (outer Batch)");
                tracing::info!(
                    "  - api_key: {} bytes",
                    flat_batch.api_key().map(|k| k.len()).unwrap_or(0)
                );
                tracing::info!("  - batch_id: {}", flat_batch.batch_id());
                tracing::info!("  - schema_type: {}", schema_type_name(flat_batch.schema_type()));
                tracing::info!("  - version: {}", flat_batch.version());

                match flat_batch.source_ip() {
                    Ok(Some(source_ip)) => {
                        tracing::info!("  - source_ip: {} bytes (forwarded)", source_ip.len());
                    }
                    Ok(None) => {
                        tracing::info!("  - source_ip: not present");
                    }
                    Err(e) => {
                        tracing::info!("  - source_ip: <error: {}>", e);
                    }
                }

                // Parse inner data based on schema type
                if let Ok(data_bytes) = flat_batch.data() {
                    tracing::info!("  - data: {} bytes", data_bytes.len());
                    self.analyze_inner_data(flat_batch.schema_type(), data_bytes);
                } else {
                    tracing::info!("  - data: <error reading>");
                }
            }
            Err(e) => {
                tracing::warn!("  ❌ Invalid FlatBuffer: {}", e);
            }
        }
    }

    /// Analyze inner data payload (EventData or LogData)
    fn analyze_inner_data(&self, schema_type: SchemaType, data: &[u8]) {
        match schema_type {
            SchemaType::Event => {
                tracing::info!("🔬 [DEBUG] Parsing inner EventData...");
                match decode_event_data(data) {
                    Ok(events) => {
                        tracing::info!("  ✅ EventData parsed: {} events", events.len());
                        for (i, event) in events.iter().enumerate() {
                            tracing::info!("  📝 [EVENT {}]:", i);
                            tracing::info!("    - event_type: {}", event.event_type);
                            tracing::info!("    - timestamp: {}", event.timestamp);
                            if let Some(device_id) = event.device_id {
                                tracing::info!("    - device_id: {}", hex::encode(device_id));
                            } else {
                                tracing::info!("    - device_id: not present");
                            }
                            if let Some(session_id) = event.session_id {
                                tracing::info!("    - session_id: {}", hex::encode(session_id));
                            } else {
                                tracing::info!("    - session_id: not present");
                            }
                            if let Some(name) = event.event_name {
                                tracing::info!("    - event_name: '{}'", name);
                            } else {
                                tracing::info!("    - event_name: not present");
                            }
                            tracing::info!("    - payload: {} bytes", event.payload.len());
                            if !event.payload.is_empty()
                                && let Ok(payload_str) = std::str::from_utf8(event.payload)
                            {
                                let truncated = if payload_str.len() > 200 {
                                    format!("{}...", &payload_str[..200])
                                } else {
                                    payload_str.to_string()
                                };
                                tracing::info!("    - payload_content: {}", truncated);
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!("  ❌ Failed to parse EventData: {}", e);
                    }
                }
            }
            SchemaType::Log => {
                tracing::info!("🔬 [DEBUG] Parsing inner LogData...");
                match decode_log_data(data) {
                    Ok(logs) => {
                        tracing::info!("  ✅ LogData parsed: {} logs", logs.len());
                        for (i, log) in logs.iter().enumerate() {
                            tracing::info!("  📝 [LOG {}]:", i);
                            tracing::info!("    - event_type: {}", log.event_type);
                            tracing::info!("    - level: {}", log.level);
                            tracing::info!("    - timestamp: {}", log.timestamp);
                            if let Some(session_id) = log.session_id {
                                tracing::info!("    - session_id: {}", hex::encode(session_id));
                            } else {
                                tracing::info!("    - session_id: not present");
                            }
                            if let Some(source) = log.source {
                                tracing::info!("    - source: '{}'", source);
                            } else {
                                tracing::info!("    - source: not present");
                            }
                            if let Some(service) = log.service {
                                tracing::info!("    - service: '{}'", service);
                            } else {
                                tracing::info!("    - service: not present");
                            }
                            tracing::info!("    - payload: {} bytes", log.payload.len());
                            if !log.payload.is_empty()
                                && let Ok(payload_str) = std::str::from_utf8(log.payload)
                            {
                                let truncated = if payload_str.len() > 200 {
                                    format!("{}...", &payload_str[..200])
                                } else {
                                    payload_str.to_string()
                                };
                                tracing::info!("    - payload_content: {}", truncated);
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!("  ❌ Failed to parse LogData: {}", e);
                    }
                }
            }
            other => {
                tracing::info!("  ⚠️ Unsupported schema type for inner parsing: {:?}", other);
            }
        }
    }

    /// Try to read a complete message from the buffer
    fn try_read_message(&self, buf: &mut BytesMut) -> Result<Option<BytesMut>, TcpSourceError> {
        if buf.len() < LENGTH_PREFIX_SIZE {
            return Ok(None);
        }

        let msg_len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);

        tracing::trace!(
            msg_len = msg_len,
            buf_len = buf.len(),
            "🔍 [DEBUG] Checking message length"
        );

        if msg_len > MAX_REASONABLE_SIZE as u32 {
            self.metrics.oversized_message();
            buf.advance(LENGTH_PREFIX_SIZE);
            tracing::warn!(
                msg_len = msg_len,
                max = MAX_REASONABLE_SIZE as u32,
                "❌ [DEBUG] Message too large"
            );
            return Err(TcpSourceError::MessageTooLarge {
                size: msg_len,
                limit: MAX_REASONABLE_SIZE as u32,
            });
        }

        let total_len = LENGTH_PREFIX_SIZE + msg_len as usize;

        if buf.len() < total_len {
            tracing::trace!(
                need = total_len,
                have = buf.len(),
                "⏳ [DEBUG] Waiting for more data"
            );
            return Ok(None);
        }

        buf.advance(LENGTH_PREFIX_SIZE);
        let msg = buf.split_to(msg_len as usize);

        tracing::debug!(msg_len = msg_len, "📦 [DEBUG] Complete message extracted");

        Ok(Some(msg))
    }

    /// Process a message
    async fn process_message(
        &self,
        msg: &[u8],
        conn_info: &ConnectionInfo,
        event_batch: &mut BatchBuilder,
        log_batch: &mut BatchBuilder,
    ) -> Result<(bool, bool), TcpSourceError> {
        let flat_batch = FlatBatch::parse(msg)?;

        // Validate API key
        let api_key = flat_batch.api_key()?;
        let workspace_id = self.auth_store.validate_slice(api_key).ok_or_else(|| {
            self.metrics.auth_failure();
            tracing::warn!(
                api_key = ?api_key,
                "❌ [DEBUG] Invalid API key"
            );
            TcpSourceError::AuthFailed
        })?;

        tracing::info!(
            workspace_id = %workspace_id,
            schema_type = ?flat_batch.schema_type(),
            "✅ [DEBUG] API key validated"
        );

        let workspace_id_num: u32 = workspace_id.as_u32();

        // Determine source IP
        let source_ip = match flat_batch.source_ip() {
            Ok(Some(ip_bytes)) => bytes_to_ip(ip_bytes),
            _ => conn_info.remote_ip,
        };

        tracing::debug!(
            source_ip = %source_ip,
            "📍 [DEBUG] Source IP determined"
        );

        // Get data payload
        let _data = flat_batch.data()?;

        // Update metrics
        self.metrics.base.message_received(msg.len() as u64);

        // Route based on schema type
        let schema_type = flat_batch.schema_type();
        let batch_type = BatchType::from_schema_type(schema_type);

        tracing::info!(
            schema_type = ?schema_type,
            batch_type = ?batch_type,
            "📋 [DEBUG] Routing message"
        );

        match batch_type {
            Some(BatchType::Event) => {
                event_batch.set_workspace_id(workspace_id_num);
                event_batch.set_source_ip(source_ip);
                let full = event_batch.add(msg, 1);
                tracing::debug!(
                    count = event_batch.count(),
                    full = full,
                    "📦 [DEBUG] Added to event batch"
                );
                Ok((full, false))
            }
            Some(BatchType::Log) => {
                log_batch.set_workspace_id(workspace_id_num);
                log_batch.set_source_ip(source_ip);
                let full = log_batch.add(msg, 1);
                tracing::debug!(
                    count = log_batch.count(),
                    full = full,
                    "📦 [DEBUG] Added to log batch"
                );
                Ok((false, full))
            }
            Some(other) => {
                tracing::warn!(
                    batch_type = ?other,
                    "⚠️ [DEBUG] Unsupported batch type, treating as event"
                );
                event_batch.set_workspace_id(workspace_id_num);
                event_batch.set_source_ip(source_ip);
                let full = event_batch.add(msg, 1);
                Ok((full, false))
            }
            None => {
                tracing::warn!(
                    schema_type = ?schema_type,
                    "⚠️ [DEBUG] Unknown schema type, treating as event"
                );
                event_batch.set_workspace_id(workspace_id_num);
                event_batch.set_source_ip(source_ip);
                let full = event_batch.add(msg, 1);
                Ok((full, false))
            }
        }
    }

    /// Flush batches
    async fn flush_batches(
        &self,
        event_batch: &mut BatchBuilder,
        log_batch: &mut BatchBuilder,
        connection_id: u64,
    ) -> Result<(), TcpSourceError> {
        let source_id = self.config.source_id();

        if !event_batch.is_empty() {
            tracing::debug!(
                count = event_batch.count(),
                "🚀 [DEBUG] Flushing event batch"
            );
            self.send_batch(event_batch, &source_id, connection_id).await?;
        }

        if !log_batch.is_empty() {
            tracing::debug!(count = log_batch.count(), "🚀 [DEBUG] Flushing log batch");
            self.send_batch(log_batch, &source_id, connection_id).await?;
        }

        Ok(())
    }

    /// Send a batch to the pipeline
    async fn send_batch(
        &self,
        builder: &mut BatchBuilder,
        source_id: &SourceId,
        connection_id: u64,
    ) -> Result<(), TcpSourceError> {
        if builder.is_empty() {
            return Ok(());
        }

        let batch_type = BatchType::Event; // Will be corrected below

        let old_builder =
            std::mem::replace(builder, BatchBuilder::new(batch_type, source_id.clone()));

        let batch = old_builder.finish();

        builder.reset_with_type(batch.batch_type());

        self.batch_sender
            .send(batch, connection_id)
            .await
            .map_err(|_| TcpSourceError::ChannelClosed)?;

        self.metrics.base.batch_sent();

        Ok(())
    }

    /// Configure socket options using socket2
    ///
    /// Sets nodelay, buffer sizes, and keepalive matching Go's SetupTCPConn.
    fn configure_socket(&self, stream: &TcpStream) -> Result<(), TcpSourceError> {
        let fd = stream.as_raw_fd();

        // SAFETY: We're borrowing the fd temporarily. We use forget() to prevent
        // socket2 from closing the fd when it drops - tokio still owns it.
        let socket = unsafe { Socket::from_raw_fd(fd) };

        // TCP_NODELAY - disable Nagle's algorithm
        if self.config.nodelay && let Err(e) = socket.set_tcp_nodelay(true) {
            tracing::warn!(error = %e, "Failed to set TCP_NODELAY");
        }

        // Socket buffer sizes
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

        Ok(())
    }
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Convert SchemaType to readable string (matches Go's schemaTypeToString)
fn schema_type_name(schema_type: SchemaType) -> &'static str {
    match schema_type {
        SchemaType::Unknown => "UNKNOWN",
        SchemaType::Event => "EVENT",
        SchemaType::Log => "LOG",
        SchemaType::Metric => "METRIC",
        SchemaType::Trace => "TRACE",
        SchemaType::Snapshot => "SNAPSHOT",
    }
}

#[cfg(test)]
#[path = "tcp_debug_test.rs"]
mod tcp_debug_test;
