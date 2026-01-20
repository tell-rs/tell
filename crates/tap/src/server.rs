//! Unix socket server for tap connections
//!
//! `TapServer` listens on a Unix socket and handles tap client connections.
//! Each client can send a `SubscribeRequest` and receives batches matching
//! their filter criteria.
//!
//! # Protocol
//!
//! All messages are length-prefixed: `[4-byte big-endian length][payload]`
//!
//! Client → Server:
//! - `Subscribe` - Request to start streaming with filter criteria
//!
//! Server → Client:
//! - `Batch` - A batch envelope with metadata and raw FlatBuffer payload
//! - `Heartbeat` - Keep-alive (every 30s)
//! - `Error` - Error message (e.g., rate limit exceeded)

use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tracing::{debug, error, info, warn};

use tell_protocol::Batch;

use crate::error::{Result, TapError};
use crate::protocol::{TapEnvelope, TapMessage, read_length_prefix};
use crate::tap_point::TapPoint;

/// Default socket path
pub const DEFAULT_SOCKET_PATH: &str = "/tmp/tell-tap.sock";

/// Server configuration
#[derive(Debug, Clone)]
pub struct TapServerConfig {
    /// Path to the Unix socket
    pub socket_path: PathBuf,
    /// Maximum concurrent connections
    pub max_connections: usize,
    /// Read buffer size
    pub read_buffer_size: usize,
    /// Heartbeat interval in seconds
    pub heartbeat_interval_secs: u64,
}

impl Default for TapServerConfig {
    fn default() -> Self {
        Self {
            socket_path: PathBuf::from(DEFAULT_SOCKET_PATH),
            max_connections: 100,
            read_buffer_size: 64 * 1024,
            heartbeat_interval_secs: 30,
        }
    }
}

impl TapServerConfig {
    /// Create config with custom socket path
    pub fn with_socket_path<P: AsRef<Path>>(mut self, path: P) -> Self {
        self.socket_path = path.as_ref().to_path_buf();
        self
    }
}

/// Unix socket server for tap connections
pub struct TapServer {
    /// Server configuration
    config: TapServerConfig,
    /// The tap point for streaming
    tap_point: Arc<TapPoint>,
}

impl TapServer {
    /// Create a new tap server
    pub fn new(tap_point: Arc<TapPoint>, config: TapServerConfig) -> Self {
        Self { config, tap_point }
    }

    /// Create with default configuration
    pub fn with_defaults(tap_point: Arc<TapPoint>) -> Self {
        Self::new(tap_point, TapServerConfig::default())
    }

    /// Get the socket path
    pub fn socket_path(&self) -> &Path {
        &self.config.socket_path
    }

    /// Run the server
    ///
    /// This method blocks until the server is shut down.
    pub async fn run(&self) -> Result<()> {
        // Remove existing socket file
        if self.config.socket_path.exists() {
            std::fs::remove_file(&self.config.socket_path).map_err(TapError::Io)?;
        }

        // Bind the listener
        let listener = UnixListener::bind(&self.config.socket_path).map_err(TapError::Io)?;

        info!(path = %self.config.socket_path.display(), "tap server listening");

        loop {
            match listener.accept().await {
                Ok((stream, _addr)) => {
                    let tap_point = Arc::clone(&self.tap_point);
                    let config = self.config.clone();

                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(stream, tap_point, config).await {
                            debug!(error = %e, "client connection ended");
                        }
                    });
                }
                Err(e) => {
                    error!(error = %e, "failed to accept connection");
                }
            }
        }
    }

    /// Start the server in a background task
    pub fn spawn(self) -> tokio::task::JoinHandle<Result<()>> {
        tokio::spawn(async move { self.run().await })
    }
}

/// Handle a single client connection
async fn handle_connection(
    mut stream: UnixStream,
    tap_point: Arc<TapPoint>,
    config: TapServerConfig,
) -> Result<()> {
    debug!("new tap client connected");

    // Read subscribe request
    let mut buf = BytesMut::with_capacity(config.read_buffer_size);

    // Read length prefix
    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .await
        .map_err(TapError::Io)?;

    let msg_len = read_length_prefix(&len_buf)
        .ok_or_else(|| TapError::Protocol("invalid length prefix".into()))?;

    // Read message
    buf.resize(msg_len as usize, 0);
    stream.read_exact(&mut buf).await.map_err(TapError::Io)?;

    // Decode message
    let msg = TapMessage::decode(buf.freeze())?;

    let request = match msg {
        TapMessage::Subscribe(req) => req,
        _ => {
            let error_msg = TapMessage::Error("expected Subscribe message".into());
            stream
                .write_all(&error_msg.encode())
                .await
                .map_err(TapError::Io)?;
            return Err(TapError::Protocol("expected Subscribe message".into()));
        }
    };

    // Subscribe to tap point
    let (subscriber_id, mut receiver) = tap_point.subscribe(&request)?;

    info!(
        subscriber_id,
        workspaces = ?request.workspace_ids,
        sources = ?request.source_ids,
        types = ?request.batch_types,
        "client subscribed"
    );

    // Set up heartbeat
    let heartbeat_interval = tokio::time::Duration::from_secs(config.heartbeat_interval_secs);
    let mut heartbeat_timer = tokio::time::interval(heartbeat_interval);

    // Main loop: send batches to client
    loop {
        tokio::select! {
            // Batch from tap point
            batch = receiver.recv() => {
                match batch {
                    Some(batch) => {
                        let envelope = batch_to_envelope(&batch);
                        let msg = TapMessage::Batch(envelope);
                        let encoded = msg.encode();

                        if let Err(e) = stream.write_all(&encoded).await {
                            warn!(error = %e, subscriber_id, "failed to send batch to client");
                            break;
                        }
                    }
                    None => {
                        // Channel closed (tap point shutting down)
                        break;
                    }
                }
            }

            // Heartbeat timer
            _ = heartbeat_timer.tick() => {
                let msg = TapMessage::Heartbeat;
                if let Err(e) = stream.write_all(&msg.encode()).await {
                    debug!(error = %e, subscriber_id, "failed to send heartbeat");
                    break;
                }
            }
        }
    }

    // Clean up
    let _ = tap_point.unsubscribe(subscriber_id);
    info!(subscriber_id, "client disconnected");

    Ok(())
}

/// Convert a Batch to a TapEnvelope for transmission
fn batch_to_envelope(batch: &Batch) -> TapEnvelope {
    TapEnvelope {
        workspace_id: batch.workspace_id(),
        source_id: batch.source_id().as_str().into(),
        batch_type: batch.batch_type().to_u8(),
        source_ip: batch.source_ip(),
        count: batch.count() as u32,
        offsets: batch.offsets().to_vec(),
        lengths: batch.lengths().to_vec(),
        payload: batch.buffer().clone(),
    }
}

#[cfg(test)]
#[path = "server_test.rs"]
mod tests;
