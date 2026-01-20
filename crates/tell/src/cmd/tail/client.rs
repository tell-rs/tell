//! Tap client - connects to the Tell tap server

use std::path::Path;

use anyhow::{Context, Result};
use bytes::{Buf, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

use tell_tap::{SubscribeRequest, TapMessage};

/// Client for connecting to the tap server
pub struct TapClient {
    stream: UnixStream,
    read_buf: BytesMut,
}

impl TapClient {
    /// Connect to the tap server at the given socket path
    pub async fn connect<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        let stream = UnixStream::connect(path)
            .await
            .with_context(|| format!("failed to connect to {}", path.display()))?;

        Ok(Self {
            stream,
            read_buf: BytesMut::with_capacity(64 * 1024),
        })
    }

    /// Send a subscribe request to the server
    pub async fn subscribe(&mut self, request: &SubscribeRequest) -> Result<()> {
        let msg = TapMessage::Subscribe(request.clone());
        let encoded = msg.encode();

        self.stream
            .write_all(&encoded)
            .await
            .context("failed to send subscribe request")?;

        Ok(())
    }

    /// Receive the next message from the server
    ///
    /// Returns `Ok(None)` if the connection is closed.
    pub async fn recv(&mut self) -> Result<Option<TapMessage>> {
        // Read length prefix (4 bytes)
        loop {
            // Try to parse a complete message from the buffer
            if self.read_buf.len() >= 4 {
                let len = u32::from_be_bytes([
                    self.read_buf[0],
                    self.read_buf[1],
                    self.read_buf[2],
                    self.read_buf[3],
                ]) as usize;

                if self.read_buf.len() >= 4 + len {
                    // We have a complete message
                    self.read_buf.advance(4);
                    let payload = self.read_buf.split_to(len).freeze();
                    let msg =
                        TapMessage::decode(payload).context("failed to decode tap message")?;
                    return Ok(Some(msg));
                }
            }

            // Need more data - read from socket
            let n = self
                .stream
                .read_buf(&mut self.read_buf)
                .await
                .context("failed to read from socket")?;

            if n == 0 {
                // Connection closed
                return Ok(None);
            }
        }
    }
}
