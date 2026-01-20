//! TCP test client for FlatBuffer protocol
//!
//! Sends length-prefixed FlatBuffer batches to the TCP source.

use std::io;

use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

use crate::batch::BuiltBatch;

/// Simple TCP test client for sending FlatBuffer batches
///
/// Protocol: 4-byte big-endian length prefix + FlatBuffer Batch
///
/// # Example
///
/// ```ignore
/// use tell_client::test::TcpTestClient;
/// use tell_client::BatchBuilder;
///
/// let batch = BatchBuilder::new()
///     .api_key([0x01; 16])
///     .data(b"test")
///     .build()?;
///
/// let mut client = TcpTestClient::connect("127.0.0.1:50000").await?;
/// client.send(&batch).await?;
/// client.close().await?;
/// ```
pub struct TcpTestClient {
    stream: TcpStream,
}

impl TcpTestClient {
    /// Connect to a TCP source
    ///
    /// # Arguments
    ///
    /// * `addr` - Address to connect to (e.g., "127.0.0.1:50000")
    ///
    /// # Errors
    ///
    /// Returns error if connection fails.
    pub async fn connect(addr: &str) -> io::Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        Ok(Self { stream })
    }

    /// Send a batch to the Tell server
    ///
    /// Writes the 4-byte length prefix followed by the batch data.
    ///
    /// # Errors
    ///
    /// Returns error if write fails.
    pub async fn send(&mut self, batch: &BuiltBatch) -> io::Result<()> {
        let data = batch.as_bytes();
        let len = data.len() as u32;

        // Write length prefix (big-endian)
        self.stream.write_all(&len.to_be_bytes()).await?;

        // Write batch data
        self.stream.write_all(data).await?;

        Ok(())
    }

    /// Send raw bytes with length prefix
    ///
    /// For testing malformed data.
    pub async fn send_raw(&mut self, data: &[u8]) -> io::Result<()> {
        let len = data.len() as u32;
        self.stream.write_all(&len.to_be_bytes()).await?;
        self.stream.write_all(data).await?;
        Ok(())
    }

    /// Flush the stream
    pub async fn flush(&mut self) -> io::Result<()> {
        self.stream.flush().await
    }

    /// Close the connection gracefully
    pub async fn close(mut self) -> io::Result<()> {
        self.stream.shutdown().await
    }

    /// Get the local address
    pub fn local_addr(&self) -> io::Result<std::net::SocketAddr> {
        self.stream.local_addr()
    }

    /// Get the peer address
    pub fn peer_addr(&self) -> io::Result<std::net::SocketAddr> {
        self.stream.peer_addr()
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_length_prefix_encoding() {
        // Verify big-endian encoding
        let len: u32 = 256;
        let bytes = len.to_be_bytes();
        assert_eq!(bytes, [0, 0, 1, 0]);

        let len: u32 = 0x12345678;
        let bytes = len.to_be_bytes();
        assert_eq!(bytes, [0x12, 0x34, 0x56, 0x78]);
    }
}
