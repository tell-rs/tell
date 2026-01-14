//! Syslog UDP test client
//!
//! Sends syslog messages over UDP.

use std::io;
use std::net::SocketAddr;

use tokio::net::UdpSocket;

/// Simple syslog UDP test client
///
/// Protocol: One UDP packet = one syslog message
///
/// # Example
///
/// ```ignore
/// use cdp_client::test::SyslogUdpTestClient;
///
/// let client = SyslogUdpTestClient::new().await?;
///
/// // RFC 3164 format
/// client.send_to("<134>Dec 20 12:34:56 host app: message", "127.0.0.1:514").await?;
///
/// // RFC 5424 format
/// client.send_to("<165>1 2023-12-20T12:36:15Z host app 1234 ID47 - message", "127.0.0.1:514").await?;
/// ```
pub struct SyslogUdpTestClient {
    socket: UdpSocket,
    target: Option<SocketAddr>,
}

impl SyslogUdpTestClient {
    /// Create a new UDP client
    ///
    /// Binds to an ephemeral port.
    ///
    /// # Errors
    ///
    /// Returns error if socket creation fails.
    pub async fn new() -> io::Result<Self> {
        let socket = UdpSocket::bind("0.0.0.0:0").await?;
        Ok(Self {
            socket,
            target: None,
        })
    }

    /// Create a new UDP client bound to a specific address
    pub async fn bind(addr: &str) -> io::Result<Self> {
        let socket = UdpSocket::bind(addr).await?;
        Ok(Self {
            socket,
            target: None,
        })
    }

    /// Connect to a target address
    ///
    /// After connecting, you can use `send()` instead of `send_to()`.
    pub async fn connect(&mut self, addr: &str) -> io::Result<()> {
        let addr: SocketAddr = addr.parse().map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidInput, format!("invalid address: {}", e))
        })?;
        self.socket.connect(addr).await?;
        self.target = Some(addr);
        Ok(())
    }

    /// Send a syslog message to a specific address
    ///
    /// # Arguments
    ///
    /// * `message` - Syslog message (RFC 3164 or RFC 5424 format)
    /// * `addr` - Target address (e.g., "127.0.0.1:514")
    ///
    /// # Errors
    ///
    /// Returns error if send fails.
    pub async fn send_to(&self, message: &str, addr: &str) -> io::Result<usize> {
        self.socket.send_to(message.as_bytes(), addr).await
    }

    /// Send a syslog message to the connected address
    ///
    /// Requires calling `connect()` first.
    ///
    /// # Errors
    ///
    /// Returns error if not connected or send fails.
    pub async fn send(&self, message: &str) -> io::Result<usize> {
        self.socket.send(message.as_bytes()).await
    }

    /// Send multiple messages to a specific address
    pub async fn send_all_to(&self, messages: &[&str], addr: &str) -> io::Result<()> {
        for msg in messages {
            self.send_to(msg, addr).await?;
        }
        Ok(())
    }

    /// Send raw bytes to a specific address
    pub async fn send_raw_to(&self, data: &[u8], addr: &str) -> io::Result<usize> {
        self.socket.send_to(data, addr).await
    }

    /// Get the local address
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }
}

/// Re-export helper functions from SyslogTcpTestClient
impl SyslogUdpTestClient {
    /// Build an RFC 3164 syslog message
    ///
    /// See [`super::SyslogTcpTestClient::rfc3164`] for details.
    pub fn rfc3164(priority: u8, hostname: &str, tag: &str, message: &str) -> String {
        super::SyslogTcpTestClient::rfc3164(priority, hostname, tag, message)
    }

    /// Build an RFC 5424 syslog message
    ///
    /// See [`super::SyslogTcpTestClient::rfc5424`] for details.
    pub fn rfc5424(
        priority: u8,
        hostname: &str,
        app_name: &str,
        proc_id: &str,
        msg_id: &str,
        message: &str,
    ) -> String {
        super::SyslogTcpTestClient::rfc5424(priority, hostname, app_name, proc_id, msg_id, message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_client_creation() {
        let client = SyslogUdpTestClient::new().await.unwrap();
        let addr = client.local_addr().unwrap();
        assert!(addr.port() > 0);
    }

    #[tokio::test]
    async fn test_client_bind() {
        let client = SyslogUdpTestClient::bind("127.0.0.1:0").await.unwrap();
        let addr = client.local_addr().unwrap();
        assert_eq!(addr.ip().to_string(), "127.0.0.1");
    }

    #[test]
    fn test_rfc3164_helper() {
        let msg = SyslogUdpTestClient::rfc3164(134, "host", "app", "test");
        assert!(msg.starts_with("<134>"));
    }

    #[test]
    fn test_rfc5424_helper() {
        let msg = SyslogUdpTestClient::rfc5424(165, "host", "app", "-", "-", "test");
        assert!(msg.starts_with("<165>1"));
    }
}
