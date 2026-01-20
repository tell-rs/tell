//! Syslog TCP test client
//!
//! Sends line-delimited syslog messages over TCP.

use std::io;

use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

/// Simple syslog TCP test client
///
/// Protocol: Line-delimited messages (LF or CRLF)
///
/// # Example
///
/// ```ignore
/// use tell_client::test::SyslogTcpTestClient;
///
/// let mut client = SyslogTcpTestClient::connect("127.0.0.1:514").await?;
///
/// // RFC 3164 format
/// client.send("<134>Dec 20 12:34:56 host app: message").await?;
///
/// // RFC 5424 format
/// client.send("<165>1 2023-12-20T12:36:15Z host app 1234 ID47 - message").await?;
///
/// client.close().await?;
/// ```
pub struct SyslogTcpTestClient {
    stream: TcpStream,
    line_ending: LineEnding,
}

/// Line ending style
#[derive(Debug, Clone, Copy, Default)]
pub enum LineEnding {
    /// Unix style (LF)
    #[default]
    Lf,
    /// Windows style (CRLF)
    CrLf,
}

impl SyslogTcpTestClient {
    /// Connect to a syslog TCP source
    ///
    /// # Arguments
    ///
    /// * `addr` - Address to connect to (e.g., "127.0.0.1:514")
    ///
    /// # Errors
    ///
    /// Returns error if connection fails.
    pub async fn connect(addr: &str) -> io::Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        Ok(Self {
            stream,
            line_ending: LineEnding::Lf,
        })
    }

    /// Set line ending style
    #[must_use]
    pub fn with_line_ending(mut self, ending: LineEnding) -> Self {
        self.line_ending = ending;
        self
    }

    /// Send a syslog message
    ///
    /// Appends the configured line ending.
    ///
    /// # Arguments
    ///
    /// * `message` - Syslog message (RFC 3164 or RFC 5424 format)
    ///
    /// # Errors
    ///
    /// Returns error if write fails.
    pub async fn send(&mut self, message: &str) -> io::Result<()> {
        self.stream.write_all(message.as_bytes()).await?;
        match self.line_ending {
            LineEnding::Lf => self.stream.write_all(b"\n").await?,
            LineEnding::CrLf => self.stream.write_all(b"\r\n").await?,
        }
        Ok(())
    }

    /// Send multiple messages
    pub async fn send_all(&mut self, messages: &[&str]) -> io::Result<()> {
        for msg in messages {
            self.send(msg).await?;
        }
        Ok(())
    }

    /// Send raw bytes (for testing)
    pub async fn send_raw(&mut self, data: &[u8]) -> io::Result<()> {
        self.stream.write_all(data).await
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

/// Helper functions for building syslog messages
impl SyslogTcpTestClient {
    /// Build an RFC 3164 syslog message
    ///
    /// # Arguments
    ///
    /// * `priority` - Priority value (facility * 8 + severity)
    /// * `hostname` - Hostname
    /// * `tag` - Application tag
    /// * `message` - Log message
    ///
    /// # Example
    ///
    /// ```
    /// use tell_client::test::SyslogTcpTestClient;
    ///
    /// let msg = SyslogTcpTestClient::rfc3164(134, "server1", "myapp", "Hello world");
    /// assert!(msg.starts_with("<134>"));
    /// assert!(msg.contains("server1"));
    /// ```
    pub fn rfc3164(priority: u8, hostname: &str, tag: &str, message: &str) -> String {
        use std::time::{SystemTime, UNIX_EPOCH};

        // Get current time for timestamp
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        let secs = now.as_secs();

        // Simple timestamp (not perfect but good enough for testing)
        let months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
        let days_since_epoch = secs / 86400;
        let month_approx = ((days_since_epoch % 365) / 30) as usize % 12;
        let day = ((days_since_epoch % 365) % 30) + 1;
        let hour = (secs % 86400) / 3600;
        let min = (secs % 3600) / 60;
        let sec = secs % 60;

        format!(
            "<{}>{} {:2} {:02}:{:02}:{:02} {} {}: {}",
            priority,
            months[month_approx],
            day,
            hour,
            min,
            sec,
            hostname,
            tag,
            message
        )
    }

    /// Build an RFC 5424 syslog message
    ///
    /// # Arguments
    ///
    /// * `priority` - Priority value
    /// * `hostname` - Hostname
    /// * `app_name` - Application name
    /// * `proc_id` - Process ID (or "-")
    /// * `msg_id` - Message ID (or "-")
    /// * `message` - Log message
    ///
    /// # Example
    ///
    /// ```
    /// use tell_client::test::SyslogTcpTestClient;
    ///
    /// let msg = SyslogTcpTestClient::rfc5424(165, "server1", "myapp", "1234", "ID47", "Hello");
    /// assert!(msg.starts_with("<165>1"));
    /// ```
    pub fn rfc5424(
        priority: u8,
        hostname: &str,
        app_name: &str,
        proc_id: &str,
        msg_id: &str,
        message: &str,
    ) -> String {
        use std::time::{SystemTime, UNIX_EPOCH};

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        let secs = now.as_secs();
        let millis = now.subsec_millis();

        // ISO 8601 timestamp (simplified)
        let year = 1970 + (secs / 31536000);
        let remaining = secs % 31536000;
        let month = (remaining / 2628000) + 1;
        let day = ((remaining % 2628000) / 86400) + 1;
        let hour = (secs % 86400) / 3600;
        let min = (secs % 3600) / 60;
        let sec = secs % 60;

        format!(
            "<{}>1 {:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z {} {} {} {} - {}",
            priority,
            year,
            month.min(12),
            day.min(31),
            hour,
            min,
            sec,
            millis,
            hostname,
            app_name,
            proc_id,
            msg_id,
            message
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rfc3164_format() {
        let msg = SyslogTcpTestClient::rfc3164(134, "server1", "myapp", "Test message");
        assert!(msg.starts_with("<134>"));
        assert!(msg.contains("server1"));
        assert!(msg.contains("myapp:"));
        assert!(msg.contains("Test message"));
    }

    #[test]
    fn test_rfc5424_format() {
        let msg = SyslogTcpTestClient::rfc5424(165, "server1", "myapp", "1234", "ID47", "Test");
        assert!(msg.starts_with("<165>1"));
        assert!(msg.contains("server1"));
        assert!(msg.contains("myapp"));
        assert!(msg.contains("1234"));
        assert!(msg.contains("ID47"));
        assert!(msg.ends_with("Test"));
    }

    #[test]
    fn test_line_ending_default() {
        let ending = LineEnding::default();
        assert!(matches!(ending, LineEnding::Lf));
    }
}
