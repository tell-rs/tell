//! Test clients for Tell sources
//!
//! Simple, synchronous-style clients for testing and benchmarking.
//! No batching, no retries - just connect and send.
//!
//! # Clients
//!
//! - [`TcpTestClient`] - FlatBuffer protocol over TCP
//! - [`SyslogTcpTestClient`] - Line-delimited syslog over TCP
//! - [`SyslogUdpTestClient`] - Syslog over UDP
//!
//! # Example
//!
//! ```ignore
//! use tell_client::test::TcpTestClient;
//! use tell_client::{BatchBuilder, SchemaType};
//!
//! let batch = BatchBuilder::new()
//!     .api_key([0x01; 16])
//!     .schema_type(SchemaType::Event)
//!     .data(event_data.as_bytes())
//!     .build()?;
//!
//! let mut client = TcpTestClient::connect("127.0.0.1:50000").await?;
//! client.send(&batch).await?;
//! ```

mod syslog_tcp;
mod syslog_udp;
mod tcp;

pub use syslog_tcp::{LineEnding, SyslogTcpTestClient};
pub use syslog_udp::SyslogUdpTestClient;
pub use tcp::TcpTestClient;
