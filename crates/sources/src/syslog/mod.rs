//! Syslog Sources
//!
//! RFC 3164 and RFC 5424 compliant syslog receivers.
//!
//! # Available Sources
//!
//! - **TCP** - Syslog over TCP with line-based framing
//! - **UDP** - Syslog over UDP with multi-worker support
//!
//! # Design
//!
//! Syslog sources differ from the main TCP source:
//! - No API key authentication (workspace ID from config)
//! - Line-based framing (newline-delimited messages)
//! - Raw message storage (parsing done by sinks/transformers)
//!
//! # Protocol Support
//!
//! Both TCP and UDP support:
//! - RFC 3164 (BSD syslog) - Legacy format
//! - RFC 5424 (IETF syslog) - Structured data
//!
//! Messages are stored raw - parsing happens downstream.

pub mod tcp;
pub mod udp;

pub use tcp::{
    SyslogTcpMetricsHandle, SyslogTcpMetricsSnapshot, SyslogTcpSource, SyslogTcpSourceConfig,
    SyslogTcpSourceError, SyslogTcpSourceMetrics,
};
pub use udp::{
    SyslogUdpMetricsHandle, SyslogUdpMetricsSnapshot, SyslogUdpSource, SyslogUdpSourceConfig,
    SyslogUdpSourceError, SyslogUdpSourceMetrics,
};
