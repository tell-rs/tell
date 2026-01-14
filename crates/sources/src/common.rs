//! Common types and utilities for sources
//!
//! Shared functionality across all source types (TCP, Syslog, etc.)

use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering};

/// Common configuration shared by all sources
#[derive(Debug, Clone)]
pub struct SourceConfig {
    /// Unique identifier for this source (used in routing)
    pub id: String,

    /// Buffer size for network reads
    pub buffer_size: usize,

    /// Channel queue size for batches
    pub queue_size: usize,

    /// Batch flush interval in milliseconds
    pub flush_interval_ms: u64,
}

/// Metrics shared by all source types
#[derive(Debug, Default)]
pub struct SourceMetrics {
    /// Currently active connections
    pub connections_active: AtomicU64,

    /// Total connections accepted
    pub connections_total: AtomicU64,

    /// Total messages received
    pub messages_received: AtomicU64,

    /// Total bytes received
    pub bytes_received: AtomicU64,

    /// Total batches sent to pipeline
    pub batches_sent: AtomicU64,

    /// Total errors encountered
    pub errors: AtomicU64,
}

impl SourceMetrics {
    /// Create new metrics instance
    pub const fn new() -> Self {
        Self {
            connections_active: AtomicU64::new(0),
            connections_total: AtomicU64::new(0),
            messages_received: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            batches_sent: AtomicU64::new(0),
            errors: AtomicU64::new(0),
        }
    }

    /// Increment active connections
    #[inline]
    pub fn connection_opened(&self) {
        self.connections_active.fetch_add(1, Ordering::Relaxed);
        self.connections_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement active connections
    #[inline]
    pub fn connection_closed(&self) {
        self.connections_active.fetch_sub(1, Ordering::Relaxed);
    }

    /// Record received message
    #[inline]
    pub fn message_received(&self, bytes: u64) {
        self.messages_received.fetch_add(1, Ordering::Relaxed);
        self.bytes_received.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record sent batch
    #[inline]
    pub fn batch_sent(&self) {
        self.batches_sent.fetch_add(1, Ordering::Relaxed);
    }

    /// Record error
    #[inline]
    pub fn error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Get snapshot of all metrics
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            connections_active: self.connections_active.load(Ordering::Relaxed),
            connections_total: self.connections_total.load(Ordering::Relaxed),
            messages_received: self.messages_received.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
            batches_sent: self.batches_sent.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
        }
    }
}

/// Point-in-time snapshot of metrics
#[derive(Debug, Clone, Copy)]
pub struct MetricsSnapshot {
    pub connections_active: u64,
    pub connections_total: u64,
    pub messages_received: u64,
    pub bytes_received: u64,
    pub batches_sent: u64,
    pub errors: u64,
}

/// Connection info passed through the source
#[derive(Debug, Clone)]
pub struct ConnectionInfo {
    /// Remote IP address
    pub remote_ip: IpAddr,

    /// Remote port
    pub remote_port: u16,

    /// Local port this connection came in on
    pub local_port: u16,
}

impl ConnectionInfo {
    /// Create from socket address
    pub fn new(remote: std::net::SocketAddr, local_port: u16) -> Self {
        Self {
            remote_ip: remote.ip(),
            remote_port: remote.port(),
            local_port,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_connection_tracking() {
        let metrics = SourceMetrics::new();

        metrics.connection_opened();
        metrics.connection_opened();
        assert_eq!(metrics.connections_active.load(Ordering::Relaxed), 2);
        assert_eq!(metrics.connections_total.load(Ordering::Relaxed), 2);

        metrics.connection_closed();
        assert_eq!(metrics.connections_active.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.connections_total.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn test_metrics_message_tracking() {
        let metrics = SourceMetrics::new();

        metrics.message_received(100);
        metrics.message_received(200);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.messages_received, 2);
        assert_eq!(snapshot.bytes_received, 300);
    }
}
