//! End-to-end benchmark configuration
//!
//! Defines source → sink combinations for full pipeline benchmarking.

use crate::BenchScenario;

/// Source type for E2E testing
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceType {
    /// TCP source (FlatBuffer protocol)
    Tcp,
    /// Syslog over TCP (line-delimited)
    SyslogTcp,
    /// Syslog over UDP (datagram)
    SyslogUdp,
    /// HTTP source (JSONL format for events)
    HttpEvents,
    /// HTTP source (JSONL format for logs)
    HttpLogs,
}

impl SourceType {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Tcp => "tcp",
            Self::SyslogTcp => "syslog_tcp",
            Self::SyslogUdp => "syslog_udp",
            Self::HttpEvents => "http_events",
            Self::HttpLogs => "http_logs",
        }
    }

    /// Default port for this source type
    pub fn default_port(&self) -> u16 {
        match self {
            Self::Tcp => 50000,
            Self::SyslogTcp => 50514,
            Self::SyslogUdp => 50515,
            Self::HttpEvents => 8080,
            Self::HttpLogs => 8080,
        }
    }

    /// HTTP endpoint path for HTTP sources
    pub fn http_path(&self) -> Option<&'static str> {
        match self {
            Self::HttpEvents => Some("/v1/events"),
            Self::HttpLogs => Some("/v1/logs"),
            _ => None,
        }
    }

    /// Whether this source uses HTTP protocol
    pub fn is_http(&self) -> bool {
        matches!(self, Self::HttpEvents | Self::HttpLogs)
    }
}

/// Sink type for E2E testing
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkType {
    /// Binary disk writer
    DiskBinary,
    /// Null sink (discard, measures pipeline overhead)
    Null,
}

impl SinkType {
    pub fn name(&self) -> &'static str {
        match self {
            Self::DiskBinary => "disk_binary",
            Self::Null => "null",
        }
    }
}

/// E2E test scenario
#[derive(Debug, Clone, Copy)]
pub struct E2EScenario {
    pub name: &'static str,
    pub source: SourceType,
    pub sink: SinkType,
    /// Which batch scenario to use (from scenarios.rs)
    pub batch_scenario_idx: usize,
    /// Number of batches to send
    pub num_batches: usize,
}

impl E2EScenario {
    /// Get the batch scenario from SCENARIOS
    pub fn batch_scenario(&self) -> &'static BenchScenario {
        &crate::SCENARIOS[self.batch_scenario_idx]
    }

    /// Total events in this E2E test
    pub fn total_events(&self) -> usize {
        self.num_batches * self.batch_scenario().events_per_batch
    }
}

/// E2E scenarios to benchmark
///
/// Each test runs one source → one sink to get accurate measurements.
pub const E2E_SCENARIOS: &[E2EScenario] = &[
    // ===========================================
    // TCP Source (FlatBuffer protocol)
    // ===========================================
    E2EScenario {
        name: "tcp_to_disk_typical",
        source: SourceType::Tcp,
        sink: SinkType::DiskBinary,
        batch_scenario_idx: 1, // typical: 100 events × 200 bytes
        num_batches: 1000,
    },
    E2EScenario {
        name: "tcp_to_disk_high_volume",
        source: SourceType::Tcp,
        sink: SinkType::DiskBinary,
        batch_scenario_idx: 2, // high_volume: 500 events × 200 bytes
        num_batches: 1000,
    },
    E2EScenario {
        name: "tcp_to_null_typical",
        source: SourceType::Tcp,
        sink: SinkType::Null,
        batch_scenario_idx: 1,
        num_batches: 1000,
    },
    // ===========================================
    // Syslog TCP Source
    // ===========================================
    E2EScenario {
        name: "syslog_tcp_to_disk_typical",
        source: SourceType::SyslogTcp,
        sink: SinkType::DiskBinary,
        batch_scenario_idx: 1,
        num_batches: 1000,
    },
    E2EScenario {
        name: "syslog_tcp_to_null_typical",
        source: SourceType::SyslogTcp,
        sink: SinkType::Null,
        batch_scenario_idx: 1,
        num_batches: 1000,
    },
    // ===========================================
    // Syslog UDP Source
    // ===========================================
    E2EScenario {
        name: "syslog_udp_to_disk_typical",
        source: SourceType::SyslogUdp,
        sink: SinkType::DiskBinary,
        batch_scenario_idx: 1,
        num_batches: 1000,
    },
    E2EScenario {
        name: "syslog_udp_to_null_typical",
        source: SourceType::SyslogUdp,
        sink: SinkType::Null,
        batch_scenario_idx: 1,
        num_batches: 1000,
    },
    // ===========================================
    // HTTP Events Source (JSONL protocol)
    // ===========================================
    E2EScenario {
        name: "http_events_to_disk_typical",
        source: SourceType::HttpEvents,
        sink: SinkType::DiskBinary,
        batch_scenario_idx: 1, // typical: 100 events × 200 bytes
        num_batches: 1000,
    },
    E2EScenario {
        name: "http_events_to_null_typical",
        source: SourceType::HttpEvents,
        sink: SinkType::Null,
        batch_scenario_idx: 1,
        num_batches: 1000,
    },
    // ===========================================
    // HTTP Logs Source (JSONL protocol)
    // ===========================================
    E2EScenario {
        name: "http_logs_to_disk_typical",
        source: SourceType::HttpLogs,
        sink: SinkType::DiskBinary,
        batch_scenario_idx: 1, // typical: 100 logs × 200 bytes
        num_batches: 1000,
    },
    E2EScenario {
        name: "http_logs_to_null_typical",
        source: SourceType::HttpLogs,
        sink: SinkType::Null,
        batch_scenario_idx: 1,
        num_batches: 1000,
    },
];

/// Quick E2E scenarios for fast iteration
pub const E2E_SCENARIOS_QUICK: &[E2EScenario] = &[
    E2E_SCENARIOS[0], // tcp_to_disk_typical
    E2E_SCENARIOS[3], // syslog_tcp_to_disk_typical
    E2E_SCENARIOS[7], // http_events_to_disk_typical
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scenario_total_events() {
        let scenario = &E2E_SCENARIOS[0];
        // typical: 100 events × 1000 batches = 100,000
        assert_eq!(scenario.total_events(), 100_000);
    }
}
