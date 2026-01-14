//! Benchmark scenarios
//!
//! # Terminology
//!
//! - Event: Single business event/log (page view, log line)
//! - Batch: Container holding multiple events
//! - Wire message: FlatBuffer-encoded batch sent over TCP

/// Batch sizes based on Go defaults
pub mod batch_sizes {
    pub const SMALL: usize = 10;
    pub const MEDIUM: usize = 100;
    pub const LARGE: usize = 500; // Go default
    pub const ALL: &[usize] = &[SMALL, MEDIUM, LARGE];
}

/// Event payload sizes in bytes
pub mod event_sizes {
    pub const TINY: usize = 50;
    pub const SMALL: usize = 100;
    pub const MEDIUM: usize = 200;
    pub const LARGE: usize = 500;
    pub const LOG: usize = 1000;
    pub const LOG_LARGE: usize = 4000;
    pub const ALL: &[usize] = &[SMALL, MEDIUM, LARGE, LOG];
}

/// Test scenario combining batch size and event size
#[derive(Debug, Clone, Copy)]
pub struct BenchScenario {
    pub name: &'static str,
    pub events_per_batch: usize,
    pub event_size: usize,
}

impl BenchScenario {
    pub const fn total_bytes(&self) -> usize {
        self.events_per_batch * self.event_size
    }
}

/// Standard benchmark scenarios
pub const SCENARIOS: &[BenchScenario] = &[
    BenchScenario {
        name: "realtime_small",
        events_per_batch: 10,
        event_size: 100,
    },
    BenchScenario {
        name: "typical",
        events_per_batch: 100,
        event_size: 200,
    },
    BenchScenario {
        name: "high_volume",
        events_per_batch: 500,
        event_size: 200,
    },
    BenchScenario {
        name: "large_events",
        events_per_batch: 100,
        event_size: 1000,
    },
];

/// Quick scenarios for fast iteration
pub const SCENARIOS_QUICK: &[BenchScenario] = &[SCENARIOS[1], SCENARIOS[2]];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scenario_total_bytes() {
        let scenario = BenchScenario {
            name: "test",
            events_per_batch: 100,
            event_size: 200,
        };
        assert_eq!(scenario.total_bytes(), 20_000);
    }
}
