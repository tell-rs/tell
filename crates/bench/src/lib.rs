//! Shared benchmark configuration for Tell
//!
//! Defines realistic test scenarios based on Go benchmark patterns.
//! Use across all crate benchmarks for consistent comparison.

mod scenarios;
pub mod e2e_config;

pub use scenarios::{BenchScenario, SCENARIOS, SCENARIOS_QUICK, batch_sizes, event_sizes};
pub use e2e_config::{E2EScenario, E2E_SCENARIOS, E2E_SCENARIOS_QUICK, SinkType, SourceType};
