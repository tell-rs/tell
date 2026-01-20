//! Shared benchmark configuration for Tell
//!
//! Defines realistic test scenarios based on Go benchmark patterns.
//! Use across all crate benchmarks for consistent comparison.

pub mod e2e_config;
mod scenarios;

pub use e2e_config::{E2E_SCENARIOS, E2E_SCENARIOS_QUICK, E2EScenario, SinkType, SourceType};
pub use scenarios::{BenchScenario, SCENARIOS, SCENARIOS_QUICK, batch_sizes, event_sizes};
