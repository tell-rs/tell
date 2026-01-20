//! Log metrics
//!
//! Metrics derived from `logs_v1` table:
//! - Log volume
//! - Top logs (by level, source, etc.)

mod top;
mod volume;

pub use top::TopLogsMetric;
pub use volume::LogVolumeMetric;
