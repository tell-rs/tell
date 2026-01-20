//! Product analytics metrics
//!
//! Metrics derived from `events_v1` table:
//! - Event counts (total, by name)
//! - Top events

mod counts;
mod top;

pub use counts::EventCountMetric;
pub use top::TopEventsMetric;
