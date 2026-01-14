//! Log module - structured log entries
//!
//! Builds `log.fbs::LogEntry` and `log.fbs::LogData` messages for
//! structured logging (optimized syslog protocol).

mod builder;
mod types;

#[cfg(test)]
mod builder_test;

pub use builder::{BuiltLogData, BuiltLogEntry, LogDataBuilder, LogEntryBuilder};
pub use types::{LogEventType, LogLevel};
