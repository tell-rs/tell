//! Event module - CDP/product analytics events
//!
//! Builds `event.fbs::Event` and `event.fbs::EventData` messages for
//! product analytics tracking (Track, Identify, Group, etc.).

mod builder;
mod types;

#[cfg(test)]
mod builder_test;

pub use builder::{BuiltEvent, BuiltEventData, EventBuilder, EventDataBuilder};
pub use types::EventType;
