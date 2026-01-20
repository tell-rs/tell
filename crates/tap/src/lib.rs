//! Tell Tap - Live streaming tap point for Tell
//!
//! This crate provides the server-side infrastructure for live data streaming
//! to CLI clients. It implements a zero-overhead tap point that:
//!
//! - Filters on metadata only (workspace_id, source_id, batch_type) - no FlatBuffer decode
//! - Supports client-requested rate limiting and sampling
//! - Auto-cleans subscribers on disconnect
//! - Has zero cost when no subscribers are connected
//!
//! # Architecture
//!
//! ```text
//! Router.route()
//!     │
//!     ├──→ Arc::new(batch)
//!     │         │
//!     │    ┌────┴────┐
//!     │    ▼         ▼
//!     │  Sinks   TapPoint ◄── metadata filters + rate limit
//!     │              │
//!     │              ▼
//!     │         Subscribers (per-client channels)
//!     │              │
//!     │              ▼
//!     │         TapServer (Unix socket)
//!     │              │
//!     └──→───────────┼──→ CLI clients
//! ```

pub mod buffer;
mod error;
pub mod filter;
pub mod protocol;
pub mod server;
pub mod subscriber;
pub mod tap_point;

pub use buffer::ReplayBuffer;
pub use error::TapError;
pub use filter::TapFilter;
pub use protocol::{SubscribeRequest, TapEnvelope, TapMessage};
pub use server::{TapServer, TapServerConfig};
pub use subscriber::{Subscriber, SubscriberManager};
pub use tap_point::TapPoint;
