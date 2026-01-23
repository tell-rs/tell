//! Tell Tap - Live streaming tap point for Tell (Unix only)
//!
//! This crate provides the server-side infrastructure for live data streaming
//! to CLI clients via Unix sockets. It implements a zero-overhead tap point that:
//!
//! - Filters on metadata only (workspace_id, source_id, batch_type) - no FlatBuffer decode
//! - Supports client-requested rate limiting and sampling
//! - Auto-cleans subscribers on disconnect
//! - Has zero cost when no subscribers are connected
//!
//! **Note:** This crate only compiles on Unix platforms (Linux, macOS) as it uses
//! Unix domain sockets for IPC.
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

#[cfg(unix)]
pub mod buffer;
#[cfg(unix)]
mod error;
#[cfg(unix)]
pub mod filter;
#[cfg(unix)]
pub mod protocol;
#[cfg(unix)]
pub mod server;
#[cfg(unix)]
pub mod subscriber;
#[cfg(unix)]
pub mod tap_point;

#[cfg(unix)]
pub use buffer::ReplayBuffer;
#[cfg(unix)]
pub use error::TapError;
#[cfg(unix)]
pub use filter::TapFilter;
#[cfg(unix)]
pub use protocol::{SubscribeRequest, TapEnvelope, TapMessage};
#[cfg(unix)]
pub use server::{TapServer, TapServerConfig};
#[cfg(unix)]
pub use subscriber::{Subscriber, SubscriberManager};
#[cfg(unix)]
pub use tap_point::TapPoint;
