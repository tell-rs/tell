//! Tell - Connectors
//!
//! Pull-based connectors that fetch data from external sources and produce
//! `Batch` instances for the pipeline.
//!
//! # Available Connectors
//!
//! - **GitHub** - Repository metrics (stars, forks, issues, etc.)
//!
//! # Design Principles
//!
//! - **Pull-based**: Connectors fetch data on-demand or on schedule
//! - **Snapshot format**: Data is captured as point-in-time snapshots
//! - **Pipeline integration**: Snapshots flow through Router → Sinks like other data
//! - **Simple interface**: Each connector implements the `Connector` trait
//!
//! # Feature Flags
//!
//! Connectors can be selectively compiled using feature flags:
//!
//! ```toml
//! [dependencies]
//! tell-connectors = { version = "0.1", default-features = false, features = ["github"] }
//! ```
//!
//! Available features:
//! - `github` (default) - GitHub repository metrics
//!
//! # Example
//!
//! ```ignore
//! use tell_connectors::{GitHub, GitHubConfig, Connector};
//!
//! let github = GitHub::new(GitHubConfig {
//!     token: Some("ghp_xxx".into()),
//!     ..Default::default()
//! });
//!
//! let batch = github.pull("rust-lang/rust").await?;
//! // batch now contains snapshot data ready for pipeline
//! ```

pub mod config;
mod error;
pub mod resilience;
mod scheduler;
mod traits;

// Conditionally compiled connectors
#[cfg(feature = "github")]
mod github;
#[cfg(feature = "shopify")]
mod shopify;

// Re-exports
pub use error::ConnectorError;
pub use scheduler::{ConnectorScheduler, ScheduledConnector};
pub use traits::Connector;

#[cfg(feature = "github")]
pub use config::GitHubConnectorConfig;
#[cfg(feature = "github")]
pub use github::{GitHub, GitHubConfig};

#[cfg(feature = "shopify")]
pub use config::ShopifyConnectorConfig;
#[cfg(feature = "shopify")]
pub use shopify::{Shopify, ShopifyConfig};

/// List of available connector types (compiled in)
pub fn available_connectors() -> &'static [&'static str] {
    &[
        #[cfg(feature = "github")]
        "github",
        #[cfg(feature = "shopify")]
        "shopify",
    ]
}
