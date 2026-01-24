//! Tell telemetry - opt-in, transparent, non-blocking product telemetry.
//!
//! This crate provides telemetry collection for Tell deployments. Key principles:
//!
//! - **Opt-in**: Disabled by default, must be explicitly enabled in config
//! - **Transparent**: Users can inspect exactly what's sent via `tell telemetry show`
//! - **Non-blocking**: Uses fire-and-forget pattern, never blocks the pipeline
//! - **Privacy-respecting**: Anonymous install ID (hash), no PII collected
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────┐     ┌──────────────┐     ┌──────────────┐
//! │  Collector  │────▶│   Payload    │────▶│   Reporter   │
//! │ (snapshots) │     │  (struct)    │     │ (async task) │
//! └─────────────┘     └──────────────┘     └──────────────┘
//!       │                                         │
//!       │ Reads from:                             │ Sends to:
//!       │ - CollectedMetrics                      │ - t.tell.rs/v1/collect
//!       │ - Config                                │
//!       │ - Feature flags                         │
//!       ▼                                         ▼
//! ┌─────────────┐                         ┌──────────────┐
//! │  Pipeline   │                         │   HTTP POST  │
//! │  (no block) │                         │  (timeout)   │
//! └─────────────┘                         └──────────────┘
//! ```
//!
//! # Usage
//!
//! ```rust,no_run
//! use tell_telemetry::{Collector, ReporterConfig, reporter};
//! use tell_telemetry::payload::ConfigShape;
//! use tell_telemetry::collector::PipelineMetrics;
//!
//! // Load or generate install ID
//! let install_id = Collector::load_or_create_install_id(
//!     &std::path::PathBuf::from("~/.tell/install_id")
//! ).unwrap_or_else(|_| Collector::generate_install_id());
//!
//! // Create collector
//! let collector = Collector::new(install_id);
//!
//! // Spawn reporter (runs in background)
//! let handle = reporter::spawn(ReporterConfig {
//!     enabled: true,
//!     ..Default::default()
//! });
//!
//! // Periodically collect and send telemetry
//! let payload = collector.collect(
//!     ConfigShape::default(),
//!     PipelineMetrics::default(),
//! );
//! let _ = handle.send(payload); // Fire-and-forget, never blocks
//! ```
//!
//! # Configuration
//!
//! ```toml
//! [telemetry]
//! enabled = false                          # opt-in
//! endpoint = "https://t.tell.rs/v1/collect"
//! interval = "7d"                          # weekly
//! ```

pub mod collector;
pub mod endpoint;
pub mod error;
pub mod payload;
pub mod reporter;

pub use collector::{Collector, Feature, FeatureCounts, PipelineMetrics};
pub use error::TelemetryError;
pub use payload::{ConfigShape, DeploymentInfo, FeatureUsage, RuntimeMetrics, TelemetryPayload};
pub use reporter::{ReporterConfig, ReporterHandle};

/// Spawn the telemetry reporter as a background task.
///
/// Returns a handle for sending telemetry. The task runs until
/// the handle is dropped or shutdown is called.
pub fn spawn_reporter(config: ReporterConfig) -> ReporterHandle {
    reporter::spawn(config)
}

/// Default path for the install ID file.
pub fn default_install_id_path() -> std::path::PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| std::path::PathBuf::from("."))
        .join(".tell")
        .join("install_id")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_install_id_path() {
        let path = default_install_id_path();
        assert!(path.to_string_lossy().contains(".tell"));
        assert!(path.to_string_lossy().contains("install_id"));
    }
}
