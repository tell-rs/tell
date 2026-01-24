//! Telemetry data collector.
//!
//! Gathers telemetry data from various sources without blocking the main pipeline.
//! All collection is done through snapshots and atomic reads.

use std::path::Path;
use std::time::Instant;

use sha2::{Digest, Sha256};

use crate::payload::{ConfigShape, DeploymentInfo, FeatureUsage, RuntimeMetrics, TelemetryPayload};

/// Collects telemetry data from the running system.
pub struct Collector {
    /// Cached deployment info (static, collected once)
    deployment: DeploymentInfo,

    /// When the collector was created (for uptime calculation)
    start_time: Instant,

    /// Feature usage tracking (updated via notify methods)
    features: FeatureUsage,
}

impl Collector {
    /// Create a new collector with the given install ID.
    ///
    /// The install ID should be loaded from persistent storage or generated
    /// if this is a fresh install.
    pub fn new(install_id: String) -> Self {
        let mut deployment = DeploymentInfo::collect();
        deployment.install_id = install_id;

        Self {
            deployment,
            start_time: Instant::now(),
            features: FeatureUsage::default(),
        }
    }

    /// Generate a deterministic install ID from machine-specific data.
    ///
    /// Returns a truncated SHA256 hash that's consistent across restarts
    /// but not reversible to identify the machine.
    pub fn generate_install_id() -> String {
        let mut hasher = Sha256::new();

        // Use hostname as primary identifier
        if let Ok(hostname) = hostname::get() {
            hasher.update(hostname.to_string_lossy().as_bytes());
        }

        // Add some entropy from the system
        hasher.update(std::env::consts::OS.as_bytes());
        hasher.update(std::env::consts::ARCH.as_bytes());

        // Add home directory path for uniqueness
        if let Some(home) = dirs::home_dir() {
            hasher.update(home.to_string_lossy().as_bytes());
        }

        let result = hasher.finalize();
        // Use first 16 bytes (32 hex chars) for reasonable uniqueness
        hex::encode(&result[..16])
    }

    /// Load install ID from file, or generate and save a new one.
    pub fn load_or_create_install_id(path: &Path) -> std::io::Result<String> {
        if path.exists() {
            let id = std::fs::read_to_string(path)?;
            let id = id.trim().to_string();
            if id.len() == 32 && id.chars().all(|c| c.is_ascii_hexdigit()) {
                return Ok(id);
            }
        }

        let id = Self::generate_install_id();

        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        std::fs::write(path, &id)?;
        Ok(id)
    }

    /// Get the deployment info.
    pub fn deployment(&self) -> &DeploymentInfo {
        &self.deployment
    }

    /// Notify that a feature was used.
    pub fn notify_feature_used(&mut self, feature: Feature) {
        match feature {
            Feature::Tail => self.features.tail_used = true,
            Feature::Query => self.features.query_used = true,
            Feature::Tui => self.features.tui_used = true,
        }
    }

    /// Update feature counts (call periodically or on changes).
    pub fn update_feature_counts(&mut self, counts: FeatureCounts) {
        self.features.boards_count = counts.boards;
        self.features.metrics_count = counts.metrics;
        self.features.api_keys_count = counts.api_keys;
        self.features.workspaces_count = counts.workspaces;
        self.features.transformers_enabled = counts.transformers_enabled;
    }

    /// Collect a complete telemetry payload.
    ///
    /// This takes a snapshot of current metrics. The metrics parameter
    /// should come from the pipeline's metric collection system.
    pub fn collect(&self, config: ConfigShape, metrics: PipelineMetrics) -> TelemetryPayload {
        let runtime = RuntimeMetrics {
            messages_total: metrics.messages_processed,
            bytes_total: metrics.bytes_processed,
            uptime_secs: self.start_time.elapsed().as_secs(),
            errors_total: metrics.errors_total,
            connections_active: metrics.connections_active,
        };

        TelemetryPayload::new(
            self.deployment.clone(),
            config,
            runtime,
            self.features.clone(),
        )
    }
}

/// Features that can be marked as used.
#[derive(Debug, Clone, Copy)]
pub enum Feature {
    Tail,
    Query,
    Tui,
}

/// Feature counts for periodic updates.
#[derive(Debug, Clone, Default)]
pub struct FeatureCounts {
    pub boards: u32,
    pub metrics: u32,
    pub api_keys: u32,
    pub workspaces: u32,
    pub transformers_enabled: bool,
}

/// Pipeline metrics snapshot for telemetry collection.
///
/// This is a simplified view of the full CollectedMetrics from tell-metrics.
#[derive(Debug, Clone, Default)]
pub struct PipelineMetrics {
    pub messages_processed: u64,
    pub bytes_processed: u64,
    pub errors_total: u64,
    pub connections_active: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_install_id() {
        let id1 = Collector::generate_install_id();
        let id2 = Collector::generate_install_id();

        // Should be deterministic
        assert_eq!(id1, id2);

        // Should be 32 hex characters
        assert_eq!(id1.len(), 32);
        assert!(id1.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_collector_uptime() {
        let collector = Collector::new("test123".to_string());

        std::thread::sleep(std::time::Duration::from_millis(10));

        let payload = collector.collect(ConfigShape::default(), PipelineMetrics::default());

        // Uptime should be at least 0 (could be 0 if very fast)
        assert!(payload.metrics.uptime_secs < 10);
    }

    #[test]
    fn test_feature_tracking() {
        let mut collector = Collector::new("test123".to_string());

        assert!(!collector.features.tail_used);
        collector.notify_feature_used(Feature::Tail);
        assert!(collector.features.tail_used);

        let payload = collector.collect(ConfigShape::default(), PipelineMetrics::default());
        assert!(payload.features.tail_used);
    }
}
