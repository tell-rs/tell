//! Telemetry payload types.
//!
//! Defines the exact structure of data sent to the telemetry endpoint.
//! All fields are intentionally transparent — users can inspect via `tell telemetry show`.

use serde::{Deserialize, Serialize};

/// Complete telemetry payload sent to the collection endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryPayload {
    /// Schema version for forward compatibility
    pub schema_version: u32,

    /// Static deployment information (collected once at startup)
    pub deployment: DeploymentInfo,

    /// Configuration shape (what's enabled, not values)
    pub config: ConfigShape,

    /// Runtime metrics (exact values)
    pub metrics: RuntimeMetrics,

    /// Feature usage flags
    pub features: FeatureUsage,
}

impl TelemetryPayload {
    /// Current schema version
    pub const SCHEMA_VERSION: u32 = 1;

    /// Create a new payload with the current schema version
    pub fn new(
        deployment: DeploymentInfo,
        config: ConfigShape,
        metrics: RuntimeMetrics,
        features: FeatureUsage,
    ) -> Self {
        Self {
            schema_version: Self::SCHEMA_VERSION,
            deployment,
            config,
            metrics,
            features,
        }
    }
}

/// Static deployment information collected once at startup.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeploymentInfo {
    /// Anonymous install identifier (SHA256 hash, not reversible)
    pub install_id: String,

    /// Tell binary version
    pub version: String,

    /// Operating system (darwin, linux, windows)
    pub os: String,

    /// CPU architecture (aarch64, x86_64)
    pub arch: String,

    /// Number of CPU cores
    pub cpu_cores: u32,

    /// Total memory in bytes
    pub memory_bytes: u64,
}

impl DeploymentInfo {
    /// Collect deployment info from the current environment
    pub fn collect() -> Self {
        let (cpu_cores, memory_bytes) = Self::system_info();

        Self {
            install_id: String::new(), // Set by caller after loading/generating
            version: env!("CARGO_PKG_VERSION").to_string(),
            os: std::env::consts::OS.to_string(),
            arch: std::env::consts::ARCH.to_string(),
            cpu_cores,
            memory_bytes,
        }
    }

    /// Get CPU cores and memory using sysinfo
    fn system_info() -> (u32, u64) {
        // Avoid sysinfo dependency - use simple detection
        // CPU cores from std
        let cpu_cores = std::thread::available_parallelism()
            .map(|p| p.get() as u32)
            .unwrap_or(1);

        // Memory detection is platform-specific, default to 0 if unavailable
        let memory_bytes = Self::detect_memory();

        (cpu_cores, memory_bytes)
    }

    #[cfg(target_os = "linux")]
    fn detect_memory() -> u64 {
        std::fs::read_to_string("/proc/meminfo")
            .ok()
            .and_then(|content| {
                content
                    .lines()
                    .find(|line| line.starts_with("MemTotal:"))
                    .and_then(|line| {
                        line.split_whitespace()
                            .nth(1)
                            .and_then(|kb| kb.parse::<u64>().ok())
                            .map(|kb| kb * 1024)
                    })
            })
            .unwrap_or(0)
    }

    #[cfg(target_os = "macos")]
    fn detect_memory() -> u64 {
        use std::process::Command;
        Command::new("sysctl")
            .args(["-n", "hw.memsize"])
            .output()
            .ok()
            .and_then(|output| {
                String::from_utf8(output.stdout)
                    .ok()
                    .and_then(|s| s.trim().parse().ok())
            })
            .unwrap_or(0)
    }

    #[cfg(target_os = "windows")]
    fn detect_memory() -> u64 {
        // Windows: would need winapi, skip for now
        0
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    fn detect_memory() -> u64 {
        0
    }
}

/// Configuration shape - what's enabled, not actual values.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ConfigShape {
    /// Source types enabled (e.g., ["tcp", "syslog_tcp", "http"])
    pub sources: Vec<String>,

    /// Sink types configured (e.g., ["clickhouse", "arrow_ipc"])
    pub sinks: Vec<String>,

    /// Connector types enabled (e.g., ["github", "shopify"])
    pub connectors: Vec<String>,

    /// Transformer types configured (e.g., ["pattern", "redact"])
    pub transformers: Vec<String>,

    /// Number of routing rules
    pub routing_rules: u32,
}

/// Runtime metrics - exact values for real analysis.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RuntimeMetrics {
    /// Total messages processed since startup
    pub messages_total: u64,

    /// Total bytes processed since startup
    pub bytes_total: u64,

    /// Uptime in seconds
    pub uptime_secs: u64,

    /// Current error count (all sources/sinks combined)
    pub errors_total: u64,

    /// Number of active connections across all sources
    pub connections_active: u64,
}

/// Feature usage flags - boolean indicators of what's being used.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FeatureUsage {
    /// Has used `tell tail` command
    pub tail_used: bool,

    /// Has used `tell query` command
    pub query_used: bool,

    /// Has transformers in pipeline
    pub transformers_enabled: bool,

    /// Number of boards created
    pub boards_count: u32,

    /// Number of custom metrics defined
    pub metrics_count: u32,

    /// Number of API keys created
    pub api_keys_count: u32,

    /// Number of workspaces (for multi-tenant deployments)
    pub workspaces_count: u32,

    /// TUI has been used
    pub tui_used: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deployment_info_collect() {
        let info = DeploymentInfo::collect();
        assert!(!info.version.is_empty());
        assert!(!info.os.is_empty());
        assert!(!info.arch.is_empty());
        assert!(info.cpu_cores >= 1);
    }

    #[test]
    fn test_payload_serialization() {
        let payload = TelemetryPayload::new(
            DeploymentInfo {
                install_id: "abc123".to_string(),
                version: "0.1.0".to_string(),
                os: "darwin".to_string(),
                arch: "aarch64".to_string(),
                cpu_cores: 12,
                memory_bytes: 32_000_000_000,
            },
            ConfigShape {
                sources: vec!["tcp".to_string()],
                sinks: vec!["clickhouse".to_string()],
                connectors: vec![],
                transformers: vec![],
                routing_rules: 1,
            },
            RuntimeMetrics {
                messages_total: 1_000_000,
                bytes_total: 500_000_000,
                uptime_secs: 3600,
                errors_total: 0,
                connections_active: 5,
            },
            FeatureUsage::default(),
        );

        let json = serde_json::to_string(&payload).unwrap();
        assert!(json.contains("\"schema_version\":1"));
        assert!(json.contains("\"messages_total\":1000000"));
    }
}
