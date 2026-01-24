//! Telemetry reporter - sends telemetry via FBS protocol.
//!
//! Primary: TCP connection to t.tell.rs (FBS protocol)
//! Fallback: HTTPS POST with FBS bytes if TCP fails
//!
//! The reporter runs in a dedicated task and never blocks the main pipeline.
//! It uses a bounded channel with try_send to ensure callers never wait.

use std::time::Duration;

use tokio::sync::mpsc;
use tracing::{debug, trace, warn};

use tell_client::batch::BatchBuilder;
use tell_client::event::{EventBuilder, EventDataBuilder};
use tell_client::test::TcpTestClient;

use crate::endpoint::{self, TELEMETRY_API_KEY};
use crate::error::TelemetryError;
use crate::payload::TelemetryPayload;

/// Channel buffer size - small since we batch anyway
const CHANNEL_BUFFER: usize = 8;

/// TCP connection timeout
const TCP_TIMEOUT: Duration = Duration::from_secs(5);

/// HTTP request timeout
const HTTP_TIMEOUT: Duration = Duration::from_secs(10);

/// Configuration for the telemetry reporter.
#[derive(Debug, Clone)]
pub struct ReporterConfig {
    /// How often to send telemetry (default: weekly)
    pub interval: Duration,

    /// Whether telemetry is enabled
    pub enabled: bool,
}

impl Default for ReporterConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(7 * 24 * 60 * 60), // 7 days
            enabled: true,                                   // Enabled by default (opt-out)
        }
    }
}

/// Handle for sending telemetry payloads to the reporter.
///
/// This is cheap to clone and can be passed to multiple components.
/// Sending never blocks - if the channel is full, the payload is dropped.
#[derive(Clone)]
pub struct ReporterHandle {
    tx: mpsc::Sender<ReporterCommand>,
}

impl ReporterHandle {
    /// Send a telemetry payload (non-blocking, fire-and-forget).
    ///
    /// Returns Ok(()) if queued successfully, Err if channel is full.
    /// Callers should not retry on error - just drop the payload.
    pub fn send(&self, payload: TelemetryPayload) -> Result<(), TelemetryError> {
        self.tx
            .try_send(ReporterCommand::Send(Box::new(payload)))
            .map_err(|_| TelemetryError::ChannelFull)
    }

    /// Request immediate flush (for graceful shutdown).
    pub fn flush(&self) -> Result<(), TelemetryError> {
        self.tx
            .try_send(ReporterCommand::Flush)
            .map_err(|_| TelemetryError::ChannelFull)
    }

    /// Shutdown the reporter.
    pub fn shutdown(&self) -> Result<(), TelemetryError> {
        self.tx
            .try_send(ReporterCommand::Shutdown)
            .map_err(|_| TelemetryError::ChannelFull)
    }
}

/// Commands sent to the reporter task.
enum ReporterCommand {
    Send(Box<TelemetryPayload>),
    Flush,
    Shutdown,
}

/// Telemetry reporter that runs in a background task.
pub struct Reporter {
    config: ReporterConfig,
    rx: mpsc::Receiver<ReporterCommand>,
    http_client: reqwest::Client,
    pending: Option<TelemetryPayload>,
}

impl Reporter {
    /// Create a new reporter and its handle.
    ///
    /// The reporter must be spawned as a task using `run()`.
    pub fn new(config: ReporterConfig) -> (Self, ReporterHandle) {
        let (tx, rx) = mpsc::channel(CHANNEL_BUFFER);

        let http_client = reqwest::Client::builder()
            .timeout(HTTP_TIMEOUT)
            .build()
            .unwrap_or_default();

        let reporter = Self {
            config,
            rx,
            http_client,
            pending: None,
        };

        let handle = ReporterHandle { tx };

        (reporter, handle)
    }

    /// Run the reporter loop.
    ///
    /// This should be spawned as a tokio task. It will run until shutdown
    /// is requested or the handle is dropped.
    pub async fn run(mut self) {
        if !self.config.enabled {
            debug!("Telemetry disabled, reporter exiting");
            return;
        }

        debug!(
            tcp = %endpoint::tcp_addr(),
            http = %endpoint::https_url(),
            interval_secs = self.config.interval.as_secs(),
            "Telemetry reporter started"
        );

        let mut interval = tokio::time::interval(self.config.interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    self.flush_pending().await;
                }
                cmd = self.rx.recv() => {
                    match cmd {
                        Some(ReporterCommand::Send(payload)) => {
                            // Keep only the latest payload (overwrites previous)
                            self.pending = Some(*payload);
                            trace!("Telemetry payload queued");
                        }
                        Some(ReporterCommand::Flush) => {
                            self.flush_pending().await;
                        }
                        Some(ReporterCommand::Shutdown) | None => {
                            debug!("Telemetry reporter shutting down");
                            self.flush_pending().await;
                            break;
                        }
                    }
                }
            }
        }
    }

    /// Flush the pending payload to the endpoint.
    async fn flush_pending(&mut self) {
        let Some(payload) = self.pending.take() else {
            return;
        };

        // Build FBS batch from payload
        let batch = match self.build_fbs_batch(&payload) {
            Ok(b) => b,
            Err(e) => {
                warn!(error = %e, "Failed to build telemetry batch");
                return;
            }
        };

        // Try TCP first
        match self.send_tcp(&batch).await {
            Ok(()) => {
                debug!("Telemetry sent via TCP");
                return;
            }
            Err(e) => {
                debug!(error = %e, "TCP failed, trying HTTP fallback");
            }
        }

        // Fall back to HTTP
        match self.send_http(&batch).await {
            Ok(()) => {
                debug!("Telemetry sent via HTTP fallback");
            }
            Err(e) => {
                // Both failed, put payload back for next interval
                warn!(error = %e, "Failed to send telemetry (both TCP and HTTP failed)");
                self.pending = Some(payload);
            }
        }
    }

    /// Build an FBS batch from the telemetry payload.
    fn build_fbs_batch(
        &self,
        payload: &TelemetryPayload,
    ) -> Result<tell_client::BuiltBatch, TelemetryError> {
        // Convert install_id hex string to 16 bytes
        let device_id = parse_install_id(&payload.deployment.install_id)?;

        // Serialize payload to JSON
        let payload_json = serde_json::to_string(payload)
            .map_err(|e| TelemetryError::Serialization(e.to_string()))?;

        // Build event
        let event = EventBuilder::new()
            .track("telemetry")
            .device_id(device_id)
            .timestamp_now()
            .payload_json(&payload_json)
            .build()
            .map_err(|e| TelemetryError::Protocol(e.to_string()))?;

        // Build event data
        let event_data = EventDataBuilder::new()
            .add(event)
            .build()
            .map_err(|e| TelemetryError::Protocol(e.to_string()))?;

        // Build batch
        BatchBuilder::new()
            .api_key(TELEMETRY_API_KEY)
            .event_data(event_data)
            .build()
            .map_err(|e| TelemetryError::Protocol(e.to_string()))
    }

    /// Send batch via TCP.
    async fn send_tcp(&self, batch: &tell_client::BuiltBatch) -> Result<(), TelemetryError> {
        let addr = endpoint::tcp_addr();

        // Connect with timeout
        let mut client = tokio::time::timeout(TCP_TIMEOUT, TcpTestClient::connect(&addr))
            .await
            .map_err(|_| TelemetryError::Network("TCP connect timeout".to_string()))?
            .map_err(|e| TelemetryError::Network(e.to_string()))?;

        // Send batch
        client
            .send(batch)
            .await
            .map_err(|e| TelemetryError::Network(e.to_string()))?;

        // Flush and close
        client
            .flush()
            .await
            .map_err(|e| TelemetryError::Network(e.to_string()))?;

        let _ = client.close().await;

        Ok(())
    }

    /// Send batch via HTTP (fallback).
    async fn send_http(&self, batch: &tell_client::BuiltBatch) -> Result<(), TelemetryError> {
        let url = endpoint::https_url();
        let data = batch.as_bytes();

        let response = self
            .http_client
            .post(&url)
            .header("Content-Type", "application/x-flatbuffers")
            .body(data.to_vec())
            .send()
            .await
            .map_err(|e| TelemetryError::Network(e.to_string()))?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(TelemetryError::Server(response.status().as_u16()))
        }
    }
}

/// Parse install_id hex string to 16 bytes.
fn parse_install_id(hex_str: &str) -> Result<[u8; 16], TelemetryError> {
    if hex_str.len() != 32 {
        return Err(TelemetryError::Protocol(format!(
            "install_id must be 32 hex chars, got {}",
            hex_str.len()
        )));
    }

    let mut bytes = [0u8; 16];
    for (i, chunk) in hex_str.as_bytes().chunks(2).enumerate() {
        let hex =
            std::str::from_utf8(chunk).map_err(|e| TelemetryError::Protocol(e.to_string()))?;
        bytes[i] =
            u8::from_str_radix(hex, 16).map_err(|e| TelemetryError::Protocol(e.to_string()))?;
    }

    Ok(bytes)
}

/// Spawn the reporter as a background task.
///
/// Returns a handle for sending telemetry. The task runs until
/// the handle is dropped or shutdown is called.
pub fn spawn(config: ReporterConfig) -> ReporterHandle {
    let (reporter, handle) = Reporter::new(config);
    tokio::spawn(reporter.run());
    handle
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ReporterConfig::default();
        assert!(config.enabled); // Enabled by default
        assert_eq!(config.interval, Duration::from_secs(7 * 24 * 60 * 60));
    }

    #[test]
    fn test_parse_install_id() {
        let hex = "aa96d5506919a7a9f2b7ebe70b7e9c5e";
        let bytes = parse_install_id(hex).unwrap();
        assert_eq!(bytes[0], 0xaa);
        assert_eq!(bytes[15], 0x5e);
    }

    #[test]
    fn test_parse_install_id_invalid_length() {
        let result = parse_install_id("abc");
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_reporter_disabled() {
        let config = ReporterConfig {
            enabled: false,
            ..Default::default()
        };

        let (reporter, _handle) = Reporter::new(config);

        // Should exit immediately when disabled
        tokio::time::timeout(Duration::from_millis(100), reporter.run())
            .await
            .expect("Reporter should exit quickly when disabled");
    }

    #[tokio::test]
    async fn test_handle_send_non_blocking() {
        let config = ReporterConfig {
            enabled: true,
            interval: Duration::from_secs(3600),
        };

        let (_reporter, handle) = Reporter::new(config);

        // Should not block even without running the reporter
        let payload = TelemetryPayload::new(
            crate::payload::DeploymentInfo::collect(),
            crate::payload::ConfigShape::default(),
            crate::payload::RuntimeMetrics::default(),
            crate::payload::FeatureUsage::default(),
        );

        // Fill the channel
        for _ in 0..CHANNEL_BUFFER {
            let _ = handle.send(payload.clone());
        }

        // Next send should fail with ChannelFull, not block
        let result = handle.send(payload);
        assert!(matches!(result, Err(TelemetryError::ChannelFull)));
    }
}
