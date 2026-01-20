//! HTTP Source - REST API for data ingestion
//!
//! Provides HTTP/REST endpoints for ingesting events and logs.
//! Supports both JSONL (for JS/TS SDKs) and binary FlatBuffer (high-performance).
//!
//! # Endpoints
//!
//! - `POST /v1/events` - Ingest analytics events (JSONL)
//! - `POST /v1/logs` - Ingest structured logs (JSONL)
//! - `POST /v1/ingest` - Binary FlatBuffer ingestion (zero-copy, ~40M events/sec)
//! - `GET /health` - Health check
//!
//! # Protocols
//!
//! ## JSONL (JSON Lines)
//!
//! ```text
//! POST /v1/events
//! Content-Type: application/json
//! X-API-Key: <hex key>
//!
//! {"type":"track","event":"page_view","device_id":"..."}
//! {"type":"track","event":"button_click","device_id":"..."}
//! ```
//!
//! ## Binary FlatBuffer
//!
//! ```text
//! POST /v1/ingest
//! Content-Type: application/x-flatbuffers
//! X-API-Key: <hex key>
//!
//! <raw Batch FlatBuffer bytes>
//! ```
//!
//! # Authentication
//!
//! API key via `Authorization: Bearer <key>` or `X-API-Key: <key>` header.
//!
//! # Example
//!
//! ```ignore
//! use tell_sources::http::{HttpSource, HttpSourceConfig};
//! use tell_auth::ApiKeyStore;
//! use std::sync::Arc;
//!
//! let config = HttpSourceConfig::with_port(8080);
//! let auth_store = Arc::new(ApiKeyStore::new());
//! let batch_sender = ShardedSender::new(vec![tx], 1);
//!
//! let source = HttpSource::new(config, auth_store, batch_sender);
//! source.run(cancel_token).await?;
//! ```

mod auth;
mod config;
mod encoder;
mod error;
mod handlers;
mod json_types;
mod jsonl;
mod metrics;
mod response;

#[cfg(test)]
mod http_test;

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use axum::routing::{get, post};
use axum::Router;
use tell_auth::ApiKeyStore;
use tell_protocol::SourceId;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

pub use config::HttpSourceConfig;
pub use error::HttpSourceError;
pub use metrics::{HttpMetricsSnapshot, HttpSourceMetrics, HttpSourceMetricsHandle};

use handlers::{HandlerState, health_check, ingest_binary, ingest_events, ingest_logs};

use crate::ShardedSender;

/// HTTP source for REST API ingestion
pub struct HttpSource {
    config: HttpSourceConfig,
    auth_store: Arc<ApiKeyStore>,
    batch_sender: ShardedSender,
    metrics: Arc<HttpSourceMetrics>,
    running: Arc<AtomicBool>,
}

impl HttpSource {
    /// Create a new HTTP source
    pub fn new(
        config: HttpSourceConfig,
        auth_store: Arc<ApiKeyStore>,
        batch_sender: ShardedSender,
    ) -> Self {
        Self {
            config,
            auth_store,
            batch_sender,
            metrics: Arc::new(HttpSourceMetrics::new()),
            running: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Get reference to metrics
    pub fn metrics(&self) -> &HttpSourceMetrics {
        &self.metrics
    }

    /// Get a metrics handle for reporting
    pub fn metrics_handle(&self) -> HttpSourceMetricsHandle {
        HttpSourceMetricsHandle::new(self.config.id.clone(), Arc::clone(&self.metrics))
    }

    /// Get the source ID
    pub fn source_id(&self) -> SourceId {
        self.config.source_id()
    }

    /// Check if the source is running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    /// Run the HTTP source
    ///
    /// Binds to the configured address and starts accepting HTTP requests.
    /// Returns when cancelled or an unrecoverable error occurs.
    pub async fn run(self, cancel: CancellationToken) -> Result<(), HttpSourceError> {
        let bind_addr = self.config.bind_address();

        let listener = TcpListener::bind(&bind_addr)
            .await
            .map_err(|e| HttpSourceError::Bind {
                address: bind_addr.clone(),
                source: e,
            })?;

        self.running.store(true, Ordering::Relaxed);

        tracing::info!(
            source_id = %self.config.id,
            address = %bind_addr,
            "HTTP source listening"
        );

        // Build shared handler state
        let state = Arc::new(HandlerState {
            auth_store: Arc::clone(&self.auth_store),
            batch_sender: self.batch_sender.clone(),
            source_id: self.config.source_id(),
            metrics: Arc::clone(&self.metrics),
            max_payload_size: self.config.max_payload_size,
        });

        // Build router with connect info for source IP tracking
        let app = build_router(state).into_make_service_with_connect_info::<SocketAddr>();

        // Run server with graceful shutdown
        let server = axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal(cancel.clone()));

        let result = server.await.map_err(|e| HttpSourceError::Http(e.to_string()));

        self.running.store(false, Ordering::Relaxed);

        tracing::info!(
            source_id = %self.config.id,
            "HTTP source stopped"
        );

        result
    }

    /// Stop the source
    pub fn stop(&self) {
        self.running.store(false, Ordering::Relaxed);
    }
}

/// Build the axum router
fn build_router(state: Arc<HandlerState>) -> Router {
    Router::new()
        .route("/v1/events", post(ingest_events))
        .route("/v1/logs", post(ingest_logs))
        .route("/v1/ingest", post(ingest_binary))
        .route("/health", get(health_check))
        .with_state(state)
}

/// Shutdown signal future
async fn shutdown_signal(cancel: CancellationToken) {
    cancel.cancelled().await;
}
