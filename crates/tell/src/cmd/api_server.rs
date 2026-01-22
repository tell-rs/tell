//! API server startup
//!
//! Starts the analytics/dashboard API server alongside the collector.
//! Configurable via `[api_server]` in config.toml.

use std::sync::Arc;

use anyhow::{Context, Result};
use tell_analytics::MetricsEngine;
use tell_api::{AppState, RouterOptions, ServerMetrics, build_router_with_options};
use tell_auth::{LocalJwtProvider, LocalUserStore};
use tell_config::{Config, QueryBackend};
use tell_control::ControlPlane;
use tell_metrics::{PipelineSnapshot, SinkMetricsProvider, SourceMetricsProvider};
use tell_query::{ClickHouseBackendConfig, PolarsBackend};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;
use tracing::{error, info, warn};

/// Server metrics configuration for the API server
pub struct ServerMetricsConfig {
    /// Function to get pipeline snapshot
    pub pipeline_snapshot: Option<Box<dyn Fn() -> PipelineSnapshot + Send + Sync>>,
    /// Source metrics providers
    pub sources: Vec<Arc<dyn SourceMetricsProvider>>,
    /// Sink metrics providers
    pub sinks: Vec<Arc<dyn SinkMetricsProvider>>,
}

impl ServerMetricsConfig {
    /// Create new server metrics config
    pub fn new(
        pipeline_snapshot: Option<Box<dyn Fn() -> PipelineSnapshot + Send + Sync>>,
        sources: Vec<Arc<dyn SourceMetricsProvider>>,
        sinks: Vec<Arc<dyn SinkMetricsProvider>>,
    ) -> Self {
        Self {
            pipeline_snapshot,
            sources,
            sinks,
        }
    }
}

/// Start the API server with optional server metrics
///
/// When server_metrics is provided, the /health and /metrics endpoints
/// will return pipeline metrics. Otherwise, /metrics returns 503.
pub async fn start_api_server_with_metrics(
    config: &Config,
    cancel: CancellationToken,
    server_metrics: Option<ServerMetricsConfig>,
) -> Result<JoinHandle<()>> {
    let api_config = &config.api_server;

    if !api_config.enabled {
        info!("API server disabled");
        return Ok(tokio::spawn(async {}));
    }

    // Initialize control plane
    let control = init_control_plane(config).await?;

    // Initialize query engine
    let query_engine = init_query_engine(config)?;

    info!(backend = query_engine.name(), "query backend initialized");

    // Create metrics engine
    let metrics_engine = MetricsEngine::new(query_engine);

    // Initialize auth
    let (auth, user_store, jwt_secret) = init_auth(config).await?;

    // Build app state
    let mut state = AppState::with_local_auth(
        metrics_engine,
        auth,
        user_store,
        &jwt_secret,
        config.auth.jwt_expires_in,
    )
    .with_control(control);

    // Add server metrics if provided
    if let Some(metrics_config) = server_metrics {
        state = state.with_server_metrics(ServerMetrics::new(
            metrics_config.pipeline_snapshot,
            metrics_config.sources,
            metrics_config.sinks,
        ));
    }

    // Build router
    let router_options = RouterOptions {
        audit_logging: api_config.audit_logging,
    };

    let mut app = build_router_with_options(state, router_options);

    // Add middleware
    app = app.layer(TraceLayer::new_for_http()).layer(
        CorsLayer::new()
            .allow_origin(Any)
            .allow_methods(Any)
            .allow_headers(Any),
    );

    // Bind
    let addr = format!("{}:{}", api_config.host, api_config.port);
    let listener = TcpListener::bind(&addr)
        .await
        .context("failed to bind API server")?;

    info!(
        addr = %addr,
        audit_logging = api_config.audit_logging,
        "API server listening"
    );

    // Spawn server task
    let handle = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                cancel.cancelled().await;
            })
            .await
            .unwrap_or_else(|e| {
                error!(error = %e, "API server error");
            });
    });

    Ok(handle)
}

/// Initialize the control plane database
async fn init_control_plane(config: &Config) -> Result<ControlPlane> {
    let control_db_path = config.api_server.control_db_path();

    // ControlPlane::new expects data_dir, creates {data_dir}/control/data.db
    let data_dir = control_db_path
        .parent()
        .and_then(|p| p.to_str())
        .unwrap_or(".");

    // Ensure directory exists
    std::fs::create_dir_all(data_dir).context("failed to create control database directory")?;

    ControlPlane::new(data_dir)
        .await
        .context("failed to initialize control plane")
}

/// Initialize the query engine based on config
fn init_query_engine(config: &Config) -> Result<Box<dyn tell_query::QueryBackend>> {
    let has_clickhouse = config.sinks.iter().any(|(_, sink)| {
        matches!(
            sink,
            tell_config::SinkConfig::Clickhouse(_) | tell_config::SinkConfig::ClickhouseNative(_)
        )
    });

    let backend = config.query.effective_backend(has_clickhouse);

    match backend {
        QueryBackend::Clickhouse => {
            let (url, database, username, password) = resolve_clickhouse_config(config);

            let ch_config = ClickHouseBackendConfig::new(&url, &database)
                .with_credentials(&username, &password);

            Ok(Box::new(tell_query::ClickHouseBackend::new(&ch_config)))
        }
        QueryBackend::Polars => {
            let data_dir = config.query.data_dir();
            Ok(Box::new(PolarsBackend::new(&data_dir, 1)))
        }
    }
}

/// Resolve ClickHouse connection config from various sources
fn resolve_clickhouse_config(config: &Config) -> (String, String, String, String) {
    // Priority: explicit query config > referenced sink > first CH sink > defaults

    // Check explicit query config
    if config.query.clickhouse_url.is_some() {
        return (
            config.query.clickhouse_url(),
            config.query.clickhouse_database(),
            config.query.clickhouse_username.clone().unwrap_or_default(),
            config.query.clickhouse_password.clone().unwrap_or_default(),
        );
    }

    // Check referenced sink
    if let Some(ref sink_name) = config.query.clickhouse_sink {
        if let Some(ch) = get_clickhouse_sink_config(config, sink_name) {
            return ch;
        }
        warn!(sink = %sink_name, "referenced ClickHouse sink not found");
    }

    // Find first ClickHouse sink
    for (name, sink) in config.sinks.iter() {
        if let Some(ch) = extract_clickhouse_config(sink) {
            info!(sink = %name, "using ClickHouse sink config for queries");
            return ch;
        }
    }

    // Defaults
    (
        "http://localhost:8123".to_string(),
        "default".to_string(),
        "default".to_string(),
        String::new(),
    )
}

/// Get ClickHouse config from a named sink
fn get_clickhouse_sink_config(
    config: &Config,
    name: &str,
) -> Option<(String, String, String, String)> {
    config.sinks.get(name).and_then(extract_clickhouse_config)
}

/// Extract connection config from a ClickHouse sink
fn extract_clickhouse_config(
    sink: &tell_config::SinkConfig,
) -> Option<(String, String, String, String)> {
    match sink {
        tell_config::SinkConfig::Clickhouse(ch) => Some((
            format!("http://{}", ch.host),
            ch.database.clone(),
            ch.username.clone(),
            ch.password.clone(),
        )),
        tell_config::SinkConfig::ClickhouseNative(ch) => Some((
            format!("http://{}", ch.host),
            ch.database.clone(),
            ch.username.clone(),
            ch.password.clone(),
        )),
        _ => None,
    }
}

/// Initialize authentication
async fn init_auth(
    config: &Config,
) -> Result<(Arc<dyn tell_auth::AuthProvider>, LocalUserStore, Vec<u8>)> {
    // Generate or use configured JWT secret
    let jwt_secret: Vec<u8> = config
        .auth
        .jwt_secret
        .as_ref()
        .map(|s| s.as_bytes().to_vec())
        .unwrap_or_else(|| {
            warn!("no JWT secret configured, generating random secret");
            let secret: [u8; 32] = rand::random();
            secret.to_vec()
        });

    let auth = Arc::new(LocalJwtProvider::new(&jwt_secret));

    // Open user store
    let db_path = &config.auth.local.db_path;

    // Ensure parent directory exists
    if let Some(parent) = db_path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent).ok();
    }

    let user_store = LocalUserStore::open(db_path)
        .await
        .context("failed to open user store")?;

    Ok((auth, user_store, jwt_secret))
}
