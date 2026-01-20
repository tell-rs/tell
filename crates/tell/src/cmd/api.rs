//! API command - Run the Tell analytics API server
//!
//! # Usage
//!
//! ```bash
//! # Run API server with default settings
//! tell api
//!
//! # Run on a specific port
//! tell api --port 8080
//!
//! # Use ClickHouse backend
//! tell api --backend clickhouse --clickhouse-url http://localhost:8123
//! ```

use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Args;
use tokio::net::TcpListener;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;
use tracing::info;

use tell_analytics::MetricsEngine;
use tell_api::{build_router, AppState};
use tell_query::{QueryBackend, QueryBackendType, QueryEngine, ResolvedQueryConfig};

/// API server command arguments
#[derive(Args, Debug)]
pub struct ApiArgs {
    /// Host to bind to
    #[arg(long, default_value = "0.0.0.0")]
    pub host: String,

    /// Port to listen on
    #[arg(short, long, default_value = "3000")]
    pub port: u16,

    /// Query backend (clickhouse, polars)
    #[arg(long, default_value = "polars")]
    pub backend: String,

    /// ClickHouse URL (when backend=clickhouse)
    #[arg(long)]
    pub clickhouse_url: Option<String>,

    /// ClickHouse database (when backend=clickhouse)
    #[arg(long, default_value = "default")]
    pub clickhouse_database: String,

    /// Parquet data directory (when backend=polars)
    #[arg(long)]
    pub data_dir: Option<PathBuf>,

    /// Enable CORS for all origins
    #[arg(long)]
    pub cors: bool,
}

/// Run the API server
pub async fn run(args: ApiArgs) -> Result<()> {
    // Build query config
    let resolved = build_query_config(&args)?;

    // Create query engine
    let query_engine = QueryEngine::from_resolved_config(&resolved)
        .context("failed to create query engine")?;

    info!(
        backend = query_engine.name(),
        "Query backend initialized"
    );

    // Create metrics engine
    let metrics = MetricsEngine::new(Box::new(query_engine));

    // Create app state
    let state = AppState::new(metrics);

    // Build router
    let mut app = build_router(state);

    // Add middleware
    app = app.layer(TraceLayer::new_for_http());

    if args.cors {
        app = app.layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        );
    }

    // Bind and serve
    let addr: SocketAddr = format!("{}:{}", args.host, args.port)
        .parse()
        .context("invalid bind address")?;

    let listener = TcpListener::bind(addr)
        .await
        .context("failed to bind to address")?;

    info!(
        address = %addr,
        cors = args.cors,
        "Tell API server starting"
    );

    axum::serve(listener, app)
        .await
        .context("server error")?;

    Ok(())
}

fn build_query_config(args: &ApiArgs) -> Result<ResolvedQueryConfig> {
    match args.backend.to_lowercase().as_str() {
        "clickhouse" | "ch" => {
            let url = args
                .clickhouse_url
                .clone()
                .unwrap_or_else(|| "http://localhost:8123".to_string());

            Ok(ResolvedQueryConfig {
                backend: QueryBackendType::ClickHouse,
                url: Some(url),
                database: Some(args.clickhouse_database.clone()),
                username: None,
                password: None,
                path: None,
                workspace_id: 1,
            })
        }
        "polars" | "local" => {
            let data_dir = args
                .data_dir
                .clone()
                .unwrap_or_else(|| PathBuf::from("./data"));

            Ok(ResolvedQueryConfig {
                backend: QueryBackendType::Local,
                url: None,
                database: None,
                username: None,
                password: None,
                path: Some(data_dir),
                workspace_id: 1,
            })
        }
        _ => Err(anyhow::anyhow!(
            "unknown backend: {}. Use 'clickhouse' or 'polars'",
            args.backend
        )),
    }
}
