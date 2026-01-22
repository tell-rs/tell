//! Serve command - Run the Tell server
//!
//! High-performance, multi-protocol data streaming engine.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Args;
use tokio::signal;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use tell_auth::ApiKeyStore;
use tell_config::{Config, RoutingConfig};
#[cfg(feature = "connector-github")]
use tell_connectors::GitHubConnectorConfig;
#[cfg(feature = "connector-shopify")]
use tell_connectors::ShopifyConnectorConfig;
use tell_connectors::{ConnectorScheduler, ScheduledConnector};
use tell_metrics::{
    PipelineMetricsProvider, SinkMetricsProvider, SourceMetricsProvider, UnifiedReporter,
};

use crate::cmd::api_server::{ServerMetricsConfig, start_api_server_with_metrics};
use tell_pipeline::{Router, RouterMetricsHandle, SinkHandle};
use tell_protocol::Batch;
use tell_routing::{RoutingTable, SinkId};
use tell_sinks::arrow_ipc::{ArrowIpcConfig, ArrowIpcSink};
use tell_sinks::clickhouse::{ArrowClickHouseSink, ClickHouseConfig, ClickHouseSink};
use tell_sinks::disk_binary::{DiskBinaryConfig, DiskBinarySink};
use tell_sinks::disk_plaintext::{DiskPlaintextConfig, DiskPlaintextSink};
use tell_sinks::forwarder::{ForwarderConfig, ForwarderSink};
use tell_sinks::null::NullSink;
use tell_sinks::parquet::{ParquetConfig, ParquetSink};
use tell_sinks::stdout::{StdoutConfig, StdoutSink};
use tell_sources::tcp::{TcpSource, TcpSourceConfig};
use tell_sources::tcp_debug::{TcpDebugSource, TcpDebugSourceConfig};
use tell_sources::{
    HttpSource, HttpSourceConfig, ShardedSender, SyslogTcpSource, SyslogTcpSourceConfig,
    SyslogUdpSource, SyslogUdpSourceConfig,
};
use tell_tap::{TapPoint, TapServer, TapServerConfig};

use std::collections::HashMap;

use crate::transformer_builder;

/// Serve command arguments
#[derive(Args, Debug)]
pub struct ServeArgs {
    /// Path to configuration file (defaults to configs/config.toml if not specified)
    #[arg(short, long)]
    pub config: Option<PathBuf>,
}

/// Run the serve command
pub async fn run(args: ServeArgs) -> Result<()> {
    let config_path = args
        .config
        .as_ref()
        .map(|p| p.display().to_string())
        .unwrap_or_else(|| "(default)".to_string());

    info!(
        version = env!("CARGO_PKG_VERSION"),
        platform = std::env::consts::OS,
        arch = std::env::consts::ARCH,
        config = %config_path,
        "Tell starting"
    );

    // Load configuration
    let config = match args.config {
        Some(path) => {
            // User explicitly provided config path - must exist
            if !path.exists() {
                return Err(anyhow::anyhow!(
                    "config file not found: {}\n\nTo create a default config, run: tell init",
                    path.display()
                ));
            }
            Config::from_file(&path).context("failed to load configuration")?
        }
        None => {
            // No config provided - try default paths, fall back to defaults
            let default_paths = [
                PathBuf::from("configs/config.toml"),
                PathBuf::from("config.toml"),
            ];

            let mut loaded = None;
            for path in &default_paths {
                if path.exists() {
                    info!(config = %path.display(), "using config file");
                    loaded = Some(Config::from_file(path).context("failed to load configuration")?);
                    break;
                }
            }

            loaded.unwrap_or_else(|| {
                info!("no config file found, using defaults (TCP on port 50000 → stdout)");
                Config::default()
            })
        }
    };

    // Run the server
    if let Err(e) = run_server(config).await {
        error!(error = %e, "server error");
        return Err(e);
    }

    info!("Tell shutdown complete");
    Ok(())
}

/// Collected metrics handles for reporting
struct MetricsHandles {
    router: Option<RouterMetricsHandle>,
    sources: Vec<Arc<dyn SourceMetricsProvider>>,
    sinks: Vec<Arc<dyn SinkMetricsProvider>>,
}

/// Main server run loop
async fn run_server(config: Config) -> Result<()> {
    // Create cancellation token for coordinated shutdown
    let cancel = CancellationToken::new();

    // Initialize API key store
    let auth_store = Arc::new(ApiKeyStore::new());

    // Load API keys if configured
    let api_keys_file = &config.global.api_keys_file;
    if !api_keys_file.is_empty() {
        load_or_generate_api_keys(&auth_store, api_keys_file)?;
    } else {
        warn!("no API keys file configured, authentication will reject all requests");
    }

    // Spawn SIGHUP handler for API key hot reload
    #[cfg(unix)]
    if !api_keys_file.is_empty() {
        let auth_store_for_reload = Arc::clone(&auth_store);
        let api_keys_path = api_keys_file.clone();
        tokio::spawn(async move {
            let mut sig = signal::unix::signal(signal::unix::SignalKind::hangup())
                .expect("failed to install SIGHUP handler");
            while sig.recv().await.is_some() {
                match auth_store_for_reload.reload(&api_keys_path) {
                    Ok(n) => info!(keys = n, "SIGHUP: reloaded API keys"),
                    Err(e) => error!(error = %e, "SIGHUP: failed to reload API keys"),
                }
            }
        });
        info!("SIGHUP handler installed for API key hot reload");
    }

    // Build routing table from config
    let (routing_table, sink_ids) = build_routing_table(&config.routing, &config.sinks);

    info!(
        sink_count = routing_table.sink_count(),
        route_count = routing_table.route_count(),
        "routing table built"
    );

    // Create pipeline router
    let mut router = Router::new(routing_table);

    // Get router metrics handle before running
    let router_metrics_handle = router.metrics_handle();

    // Create and configure tap point for live streaming (always enabled, zero-cost when idle)
    let tap_point = Arc::new(TapPoint::new());
    router.set_tap_point(Arc::clone(&tap_point));

    // Start tap maintenance task (rate limit resets, cleanup)
    let tap_maintenance = tap_point.spawn_maintenance();

    // Build and register transformer chains from routing rules
    let source_transformers = transformer_builder::build_source_transformers(&config.routing)
        .context("failed to build transformer chains")?;

    for (source_id, chain) in source_transformers {
        router.set_source_transformers(source_id, chain);
    }

    if router.has_transformers() {
        info!(
            source_transformer_count = router.source_transformer_count(),
            "transformer chains configured"
        );
    }

    // Create sharded channels for sources to send batches to router workers
    // Number of shards from config (default: auto = num_cpus)
    let num_shards = config.global.effective_router_workers();
    let queue_size_per_shard = config.global.queue_size / num_shards.max(1);

    let mut senders = Vec::with_capacity(num_shards);
    let mut receivers = Vec::with_capacity(num_shards);

    for _ in 0..num_shards {
        let (tx, rx) = crossfire::mpsc::bounded_async::<Batch>(queue_size_per_shard.max(100));
        senders.push(tx);
        receivers.push(rx);
    }

    let sharded_sender = ShardedSender::new(senders);

    info!(
        router_workers = num_shards,
        queue_size_per_shard = queue_size_per_shard,
        configured = config.global.router_workers.is_some(),
        "created sharded channels for router workers"
    );

    // Collect metrics handles
    let mut metrics_handles = MetricsHandles {
        router: Some(router_metrics_handle),
        sources: Vec::new(),
        sinks: Vec::new(),
    };

    // Create and register sinks (collect metrics handles)
    let sink_tasks =
        create_and_register_sinks(&mut router, &sink_ids, &config, &mut metrics_handles.sinks)
            .await;

    info!(sink_count = router.sink_count(), "sinks registered");

    // Start tap server for live streaming (Unix socket)
    let tap_server_config = TapServerConfig::default();
    let tap_socket_path = tap_server_config.socket_path.clone();
    let tap_server = TapServer::new(Arc::clone(&tap_point), tap_server_config);
    let tap_server_task = tap_server.spawn();

    info!(
        socket = %tap_socket_path.display(),
        "tap server started (use `tell tail` to connect)"
    );

    // Spawn router workers (one per shard)
    let router_tasks = router.run_sharded(receivers);

    // Start all configured sources (collect metrics handles)
    let source_tasks = start_sources(
        &config,
        Arc::clone(&auth_store),
        sharded_sender.clone(),
        &mut metrics_handles.sources,
        cancel.clone(),
    )
    .await?;

    // Start connector scheduler if connectors are configured
    let connector_task = start_connector_scheduler(&config, sharded_sender).await;

    // Create server metrics config for API server
    let server_metrics = {
        let router_handle = metrics_handles.router.clone();
        let pipeline_snapshot: Option<
            Box<dyn Fn() -> tell_metrics::PipelineSnapshot + Send + Sync>,
        > = router_handle.map(|h| {
            Box::new(move || h.pipeline_snapshot())
                as Box<dyn Fn() -> tell_metrics::PipelineSnapshot + Send + Sync>
        });

        ServerMetricsConfig::new(
            pipeline_snapshot,
            metrics_handles.sources.clone(),
            metrics_handles.sinks.clone(),
        )
    };

    // Start API server with server metrics (provides /health and /metrics endpoints)
    let api_server_task =
        start_api_server_with_metrics(&config, cancel.clone(), Some(server_metrics))
            .await
            .context("failed to start API server")?;

    // Start metrics reporter if enabled
    let metrics_task = if config.metrics.enabled {
        let reporter = build_metrics_reporter(&config, metrics_handles);
        let cancel_clone = cancel.clone();
        Some(tokio::spawn(async move {
            reporter.run(cancel_clone).await;
        }))
    } else {
        info!("metrics reporting disabled");
        None
    };

    info!(
        source_count = source_tasks.len(),
        connector_count = config.connectors.len(),
        metrics_enabled = config.metrics.enabled,
        api_port = config.api_server.port,
        "Tell server running"
    );

    // Wait for shutdown signal
    wait_for_shutdown().await;

    info!("shutdown signal received, stopping server...");

    // Signal all components to stop via cancellation token
    cancel.cancel();

    let shutdown_timeout = tokio::time::Duration::from_secs(config.global.shutdown_timeout_secs);

    // Wait for sources to finish gracefully (they will stop accepting new connections
    // and flush any in-progress batches)
    info!("waiting for sources to shut down gracefully...");
    for task in source_tasks {
        // Give each source a reasonable time to finish
        match tokio::time::timeout(shutdown_timeout, task).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                warn!(error = %e, "source task panicked during shutdown");
            }
            Err(_) => {
                warn!("source task did not finish within timeout, continuing shutdown");
            }
        }
    }

    // Wait for router workers to finish draining (they close sink channels when done)
    info!("waiting for router workers to drain...");
    for task in router_tasks {
        match tokio::time::timeout(shutdown_timeout, task).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => warn!(error = %e, "router worker panicked"),
            Err(_) => warn!("router worker did not finish within timeout"),
        }
    }

    // Wait for sinks to drain remaining batches
    info!("waiting for sinks to flush...");
    for task in sink_tasks {
        match tokio::time::timeout(shutdown_timeout, task).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => warn!(error = %e, "sink task panicked"),
            Err(_) => warn!("sink did not flush within timeout"),
        }
    }

    // Clean up remaining tasks
    tap_server_task.abort();
    tap_maintenance.abort();
    api_server_task.abort();
    if let Some(task) = metrics_task {
        task.abort();
    }
    if let Some(task) = connector_task {
        task.abort();
    }

    // Clean up tap socket
    if tap_socket_path.exists() {
        let _ = std::fs::remove_file(&tap_socket_path);
    }

    Ok(())
}

/// Build the unified metrics reporter with all component handles
fn build_metrics_reporter(config: &Config, handles: MetricsHandles) -> UnifiedReporter {
    let mut builder = UnifiedReporter::builder().config(config.metrics.clone());

    // Add pipeline metrics
    if let Some(router_handle) = handles.router {
        builder = builder.pipeline(Arc::new(router_handle));
    }

    // Add source metrics
    builder = builder.sources(handles.sources);

    // Add sink metrics
    builder = builder.sinks(handles.sinks);

    builder.build()
}

/// Registered sink IDs for routing (name -> SinkId)
type SinkIds = HashMap<String, SinkId>;

/// Build routing table from configuration
/// Registers all sinks referenced in routing config
fn build_routing_table(
    routing_config: &RoutingConfig,
    sinks_config: &tell_config::SinksConfig,
) -> (RoutingTable, SinkIds) {
    use std::collections::HashSet;

    let mut table = RoutingTable::new();

    // Collect all sink names referenced in routing
    let mut referenced_sinks: HashSet<&str> = HashSet::new();

    // From default routing
    for name in &routing_config.default {
        referenced_sinks.insert(name.as_str());
    }

    // From routing rules
    for rule in &routing_config.rules {
        for name in &rule.sinks {
            referenced_sinks.insert(name.as_str());
        }
    }

    // If nothing referenced, default to stdout
    if referenced_sinks.is_empty() {
        referenced_sinks.insert("stdout");
    }

    // Register all referenced sinks
    let mut sink_ids: SinkIds = HashMap::new();

    for &sink_name in &referenced_sinks {
        // Check if sink exists in config or is a built-in type
        let exists =
            sinks_config.contains(sink_name) || sink_name == "null" || sink_name == "stdout";

        if exists {
            let id = table.register_sink(sink_name);
            sink_ids.insert(sink_name.to_string(), id);
            info!(sink = %sink_name, "registered sink");
        } else {
            warn!(sink = %sink_name, "sink referenced in routing but not configured, skipping");
        }
    }

    // Helper to resolve sink name to ID
    let resolve_sink = |name: &str| -> Option<SinkId> { sink_ids.get(name).copied() };

    // Set up routing based on config
    if routing_config.has_default() {
        let default_sinks: Vec<SinkId> = routing_config
            .default
            .iter()
            .filter_map(|name| resolve_sink(name))
            .collect();
        table.set_default(default_sinks);
    } else {
        // No default configured - use stdout as default if registered
        if let Some(&stdout_id) = sink_ids.get("stdout") {
            table.set_default(vec![stdout_id]);
        }
    }

    // Add routing rules from config
    for rule in &routing_config.rules {
        let sinks: Vec<SinkId> = rule
            .sinks
            .iter()
            .filter_map(|name| resolve_sink(name))
            .collect();

        if !sinks.is_empty() {
            // Add route based on match condition
            if let Some(ref source_name) = rule.match_condition.source {
                table.add_route(tell_protocol::SourceId::new(source_name), sinks.clone());
                info!(
                    source = %source_name,
                    sinks = ?rule.sinks,
                    "added routing rule"
                );
            }

            // Handle source_type matches by adding routes for known source types
            if let Some(ref source_type) = rule.match_condition.source_type {
                match source_type.as_str() {
                    "tcp" => {
                        // TCP type sources
                        table.add_route(tell_protocol::SourceId::new("tcp_main"), sinks.clone());
                        table.add_route(tell_protocol::SourceId::new("tcp_debug"), sinks.clone());
                    }
                    "syslog" => {
                        // Syslog type sources
                        table.add_route(tell_protocol::SourceId::new("syslog_tcp"), sinks.clone());
                        table.add_route(tell_protocol::SourceId::new("syslog_udp"), sinks.clone());
                    }
                    _ => {
                        warn!(source_type = %source_type, "unknown source type in routing rule");
                    }
                }
                info!(
                    source_type = %source_type,
                    sinks = ?rule.sinks,
                    "added source type routing rule"
                );
            }
        }
    }

    (table, sink_ids)
}

/// Create and register sinks with the router
async fn create_and_register_sinks(
    router: &mut Router,
    sink_ids: &SinkIds,
    config: &Config,
    sink_metrics: &mut Vec<Arc<dyn SinkMetricsProvider>>,
) -> Vec<JoinHandle<()>> {
    let mut tasks = Vec::new();

    for (sink_name, &sink_id) in sink_ids {
        let queue_size = config.global.queue_size;

        // Get sink config if it exists
        let sink_config = config.sinks.get(sink_name);

        match (sink_name.as_str(), sink_config) {
            // Built-in null sink (or explicit null config)
            ("null", _) | (_, Some(tell_config::SinkConfig::Null(_))) => {
                let (tx, rx) = mpsc::channel(queue_size);
                let handle = SinkHandle::new(sink_id, sink_name, tx);
                router.register_sink(handle);

                let sink = NullSink::with_name(rx, sink_name);
                sink_metrics.push(Arc::new(sink.metrics_handle()));

                let name = sink_name.clone();
                tasks.push(tokio::spawn(async move {
                    let metrics = sink.run().await;
                    info!(
                        sink = %name,
                        batches = metrics.batches_received,
                        messages = metrics.messages_received,
                        "null sink finished"
                    );
                }));
            }

            // Built-in stdout sink (or explicit stdout config)
            ("stdout", _) | (_, Some(tell_config::SinkConfig::Stdout(_))) => {
                let (tx, rx) = mpsc::channel(queue_size);
                let handle = SinkHandle::new(sink_id, sink_name, tx);
                router.register_sink(handle);

                let sink = StdoutSink::with_name_and_config(rx, sink_name, StdoutConfig::default());
                sink_metrics.push(Arc::new(sink.metrics_handle()));

                let name = sink_name.clone();
                tasks.push(tokio::spawn(async move {
                    let snapshot = sink.run().await;
                    info!(
                        sink = %name,
                        batches = snapshot.batches_received,
                        messages = snapshot.messages_received,
                        "stdout sink finished"
                    );
                }));
            }

            // ClickHouse Arrow sink (HTTP protocol, high throughput)
            (_, Some(tell_config::SinkConfig::Clickhouse(ch_config))) => {
                if !ch_config.enabled {
                    info!(sink = %sink_name, "clickhouse sink disabled, skipping");
                    continue;
                }

                let (tx, rx) = mpsc::channel(queue_size);
                let handle = SinkHandle::new(sink_id, sink_name, tx);
                router.register_sink(handle);

                // Build ClickHouse config from tell_config
                let clickhouse_config = ClickHouseConfig::default()
                    .with_url(format!("http://{}", ch_config.host))
                    .with_database(&ch_config.database)
                    .with_credentials(&ch_config.username, &ch_config.password)
                    .with_batch_size(ch_config.batch_size)
                    .with_flush_interval(ch_config.flush_interval);

                let sink = ArrowClickHouseSink::with_name(clickhouse_config, rx, sink_name);
                sink_metrics.push(Arc::new(sink.metrics_handle()));

                let name = sink_name.clone();
                tasks.push(tokio::spawn(async move {
                    match sink.run().await {
                        Ok(snapshot) => {
                            info!(
                                sink = %name,
                                batches = snapshot.batches_received,
                                events = snapshot.events_written,
                                logs = snapshot.logs_written,
                                errors = snapshot.write_errors,
                                "clickhouse arrow sink finished"
                            );
                        }
                        Err(e) => {
                            error!(sink = %name, error = %e, "clickhouse arrow sink error");
                        }
                    }
                }));
            }

            // ClickHouse Native sink (native protocol via clickhouse crate)
            (_, Some(tell_config::SinkConfig::ClickhouseNative(ch_config))) => {
                if !ch_config.enabled {
                    info!(sink = %sink_name, "clickhouse_native sink disabled, skipping");
                    continue;
                }

                let (tx, rx) = mpsc::channel(queue_size);
                let handle = SinkHandle::new(sink_id, sink_name, tx);
                router.register_sink(handle);

                // Build ClickHouse config from tell_config (native uses http:// prefix still for crate)
                let clickhouse_config = ClickHouseConfig::default()
                    .with_url(format!("http://{}", ch_config.host))
                    .with_database(&ch_config.database)
                    .with_credentials(&ch_config.username, &ch_config.password)
                    .with_batch_size(ch_config.batch_size)
                    .with_flush_interval(ch_config.flush_interval);

                let sink = ClickHouseSink::with_name(clickhouse_config, rx, sink_name);
                sink_metrics.push(Arc::new(sink.metrics_handle()));

                let name = sink_name.clone();
                tasks.push(tokio::spawn(async move {
                    match sink.run().await {
                        Ok(snapshot) => {
                            info!(
                                sink = %name,
                                batches = snapshot.batches_received,
                                events = snapshot.events_written,
                                logs = snapshot.logs_written,
                                errors = snapshot.write_errors,
                                "clickhouse_native sink finished"
                            );
                        }
                        Err(e) => {
                            error!(sink = %name, error = %e, "clickhouse_native sink error");
                        }
                    }
                }));
            }

            // Disk Binary sink
            (_, Some(tell_config::SinkConfig::DiskBinary(disk_config))) => {
                if !disk_config.enabled {
                    info!(sink = %sink_name, "disk_binary sink disabled, skipping");
                    continue;
                }

                let (tx, rx) = mpsc::channel(queue_size);
                let handle = SinkHandle::new(sink_id, sink_name, tx);
                router.register_sink(handle);

                let sink_config = DiskBinaryConfig {
                    id: sink_name.clone(),
                    path: disk_config.path.clone().into(),
                    buffer_size: disk_config.buffer_size,
                    compression: matches!(disk_config.compression, tell_config::Compression::Lz4),
                    rotation_interval: match disk_config.rotation {
                        tell_config::RotationInterval::Hourly => {
                            tell_sinks::util::RotationInterval::Hourly
                        }
                        tell_config::RotationInterval::Daily => {
                            tell_sinks::util::RotationInterval::Daily
                        }
                    },
                    ..Default::default()
                };

                let sink = DiskBinarySink::new(sink_config, rx);
                sink_metrics.push(Arc::new(sink.metrics_handle()));

                let name = sink_name.clone();
                let path = disk_config.path.clone();
                tasks.push(tokio::spawn(async move {
                    let snapshot = sink.run().await;
                    info!(
                        sink = %name,
                        path = %path,
                        batches = snapshot.batches_received,
                        bytes = snapshot.bytes_written,
                        "disk_binary sink finished"
                    );
                }));
            }

            // Disk Plaintext sink
            (_, Some(tell_config::SinkConfig::DiskPlaintext(disk_config))) => {
                if !disk_config.enabled {
                    info!(sink = %sink_name, "disk_plaintext sink disabled, skipping");
                    continue;
                }

                let (tx, rx) = mpsc::channel(queue_size);
                let handle = SinkHandle::new(sink_id, sink_name, tx);
                router.register_sink(handle);

                let sink_config = DiskPlaintextConfig {
                    path: disk_config.path.clone().into(),
                    buffer_size: disk_config.buffer_size,
                    queue_size: disk_config.write_queue_size,
                    compression: matches!(disk_config.compression, tell_config::Compression::Lz4),
                    rotation_interval: match disk_config.rotation {
                        tell_config::RotationInterval::Hourly => {
                            tell_sinks::util::RotationInterval::Hourly
                        }
                        tell_config::RotationInterval::Daily => {
                            tell_sinks::util::RotationInterval::Daily
                        }
                    },
                    flush_interval: disk_config.flush_interval,
                    ..Default::default()
                };

                let sink = DiskPlaintextSink::new(sink_config, rx);
                sink_metrics.push(Arc::new(sink.metrics_handle()));

                let name = sink_name.clone();
                let path = disk_config.path.clone();
                tasks.push(tokio::spawn(async move {
                    let snapshot = sink.run().await;
                    info!(
                        sink = %name,
                        path = %path,
                        batches = snapshot.batches_received,
                        bytes = snapshot.bytes_written,
                        "disk_plaintext sink finished"
                    );
                }));
            }

            // Parquet sink
            (_, Some(tell_config::SinkConfig::Parquet(pq_config))) => {
                if !pq_config.enabled {
                    info!(sink = %sink_name, "parquet sink disabled, skipping");
                    continue;
                }

                let (tx, rx) = mpsc::channel(queue_size);
                let handle = SinkHandle::new(sink_id, sink_name, tx);
                router.register_sink(handle);

                let mut sink_config = ParquetConfig::default().with_path(&pq_config.path);

                sink_config.rotation_interval = match pq_config.rotation {
                    tell_config::RotationInterval::Hourly => {
                        tell_sinks::parquet::RotationInterval::Hourly
                    }
                    tell_config::RotationInterval::Daily => {
                        tell_sinks::parquet::RotationInterval::Daily
                    }
                };

                sink_config.compression = match pq_config.compression {
                    tell_config::ParquetCompression::Snappy => {
                        tell_sinks::parquet::Compression::Snappy
                    }
                    tell_config::ParquetCompression::Zstd => tell_sinks::parquet::Compression::Zstd,
                    tell_config::ParquetCompression::Lz4 => tell_sinks::parquet::Compression::Lz4,
                    tell_config::ParquetCompression::Uncompressed => {
                        tell_sinks::parquet::Compression::None
                    }
                };

                let sink = ParquetSink::new(sink_config, rx);
                sink_metrics.push(Arc::new(sink.metrics_handle()));

                let name = sink_name.clone();
                let path = pq_config.path.clone();
                tasks.push(tokio::spawn(async move {
                    match sink.run().await {
                        Ok(snapshot) => {
                            info!(
                                sink = %name,
                                path = %path,
                                batches = snapshot.batches_received,
                                events = snapshot.event_rows_written,
                                logs = snapshot.log_rows_written,
                                "parquet sink finished"
                            );
                        }
                        Err(e) => {
                            error!(sink = %name, error = %e, "parquet sink error");
                        }
                    }
                }));
            }

            // Arrow IPC sink
            (_, Some(tell_config::SinkConfig::ArrowIpc(arrow_config))) => {
                if !arrow_config.enabled {
                    info!(sink = %sink_name, "arrow_ipc sink disabled, skipping");
                    continue;
                }

                let (tx, rx) = mpsc::channel(queue_size);
                let handle = SinkHandle::new(sink_id, sink_name, tx);
                router.register_sink(handle);

                let mut sink_config = ArrowIpcConfig::default()
                    .with_path(&arrow_config.path)
                    .with_buffer_size(arrow_config.buffer_size);

                sink_config.rotation_interval = match arrow_config.rotation {
                    tell_config::RotationInterval::Hourly => {
                        tell_sinks::arrow_ipc::RotationInterval::Hourly
                    }
                    tell_config::RotationInterval::Daily => {
                        tell_sinks::arrow_ipc::RotationInterval::Daily
                    }
                };

                sink_config.flush_interval = arrow_config.flush_interval;

                let sink = ArrowIpcSink::new(sink_config, rx);
                sink_metrics.push(Arc::new(sink.metrics_handle()));

                let name = sink_name.clone();
                let path = arrow_config.path.clone();
                tasks.push(tokio::spawn(async move {
                    match sink.run().await {
                        Ok(snapshot) => {
                            info!(
                                sink = %name,
                                path = %path,
                                batches = snapshot.batches_received,
                                events = snapshot.event_rows_written,
                                logs = snapshot.log_rows_written,
                                "arrow_ipc sink finished"
                            );
                        }
                        Err(e) => {
                            error!(sink = %name, error = %e, "arrow_ipc sink error");
                        }
                    }
                }));
            }

            // Forwarder sink
            (_, Some(tell_config::SinkConfig::Forwarder(fwd_config))) => {
                if !fwd_config.enabled {
                    info!(sink = %sink_name, "forwarder sink disabled, skipping");
                    continue;
                }

                let (tx, rx) = mpsc::channel(queue_size);
                let handle = SinkHandle::new(sink_id, sink_name, tx);
                router.register_sink(handle);

                let sink_config =
                    match ForwarderConfig::new(&fwd_config.target, &fwd_config.api_key) {
                        Ok(cfg) => cfg
                            .with_connection_timeout(fwd_config.connection_timeout)
                            .with_write_timeout(fwd_config.write_timeout)
                            .with_retry_attempts(fwd_config.retry_attempts)
                            .with_retry_interval(fwd_config.retry_interval)
                            .with_reconnect_interval(fwd_config.reconnect_interval),
                        Err(e) => {
                            error!(sink = %sink_name, error = %e, "invalid forwarder config");
                            continue;
                        }
                    };

                let sink = ForwarderSink::new(sink_config, rx);
                sink_metrics.push(Arc::new(sink.metrics_handle()));

                let name = sink_name.clone();
                let target = fwd_config.target.clone();
                tasks.push(tokio::spawn(async move {
                    let snapshot = sink.run().await;
                    info!(
                        sink = %name,
                        target = %target,
                        batches = snapshot.batches_received,
                        bytes = snapshot.bytes_sent,
                        "forwarder sink finished"
                    );
                }));
            }

            // No config found - should not happen as build_routing_table filters these
            (_, None) => {
                warn!(sink = %sink_name, "sink has no configuration");
            }
        }
    }

    tasks
}

/// Start all configured sources
async fn start_sources(
    config: &Config,
    auth_store: Arc<ApiKeyStore>,
    sharded_sender: ShardedSender,
    source_metrics: &mut Vec<Arc<dyn SourceMetricsProvider>>,
    cancel: CancellationToken,
) -> Result<Vec<JoinHandle<()>>> {
    let mut tasks = Vec::new();

    // Start TCP sources
    for (idx, tcp_cfg) in config.sources.tcp.iter().enumerate() {
        if !tcp_cfg.enabled {
            continue;
        }

        let source_id = if idx == 0 {
            "tcp_main".to_string()
        } else {
            format!("tcp_{}", idx)
        };

        let tcp_config = TcpSourceConfig {
            id: source_id.clone(),
            address: tcp_cfg.address.clone(),
            port: tcp_cfg.port,
            buffer_size: tcp_cfg.buffer_size.unwrap_or(config.global.buffer_size),
            batch_size: config.global.batch_size,
            flush_interval: tcp_cfg.flush_interval,
            forwarding_mode: tcp_cfg.forwarding_mode,
            nodelay: tcp_cfg.no_delay,
            ..Default::default()
        };

        info!(
            source_id = %source_id,
            address = %tcp_config.address,
            port = tcp_config.port,
            forwarding_mode = tcp_config.forwarding_mode,
            "starting TCP source"
        );

        let tcp_source =
            TcpSource::new(tcp_config, Arc::clone(&auth_store), sharded_sender.clone());

        // Collect metrics handle before running
        source_metrics.push(Arc::new(tcp_source.metrics_handle()));

        let cancel_token = cancel.clone();
        tasks.push(tokio::spawn(async move {
            if let Err(e) = tcp_source.run(cancel_token).await {
                error!(source_id = %source_id, error = %e, "TCP source error");
            }
        }));
    }

    // Start Syslog TCP source
    if let Some(ref syslog_tcp_cfg) = config.sources.syslog_tcp
        && syslog_tcp_cfg.enabled
    {
        let workspace_id = parse_workspace_id(&syslog_tcp_cfg.workspace_id);

        let syslog_tcp_config = SyslogTcpSourceConfig {
            id: "syslog_tcp".to_string(),
            address: syslog_tcp_cfg.address.clone(),
            port: syslog_tcp_cfg.port,
            buffer_size: syslog_tcp_cfg
                .buffer_size
                .unwrap_or(config.global.buffer_size),
            queue_size: syslog_tcp_cfg
                .queue_size
                .unwrap_or(config.global.queue_size),
            batch_size: config.global.batch_size,
            flush_interval: syslog_tcp_cfg.flush_interval,
            nodelay: syslog_tcp_cfg.no_delay,
            connection_timeout: syslog_tcp_cfg.connection_timeout,
            workspace_id,
            max_message_size: syslog_tcp_cfg.max_message_size,
            socket_buffer_size: 256 * 1024,
        };

        info!(
            source_id = "syslog_tcp",
            address = %syslog_tcp_config.address,
            port = syslog_tcp_config.port,
            workspace_id = workspace_id,
            max_message_size = syslog_tcp_config.max_message_size,
            "starting Syslog TCP source"
        );

        let syslog_tcp_source = SyslogTcpSource::new(syslog_tcp_config, sharded_sender.clone());

        // Collect metrics handle before running
        source_metrics.push(Arc::new(syslog_tcp_source.metrics_handle()));

        let cancel_token = cancel.clone();
        tasks.push(tokio::spawn(async move {
            if let Err(e) = syslog_tcp_source.run(cancel_token).await {
                error!(source_id = "syslog_tcp", error = %e, "Syslog TCP source error");
            }
        }));
    }

    // Start Syslog UDP source
    if let Some(ref syslog_udp_cfg) = config.sources.syslog_udp
        && syslog_udp_cfg.enabled
    {
        let workspace_id = parse_workspace_id(&syslog_udp_cfg.workspace_id);

        let syslog_udp_config = SyslogUdpSourceConfig {
            id: "syslog_udp".to_string(),
            address: syslog_udp_cfg.address.clone(),
            port: syslog_udp_cfg.port,
            buffer_size: syslog_udp_cfg
                .buffer_size
                .unwrap_or(config.global.buffer_size),
            queue_size: config.global.queue_size,
            batch_size: config.global.batch_size,
            flush_interval: std::time::Duration::from_millis(50), // Faster for UDP
            num_workers: syslog_udp_cfg.num_workers,
            workspace_id,
            max_message_size: syslog_udp_cfg.max_message_size,
        };

        info!(
            source_id = "syslog_udp",
            address = %syslog_udp_config.address,
            port = syslog_udp_config.port,
            workspace_id = workspace_id,
            num_workers = syslog_udp_config.num_workers,
            max_message_size = syslog_udp_config.max_message_size,
            "starting Syslog UDP source"
        );

        let syslog_udp_source = SyslogUdpSource::new(syslog_udp_config, sharded_sender.clone());

        // Collect metrics handle before running
        source_metrics.push(Arc::new(syslog_udp_source.metrics_handle()));

        let cancel_token = cancel.clone();
        tasks.push(tokio::spawn(async move {
            if let Err(e) = syslog_udp_source.run(cancel_token).await {
                error!(source_id = "syslog_udp", error = %e, "Syslog UDP source error");
            }
        }));
    }

    // Start TCP Debug source if configured
    if let Some(ref tcp_debug_cfg) = config.sources.tcp_debug
        && tcp_debug_cfg.enabled
    {
        let tcp_debug_config = TcpDebugSourceConfig {
            id: "tcp_debug".to_string(),
            address: tcp_debug_cfg.address.clone(),
            port: tcp_debug_cfg.port,
            buffer_size: tcp_debug_cfg
                .buffer_size
                .unwrap_or(config.global.buffer_size),
            batch_size: config.global.batch_size,
            flush_interval: tcp_debug_cfg.flush_interval,
            nodelay: tcp_debug_cfg.no_delay,
            socket_buffer_size: 256 * 1024, // 256KB default
        };

        warn!(
            source_id = "tcp_debug",
            address = %tcp_debug_config.address,
            port = tcp_debug_config.port,
            "Starting TCP DEBUG source - NOT FOR PRODUCTION USE"
        );

        let tcp_debug_source = TcpDebugSource::new(
            tcp_debug_config,
            Arc::clone(&auth_store),
            sharded_sender.clone(),
        );

        let cancel_token = cancel.clone();
        tasks.push(tokio::spawn(async move {
            if let Err(e) = tcp_debug_source.run(cancel_token).await {
                error!(source_id = "tcp_debug", error = %e, "TCP debug source error");
            }
        }));
    }

    // Start HTTP REST API source
    if let Some(ref http_cfg) = config.sources.http
        && http_cfg.enabled
    {
        let http_config = HttpSourceConfig {
            id: "http".to_string(),
            address: http_cfg.address.clone(),
            port: http_cfg.port,
            max_payload_size: http_cfg.max_payload_size,
            batch_size: http_cfg.batch_size,
            cors_enabled: http_cfg.cors_enabled,
            ..Default::default()
        };

        info!(
            source_id = "http",
            address = %http_config.address,
            port = http_config.port,
            max_payload_size = http_config.max_payload_size,
            cors_enabled = http_config.cors_enabled,
            "starting HTTP source"
        );

        let http_source =
            HttpSource::new(http_config, Arc::clone(&auth_store), sharded_sender.clone());

        // Collect metrics handle before running
        source_metrics.push(Arc::new(http_source.metrics_handle()));

        let cancel_token = cancel.clone();
        tasks.push(tokio::spawn(async move {
            if let Err(e) = http_source.run(cancel_token).await {
                error!(source_id = "http", error = %e, "HTTP source error");
            }
        }));
    }

    // If no sources configured in non-debug mode, start default TCP source
    if tasks.is_empty() {
        let tcp_config = TcpSourceConfig {
            id: "tcp_main".into(),
            address: "0.0.0.0".into(),
            port: 50000,
            batch_size: config.global.batch_size,
            ..Default::default()
        };

        info!(
            source_id = "tcp_main",
            address = %tcp_config.address,
            port = tcp_config.port,
            "starting default TCP source (no sources configured)"
        );

        let tcp_source = TcpSource::new(tcp_config, auth_store, sharded_sender);

        // Collect metrics handle before running
        source_metrics.push(Arc::new(tcp_source.metrics_handle()));

        let cancel_token = cancel.clone();
        tasks.push(tokio::spawn(async move {
            if let Err(e) = tcp_source.run(cancel_token).await {
                error!(source_id = "tcp_main", error = %e, "TCP source error");
            }
        }));
    }

    Ok(tasks)
}

/// Parse workspace ID from config string to u32
fn parse_workspace_id(workspace_id: &Option<String>) -> u32 {
    match workspace_id {
        Some(s) => match s.parse() {
            Ok(id) => id,
            Err(e) => {
                tracing::warn!(
                    workspace_id = %s,
                    error = %e,
                    "invalid workspace_id format, using default (1)"
                );
                1
            }
        },
        None => 1,
    }
}

/// Load API keys from file or generate a default one
fn load_or_generate_api_keys(auth_store: &ApiKeyStore, api_keys_file: &str) -> Result<()> {
    match auth_store.reload(api_keys_file) {
        Ok(count) => {
            info!(file = %api_keys_file, count = count, "loaded API keys");
            Ok(())
        }
        Err(e) => {
            // If file doesn't exist, create it with a generated key
            if !Path::new(api_keys_file).exists() {
                match generate_default_api_key(api_keys_file) {
                    Ok(key_hex) => {
                        info!(
                            file = %api_keys_file,
                            "generated default API key (save this!): {}",
                            key_hex
                        );
                        // Reload after creating
                        if let Err(e) = auth_store.reload(api_keys_file) {
                            warn!(error = %e, "failed to reload after generating key");
                        }
                        Ok(())
                    }
                    Err(gen_err) => {
                        warn!(
                            file = %api_keys_file,
                            error = %e,
                            gen_error = %gen_err,
                            "failed to load or generate API keys"
                        );
                        Ok(()) // Continue anyway, auth will reject all requests
                    }
                }
            } else {
                warn!(
                    file = %api_keys_file,
                    error = %e,
                    "failed to load API keys, authentication will reject all requests"
                );
                Ok(()) // Continue anyway
            }
        }
    }
}

/// Wait for SIGINT or SIGTERM
async fn wait_for_shutdown() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

/// Start the connector scheduler if connectors are configured
async fn start_connector_scheduler(
    config: &Config,
    sharded_sender: ShardedSender,
) -> Option<JoinHandle<()>> {
    if config.connectors.is_empty() {
        return None;
    }

    // Create a channel for the scheduler to send batches
    let (batch_tx, mut batch_rx) = mpsc::channel::<Arc<Batch>>(1000);

    // Build scheduler with configured connectors
    let mut scheduler = ConnectorScheduler::new(batch_tx);

    for (name, raw_config) in config.connectors.iter() {
        if !raw_config.is_enabled() {
            info!(connector = %name, "connector disabled, skipping");
            continue;
        }

        match raw_config.connector_type.as_str() {
            #[cfg(feature = "connector-github")]
            "github" => match GitHubConnectorConfig::from_toml(&raw_config.config) {
                Ok(github_config) => {
                    match ScheduledConnector::github(
                        name.clone(),
                        &github_config,
                        raw_config.schedule.as_deref(),
                    ) {
                        Ok(connector) => {
                            scheduler.add(connector);
                        }
                        Err(e) => {
                            error!(connector = %name, error = %e, "failed to create GitHub connector");
                        }
                    }
                }
                Err(e) => {
                    error!(connector = %name, error = %e, "failed to parse GitHub config");
                }
            },
            #[cfg(feature = "connector-shopify")]
            "shopify" => match ShopifyConnectorConfig::from_toml(&raw_config.config) {
                Ok(shopify_config) => {
                    match ScheduledConnector::shopify(
                        name.clone(),
                        &shopify_config,
                        raw_config.schedule.as_deref(),
                    ) {
                        Ok(connector) => {
                            scheduler.add(connector);
                        }
                        Err(e) => {
                            error!(connector = %name, error = %e, "failed to create Shopify connector");
                        }
                    }
                }
                Err(e) => {
                    error!(connector = %name, error = %e, "failed to parse Shopify config");
                }
            },
            other => {
                warn!(connector = %name, connector_type = %other, "unknown connector type, available: {:?}", tell_connectors::available_connectors());
            }
        }
    }

    // Spawn a task to forward batches from scheduler to the pipeline
    let sender = sharded_sender;
    // Allocate a single connection ID for all connector batches
    let connector_connection_id = sender.allocate_connection_id();
    let forwarder = tokio::spawn(async move {
        while let Some(batch) = batch_rx.recv().await {
            // Unwrap the Arc and send through the sharded sender
            // This does a clone of the batch since we need to consume the Arc
            let batch = Arc::try_unwrap(batch).unwrap_or_else(|arc| (*arc).clone());
            if let Err(e) = sender.send(batch, connector_connection_id).await {
                error!(error = %e, "failed to forward connector batch to pipeline");
            }
        }
    });

    // Spawn the scheduler
    let scheduler_task = tokio::spawn(async move {
        if let Err(e) = scheduler.run().await {
            error!(error = %e, "connector scheduler error");
        }
    });

    // Return a combined task handle
    Some(tokio::spawn(async move {
        tokio::select! {
            _ = forwarder => {}
            _ = scheduler_task => {}
        }
    }))
}

/// Generate a random API key and write it to the specified file
fn generate_default_api_key(path: &str) -> Result<String> {
    use std::time::{SystemTime, UNIX_EPOCH};

    // Generate a pseudo-random key using system time and process id
    // For production, consider using a proper random number generator
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let seed = now.as_nanos() as u64 ^ (std::process::id() as u64);

    // Simple PRNG (xorshift)
    let mut state = seed;
    let mut key_bytes = [0u8; 16];
    for byte in &mut key_bytes {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        *byte = state as u8;
    }

    // Convert to hex string
    let key_hex: String = key_bytes.iter().map(|b| format!("{:02x}", b)).collect();

    // Create parent directory if needed
    if let Some(parent) = Path::new(path)
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
    {
        fs::create_dir_all(parent).context("failed to create API keys directory")?;
    }

    // Write key file with default workspace (numeric ID like Go version)
    let content = format!("# Auto-generated API key - save this!\n{}:1\n", key_hex);
    fs::write(path, &content).context("failed to write API keys file")?;

    Ok(key_hex)
}
