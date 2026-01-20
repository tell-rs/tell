//! End-to-end benchmark suite
//!
//! Two types of benchmarks:
//!
//! 1. **Pipeline benchmarks**: Batch injection → Router → Sink
//!    - Measures pipeline + sink throughput without network I/O
//!
//! 2. **Network E2E benchmarks**: TestClient → Source → Router → Sink
//!    - Measures full end-to-end throughput including network I/O
//!
//! Run with: `cargo bench -p tell-bench --bench e2e`

use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tell_auth::ApiKeyStore;
use tell_bench::SCENARIOS;
use tell_client::test::{SyslogTcpTestClient, SyslogUdpTestClient, TcpTestClient};
use tell_client::{BatchBuilder as ClientBatchBuilder, SchemaType};
use tell_pipeline::{Router, SinkHandle};
use tell_routing::RoutingTable;
use tell_protocol::{Batch, BatchBuilder, BatchType, SourceId};
use tell_sinks::disk_binary::{DiskBinaryConfig, DiskBinarySink};
use tell_sources::{
    SyslogTcpSource, SyslogTcpSourceConfig, SyslogUdpSource, SyslogUdpSourceConfig, TcpSource,
    TcpSourceConfig,
};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use tempfile::TempDir;
use tokio::net::{TcpListener, UdpSocket};
use tokio::runtime::Runtime;
use tokio::sync::mpsc;

// =============================================================================
// Batch Generator (for pipeline benchmarks)
// =============================================================================

/// Create a test batch with specified events
fn create_test_batch(events_per_batch: usize, event_size: usize) -> Batch {
    let source_id = SourceId::new("bench-source");
    let mut builder = BatchBuilder::new(BatchType::Event, source_id);
    builder.set_workspace_id(12345);
    builder.set_source_ip(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 100)));

    let event_data = vec![0x42u8; event_size];
    for _ in 0..events_per_batch {
        builder.add(&event_data, 1);
    }

    builder.finish()
}

// =============================================================================
// Null Sink (for measuring pipeline overhead)
// =============================================================================

struct NullSink {
    receiver: mpsc::Receiver<Arc<Batch>>,
    count: u64,
}

impl NullSink {
    fn new(receiver: mpsc::Receiver<Arc<Batch>>) -> Self {
        Self { receiver, count: 0 }
    }

    async fn run(mut self) -> u64 {
        while let Some(_batch) = self.receiver.recv().await {
            self.count += 1;
        }
        self.count
    }
}

// =============================================================================
// Pipeline Benchmark Runners
// =============================================================================

/// Run pipeline → disk sink benchmark
async fn run_pipeline_to_disk(
    num_batches: usize,
    events_per_batch: usize,
    event_size: usize,
    temp_dir: &TempDir,
) -> Duration {
    let (source_tx, source_rx) = mpsc::channel::<Batch>(10000);
    let (sink_tx, sink_rx) = mpsc::channel::<Arc<Batch>>(10000);

    let mut routing_table = RoutingTable::new();
    let sink_id = routing_table.register_sink("disk_binary");
    routing_table.set_default(vec![sink_id]);

    let mut router = Router::new(routing_table);
    router.register_sink(SinkHandle::new(sink_id, "disk_binary", sink_tx));

    let mut sink_config = DiskBinaryConfig::default();
    sink_config.path = temp_dir.path().to_path_buf();
    sink_config.compression = false;
    sink_config.flush_interval = Duration::from_millis(10);
    let sink = DiskBinarySink::new(sink_config, sink_rx);

    let batches: Vec<Batch> = (0..num_batches)
        .map(|_| create_test_batch(events_per_batch, event_size))
        .collect();

    let router_handle = tokio::spawn(async move { router.run(source_rx).await });
    let sink_handle = tokio::spawn(async move { sink.run().await });

    let start = Instant::now();

    for batch in batches {
        source_tx.send(batch).await.expect("send batch");
    }
    drop(source_tx);

    let _ = router_handle.await;
    let _ = sink_handle.await;

    start.elapsed()
}

/// Run pipeline → null sink benchmark (measures pipeline overhead)
async fn run_pipeline_to_null(
    num_batches: usize,
    events_per_batch: usize,
    event_size: usize,
) -> Duration {
    let (source_tx, source_rx) = mpsc::channel::<Batch>(10000);
    let (sink_tx, sink_rx) = mpsc::channel::<Arc<Batch>>(10000);

    let mut routing_table = RoutingTable::new();
    let sink_id = routing_table.register_sink("null");
    routing_table.set_default(vec![sink_id]);

    let mut router = Router::new(routing_table);
    router.register_sink(SinkHandle::new(sink_id, "null", sink_tx));

    let sink = NullSink::new(sink_rx);

    let batches: Vec<Batch> = (0..num_batches)
        .map(|_| create_test_batch(events_per_batch, event_size))
        .collect();

    let router_handle = tokio::spawn(async move { router.run(source_rx).await });
    let sink_handle = tokio::spawn(async move { sink.run().await });

    let start = Instant::now();

    for batch in batches {
        source_tx.send(batch).await.expect("send batch");
    }
    drop(source_tx);

    let _ = router_handle.await;
    let _ = sink_handle.await;

    start.elapsed()
}

// =============================================================================
// Network E2E Benchmark Runners
// =============================================================================

/// Get an available port
async fn get_available_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

/// Get an available UDP port
async fn get_available_udp_port() -> u16 {
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let port = socket.local_addr().unwrap().port();
    drop(socket);
    port
}

/// Run Syslog TCP E2E benchmark
/// TestClient → SyslogTcpSource → Router → NullSink
async fn run_syslog_tcp_e2e(num_messages: usize, message_size: usize) -> Duration {
    let port = get_available_port().await;

    // Set up channels - using bounded to control flow
    let (batch_tx, mut batch_rx) = mpsc::channel::<Batch>(10000);
    let (sink_tx, sink_rx) = mpsc::channel::<Arc<Batch>>(10000);

    // Set up routing
    let mut routing_table = RoutingTable::new();
    let sink_id = routing_table.register_sink("null");
    routing_table.set_default(vec![sink_id]);

    let mut router = Router::new(routing_table);
    router.register_sink(SinkHandle::new(sink_id, "null", sink_tx));

    // Set up source
    let source_config = SyslogTcpSourceConfig {
        id: "bench_syslog_tcp".into(),
        address: "127.0.0.1".into(),
        port,
        flush_interval: Duration::from_millis(10),
        batch_size: 500,
        ..Default::default()
    };

    let source = Arc::new(SyslogTcpSource::new(source_config, batch_tx));
    let sink = NullSink::new(sink_rx);

    // Pre-generate messages
    let payload = "x".repeat(message_size.saturating_sub(50)); // Account for syslog header
    let messages: Vec<String> = (0..num_messages)
        .map(|_| SyslogTcpTestClient::rfc3164(134, "bench", "app", &payload))
        .collect();

    // Spawn source and sink
    let source_clone = Arc::clone(&source);
    let source_handle = tokio::spawn(async move {
        let _ = source_clone.run().await;
    });

    let sink_handle = tokio::spawn(async move { sink.run().await });

    // Wait for source to start
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Connect and send messages - measure send + processing time
    let start = Instant::now();

    let mut client = SyslogTcpTestClient::connect(&format!("127.0.0.1:{}", port))
        .await
        .expect("connect to syslog tcp");

    for msg in &messages {
        client.send(msg).await.expect("send message");
    }
    client.flush().await.expect("flush");

    // Close connection to signal EOF
    let _ = client.close().await;

    // Wait for batches to arrive and forward them to sink
    // This replaces the full Router.run() which would block forever
    let expected_batches = (num_messages / 500) + 1; // batch_size = 500
    let mut received_batches = 0;

    while received_batches < expected_batches {
        match tokio::time::timeout(Duration::from_millis(200), batch_rx.recv()).await {
            Ok(Some(batch)) => {
                received_batches += 1;
                router.route(batch).await;
            }
            Ok(None) => break, // Channel closed
            Err(_) => break,   // Timeout - no more batches coming
        }
    }

    let elapsed = start.elapsed();

    // Stop source
    source.stop();

    // Clean up
    drop(router);
    let _ = tokio::time::timeout(Duration::from_millis(100), source_handle).await;
    let _ = tokio::time::timeout(Duration::from_millis(100), sink_handle).await;

    elapsed
}

/// Run Syslog UDP E2E benchmark
/// TestClient → SyslogUdpSource → Router → NullSink
async fn run_syslog_udp_e2e(num_messages: usize, message_size: usize) -> Duration {
    let port = get_available_udp_port().await;

    // Set up channels
    let (batch_tx, mut batch_rx) = mpsc::channel::<Batch>(10000);
    let (sink_tx, sink_rx) = mpsc::channel::<Arc<Batch>>(10000);

    // Set up routing
    let mut routing_table = RoutingTable::new();
    let sink_id = routing_table.register_sink("null");
    routing_table.set_default(vec![sink_id]);

    let mut router = Router::new(routing_table);
    router.register_sink(SinkHandle::new(sink_id, "null", sink_tx));

    // Set up source
    let source_config = SyslogUdpSourceConfig {
        id: "bench_syslog_udp".into(),
        address: "127.0.0.1".into(),
        port,
        flush_interval: Duration::from_millis(10),
        batch_size: 500,
        num_workers: 1, // Single worker for benchmark consistency
        ..Default::default()
    };

    let source = Arc::new(SyslogUdpSource::new(source_config, batch_tx));
    let sink = NullSink::new(sink_rx);

    // Pre-generate messages
    let payload = "x".repeat(message_size.saturating_sub(50));
    let messages: Vec<String> = (0..num_messages)
        .map(|_| SyslogUdpTestClient::rfc3164(134, "bench", "app", &payload))
        .collect();

    // Spawn source and sink
    let source_clone = Arc::clone(&source);
    let source_handle = tokio::spawn(async move {
        let _ = source_clone.run().await;
    });

    let sink_handle = tokio::spawn(async move { sink.run().await });

    // Wait for source to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Send messages
    let start = Instant::now();

    let client = SyslogUdpTestClient::new().await.expect("create udp client");
    let addr = format!("127.0.0.1:{}", port);

    for msg in &messages {
        let _ = client.send_to(msg, &addr).await;
    }

    // Wait for batches and route them
    let expected_batches = (num_messages / 500) + 1;
    let mut received_batches = 0;

    while received_batches < expected_batches {
        match tokio::time::timeout(Duration::from_millis(200), batch_rx.recv()).await {
            Ok(Some(batch)) => {
                received_batches += 1;
                router.route(batch).await;
            }
            Ok(None) => break,
            Err(_) => break,
        }
    }

    let elapsed = start.elapsed();

    // Stop source
    source.stop();

    // Clean up
    drop(router);
    let _ = tokio::time::timeout(Duration::from_millis(100), source_handle).await;
    let _ = tokio::time::timeout(Duration::from_millis(100), sink_handle).await;

    elapsed
}

/// Run TCP E2E benchmark with FlatBuffer protocol
/// TcpTestClient → TcpSource → Router → NullSink
async fn run_tcp_e2e(num_batches: usize, events_per_batch: usize, event_size: usize) -> Duration {
    let port = get_available_port().await;

    // Set up API key store with a test key
    let api_key: [u8; 16] = [0x01; 16];
    let auth_store = Arc::new(ApiKeyStore::new());
    auth_store.insert(api_key, 12345.into());

    // Set up channels
    let (batch_tx, mut batch_rx) = mpsc::channel::<Batch>(10000);
    let (sink_tx, sink_rx) = mpsc::channel::<Arc<Batch>>(10000);

    // Set up routing
    let mut routing_table = RoutingTable::new();
    let sink_id = routing_table.register_sink("null");
    routing_table.set_default(vec![sink_id]);

    let mut router = Router::new(routing_table);
    router.register_sink(SinkHandle::new(sink_id, "null", sink_tx));

    // Set up source
    let source_config = TcpSourceConfig {
        id: "bench_tcp".into(),
        address: "127.0.0.1".into(),
        port,
        flush_interval: Duration::from_millis(10),
        batch_size: 500,
        ..Default::default()
    };

    let source = TcpSource::new(source_config, Arc::clone(&auth_store), batch_tx);
    let sink = NullSink::new(sink_rx);

    // Pre-build batches
    let event_data = vec![0x42u8; event_size];
    let mut all_data = Vec::new();
    for _ in 0..events_per_batch {
        all_data.extend_from_slice(&event_data);
    }

    let batches: Vec<_> = (0..num_batches)
        .map(|_| {
            ClientBatchBuilder::new()
                .api_key(api_key)
                .schema_type(SchemaType::Event)
                .data(&all_data)
                .build()
                .expect("build batch")
        })
        .collect();

    // Spawn source and sink
    let source_handle = tokio::spawn(async move {
        let _ = source.run().await;
    });

    let sink_handle = tokio::spawn(async move { sink.run().await });

    // Wait for source to start
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Connect and send batches
    let start = Instant::now();

    let mut client = TcpTestClient::connect(&format!("127.0.0.1:{}", port))
        .await
        .expect("connect to tcp source");

    for batch in &batches {
        client.send(batch).await.expect("send batch");
    }
    client.flush().await.expect("flush");

    // Close connection
    let _ = client.close().await;

    // Wait for batches and route them
    // TCP source batches messages from each connection, so expect fewer batches
    let expected_batches = (num_batches / 500) + 1;
    let mut received_batches = 0;

    while received_batches < expected_batches {
        match tokio::time::timeout(Duration::from_millis(200), batch_rx.recv()).await {
            Ok(Some(batch)) => {
                received_batches += 1;
                router.route(batch).await;
            }
            Ok(None) => break,
            Err(_) => break,
        }
    }

    let elapsed = start.elapsed();

    // Clean up
    drop(router);
    let _ = tokio::time::timeout(Duration::from_millis(200), source_handle).await;
    let _ = tokio::time::timeout(Duration::from_millis(100), sink_handle).await;

    elapsed
}

// =============================================================================
// Criterion Benchmarks - Pipeline
// =============================================================================

fn bench_pipeline_to_disk(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("pipeline_to_DiskBinarySink");

    group.sample_size(10);
    group.measurement_time(Duration::from_secs(20));

    let num_batches = 1000;

    for scenario in SCENARIOS {
        let total_events = num_batches * scenario.events_per_batch;

        group.throughput(Throughput::Elements(total_events as u64));
        group.bench_function(BenchmarkId::new("events", scenario.name), |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let temp_dir = TempDir::new().unwrap();
                    total += rt.block_on(run_pipeline_to_disk(
                        num_batches,
                        scenario.events_per_batch,
                        scenario.event_size,
                        &temp_dir,
                    ));
                }
                total
            });
        });
    }

    group.finish();
}

fn bench_pipeline_to_null(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("pipeline_to_NullSink");

    group.sample_size(10);
    group.measurement_time(Duration::from_secs(20));

    let num_batches = 1000;

    for scenario in SCENARIOS {
        let total_events = num_batches * scenario.events_per_batch;

        group.throughput(Throughput::Elements(total_events as u64));
        group.bench_function(BenchmarkId::new("events", scenario.name), |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    total += rt.block_on(run_pipeline_to_null(
                        num_batches,
                        scenario.events_per_batch,
                        scenario.event_size,
                    ));
                }
                total
            });
        });
    }

    group.finish();
}

// =============================================================================
// Criterion Benchmarks - Network E2E
// =============================================================================

fn bench_syslog_tcp_e2e(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("e2e_SyslogTcpSource_to_NullSink");

    group.sample_size(10);
    group.measurement_time(Duration::from_secs(30));

    // Test with different message counts
    for (num_messages, msg_size) in [(10_000, 200), (50_000, 200), (10_000, 500)] {
        group.throughput(Throughput::Elements(num_messages as u64));
        group.bench_function(
            BenchmarkId::new("messages", format!("{}x{}B", num_messages, msg_size)),
            |b| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        total += rt.block_on(run_syslog_tcp_e2e(num_messages, msg_size));
                    }
                    total
                });
            },
        );
    }

    group.finish();
}

fn bench_syslog_udp_e2e(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("e2e_SyslogUdpSource_to_NullSink");

    group.sample_size(10);
    group.measurement_time(Duration::from_secs(30));

    // Test with different message counts (UDP is faster but may drop)
    for (num_messages, msg_size) in [(10_000, 200), (50_000, 200), (10_000, 500)] {
        group.throughput(Throughput::Elements(num_messages as u64));
        group.bench_function(
            BenchmarkId::new("messages", format!("{}x{}B", num_messages, msg_size)),
            |b| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        total += rt.block_on(run_syslog_udp_e2e(num_messages, msg_size));
                    }
                    total
                });
            },
        );
    }

    group.finish();
}

fn bench_tcp_e2e(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("e2e_TcpSource_to_NullSink");

    group.sample_size(10);
    group.measurement_time(Duration::from_secs(30));

    // Test with typical scenario
    let num_batches = 1000;
    for scenario in &SCENARIOS[1..2] {
        // Just "typical" scenario
        let total_events = num_batches * scenario.events_per_batch;

        group.throughput(Throughput::Elements(total_events as u64));
        group.bench_function(BenchmarkId::new("events", scenario.name), |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    total += rt.block_on(run_tcp_e2e(
                        num_batches,
                        scenario.events_per_batch,
                        scenario.event_size,
                    ));
                }
                total
            });
        });
    }

    group.finish();
}

// =============================================================================
// Criterion Setup
// =============================================================================

criterion_group!(
    benches,
    bench_pipeline_to_disk,
    bench_pipeline_to_null,
    bench_syslog_tcp_e2e,
    bench_syslog_udp_e2e,
    bench_tcp_e2e,
);

criterion_main!(benches);
