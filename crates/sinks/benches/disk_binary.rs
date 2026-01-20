//! DiskBinary Sink benchmark suite
//!
//! Benchmarks for disk binary sink write throughput.
//!
//! Run with: `cargo bench -p tell-sinks --bench disk_binary`
//!
//! # Terminology
//!
//! - **Event**: Single business event/log (e.g., page view, log line)
//! - **Batch**: Container holding multiple events, routed through pipeline
//! - **Wire message**: FlatBuffer-encoded batch sent over TCP
//!
//! # What we measure
//!
//! - Batch serialization (hot path - serialize_batch)
//! - Buffer pool effectiveness
//! - Full sink throughput (with actual disk I/O)
//! - Compression overhead (LZ4 vs uncompressed)
//!
//! # Scenarios (from Go benchmarks)
//!
//! - Typical: 100 events/batch, 200 bytes/event
//! - High volume: 500 events/batch, 200 bytes/event (Go default)
//! - Large events: 100 events/batch, 1000 bytes/event

use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use tell_bench::{BenchScenario, SCENARIOS};
use tell_protocol::{Batch, BatchBuilder, BatchType, SourceId};
use tell_sinks::disk_binary::{DiskBinaryConfig, DiskBinarySink, METADATA_SIZE};
use tell_sinks::util::{AtomicRotationSink, BinaryWriter, BufferPool, RotationConfig};
use criterion::{
    black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput,
};
use tempfile::TempDir;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;

/// Create a test batch from a scenario
fn create_batch_from_scenario(scenario: &BenchScenario) -> Batch {
    create_test_batch(scenario.events_per_batch, scenario.event_size)
}

/// Create a test batch with specified number of events and event size
fn create_test_batch(events_per_batch: usize, event_size: usize) -> Batch {
    let source_id = SourceId::new("bench");
    let mut builder = BatchBuilder::new(BatchType::Event, source_id);
    builder.set_workspace_id(12345);
    builder.set_source_ip(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 100)));

    // Create test event data
    let event_data = vec![0x42u8; event_size];

    for _ in 0..events_per_batch {
        builder.add(&event_data, 1);
    }

    builder.finish()
}

// =============================================================================
// Buffer Pool Benchmarks
// =============================================================================

/// Benchmark: Buffer pool get/put cycle
fn bench_buffer_pool(c: &mut Criterion) {
    let mut group = c.benchmark_group("sink_buffer_pool");

    let pool = BufferPool::new(64, 32 * 1024 * 1024);

    group.bench_function("get_put_cycle", |b| {
        b.iter(|| {
            let buf = pool.get();
            black_box(&buf);
            pool.put(buf);
        });
    });

    // Measure hit rate under contention
    group.bench_function("get_only", |b| {
        b.iter(|| {
            let buf = pool.get();
            black_box(buf)
        });
    });

    group.finish();
}

// =============================================================================
// Serialization Benchmarks
// =============================================================================

/// Benchmark: Batch serialization (the hot path)
///
/// This measures how fast we can serialize a Batch to the binary format:
/// [24-byte metadata][4-byte size][event data] per event
fn bench_serialization(c: &mut Criterion) {
    let mut group = c.benchmark_group("sink_serialization");

    // Test standard scenarios from Go benchmarks
    for scenario in SCENARIOS {
        let batch = create_batch_from_scenario(scenario);
        let total_bytes =
            scenario.events_per_batch * (METADATA_SIZE + 4 + scenario.event_size);

        group.throughput(Throughput::Elements(scenario.events_per_batch as u64));
        group.bench_with_input(
            BenchmarkId::new("events", scenario.name),
            &batch,
            |b, batch| {
                let mut buffer = BytesMut::with_capacity(total_bytes + 1024);
                b.iter(|| {
                    buffer.clear();
                    let result = serialize_batch_inline(batch, &mut buffer);
                    black_box(result)
                });
            },
        );
    }

    // Also test bytes/sec for the high-volume scenario
    let scenario = &SCENARIOS[2]; // high_volume
    let batch = create_batch_from_scenario(scenario);
    let total_bytes = scenario.events_per_batch * (METADATA_SIZE + 4 + scenario.event_size);

    group.throughput(Throughput::Bytes(total_bytes as u64));
    group.bench_with_input(
        BenchmarkId::new("bytes", scenario.name),
        &batch,
        |b, batch| {
            let mut buffer = BytesMut::with_capacity(total_bytes + 1024);
            b.iter(|| {
                buffer.clear();
                let result = serialize_batch_inline(batch, &mut buffer);
                black_box(result)
            });
        },
    );

    group.finish();
}

/// Inline serialization matching DiskBinarySink::serialize_batch
/// Returns (event_count, bytes_written)
fn serialize_batch_inline(batch: &Batch, buffer: &mut BytesMut) -> (usize, usize) {
    let timestamp = 1704067200000u64; // Fixed timestamp for benchmark
    let source_ip: [u8; 16] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 192, 168, 1, 100];

    let mut event_count = 0;
    let mut bytes_written = 0;

    for i in 0..batch.message_count() {
        let Some(event_data) = batch.get_message(i) else {
            continue;
        };

        // Write metadata (24 bytes)
        buffer.extend_from_slice(&timestamp.to_be_bytes());
        buffer.extend_from_slice(&source_ip);

        // Write event size (4 bytes)
        buffer.extend_from_slice(&(event_data.len() as u32).to_be_bytes());

        // Write event data
        buffer.extend_from_slice(event_data);

        event_count += 1;
        bytes_written += METADATA_SIZE + 4 + event_data.len();
    }

    (event_count, bytes_written)
}

// =============================================================================
// Rotation System Benchmarks
// =============================================================================

/// Benchmark: Submit to rotation system (non-blocking path)
fn bench_rotation_submit(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("sink_rotation");

    // Pre-serialize a batch
    let batch = create_test_batch(100, 200);
    let mut template_buffer = BytesMut::with_capacity(100 * (METADATA_SIZE + 4 + 200) + 1024);
    serialize_batch_inline(&batch, &mut template_buffer);

    // Create rotation system inside runtime context
    let temp_dir = TempDir::new().unwrap();
    let rotation = rt.block_on(async {
        let config = RotationConfig {
            base_path: temp_dir.path().to_path_buf(),
            rotation_interval: sinks::util::RotationInterval::Hourly,
            file_prefix: "data".into(),
            buffer_size: 32 * 1024 * 1024,
            queue_size: 10000,
            flush_interval: Duration::from_millis(100),
        };
        let writer = BinaryWriter::uncompressed(config.buffer_size);
        Arc::new(AtomicRotationSink::new(config, writer))
    });

    group.throughput(Throughput::Elements(100)); // 100 messages per submit
    group.bench_function("submit_nonblocking", |b| {
        b.to_async(&rt).iter(|| {
            let rotation = Arc::clone(&rotation);
            let template = template_buffer.clone();
            async move {
                let mut buffer = rotation.get_buffer();
                buffer.extend_from_slice(&template);
                match rotation.submit("12345", buffer, 100) {
                    Ok(()) => {}
                    Err(buf) => rotation.put_buffer(buf),
                }
            }
        });
    });

    // Cleanup
    rt.block_on(async { rotation.stop().await });

    group.finish();
}

// =============================================================================
// Full Sink Benchmarks
// =============================================================================

/// Benchmark: Full sink throughput with disk I/O
fn bench_full_sink(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("sink_full");

    // Increase measurement time for I/O benchmarks
    group.measurement_time(Duration::from_secs(10));

    for compression in [false, true] {
        let label = if compression { "lz4" } else { "uncompressed" };

        group.throughput(Throughput::Elements(1000)); // 1000 batches
        group.bench_function(BenchmarkId::new("1000_batches", label), |b| {
            b.iter_custom(|iters| {
                let mut total_duration = Duration::ZERO;

                for _ in 0..iters {
                    let temp_dir = TempDir::new().unwrap();
                    let (tx, rx) = mpsc::channel(10000);

                    let mut config = DiskBinaryConfig::default();
                    config.path = temp_dir.path().to_path_buf();
                    config.compression = compression;
                    config.flush_interval = Duration::from_millis(10);

                    let sink = DiskBinarySink::new(config, rx);

                    // Pre-create batches
                    let batches: Vec<Arc<Batch>> = (0..1000)
                        .map(|_| Arc::new(create_test_batch(100, 200)))
                        .collect();

                    let start = std::time::Instant::now();

                    // Run sink in background
                    let sink_handle = rt.spawn(async move { sink.run().await });

                    // Send all batches
                    rt.block_on(async {
                        for batch in batches {
                            tx.send(batch).await.unwrap();
                        }
                        drop(tx); // Close channel
                    });

                    // Wait for sink to finish
                    let _metrics = rt.block_on(sink_handle).unwrap();

                    total_duration += start.elapsed();
                }

                total_duration
            });
        });
    }

    group.finish();
}

/// Benchmark: Events per second throughput (end-to-end with disk I/O)
///
/// Tests all scenarios: 1000 batches per scenario
fn bench_throughput(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("sink_throughput");

    group.sample_size(10); // Fewer samples for I/O benchmark
    group.measurement_time(Duration::from_secs(15));

    let num_batches = 1000;

    for scenario in SCENARIOS {
        let total_events = num_batches * scenario.events_per_batch;

        group.throughput(Throughput::Elements(total_events as u64));
        group.bench_function(BenchmarkId::new("events", scenario.name), |b| {
            b.iter_custom(|iters| {
                let mut total_duration = Duration::ZERO;

                for _ in 0..iters {
                    let temp_dir = TempDir::new().unwrap();
                    let (tx, rx) = mpsc::channel(10000);

                    let mut config = DiskBinaryConfig::default();
                    config.path = temp_dir.path().to_path_buf();
                    config.compression = false;
                    config.flush_interval = Duration::from_millis(10);

                    let sink = DiskBinarySink::new(config, rx);

                    let batches: Vec<Arc<Batch>> = (0..num_batches)
                        .map(|_| Arc::new(create_batch_from_scenario(scenario)))
                        .collect();

                    let start = std::time::Instant::now();

                    let sink_handle = rt.spawn(async move { sink.run().await });

                    rt.block_on(async {
                        for batch in batches {
                            tx.send(batch).await.unwrap();
                        }
                        drop(tx);
                    });

                    let _metrics = rt.block_on(sink_handle).unwrap();
                    total_duration += start.elapsed();
                }

                total_duration
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
    bench_buffer_pool,
    bench_serialization,
    bench_rotation_submit,
    bench_full_sink,
    bench_throughput,
);

criterion_main!(benches);
