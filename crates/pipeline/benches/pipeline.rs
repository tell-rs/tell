//! Pipeline benchmark suite
//!
//! Benchmarks for the async router and pipeline operations.
//!
//! Run with: `cargo bench -p tell-pipeline`

use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use tell_pipeline::{Router, SinkHandle};
use tell_protocol::{Batch, BatchBuilder, BatchType, SourceId};
use tell_routing::RoutingTable;
use tell_tap::{SubscribeRequest, TapPoint};
use tokio::runtime::Runtime;
use tokio::sync::mpsc;

/// Create a test batch with the specified number of messages
fn create_test_batch(source_id: &str, message_count: usize) -> Batch {
    let mut builder = BatchBuilder::new(BatchType::Event, SourceId::new(source_id));
    builder.set_workspace_id(1);

    for i in 0..message_count {
        let msg = format!("test message {} with some additional data", i);
        builder.add(msg.as_bytes(), 1);
    }

    builder.finish()
}

/// Benchmark routing to a single sink
fn bench_route_single_sink(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("route_single_sink");

    for batch_size in [1, 10, 100, 500, 1000] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            &batch_size,
            |b, &size| {
                let mut table = RoutingTable::new();
                let sink_id = table.register_sink("default");
                table.set_default(vec![sink_id]);

                let mut router = Router::new(table);

                let (tx, mut rx) = mpsc::channel(10000);
                let handle = SinkHandle::new(sink_id, "benchmark_sink", tx);
                router.register_sink(handle);

                // Drain receiver in background
                rt.spawn(async move { while rx.recv().await.is_some() {} });

                b.to_async(&rt).iter(|| async {
                    let batch = create_test_batch("src", size);
                    black_box(router.route(batch).await)
                });
            },
        );
    }

    group.finish();
}

/// Benchmark routing to multiple sinks (fan-out)
fn bench_route_fanout(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("route_fanout");

    for num_sinks in [2, 3, 5, 10] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::from_parameter(num_sinks),
            &num_sinks,
            |b, &num_sinks| {
                let mut table = RoutingTable::new();
                let mut sink_ids = Vec::with_capacity(num_sinks);

                for i in 0..num_sinks {
                    sink_ids.push(table.register_sink(format!("sink_{}", i)));
                }

                table.set_default(sink_ids.clone());

                let mut router = Router::new(table);

                for (i, sink_id) in sink_ids.iter().enumerate() {
                    let (tx, mut rx) = mpsc::channel(10000);
                    let handle = SinkHandle::new(*sink_id, format!("sink_{}", i), tx);
                    router.register_sink(handle);

                    // Drain receiver in background
                    rt.spawn(async move { while rx.recv().await.is_some() {} });
                }

                b.to_async(&rt).iter(|| async {
                    let batch = create_test_batch("src", 100);
                    black_box(router.route(batch).await)
                });
            },
        );
    }

    group.finish();
}

/// Benchmark Arc wrapping overhead (the only allocation in hot path)
fn bench_arc_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("arc_overhead");

    for batch_size in [1, 100, 500, 1000] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            &batch_size,
            |b, &size| {
                let batch = create_test_batch("src", size);

                b.iter(|| {
                    let arc = Arc::new(black_box(batch.clone()));
                    // Simulate fan-out to 3 sinks
                    let _arc1 = Arc::clone(&arc);
                    let _arc2 = Arc::clone(&arc);
                    let _arc3 = Arc::clone(&arc);
                    black_box(arc)
                });
            },
        );
    }

    group.finish();
}

/// Benchmark routing table lookup
fn bench_routing_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("routing_lookup");

    for num_routes in [10, 100, 1000] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::from_parameter(num_routes),
            &num_routes,
            |b, &num_routes| {
                let mut table = RoutingTable::new();
                let sink_id = table.register_sink("default");

                // Add many routes
                for i in 0..num_routes {
                    table.add_route(SourceId::new(format!("source_{}", i)), vec![sink_id]);
                }

                // Lookup a specific source
                let lookup_source = SourceId::new(format!("source_{}", num_routes / 2));

                b.iter(|| black_box(table.route(&lookup_source)));
            },
        );
    }

    group.finish();
}

/// Benchmark metrics recording
fn bench_metrics(c: &mut Criterion) {
    use pipeline::RouterMetrics;

    let mut group = c.benchmark_group("metrics");

    group.bench_function("record_received", |b| {
        let metrics = RouterMetrics::new();
        b.iter(|| {
            metrics.record_received(100, 5000);
            black_box(&metrics)
        });
    });

    group.bench_function("record_routed", |b| {
        let metrics = RouterMetrics::new();
        b.iter(|| {
            metrics.record_routed();
            black_box(&metrics)
        });
    });

    group.bench_function("snapshot", |b| {
        let metrics = RouterMetrics::new();
        metrics.record_received(1000, 50000);
        metrics.record_routed();
        metrics.record_routed();
        metrics.record_backpressure();

        b.iter(|| black_box(metrics.snapshot()));
    });

    group.finish();
}

/// Benchmark channel operations (baseline for understanding pipeline overhead)
fn bench_channel_baseline(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("channel_baseline");

    group.bench_function("mpsc_send_recv", |b| {
        let (tx, mut rx) = mpsc::channel::<Arc<Batch>>(10000);

        // Drain in background
        rt.spawn(async move { while rx.recv().await.is_some() {} });

        let batch = Arc::new(create_test_batch("src", 100));

        b.to_async(&rt).iter(|| {
            let batch = Arc::clone(&batch);
            let tx = tx.clone();
            async move {
                black_box(tx.send(batch).await.unwrap());
            }
        });
    });

    group.bench_function("mpsc_try_send", |b| {
        let (tx, mut rx) = mpsc::channel::<Arc<Batch>>(10000);

        // Drain in background
        rt.spawn(async move { while rx.recv().await.is_some() {} });

        let batch = Arc::new(create_test_batch("src", 100));

        b.iter(|| {
            let batch = Arc::clone(&batch);
            black_box(tx.try_send(batch).unwrap());
        });
    });

    group.finish();
}

/// Benchmark throughput (batches per second)
fn bench_throughput(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("throughput");
    group.throughput(Throughput::Elements(1000));

    group.bench_function("1000_batches_single_sink", |b| {
        let mut table = RoutingTable::new();
        let sink_id = table.register_sink("default");
        table.set_default(vec![sink_id]);

        let mut router = Router::new(table);

        let (tx, mut rx) = mpsc::channel(100000);
        let handle = SinkHandle::new(sink_id, "benchmark_sink", tx);
        router.register_sink(handle);

        // Drain receiver in background
        rt.spawn(async move { while rx.recv().await.is_some() {} });

        b.to_async(&rt).iter(|| async {
            for _ in 0..1000 {
                let batch = create_test_batch("src", 100);
                router.route(batch).await;
            }
        });
    });

    group.bench_function("1000_batches_3_sinks", |b| {
        let mut table = RoutingTable::new();
        let sink1 = table.register_sink("sink1");
        let sink2 = table.register_sink("sink2");
        let sink3 = table.register_sink("sink3");
        table.set_default(vec![sink1, sink2, sink3]);

        let mut router = Router::new(table);

        for (i, sink_id) in [sink1, sink2, sink3].iter().enumerate() {
            let (tx, mut rx) = mpsc::channel(100000);
            let handle = SinkHandle::new(*sink_id, format!("sink_{}", i), tx);
            router.register_sink(handle);

            rt.spawn(async move { while rx.recv().await.is_some() {} });
        }

        b.to_async(&rt).iter(|| async {
            for _ in 0..1000 {
                let batch = create_test_batch("src", 100);
                router.route(batch).await;
            }
        });
    });

    group.finish();
}

/// Benchmark tap point overhead in the routing path
///
/// Tests three scenarios:
/// 1. No tap configured (baseline)
/// 2. Tap configured, no subscribers (idle - should be near-zero overhead)
/// 3. Tap configured with subscriber (streaming)
fn bench_tap_overhead(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("tap_overhead");
    group.throughput(Throughput::Elements(1));

    // Scenario 1: No tap configured (baseline)
    group.bench_function("no_tap", |b| {
        let mut table = RoutingTable::new();
        let sink_id = table.register_sink("default");
        table.set_default(vec![sink_id]);

        let mut router = Router::new(table);

        let (tx, mut rx) = mpsc::channel(10000);
        let handle = SinkHandle::new(sink_id, "benchmark_sink", tx);
        router.register_sink(handle);

        rt.spawn(async move { while rx.recv().await.is_some() {} });

        b.to_async(&rt).iter(|| async {
            let batch = create_test_batch("src", 100);
            black_box(router.route(batch).await)
        });
    });

    // Scenario 2: Tap configured, no subscribers (idle)
    group.bench_function("tap_idle", |b| {
        let mut table = RoutingTable::new();
        let sink_id = table.register_sink("default");
        table.set_default(vec![sink_id]);

        let mut router = Router::new(table);

        // Configure tap point but don't subscribe
        let tap_point = Arc::new(TapPoint::new());
        router.set_tap_point(Arc::clone(&tap_point));

        let (tx, mut rx) = mpsc::channel(10000);
        let handle = SinkHandle::new(sink_id, "benchmark_sink", tx);
        router.register_sink(handle);

        rt.spawn(async move { while rx.recv().await.is_some() {} });

        b.to_async(&rt).iter(|| async {
            let batch = create_test_batch("src", 100);
            black_box(router.route(batch).await)
        });
    });

    // Scenario 3: Tap configured with active subscriber (streaming)
    group.bench_function("tap_streaming", |b| {
        let mut table = RoutingTable::new();
        let sink_id = table.register_sink("default");
        table.set_default(vec![sink_id]);

        let mut router = Router::new(table);

        // Configure tap point WITH subscriber
        let tap_point = Arc::new(TapPoint::new());
        let request = SubscribeRequest::new();
        let (_sub_id, mut tap_rx) = tap_point.subscribe(&request).unwrap();
        router.set_tap_point(Arc::clone(&tap_point));

        let (tx, mut rx) = mpsc::channel(10000);
        let handle = SinkHandle::new(sink_id, "benchmark_sink", tx);
        router.register_sink(handle);

        // Drain both sink and tap receivers
        rt.spawn(async move { while rx.recv().await.is_some() {} });
        rt.spawn(async move { while tap_rx.recv().await.is_some() {} });

        b.to_async(&rt).iter(|| async {
            let batch = create_test_batch("src", 100);
            black_box(router.route(batch).await)
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_route_single_sink,
    bench_route_fanout,
    bench_arc_overhead,
    bench_routing_lookup,
    bench_metrics,
    bench_channel_baseline,
    bench_throughput,
    bench_tap_overhead,
);

criterion_main!(benches);
