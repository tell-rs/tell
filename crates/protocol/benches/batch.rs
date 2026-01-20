//! Benchmarks for Batch zero-copy verification
//!
//! These benchmarks verify that:
//! 1. Batch cloning is O(1) - no data copying
//! 2. Message access is zero-allocation
//! 3. BatchBuilder is efficient for accumulation

use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};

use tell_protocol::{Batch, BatchBuilder, BatchType, SourceId};

/// Create a test batch with N messages of given size
fn create_batch(message_count: usize, message_size: usize) -> Batch {
    let source_id = SourceId::new("bench_source");
    let mut builder = BatchBuilder::new(BatchType::Event, source_id);
    builder.set_max_items(message_count + 1); // Prevent early flush

    let message = vec![0xABu8; message_size];
    for _ in 0..message_count {
        builder.add(&message, 1);
    }

    builder.finish()
}

/// Benchmark batch cloning - should be O(1) regardless of size
fn bench_batch_clone(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_clone");

    for size in [100, 1000, 10000] {
        let batch = create_batch(size, 100);

        group.throughput(Throughput::Elements(1));
        group.bench_function(format!("{}_messages", size), |b| {
            b.iter(|| {
                let cloned = black_box(batch.clone());
                black_box(cloned)
            })
        });
    }

    group.finish();
}

/// Benchmark message access - should be zero allocation
fn bench_message_access(c: &mut Criterion) {
    let mut group = c.benchmark_group("message_access");

    let batch = create_batch(500, 100);

    group.throughput(Throughput::Elements(500));
    group.bench_function("sequential_access", |b| {
        b.iter(|| {
            for i in 0..batch.message_count() {
                black_box(batch.get_message(i));
            }
        })
    });

    group.bench_function("iterator_access", |b| {
        b.iter(|| {
            for msg in batch.messages() {
                black_box(msg);
            }
        })
    });

    group.finish();
}

/// Benchmark batch building
fn bench_batch_builder(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_builder");

    let message = vec![0xABu8; 100];

    group.throughput(Throughput::Elements(500));
    group.bench_function("build_500_messages", |b| {
        b.iter(|| {
            let source_id = SourceId::new("bench");
            let mut builder = BatchBuilder::new(BatchType::Event, source_id);
            builder.set_max_items(501);

            for _ in 0..500 {
                builder.add(&message, 1);
            }

            black_box(builder.finish())
        })
    });

    group.finish();
}

/// Benchmark to verify clone shares memory (pointer comparison)
fn bench_clone_memory_sharing(c: &mut Criterion) {
    let batch = create_batch(100, 1000); // 100KB of data

    c.bench_function("verify_zero_copy_clone", |b| {
        b.iter(|| {
            let cloned = batch.clone();

            // Verify pointers are the same (zero-copy)
            let ptr1 = batch.get_message(0).unwrap().as_ptr();
            let ptr2 = cloned.get_message(0).unwrap().as_ptr();

            // This assertion should always pass - if it doesn't,
            // zero-copy is broken
            assert_eq!(ptr1, ptr2);

            black_box((ptr1, ptr2))
        })
    });
}

criterion_group!(
    benches,
    bench_batch_clone,
    bench_message_access,
    bench_batch_builder,
    bench_clone_memory_sharing,
);

criterion_main!(benches);
