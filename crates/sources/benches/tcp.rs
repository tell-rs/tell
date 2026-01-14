//! TCP Source benchmark suite
//!
//! Benchmarks for the hot path in TCP source processing.
//!
//! Run with: `cargo bench -p cdp-sources --bench tcp`
//!
//! # Terminology
//!
//! - **Event**: Single business event/log (e.g., page view, log line)
//! - **Batch**: Container holding multiple events (application layer)
//! - **Wire message**: FlatBuffer-encoded batch sent over TCP (what we measure here)
//!
//! The TCP source operates at the **wire message** level - it doesn't parse
//! the event contents inside the data payload. The data payload is opaque bytes
//! that get passed to sinks for processing.
//!
//! # What we're measuring
//!
//! - Wire message framing (length-prefix parsing)
//! - FlatBuffer parsing (FlatBatch::parse)
//! - API key validation
//! - IP address conversion
//! - Full wire message processing pipeline
//!
//! # Test Scenarios
//!
//! Uses payload sizes based on `cdp_bench` scenarios:
//! - Small: ~1KB payload (realtime_small: 10 events × 100 bytes)
//! - Medium: ~20KB payload (typical: 100 events × 200 bytes)
//! - Large: ~100KB payload (high_volume: 500 events × 200 bytes)

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::Arc;

use bytes::{Buf, BytesMut};
use cdp_auth::ApiKeyStore;
use cdp_client::BatchBuilder;
use cdp_bench::{BenchScenario, SCENARIOS};
use cdp_protocol::{FlatBatch, SchemaType};
use cdp_sources::tcp::{bytes_to_ip, ip_to_bytes};
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};

/// Test API key (hex: 0102030405060708090a0b0c0d0e0f10)
const TEST_API_KEY: [u8; 16] = [
    0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
];

/// Create a valid FlatBuffer wire message with given payload size
fn create_wire_message(payload_size: usize) -> Vec<u8> {
    let data = vec![0x42u8; payload_size];
    BatchBuilder::new()
        .api_key(TEST_API_KEY)
        .schema_type(SchemaType::Event)
        .data(&data)
        .build()
        .expect("valid batch")
        .as_bytes()
        .to_vec()
}

/// Create a wire message with payload size based on a benchmark scenario
fn create_wire_message_from_scenario(scenario: &BenchScenario) -> Vec<u8> {
    // Payload size = events_per_batch * event_size (simulated batch payload)
    let payload_size = scenario.events_per_batch * scenario.event_size;
    create_wire_message(payload_size)
}

/// Create a length-prefixed wire message (TCP framing format)
fn create_framed_message(payload_size: usize) -> Vec<u8> {
    let msg = create_wire_message(payload_size);
    let len = msg.len() as u32;
    let mut framed = Vec::with_capacity(4 + msg.len());
    framed.extend_from_slice(&len.to_be_bytes());
    framed.extend_from_slice(&msg);
    framed
}

/// Create a framed message from a benchmark scenario
fn create_framed_message_from_scenario(scenario: &BenchScenario) -> Vec<u8> {
    let payload_size = scenario.events_per_batch * scenario.event_size;
    create_framed_message(payload_size)
}

/// Create an auth store with the test key
fn create_auth_store() -> Arc<ApiKeyStore> {
    let store = ApiKeyStore::new();
    // Add key in hex format: "0102030405060708090a0b0c0d0e0f10:test_workspace"
    let key_line = "0102030405060708090a0b0c0d0e0f10:1";
    store.reload_from_str(key_line).expect("valid key line");
    Arc::new(store)
}

// =============================================================================
// Wire Message Framing Benchmarks
// =============================================================================

/// Benchmark: Parse length prefix from buffer
///
/// This simulates the hot path in try_read_message()
fn bench_message_framing(c: &mut Criterion) {
    let mut group = c.benchmark_group("tcp_framing");

    // Test with realistic payload sizes from shared scenarios
    for scenario in SCENARIOS {
        let framed = create_framed_message_from_scenario(scenario);
        let msg_len = framed.len();

        group.throughput(Throughput::Bytes(msg_len as u64));
        group.bench_with_input(
            BenchmarkId::new("parse_frame", scenario.name),
            &framed,
            |b, framed| {
                b.iter(|| {
                    let mut buf = BytesMut::from(&framed[..]);

                    // Parse length prefix (4 bytes, big-endian)
                    let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
                    buf.advance(4);

                    // Extract message (zero-copy split)
                    let msg = buf.split_to(len as usize);
                    black_box(msg)
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: Multiple wire messages in a single buffer (batch processing)
///
/// Tests processing multiple wire messages from a TCP read buffer.
/// Uses "typical" scenario (100 events × 200 bytes payload).
fn bench_multi_message_framing(c: &mut Criterion) {
    let mut group = c.benchmark_group("tcp_multi_framing");

    // Use typical scenario for wire message size
    let typical = &SCENARIOS[1]; // typical: 100 events × 200 bytes
    let single_framed = create_framed_message_from_scenario(typical);

    for num_messages in [10, 100, 500] {
        // Create buffer with multiple framed messages
        let mut multi_buf = Vec::with_capacity(single_framed.len() * num_messages);
        for _ in 0..num_messages {
            multi_buf.extend_from_slice(&single_framed);
        }

        group.throughput(Throughput::Elements(num_messages as u64));
        group.bench_with_input(
            BenchmarkId::new("parse_batch", num_messages),
            &multi_buf,
            |b, multi_buf| {
                b.iter(|| {
                    let mut buf = BytesMut::from(&multi_buf[..]);
                    let mut count = 0;

                    while buf.len() >= 4 {
                        let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
                        if buf.len() < 4 + len {
                            break;
                        }
                        buf.advance(4);
                        let msg = buf.split_to(len);
                        black_box(&msg);
                        count += 1;
                    }

                    black_box(count)
                });
            },
        );
    }

    group.finish();
}

// =============================================================================
// FlatBuffer Parsing Benchmarks
// =============================================================================

/// Benchmark: FlatBatch::parse (zero-copy FlatBuffer parsing)
fn bench_flatbuffer_parse(c: &mut Criterion) {
    let mut group = c.benchmark_group("tcp_flatbuffer");

    // Test with realistic payload sizes from shared scenarios
    for scenario in SCENARIOS {
        let msg = create_wire_message_from_scenario(scenario);

        group.throughput(Throughput::Bytes(msg.len() as u64));
        group.bench_with_input(
            BenchmarkId::new("parse", scenario.name),
            &msg,
            |b, msg| {
                b.iter(|| {
                    let batch = FlatBatch::parse(msg).expect("valid flatbuffer");
                    black_box(batch)
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: FlatBatch field access
fn bench_flatbuffer_access(c: &mut Criterion) {
    let mut group = c.benchmark_group("tcp_flatbuffer_access");

    // Use typical scenario for field access tests
    let typical = &SCENARIOS[1];
    let msg = create_wire_message_from_scenario(typical);
    let batch = FlatBatch::parse(&msg).expect("valid flatbuffer");

    group.bench_function("schema_type", |b| {
        b.iter(|| black_box(batch.schema_type()));
    });

    group.bench_function("api_key_bytes", |b| {
        b.iter(|| black_box(batch.api_key_bytes()));
    });

    group.bench_function("data", |b| {
        b.iter(|| black_box(batch.data()));
    });

    group.bench_function("all_fields", |b| {
        b.iter(|| {
            let _ = batch.schema_type();
            let _ = batch.api_key_bytes();
            let _ = batch.data();
            black_box(())
        });
    });

    group.finish();
}

// =============================================================================
// API Key Validation Benchmarks
// =============================================================================

/// Benchmark: API key lookup in auth store
fn bench_api_key_validation(c: &mut Criterion) {
    let mut group = c.benchmark_group("tcp_auth");

    let store = create_auth_store();

    // Valid key lookup
    group.bench_function("validate_valid_key", |b| {
        b.iter(|| {
            let result = store.validate_slice(&TEST_API_KEY);
            black_box(result)
        });
    });

    // Invalid key lookup (cache miss)
    let invalid_key = [0xffu8; 16];
    group.bench_function("validate_invalid_key", |b| {
        b.iter(|| {
            let result = store.validate_slice(&invalid_key);
            black_box(result)
        });
    });

    group.finish();
}

/// Benchmark: Auth store with many keys (scalability)
fn bench_api_key_scalability(c: &mut Criterion) {
    let mut group = c.benchmark_group("tcp_auth_scale");

    for num_keys in [10, 100, 1000] {
        let store = ApiKeyStore::new();

        // Add many keys
        for i in 0..num_keys {
            let key_hex = format!("{:032x}", i);
            let line = format!("{}:{}", key_hex, i);
            store.reload_from_str(&line).expect("valid key");
        }

        // Also add our test key
        store
            .reload_from_str("0102030405060708090a0b0c0d0e0f10:99999")
            .expect("valid key");

        let store = Arc::new(store);

        group.bench_with_input(BenchmarkId::new("lookup", num_keys), &store, |b, store| {
            b.iter(|| {
                let result = store.validate_slice(&TEST_API_KEY);
                black_box(result)
            });
        });
    }

    group.finish();
}

// =============================================================================
// IP Conversion Benchmarks
// =============================================================================

/// Benchmark: IP address conversion (hot path for every message)
fn bench_ip_conversion(c: &mut Criterion) {
    let mut group = c.benchmark_group("tcp_ip");

    // IPv4 conversion
    let ipv4 = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 100));
    group.bench_function("ipv4_to_bytes", |b| {
        b.iter(|| black_box(ip_to_bytes(ipv4)));
    });

    let ipv4_bytes: [u8; 16] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 192, 168, 1, 100];
    group.bench_function("bytes_to_ipv4", |b| {
        b.iter(|| black_box(bytes_to_ip(&ipv4_bytes)));
    });

    // IPv6 conversion
    let ipv6 = IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1));
    group.bench_function("ipv6_to_bytes", |b| {
        b.iter(|| black_box(ip_to_bytes(ipv6)));
    });

    let ipv6_bytes: [u8; 16] = [0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
    group.bench_function("bytes_to_ipv6", |b| {
        b.iter(|| black_box(bytes_to_ip(&ipv6_bytes)));
    });

    // Roundtrip
    group.bench_function("ipv4_roundtrip", |b| {
        b.iter(|| {
            let bytes = ip_to_bytes(ipv4);
            black_box(bytes_to_ip(&bytes))
        });
    });

    group.finish();
}

// =============================================================================
// Combined Hot Path Benchmarks
// =============================================================================

/// Benchmark: Full wire message processing pipeline (simulated)
///
/// This measures the combined overhead of:
/// 1. Frame parsing
/// 2. FlatBuffer parsing
/// 3. API key validation
/// 4. IP conversion
fn bench_full_pipeline(c: &mut Criterion) {
    let mut group = c.benchmark_group("tcp_pipeline");

    let auth_store = create_auth_store();
    let conn_ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));

    // Test with realistic payload sizes from shared scenarios
    for scenario in SCENARIOS {
        let framed = create_framed_message_from_scenario(scenario);

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("process_message", scenario.name),
            &framed,
            |b, framed| {
                b.iter(|| {
                    // 1. Parse frame
                    let mut buf = BytesMut::from(&framed[..]);
                    let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
                    buf.advance(4);
                    let msg = buf.split_to(len);

                    // 2. Parse FlatBuffer
                    let batch = FlatBatch::parse(&msg).expect("valid");

                    // 3. Validate API key
                    let api_key = batch.api_key_bytes().expect("has key");
                    let _workspace = auth_store.validate_slice(api_key);

                    // 4. Convert IP
                    let _ip_bytes = ip_to_bytes(conn_ip);

                    // 5. Get schema type (for routing)
                    let _schema = batch.schema_type();

                    // 6. Get data (for batch building)
                    let _data = batch.data();

                    black_box(())
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: Throughput test (wire messages per second)
///
/// Measures end-to-end throughput processing 1000 wire messages.
/// Uses "typical" scenario (100 events × 200 bytes payload per wire message).
fn bench_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("tcp_throughput");

    let auth_store = create_auth_store();
    let conn_ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));

    // Use typical scenario for wire message size
    let typical = &SCENARIOS[1]; // typical: 100 events × 200 bytes
    let num_messages = 1000;
    let single_framed = create_framed_message_from_scenario(typical);

    let mut multi_buf = Vec::with_capacity(single_framed.len() * num_messages);
    for _ in 0..num_messages {
        multi_buf.extend_from_slice(&single_framed);
    }

    // Each wire message contains typical.events_per_batch events
    let total_events = num_messages * typical.events_per_batch;

    // Measure wire messages/sec
    group.throughput(Throughput::Elements(num_messages as u64));
    group.bench_function("wire_messages_1000", |b| {
        b.iter(|| {
            let mut buf = BytesMut::from(&multi_buf[..]);
            let mut processed = 0;

            while buf.len() >= 4 {
                // Parse frame
                let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
                if buf.len() < 4 + len {
                    break;
                }
                buf.advance(4);
                let msg = buf.split_to(len);

                // Parse + validate
                if let Ok(batch) = FlatBatch::parse(&msg) {
                    if let Ok(api_key) = batch.api_key_bytes() {
                        let _ = auth_store.validate_slice(api_key);
                    }
                    let _ = ip_to_bytes(conn_ip);
                    let _ = batch.schema_type();
                    let _ = batch.data();
                }

                processed += 1;
            }

            black_box(processed)
        });
    });

    // Also report events/sec (wire_messages × events_per_batch)
    group.throughput(Throughput::Elements(total_events as u64));
    group.bench_function("events_equivalent", |b| {
        b.iter(|| {
            let mut buf = BytesMut::from(&multi_buf[..]);
            let mut processed = 0;

            while buf.len() >= 4 {
                let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
                if buf.len() < 4 + len {
                    break;
                }
                buf.advance(4);
                let msg = buf.split_to(len);

                if let Ok(batch) = FlatBatch::parse(&msg) {
                    if let Ok(api_key) = batch.api_key_bytes() {
                        let _ = auth_store.validate_slice(api_key);
                    }
                    let _ = ip_to_bytes(conn_ip);
                    let _ = batch.schema_type();
                    let _ = batch.data();
                }

                processed += 1;
            }

            black_box(processed)
        });
    });

    group.finish();
}

// =============================================================================
// Criterion Setup
// =============================================================================

criterion_group!(
    benches,
    bench_message_framing,
    bench_multi_message_framing,
    bench_flatbuffer_parse,
    bench_flatbuffer_access,
    bench_api_key_validation,
    bench_api_key_scalability,
    bench_ip_conversion,
    bench_full_pipeline,
    bench_throughput,
);

criterion_main!(benches);
