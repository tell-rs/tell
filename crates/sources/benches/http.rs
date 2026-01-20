//! HTTP Source benchmark suite
//!
//! Benchmarks for the hot path in HTTP source processing.
//!
//! Run with: `cargo bench -p tell-sources --bench http`
//!
//! # What we're measuring
//!
//! - JSONL parsing performance
//! - JSON to FlatBuffer encoding
//! - JSON depth checking (DoS protection)
//! - Full HTTP request processing pipeline
//!
//! # Test Scenarios
//!
//! - Small: 10 events/logs per request
//! - Medium: 100 events/logs per request
//! - Large: 1000 events/logs per request

use criterion::{
    black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput,
};

use tell_protocol::{
    BatchEncoder, EncodedEvent, EncodedLogEntry, EventEncoder, EventTypeValue, FlatBatch,
    LogEncoder, LogLevelValue, SchemaType,
};

/// Test API key
const TEST_API_KEY: [u8; 16] = [
    0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
    0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
];

/// Test device ID (UUID bytes)
const TEST_DEVICE_ID: [u8; 16] = [
    0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4,
    0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00,
];

// =============================================================================
// Test Data Generation
// =============================================================================

/// Generate a single JSONL event line
fn generate_event_json(idx: usize) -> String {
    format!(
        r#"{{"type":"track","event":"page_view_{}","device_id":"550e8400-e29b-41d4-a716-446655440000","timestamp":1700000000000,"properties":{{"page":"/page/{}","referrer":"google"}}}}"#,
        idx, idx
    )
}

/// Generate a single JSONL log line
fn generate_log_json(idx: usize) -> String {
    format!(
        r#"{{"level":"info","message":"Request processed #{}","timestamp":1700000000000,"service":"api","source":"web-01","data":{{"request_id":"req_{}"}}}}"#,
        idx, idx
    )
}

/// Generate JSONL body with N events
fn generate_events_jsonl(count: usize) -> Vec<u8> {
    let lines: Vec<String> = (0..count).map(generate_event_json).collect();
    lines.join("\n").into_bytes()
}

/// Generate JSONL body with N logs
fn generate_logs_jsonl(count: usize) -> Vec<u8> {
    let lines: Vec<String> = (0..count).map(generate_log_json).collect();
    lines.join("\n").into_bytes()
}

/// Generate EncodedEvent structs for encoding
fn generate_encoded_events(count: usize) -> Vec<EncodedEvent> {
    (0..count)
        .map(|i| EncodedEvent {
            event_type: EventTypeValue::Track,
            timestamp: 1700000000000,
            device_id: Some(TEST_DEVICE_ID),
            session_id: None,
            event_name: Some(format!("page_view_{}", i)),
            payload: format!(r#"{{"page":"/page/{}","referrer":"google"}}"#, i).into_bytes(),
        })
        .collect()
}

/// Generate EncodedLogEntry structs for encoding
fn generate_encoded_logs(count: usize) -> Vec<EncodedLogEntry> {
    (0..count)
        .map(|i| EncodedLogEntry {
            event_type: tell_protocol::LogEventTypeValue::Log,
            session_id: None,
            level: LogLevelValue::Info,
            timestamp: 1700000000000,
            source: Some("web-01".to_string()),
            service: Some("api".to_string()),
            payload: format!(r#"{{"message":"Request #{}","request_id":"req_{}"}}"#, i, i)
                .into_bytes(),
        })
        .collect()
}

/// Generate deeply nested JSON for depth check benchmarks
fn generate_nested_json(depth: usize) -> Vec<u8> {
    let mut json = String::with_capacity(depth * 10);
    for _ in 0..depth {
        json.push_str(r#"{"a":"#);
    }
    json.push_str(r#""value""#);
    for _ in 0..depth {
        json.push('}');
    }
    json.into_bytes()
}

// =============================================================================
// JSONL Parsing Benchmarks
// =============================================================================

/// Benchmark: Parse JSONL events
fn bench_jsonl_parse_events(c: &mut Criterion) {
    let mut group = c.benchmark_group("http_jsonl_parse_events");

    for count in [10, 100, 1000] {
        let jsonl = generate_events_jsonl(count);

        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(
            BenchmarkId::new("parse", count),
            &jsonl,
            |b, jsonl| {
                b.iter(|| {
                    let mut events = Vec::new();
                    for line in jsonl.split(|&b| b == b'\n') {
                        if line.is_empty() {
                            continue;
                        }
                        if let Ok(event) = serde_json::from_slice::<serde_json::Value>(line) {
                            events.push(event);
                        }
                    }
                    black_box(events)
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: Parse JSONL logs
fn bench_jsonl_parse_logs(c: &mut Criterion) {
    let mut group = c.benchmark_group("http_jsonl_parse_logs");

    for count in [10, 100, 1000] {
        let jsonl = generate_logs_jsonl(count);

        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(
            BenchmarkId::new("parse", count),
            &jsonl,
            |b, jsonl| {
                b.iter(|| {
                    let mut logs = Vec::new();
                    for line in jsonl.split(|&b| b == b'\n') {
                        if line.is_empty() {
                            continue;
                        }
                        if let Ok(log) = serde_json::from_slice::<serde_json::Value>(line) {
                            logs.push(log);
                        }
                    }
                    black_box(logs)
                });
            },
        );
    }

    group.finish();
}

// =============================================================================
// FlatBuffer Encoding Benchmarks
// =============================================================================

/// Benchmark: Encode events to FlatBuffer
fn bench_encode_events(c: &mut Criterion) {
    let mut group = c.benchmark_group("http_encode_events");

    for count in [10, 100, 1000] {
        let events = generate_encoded_events(count);

        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(
            BenchmarkId::new("encode", count),
            &events,
            |b, events| {
                b.iter(|| {
                    let event_data = EventEncoder::encode_events(events);
                    black_box(event_data)
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: Encode logs to FlatBuffer
fn bench_encode_logs(c: &mut Criterion) {
    let mut group = c.benchmark_group("http_encode_logs");

    for count in [10, 100, 1000] {
        let logs = generate_encoded_logs(count);

        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::new("encode", count), &logs, |b, logs| {
            b.iter(|| {
                let log_data = LogEncoder::encode_logs(logs);
                black_box(log_data)
            });
        });
    }

    group.finish();
}

/// Benchmark: Full batch encoding (events + batch wrapper)
fn bench_full_batch_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("http_full_batch");

    for count in [10, 100, 1000] {
        let events = generate_encoded_events(count);

        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(
            BenchmarkId::new("encode_batch", count),
            &events,
            |b, events| {
                b.iter(|| {
                    // Encode events
                    let event_data = EventEncoder::encode_events(events);

                    // Wrap in batch
                    let mut batch_encoder = BatchEncoder::new();
                    batch_encoder.set_api_key(&TEST_API_KEY);
                    batch_encoder.set_schema_type(SchemaType::Event);
                    let batch = batch_encoder.encode(&event_data);

                    black_box(batch)
                });
            },
        );
    }

    group.finish();
}

// =============================================================================
// Round-trip Benchmarks
// =============================================================================

/// Benchmark: Full encode + decode round-trip
fn bench_roundtrip(c: &mut Criterion) {
    let mut group = c.benchmark_group("http_roundtrip");

    for count in [10, 100] {
        let events = generate_encoded_events(count);

        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(
            BenchmarkId::new("events", count),
            &events,
            |b, events| {
                b.iter(|| {
                    // Encode
                    let event_data = EventEncoder::encode_events(events);
                    let mut batch_encoder = BatchEncoder::new();
                    batch_encoder.set_api_key(&TEST_API_KEY);
                    batch_encoder.set_schema_type(SchemaType::Event);
                    let batch_bytes = batch_encoder.encode(&event_data);

                    // Decode and verify (black_box each to prevent optimization)
                    let batch = FlatBatch::parse(&batch_bytes).expect("valid");
                    black_box(batch.schema_type());
                    black_box(batch.api_key().expect("api_key"));
                    black_box(batch.data().expect("data").len());

                    batch_bytes.len()
                });
            },
        );
    }

    group.finish();
}

// =============================================================================
// DoS Protection Benchmarks
// =============================================================================

/// Check if JSON exceeds maximum nesting depth (copy of handler function)
fn exceeds_json_depth(data: &[u8], max_depth: usize) -> bool {
    let mut depth: usize = 0;
    let mut in_string = false;
    let mut escape_next = false;

    for &byte in data {
        if escape_next {
            escape_next = false;
            continue;
        }

        match byte {
            b'\\' if in_string => {
                escape_next = true;
            }
            b'"' => {
                in_string = !in_string;
            }
            b'{' | b'[' if !in_string => {
                depth += 1;
                if depth > max_depth {
                    return true;
                }
            }
            b'}' | b']' if !in_string => {
                depth = depth.saturating_sub(1);
            }
            _ => {}
        }
    }

    false
}

/// Benchmark: JSON depth checking
fn bench_depth_check(c: &mut Criterion) {
    let mut group = c.benchmark_group("http_depth_check");

    // Normal depth (should pass)
    let normal_json = generate_nested_json(10);
    group.throughput(Throughput::Bytes(normal_json.len() as u64));
    group.bench_with_input(
        BenchmarkId::new("depth", "10_levels"),
        &normal_json,
        |b, json| {
            b.iter(|| black_box(exceeds_json_depth(json, 32)));
        },
    );

    // Deep nesting (should fail quickly)
    let deep_json = generate_nested_json(50);
    group.throughput(Throughput::Bytes(deep_json.len() as u64));
    group.bench_with_input(
        BenchmarkId::new("depth", "50_levels"),
        &deep_json,
        |b, json| {
            b.iter(|| black_box(exceeds_json_depth(json, 32)));
        },
    );

    // Large flat JSON (many fields, no nesting)
    let flat_json = generate_events_jsonl(100);
    group.throughput(Throughput::Bytes(flat_json.len() as u64));
    group.bench_with_input(
        BenchmarkId::new("depth", "flat_100_events"),
        &flat_json,
        |b, json| {
            b.iter(|| {
                for line in json.split(|&b| b == b'\n') {
                    black_box(exceeds_json_depth(line, 32));
                }
            });
        },
    );

    group.finish();
}

// =============================================================================
// Throughput Benchmarks
// =============================================================================

/// Benchmark: End-to-end throughput (parse JSONL + encode to FlatBuffer)
fn bench_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("http_throughput");

    // Simulate 1000 events in a single request
    let jsonl = generate_events_jsonl(1000);
    let events = generate_encoded_events(1000);

    // Measure bytes/sec for JSONL parsing
    group.throughput(Throughput::Bytes(jsonl.len() as u64));
    group.bench_function("parse_1000_events", |b| {
        b.iter(|| {
            let mut parsed = Vec::new();
            for line in jsonl.split(|&b| b == b'\n') {
                if !line.is_empty() {
                    if let Ok(v) = serde_json::from_slice::<serde_json::Value>(line) {
                        parsed.push(v);
                    }
                }
            }
            black_box(parsed)
        });
    });

    // Measure events/sec for encoding
    group.throughput(Throughput::Elements(1000));
    group.bench_function("encode_1000_events", |b| {
        b.iter(|| {
            let event_data = EventEncoder::encode_events(&events);
            let mut batch_encoder = BatchEncoder::new();
            batch_encoder.set_api_key(&TEST_API_KEY);
            batch_encoder.set_schema_type(SchemaType::Event);
            let batch = batch_encoder.encode(&event_data);
            black_box(batch)
        });
    });

    group.finish();
}

// =============================================================================
// Criterion Setup
// =============================================================================

criterion_group!(
    benches,
    bench_jsonl_parse_events,
    bench_jsonl_parse_logs,
    bench_encode_events,
    bench_encode_logs,
    bench_full_batch_encode,
    bench_roundtrip,
    bench_depth_check,
    bench_throughput,
);

criterion_main!(benches);
