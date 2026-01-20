//! Syslog Source benchmark suite
//!
//! Benchmarks for syslog TCP and UDP source processing.
//!
//! Run with: `cargo bench -p tell-sources --bench syslog`
//!
//! # What we measure
//!
//! - Line finding in buffer (TCP framing)
//! - Newline trimming
//! - BatchBuilder throughput with syslog messages
//! - Full message processing simulation

use std::net::{IpAddr, Ipv4Addr};

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use tell_protocol::{BatchBuilder, BatchType, SourceId};

// =============================================================================
// Test Data
// =============================================================================

/// Create a syslog message of specified size
fn create_syslog_message(size: usize) -> Vec<u8> {
    // Start with RFC 3164 prefix
    let prefix = b"<134>Jan  7 12:00:00 host app: ";
    let mut msg = prefix.to_vec();

    // Fill remaining with realistic log content
    let filler = b"log message content ";
    while msg.len() < size {
        let to_add = (size - msg.len()).min(filler.len());
        msg.extend_from_slice(&filler[..to_add]);
    }
    msg.truncate(size);
    msg
}

/// Create a buffer with multiple newline-delimited messages
fn create_line_buffer(msg_size: usize, num_messages: usize) -> Vec<u8> {
    let msg = create_syslog_message(msg_size);
    let mut buffer = Vec::with_capacity((msg_size + 1) * num_messages);
    for _ in 0..num_messages {
        buffer.extend_from_slice(&msg);
        buffer.push(b'\n');
    }
    buffer
}

// =============================================================================
// Line Finding Benchmarks (TCP framing)
// =============================================================================

/// Benchmark: Find newlines in buffer (TCP line-based framing)
fn bench_line_finding(c: &mut Criterion) {
    let mut group = c.benchmark_group("syslog_line_finding");

    for num_messages in [10, 100, 500] {
        let buffer = create_line_buffer(200, num_messages);

        group.throughput(Throughput::Elements(num_messages as u64));
        group.bench_with_input(
            BenchmarkId::new("find_lines", num_messages),
            &buffer,
            |b, buffer| {
                b.iter(|| {
                    let mut count = 0;
                    let mut start = 0;
                    for (i, &byte) in buffer.iter().enumerate() {
                        if byte == b'\n' {
                            black_box(&buffer[start..i]);
                            start = i + 1;
                            count += 1;
                        }
                    }
                    black_box(count)
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: memchr-style newline finding (optimized)
fn bench_line_finding_memchr(c: &mut Criterion) {
    let mut group = c.benchmark_group("syslog_line_finding_memchr");

    for num_messages in [10, 100, 500] {
        let buffer = create_line_buffer(200, num_messages);

        group.throughput(Throughput::Elements(num_messages as u64));
        group.bench_with_input(
            BenchmarkId::new("find_lines", num_messages),
            &buffer,
            |b, buffer| {
                b.iter(|| {
                    let mut count = 0;
                    let mut remaining = &buffer[..];
                    while let Some(pos) = remaining.iter().position(|&b| b == b'\n') {
                        black_box(&remaining[..pos]);
                        remaining = &remaining[pos + 1..];
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
// Newline Trimming Benchmarks
// =============================================================================

/// Trim trailing newline (matches UDP implementation)
#[inline]
fn trim_trailing_newline(data: &[u8]) -> &[u8] {
    let mut end = data.len();
    if end > 0 && data[end - 1] == b'\n' {
        end -= 1;
        if end > 0 && data[end - 1] == b'\r' {
            end -= 1;
        }
    }
    &data[..end]
}

/// Benchmark: Newline trimming
fn bench_newline_trim(c: &mut Criterion) {
    let mut group = c.benchmark_group("syslog_newline_trim");

    // Different message endings
    let msg_lf = b"<134>Jan  7 12:00:00 host app: test message\n";
    let msg_crlf = b"<134>Jan  7 12:00:00 host app: test message\r\n";
    let msg_none = b"<134>Jan  7 12:00:00 host app: test message";

    group.bench_function("with_lf", |b| {
        b.iter(|| black_box(trim_trailing_newline(msg_lf)));
    });

    group.bench_function("with_crlf", |b| {
        b.iter(|| black_box(trim_trailing_newline(msg_crlf)));
    });

    group.bench_function("no_newline", |b| {
        b.iter(|| black_box(trim_trailing_newline(msg_none)));
    });

    group.finish();
}

// =============================================================================
// BatchBuilder Benchmarks
// =============================================================================

/// Benchmark: Adding syslog messages to BatchBuilder
fn bench_batch_building(c: &mut Criterion) {
    let mut group = c.benchmark_group("syslog_batch_building");

    // Test with different message sizes
    for msg_size in [100, 200, 500, 1000] {
        let message = create_syslog_message(msg_size);
        let num_messages = 500; // Go default batch size

        group.throughput(Throughput::Elements(num_messages as u64));
        group.bench_with_input(
            BenchmarkId::new("add_messages", msg_size),
            &message,
            |b, message| {
                b.iter(|| {
                    let source_id = SourceId::new("syslog-bench");
                    let mut builder = BatchBuilder::new(BatchType::Syslog, source_id);
                    builder.set_workspace_id(12345);
                    builder.set_source_ip(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 100)));

                    for _ in 0..num_messages {
                        builder.add(message, 1);
                    }

                    black_box(builder.finish())
                });
            },
        );
    }

    group.finish();
}

// =============================================================================
// Full Pipeline Simulation
// =============================================================================

/// Benchmark: Full syslog message processing (TCP style)
fn bench_tcp_pipeline(c: &mut Criterion) {
    let mut group = c.benchmark_group("syslog_tcp_pipeline");

    for num_messages in [100, 500, 1000] {
        let buffer = create_line_buffer(200, num_messages);

        group.throughput(Throughput::Elements(num_messages as u64));
        group.bench_with_input(
            BenchmarkId::new("process_messages", num_messages),
            &buffer,
            |b, buffer| {
                b.iter(|| {
                    let source_id = SourceId::new("syslog-tcp-bench");
                    let mut builder = BatchBuilder::new(BatchType::Syslog, source_id);
                    builder.set_workspace_id(12345);
                    builder.set_source_ip(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 100)));

                    let mut count = 0;
                    let mut start = 0;

                    for (i, &byte) in buffer.iter().enumerate() {
                        if byte == b'\n' {
                            let line = &buffer[start..i];
                            if !line.is_empty() {
                                builder.add(line, 1);
                                count += 1;
                            }
                            start = i + 1;
                        }
                    }

                    black_box(builder.finish());
                    black_box(count)
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: Full syslog message processing (UDP style)
fn bench_udp_pipeline(c: &mut Criterion) {
    let mut group = c.benchmark_group("syslog_udp_pipeline");

    for num_packets in [100, 500, 1000] {
        // Pre-create packets with newlines (some UDP clients add them)
        let packets: Vec<Vec<u8>> = (0..num_packets)
            .map(|_| {
                let mut p = create_syslog_message(200);
                p.push(b'\n');
                p
            })
            .collect();

        group.throughput(Throughput::Elements(num_packets as u64));
        group.bench_with_input(
            BenchmarkId::new("process_packets", num_packets),
            &packets,
            |b, packets| {
                b.iter(|| {
                    let source_id = SourceId::new("syslog-udp-bench");
                    let mut builder = BatchBuilder::new(BatchType::Syslog, source_id);
                    builder.set_workspace_id(12345);

                    for packet in packets {
                        // Trim newline
                        let message = trim_trailing_newline(packet);
                        if !message.is_empty() {
                            builder.set_source_ip(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
                            builder.add(message, 1);
                        }
                    }

                    black_box(builder.finish())
                });
            },
        );
    }

    group.finish();
}

// =============================================================================
// Criterion Setup
// =============================================================================

criterion_group!(
    benches,
    bench_line_finding,
    bench_line_finding_memchr,
    bench_newline_trim,
    bench_batch_building,
    bench_tcp_pipeline,
    bench_udp_pipeline,
);

criterion_main!(benches);
