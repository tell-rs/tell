# CLAUDE.md

This file provides guidance to Claude when working with code in this repository.

## Project Overview

CDP Collector (Rust) is a high-performance, multi-protocol data streaming engine. It processes CDP/product analytics events and structured logs at 40M+ events/sec with minimal resource footprint.

This is a rewrite of the Go-based cdp-collector, focusing on:
- **Memory safety** without garbage collection pauses
- **Zero-copy processing** with predictable latency
- **Flexible routing** - source-based routing with O(1) lookup
- **Smaller binary size** and faster startup

## Architecture

### Data Flow
```
[Sources]              [Router]              [Sinks]
  TCP ─────┐                              ┌──→ ClickHouse
  Syslog ──┼──→ [Routing Table] ──→ Fan-out ──→ Disk Binary
  UDP ─────┘         O(1)                 └──→ Parquet/etc
```

### Key Design Principles
- **Zero-copy pipeline**: `bytes::Bytes` for reference-counted buffers
- **Pre-compiled routing**: Routing decisions made at config load, not per-message
- **Arc for fan-out**: Sending to N sinks = N reference increments, not N copies
- **Channel-based communication**: `tokio::sync::mpsc` for async message passing

## Workspace Structure

```
cdp-collector-rust/
├── Cargo.toml                    # Workspace root
├── crates/
│   ├── protocol/                 # Zero-copy core - FlatBuffers, Batch types
│   ├── routing/                  # Routing rules, pre-compiled tables
│   ├── pipeline/                 # Router, channels, backpressure
│   ├── sources/                  # TCP, Syslog sources
│   │   ├── tcp/
│   │   └── syslog/
│   ├── sinks/                    # Output destinations
│   │   ├── null/
│   │   ├── stdout/
│   │   ├── disk_binary/
│   │   ├── disk_plaintext/
│   │   ├── clickhouse/
│   │   ├── parquet/
│   │   └── forwarder/
│   ├── auth/                     # API key management with hot reload
│   ├── transform/                # Transformer chain (pattern matching, etc)
│   ├── config/                   # YAML config loading + validation
│   ├── metrics/                  # Internal metrics reporting
│   └── collector/                # Binary entry point
```

**Crate responsibilities:**
- **protocol** - Shared types, zero-copy `Batch` struct, FlatBuffers integration
- **routing** - `RoutingTable` with O(1) source→sinks lookup
- **pipeline** - `Router` that receives batches and fans out via channels
- **sources** - Async network listeners, produce `Batch` instances
- **sinks** - Consume `Arc<Batch>`, write to destinations
- **auth** - API key validation, workspace ID lookup, hot reload
- **transform** - Optional batch transformation (pattern extraction, enrichment)
- **config** - Deserialize YAML, validate, build typed config structs
- **metrics** - Atomic counters, periodic reporting
- **collector** - Main binary, wires everything together

## Coding Standards (Non-Negotiable)

### Structure

- **500 lines max** per file - split into logical sub-modules if larger
- **One file, one job** - each module has a single, well-defined responsibility
- **Tests in `*_test.rs`** files or `tests/` directory, never inline `#[cfg(test)]` in main code
- **Functions <30 lines** typically - extract helpers for complex logic

### Safety

- **Early exit pattern**: Return errors early, avoid deeply nested logic
- **No `unwrap()`** in library code - always use `?` or explicit error handling
- **No `expect()`** without a descriptive message explaining why it's safe
- **`#[must_use]`** on important return types that shouldn't be ignored
- **Every `Result` must be handled** - no silent failures

### Error Handling

- **`thiserror`** for library crate errors - structured, typed errors
- **`anyhow`** for binary crate only - convenient error chaining
- **Error context**: Always add context when propagating errors
  ```rust
  // Good
  file.read(&mut buf).context("failed to read batch header")?;
  
  // Bad
  file.read(&mut buf)?;
  ```

### Concurrency (in order of preference)

1. **Channels** (`tokio::sync::mpsc`) - default choice for async communication
2. **Atomics** (`std::sync::atomic`) - for counters, flags, metrics
3. **`Arc<T>`** - for shared ownership across tasks
4. **Locks** (`parking_lot::Mutex`/`RwLock`) - only when truly necessary, prefer channels

### Performance

- **Zero-copy where possible**: Use `bytes::Bytes` for buffer sharing
- **Avoid allocations in hot path**: Pre-allocate, reuse buffers
- **`#[inline]`** on small, frequently-called functions
- **Benchmark before optimizing**: Use `criterion` for micro-benchmarks

### Style (Rust 2024 Edition)

- **Standard `rustfmt`** - no custom rules, run `cargo fmt`
- **`cargo clippy`** must pass with no warnings
- **Comments for "why" not "what"** - code should be self-documenting
- **Doc comments (`///`)** on all public items
- **Module-level docs (`//!`)** explaining the module's purpose

### Dependencies

Core dependencies (keep minimal):
- `tokio` - async runtime (features: rt-multi-thread, net, sync, io-util, time)
- `bytes` - zero-copy buffer management
- `flatbuffers` - serialization (matches Go implementation)
- `serde` + `serde_yaml` - configuration
- `thiserror` - library errors
- `anyhow` - binary errors
- `tracing` - structured logging
- `parking_lot` - faster mutexes (when locks needed)

Avoid adding dependencies without justification.

### Testing

- **Unit tests** for all public functions
- **Integration tests** for crate interactions
- **Each crate must be fully tested** before moving to next phase
- **Property-based testing** with `proptest` for serialization/parsing
- **Benchmarks** with `criterion` for performance-critical code

## Key Files

- `TODO.md` - Implementation progress tracker with phases
- `CHANGELOG.md` - Version history and breaking changes
- `config.example.yaml` - Example configuration with all options documented

## Development Workflow

### Building
```bash
cargo build --release
cargo test --all
cargo clippy --all -- -D warnings
cargo fmt --all -- --check
```

### Running
```bash
cargo run --release --bin collector -- --config config.yaml
```

### Testing a specific crate
```bash
cargo test -p cdp-protocol
cargo test -p cdp-routing
```

## FlatBuffers Integration

Schemas are in `../cdp-collector/pkg/schema/`:
- `common.fbs` - Batch wrapper, SchemaType enum
- `event.fbs` - CDP events
- `log.fbs` - Structured logs
- `metric.fbs` - Metrics (experimental)
- `trace.fbs` - Traces (experimental)

Generate Rust code:
```bash
flatc --rust -o crates/protocol/src/generated/ ../cdp-collector/pkg/schema/*.fbs
```

## Performance Targets

| Metric | Target |
|--------|--------|
| Throughput | 40M+ events/sec |
| Latency P99 | <1ms (no GC pauses) |
| Memory per batch | ~0 overhead (zero-copy) |
| Binary size | <10MB |
| Startup time | <100ms |

## Common Patterns

### Creating a new sink
```rust
// crates/sinks/src/my_sink/mod.rs
pub struct MySink {
    receiver: mpsc::Receiver<Arc<Batch>>,
    config: MySinkConfig,
}

impl MySink {
    pub async fn run(mut self) -> Result<()> {
        while let Some(batch) = self.receiver.recv().await {
            self.process_batch(&batch).await?;
        }
        Ok(())
    }
}
```

### Zero-copy batch access
```rust
// Don't copy - get a slice reference
let msg: &[u8] = batch.get_message(i);

// For fan-out - clone Arc, not data
let batch = Arc::new(batch);
for sink in sinks {
    sink.send(Arc::clone(&batch)).await;
}
```

### Routing lookup
```rust
// O(1) lookup - no allocation
let sink_ids: &[SinkId] = routing_table.route(&batch.source_id);
```

### Graceful Shutdown
Sources and long-running tasks should accept `CancellationToken` for coordinated shutdown.
