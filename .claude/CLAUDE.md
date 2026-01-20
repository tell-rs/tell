# CLAUDE.md

This file provides guidance to Claude when working with code in this repository.

## Project Overview

Tell is a high-performance, multi-protocol data streaming engine. It processes product analytics events and structured logs at 40M+ events/sec with minimal resource footprint.

This is a rewrite of the original Go implementation, focusing on:
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
tell-rs/
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
│   ├── auth/                     # Auth core: RBAC, tokens, API keys
│   ├── analytics/                # Analytics queries (DAU, MAU, retention, etc)
│   ├── query/                    # Query engine, ClickHouse integration
│   ├── api/                      # HTTP API (Axum), auth middleware
│   ├── transform/                # Transformer chain (pattern matching, etc)
│   ├── config/                   # TOML config loading + validation
│   ├── metrics/                  # Internal metrics reporting
│   └── collector/                # Binary entry point (produces 'tell' binary)
```

**Crate responsibilities:**
- **protocol** - Shared types, zero-copy `Batch` struct, FlatBuffers integration
- **routing** - `RoutingTable` with O(1) source→sinks lookup
- **pipeline** - `Router` that receives batches and fans out via channels
- **sources** - Async network listeners, produce `Batch` instances
- **sinks** - Consume `Arc<Batch>`, write to destinations
- **auth** - RBAC (roles/permissions), JWT claims, streaming API keys
- **analytics** - Product analytics: DAU, MAU, retention, funnel, etc.
- **query** - Query builder, ClickHouse client, time series data
- **api** - HTTP routes, auth middleware, permission enforcement
- **transform** - Optional batch transformation (pattern extraction, enrichment)
- **config** - Deserialize YAML, validate, build typed config structs
- **metrics** - Atomic counters, periodic reporting
- **collector** - Main binary (`tell`), wires everything together

## Authentication & API

### Two Auth Systems

| System | Format | Use Case |
|--------|--------|----------|
| Streaming | `000102...0f` (32 hex) | Collector ingestion, O(1) lookup |
| HTTP API | `tell_<jwt>` | Dashboard/API, role-based |

### Roles (4) & Permissions (3)

| Role | Permission | What they can do |
|------|------------|------------------|
| `Viewer` | (view) | View analytics and shared content |
| `Editor` | `Create` | Create/edit own dashboards, metrics |
| `Admin` | `Admin` | Manage workspace (members, settings) |
| `Platform` | `Platform` | Cross-workspace ops (enterprise) |

Roles are hierarchical: Platform > Admin > Editor > Viewer

### Content Ownership

```rust
// Most checks are ownership-based
fn can_edit(content: &Content, user: &User) -> bool {
    content.owner_id == user.id || user.is_admin()
}
```

### Route Protection

```rust
use tell_api::auth::{Permission, RouterExt, AuthUser};

// Editor+ routes (create content)
Router::new()
    .route("/dashboard", post(create_dashboard))
    .with_permission(Permission::Create)

// Admin routes (manage workspace)
Router::new()
    .route("/members", post(add_member))
    .with_permission(Permission::Admin)

// Handler checks
async fn handler(user: AuthUser) -> impl IntoResponse {
    if user.can_create() { /* Editor+ */ }
    if user.is_admin() { /* Admin+ */ }
}
```

### API Keys

- Any user can create API keys
- Keys inherit creator's role
- Used for MCP, CLI, integrations

### Public Paths

`/health`, `/api/v1/auth/*`, `/s/*` (shared links)

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
cargo run --release --bin tell -- --config configs/config.toml
```

### Testing a specific crate
```bash
cargo test -p tell-protocol
cargo test -p tell-routing
```

## FlatBuffers Integration

Schemas are in `crates/protocol/schema/`:
- `common.fbs` - Batch wrapper, SchemaType enum
- `event.fbs` - Product analytics events
- `log.fbs` - Structured logs
- `metric.fbs` - Metrics (experimental)
- `trace.fbs` - Traces (experimental)

Generate Rust code:
```bash
flatc --rust -o crates/protocol/src/generated/ crates/protocol/schema/*.fbs
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
