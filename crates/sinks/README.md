# cdp-sinks

High-performance output sinks for CDP Collector with 40M+ events/sec throughput.

## Architecture

```
[Router] --Arc<Batch>--> [Sink Channel] --> [Sink Task] --> [Destination]
```

The atomic buffer chain pattern ensures zero data loss during file rotation:

```
[Batch] → [Buffer Pool] → [Serialize] → [Active Chain] → [Disk]
                                             ↓ (rotation)
                                        [New Chain]
                                             ↓
                                   [Old Chain drains via Arc]
```

## Module Structure

```
src/
├── lib.rs                    # Public exports
├── common.rs                 # SinkError, SinkMetrics, SinkConfig
├── common_test.rs
├── util/                     # Shared utilities for disk sinks
│   ├── mod.rs
│   ├── buffer_pool.rs        # Lock-free buffer pool
│   ├── buffer_pool_test.rs
│   ├── chain_writer.rs       # Pluggable writers (PlainText, LZ4, Binary)
│   ├── chain_writer_test.rs
│   ├── atomic_rotation.rs    # Zero-loss file rotation with ArcSwap
│   └── atomic_rotation_test.rs
├── null/                     # Null sink (benchmarking)
│   ├── mod.rs
│   └── null_test.rs
├── stdout/                   # Stdout sink (debugging)
│   ├── mod.rs
│   └── stdout_test.rs
├── disk_binary/              # Binary disk sink
│   ├── mod.rs
│   ├── writer.rs             # MessageMetadata, BinaryReader
│   └── disk_binary_test.rs
├── disk_plaintext/           # Plaintext disk sink
│   ├── mod.rs
│   └── disk_plaintext_test.rs
├── clickhouse/               # ClickHouse sink
│   └── mod.rs
├── parquet/                  # Parquet sink
│   └── mod.rs
└── forwarder/            # Collector-to-collector
    └── mod.rs
```

## Available Sinks

| Sink | Purpose | Uses Rotation | Status |
|------|---------|---------------|--------|
| `null` | Benchmarking (discard all) | No | ✅ Complete |
| `stdout` | Debug output | No | ✅ Complete |
| `disk_binary` | Binary storage with metadata | Yes | ✅ Complete |
| `disk_plaintext` | Human-readable logs | Yes | ✅ Complete |
| `clickhouse` | Analytics database (CDP v1.1) | No | ✅ Complete |
| `parquet` | Columnar storage | Yes | ✅ Complete |
| `forwarder` | Collector-to-collector | No | ✅ Complete |

## Utilities (`util/`)

Reusable components for building disk sinks:

### `buffer_pool.rs` - Lock-Free Buffer Pool

Pre-allocated buffer pool using `crossbeam::ArrayQueue` for O(1) get/put.

```rust
use cdp_sinks::util::BufferPool;

// Pre-allocate 64 buffers of 32MB each
let pool = BufferPool::new(64, 32 * 1024 * 1024);

// Hot path: ~10ns get (lock-free)
let mut buf = pool.get();
serialize_batch(&batch, &mut buf);

// After write: return to pool
pool.put(buf);
```

### `chain_writer.rs` - Pluggable Writers

Trait-based writers for different output formats:

```rust
use cdp_sinks::util::{ChainWriter, PlainTextWriter, Lz4Writer, BinaryWriter};

// Plain buffered writer
let writer = PlainTextWriter::new(32 * 1024 * 1024);

// LZ4 compressed writer  
let writer = Lz4Writer::new(32 * 1024 * 1024);

// Binary with optional compression
let writer = BinaryWriter::new(32 * 1024 * 1024, true); // compressed
let writer = BinaryWriter::uncompressed(32 * 1024 * 1024);
```

### `atomic_rotation.rs` - Zero-Loss File Rotation

Lock-free file rotation using `ArcSwap<BufferChain>`:

```rust
use cdp_sinks::util::{AtomicRotationSink, RotationConfig, RotationInterval, BinaryWriter};

let config = RotationConfig {
    base_path: "/data/logs".into(),
    rotation_interval: RotationInterval::Hourly,
    file_prefix: "data".into(),
    buffer_size: 32 * 1024 * 1024,
    queue_size: 1000,
    flush_interval: Duration::from_millis(100),
};

let writer = BinaryWriter::uncompressed(config.buffer_size);
let sink = AtomicRotationSink::new(config, writer);

// Hot path: non-blocking submit (~50ns)
sink.submit("workspace_123", buffer, message_count)?;

// Rotation happens atomically in background
sink.check_rotation_all().await;
```

## Disk Binary Sink

High-performance binary storage with 24-byte metadata headers.

### File Format

Each message is stored as:
```
[24-byte Metadata][4-byte size][FlatBuffer data]
```

Metadata layout:
- `batch_timestamp`: u64 (8 bytes) - Unix milliseconds
- `source_ip`: [u8; 16] (16 bytes) - IPv6 format (IPv4 mapped as ::ffff:x.x.x.x)

### Directory Structure

```
{base_path}/{workspace_id}/{date}/{hour}/data.bin
{base_path}/{workspace_id}/{date}/{hour}/data.bin.lz4  # if compression enabled
```

### Usage

```rust
use cdp_sinks::disk_binary::{DiskBinarySink, DiskBinaryConfig};
use tokio::sync::mpsc;

let config = DiskBinaryConfig::default()
    .with_path("/data/archive")
    .with_compression()
    .with_daily_rotation();

let (tx, rx) = mpsc::channel(1000);
let sink = DiskBinarySink::new(config, rx);

// Spawn sink task
tokio::spawn(sink.run());

// Send batches
tx.send(batch).await?;
```

### Reading Binary Files

```rust
use cdp_sinks::disk_binary::writer::{BinaryReader, SeekableBinaryReader};

// Sequential reading (supports .bin and .bin.lz4)
let mut reader = BinaryReader::open("data.bin")?;
for msg in reader.messages() {
    let msg = msg?;
    println!("timestamp: {}, ip: {}", 
        msg.metadata.batch_timestamp,
        msg.metadata.source_ip_string());
}

// Time-range scanning (uncompressed only)
let mut reader = SeekableBinaryReader::open("data.bin")?;
let messages = reader.scan_time_range(start_time, end_time)?;
```

## Creating a New Sink

### 1. Create the module directory

```
src/my_sink/
├── mod.rs
└── my_sink_test.rs
```

### 2. Implement the sink

```rust
// src/my_sink/mod.rs
use std::sync::Arc;
use cdp_protocol::Batch;
use tokio::sync::mpsc;

pub struct MySink {
    receiver: mpsc::Receiver<Arc<Batch>>,
    metrics: MySinkMetrics,
}

impl MySink {
    pub fn new(receiver: mpsc::Receiver<Arc<Batch>>) -> Self {
        Self {
            receiver,
            metrics: MySinkMetrics::new(),
        }
    }

    pub async fn run(mut self) -> MySinkMetrics {
        while let Some(batch) = self.receiver.recv().await {
            self.process_batch(&batch).await;
        }
        self.metrics
    }
}

#[cfg(test)]
#[path = "my_sink_test.rs"]
mod my_sink_test;
```

### 3. Add tests in separate file

```rust
// src/my_sink/my_sink_test.rs
use super::*;

#[tokio::test]
async fn test_my_sink_processes_batch() {
    // ...
}
```

### 4. Register in lib.rs

```rust
pub mod my_sink;
```

## Testing Guidelines

- Tests go in `*_test.rs` files, **not** inline `#[cfg(test)]` blocks
- Each sink module references its tests via `#[path = "..._test.rs"]`
- Use `super::*` for imports in test files

## Performance Targets

| Metric | Target |
|--------|--------|
| Hot path latency | <2µs |
| Throughput | 40M+ events/sec |
| Buffer pool hit rate | >99% |
| Rotation downtime | 0 (atomic swap) |

## Tests

```bash
cargo test -p cdp-sinks
```

312 tests covering:
- Buffer pool (14 tests)
- Chain writers (20 tests)
- Atomic rotation (18 tests)
- Common utilities (16 tests)
- Null sink (15 tests)
- Stdout sink (18 tests)
- Disk binary sink (27 tests)
- Disk plaintext sink (29 tests)
- Forwarder sink (28 tests)
- Parquet sink (61 tests) - schema, writer, compression, integration
- ClickHouse sink (66 tests) - CDP v1.1 schema, 6 tables, event routing, IDENTIFY handling

## Parquet Sink

The Parquet sink writes columnar storage files optimized for analytical queries.

### Output Compatibility

Parquet files generated by this sink can be queried by any tool that supports the Parquet format:

| Tool | Query Example |
|------|---------------|
| **Apache Spark** | `spark.read.parquet("path/")` |
| **DuckDB** | `SELECT * FROM 'path/*.parquet'` |
| **ClickHouse** | `SELECT * FROM file('path/*.parquet', Parquet)` |
| **Pandas** | `pd.read_parquet("path/")` |
| **Polars** | `pl.read_parquet("path/")` |
| **AWS Athena** | Query via S3 path |
| **Google BigQuery** | Load from GCS |
| **Snowflake** | `SELECT * FROM @stage/path/` |

### Directory Structure

Files are partitioned by workspace, date, and hour for efficient querying:

```
parquet/
└── {workspace_id}/
    └── {date}/
        └── {hour}/
            ├── events.parquet
            └── logs.parquet
```

### Compression Options

| Codec | Use Case |
|-------|----------|
| `Snappy` (default) | Fast compression, good for real-time |
| `Zstd` | Best compression ratio, good for archival |
| `LZ4` | Fastest, lower ratio |
| `None` | Maximum write speed |

### Usage

```rust
use cdp_sinks::parquet::{ParquetSink, ParquetConfig, Compression};

let config = ParquetConfig::default()
    .with_path("/data/warehouse")
    .with_zstd()                    // Best compression
    .with_daily_rotation()          // Rotate daily instead of hourly
    .with_buffer_size(5000);        // Buffer 5k rows before flush

let (tx, rx) = mpsc::channel(1000);
let sink = ParquetSink::new(config, rx);
tokio::spawn(sink.run());
```

## Sink Format Comparison

| Sink | Format | Use Case | Vendor Lock-in |
|------|--------|----------|----------------|
| `disk_binary` | Custom binary + FlatBuffers | High-speed replay, minimal overhead | None |
| `disk_plaintext` | Human-readable text | Debugging, log tailing | None |
| `parquet` | Apache Parquet | Analytics, data warehouse | None |
| `clickhouse` | ClickHouse native | Real-time analytics | ClickHouse |

## ClickHouse Sink (CDP v1.1 Schema)

Full CDP analytics sink with 6 tables, event-type routing, and IDENTIFY handling.

### Event Routing

| EventType | Target Tables |
|-----------|---------------|
| `TRACK` | `events_v1` |
| `IDENTIFY` | `users_v1` + `user_devices` + `user_traits` (3 parallel inserts) |
| `CONTEXT` | `context_v1` (device/location fields extracted) |
| `LOG` | `logs_v1` (with optional `pattern_id`) |

### Table Schemas (6 Tables)

#### events_v1 - TRACK events
```sql
CREATE TABLE events_v1 (
    timestamp DateTime64(3),
    event_name String,
    device_id UUID,
    session_id UUID,
    properties String,
    _raw String,
    source_ip FixedString(16)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp, device_id);
```

#### users_v1 - IDENTIFY events (core identity)
```sql
CREATE TABLE users_v1 (
    user_id String,
    email String,
    name String,
    updated_at DateTime64(3)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY user_id;
```

#### user_devices - IDENTIFY events (device relationships)
```sql
CREATE TABLE user_devices (
    user_id String,
    device_id UUID,
    linked_at DateTime64(3)
) ENGINE = ReplacingMergeTree(linked_at)
ORDER BY (user_id, device_id);
```

#### user_traits - IDENTIFY events (key-value traits)
```sql
CREATE TABLE user_traits (
    user_id String,
    trait_key String,
    trait_value String,
    updated_at DateTime64(3)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (user_id, trait_key);
```

#### context_v1 - CONTEXT events
```sql
CREATE TABLE context_v1 (
    timestamp DateTime64(3),
    device_id UUID,
    session_id UUID,
    device_type String,
    device_model String,
    operating_system String,
    os_version String,
    app_version String,
    app_build String,
    timezone String,
    locale FixedString(5),
    country String,
    region String,
    city String,
    properties String,
    source_ip FixedString(16)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp, device_id);
```

#### logs_v1 - Log entries
```sql
CREATE TABLE logs_v1 (
    timestamp DateTime64(3),
    level LowCardinality(String),
    source String,
    service String,
    session_id UUID,
    source_ip FixedString(16),
    pattern_id Nullable(UInt64),
    message String,
    _raw String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp, level);
```

### Usage

```rust
use cdp_sinks::clickhouse::{ClickHouseSink, ClickHouseConfig};

let config = ClickHouseConfig::default()
    .with_url("http://localhost:8123")
    .with_database("cdp")
    .with_credentials("user", "password")
    .with_batch_size(1000)
    .with_flush_interval(Duration::from_secs(5))
    .with_retry_attempts(3);

let (tx, rx) = mpsc::channel(1000);
let sink = ClickHouseSink::new(config, rx);
tokio::spawn(sink.run());
```

### Features

| Feature | Description |
|---------|-------------|
| **Per-table batching** | Separate batches for each table type |
| **Concurrent flush** | All 6 tables flushed in parallel |
| **IDENTIFY → 3 tables** | Users, devices, traits written atomically |
| **CONTEXT extraction** | Device/location fields parsed from payload |
| **Pattern ID support** | Optional pattern_id for logs from transformer |
| **User ID generation** | Deterministic UUID v5 from normalized email |
| **Retry logic** | Exponential backoff on transient failures |
| `forwarder` | FlatBuffers over TCP | Collector-to-collector | None |

This flexibility allows you to:
- **Archive** to Parquet for long-term analytics with any query engine
- **Stream** to ClickHouse for real-time dashboards
- **Replay** from binary files for debugging or reprocessing
- **Forward** between collectors for multi-region setups