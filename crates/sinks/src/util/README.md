# Sink Utilities

Shared components for high-performance disk-based sinks.

## Modules

| Module | Purpose | Used By |
|--------|---------|---------|
| `arrow_rows` | Arrow schemas & row types | parquet, arrow_ipc |
| `atomic_rotation` | Lock-free file rotation | disk_binary, disk_plaintext |
| `buffer_pool` | Pre-allocated buffer pool | disk_binary, disk_plaintext |
| `chain_writer` | Pluggable writers | disk_binary, disk_plaintext |
| `json` | JSON field extraction | clickhouse |
| `rate_limited_logger` | Throttled error logging | all sinks |

## Architecture

```
[Batch] → [Buffer Pool] → [Serialize] → [Active Chain] → [Disk]
                                              ↓ (rotation)
                                         [New Chain]
                                              ↓
                                    [Old Chain drains via Arc]
```

## arrow_rows

Shared Arrow schemas for columnar sinks. Ensures Parquet and Arrow IPC use identical schemas.

```rust
use crate::util::{EventRow, LogRow, SnapshotRow};
use crate::util::{event_schema, log_schema, snapshot_schema};
use crate::util::{events_to_record_batch, logs_to_record_batch, snapshots_to_record_batch};
```

### Row Types

```rust
pub struct EventRow {
    pub timestamp: i64,
    pub batch_timestamp: i64,
    pub workspace_id: u64,
    pub event_type: String,
    pub event_name: Option<String>,
    pub device_id: Option<Vec<u8>>,
    pub session_id: Option<Vec<u8>>,
    pub source_ip: Vec<u8>,
    pub payload: Vec<u8>,
}
```

## atomic_rotation

Lock-free file rotation achieving 40M+ events/sec.

**Key features:**
- `ArcSwap` for atomic chain switching
- Background draining via Arc refcount
- Zero data loss during rotation

```rust
use crate::util::{AtomicRotationSink, RotationConfig, RotationInterval};
```

## buffer_pool

Pre-allocated `BytesMut` buffers to reduce allocations.

```rust
use crate::util::{BufferPool, BufferPoolMetrics};

let pool = BufferPool::new(16, 32 * 1024); // 16 buffers, 32KB each
let mut buf = pool.get();
// ... use buffer ...
pool.put(buf);
```

## chain_writer

Pluggable writers for different output formats.

| Writer | Description |
|--------|-------------|
| `PlainTextWriter` | Human-readable text |
| `BinaryWriter` | Binary with length prefix |
| `Lz4Writer` | LZ4 compressed |

```rust
use crate::util::{ChainWriter, PlainTextWriter, Lz4Writer};
```

## json

Fast JSON field extraction without full parsing.

```rust
use crate::util::{extract_json_string, extract_json_object};

let email = extract_json_string(payload, "email");
let traits = extract_json_object(payload, "traits");
```

## rate_limited_logger

Prevents log spam during error storms.

```rust
use crate::util::RateLimitedLogger;

let logger = RateLimitedLogger::new(Duration::from_secs(60));
logger.error("write failed", &error, Some(&data));
// Only logs once per minute for same error type
```

## Performance Targets

| Metric | Target |
|--------|--------|
| Throughput | 40M+ events/sec |
| Buffer reuse | >95% hit rate |
| Rotation | Zero data loss |
| Syscalls | Minimized (32MB buffers) |
