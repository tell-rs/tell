# Arrow IPC Sink

Fast columnar storage for **hot data** - real-time dashboards, inter-service communication.

## TL;DR

```toml
[sinks.arrow_ipc]
type = "arrow_ipc"
path = "arrow/"
rotation = "hourly"      # hourly, daily
buffer_size = 10000      # rows before flush
flush_interval = "60s"
```

```bash
# Query with DuckDB
duckdb -c "SELECT * FROM 'arrow/42/2024-01-15/14/events.arrow' LIMIT 10"
```

## When to Use

| Use Case | Arrow IPC | Parquet |
|----------|-----------|---------|
| Real-time dashboards | **Yes** | No |
| Inter-service comms | **Yes** | No |
| Hot data (recent) | **Yes** | No |
| Analytics queries | No | **Yes** |
| Long-term storage | No | **Yes** |

**Key difference:** Arrow IPC is ~10x faster I/O but no compression. Use for data accessed frequently, Parquet for archival.

## Configuration

```toml
[sinks.arrow_ipc]
type = "arrow_ipc"
path = "arrow/"                  # Output directory (required)
rotation = "hourly"              # hourly, daily (default: hourly)
buffer_size = 10000              # Rows to buffer (default: 10000)
flush_interval = "60s"           # Flush interval (default: 60s)
metrics_enabled = true
metrics_interval = "10s"
```

**Why hourly default?** Hot data rotates frequently for:
- Faster file discovery
- Smaller file sizes for quick reads
- Better cache locality

## File Layout

```
{path}/
└── {workspace_id}/
    └── {date}/
        └── {hour}/           # Only with hourly rotation
            ├── events.arrow
            ├── logs.arrow
            └── snapshots.arrow
```

## Schema

Same schema as Parquet (shared via `util/arrow_rows.rs`). Field order optimized for predicate pushdown.

### Events (9 columns)

| # | Column | Type | Description |
|---|--------|------|-------------|
| 0 | `timestamp` | INT64 | Event time (ms since epoch) |
| 1 | `batch_timestamp` | INT64 | Processing time (ms since epoch) |
| 2 | `workspace_id` | UINT64 | Tenant identifier |
| 3 | `event_type` | UTF8 | track, identify, group, alias, enrich |
| 4 | `event_name` | UTF8 | Optional: page_view, button_click, etc. |
| 5 | `device_id` | BINARY(16) | Device UUID |
| 6 | `session_id` | BINARY(16) | Session UUID |
| 7 | `source_ip` | BINARY(16) | IPv6 address (v4 mapped) |
| 8 | `payload` | BINARY | JSON bytes |

### Logs (10 columns)

| # | Column | Type | Description |
|---|--------|------|-------------|
| 0 | `timestamp` | INT64 | Log time (ms since epoch) |
| 1 | `batch_timestamp` | INT64 | Processing time (ms since epoch) |
| 2 | `workspace_id` | UINT64 | Tenant identifier |
| 3 | `level` | UTF8 | emergency, alert, critical, error, warning, notice, info, debug |
| 4 | `event_type` | UTF8 | log, enrich |
| 5 | `source` | UTF8 | Hostname/instance |
| 6 | `service` | UTF8 | Service/app name |
| 7 | `session_id` | BINARY(16) | Session UUID |
| 8 | `source_ip` | BINARY(16) | IPv6 address |
| 9 | `payload` | BINARY | Log message bytes |

### Snapshots (7 columns)

| # | Column | Type | Description |
|---|--------|------|-------------|
| 0 | `timestamp` | INT64 | Snapshot time (ms since epoch) |
| 1 | `batch_timestamp` | INT64 | Processing time (ms since epoch) |
| 2 | `workspace_id` | UINT64 | Tenant identifier |
| 3 | `source` | UTF8 | Connector: github, stripe, linear |
| 4 | `entity` | UTF8 | Resource: user/repo, acct_123 |
| 5 | `source_ip` | BINARY(16) | IPv6 address |
| 6 | `payload` | BINARY | JSON bytes |

## Query Tools

### DuckDB (Recommended)

```bash
# Install
brew install duckdb  # macOS
apt install duckdb   # Ubuntu

# Query single file
duckdb -c "SELECT * FROM 'arrow/42/2024-01-15/14/events.arrow' LIMIT 10"

# Query multiple files
duckdb -c "SELECT event_type, COUNT(*) FROM 'arrow/42/2024-01-15/*/events.arrow' GROUP BY 1"

# Filter by timestamp
duckdb -c "SELECT * FROM 'arrow/**/*.arrow' WHERE timestamp > 1700000000000"
```

### Python (PyArrow / Polars)

```python
# PyArrow
import pyarrow as pa
with pa.ipc.open_file("arrow/42/2024-01-15/14/events.arrow") as reader:
    table = reader.read_all()
    df = table.to_pandas()

# Polars (faster, recommended)
import polars as pl
df = pl.read_ipc("arrow/42/2024-01-15/14/events.arrow")

# Memory-mapped for zero-copy (fastest)
df = pl.read_ipc("events.arrow", memory_map=True)

# Scan multiple files
df = pl.scan_ipc("arrow/**/*.arrow").filter(
    pl.col("workspace_id") == 42
).collect()
```

### DataFusion (Rust)

```rust
use datafusion::prelude::*;

let ctx = SessionContext::new();
ctx.register_ipc_file("events", "arrow/42/2024-01-15/14/events.arrow").await?;
let df = ctx.sql("SELECT * FROM events WHERE workspace_id = 42").await?;
```

## Performance Comparison

| Operation | Arrow IPC | Parquet (Snappy) |
|-----------|-----------|------------------|
| Write 1M rows | ~0.5s | ~2s |
| Read 1M rows | ~0.1s | ~1s |
| File size | ~100MB | ~30MB |
| Memory-map | Yes | No |

**Trade-off:** Arrow IPC uses ~3x more disk space but reads ~10x faster.

## Architecture

```
[Batch] → [Buffer by workspace/time] → [Arrow RecordBatch] → [IPC FileWriter] → [Disk]
                                              ↑
                                    Shared with Parquet
                                    (util/arrow_rows.rs)
```

Key files:
- `mod.rs` - Sink implementation, buffering, metrics
- `writer.rs` - Arrow IPC file writing

## Metrics

| Metric | Description |
|--------|-------------|
| `batches_received` | Total batches processed |
| `event_rows_written` | Event rows written |
| `log_rows_written` | Log rows written |
| `snapshot_rows_written` | Snapshot rows written |
| `bytes_written` | Bytes on disk (uncompressed) |
| `files_created` | Files created |
| `write_errors` | Write failures |
| `decode_errors` | FlatBuffer decode failures |

## Hot/Cold Data Pattern

Common setup: Arrow IPC for recent data, Parquet for archives.

```toml
# Hot data - last 24h, fast access
[sinks.arrow_ipc]
type = "arrow_ipc"
path = "hot/"
rotation = "hourly"

# Cold data - archives, compressed
[sinks.parquet]
type = "parquet"
path = "cold/"
rotation = "daily"
compression = "zstd"
```

Then periodically move aged Arrow files to Parquet:
```bash
# Convert old Arrow to Parquet (example with DuckDB)
duckdb -c "COPY (SELECT * FROM 'hot/42/2024-01-14/**/*.arrow') TO 'cold/42/2024-01-14/events.parquet'"
rm -rf hot/42/2024-01-14/
```
