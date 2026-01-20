# Parquet Sink

Columnar storage for **cold data** - analytics, data warehousing, long-term archival.

## TL;DR

```toml
[sinks.parquet]
type = "parquet"
path = "parquet/"
compression = "snappy"   # snappy, gzip, lz4, uncompressed
rotation = "daily"       # hourly, daily
```

```bash
# Query with DuckDB
duckdb -c "SELECT * FROM 'parquet/42/2024-01-15/events.parquet' LIMIT 10"
```

## When to Use

| Use Case | Parquet | Arrow IPC |
|----------|---------|-----------|
| Analytics queries | **Yes** | No |
| Long-term storage | **Yes** | No |
| Data warehousing | **Yes** | No |
| Real-time dashboards | No | **Yes** |
| Inter-service comms | No | **Yes** |

## Configuration

```toml
[sinks.parquet]
type = "parquet"
path = "parquet/"                # Output directory (required)
rotation = "daily"               # hourly, daily (default: daily)
compression = "snappy"           # snappy, gzip, lz4, uncompressed (default: snappy)
data_format = "json"             # json, binary (default: json)
metrics_enabled = true
metrics_interval = "10s"
```

**Compression trade-offs:**

| Codec | Speed | Ratio | Use Case |
|-------|-------|-------|----------|
| `snappy` | Fast | Good | Default, balanced |
| `lz4` | Fastest | Lower | High throughput |
| `zstd` | Slow | Best | Archival, storage cost |
| `uncompressed` | N/A | None | Debugging |

## File Layout

```
{path}/
└── {workspace_id}/
    └── {date}/
        └── {hour}/           # Only with hourly rotation
            ├── events.parquet
            ├── logs.parquet
            └── snapshots.parquet
```

## Schema

Field order is optimized for **predicate pushdown** - filter columns first, payload last.

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

# Query
duckdb -c "SELECT * FROM 'parquet/42/2024-01-15/events.parquet' LIMIT 10"

# Query multiple files
duckdb -c "SELECT event_type, COUNT(*) FROM 'parquet/42/2024-01-*/events.parquet' GROUP BY 1"

# Filter by timestamp (uses predicate pushdown)
duckdb -c "SELECT * FROM 'parquet/**/*.parquet' WHERE timestamp > 1700000000000"
```

### Python (PyArrow / Polars)

```python
# PyArrow
import pyarrow.parquet as pq
table = pq.read_table("parquet/42/2024-01-15/events.parquet")
df = table.to_pandas()

# Polars (faster)
import polars as pl
df = pl.read_parquet("parquet/42/2024-01-15/events.parquet")

# Query with filter pushdown
df = pl.scan_parquet("parquet/**/*.parquet").filter(
    pl.col("workspace_id") == 42
).collect()
```

### ClickHouse

```sql
SELECT * FROM file('parquet/42/2024-01-15/events.parquet', Parquet)

-- Or import into table
INSERT INTO events SELECT * FROM file('parquet/**/*.parquet', Parquet)
```

### Spark

```python
df = spark.read.parquet("parquet/")
df.filter(df.workspace_id == 42).show()
```

## Architecture

```
[Batch] → [Buffer by workspace/time] → [Arrow RecordBatch] → [Parquet Writer] → [Disk]
                                              ↑
                                    Shared with Arrow IPC
                                    (util/arrow_rows.rs)
```

Key files:
- `mod.rs` - Sink implementation, buffering, metrics
- `writer.rs` - Parquet file writing
- `schema.rs` - Compression config (schemas in `util/arrow_rows.rs`)

## Metrics

| Metric | Description |
|--------|-------------|
| `batches_received` | Total batches processed |
| `event_rows_written` | Event rows written |
| `log_rows_written` | Log rows written |
| `snapshot_rows_written` | Snapshot rows written |
| `bytes_written` | Compressed bytes on disk |
| `files_created` | Files created |
| `write_errors` | Write failures |
| `decode_errors` | FlatBuffer decode failures |
