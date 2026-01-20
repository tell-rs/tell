# ClickHouse Sink (Tell v1.1 Schema)

High-performance ClickHouse sink implementing the full Tell v1.1 analytics schema.

## TL;DR

- **2 implementations**: Arrow HTTP (recommended) and Native protocol
- **7 tables**: events_v1, users_v1, user_devices, user_traits, context_v1, logs_v1, snapshots_v1
- **Smart routing**: TRACK→events, IDENTIFY→3 tables (parallel), CONTEXT→context, LOG→logs
- **Concurrent flush**: All tables flushed in parallel via tokio::join!

## Implementations

| Type | Config | Port | Protocol | Use Case |
|------|--------|------|----------|----------|
| `clickhouse` | Arrow HTTP | 8123 | HTTP + Arrow IPC | **Recommended** - High throughput (65M+ events/sec) |
| `clickhouse_native` | Native | 9000 | ClickHouse native | Legacy - If you need native protocol features |

### Arrow HTTP Sink (Recommended)

```toml
[sinks.clickhouse]
type = "clickhouse"
host = "localhost:8123"    # HTTP port
database = "tell"
username = "default"
password = "secret"
batch_size = 50000
flush_interval = "5s"
```

```rust
use sinks::clickhouse::{ArrowClickHouseSink, ClickHouseConfig};

let config = ClickHouseConfig::default()
    .with_url("http://localhost:8123")
    .with_database("tell")
    .with_credentials("user", "password")
    .with_batch_size(50000);

let sink = ArrowClickHouseSink::new(config, receiver);
tokio::spawn(sink.run());
```

**Why Arrow?**
- UUIDs: FlatBuffer bytes → Arrow FixedSizeBinary(16) → ClickHouse UUID (single 16-byte copy)
- Columnar batches match ClickHouse's internal storage format
- HTTP `FORMAT Arrow` endpoint - native Arrow ingestion, no row parsing

### Native Protocol Sink

```toml
[sinks.clickhouse_legacy]
type = "clickhouse_native"
host = "localhost:9000"    # Native port
database = "tell"
username = "default"
password = "secret"
batch_size = 50000
flush_interval = "5s"
```

```rust
use sinks::clickhouse::{ClickHouseSink, ClickHouseConfig};

let config = ClickHouseConfig::default()
    .with_url("http://localhost:9000")  // Note: crate still uses http:// prefix
    .with_database("tell")
    .with_credentials("user", "password");

let sink = ClickHouseSink::new(config, receiver);
tokio::spawn(sink.run());
```

## Event Routing

| EventType | Target Tables | Notes |
|-----------|---------------|-------|
| `TRACK` | events_v1 | Standard event tracking |
| `IDENTIFY` | users_v1 + user_devices + user_traits | 3 parallel inserts |
| `CONTEXT` | context_v1 | Device/location fields extracted from payload |
| `GROUP`, `ALIAS`, `ENRICH` | *(dropped)* | Logged as warning, not routed |
| `LOG` | logs_v1 | With optional pattern_id from transformer |
| `SNAPSHOT` | snapshots_v1 | Connector data snapshots |

## Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `host` | *(required)* | ClickHouse endpoint (8123 for HTTP, 9000 for native) |
| `database` | `default` | Database name |
| `username` | `default` | Auth username |
| `password` | *(empty)* | Auth password |
| `batch_size` | `50000` | Rows per table before flush |
| `flush_interval` | `5s` | Timer-based flush |
| `retry_attempts` | `3` | Retries with exponential backoff |

## Table Schemas

### events_v1 (TRACK)
```sql
CREATE TABLE events_v1 (
    timestamp DateTime64(3),
    event_name LowCardinality(String),
    device_id UUID,
    session_id UUID,
    source_ip FixedString(16),
    properties JSON,
    _raw String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp, device_id);
```

### users_v1 (IDENTIFY)
```sql
CREATE TABLE users_v1 (
    user_id String,        -- UUID v5 from normalized email
    email String,
    name String,
    updated_at DateTime64(3)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY user_id;
```

### user_devices (IDENTIFY)
```sql
CREATE TABLE user_devices (
    user_id String,
    device_id UUID,
    linked_at DateTime64(3)
) ENGINE = ReplacingMergeTree(linked_at)
ORDER BY (user_id, device_id);
```

### user_traits (IDENTIFY)
```sql
CREATE TABLE user_traits (
    user_id String,
    trait_key String,
    trait_value String,
    updated_at DateTime64(3)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (user_id, trait_key);
```

### context_v1 (CONTEXT)
```sql
CREATE TABLE context_v1 (
    timestamp DateTime64(3),
    device_id UUID,
    session_id UUID,
    source_ip FixedString(16),
    device_type LowCardinality(String),
    device_model LowCardinality(String),
    operating_system LowCardinality(String),
    os_version String,
    app_version String,
    app_build String,
    timezone String,
    locale FixedString(5),
    country LowCardinality(String),
    region String,
    city String,
    properties JSON
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp, device_id);
```

### logs_v1 (LOG)
```sql
CREATE TABLE logs_v1 (
    timestamp DateTime64(3),
    level Enum8('EMERGENCY'=0, 'ALERT'=1, 'CRITICAL'=2, 'ERROR'=3,
                'WARNING'=4, 'NOTICE'=5, 'INFO'=6, 'DEBUG'=7, 'TRACE'=8),
    source LowCardinality(String),
    service LowCardinality(String),
    session_id UUID,
    source_ip FixedString(16),
    pattern_id Nullable(UInt64),
    message JSON,
    _raw String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp, level);
```

### snapshots_v1 (SNAPSHOT)
```sql
CREATE TABLE snapshots_v1 (
    timestamp DateTime64(3),
    connector LowCardinality(String),
    entity String,
    metrics JSON
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp, connector);
```

## IDENTIFY Flow

When an IDENTIFY event arrives:

1. **Extract email** from payload JSON
2. **Generate user_id** via deterministic UUID v5 from normalized (lowercase, trimmed) email
3. **Insert to users_v1**: user_id, email, name
4. **Insert to user_devices**: user_id + device_id relationship
5. **Insert to user_traits**: Expand `traits` object to individual key-value rows

All 3 inserts happen in parallel.
