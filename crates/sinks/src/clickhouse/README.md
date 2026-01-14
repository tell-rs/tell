# ClickHouse Sink (CDP v1.1 Schema)

High-performance ClickHouse sink implementing the full CDP v1.1 analytics schema.

## TL;DR

- **6 tables**: events_v1, users_v1, user_devices, user_traits, context_v1, logs_v1
- **Smart routing**: TRACK→events, IDENTIFY→3 tables (parallel), CONTEXT→context, LOG→logs
- **IDENTIFY handling**: Extracts email→user_id, links devices, expands traits to key-value rows
- **CONTEXT extraction**: Parses device_type, os, app_version, timezone, locale, country, city from payload
- **Pattern ID support**: Optional pattern_id field for logs from transformer
- **Concurrent flush**: All 6 tables flushed in parallel via tokio::join!
- **66 tests**: Full coverage of routing, extraction, and edge cases

## Event Routing

| EventType | Target Tables | Notes |
|-----------|---------------|-------|
| `TRACK` | events_v1 | Standard event tracking |
| `IDENTIFY` | users_v1 + user_devices + user_traits | 3 parallel inserts |
| `CONTEXT` | context_v1 | Device/location fields extracted from payload |
| `GROUP`, `ALIAS`, `ENRICH` | *(dropped)* | Logged as warning, not routed (matches Go) |
| `LOG` | logs_v1 | With optional pattern_id |

## Usage

```rust
use cdp_sinks::clickhouse::{ClickHouseSink, ClickHouseConfig};

let config = ClickHouseConfig::default()
    .with_url("http://localhost:8123")
    .with_database("cdp")
    .with_credentials("user", "password")
    .with_batch_size(1000);

let (tx, rx) = mpsc::channel(1000);
let sink = ClickHouseSink::new(config, rx);
tokio::spawn(sink.run());
```

## Table Schemas

### events_v1 (TRACK)
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

### users_v1 (IDENTIFY - core identity)
```sql
CREATE TABLE users_v1 (
    user_id String,        -- UUID v5 from normalized email
    email String,
    name String,
    updated_at DateTime64(3)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY user_id;
```

### user_devices (IDENTIFY - device relationships)
```sql
CREATE TABLE user_devices (
    user_id String,
    device_id UUID,
    linked_at DateTime64(3)
) ENGINE = ReplacingMergeTree(linked_at)
ORDER BY (user_id, device_id);
```

### user_traits (IDENTIFY - key-value traits)
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

### logs_v1 (LOG)
```sql
CREATE TABLE logs_v1 (
    timestamp DateTime64(3),
    level LowCardinality(String),
    source String,
    service String,
    session_id UUID,
    source_ip FixedString(16),
    pattern_id Nullable(UInt64),  -- From transformer
    message String,
    _raw String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp, level);
```

## IDENTIFY Flow

When an IDENTIFY event arrives:

1. **Extract email** from payload JSON
2. **Generate user_id** via deterministic UUID v5 from normalized (lowercase, trimmed) email
3. **Insert to users_v1**: user_id, email, name
4. **Insert to user_devices**: user_id + device_id relationship
5. **Insert to user_traits**: Expand `traits` object to individual key-value rows

All 3 inserts happen in parallel.

## Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `url` | `http://localhost:8123` | ClickHouse HTTP endpoint |
| `database` | `default` | Database name |
| `username` | None | Optional auth |
| `password` | None | Optional auth |
| `batch_size` | 1000 | Rows per table before flush |
| `flush_interval` | 5s | Timer-based flush |
| `retry_attempts` | 3 | Retries with exponential backoff |

## Parity with Go Implementation

This Rust implementation matches the Go `cdp-collector/pkg/sinks/clickhouse` sink:

- ✅ Same 6-table schema
- ✅ Same event routing logic
- ✅ Same IDENTIFY → 3 table handling
- ✅ Same CONTEXT field extraction
- ✅ Same user_id generation (UUID v5 from email)
- ✅ Same pattern_id support for logs
- ✅ Same concurrent flush pattern