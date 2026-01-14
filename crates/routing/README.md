# cdp-routing

Zero-copy routing table for O(1) source→sinks lookup.

## Overview

The routing system connects **sources** to **sinks** with optional **transformers**:

```
Sources              Transformers           Router              Sinks
───────              ────────────           ──────              ─────
tcp_main  ──┐                                              ┌──► clickhouse
tcp_debug ──┼──► [pattern_matcher] ──► Routing Table ──────┼──► disk_plaintext
syslog_tcp ─┤                             (O(1))           ├──► parquet
syslog_udp ─┘                                              └──► stdout
```

## Configuration

Routing is configured in your `config.toml`:

```toml
[routing]
# Default sinks for unmatched traffic (empty = drop)
default = ["stdout"]

# Routing rules (evaluated in order, first match wins)
[[routing.rules]]
match = { source_type = "tcp" }
sinks = ["clickhouse", "disk_plaintext"]

[[routing.rules]]
match = { source_type = "syslog" }
sinks = ["disk_plaintext"]
```

### Match Conditions

| Condition | Example | Matches |
|-----------|---------|---------|
| Exact source | `source = "tcp_main"` | Only `tcp_main` |
| Source type | `source_type = "tcp"` | `tcp_main`, `tcp_debug` |
| Source type | `source_type = "syslog"` | `syslog_tcp`, `syslog_udp` |
| Combined (AND) | `source = "syslog_tcp", source_type = "syslog"` | Only `syslog_tcp` |

### Routing Behavior

1. **First-match-wins**: Rules are evaluated in order; first matching rule determines the sinks
2. **Fan-out**: A single source can route to multiple sinks (data is Arc-cloned, not copied)
3. **Default fallback**: Unmatched traffic goes to `default` sinks (or dropped if empty)

## Transformers

Transformers process batches before they're routed to sinks.

```toml
[[routing.rules]]
match = { source_type = "syslog" }
sinks = ["clickhouse"]

# Transformers applied in order before routing
[[routing.rules.transformers]]
type = "pattern_matcher"
similarity_threshold = 0.5
cache_size = 100000

[[routing.rules.transformers]]
type = "redact"
patterns = ["email", "ssn_us"]
```

### Available Transformers

| Type | Purpose |
|------|---------|
| `pattern_matcher` | Log pattern clustering/deduplication |
| `reduce` | Aggregate events by fields |
| `filter` | Drop/keep batches by conditions |
| `redact` | Mask sensitive data (email, SSN, etc.) |
| `noop` | Pass-through (testing) |

### Transformer Scope

> **Note:** Per-route transformers are not yet supported. Transformers are applied to all routes from a source.

If a source has transformers configured, they apply to **all** sinks that source routes to:

```toml
[[routing.rules]]
match = { source_type = "syslog" }
sinks = ["clickhouse", "disk"]  # Both receive transformed data

[[routing.rules.transformers]]
type = "pattern_matcher"  # Applied before routing to ANY sink
```

## Complete Example

```toml
# =============================================================================
# Sources
# =============================================================================
[[sources.tcp]]
enabled = true
port = 50000

[sources.syslog_tcp]
enabled = true
port = 1514
workspace_id = "42"

[sources.syslog_udp]
enabled = true
port = 1514
workspace_id = "42"

# =============================================================================
# Sinks
# =============================================================================
[sinks.stdout]
type = "stdout"

[sinks.clickhouse]
type = "clickhouse"
url = "http://localhost:8123"

[sinks.disk]
type = "disk_plaintext"
path = "data/logs"

# =============================================================================
# Routing
# =============================================================================
[routing]
default = ["stdout"]

# TCP events → ClickHouse for analytics
[[routing.rules]]
match = { source_type = "tcp" }
sinks = ["clickhouse"]

# Syslog → Disk with pattern matching
[[routing.rules]]
match = { source_type = "syslog" }
sinks = ["disk", "clickhouse"]

[[routing.rules.transformers]]
type = "pattern_matcher"
similarity_threshold = 0.5
cache_size = 100000
```

## Data Flow

```
1. Batch arrives from tcp_main
2. Router looks up tcp_main in routing table → O(1)
3. If transforms configured for tcp_main → apply chain
4. Route returns &[clickhouse_id]
5. Arc::clone(batch) sent to clickhouse sink channel
```

## Implementation Details

### Zero-Copy Guarantees

- All allocations happen during table compilation at startup
- `route()` returns a slice into pre-allocated storage
- No string operations during routing
- No heap allocations in hot path

### Performance

| Operation | Complexity | Allocations |
|-----------|------------|-------------|
| `route()` | O(1) | 0 |
| `SinkId` copy | O(1) | 0 (2 bytes, fits in register) |
| Table build | O(n) | Once at startup |

### API Usage

```rust
use cdp_routing::{RoutingTable, RoutingTableBuilder, SinkId, SourceId};

// Build table at startup
let mut builder = RoutingTableBuilder::new();

// Register sinks (get numeric IDs)
builder.register_sink("stdout");      // SinkId(0)
builder.register_sink("clickhouse");  // SinkId(1)

// Set default for unmatched sources
builder.set_default_by_name(vec!["stdout".into()]);

// Add routes
builder.add_route_by_name("tcp_main", &["clickhouse"]).unwrap();

let table = builder.build().unwrap();

// Hot path: O(1) lookup, zero-copy
let sinks: &[SinkId] = table.route(&SourceId::new("tcp_main"));
```

## Tests

```bash
cargo test -p cdp-routing
```
