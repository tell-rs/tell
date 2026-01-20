# Stdout Sink

Human-readable output to terminal. Used for **debugging** and development.

## TL;DR

```toml
[sinks.stdout]
type = "stdout"
```

## When to Use

- Debugging incoming data
- Development and testing
- Inspecting batch contents
- Verifying routing rules

## Configuration

```toml
[sinks.stdout]
type = "stdout"
enabled = true               # default: true
metrics_enabled = true       # default: true
metrics_interval = "10s"     # default: 10s
```

## Output Format

```
[2024-01-15 14:30:00] workspace=42 source=192.168.1.1 type=Event messages=5
  [0] track: page_view {"page": "/home"}
  [1] track: button_click {"button": "signup"}
  ...
```

## Debug Routing

```toml
# Send specific source to stdout for debugging
[[routing.rules]]
match = { source = "tcp_debug" }
sinks = ["stdout"]

# Default traffic to production sinks
[routing]
default = ["clickhouse", "parquet"]
```

## Metrics

| Metric | Description |
|--------|-------------|
| `batches_received` | Total batches printed |
| `messages_received` | Total messages printed |

## Notes

- **Not for production** - stdout blocks on terminal I/O
- Use `null` sink for benchmarking (no I/O overhead)
- Colored output when terminal supports it
