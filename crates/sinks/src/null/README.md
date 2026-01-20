# Null Sink

Discards all data. Used for **benchmarking** pipeline throughput.

## TL;DR

```toml
[sinks.null]
type = "null"
```

## When to Use

- Benchmarking maximum pipeline throughput
- Testing routing without I/O overhead
- Debugging source/transform issues (isolate sink)

## Configuration

```toml
[sinks.null]
type = "null"
enabled = true               # default: true
metrics_enabled = true       # default: true
metrics_interval = "10s"     # default: 10s
```

## Metrics

| Metric | Description |
|--------|-------------|
| `batches_received` | Total batches discarded |
| `messages_received` | Total messages discarded |

## Benchmarking Example

```toml
# Route all traffic to null for throughput testing
[sinks.null]
type = "null"

[routing]
default = ["null"]
```

```bash
# Run benchmark
cargo run --release -- --config config.toml

# Check metrics for throughput
# Look for: "batches_received" rate
```
