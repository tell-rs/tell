# Disk Plaintext Sink

Human-readable text logs on disk. Good for **grep**, **tail**, and log aggregators.

## TL;DR

```toml
[sinks.disk_plaintext]
type = "disk_plaintext"
path = "logs/"
rotation = "daily"
compression = "none"     # none, lz4
```

```bash
# Tail logs
tail -f logs/42/2024-01-15/messages.log

# Search
grep "error" logs/42/2024-01-15/messages.log
```

## When to Use

- Human-readable logs for ops
- Integration with log aggregators (Fluentd, Logstash)
- Quick grep/tail debugging
- Compliance archives requiring readable format

## Configuration

```toml
[sinks.disk_plaintext]
type = "disk_plaintext"
path = "logs/"                   # Output directory (required)
rotation = "daily"               # hourly, daily (default: daily)
compression = "none"             # none, lz4 (default: none)
buffer_size = 8388608            # 8MB (default)
write_queue_size = 10000         # default: 10000
flush_interval = "100ms"         # default: 100ms
metrics_enabled = true
metrics_interval = "10s"
```

## File Layout

```
{path}/
└── {workspace_id}/
    └── {date}/
        └── {hour}/              # Only with hourly rotation
            └── messages.log     # or messages.log.lz4
```

## Output Format

Each line is a single message:
```
2024-01-15T14:30:00.123Z [track] page_view {"page": "/home", "user": "u123"}
2024-01-15T14:30:00.456Z [identify] {"user_id": "u123", "email": "user@example.com"}
2024-01-15T14:30:01.789Z [log:info] GET /api/health 200
```

## Compression

```toml
# LZ4 compression (fast, ~3x smaller)
compression = "lz4"
```

Reading compressed files:
```bash
# Decompress and read
lz4cat logs/42/2024-01-15/messages.log.lz4

# Pipe to grep
lz4cat logs/42/2024-01-15/messages.log.lz4 | grep "error"
```

## Log Aggregator Integration

### Fluentd

```xml
<source>
  @type tail
  path /var/tell/logs/**/messages.log
  pos_file /var/tell/fluentd.pos
  <parse>
    @type json
  </parse>
</source>
```

### Filebeat

```yaml
filebeat.inputs:
  - type: log
    paths:
      - /var/tell/logs/**/messages.log
```

## Metrics

| Metric | Description |
|--------|-------------|
| `batches_received` | Total batches processed |
| `bytes_written` | Bytes written to disk |
| `files_created` | Files created (rotations) |
| `write_errors` | Write failures |

## Notes

- Uses atomic file rotation (zero data loss)
- Buffer pool reduces allocations
- For columnar analytics, use `parquet` sink instead
