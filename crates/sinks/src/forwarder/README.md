# Forwarder Sink

Tell-to-Tell forwarding for **edge deployments** and **multi-region** architectures.

## TL;DR

```toml
[sinks.forwarder]
type = "forwarder"
target = "central.example.com:8081"
api_key = "0123456789abcdef0123456789abcdef"
```

## When to Use

- Edge → Central data collection
- Multi-region aggregation
- SOC (Security Operations Center) forwarding
- Network segmentation (DMZ → internal)

## Architecture

```
┌─────────────┐         ┌─────────────┐         ┌─────────────┐
│ Edge Tell   │ ──TCP── │ Central Tell│ ──────► │ ClickHouse  │
│ (forwarder) │         │ (receiver)  │         │ Parquet     │
└─────────────┘         └─────────────┘         └─────────────┘
     SOC                    Datacenter               Storage
```

## Configuration

### Edge (Forwarder)

```toml
[sinks.forwarder]
type = "forwarder"
target = "central.example.com:8081"    # Required
api_key = "0123456789abcdef0123456789abcdef"  # 32 hex chars, required
buffer_size = 262144             # 256KB (default)
connection_timeout = "10s"       # default: 10s
write_timeout = "5s"             # default: 5s
retry_attempts = 3               # default: 3
retry_interval = "1s"            # default: 1s
reconnect_interval = "5s"        # default: 5s
metrics_enabled = true
metrics_interval = "10s"
```

### Central (Receiver)

```toml
# Accept forwarded traffic on dedicated port
[[sources.tcp]]
port = 8081
forwarding_mode = true           # Trust source_ip from forwarders

[routing]
default = ["clickhouse", "parquet"]
```

## API Key

The API key authenticates the forwarder to the central server.

```bash
# Generate a random API key
openssl rand -hex 16
# Output: 0123456789abcdef0123456789abcdef
```

Must be registered in central server's `apikeys.conf`:
```
# configs/apikeys.conf
0123456789abcdef0123456789abcdef:42:edge-soc-1
```

Format: `{key}:{workspace_id}:{description}`

## Source IP Preservation

With `forwarding_mode = true`, the original client IP is preserved:

```
Client (192.168.1.100) → Edge Tell → Central Tell
                                     ↓
                              source_ip = 192.168.1.100
                              (not edge server IP)
```

## Retry & Reconnection

| Setting | Default | Description |
|---------|---------|-------------|
| `retry_attempts` | 3 | Retries per batch on failure |
| `retry_interval` | 1s | Wait between retries |
| `reconnect_interval` | 5s | Wait before reconnecting |
| `connection_timeout` | 10s | TCP connect timeout |
| `write_timeout` | 5s | Per-batch write timeout |

## Metrics

| Metric | Description |
|--------|-------------|
| `batches_received` | Batches received for forwarding |
| `batches_sent` | Batches successfully forwarded |
| `bytes_sent` | Bytes sent over network |
| `connection_errors` | Connection failures |
| `write_errors` | Write failures |
| `retries` | Retry attempts |

## Multi-Region Example

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  US-West    │     │  US-East    │     │  EU-West    │
│  Edge Tell  │     │  Edge Tell  │     │  Edge Tell  │
└──────┬──────┘     └──────┬──────┘     └──────┬──────┘
       │                   │                   │
       └───────────────────┼───────────────────┘
                           │
                    ┌──────▼──────┐
                    │   Central   │
                    │    Tell     │
                    └──────┬──────┘
                           │
              ┌────────────┼────────────┐
              ▼            ▼            ▼
         ClickHouse    Parquet      S3 Archive
```

## Notes

- TCP connection with keep-alive
- Automatic reconnection on disconnect
- Batches buffered during reconnection
- Same FlatBuffer protocol as client → Tell
