# HTTP Source

High-performance HTTP REST API source for Tell, supporting both JSON and binary FlatBuffer ingestion.

## Endpoints

| Endpoint | Content-Type | Description |
|----------|--------------|-------------|
| `POST /v1/events` | `application/json` | JSONL events (one JSON per line) |
| `POST /v1/logs` | `application/json` | JSONL logs (one JSON per line) |
| `POST /v1/ingest` | `application/x-flatbuffers` | Binary FlatBuffer batch (zero-copy) |
| `GET /health` | - | Health check |

## Performance

| Format | Throughput | Notes |
|--------|------------|-------|
| JSON (JSONL) | ~2M events/sec | Requires parsing and conversion |
| FlatBuffer | ~40M events/sec | Zero-copy passthrough |

## Data Formats

### Events (`/v1/events`)

JSONL format - one JSON object per line:

```json
{"type":"track","event":"page_view","device_id":"550e8400-e29b-41d4-a716-446655440000","timestamp":1700000000000,"properties":{"page":"/home"}}
{"type":"identify","device_id":"550e8400-e29b-41d4-a716-446655440000","user_id":"user_123","traits":{"name":"John"}}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | string | Yes | Event type: `track`, `identify`, `group`, `alias`, `enrich`, `context` |
| `device_id` | string | Yes | Device UUID |
| `event` | string | For track | Event name (e.g., `page_view`, `button_click`) |
| `user_id` | string | For identify | User identifier |
| `group_id` | string | For group | Group identifier |
| `session_id` | string | No | Session UUID for correlation |
| `timestamp` | number | No | Unix timestamp in milliseconds (server sets if missing) |
| `properties` | object | No | Event properties (track events) |
| `traits` | object | No | User traits (identify events) |
| `context` | object | No | Additional context data |

### Logs (`/v1/logs`)

JSONL format - one JSON object per line:

```json
{"level":"info","message":"User logged in","service":"auth-service","session_id":"abc123","data":{"user_id":"123"}}
{"level":"error","message":"Database connection failed","service":"api","data":{"error":"timeout"}}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `message` | string | Yes | Log message |
| `level` | string | No | Log level: `trace`, `debug`, `info`, `warning`, `error`, `critical` (default: `info`) |
| `timestamp` | number | No | Unix timestamp in milliseconds |
| `source` | string | No | Source hostname/instance |
| `service` | string | No | Service/application name |
| `session_id` | string | No | Session UUID for correlation |
| `data` | object | No | Additional structured data |
| `type` | string | No | Log type: `log` or `enrich` (default: `log`) |

### Binary FlatBuffer (`/v1/ingest`)

Raw Batch FlatBuffer bytes - same wire format as TCP source. Use `tell-bench` or the SDK to generate.

## Authentication

All endpoints require an API key via header:
- `X-API-Key: <32 hex characters>` or
- `Authorization: Bearer <32 hex characters>`

For binary ingest, the API key in the header must match the API key embedded in the FlatBuffer batch.

## Usage

```bash
# JSON events
curl -X POST http://localhost:8080/v1/events \
  -H "X-API-Key: 000102030405060708090a0b0c0d0e0f" \
  -H "Content-Type: application/json" \
  -d '{"type":"track","event":"test","device_id":"550e8400-e29b-41d4-a716-446655440000","timestamp":1700000000000}'

# JSON logs
curl -X POST http://localhost:8080/v1/logs \
  -H "X-API-Key: 000102030405060708090a0b0c0d0e0f" \
  -H "Content-Type: application/json" \
  -d '{"level":"info","message":"test log","timestamp":1700000000000}'

# Multiple events (JSONL - newline separated)
curl -X POST http://localhost:8080/v1/events \
  -H "X-API-Key: 000102030405060708090a0b0c0d0e0f" \
  -H "Content-Type: application/json" \
  -d '{"type":"track","event":"page_view","device_id":"abc"}
{"type":"track","event":"button_click","device_id":"abc"}'

# Binary FlatBuffer (use tell-bench or SDK)
tell-bench http-fbs load -c 5 --events 1000000
```

## Configuration

```toml
[sources.http]
enabled = true
port = 8080
host = "0.0.0.0"              # Bind address (default: 0.0.0.0)
max_payload_size = 10485760   # 10 MB max request body
```

## Response Codes

| Code | Description |
|------|-------------|
| 202 Accepted | All items accepted |
| 207 Multi-Status | Partial success (some items rejected) |
| 400 Bad Request | All items rejected or invalid request |
| 401 Unauthorized | Missing or invalid API key |
| 403 Forbidden | API key mismatch (binary endpoint) |
| 413 Payload Too Large | Request body exceeds `max_payload_size` |
| 415 Unsupported Media Type | Wrong Content-Type for binary endpoint |
| 503 Service Unavailable | Pipeline channel full |

## Module Structure

- `mod.rs` - Server startup and router
- `handlers.rs` - Request handlers for each endpoint
- `auth.rs` - API key extraction and validation
- `jsonl.rs` - JSONL parsing with DoS protection
- `response.rs` - Response building utilities
- `encoder.rs` - JSON to FlatBuffer conversion
- `json_types.rs` - JSON schema definitions
- `config.rs` - Configuration types
- `error.rs` - Error types
- `metrics.rs` - HTTP-specific metrics

## Benchmarks

```bash
# Criterion benchmarks
cargo bench -p tell-sources --bench http

# Load testing
tell-bench http-json load -c 5 --events 1000000
tell-bench http-fbs load -c 5 --events 10000000
```
