# Log Module

Builds structured log entries (`log.fbs`).

## TL;DR

```rust
use cdp_client::log::{LogEntryBuilder, LogDataBuilder, LogLevel};

// Single log entry
let log = LogEntryBuilder::new()
    .level(LogLevel::Error)
    .source("web-01.prod")
    .service("api-gateway")
    .timestamp_now()
    .payload_json(r#"{"error": "connection timeout", "retry": 3}"#)
    .build()?;

// Batch of logs
let log_data = LogDataBuilder::new()
    .add(log)
    .build()?;

// Wrap in batch for sending
let batch = BatchBuilder::new()
    .api_key(key)
    .log_data(log_data)
    .build()?;
```

## Log Levels (RFC 5424)

| Level | Value | Description |
|-------|-------|-------------|
| `Emergency` | 0 | System unusable |
| `Alert` | 1 | Immediate action needed |
| `Critical` | 2 | Critical conditions |
| `Error` | 3 | Error conditions |
| `Warning` | 4 | Warning conditions |
| `Notice` | 5 | Normal but significant |
| `Info` | 6 | Informational |
| `Debug` | 7 | Debug messages |
| `Trace` | 8 | Detailed tracing |

## Wire Format

```
LogEntry {
    event_type: LogEventType   // LOG or ENRICH
    session_id: [u8; 16]       // Session UUID
    level: LogLevel            // Severity
    timestamp: u64             // Unix ms
    source: string             // Hostname
    service: string            // App name
    payload: [u8]              // Structured data
}

LogData {
    logs: [LogEntry]           // Vector of log entries
}
```
