# tell-client

FlatBuffer builders for analytics events and logs. Used for testing and SDK development.

## Modules

| Module | Purpose |
|--------|---------|
| `batch` | Outer Batch wrapper (common.fbs) |
| `event` | Analytics events - Track, Identify, Group, etc. (event.fbs) |
| `log` | Structured log entries - RFC 5424 levels (log.fbs) |

## Quick Start

### Events

```rust
use tell_client::{BatchBuilder, SchemaType};
use tell_client::event::{EventBuilder, EventDataBuilder, EventType};

// Build events
let event = EventBuilder::new()
    .track("checkout_completed")
    .device_id([0x01; 16])
    .session_id([0x02; 16])
    .timestamp_now()
    .payload_json(r#"{"order_id": "123", "amount": 99.99}"#)
    .build()?;

// Batch multiple events
let event_data = EventDataBuilder::new()
    .add(event)
    .build()?;

// Wrap in Batch for sending
let batch = BatchBuilder::new()
    .api_key([0x01; 16])
    .event_data(event_data)  // Sets schema_type automatically
    .build()?;

// Send batch.as_bytes() to tell server
```

### Logs

```rust
use tell_client::{BatchBuilder, SchemaType};
use tell_client::log::{LogEntryBuilder, LogDataBuilder, LogLevel};

// Build log entries
let log = LogEntryBuilder::new()
    .error()
    .source("web-01.prod")
    .service("api-gateway")
    .timestamp_now()
    .payload_json(r#"{"error": "connection timeout", "retry": 3}"#)
    .build()?;

// Batch multiple logs
let log_data = LogDataBuilder::new()
    .add(log)
    .build()?;

// Wrap in Batch for sending
let batch = BatchBuilder::new()
    .api_key([0x01; 16])
    .log_data(log_data)  // Sets schema_type automatically
    .build()?;
```

### Raw Batch (low-level)

```rust
use tell_client::{BatchBuilder, SchemaType};

let batch = BatchBuilder::new()
    .api_key([0x01; 16])
    .schema_type(SchemaType::Event)
    .data(b"raw flatbuffer bytes")
    .version(100)           // Optional: protocol version
    .batch_id(12345)        // Optional: deduplication ID
    .source_ip([0; 16])     // Optional: for forwarded batches
    .build()?;
```

## Event Types

| Type | Method | Destination |
|------|--------|-------------|
| Track | `.track("name")` | events table |
| Identify | `.identify()` | users table |
| Group | `.group()` | users table |
| Alias | `.event_type(EventType::Alias)` | users table |
| Enrich | `.event_type(EventType::Enrich)` | metadata |
| Context | `.event_type(EventType::Context)` | context table |

## Log Levels

| Level | Method | RFC 5424 |
|-------|--------|----------|
| Emergency | `.emergency()` | 0 |
| Alert | `.alert()` | 1 |
| Critical | `.critical()` | 2 |
| Error | `.error()` | 3 |
| Warning | `.warning()` | 4 |
| Notice | `.notice()` | 5 |
| Info | `.info()` | 6 (default) |
| Debug | `.debug()` | 7 |
| Trace | `.trace()` | 8 |

## Test Clients

Simple async clients for testing and benchmarking:

```rust
use tell_client::test::{TcpTestClient, SyslogTcpTestClient, SyslogUdpTestClient};

// FlatBuffer over TCP
let mut tcp = TcpTestClient::connect("127.0.0.1:50000").await?;
tcp.send(&batch).await?;
tcp.close().await?;

// Syslog over TCP (line-delimited)
let mut syslog_tcp = SyslogTcpTestClient::connect("127.0.0.1:514").await?;
syslog_tcp.send("<134>Dec 20 12:34:56 host app: message").await?;
syslog_tcp.close().await?;

// Syslog over UDP
let syslog_udp = SyslogUdpTestClient::new().await?;
syslog_udp.send_to("<134>Dec 20 12:34:56 host app: message", "127.0.0.1:514").await?;
```

### Syslog Message Helpers

```rust
// RFC 3164 format
let msg = SyslogTcpTestClient::rfc3164(134, "hostname", "app", "message");

// RFC 5424 format
let msg = SyslogTcpTestClient::rfc5424(165, "hostname", "app", "1234", "ID47", "message");
```

## Tests

```bash
cargo test -p tell-client
```

172 tests including roundtrip validation (build -> parse -> verify).
