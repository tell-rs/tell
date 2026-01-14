# Batch Module

Builds the outer FlatBuffer wrapper (`common.fbs::Batch`).

## TL;DR

```rust
use cdp_client::{BatchBuilder, SchemaType};

let batch = BatchBuilder::new()
    .api_key([0x01; 16])
    .schema_type(SchemaType::Event)
    .data(event_data.as_bytes())  // Inner FlatBuffer (EventData, LogData, etc.)
    .build()?;

// Send batch.as_bytes() over TCP
```

## Wire Format

```
Batch {
    api_key: [u8; 16]       // Required - auth token
    schema_type: SchemaType // Event, Log, Metric, Trace, Inventory
    version: u8             // Protocol version (default: 100)
    batch_id: u64           // Deduplication ID
    data: [u8]              // Required - schema-specific payload
    source_ip: [u8; 16]     // Optional - for forwarded batches
}
```

## Usage with Event/Log Modules

```rust
use cdp_client::{BatchBuilder, EventBuilder, EventDataBuilder, SchemaType};

// Build inner payload
let event = EventBuilder::new()
    .track("page_view")
    .device_id(uuid)
    .build()?;

let event_data = EventDataBuilder::new()
    .add(event)
    .build()?;

// Wrap in batch
let batch = BatchBuilder::new()
    .api_key(key)
    .schema_type(SchemaType::Event)
    .event_data(event_data)
    .build()?;
```
