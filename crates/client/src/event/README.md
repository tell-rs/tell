# Event Module

Builds CDP/product analytics events (`event.fbs`).

## TL;DR

```rust
use cdp_client::event::{EventBuilder, EventDataBuilder, EventType};

// Single event
let event = EventBuilder::new()
    .event_type(EventType::Track)
    .event_name("checkout_completed")
    .device_id(device_uuid)
    .session_id(session_uuid)
    .timestamp_now()
    .payload_json(r#"{"order_id": "123", "amount": 99.99}"#)?
    .build()?;

// Batch of events
let event_data = EventDataBuilder::new()
    .add(event)
    .build()?;

// Wrap in batch for sending
let batch = BatchBuilder::new()
    .api_key(key)
    .event_data(event_data)
    .build()?;
```

## Event Types

| Type | Purpose | Destination |
|------|---------|-------------|
| `Track` | User actions (clicks, views) | events table |
| `Identify` | User traits updates | users table |
| `Group` | Group membership | users table |
| `Alias` | Identity merging | users table |
| `Enrich` | Entity enrichment | metadata |
| `Context` | Session/device context | context table |

## Wire Format

```
Event {
    event_type: EventType     // Routing selector
    timestamp: u64            // Unix ms
    device_id: [u8; 16]       // Device UUID
    session_id: [u8; 16]      // Session UUID
    event_name: string        // e.g. "page_view"
    payload: [u8]             // JSON bytes
}

EventData {
    events: [Event]           // Vector of events
}
```
