//! Tests for EventBuilder and EventDataBuilder

use crate::BuilderError;
use crate::event::{EventBuilder, EventDataBuilder, EventType};

// =============================================================================
// EventBuilder basic tests
// =============================================================================

#[test]
fn test_build_minimal_event() {
    let event = EventBuilder::new().build().unwrap();

    assert!(!event.is_empty());
    assert!(event.len() > 0);
}

#[test]
fn test_build_track_event() {
    let event = EventBuilder::new()
        .track("page_view")
        .device_id([0x01; 16])
        .timestamp(1234567890000)
        .payload_json(r#"{"page": "/home"}"#)
        .build()
        .unwrap();

    assert!(!event.is_empty());
}

#[test]
fn test_build_identify_event() {
    let event = EventBuilder::new()
        .identify()
        .device_id([0x02; 16])
        .payload_json(r#"{"name": "John", "email": "john@example.com"}"#)
        .build()
        .unwrap();

    assert!(!event.is_empty());
}

#[test]
fn test_build_group_event() {
    let event = EventBuilder::new()
        .group()
        .device_id([0x03; 16])
        .payload_json(r#"{"group_id": "company-123"}"#)
        .build()
        .unwrap();

    assert!(!event.is_empty());
}

#[test]
fn test_build_full_event() {
    let event = EventBuilder::new()
        .event_type(EventType::Track)
        .event_name("checkout_completed")
        .device_id([0x01; 16])
        .session_id([0x02; 16])
        .timestamp(1700000000000)
        .payload_json(r#"{"order_id": "order-123", "amount": 99.99}"#)
        .build()
        .unwrap();

    assert!(!event.is_empty());
}

// =============================================================================
// EventType tests
// =============================================================================

#[test]
fn test_event_type_track() {
    let event = EventBuilder::new()
        .event_type(EventType::Track)
        .build()
        .unwrap();

    assert!(!event.is_empty());
}

#[test]
fn test_event_type_identify() {
    let event = EventBuilder::new()
        .event_type(EventType::Identify)
        .build()
        .unwrap();

    assert!(!event.is_empty());
}

#[test]
fn test_event_type_alias() {
    let event = EventBuilder::new()
        .event_type(EventType::Alias)
        .build()
        .unwrap();

    assert!(!event.is_empty());
}

#[test]
fn test_event_type_enrich() {
    let event = EventBuilder::new()
        .event_type(EventType::Enrich)
        .build()
        .unwrap();

    assert!(!event.is_empty());
}

#[test]
fn test_event_type_context() {
    let event = EventBuilder::new()
        .event_type(EventType::Context)
        .build()
        .unwrap();

    assert!(!event.is_empty());
}

// =============================================================================
// UUID slice tests
// =============================================================================

#[test]
fn test_device_id_slice_valid() {
    let id: [u8; 16] = [0x01; 16];
    let event = EventBuilder::new()
        .device_id_slice(&id)
        .unwrap()
        .build()
        .unwrap();

    assert!(!event.is_empty());
}

#[test]
fn test_device_id_slice_too_short() {
    let result = EventBuilder::new().device_id_slice(&[0x01; 10]);

    assert!(matches!(
        result,
        Err(BuilderError::InvalidUuidLength {
            field: "device_id",
            len: 10
        })
    ));
}

#[test]
fn test_device_id_slice_too_long() {
    let result = EventBuilder::new().device_id_slice(&[0x01; 20]);

    assert!(matches!(
        result,
        Err(BuilderError::InvalidUuidLength {
            field: "device_id",
            len: 20
        })
    ));
}

#[test]
fn test_session_id_slice_valid() {
    let id: [u8; 16] = [0x02; 16];
    let event = EventBuilder::new()
        .session_id_slice(&id)
        .unwrap()
        .build()
        .unwrap();

    assert!(!event.is_empty());
}

#[test]
fn test_session_id_slice_too_short() {
    let result = EventBuilder::new().session_id_slice(&[0x02; 8]);

    assert!(matches!(
        result,
        Err(BuilderError::InvalidUuidLength {
            field: "session_id",
            len: 8
        })
    ));
}

// =============================================================================
// Timestamp tests
// =============================================================================

#[test]
fn test_timestamp_explicit() {
    let event = EventBuilder::new()
        .timestamp(1700000000000)
        .build()
        .unwrap();

    assert!(!event.is_empty());
}

#[test]
fn test_timestamp_now() {
    let event = EventBuilder::new().timestamp_now().build().unwrap();

    assert!(!event.is_empty());
}

#[test]
fn test_timestamp_zero() {
    let event = EventBuilder::new().timestamp(0).build().unwrap();

    assert!(!event.is_empty());
}

#[test]
fn test_timestamp_max() {
    let event = EventBuilder::new().timestamp(u64::MAX).build().unwrap();

    assert!(!event.is_empty());
}

// =============================================================================
// Event name tests
// =============================================================================

#[test]
fn test_event_name_short() {
    let event = EventBuilder::new().event_name("click").build().unwrap();

    assert!(!event.is_empty());
}

#[test]
fn test_event_name_long_valid() {
    let name = "a".repeat(256);
    let event = EventBuilder::new().event_name(&name).build().unwrap();

    assert!(!event.is_empty());
}

#[test]
fn test_event_name_too_long() {
    let name = "a".repeat(257);
    let result = EventBuilder::new().event_name(&name).build();

    assert!(matches!(
        result,
        Err(BuilderError::EventNameTooLong { len: 257, max: 256 })
    ));
}

#[test]
fn test_event_name_unicode() {
    let event = EventBuilder::new().event_name("購入完了").build().unwrap();

    assert!(!event.is_empty());
}

#[test]
fn test_event_name_with_spaces() {
    let event = EventBuilder::new()
        .event_name("button click")
        .build()
        .unwrap();

    assert!(!event.is_empty());
}

// =============================================================================
// Payload tests
// =============================================================================

#[test]
fn test_payload_empty() {
    let event = EventBuilder::new().payload(b"").build().unwrap();

    assert!(!event.is_empty());
}

#[test]
fn test_payload_json() {
    let event = EventBuilder::new()
        .payload_json(r#"{"key": "value"}"#)
        .build()
        .unwrap();

    assert!(!event.is_empty());
}

#[test]
fn test_payload_binary() {
    let binary_data: Vec<u8> = (0u8..=255).collect();
    let event = EventBuilder::new().payload(&binary_data).build().unwrap();

    assert!(!event.is_empty());
}

#[test]
fn test_payload_large_valid() {
    let large = vec![0xABu8; 1024 * 1024]; // 1 MB (exactly at limit)
    let event = EventBuilder::new().payload(&large).build().unwrap();

    assert!(!event.is_empty());
}

#[test]
fn test_payload_too_large() {
    let huge = vec![0xABu8; 1024 * 1024 + 1]; // 1 MB + 1 byte
    let result = EventBuilder::new().payload(&huge).build();

    assert!(matches!(result, Err(BuilderError::PayloadTooLarge { .. })));
}

#[test]
fn test_payload_owned() {
    let data = vec![1, 2, 3, 4, 5];
    let event = EventBuilder::new().payload_owned(data).build().unwrap();

    assert!(!event.is_empty());
}

// =============================================================================
// EventDataBuilder tests
// =============================================================================

#[test]
fn test_event_data_single() {
    let event = EventBuilder::new().track("test").build().unwrap();

    let event_data = EventDataBuilder::new().add(event).build().unwrap();

    assert!(!event_data.is_empty());
}

#[test]
fn test_event_data_multiple() {
    let event1 = EventBuilder::new()
        .track("page_view")
        .device_id([0x01; 16])
        .build()
        .unwrap();

    let event2 = EventBuilder::new()
        .track("button_click")
        .device_id([0x01; 16])
        .build()
        .unwrap();

    let event3 = EventBuilder::new()
        .identify()
        .device_id([0x01; 16])
        .payload_json(r#"{"name": "John"}"#)
        .build()
        .unwrap();

    let event_data = EventDataBuilder::new()
        .add(event1)
        .add(event2)
        .add(event3)
        .build()
        .unwrap();

    assert!(!event_data.is_empty());
}

#[test]
fn test_event_data_extend() {
    let events: Vec<_> = (0..5)
        .map(|i| {
            EventBuilder::new()
                .track(&format!("event_{}", i))
                .build()
                .unwrap()
        })
        .collect();

    let event_data = EventDataBuilder::new().extend(events).build().unwrap();

    assert!(!event_data.is_empty());
}

#[test]
fn test_event_data_empty_error() {
    let result = EventDataBuilder::new().build();

    assert!(matches!(result, Err(BuilderError::EmptyEventData)));
}

#[test]
fn test_event_data_len() {
    let event1 = EventBuilder::new().build().unwrap();
    let event2 = EventBuilder::new().build().unwrap();

    let builder = EventDataBuilder::new().add(event1).add(event2);

    assert_eq!(builder.len(), 2);
    assert!(!builder.is_empty());
}

#[test]
fn test_event_data_is_empty() {
    let builder = EventDataBuilder::new();

    assert!(builder.is_empty());
    assert_eq!(builder.len(), 0);
}

// =============================================================================
// BuiltEvent/BuiltEventData tests
// =============================================================================

#[test]
fn test_built_event_as_bytes() {
    let event = EventBuilder::new().build().unwrap();
    let bytes = event.as_bytes();

    assert!(!bytes.is_empty());
}

#[test]
fn test_built_event_into_vec() {
    let event = EventBuilder::new().build().unwrap();
    let vec = event.into_vec();

    assert!(!vec.is_empty());
}

#[test]
fn test_built_event_data_as_bytes() {
    let event = EventBuilder::new().build().unwrap();
    let event_data = EventDataBuilder::new().add(event).build().unwrap();

    let bytes = event_data.as_bytes();
    assert!(!bytes.is_empty());
}

#[test]
fn test_built_event_data_into_vec() {
    let event = EventBuilder::new().build().unwrap();
    let event_data = EventDataBuilder::new().add(event).build().unwrap();

    let vec = event_data.into_vec();
    assert!(!vec.is_empty());
}

// =============================================================================
// Builder clone/default tests
// =============================================================================

#[test]
fn test_event_builder_clone() {
    let builder = EventBuilder::new()
        .event_type(EventType::Track)
        .device_id([0x01; 16]);

    let cloned = builder.clone();

    let event1 = builder.event_name("event1").build().unwrap();
    let event2 = cloned.event_name("event2").build().unwrap();

    assert!(!event1.is_empty());
    assert!(!event2.is_empty());
}

#[test]
fn test_event_builder_default() {
    let event = EventBuilder::default().build().unwrap();

    assert!(!event.is_empty());
}

#[test]
fn test_event_data_builder_clone() {
    let event = EventBuilder::new().build().unwrap();
    let builder = EventDataBuilder::new().add(event);

    let _cloned = builder.clone();
    // Both should be usable
}

#[test]
fn test_event_data_builder_default() {
    let builder = EventDataBuilder::default();

    assert!(builder.is_empty());
}

// =============================================================================
// Large batch tests
// =============================================================================

#[test]
fn test_event_data_100_events() {
    let events: Vec<_> = (0..100)
        .map(|i| {
            EventBuilder::new()
                .track(&format!("event_{}", i))
                .device_id([i as u8; 16])
                .timestamp(1700000000000 + i as u64)
                .payload_json(&format!(r#"{{"index": {}}}"#, i))
                .build()
                .unwrap()
        })
        .collect();

    let event_data = EventDataBuilder::new().extend(events).build().unwrap();

    assert!(!event_data.is_empty());
    // Should be reasonably sized (not exploding)
    assert!(event_data.len() < 100_000);
}

// =============================================================================
// Roundtrip tests with tell-protocol decoder
// =============================================================================

#[test]
fn test_event_data_roundtrip_single_event() {
    use tell_protocol::decode_event_data;

    let event = EventBuilder::new()
        .track("page_view")
        .device_id([
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
            0x0f, 0x10,
        ])
        .session_id([
            0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e,
            0x1f, 0x20,
        ])
        .timestamp(1700000000000)
        .payload_json(r#"{"page": "/home", "referrer": "google"}"#)
        .build()
        .unwrap();

    let event_data = EventDataBuilder::new().add(event).build().unwrap();

    // Decode using tell-protocol
    let decoded = decode_event_data(event_data.as_bytes()).expect("should decode event data");

    assert_eq!(decoded.len(), 1, "should have decoded 1 event");
    let ev = &decoded[0];
    assert_eq!(ev.event_type, tell_protocol::EventType::Track);
    assert_eq!(ev.event_name, Some("page_view"));
    assert_eq!(ev.timestamp, 1700000000000);
    assert_eq!(
        ev.device_id,
        Some(&[
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
            0x0f, 0x10
        ])
    );
    assert_eq!(
        ev.session_id,
        Some(&[
            0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e,
            0x1f, 0x20
        ])
    );
    assert_eq!(
        std::str::from_utf8(ev.payload).unwrap(),
        r#"{"page": "/home", "referrer": "google"}"#
    );
}

#[test]
fn test_event_data_roundtrip_multiple_events() {
    use tell_protocol::decode_event_data;

    let event1 = EventBuilder::new()
        .track("event_1")
        .device_id([0x01; 16])
        .timestamp(1000)
        .build()
        .unwrap();

    let event2 = EventBuilder::new()
        .identify()
        .device_id([0x02; 16])
        .timestamp(2000)
        .payload_json(r#"{"email": "test@example.com"}"#)
        .build()
        .unwrap();

    let event3 = EventBuilder::new()
        .event_type(EventType::Context)
        .device_id([0x03; 16])
        .timestamp(3000)
        .build()
        .unwrap();

    let event_data = EventDataBuilder::new()
        .add(event1)
        .add(event2)
        .add(event3)
        .build()
        .unwrap();

    let decoded = decode_event_data(event_data.as_bytes()).expect("should decode event data");

    assert_eq!(decoded.len(), 3, "should have decoded 3 events");

    assert_eq!(decoded[0].event_type, tell_protocol::EventType::Track);
    assert_eq!(decoded[0].event_name, Some("event_1"));
    assert_eq!(decoded[0].timestamp, 1000);

    assert_eq!(decoded[1].event_type, tell_protocol::EventType::Identify);
    assert_eq!(decoded[1].timestamp, 2000);

    assert_eq!(decoded[2].event_type, tell_protocol::EventType::Context);
    assert_eq!(decoded[2].timestamp, 3000);
}

#[test]
fn test_full_batch_roundtrip() {
    use crate::BatchBuilder;
    use tell_protocol::{FlatBatch, SchemaType, decode_event_data};

    let event = EventBuilder::new()
        .track("checkout")
        .device_id([0xAB; 16])
        .session_id([0xCD; 16])
        .timestamp(1700000000000)
        .payload_json(r#"{"total": 99.99}"#)
        .build()
        .unwrap();

    let event_data = EventDataBuilder::new().add(event).build().unwrap();

    let batch = BatchBuilder::new()
        .api_key([0x42; 16])
        .event_data(event_data)
        .build()
        .unwrap();

    // Parse the outer Batch
    let fb = FlatBatch::parse(batch.as_bytes()).expect("should parse batch");
    assert_eq!(fb.schema_type(), SchemaType::Event);
    assert_eq!(fb.api_key().unwrap(), &[0x42; 16]);

    // Decode the inner EventData
    let data = fb.data().expect("should have data");
    let decoded = decode_event_data(data).expect("should decode event data");

    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].event_type, tell_protocol::EventType::Track);
    assert_eq!(decoded[0].event_name, Some("checkout"));
}
