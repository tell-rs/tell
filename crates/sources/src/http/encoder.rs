//! JSON to FlatBuffer encoding
//!
//! Converts JSON event/log requests to FlatBuffer wire format.

use std::time::{SystemTime, UNIX_EPOCH};

use tell_protocol::{
    BatchEncoder, EncodedEvent, EncodedLogEntry, EventEncoder, EventTypeValue, LogEncoder,
    LogEventTypeValue, LogLevelValue, SchemaType,
};

use super::error::HttpSourceError;
use super::json_types::{JsonEvent, JsonLogEntry};

/// Encode events from JSON to FlatBuffer Batch format
///
/// Takes a slice of parsed JSON events and returns a complete Batch FlatBuffer.
pub fn encode_events(events: &[JsonEvent], api_key: &[u8; 16]) -> Result<Vec<u8>, HttpSourceError> {
    if events.is_empty() {
        return Err(HttpSourceError::InvalidRequest(
            "no events to encode".into(),
        ));
    }

    // Convert JSON events to EncodedEvent structs
    let encoded: Vec<EncodedEvent> = events
        .iter()
        .map(json_event_to_encoded)
        .collect::<Result<Vec<_>, _>>()?;

    // Encode EventData
    let event_data = EventEncoder::encode_events(&encoded);

    // Wrap in Batch
    let mut batch_encoder = BatchEncoder::new();
    batch_encoder.set_api_key(api_key);
    batch_encoder.set_schema_type(SchemaType::Event);

    Ok(batch_encoder.encode(&event_data))
}

/// Encode logs from JSON to FlatBuffer Batch format
pub fn encode_logs(logs: &[JsonLogEntry], api_key: &[u8; 16]) -> Result<Vec<u8>, HttpSourceError> {
    if logs.is_empty() {
        return Err(HttpSourceError::InvalidRequest("no logs to encode".into()));
    }

    // Convert JSON logs to EncodedLogEntry structs
    let encoded: Vec<EncodedLogEntry> = logs
        .iter()
        .map(json_log_to_encoded)
        .collect::<Result<Vec<_>, _>>()?;

    // Encode LogData
    let log_data = LogEncoder::encode_logs(&encoded);

    // Wrap in Batch
    let mut batch_encoder = BatchEncoder::new();
    batch_encoder.set_api_key(api_key);
    batch_encoder.set_schema_type(SchemaType::Log);

    Ok(batch_encoder.encode(&log_data))
}

/// Convert a JSON event to EncodedEvent
fn json_event_to_encoded(event: &JsonEvent) -> Result<EncodedEvent, HttpSourceError> {
    let event_type = EventTypeValue::from_str(&event.event_type);

    // Parse device_id as UUID
    let device_id = parse_uuid(&event.device_id).ok_or_else(|| HttpSourceError::InvalidField {
        field: "device_id",
        message: "must be a valid UUID".into(),
    })?;

    // Parse optional session_id
    let session_id = event.session_id.as_ref().and_then(|s| parse_uuid(s));

    // Get timestamp or use current time
    let timestamp = event.timestamp.unwrap_or_else(current_timestamp_ms);

    // Get event name
    let event_name = event.event.clone();

    // Build payload JSON (combine properties, traits, context)
    let payload = build_event_payload(event)?;

    Ok(EncodedEvent {
        event_type,
        timestamp,
        device_id: Some(device_id),
        session_id,
        event_name,
        payload,
    })
}

/// Convert a JSON log entry to EncodedLogEntry
fn json_log_to_encoded(log: &JsonLogEntry) -> Result<EncodedLogEntry, HttpSourceError> {
    let event_type = LogEventTypeValue::from_str(&log.log_type);
    let level = LogLevelValue::from_str(&log.level);

    // Parse optional session_id
    let session_id = log.session_id.as_ref().and_then(|s| parse_uuid(s));

    // Get timestamp or use current time
    let timestamp = log.timestamp.unwrap_or_else(current_timestamp_ms);

    // Build payload JSON
    let payload = build_log_payload(log)?;

    Ok(EncodedLogEntry {
        event_type,
        session_id,
        level,
        timestamp,
        source: log.source.clone(),
        service: log.service.clone(),
        payload,
    })
}

/// Parse a UUID string to 16 bytes
fn parse_uuid(s: &str) -> Option<[u8; 16]> {
    // Remove hyphens and parse as hex
    let hex: String = s.chars().filter(|c| *c != '-').collect();

    if hex.len() != 32 {
        return None;
    }

    let mut bytes = [0u8; 16];
    for (i, chunk) in hex.as_bytes().chunks(2).enumerate() {
        let hex_str = std::str::from_utf8(chunk).ok()?;
        bytes[i] = u8::from_str_radix(hex_str, 16).ok()?;
    }

    Some(bytes)
}

/// Get current timestamp in milliseconds
fn current_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Build event payload JSON from various fields
fn build_event_payload(event: &JsonEvent) -> Result<Vec<u8>, HttpSourceError> {
    let mut payload = serde_json::Map::new();

    // Add user_id if present
    if let Some(ref user_id) = event.user_id {
        payload.insert("user_id".into(), serde_json::Value::String(user_id.clone()));
    }

    // Add group_id if present
    if let Some(ref group_id) = event.group_id {
        payload.insert(
            "group_id".into(),
            serde_json::Value::String(group_id.clone()),
        );
    }

    // Merge properties
    if let Some(ref props) = event.properties {
        if let serde_json::Value::Object(map) = props {
            for (k, v) in map {
                payload.insert(k.clone(), v.clone());
            }
        }
    }

    // Merge traits
    if let Some(ref traits) = event.traits {
        if let serde_json::Value::Object(map) = traits {
            for (k, v) in map {
                payload.insert(k.clone(), v.clone());
            }
        }
    }

    // Add context separately if present
    if let Some(ref ctx) = event.context {
        payload.insert("_context".into(), ctx.clone());
    }

    // Serialize to bytes
    serde_json::to_vec(&serde_json::Value::Object(payload))
        .map_err(|e| HttpSourceError::InvalidRequest(format!("failed to serialize payload: {e}")))
}

/// Build log payload JSON
fn build_log_payload(log: &JsonLogEntry) -> Result<Vec<u8>, HttpSourceError> {
    let mut payload = serde_json::Map::new();

    // Always include message
    payload.insert(
        "message".into(),
        serde_json::Value::String(log.message.clone()),
    );

    // Merge additional data
    if let Some(ref data) = log.data {
        if let serde_json::Value::Object(map) = data {
            for (k, v) in map {
                payload.insert(k.clone(), v.clone());
            }
        }
    }

    serde_json::to_vec(&serde_json::Value::Object(payload))
        .map_err(|e| HttpSourceError::InvalidRequest(format!("failed to serialize payload: {e}")))
}
