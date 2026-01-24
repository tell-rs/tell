//! Tap formatting helpers for live event tail.
//!
//! Unix-only functionality for formatting tap envelopes.

/// Batch type constants (aligned with SchemaType wire values)
#[cfg(unix)]
const BATCH_TYPE_EVENT: u8 = 1;
#[cfg(unix)]
const BATCH_TYPE_LOG: u8 = 2;

/// Format a TapEnvelope into human-readable strings (one per decoded item)
#[cfg(unix)]
pub fn format_tap_envelope(envelope: &tell_tap::TapEnvelope) -> String {
    match envelope.batch_type {
        BATCH_TYPE_EVENT => format_events(envelope),
        BATCH_TYPE_LOG => format_logs(envelope),
        _ => {
            // Fallback for other types - show metadata
            let batch_type = match envelope.batch_type {
                3 => "METRIC",
                4 => "TRACE",
                5 => "SNAPSHOT",
                _ => "UNKNOWN",
            };
            format!(
                "[{}] source: {}, workspace: {}, count: {}",
                batch_type, envelope.source_id, envelope.workspace_id, envelope.count
            )
        }
    }
}

/// Get a message from the envelope by index
#[cfg(unix)]
fn get_envelope_message(envelope: &tell_tap::TapEnvelope, index: usize) -> Option<&[u8]> {
    if index >= envelope.offsets.len() {
        return None;
    }
    let start = envelope.offsets[index] as usize;
    let len = envelope.lengths[index] as usize;
    if start + len > envelope.payload.len() {
        return None;
    }
    Some(&envelope.payload[start..start + len])
}

/// Format events from envelope
#[cfg(unix)]
fn format_events(envelope: &tell_tap::TapEnvelope) -> String {
    use tell_protocol::{FlatBatch, decode_event_data};

    let mut lines = Vec::new();

    for i in 0..envelope.offsets.len() {
        let Some(msg) = get_envelope_message(envelope, i) else {
            continue;
        };

        let flat_batch = match FlatBatch::parse(msg) {
            Ok(fb) => fb,
            Err(e) => {
                lines.push(format!("[EVENT] parse error: {}", e));
                continue;
            }
        };

        let data = match flat_batch.data() {
            Ok(d) => d,
            Err(e) => {
                lines.push(format!("[EVENT] data error: {}", e));
                continue;
            }
        };

        let events = match decode_event_data(data) {
            Ok(e) => e,
            Err(e) => {
                lines.push(format!("[EVENT] decode error: {}", e));
                continue;
            }
        };

        for event in events {
            let name = event.event_name.unwrap_or("-");
            let event_type = event.event_type.as_str();
            let payload = format_payload_short(event.payload);

            lines.push(format!("[EVENT] {} {} {}", event_type, name, payload));
        }
    }

    if lines.is_empty() {
        format!(
            "[EVENT] source: {}, count: {}",
            envelope.source_id, envelope.count
        )
    } else {
        lines.join("\n")
    }
}

/// Format logs from envelope
#[cfg(unix)]
fn format_logs(envelope: &tell_tap::TapEnvelope) -> String {
    use tell_protocol::{FlatBatch, decode_log_data};

    let mut lines = Vec::new();

    for i in 0..envelope.offsets.len() {
        let Some(msg) = get_envelope_message(envelope, i) else {
            continue;
        };

        let flat_batch = match FlatBatch::parse(msg) {
            Ok(fb) => fb,
            Err(e) => {
                lines.push(format!("[LOG] parse error: {}", e));
                continue;
            }
        };

        let data = match flat_batch.data() {
            Ok(d) => d,
            Err(e) => {
                lines.push(format!("[LOG] data error: {}", e));
                continue;
            }
        };

        let logs = match decode_log_data(data) {
            Ok(l) => l,
            Err(e) => {
                lines.push(format!("[LOG] decode error: {}", e));
                continue;
            }
        };

        for log in logs {
            let level = log.level.as_str();
            let service = log.service.unwrap_or("-");
            let payload = format_payload_short(log.payload);

            lines.push(format!("[LOG] {} {} {}", level, service, payload));
        }
    }

    if lines.is_empty() {
        format!(
            "[LOG] source: {}, count: {}",
            envelope.source_id, envelope.count
        )
    } else {
        lines.join("\n")
    }
}

/// Format payload as short inline string
pub fn format_payload_short(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        return String::new();
    }

    // Try JSON first - compact it
    if let Ok(json) = serde_json::from_slice::<serde_json::Value>(bytes) {
        let compact = serde_json::to_string(&json).unwrap_or_default();
        if compact.len() > 60 {
            format!("{}...", &compact[..57])
        } else {
            compact
        }
    } else if let Ok(s) = std::str::from_utf8(bytes) {
        // UTF-8 string
        if s.len() > 60 {
            format!("{}...", &s[..57])
        } else {
            s.to_string()
        }
    } else {
        // Binary - show length
        format!("<{} bytes>", bytes.len())
    }
}
