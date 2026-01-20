//! Shared Arrow row types and schema definitions
//!
//! Provides common row structs and Arrow schema definitions used by both
//! Parquet and Arrow IPC sinks. Field order is optimized for predicate
//! pushdown in analytical queries.

use std::sync::Arc;

use arrow::array::{ArrayRef, BinaryArray, Int64Array, RecordBatch, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};

// =============================================================================
// Event Schema
// =============================================================================

/// Event row for columnar storage
///
/// Field order optimized for predicate pushdown:
/// 1. timestamp       - Primary filter (time range queries)
/// 2. batch_timestamp - Processing time (monitoring, data freshness)
/// 3. workspace_id    - Tenant isolation (multi-tenant filtering)
/// 4. event_type      - Category filter (track/identify/group/alias)
/// 5. event_name      - Event filter (page_view, button_click)
/// 6. device_id       - User/device lookup
/// 7. session_id      - Session lookup
/// 8. source_ip       - Enrichment data
/// 9. payload         - Large blob, accessed last
#[derive(Debug, Clone)]
pub struct EventRow {
    /// Event timestamp in milliseconds since epoch (when event occurred)
    pub timestamp: i64,
    /// Batch processing timestamp (when sink received the data)
    pub batch_timestamp: i64,
    /// Workspace ID for tenant isolation
    pub workspace_id: u64,
    /// Event type: track, identify, group, alias, enrich, context
    pub event_type: String,
    /// Optional event name (e.g., page_view, button_click)
    pub event_name: Option<String>,
    /// Device UUID (16 bytes)
    pub device_id: Option<Vec<u8>>,
    /// Session UUID (16 bytes)
    pub session_id: Option<Vec<u8>>,
    /// Source IP address (16 bytes IPv6 format)
    pub source_ip: Vec<u8>,
    /// Event payload (JSON bytes)
    pub payload: Vec<u8>,
}

/// Create the Arrow schema for events
pub fn event_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Int64, false),
        Field::new("batch_timestamp", DataType::Int64, false),
        Field::new("workspace_id", DataType::UInt64, false),
        Field::new("event_type", DataType::Utf8, false),
        Field::new("event_name", DataType::Utf8, true),
        Field::new("device_id", DataType::Binary, true),
        Field::new("session_id", DataType::Binary, true),
        Field::new("source_ip", DataType::Binary, false),
        Field::new("payload", DataType::Binary, false),
    ]))
}

/// Convert event rows to Arrow RecordBatch
///
/// Field order optimized for predicate pushdown:
/// timestamp, batch_timestamp, workspace_id, event_type, event_name,
/// device_id, session_id, source_ip, payload
pub fn events_to_record_batch(
    rows: Vec<EventRow>,
    schema: Arc<Schema>,
) -> Result<RecordBatch, arrow::error::ArrowError> {
    let len = rows.len();

    // Pre-allocate arrays (in schema order for predicate pushdown)
    let mut timestamps = Vec::with_capacity(len);
    let mut batch_timestamps = Vec::with_capacity(len);
    let mut workspace_ids = Vec::with_capacity(len);
    let mut event_types = Vec::with_capacity(len);
    let mut event_names: Vec<Option<&str>> = Vec::with_capacity(len);
    let mut device_ids: Vec<Option<&[u8]>> = Vec::with_capacity(len);
    let mut session_ids: Vec<Option<&[u8]>> = Vec::with_capacity(len);
    let mut source_ips: Vec<&[u8]> = Vec::with_capacity(len);
    let mut payloads: Vec<&[u8]> = Vec::with_capacity(len);

    // Collect values - need to keep ownership
    let rows_ref: Vec<_> = rows.iter().collect();

    for row in &rows_ref {
        timestamps.push(row.timestamp);
        batch_timestamps.push(row.batch_timestamp);
        workspace_ids.push(row.workspace_id);
        event_types.push(row.event_type.as_str());
        event_names.push(row.event_name.as_deref());
        device_ids.push(row.device_id.as_deref());
        session_ids.push(row.session_id.as_deref());
        source_ips.push(row.source_ip.as_slice());
        payloads.push(row.payload.as_slice());
    }

    // Create Arrow arrays (must match schema field order)
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(timestamps)),       // 0: timestamp
        Arc::new(Int64Array::from(batch_timestamps)), // 1: batch_timestamp
        Arc::new(UInt64Array::from(workspace_ids)),   // 2: workspace_id
        Arc::new(StringArray::from(event_types)),     // 3: event_type
        Arc::new(StringArray::from(event_names)),     // 4: event_name
        Arc::new(BinaryArray::from(device_ids)),      // 5: device_id
        Arc::new(BinaryArray::from(session_ids)),     // 6: session_id
        Arc::new(BinaryArray::from(source_ips)),      // 7: source_ip
        Arc::new(BinaryArray::from(payloads)),        // 8: payload
    ];

    RecordBatch::try_new(schema, columns)
}

// =============================================================================
// Log Schema
// =============================================================================

/// Log row for columnar storage
///
/// Field order optimized for predicate pushdown:
/// 1. timestamp       - Primary filter (time range queries)
/// 2. batch_timestamp - Processing time (monitoring, data freshness)
/// 3. workspace_id    - Tenant isolation (multi-tenant filtering)
/// 4. level           - Log level filter (error/warn/info)
/// 5. event_type      - Event type (log, enrich)
/// 6. source          - Host filter
/// 7. service         - Service filter
/// 8. session_id      - Session lookup
/// 9. source_ip       - Enrichment data
/// 10. payload        - Large blob, accessed last
#[derive(Debug, Clone)]
pub struct LogRow {
    /// Log timestamp in milliseconds since epoch (when log was generated)
    pub timestamp: i64,
    /// Batch processing timestamp (when sink received the data)
    pub batch_timestamp: i64,
    /// Workspace ID for tenant isolation
    pub workspace_id: u64,
    /// Log level: emergency, alert, critical, error, warning, notice, info, debug, trace
    pub level: String,
    /// Log event type: log, enrich
    pub event_type: String,
    /// Source hostname/instance
    pub source: Option<String>,
    /// Service/application name
    pub service: Option<String>,
    /// Session UUID (16 bytes)
    pub session_id: Option<Vec<u8>>,
    /// Source IP address (16 bytes IPv6 format)
    pub source_ip: Vec<u8>,
    /// Log payload (structured data as bytes)
    pub payload: Vec<u8>,
}

/// Create the Arrow schema for logs
pub fn log_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Int64, false),
        Field::new("batch_timestamp", DataType::Int64, false),
        Field::new("workspace_id", DataType::UInt64, false),
        Field::new("level", DataType::Utf8, false),
        Field::new("event_type", DataType::Utf8, false),
        Field::new("source", DataType::Utf8, true),
        Field::new("service", DataType::Utf8, true),
        Field::new("session_id", DataType::Binary, true),
        Field::new("source_ip", DataType::Binary, false),
        Field::new("payload", DataType::Binary, false),
    ]))
}

/// Convert log rows to Arrow RecordBatch
///
/// Field order optimized for predicate pushdown:
/// timestamp, batch_timestamp, workspace_id, level, event_type,
/// source, service, session_id, source_ip, payload
pub fn logs_to_record_batch(
    rows: Vec<LogRow>,
    schema: Arc<Schema>,
) -> Result<RecordBatch, arrow::error::ArrowError> {
    let len = rows.len();

    // Pre-allocate arrays (in schema order for predicate pushdown)
    let mut timestamps = Vec::with_capacity(len);
    let mut batch_timestamps = Vec::with_capacity(len);
    let mut workspace_ids = Vec::with_capacity(len);
    let mut levels = Vec::with_capacity(len);
    let mut event_types = Vec::with_capacity(len);
    let mut sources: Vec<Option<&str>> = Vec::with_capacity(len);
    let mut services: Vec<Option<&str>> = Vec::with_capacity(len);
    let mut session_ids: Vec<Option<&[u8]>> = Vec::with_capacity(len);
    let mut source_ips: Vec<&[u8]> = Vec::with_capacity(len);
    let mut payloads: Vec<&[u8]> = Vec::with_capacity(len);

    // Collect values
    let rows_ref: Vec<_> = rows.iter().collect();

    for row in &rows_ref {
        timestamps.push(row.timestamp);
        batch_timestamps.push(row.batch_timestamp);
        workspace_ids.push(row.workspace_id);
        levels.push(row.level.as_str());
        event_types.push(row.event_type.as_str());
        sources.push(row.source.as_deref());
        services.push(row.service.as_deref());
        session_ids.push(row.session_id.as_deref());
        source_ips.push(row.source_ip.as_slice());
        payloads.push(row.payload.as_slice());
    }

    // Create Arrow arrays (must match schema field order)
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(timestamps)),       // 0: timestamp
        Arc::new(Int64Array::from(batch_timestamps)), // 1: batch_timestamp
        Arc::new(UInt64Array::from(workspace_ids)),   // 2: workspace_id
        Arc::new(StringArray::from(levels)),          // 3: level
        Arc::new(StringArray::from(event_types)),     // 4: event_type
        Arc::new(StringArray::from(sources)),         // 5: source
        Arc::new(StringArray::from(services)),        // 6: service
        Arc::new(BinaryArray::from(session_ids)),     // 7: session_id
        Arc::new(BinaryArray::from(source_ips)),      // 8: source_ip
        Arc::new(BinaryArray::from(payloads)),        // 9: payload
    ];

    RecordBatch::try_new(schema, columns)
}

// =============================================================================
// Snapshot Schema
// =============================================================================

/// Snapshot row for columnar storage
///
/// Stores point-in-time snapshots from external data sources (connectors).
/// Used for pulling metrics from GitHub, Stripe, Linear, etc.
///
/// Field order optimized for predicate pushdown:
/// 1. timestamp       - Primary filter (time range queries)
/// 2. batch_timestamp - Processing time (monitoring, data freshness)
/// 3. workspace_id    - Tenant isolation (multi-tenant filtering)
/// 4. source          - Connector name filter (github, stripe, linear)
/// 5. entity          - Resource ID filter (user/repo, acct_123)
/// 6. source_ip       - Enrichment data
/// 7. payload         - JSON metrics blob, accessed last
#[derive(Debug, Clone)]
pub struct SnapshotRow {
    /// Snapshot timestamp in milliseconds since epoch
    pub timestamp: i64,
    /// Batch processing timestamp (when sink received the data)
    pub batch_timestamp: i64,
    /// Workspace ID for tenant isolation
    pub workspace_id: u64,
    /// Connector name: github, stripe, linear, etc.
    pub source: String,
    /// Resource identifier: user/repo, acct_123, etc.
    pub entity: String,
    /// Source IP address (16 bytes IPv6 format)
    pub source_ip: Vec<u8>,
    /// Snapshot payload (JSON metrics as bytes)
    pub payload: Vec<u8>,
}

/// Create the Arrow schema for snapshots
pub fn snapshot_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Int64, false),
        Field::new("batch_timestamp", DataType::Int64, false),
        Field::new("workspace_id", DataType::UInt64, false),
        Field::new("source", DataType::Utf8, false),
        Field::new("entity", DataType::Utf8, false),
        Field::new("source_ip", DataType::Binary, false),
        Field::new("payload", DataType::Binary, false),
    ]))
}

/// Convert snapshot rows to Arrow RecordBatch
///
/// Field order optimized for predicate pushdown:
/// timestamp, batch_timestamp, workspace_id, source, entity, source_ip, payload
pub fn snapshots_to_record_batch(
    rows: Vec<SnapshotRow>,
    schema: Arc<Schema>,
) -> Result<RecordBatch, arrow::error::ArrowError> {
    let len = rows.len();

    // Pre-allocate arrays (in schema order for predicate pushdown)
    let mut timestamps = Vec::with_capacity(len);
    let mut batch_timestamps = Vec::with_capacity(len);
    let mut workspace_ids = Vec::with_capacity(len);
    let mut sources = Vec::with_capacity(len);
    let mut entities = Vec::with_capacity(len);
    let mut source_ips: Vec<&[u8]> = Vec::with_capacity(len);
    let mut payloads: Vec<&[u8]> = Vec::with_capacity(len);

    // Collect values
    let rows_ref: Vec<_> = rows.iter().collect();

    for row in &rows_ref {
        timestamps.push(row.timestamp);
        batch_timestamps.push(row.batch_timestamp);
        workspace_ids.push(row.workspace_id);
        sources.push(row.source.as_str());
        entities.push(row.entity.as_str());
        source_ips.push(row.source_ip.as_slice());
        payloads.push(row.payload.as_slice());
    }

    // Create Arrow arrays (must match schema field order)
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(timestamps)),       // 0: timestamp
        Arc::new(Int64Array::from(batch_timestamps)), // 1: batch_timestamp
        Arc::new(UInt64Array::from(workspace_ids)),   // 2: workspace_id
        Arc::new(StringArray::from(sources)),         // 3: source
        Arc::new(StringArray::from(entities)),        // 4: entity
        Arc::new(BinaryArray::from(source_ips)),      // 5: source_ip
        Arc::new(BinaryArray::from(payloads)),        // 6: payload
    ];

    RecordBatch::try_new(schema, columns)
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_schema_fields() {
        let schema = event_schema();
        assert_eq!(schema.fields().len(), 9);

        // Verify optimized field order
        assert_eq!(schema.field(0).name(), "timestamp");
        assert_eq!(schema.field(1).name(), "batch_timestamp");
        assert_eq!(schema.field(2).name(), "workspace_id");
        assert_eq!(schema.field(3).name(), "event_type");
        assert_eq!(schema.field(4).name(), "event_name");
        assert_eq!(schema.field(5).name(), "device_id");
        assert_eq!(schema.field(6).name(), "session_id");
        assert_eq!(schema.field(7).name(), "source_ip");
        assert_eq!(schema.field(8).name(), "payload");
    }

    #[test]
    fn test_log_schema_fields() {
        let schema = log_schema();
        assert_eq!(schema.fields().len(), 10);

        // Verify optimized field order
        assert_eq!(schema.field(0).name(), "timestamp");
        assert_eq!(schema.field(1).name(), "batch_timestamp");
        assert_eq!(schema.field(2).name(), "workspace_id");
        assert_eq!(schema.field(3).name(), "level");
        assert_eq!(schema.field(4).name(), "event_type");
        assert_eq!(schema.field(5).name(), "source");
        assert_eq!(schema.field(6).name(), "service");
        assert_eq!(schema.field(7).name(), "session_id");
        assert_eq!(schema.field(8).name(), "source_ip");
        assert_eq!(schema.field(9).name(), "payload");
    }

    #[test]
    fn test_snapshot_schema_fields() {
        let schema = snapshot_schema();
        assert_eq!(schema.fields().len(), 7);

        // Verify optimized field order
        assert_eq!(schema.field(0).name(), "timestamp");
        assert_eq!(schema.field(1).name(), "batch_timestamp");
        assert_eq!(schema.field(2).name(), "workspace_id");
        assert_eq!(schema.field(3).name(), "source");
        assert_eq!(schema.field(4).name(), "entity");
        assert_eq!(schema.field(5).name(), "source_ip");
        assert_eq!(schema.field(6).name(), "payload");
    }

    #[test]
    fn test_events_to_record_batch() {
        let events = vec![
            EventRow {
                timestamp: 1700000000000,
                batch_timestamp: 1700000000100,
                workspace_id: 42,
                event_type: "track".to_string(),
                event_name: Some("page_view".to_string()),
                device_id: Some(vec![1; 16]),
                session_id: Some(vec![2; 16]),
                source_ip: vec![0; 16],
                payload: b"{}".to_vec(),
            },
            EventRow {
                timestamp: 1700000001000,
                batch_timestamp: 1700000001100,
                workspace_id: 42,
                event_type: "identify".to_string(),
                event_name: None,
                device_id: None,
                session_id: None,
                source_ip: vec![0; 16],
                payload: b"{}".to_vec(),
            },
        ];

        let schema = event_schema();
        let batch = events_to_record_batch(events, schema).unwrap();

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 9);
    }

    #[test]
    fn test_logs_to_record_batch() {
        let logs = vec![LogRow {
            timestamp: 1700000000000,
            batch_timestamp: 1700000000100,
            workspace_id: 42,
            level: "info".to_string(),
            event_type: "log".to_string(),
            source: Some("host-1".to_string()),
            service: Some("api".to_string()),
            session_id: None,
            source_ip: vec![0; 16],
            payload: b"test".to_vec(),
        }];

        let schema = log_schema();
        let batch = logs_to_record_batch(logs, schema).unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 10);
    }

    #[test]
    fn test_snapshots_to_record_batch() {
        let snapshots = vec![SnapshotRow {
            timestamp: 1700000000000,
            batch_timestamp: 1700000000100,
            workspace_id: 42,
            source: "github".to_string(),
            entity: "user/repo".to_string(),
            source_ip: vec![0; 16],
            payload: b"{\"stars\": 100}".to_vec(),
        }];

        let schema = snapshot_schema();
        let batch = snapshots_to_record_batch(snapshots, schema).unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 7);
    }
}
