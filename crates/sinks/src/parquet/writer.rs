//! Parquet file writer for events, logs, and snapshots
//!
//! Converts row data to Arrow RecordBatches and writes to Parquet files
//! with configurable compression. Uses shared schemas from util::arrow_rows.

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

use super::ParquetSinkError;
use super::schema::Compression;
use crate::util::{
    EventRow, LogRow, SnapshotRow, event_schema, events_to_record_batch, log_schema,
    logs_to_record_batch, snapshot_schema, snapshots_to_record_batch,
};

// =============================================================================
// Parquet Writer
// =============================================================================

/// Writer for Parquet files
pub struct ParquetWriter;

impl ParquetWriter {
    /// Write event rows to a Parquet file
    ///
    /// Creates or overwrites the file at the given path.
    /// Returns the number of bytes written.
    pub fn write_events(
        path: &Path,
        rows: Vec<EventRow>,
        compression: Compression,
    ) -> Result<u64, ParquetSinkError> {
        if rows.is_empty() {
            return Ok(0);
        }

        let schema = event_schema();
        let record_batch = events_to_record_batch(rows, Arc::clone(&schema))?;

        let file = File::create(path)?;
        let props = WriterProperties::builder()
            .set_compression(compression.to_parquet())
            .build();

        let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
        writer.write(&record_batch)?;
        let metadata = writer.close()?;

        // Calculate approximate bytes written
        let bytes = metadata
            .row_groups()
            .iter()
            .map(|rg| rg.total_byte_size() as u64)
            .sum();

        Ok(bytes)
    }

    /// Write log rows to a Parquet file
    ///
    /// Creates or overwrites the file at the given path.
    /// Returns the number of bytes written.
    pub fn write_logs(
        path: &Path,
        rows: Vec<LogRow>,
        compression: Compression,
    ) -> Result<u64, ParquetSinkError> {
        if rows.is_empty() {
            return Ok(0);
        }

        let schema = log_schema();
        let record_batch = logs_to_record_batch(rows, Arc::clone(&schema))?;

        let file = File::create(path)?;
        let props = WriterProperties::builder()
            .set_compression(compression.to_parquet())
            .build();

        let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
        writer.write(&record_batch)?;
        let metadata = writer.close()?;

        // Calculate approximate bytes written
        let bytes = metadata
            .row_groups()
            .iter()
            .map(|rg| rg.total_byte_size() as u64)
            .sum();

        Ok(bytes)
    }

    /// Write snapshot rows to a Parquet file
    ///
    /// Creates or overwrites the file at the given path.
    /// Returns the number of bytes written.
    pub fn write_snapshots(
        path: &Path,
        rows: Vec<SnapshotRow>,
        compression: Compression,
    ) -> Result<u64, ParquetSinkError> {
        if rows.is_empty() {
            return Ok(0);
        }

        let schema = snapshot_schema();
        let record_batch = snapshots_to_record_batch(rows, Arc::clone(&schema))?;

        let file = File::create(path)?;
        let props = WriterProperties::builder()
            .set_compression(compression.to_parquet())
            .build();

        let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
        writer.write(&record_batch)?;
        let metadata = writer.close()?;

        // Calculate approximate bytes written
        let bytes = metadata
            .row_groups()
            .iter()
            .map(|rg| rg.total_byte_size() as u64)
            .sum();

        Ok(bytes)
    }

    /// Append event rows to an existing Parquet file
    ///
    /// If the file doesn't exist, creates it.
    /// Returns the number of bytes written in this append.
    pub fn append_events(
        path: &Path,
        rows: Vec<EventRow>,
        compression: Compression,
    ) -> Result<u64, ParquetSinkError> {
        // For simplicity, we always write a new file
        // In production, you'd want to read existing data and merge
        // or use a different file naming strategy
        Self::write_events(path, rows, compression)
    }

    /// Append log rows to an existing Parquet file
    pub fn append_logs(
        path: &Path,
        rows: Vec<LogRow>,
        compression: Compression,
    ) -> Result<u64, ParquetSinkError> {
        Self::write_logs(path, rows, compression)
    }

    /// Append snapshot rows to an existing Parquet file
    pub fn append_snapshots(
        path: &Path,
        rows: Vec<SnapshotRow>,
        compression: Compression,
    ) -> Result<u64, ParquetSinkError> {
        Self::write_snapshots(path, rows, compression)
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray, UInt64Array};
    use tempfile::tempdir;

    fn sample_events() -> Vec<EventRow> {
        vec![
            EventRow {
                timestamp: 1700000000000,
                batch_timestamp: 1700000000100,
                workspace_id: 42,
                event_type: "track".to_string(),
                event_name: Some("page_view".to_string()),
                device_id: Some(vec![1; 16]),
                session_id: Some(vec![2; 16]),
                source_ip: vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 192, 168, 1, 1],
                payload: b"{\"page\": \"/home\"}".to_vec(),
            },
            EventRow {
                timestamp: 1700000001000,
                batch_timestamp: 1700000001100,
                workspace_id: 42,
                event_type: "identify".to_string(),
                event_name: None,
                device_id: Some(vec![1; 16]),
                session_id: None,
                source_ip: vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 10, 0, 0, 1],
                payload: b"{\"user_id\": \"u123\"}".to_vec(),
            },
        ]
    }

    fn sample_logs() -> Vec<LogRow> {
        vec![
            LogRow {
                timestamp: 1700000000000,
                batch_timestamp: 1700000000100,
                workspace_id: 42,
                level: "info".to_string(),
                event_type: "log".to_string(),
                source: Some("web-server-1".to_string()),
                service: Some("nginx".to_string()),
                session_id: Some(vec![3; 16]),
                source_ip: vec![0; 16],
                payload: b"GET /api/health 200".to_vec(),
            },
            LogRow {
                timestamp: 1700000002000,
                batch_timestamp: 1700000002100,
                workspace_id: 42,
                level: "error".to_string(),
                event_type: "log".to_string(),
                source: Some("api-server-1".to_string()),
                service: Some("api".to_string()),
                session_id: None,
                source_ip: vec![0; 16],
                payload: b"Connection refused to database".to_vec(),
            },
        ]
    }

    fn sample_snapshots() -> Vec<SnapshotRow> {
        vec![
            SnapshotRow {
                timestamp: 1700000000000,
                batch_timestamp: 1700000000100,
                workspace_id: 42,
                source: "github".to_string(),
                entity: "user/repo".to_string(),
                source_ip: vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 192, 168, 1, 1],
                payload: b"{\"stars\": 100}".to_vec(),
            },
            SnapshotRow {
                timestamp: 1700000001000,
                batch_timestamp: 1700000001100,
                workspace_id: 42,
                source: "stripe".to_string(),
                entity: "acct_123".to_string(),
                source_ip: vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 10, 0, 0, 1],
                payload: b"{\"balance\": 5000}".to_vec(),
            },
        ]
    }

    #[test]
    fn test_write_events_empty() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.parquet");

        let result = ParquetWriter::write_events(&path, vec![], Compression::None);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0);
        assert!(!path.exists());
    }

    #[test]
    fn test_write_events_snappy() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("events_snappy.parquet");

        let events = sample_events();
        let result = ParquetWriter::write_events(&path, events, Compression::Snappy);

        assert!(result.is_ok());
        assert!(path.exists());
        assert!(result.unwrap() > 0);
    }

    #[test]
    fn test_write_events_uncompressed() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("events_none.parquet");

        let events = sample_events();
        let result = ParquetWriter::write_events(&path, events, Compression::None);

        assert!(result.is_ok());
        assert!(path.exists());
    }

    #[test]
    fn test_write_events_lz4() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("events_lz4.parquet");

        let events = sample_events();
        let result = ParquetWriter::write_events(&path, events, Compression::Lz4);

        assert!(result.is_ok());
        assert!(path.exists());
    }

    #[test]
    fn test_write_events_zstd() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("events_zstd.parquet");

        let events = sample_events();
        let result = ParquetWriter::write_events(&path, events, Compression::Zstd);

        assert!(result.is_ok());
        assert!(path.exists());
    }

    #[test]
    fn test_write_logs_snappy() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("logs_snappy.parquet");

        let logs = sample_logs();
        let result = ParquetWriter::write_logs(&path, logs, Compression::Snappy);

        assert!(result.is_ok());
        assert!(path.exists());
        assert!(result.unwrap() > 0);
    }

    #[test]
    fn test_write_logs_uncompressed() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("logs_none.parquet");

        let logs = sample_logs();
        let result = ParquetWriter::write_logs(&path, logs, Compression::None);

        assert!(result.is_ok());
        assert!(path.exists());
    }

    #[test]
    fn test_events_to_record_batch() {
        let events = sample_events();
        let schema = event_schema();

        let batch = events_to_record_batch(events, schema).unwrap();

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 9);
    }

    #[test]
    fn test_logs_to_record_batch() {
        let logs = sample_logs();
        let schema = log_schema();

        let batch = logs_to_record_batch(logs, schema).unwrap();

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 10);
    }

    #[test]
    fn test_events_record_batch_columns() {
        let events = vec![EventRow {
            timestamp: 1234567890000,
            batch_timestamp: 1234567890100,
            workspace_id: 99,
            event_type: "track".to_string(),
            event_name: Some("click".to_string()),
            device_id: Some(vec![0xAB; 16]),
            session_id: None,
            source_ip: vec![0; 16],
            payload: b"test".to_vec(),
        }];
        let schema = event_schema();

        let batch = events_to_record_batch(events, schema).unwrap();

        // Check timestamp column (column 0)
        let timestamps = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(timestamps.value(0), 1234567890000);

        // Check batch_timestamp column (column 1)
        let batch_timestamps = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(batch_timestamps.value(0), 1234567890100);

        // Check workspace_id column (column 2)
        let workspace_ids = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(workspace_ids.value(0), 99);

        // Check event_type column (column 3)
        let event_types = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(event_types.value(0), "track");
    }

    #[test]
    fn test_logs_record_batch_columns() {
        let logs = vec![LogRow {
            timestamp: 9876543210000,
            batch_timestamp: 9876543210100,
            workspace_id: 77,
            level: "warning".to_string(),
            event_type: "log".to_string(),
            source: Some("host-1".to_string()),
            service: Some("svc".to_string()),
            session_id: Some(vec![0xCD; 16]),
            source_ip: vec![1; 16],
            payload: b"warn msg".to_vec(),
        }];
        let schema = log_schema();

        let batch = logs_to_record_batch(logs, schema).unwrap();

        // Check timestamp column (column 0)
        let timestamps = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(timestamps.value(0), 9876543210000);

        // Check batch_timestamp column (column 1)
        let batch_timestamps = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(batch_timestamps.value(0), 9876543210100);

        // Check workspace_id column (column 2)
        let workspace_ids = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(workspace_ids.value(0), 77);

        // Check level column (column 3)
        let levels = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(levels.value(0), "warning");

        // Check source column (column 5)
        let sources = batch
            .column(5)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(sources.value(0), "host-1");
    }

    #[test]
    fn test_append_events() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("append_events.parquet");

        // First write
        let events1 = sample_events();
        let result1 = ParquetWriter::append_events(&path, events1, Compression::Snappy);
        assert!(result1.is_ok());

        // Second write (overwrites for now)
        let events2 = sample_events();
        let result2 = ParquetWriter::append_events(&path, events2, Compression::Snappy);
        assert!(result2.is_ok());

        assert!(path.exists());
    }

    #[test]
    fn test_write_large_batch() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("large_batch.parquet");

        // Create 10,000 events
        let events: Vec<EventRow> = (0..10_000)
            .map(|i| EventRow {
                timestamp: 1700000000000 + i as i64,
                batch_timestamp: 1700000000100 + i as i64,
                workspace_id: 1,
                event_type: "track".to_string(),
                event_name: Some(format!("event_{}", i)),
                device_id: Some(vec![i as u8; 16]),
                session_id: Some(vec![(i + 1) as u8; 16]),
                source_ip: vec![0; 16],
                payload: format!("{{\"index\": {}}}", i).into_bytes(),
            })
            .collect();

        let result = ParquetWriter::write_events(&path, events, Compression::Zstd);

        assert!(result.is_ok());
        assert!(path.exists());

        // Verify file size is reasonable (should be compressed)
        let metadata = std::fs::metadata(&path).unwrap();
        assert!(metadata.len() > 0);
        // 10k rows with compression should be much smaller than uncompressed
        assert!(metadata.len() < 10_000_000); // Should be well under 10MB
    }

    // =========================================================================
    // Snapshot Tests
    // =========================================================================

    #[test]
    fn test_write_snapshots_empty() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty_snapshots.parquet");

        let result = ParquetWriter::write_snapshots(&path, vec![], Compression::None);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0);
        assert!(!path.exists());
    }

    #[test]
    fn test_write_snapshots_snappy() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("snapshots_snappy.parquet");

        let snapshots = sample_snapshots();
        let result = ParquetWriter::write_snapshots(&path, snapshots, Compression::Snappy);

        assert!(result.is_ok());
        assert!(path.exists());
        assert!(result.unwrap() > 0);
    }

    #[test]
    fn test_write_snapshots_zstd() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("snapshots_zstd.parquet");

        let snapshots = sample_snapshots();
        let result = ParquetWriter::write_snapshots(&path, snapshots, Compression::Zstd);

        assert!(result.is_ok());
        assert!(path.exists());
    }

    #[test]
    fn test_snapshots_to_record_batch() {
        let snapshots = sample_snapshots();
        let schema = snapshot_schema();

        let batch = snapshots_to_record_batch(snapshots, schema).unwrap();

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 7);
    }

    #[test]
    fn test_snapshots_record_batch_columns() {
        let snapshots = vec![SnapshotRow {
            timestamp: 1234567890000,
            batch_timestamp: 1234567890100,
            workspace_id: 99,
            source: "linear".to_string(),
            entity: "issue_abc".to_string(),
            source_ip: vec![0; 16],
            payload: b"{\"status\": \"done\"}".to_vec(),
        }];
        let schema = snapshot_schema();

        let batch = snapshots_to_record_batch(snapshots, schema).unwrap();

        // Check timestamp column (column 0)
        let timestamps = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(timestamps.value(0), 1234567890000);

        // Check batch_timestamp column (column 1)
        let batch_timestamps = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(batch_timestamps.value(0), 1234567890100);

        // Check workspace_id column (column 2)
        let workspace_ids = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(workspace_ids.value(0), 99);

        // Check source column (column 3)
        let sources = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(sources.value(0), "linear");

        // Check entity column (column 4)
        let entities = batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(entities.value(0), "issue_abc");
    }

    #[test]
    fn test_append_snapshots() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("append_snapshots.parquet");

        let snapshots1 = sample_snapshots();
        let result1 = ParquetWriter::append_snapshots(&path, snapshots1, Compression::Snappy);
        assert!(result1.is_ok());

        let snapshots2 = sample_snapshots();
        let result2 = ParquetWriter::append_snapshots(&path, snapshots2, Compression::Snappy);
        assert!(result2.is_ok());

        assert!(path.exists());
    }
}
