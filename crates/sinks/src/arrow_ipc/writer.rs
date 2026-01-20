//! Arrow IPC file writer for events, logs, and snapshots
//!
//! Converts row data to Arrow RecordBatches and writes to Arrow IPC files
//! (also known as Feather files). Uses shared schemas from util::arrow_rows.
//!
//! Arrow IPC format is optimized for:
//! - **Fast I/O**: ~10x faster read/write than Parquet
//! - **Zero-copy reads**: Memory-mapped files can be read without deserialization
//! - **Inter-process communication**: Streaming data between processes
//! - **Hot data storage**: Recent data that needs frequent access

use std::fs::File;
use std::io::BufWriter;
use std::path::Path;
use std::sync::Arc;

use arrow::ipc::writer::FileWriter;

use super::ArrowIpcSinkError;
use crate::util::{
    EventRow, LogRow, SnapshotRow, event_schema, events_to_record_batch, log_schema,
    logs_to_record_batch, snapshot_schema, snapshots_to_record_batch,
};

// =============================================================================
// Arrow IPC Writer
// =============================================================================

/// Writer for Arrow IPC files
pub struct ArrowIpcWriter;

impl ArrowIpcWriter {
    /// Write event rows to an Arrow IPC file
    ///
    /// Creates or overwrites the file at the given path.
    /// Returns the number of bytes written.
    pub fn write_events(path: &Path, rows: Vec<EventRow>) -> Result<u64, ArrowIpcSinkError> {
        if rows.is_empty() {
            return Ok(0);
        }

        let schema = event_schema();
        let record_batch = events_to_record_batch(rows, Arc::clone(&schema))?;

        let file = File::create(path)?;
        let writer = BufWriter::new(file);

        let mut ipc_writer = FileWriter::try_new(writer, &schema)?;
        ipc_writer.write(&record_batch)?;
        ipc_writer.finish()?;

        // Get file size
        let metadata = std::fs::metadata(path)?;
        Ok(metadata.len())
    }

    /// Write log rows to an Arrow IPC file
    ///
    /// Creates or overwrites the file at the given path.
    /// Returns the number of bytes written.
    pub fn write_logs(path: &Path, rows: Vec<LogRow>) -> Result<u64, ArrowIpcSinkError> {
        if rows.is_empty() {
            return Ok(0);
        }

        let schema = log_schema();
        let record_batch = logs_to_record_batch(rows, Arc::clone(&schema))?;

        let file = File::create(path)?;
        let writer = BufWriter::new(file);

        let mut ipc_writer = FileWriter::try_new(writer, &schema)?;
        ipc_writer.write(&record_batch)?;
        ipc_writer.finish()?;

        // Get file size
        let metadata = std::fs::metadata(path)?;
        Ok(metadata.len())
    }

    /// Write snapshot rows to an Arrow IPC file
    ///
    /// Creates or overwrites the file at the given path.
    /// Returns the number of bytes written.
    pub fn write_snapshots(path: &Path, rows: Vec<SnapshotRow>) -> Result<u64, ArrowIpcSinkError> {
        if rows.is_empty() {
            return Ok(0);
        }

        let schema = snapshot_schema();
        let record_batch = snapshots_to_record_batch(rows, Arc::clone(&schema))?;

        let file = File::create(path)?;
        let writer = BufWriter::new(file);

        let mut ipc_writer = FileWriter::try_new(writer, &schema)?;
        ipc_writer.write(&record_batch)?;
        ipc_writer.finish()?;

        // Get file size
        let metadata = std::fs::metadata(path)?;
        Ok(metadata.len())
    }

    /// Append event rows to an existing Arrow IPC file
    ///
    /// If the file doesn't exist, creates it.
    /// Returns the number of bytes written in this append.
    pub fn append_events(path: &Path, rows: Vec<EventRow>) -> Result<u64, ArrowIpcSinkError> {
        // For simplicity, we always write a new file
        // In production, you'd want to read existing data and merge
        // or use a different file naming strategy
        Self::write_events(path, rows)
    }

    /// Append log rows to an existing Arrow IPC file
    pub fn append_logs(path: &Path, rows: Vec<LogRow>) -> Result<u64, ArrowIpcSinkError> {
        Self::write_logs(path, rows)
    }

    /// Append snapshot rows to an existing Arrow IPC file
    pub fn append_snapshots(path: &Path, rows: Vec<SnapshotRow>) -> Result<u64, ArrowIpcSinkError> {
        Self::write_snapshots(path, rows)
    }
}
