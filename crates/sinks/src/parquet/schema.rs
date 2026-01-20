//! Parquet-specific schema definitions
//!
//! Re-exports shared Arrow row types and schemas from util::arrow_rows,
//! and provides Parquet-specific compression codec configuration.

// Re-export shared row types from util (schema functions used internally)
pub use crate::util::arrow_rows::{EventRow, LogRow, SnapshotRow};

// =============================================================================
// Compression
// =============================================================================

/// Parquet compression codec
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Compression {
    /// No compression
    None,
    /// Snappy compression (fast, moderate ratio)
    #[default]
    Snappy,
    /// LZ4 compression (very fast, lower ratio)
    Lz4,
    /// Zstd compression (slower, best ratio)
    Zstd,
}

impl Compression {
    /// Convert to parquet compression type
    pub fn to_parquet(self) -> parquet::basic::Compression {
        match self {
            Self::None => parquet::basic::Compression::UNCOMPRESSED,
            Self::Snappy => parquet::basic::Compression::SNAPPY,
            Self::Lz4 => parquet::basic::Compression::LZ4,
            Self::Zstd => parquet::basic::Compression::ZSTD(Default::default()),
        }
    }

    /// Parse from string representation
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "none" | "uncompressed" => Some(Self::None),
            "snappy" => Some(Self::Snappy),
            "lz4" => Some(Self::Lz4),
            "zstd" => Some(Self::Zstd),
            _ => None,
        }
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::{event_schema, log_schema, snapshot_schema};
    use arrow::datatypes::DataType;

    #[test]
    fn test_compression_to_parquet() {
        assert!(matches!(
            Compression::None.to_parquet(),
            parquet::basic::Compression::UNCOMPRESSED
        ));
        assert!(matches!(
            Compression::Snappy.to_parquet(),
            parquet::basic::Compression::SNAPPY
        ));
        assert!(matches!(
            Compression::Lz4.to_parquet(),
            parquet::basic::Compression::LZ4
        ));
        assert!(matches!(
            Compression::Zstd.to_parquet(),
            parquet::basic::Compression::ZSTD(_)
        ));
    }

    #[test]
    fn test_compression_from_str() {
        assert_eq!(Compression::parse("none"), Some(Compression::None));
        assert_eq!(Compression::parse("uncompressed"), Some(Compression::None));
        assert_eq!(Compression::parse("snappy"), Some(Compression::Snappy));
        assert_eq!(Compression::parse("SNAPPY"), Some(Compression::Snappy));
        assert_eq!(Compression::parse("lz4"), Some(Compression::Lz4));
        assert_eq!(Compression::parse("zstd"), Some(Compression::Zstd));
        assert_eq!(Compression::parse("invalid"), None);
    }

    #[test]
    fn test_compression_default() {
        let compression = Compression::default();
        assert_eq!(compression, Compression::Snappy);
    }

    // Schema tests now in util/arrow_rows.rs, but verify re-exports work
    #[test]
    fn test_event_schema_fields() {
        let schema = event_schema();
        assert_eq!(schema.fields().len(), 9);
        assert_eq!(schema.field(0).name(), "timestamp");
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    }

    #[test]
    fn test_log_schema_fields() {
        let schema = log_schema();
        assert_eq!(schema.fields().len(), 10);
        assert_eq!(schema.field(0).name(), "timestamp");
    }

    #[test]
    fn test_snapshot_schema_fields() {
        let schema = snapshot_schema();
        assert_eq!(schema.fields().len(), 7);
        assert_eq!(schema.field(0).name(), "timestamp");
    }

    #[test]
    fn test_event_row() {
        let row = EventRow {
            timestamp: 1234567890000,
            batch_timestamp: 1234567890100,
            workspace_id: 42,
            event_type: "track".to_string(),
            event_name: Some("page_view".to_string()),
            device_id: Some(vec![0; 16]),
            session_id: Some(vec![1; 16]),
            source_ip: vec![0; 16],
            payload: b"{}".to_vec(),
        };

        assert_eq!(row.timestamp, 1234567890000);
        assert_eq!(row.batch_timestamp, 1234567890100);
        assert_eq!(row.workspace_id, 42);
        assert_eq!(row.event_type, "track");
    }

    #[test]
    fn test_log_row() {
        let row = LogRow {
            timestamp: 1234567890000,
            batch_timestamp: 1234567890100,
            workspace_id: 42,
            level: "info".to_string(),
            event_type: "log".to_string(),
            source: Some("app-server-1".to_string()),
            service: Some("api".to_string()),
            session_id: None,
            source_ip: vec![0; 16],
            payload: b"log message".to_vec(),
        };

        assert_eq!(row.timestamp, 1234567890000);
        assert_eq!(row.level, "info");
        assert_eq!(row.source, Some("app-server-1".to_string()));
    }

    #[test]
    fn test_snapshot_row() {
        let row = SnapshotRow {
            timestamp: 1234567890000,
            batch_timestamp: 1234567890100,
            workspace_id: 42,
            source: "github".to_string(),
            entity: "user/repo".to_string(),
            source_ip: vec![0; 16],
            payload: b"{\"stars\": 100}".to_vec(),
        };

        assert_eq!(row.timestamp, 1234567890000);
        assert_eq!(row.source, "github");
        assert_eq!(row.entity, "user/repo");
    }
}
