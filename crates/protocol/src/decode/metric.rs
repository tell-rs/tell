//! Metric decoding from FlatBuffer payloads
//!
//! Parses MetricData FlatBuffer containing performance metrics.
//!
//! # Schema Reference (metric.fbs)
//!
//! ```text
//! table MetricEntry {
//!     metric_type:MetricType (id: 0);
//!     timestamp:uint64 (id: 1);
//!     name:string (required, id: 2);
//!     value:double (id: 3);
//!     source:string (id: 4);
//!     service:string (id: 5);
//!     labels:[Label] (id: 6);
//!     int_labels:[IntLabel] (id: 10);
//!     temporality:Temporality (id: 7);
//!     histogram:Histogram (id: 8);
//!     session_id:[ubyte] (id: 9);
//! }
//! table MetricData { metrics:[MetricEntry] (required); }
//! ```

use super::table::{FlatTable, read_u32};
use crate::{ProtocolError, Result};

// =============================================================================
// Metric Types
// =============================================================================

/// Metric type - determines how to interpret the value
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum MetricType {
    Unknown = 0,
    Gauge = 1,
    Counter = 2,
    Histogram = 3,
}

impl MetricType {
    #[inline]
    pub const fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Gauge,
            2 => Self::Counter,
            3 => Self::Histogram,
            _ => Self::Unknown,
        }
    }

    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Gauge => "gauge",
            Self::Counter => "counter",
            Self::Histogram => "histogram",
        }
    }
}

impl std::fmt::Display for MetricType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Aggregation temporality
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Temporality {
    Unspecified = 0,
    Cumulative = 1,
    Delta = 2,
}

impl Temporality {
    #[inline]
    pub const fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Cumulative,
            2 => Self::Delta,
            _ => Self::Unspecified,
        }
    }

    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unspecified => "unspecified",
            Self::Cumulative => "cumulative",
            Self::Delta => "delta",
        }
    }
}

impl std::fmt::Display for Temporality {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

// =============================================================================
// Decoded Types
// =============================================================================

/// A string label (key-value pair)
#[derive(Debug, Clone)]
pub struct DecodedLabel<'a> {
    pub key: &'a str,
    pub value: &'a str,
}

/// An integer label (key-value pair)
#[derive(Debug, Clone)]
pub struct DecodedIntLabel<'a> {
    pub key: &'a str,
    pub value: i64,
}

/// A histogram bucket
#[derive(Debug, Clone, Copy)]
pub struct DecodedBucket {
    pub upper_bound: f64,
    pub count: u64,
}

/// Histogram data
#[derive(Debug, Clone)]
pub struct DecodedHistogram {
    pub count: u64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
    pub buckets: Vec<DecodedBucket>,
}

/// A decoded metric entry from MetricData
#[derive(Debug, Clone)]
pub struct DecodedMetric<'a> {
    pub metric_type: MetricType,
    pub timestamp: u64,
    pub name: &'a str,
    pub value: f64,
    pub source: Option<&'a str>,
    pub service: Option<&'a str>,
    pub labels: Vec<DecodedLabel<'a>>,
    pub int_labels: Vec<DecodedIntLabel<'a>>,
    pub temporality: Temporality,
    pub histogram: Option<DecodedHistogram>,
    pub session_id: Option<&'a [u8; 16]>,
}

// =============================================================================
// MetricData Parser
// =============================================================================

/// Parse MetricData FlatBuffer from bytes
pub fn decode_metric_data(buf: &[u8]) -> Result<Vec<DecodedMetric<'_>>> {
    if buf.len() < 8 {
        return Err(ProtocolError::too_short(8, buf.len()));
    }

    let root_offset = read_u32(buf, 0)? as usize;
    if root_offset >= buf.len() {
        return Err(ProtocolError::invalid_flatbuffer(
            "root offset out of bounds",
        ));
    }

    let table = FlatTable::parse(buf, root_offset)?;

    let metrics_vec = table
        .read_vector_of_tables(0)?
        .ok_or_else(|| ProtocolError::missing_field("metrics"))?;

    let mut metrics = Vec::with_capacity(metrics_vec.len());
    for metric_table in metrics_vec {
        let metric = parse_metric(&metric_table)?;
        metrics.push(metric);
    }

    Ok(metrics)
}

fn parse_metric<'a>(table: &FlatTable<'a>) -> Result<DecodedMetric<'a>> {
    // Field 0: metric_type (u8)
    let metric_type = MetricType::from_u8(table.read_u8(0, 0));

    // Field 1: timestamp (u64)
    let timestamp = table.read_u64(1, 0);

    // Field 2: name (string, required)
    let name = table.read_string(2)?.unwrap_or("");

    // Field 3: value (double)
    let value = table.read_f64(3, 0.0);

    // Field 4: source (string)
    let source = table.read_string(4)?;

    // Field 5: service (string)
    let service = table.read_string(5)?;

    // Field 6: labels (vector of Label tables)
    let labels = parse_labels(table, 6)?;

    // Field 7: temporality (u8)
    let temporality = Temporality::from_u8(table.read_u8(7, 0));

    // Field 8: histogram (Histogram table)
    let histogram = parse_histogram(table, 8)?;

    // Field 9: session_id (16 bytes)
    let session_id = table.read_fixed_bytes::<16>(9)?;

    // Field 10: int_labels (vector of IntLabel tables)
    let int_labels = parse_int_labels(table, 10)?;

    Ok(DecodedMetric {
        metric_type,
        timestamp,
        name,
        value,
        source,
        service,
        labels,
        int_labels,
        temporality,
        histogram,
        session_id,
    })
}

fn parse_labels<'a>(table: &FlatTable<'a>, field: usize) -> Result<Vec<DecodedLabel<'a>>> {
    let Some(labels_vec) = table.read_vector_of_tables(field)? else {
        return Ok(Vec::new());
    };

    let mut labels = Vec::with_capacity(labels_vec.len());
    for label_table in labels_vec {
        let key = label_table.read_string(0)?.unwrap_or("");
        let value = label_table.read_string(1)?.unwrap_or("");
        labels.push(DecodedLabel { key, value });
    }
    Ok(labels)
}

fn parse_int_labels<'a>(table: &FlatTable<'a>, field: usize) -> Result<Vec<DecodedIntLabel<'a>>> {
    let Some(labels_vec) = table.read_vector_of_tables(field)? else {
        return Ok(Vec::new());
    };

    let mut labels = Vec::with_capacity(labels_vec.len());
    for label_table in labels_vec {
        let key = label_table.read_string(0)?.unwrap_or("");
        let value = label_table.read_i64(1, 0);
        labels.push(DecodedIntLabel { key, value });
    }
    Ok(labels)
}

fn parse_histogram<'a>(table: &FlatTable<'a>, field: usize) -> Result<Option<DecodedHistogram>> {
    let Some(hist_table) = table.read_table(field)? else {
        return Ok(None);
    };

    // Field 0: count (u64)
    let count = hist_table.read_u64(0, 0);

    // Field 1: sum (double)
    let sum = hist_table.read_f64(1, 0.0);

    // Field 2: buckets (vector of Bucket tables)
    let buckets = if let Some(buckets_vec) = hist_table.read_vector_of_tables(2)? {
        let mut buckets = Vec::with_capacity(buckets_vec.len());
        for bucket_table in buckets_vec {
            let upper_bound = bucket_table.read_f64(0, 0.0);
            let bucket_count = bucket_table.read_u64(1, 0);
            buckets.push(DecodedBucket {
                upper_bound,
                count: bucket_count,
            });
        }
        buckets
    } else {
        Vec::new()
    };

    // Field 3: min (double)
    let min = hist_table.read_f64(3, 0.0);

    // Field 4: max (double)
    let max = hist_table.read_f64(4, 0.0);

    Ok(Some(DecodedHistogram {
        count,
        sum,
        min,
        max,
        buckets,
    }))
}
