//! Tests for time series types

use crate::timeseries::{ComparisonData, TimeSeriesData, TimeSeriesPoint};

#[test]
fn test_point_new() {
    let point = TimeSeriesPoint::new("2024-01-15", 100.0);
    assert_eq!(point.date, "2024-01-15");
    assert_eq!(point.value, 100.0);
    assert!(point.dimension.is_none());
}

#[test]
fn test_point_with_dimension() {
    let point = TimeSeriesPoint::with_dimension("2024-01-15", 100.0, "mobile");
    assert_eq!(point.date, "2024-01-15");
    assert_eq!(point.value, 100.0);
    assert_eq!(point.dimension, Some("mobile".to_string()));
}

#[test]
fn test_timeseries_empty() {
    let ts = TimeSeriesData::empty();
    assert!(ts.is_empty());
    assert_eq!(ts.total, 0.0);
    assert_eq!(ts.min, 0.0);
    assert_eq!(ts.max, 0.0);
    assert_eq!(ts.avg, 0.0);
}

#[test]
fn test_timeseries_from_points() {
    let points = vec![
        TimeSeriesPoint::new("2024-01-01", 10.0),
        TimeSeriesPoint::new("2024-01-02", 20.0),
        TimeSeriesPoint::new("2024-01-03", 30.0),
    ];

    let ts = TimeSeriesData::from_points(points);

    assert_eq!(ts.len(), 3);
    assert_eq!(ts.total, 60.0);
    assert_eq!(ts.min, 10.0);
    assert_eq!(ts.max, 30.0);
    assert_eq!(ts.avg, 20.0);
}

#[test]
fn test_timeseries_single_point() {
    let points = vec![TimeSeriesPoint::new("2024-01-01", 42.0)];

    let ts = TimeSeriesData::from_points(points);

    assert_eq!(ts.total, 42.0);
    assert_eq!(ts.min, 42.0);
    assert_eq!(ts.max, 42.0);
    assert_eq!(ts.avg, 42.0);
}

#[test]
fn test_comparison_data_calculate() {
    // Growth case
    let comp = ComparisonData::calculate(150.0, 100.0);
    assert_eq!(comp.previous_total, 100.0);
    assert_eq!(comp.change, 50.0);
    assert_eq!(comp.percent_change, 50.0);

    // Decline case
    let comp = ComparisonData::calculate(80.0, 100.0);
    assert_eq!(comp.change, -20.0);
    assert_eq!(comp.percent_change, -20.0);

    // No change
    let comp = ComparisonData::calculate(100.0, 100.0);
    assert_eq!(comp.change, 0.0);
    assert_eq!(comp.percent_change, 0.0);
}

#[test]
fn test_comparison_from_zero() {
    // Previous was 0, now has value
    let comp = ComparisonData::calculate(100.0, 0.0);
    assert_eq!(comp.percent_change, 100.0);

    // Both zero
    let comp = ComparisonData::calculate(0.0, 0.0);
    assert_eq!(comp.percent_change, 0.0);
}

#[test]
fn test_timeseries_with_comparison() {
    let points = vec![TimeSeriesPoint::new("2024-01-01", 100.0)];
    let ts = TimeSeriesData::from_points(points);

    let comparison = ComparisonData::calculate(100.0, 80.0);
    let ts = ts.with_comparison(comparison);

    assert!(ts.comparison.is_some());
    let comp = ts.comparison.unwrap();
    assert_eq!(comp.previous_total, 80.0);
    assert_eq!(comp.percent_change, 25.0);
}

#[test]
fn test_timeseries_serialization() {
    let points = vec![TimeSeriesPoint::new("2024-01-01", 100.0)];
    let ts = TimeSeriesData::from_points(points);

    let json = serde_json::to_string(&ts).unwrap();
    assert!(json.contains("2024-01-01"));
    assert!(json.contains("100"));

    // Deserialize back
    let parsed: TimeSeriesData = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.total, 100.0);
}
