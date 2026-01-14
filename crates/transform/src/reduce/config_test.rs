//! Tests for reduce configuration

use super::*;

#[test]
fn test_default_config() {
    let config = ReduceConfig::default();
    assert!(config.enabled);
    assert!(config.group_by.is_empty());
    assert_eq!(config.window_ms, 5000);
    assert_eq!(config.max_events, 1000);
    assert_eq!(config.min_events, 2);
}

#[test]
fn test_builder_pattern() {
    let config = ReduceConfig::new()
        .with_group_by(vec!["error_code".to_string(), "host".to_string()])
        .with_window_ms(10000)
        .with_max_events(500)
        .with_min_events(5);

    assert_eq!(config.group_by, vec!["error_code", "host"]);
    assert_eq!(config.window_ms, 10000);
    assert_eq!(config.max_events, 500);
    assert_eq!(config.min_events, 5);
}

#[test]
fn test_disabled() {
    let config = ReduceConfig::new().disabled();
    assert!(!config.enabled);
}

#[test]
fn test_window_duration() {
    let config = ReduceConfig::new().with_window_ms(3000);
    assert_eq!(config.window_duration(), std::time::Duration::from_millis(3000));
}

#[test]
fn test_validation_valid() {
    let config = ReduceConfig::default();
    assert!(config.validate().is_ok());
}

#[test]
fn test_validation_zero_window() {
    let config = ReduceConfig::new().with_window_ms(0);
    let err = config.validate().unwrap_err();
    assert!(err.contains("window_ms"));
}

#[test]
fn test_validation_min_exceeds_max() {
    let config = ReduceConfig::new()
        .with_max_events(10)
        .with_min_events(20);
    let err = config.validate().unwrap_err();
    assert!(err.contains("min_events"));
    assert!(err.contains("max_events"));
}

#[test]
fn test_max_events_minimum() {
    let config = ReduceConfig::new().with_max_events(0);
    assert_eq!(config.max_events, 1); // Clamped to minimum
}

#[test]
fn test_min_events_minimum() {
    let config = ReduceConfig::new().with_min_events(0);
    assert_eq!(config.min_events, 1); // Clamped to minimum
}
