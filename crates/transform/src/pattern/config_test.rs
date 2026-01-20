//! Tests for pattern configuration

use super::*;

#[test]
fn test_default_config() {
    let config = PatternConfig::default();

    assert!(config.enabled);
    assert_eq!(config.similarity_threshold, 0.5);
    assert_eq!(config.max_child_nodes, 100);
    assert_eq!(config.cache_size, 100_000);
    assert!(!config.persistence.enabled);
    assert!(!config.reload.enabled);
}

#[test]
fn test_config_builder() {
    let config = PatternConfig::new()
        .with_similarity_threshold(0.7)
        .with_max_child_nodes(50)
        .with_cache_size(50_000);

    assert_eq!(config.similarity_threshold, 0.7);
    assert_eq!(config.max_child_nodes, 50);
    assert_eq!(config.cache_size, 50_000);
}

#[test]
fn test_similarity_threshold_clamping() {
    let config = PatternConfig::new().with_similarity_threshold(1.5);
    assert_eq!(config.similarity_threshold, 1.0);

    let config = PatternConfig::new().with_similarity_threshold(-0.5);
    assert_eq!(config.similarity_threshold, 0.0);
}

#[test]
fn test_max_child_nodes_minimum() {
    let config = PatternConfig::new().with_max_child_nodes(0);
    assert_eq!(config.max_child_nodes, 1);
}

#[test]
fn test_disabled_config() {
    let config = PatternConfig::new().disabled();
    assert!(!config.enabled);
}

#[test]
fn test_config_validation_valid() {
    let config = PatternConfig::default();
    assert!(config.validate().is_ok());
}

#[test]
fn test_config_validation_invalid_threshold() {
    let mut config = PatternConfig::default();
    config.similarity_threshold = 1.5; // Bypass builder clamping

    let result = config.validate();
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("similarity_threshold"));
}

#[test]
fn test_config_validation_invalid_max_children() {
    let mut config = PatternConfig::default();
    config.max_child_nodes = 0;

    let result = config.validate();
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("max_child_nodes"));
}

#[test]
fn test_persistence_config_default() {
    let config = PersistenceConfig::default();

    assert!(!config.enabled);
    assert!(config.file_path.is_none());
    assert_eq!(config.flush_interval, Duration::from_secs(5));
    assert_eq!(config.batch_size, 100);
}

#[test]
fn test_persistence_with_file() {
    let config = PersistenceConfig::default()
        .with_file(PathBuf::from("/tmp/patterns.json"));

    assert!(config.enabled);
    assert_eq!(config.file_path, Some(PathBuf::from("/tmp/patterns.json")));
}

#[test]
fn test_persistence_validation_enabled_no_path() {
    let mut config = PersistenceConfig::default();
    config.enabled = true;
    // file_path is None

    let result = config.validate();
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("file_path"));
}

#[test]
fn test_persistence_validation_zero_flush_interval() {
    let mut config = PersistenceConfig::default();
    config.flush_interval = Duration::ZERO;

    let result = config.validate();
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("flush_interval"));
}

#[test]
fn test_persistence_validation_zero_batch_size() {
    let mut config = PersistenceConfig::default();
    config.batch_size = 0;

    let result = config.validate();
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("batch_size"));
}

#[test]
fn test_reload_config_default() {
    let config = ReloadConfig::default();

    assert!(!config.enabled);
    assert_eq!(config.interval, Duration::from_secs(60));
}

#[test]
fn test_reload_with_interval() {
    let config = ReloadConfig::default()
        .with_interval(Duration::from_secs(30));

    assert!(config.enabled);
    assert_eq!(config.interval, Duration::from_secs(30));
}

#[test]
fn test_reload_validation_enabled_zero_interval() {
    let mut config = ReloadConfig::default();
    config.enabled = true;
    config.interval = Duration::ZERO;

    let result = config.validate();
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("interval"));
}

#[test]
fn test_clickhouse_config_default() {
    let config = ClickHouseConfig::default();

    assert_eq!(config.url, "http://localhost:8123");
    assert_eq!(config.database, "tell");
    assert_eq!(config.table, "log_patterns");
    assert_eq!(config.pool_size, 5);
}
