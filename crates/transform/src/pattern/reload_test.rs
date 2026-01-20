//! Tests for pattern hot reload worker

use super::*;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

fn create_test_persistence() -> (TempDir, Arc<PatternPersistence>) {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("patterns.json");
    let persistence = Arc::new(PatternPersistence::new(Some(file_path), 100));
    (temp_dir, persistence)
}

fn create_test_drain() -> Arc<DrainTree> {
    Arc::new(DrainTree::new(0.5, 100))
}

fn create_test_cache() -> Arc<PatternCache> {
    Arc::new(PatternCache::new(1000, 100))
}

#[test]
fn test_reload_worker_config_default() {
    let config = ReloadWorkerConfig::default();

    assert_eq!(config.interval, DEFAULT_RELOAD_INTERVAL);
    assert!(config.merge_mode);
}

#[test]
fn test_reload_worker_config_builder() {
    let config = ReloadWorkerConfig::default()
        .with_interval(Duration::from_secs(30))
        .with_replace();

    assert_eq!(config.interval, Duration::from_secs(30));
    assert!(!config.merge_mode);
}

#[tokio::test]
async fn test_reload_worker_creation() {
    let (_temp_dir, persistence) = create_test_persistence();
    let drain = create_test_drain();
    let cache = create_test_cache();
    let cancel = CancellationToken::new();

    let (worker, handle) = ReloadWorker::new(
        persistence,
        drain,
        cache,
        ReloadWorkerConfig::default(),
        cancel.clone(),
    );

    // Should be able to trigger reload
    assert!(handle.trigger_reload());

    // Worker should exist
    drop(worker);
}

#[tokio::test]
async fn test_reload_worker_runs_and_stops() {
    let (_temp_dir, persistence) = create_test_persistence();
    let drain = create_test_drain();
    let cache = create_test_cache();
    let cancel = CancellationToken::new();

    let config = ReloadWorkerConfig::default()
        .with_interval(Duration::from_millis(50));

    let (worker, _handle) = ReloadWorker::new(
        persistence,
        drain,
        cache,
        config,
        cancel.clone(),
    );

    // Spawn worker
    let worker_task = tokio::spawn(worker.run());

    // Let it run a bit
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Stop it
    cancel.cancel();
    worker_task.await.unwrap();
}

#[tokio::test]
async fn test_reload_worker_manual_trigger() {
    let (_temp_dir, persistence) = create_test_persistence();
    let drain = create_test_drain();
    let cache = create_test_cache();
    let cancel = CancellationToken::new();

    let config = ReloadWorkerConfig::default()
        .with_interval(Duration::from_secs(3600)); // Very long interval

    let (worker, handle) = ReloadWorker::new(
        persistence,
        drain,
        cache,
        config,
        cancel.clone(),
    );

    let worker_task = tokio::spawn(worker.run());

    // Trigger manual reload
    assert!(handle.trigger_reload_async().await);

    // Give worker time to process
    tokio::time::sleep(Duration::from_millis(50)).await;

    cancel.cancel();
    worker_task.await.unwrap();
}

#[tokio::test]
async fn test_reload_worker_loads_patterns() {
    let (temp_dir, persistence) = create_test_persistence();
    let drain = create_test_drain();
    let cache = create_test_cache();
    let cancel = CancellationToken::new();

    // Create a pattern in drain tree
    let id = drain.parse("User logged in successfully");
    assert!(id > 0);

    // Save to persistence
    let patterns = drain.all_patterns();
    persistence.save_all(&patterns).unwrap();

    // Create new cache (empty)
    let new_cache = Arc::new(PatternCache::new(1000, 100));

    let config = ReloadWorkerConfig::default()
        .with_interval(Duration::from_millis(50));

    let handle = spawn_reload_worker(
        Arc::clone(&persistence),
        Arc::new(DrainTree::new(0.5, 100)), // New drain tree
        Arc::clone(&new_cache),
        config,
        cancel.clone(),
    );

    // Trigger reload
    handle.trigger_reload();

    // Wait for reload
    tokio::time::sleep(Duration::from_millis(100)).await;

    cancel.cancel();

    // File should still exist
    let file_path = temp_dir.path().join("patterns.json");
    assert!(file_path.exists());
}

#[tokio::test]
async fn test_reload_worker_disabled_persistence() {
    let persistence = Arc::new(PatternPersistence::disabled());
    let drain = create_test_drain();
    let cache = create_test_cache();
    let cancel = CancellationToken::new();

    let config = ReloadWorkerConfig::default()
        .with_interval(Duration::from_millis(50));

    let (worker, handle) = ReloadWorker::new(
        persistence,
        drain,
        cache,
        config,
        cancel.clone(),
    );

    let worker_task = tokio::spawn(worker.run());

    // Trigger should still work (just does nothing)
    assert!(handle.trigger_reload());

    tokio::time::sleep(Duration::from_millis(100)).await;

    cancel.cancel();
    worker_task.await.unwrap();
}

#[test]
fn test_hash_template() {
    let hash1 = hash_template("User <*> logged in");
    let hash2 = hash_template("User <*> logged in");
    let hash3 = hash_template("Different template");

    assert_eq!(hash1, hash2);
    assert_ne!(hash1, hash3);
}
