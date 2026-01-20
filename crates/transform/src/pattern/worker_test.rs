//! Tests for background pattern worker

use super::*;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

fn create_test_persistence() -> (TempDir, Arc<PatternPersistence>) {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("patterns.json");
    let persistence = Arc::new(PatternPersistence::new(Some(file_path), 100));
    (temp_dir, persistence)
}

fn create_test_pattern(id: u64, template: &str) -> Pattern {
    let tokens: Vec<Option<String>> = template
        .split_whitespace()
        .map(|t| {
            if t == "<*>" {
                None
            } else {
                Some(t.to_string())
            }
        })
        .collect();

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    Pattern {
        id,
        template: template.to_string(),
        canonical_name: super::super::drain::generate_canonical_name(&tokens),
        tokens,
        count: 1,
        first_seen: now,
        last_seen: now,
    }
}

#[test]
fn test_worker_config_default() {
    let config = WorkerConfig::default();

    assert_eq!(config.channel_capacity, DEFAULT_CHANNEL_CAPACITY);
    assert_eq!(config.flush_interval, DEFAULT_FLUSH_INTERVAL);
    assert_eq!(config.batch_size, DEFAULT_BATCH_SIZE);
}

#[test]
fn test_worker_config_builder() {
    let config = WorkerConfig::default()
        .with_channel_capacity(5000)
        .with_flush_interval(Duration::from_secs(10))
        .with_batch_size(50);

    assert_eq!(config.channel_capacity, 5000);
    assert_eq!(config.flush_interval, Duration::from_secs(10));
    assert_eq!(config.batch_size, 50);
}

#[test]
fn test_worker_config_minimum_values() {
    let config = WorkerConfig::default()
        .with_channel_capacity(0)
        .with_batch_size(0);

    // Should be clamped to minimum of 1
    assert_eq!(config.channel_capacity, 1);
    assert_eq!(config.batch_size, 1);
}

#[tokio::test]
async fn test_worker_handle_send() {
    let (_temp_dir, persistence) = create_test_persistence();
    let cancel = CancellationToken::new();
    let config = WorkerConfig::default().with_channel_capacity(100);

    let (worker, handle) = PatternWorker::new(persistence, config, cancel.clone());

    // Spawn worker
    let worker_task = tokio::spawn(worker.run());

    // Send pattern
    let pattern = create_test_pattern(1, "User logged in");
    assert!(handle.send(pattern));

    // Worker should be active
    assert!(handle.is_active());

    // Shutdown
    cancel.cancel();
    worker_task.await.unwrap();

    // Handle should be inactive after shutdown
    assert!(!handle.is_active());
}

#[tokio::test]
async fn test_worker_batch_flush() {
    let (temp_dir, persistence) = create_test_persistence();
    let cancel = CancellationToken::new();
    let config = WorkerConfig::default()
        .with_batch_size(3)
        .with_flush_interval(Duration::from_secs(60)); // Long interval to test batch-based flush

    let (worker, handle) = PatternWorker::new(Arc::clone(&persistence), config, cancel.clone());

    // Spawn worker
    let worker_task = tokio::spawn(worker.run());

    // Send 3 patterns (should trigger batch flush)
    for i in 1..=3 {
        let pattern = create_test_pattern(i, &format!("Pattern {}", i));
        handle.send(pattern);
    }

    // Give worker time to process
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Shutdown
    cancel.cancel();
    worker_task.await.unwrap();

    // Check file was written
    let file_path = temp_dir.path().join("patterns.json");
    assert!(file_path.exists());

    // Load and verify
    let loaded = persistence.load().unwrap();
    assert_eq!(loaded.len(), 3);
}

#[tokio::test]
async fn test_worker_periodic_flush() {
    let (temp_dir, persistence) = create_test_persistence();
    let cancel = CancellationToken::new();
    let config = WorkerConfig::default()
        .with_batch_size(100) // High threshold
        .with_flush_interval(Duration::from_millis(50)); // Short interval

    let (worker, handle) = PatternWorker::new(Arc::clone(&persistence), config, cancel.clone());

    // Spawn worker
    let worker_task = tokio::spawn(worker.run());

    // Send 2 patterns (below batch threshold)
    for i in 1..=2 {
        let pattern = create_test_pattern(i, &format!("Pattern {}", i));
        handle.send(pattern);
    }

    // Wait for periodic flush
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Shutdown
    cancel.cancel();
    worker_task.await.unwrap();

    // Check patterns were persisted
    let file_path = temp_dir.path().join("patterns.json");
    assert!(file_path.exists());

    let loaded = persistence.load().unwrap();
    assert_eq!(loaded.len(), 2);
}

#[tokio::test]
async fn test_worker_shutdown_flush() {
    let (temp_dir, persistence) = create_test_persistence();
    let cancel = CancellationToken::new();
    let config = WorkerConfig::default()
        .with_batch_size(100) // High threshold
        .with_flush_interval(Duration::from_secs(60)); // Long interval

    let (worker, handle) = PatternWorker::new(Arc::clone(&persistence), config, cancel.clone());

    // Spawn worker
    let worker_task = tokio::spawn(worker.run());

    // Send patterns
    for i in 1..=5 {
        let pattern = create_test_pattern(i, &format!("Pattern {}", i));
        handle.send(pattern);
    }

    // Give a moment for patterns to be received
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Shutdown immediately (should flush remaining)
    cancel.cancel();
    worker_task.await.unwrap();

    // All patterns should be persisted
    let file_path = temp_dir.path().join("patterns.json");
    assert!(file_path.exists());

    let loaded = persistence.load().unwrap();
    assert_eq!(loaded.len(), 5);
}

#[tokio::test]
async fn test_worker_channel_full_returns_false() {
    let (_temp_dir, persistence) = create_test_persistence();
    let cancel = CancellationToken::new();
    let config = WorkerConfig::default()
        .with_channel_capacity(2); // Very small capacity

    let (_worker, handle) = PatternWorker::new(persistence, config, cancel);

    // Note: We don't spawn the worker, so the channel will fill up

    // Send patterns until channel is full
    let p1 = create_test_pattern(1, "Pattern 1");
    let p2 = create_test_pattern(2, "Pattern 2");
    let p3 = create_test_pattern(3, "Pattern 3");

    assert!(handle.send(p1));
    assert!(handle.send(p2));
    // Third should fail (channel full)
    assert!(!handle.send(p3));
}

#[tokio::test]
async fn test_spawn_pattern_worker() {
    let (_temp_dir, persistence) = create_test_persistence();
    let cancel = CancellationToken::new();
    let config = WorkerConfig::default();

    let handle = spawn_pattern_worker(persistence, config, cancel.clone());

    // Should be active
    assert!(handle.is_active());

    // Send a pattern
    let pattern = create_test_pattern(1, "Test pattern");
    assert!(handle.send(pattern));

    // Shutdown
    cancel.cancel();

    // Give time for worker to stop
    tokio::time::sleep(Duration::from_millis(50)).await;
}
