//! Resilience utilities for connectors
//!
//! Provides timeout, retry, and circuit breaker patterns for reliable
//! connector operation.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::time::timeout;
use tracing::{debug, warn};

/// Default request timeout
pub const DEFAULT_TIMEOUT_SECS: u64 = 30;

/// Default max retries for transient failures
pub const DEFAULT_MAX_RETRIES: u32 = 3;

/// Default circuit breaker failure threshold
pub const DEFAULT_FAILURE_THRESHOLD: u32 = 5;

/// Default circuit breaker cooldown period
pub const DEFAULT_COOLDOWN_SECS: u64 = 600; // 10 minutes

/// Resilience configuration for a connector
#[derive(Debug, Clone)]
pub struct ResilienceConfig {
    /// Request timeout in seconds
    pub timeout_secs: u64,
    /// Maximum retry attempts for transient failures
    pub max_retries: u32,
    /// Base delay for exponential backoff (doubles each retry)
    pub retry_base_delay_ms: u64,
    /// Number of consecutive failures before circuit opens
    pub failure_threshold: u32,
    /// How long circuit stays open before allowing retry
    pub cooldown_secs: u64,
}

impl Default for ResilienceConfig {
    fn default() -> Self {
        Self {
            timeout_secs: DEFAULT_TIMEOUT_SECS,
            max_retries: DEFAULT_MAX_RETRIES,
            retry_base_delay_ms: 1000, // 1 second
            failure_threshold: DEFAULT_FAILURE_THRESHOLD,
            cooldown_secs: DEFAULT_COOLDOWN_SECS,
        }
    }
}

impl ResilienceConfig {
    /// Get timeout as Duration
    pub fn timeout(&self) -> Duration {
        Duration::from_secs(self.timeout_secs)
    }

    /// Get retry delay for attempt N (exponential backoff)
    pub fn retry_delay(&self, attempt: u32) -> Duration {
        let delay_ms = self.retry_base_delay_ms * (1 << attempt.min(6)); // cap at 64x
        Duration::from_millis(delay_ms)
    }
}

/// Circuit breaker state
#[derive(Debug)]
pub struct CircuitBreaker {
    /// Connector name for logging
    name: String,
    /// Consecutive failure count
    failures: AtomicU32,
    /// Timestamp when circuit opened (0 = closed)
    opened_at: AtomicU64,
    /// Configuration
    config: ResilienceConfig,
}

impl CircuitBreaker {
    /// Create a new circuit breaker
    pub fn new(name: impl Into<String>, config: ResilienceConfig) -> Self {
        Self {
            name: name.into(),
            failures: AtomicU32::new(0),
            opened_at: AtomicU64::new(0),
            config,
        }
    }

    /// Check if circuit is open (requests should be blocked)
    pub fn is_open(&self) -> bool {
        let opened_at = self.opened_at.load(Ordering::Relaxed);
        if opened_at == 0 {
            return false;
        }

        // Check if cooldown has passed
        let now = Instant::now().elapsed().as_secs();
        let elapsed = now.saturating_sub(opened_at);

        if elapsed >= self.config.cooldown_secs {
            // Cooldown passed, allow half-open state (one request)
            false
        } else {
            true
        }
    }

    /// Record a successful request
    pub fn record_success(&self) {
        let prev_failures = self.failures.swap(0, Ordering::Relaxed);
        let was_open = self.opened_at.swap(0, Ordering::Relaxed) > 0;

        if was_open {
            debug!(
                connector = %self.name,
                prev_failures,
                "circuit breaker closed after successful request"
            );
        }
    }

    /// Record a failed request, returns true if circuit just opened
    pub fn record_failure(&self) -> bool {
        let failures = self.failures.fetch_add(1, Ordering::Relaxed) + 1;

        if failures >= self.config.failure_threshold {
            let was_closed = self.opened_at.load(Ordering::Relaxed) == 0;
            if was_closed {
                // Use a simple timestamp (seconds since some point)
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                self.opened_at.store(now, Ordering::Relaxed);

                warn!(
                    connector = %self.name,
                    failures,
                    cooldown_secs = self.config.cooldown_secs,
                    "circuit breaker opened after consecutive failures"
                );
                return true;
            }
        }
        false
    }

    /// Get current failure count
    pub fn failure_count(&self) -> u32 {
        self.failures.load(Ordering::Relaxed)
    }

    /// Check if circuit is currently open
    pub fn is_currently_open(&self) -> bool {
        self.opened_at.load(Ordering::Relaxed) > 0
    }
}

/// Determines if an error is retryable
pub fn is_retryable_error(error: &reqwest::Error) -> bool {
    // Retry on timeout, connection errors, and 5xx responses
    if error.is_timeout() || error.is_connect() {
        return true;
    }

    if let Some(status) = error.status() {
        // Retry on server errors (5xx) and rate limits (429)
        return status.is_server_error() || status.as_u16() == 429;
    }

    false
}

/// Execute a request with timeout and retry logic
pub async fn execute_with_retry<F, Fut, T, E>(
    config: &ResilienceConfig,
    circuit: Option<&CircuitBreaker>,
    operation_name: &str,
    mut operation: F,
) -> Result<T, RetryError<E>>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    // Check circuit breaker
    if let Some(cb) = circuit
        && cb.is_open()
    {
        return Err(RetryError::CircuitOpen);
    }

    let mut last_error = None;

    for attempt in 0..=config.max_retries {
        if attempt > 0 {
            let delay = config.retry_delay(attempt - 1);
            debug!(
                operation = operation_name,
                attempt,
                delay_ms = delay.as_millis(),
                "retrying after delay"
            );
            tokio::time::sleep(delay).await;
        }

        // Execute with timeout
        let result = timeout(config.timeout(), operation()).await;

        match result {
            Ok(Ok(value)) => {
                // Success
                if let Some(cb) = circuit {
                    cb.record_success();
                }
                return Ok(value);
            }
            Ok(Err(e)) => {
                // Operation failed
                last_error = Some(format!("{}", e));

                if attempt < config.max_retries {
                    debug!(
                        operation = operation_name,
                        attempt,
                        error = %e,
                        "request failed, will retry"
                    );
                }
            }
            Err(_) => {
                // Timeout
                last_error = Some("request timed out".to_string());

                if attempt < config.max_retries {
                    debug!(
                        operation = operation_name,
                        attempt,
                        timeout_secs = config.timeout_secs,
                        "request timed out, will retry"
                    );
                }
            }
        }
    }

    // All retries exhausted
    if let Some(cb) = circuit {
        cb.record_failure();
    }

    Err(RetryError::Exhausted {
        attempts: config.max_retries + 1,
        last_error: last_error.unwrap_or_else(|| "unknown error".to_string()),
    })
}

/// Error from retry operation
#[derive(Debug)]
pub enum RetryError<E> {
    /// Circuit breaker is open
    CircuitOpen,
    /// All retry attempts exhausted
    Exhausted {
        attempts: u32,
        last_error: String,
    },
    /// Non-retryable error
    Permanent(E),
}

impl<E: std::fmt::Display> std::fmt::Display for RetryError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RetryError::CircuitOpen => write!(f, "circuit breaker is open"),
            RetryError::Exhausted { attempts, last_error } => {
                write!(f, "failed after {} attempts: {}", attempts, last_error)
            }
            RetryError::Permanent(e) => write!(f, "permanent error: {}", e),
        }
    }
}

/// Metrics for connector resilience
#[derive(Debug, Default)]
pub struct ResilienceMetrics {
    /// Total requests attempted
    pub requests: AtomicU64,
    /// Successful requests
    pub successes: AtomicU64,
    /// Failed requests (after all retries)
    pub failures: AtomicU64,
    /// Requests blocked by circuit breaker
    pub circuit_blocked: AtomicU64,
    /// Total retries performed
    pub retries: AtomicU64,
    /// Timeouts
    pub timeouts: AtomicU64,
}

impl ResilienceMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_success(&self) {
        self.requests.fetch_add(1, Ordering::Relaxed);
        self.successes.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_failure(&self) {
        self.requests.fetch_add(1, Ordering::Relaxed);
        self.failures.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_circuit_blocked(&self) {
        self.circuit_blocked.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_retry(&self) {
        self.retries.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_timeout(&self) {
        self.timeouts.fetch_add(1, Ordering::Relaxed);
    }
}
